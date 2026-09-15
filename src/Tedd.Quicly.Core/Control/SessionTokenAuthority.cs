using System.Buffers.Binary;
using System.Security.Cryptography;
using Tedd.Quicly.Core.Time;

namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Server-side minting and validation of session tokens (PROTOCOL.md §4.1).
/// </summary>
/// <remarks>
/// <para>
/// Token layout (<see cref="TokenLength"/> = 69 bytes, integers little-endian):
/// <c>version u8 (= 1) ‖ sessionId u64 ‖ epoch u32 ‖ expiry i64 ‖ random 16 ‖ HMAC-SHA256(key, all 37 preceding bytes)</c>.
/// <c>sessionId</c> and <c>epoch</c> are in clear because a token is a <em>locator</em>, not a credential (a resume must
/// also present an auth token the admission policy accepts); the HMAC makes every field tamper-evident. <c>expiry</c>
/// is absolute microseconds on this authority's <see cref="IClock"/>; with a process-relative clock tokens do not
/// survive a restart, which matches sessions not surviving one.
/// </para>
/// <para>
/// Single use: <see cref="TryValidate"/> records the token's random part in a bounded replay cache (entries expire
/// with their token); presenting it again yields <see cref="SessionTokenStatus.Replayed"/>. When the cache holds
/// <c>replayCacheCapacity</c> unexpired entries a new token is refused with
/// <see cref="SessionTokenStatus.ReplayCacheFull"/> (fail closed). Invalidating older tokens once a newer one was issued
/// is the session registry's job: it compares the returned <c>epoch</c> with the session's current epoch.
/// </para>
/// <para>
/// Rotation: <see cref="RotateKey"/> (or the two-key constructor) keeps accepting tokens signed with the previous key
/// until a deadline; new tokens are always signed with the current key. MACs are compared with
/// <see cref="CryptographicOperations.FixedTimeEquals(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>.
/// </para>
/// <para>
/// Thread-safe. <see cref="Mint"/>, <see cref="TryValidate"/> and <see cref="TryInspect"/> do not allocate (HMAC is
/// computed with the one-shot API into stack buffers; the replay cache is pre-allocated).
/// </para>
/// </remarks>
public sealed class SessionTokenAuthority : IDisposable
{
    /// <summary>Encoded token length in bytes.</summary>
    public const int TokenLength = 69;

    /// <summary>Required HMAC key length in bytes.</summary>
    public const int KeyLength = 32;

    /// <summary>Token format version (first byte).</summary>
    public const byte FormatVersion = 1;

    /// <summary>Default replay-cache capacity (unexpired consumed tokens remembered at once).</summary>
    public const int DefaultReplayCacheCapacity = 16_384;

    private const int SessionIdOffset = 1;
    private const int EpochOffset = 9;
    private const int ExpiryOffset = 13;
    private const int RandomOffset = 21;
    private const int RandomLength = 16;
    private const int MacOffset = 37;
    private const int MacLength = 32;

    private readonly IClock _clock;
    private readonly ReplayCache _replayCache;
    private readonly Lock _lock = new();
    private KeySet? _keys;

    /// <summary>Creates an authority with a single key.</summary>
    /// <param name="key">The 32-byte HMAC key (copied).</param>
    /// <param name="clock">Clock used for expiry and key deadlines.</param>
    /// <param name="replayCacheCapacity">Consumed, unexpired tokens remembered at once (1 … 4 194 304).</param>
    /// <exception cref="ArgumentException"><paramref name="key"/> is not 32 bytes.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="replayCacheCapacity"/> is out of range.</exception>
    public SessionTokenAuthority(ReadOnlySpan<byte> key, IClock clock, int replayCacheCapacity = DefaultReplayCacheCapacity)
        : this(key, ReadOnlySpan<byte>.Empty, long.MinValue, clock, replayCacheCapacity)
    {
    }

    /// <summary>Creates an authority that also accepts tokens signed with <paramref name="previousKey"/> until a deadline.</summary>
    /// <param name="key">The current 32-byte HMAC key (copied); signs every new token.</param>
    /// <param name="previousKey">The previous 32-byte key (copied), or empty for none.</param>
    /// <param name="previousKeyValidUntilMicros">Tokens signed with <paramref name="previousKey"/> verify while <c>now</c> is before this time.</param>
    /// <param name="clock">Clock used for expiry and key deadlines.</param>
    /// <param name="replayCacheCapacity">Consumed, unexpired tokens remembered at once (1 … 4 194 304).</param>
    /// <exception cref="ArgumentException">A key is not 32 bytes.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="replayCacheCapacity"/> is out of range.</exception>
    public SessionTokenAuthority(ReadOnlySpan<byte> key, ReadOnlySpan<byte> previousKey, long previousKeyValidUntilMicros, IClock clock,
        int replayCacheCapacity = DefaultReplayCacheCapacity)
    {
        ArgumentNullException.ThrowIfNull(clock);
        ThrowIfBadKey(key, nameof(key));
        if (!previousKey.IsEmpty)
        {
            ThrowIfBadKey(previousKey, nameof(previousKey));
        }

        _clock = clock;
        _replayCache = new ReplayCache(replayCacheCapacity);
        _keys = new KeySet(key.ToArray(), previousKey.IsEmpty ? null : previousKey.ToArray(), previousKeyValidUntilMicros);
    }

    /// <summary>Capacity of the replay cache.</summary>
    public int ReplayCacheCapacity => _replayCache.Capacity;

    /// <summary>Entries currently held by the replay cache (diagnostics; includes expired entries not yet swept).</summary>
    public int ReplayCacheCount
    {
        get
        {
            lock (_lock)
            {
                return _replayCache.Count;
            }
        }
    }

    /// <summary>Mints a token for (<paramref name="sessionId"/>, <paramref name="epoch"/>) signed with the current key.</summary>
    /// <param name="sessionId">The session.</param>
    /// <param name="epoch">The epoch the token is issued in (the HelloAck's epoch).</param>
    /// <param name="expiryMicros">Absolute expiry on this authority's clock (typically <c>now + graceMicros</c>).</param>
    /// <param name="destination">Receives the token; at least <see cref="TokenLength"/> bytes.</param>
    /// <returns><see cref="TokenLength"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is shorter than <see cref="TokenLength"/>.</exception>
    /// <exception cref="ObjectDisposedException">The authority was disposed.</exception>
    public int Mint(ulong sessionId, uint epoch, long expiryMicros, Span<byte> destination)
    {
        KeySet keys = Volatile.Read(ref _keys) ?? throw new ObjectDisposedException(nameof(SessionTokenAuthority));
        if (destination.Length < TokenLength)
        {
            throw new ArgumentException($"A session token needs {TokenLength} bytes.", nameof(destination));
        }

        Span<byte> token = destination.Slice(0, TokenLength);
        token[0] = FormatVersion;
        BinaryPrimitives.WriteUInt64LittleEndian(token.Slice(SessionIdOffset), sessionId);
        BinaryPrimitives.WriteUInt32LittleEndian(token.Slice(EpochOffset), epoch);
        BinaryPrimitives.WriteInt64LittleEndian(token.Slice(ExpiryOffset), expiryMicros);
        RandomNumberGenerator.Fill(token.Slice(RandomOffset, RandomLength));
        HMACSHA256.HashData(keys.Current, token.Slice(0, MacOffset), token.Slice(MacOffset, MacLength));
        return TokenLength;
    }

    /// <summary>
    /// Validates <paramref name="token"/> and consumes it (single use): on <see cref="SessionTokenStatus.Valid"/> the same
    /// token is <see cref="SessionTokenStatus.Replayed"/> from then on.
    /// </summary>
    /// <param name="token">The presented token (untrusted).</param>
    /// <param name="sessionId">The session named by the token when valid, otherwise 0.</param>
    /// <param name="epoch">The epoch the token was issued in when valid, otherwise 0.</param>
    /// <returns>The outcome; report anything but <see cref="SessionTokenStatus.Valid"/> to the peer as <see cref="HelloStatus.Rejected"/>.</returns>
    /// <exception cref="ObjectDisposedException">The authority was disposed.</exception>
    public SessionTokenStatus TryValidate(ReadOnlySpan<byte> token, out ulong sessionId, out uint epoch) =>
        Validate(token, consume: true, out sessionId, out epoch);

    /// <summary>Validates <paramref name="token"/> like <see cref="TryValidate"/> but does not consume it.</summary>
    /// <param name="token">The presented token (untrusted).</param>
    /// <param name="sessionId">The session named by the token when valid, otherwise 0.</param>
    /// <param name="epoch">The epoch the token was issued in when valid, otherwise 0.</param>
    /// <returns>The outcome (<see cref="SessionTokenStatus.Replayed"/> when it was already consumed).</returns>
    /// <exception cref="ObjectDisposedException">The authority was disposed.</exception>
    public SessionTokenStatus TryInspect(ReadOnlySpan<byte> token, out ulong sessionId, out uint epoch) =>
        Validate(token, consume: false, out sessionId, out epoch);

    /// <summary>
    /// Makes <paramref name="newKey"/> the signing key; the current key becomes the previous key and keeps verifying
    /// until <paramref name="previousKeyValidUntilMicros"/>. The key it replaces as "previous" is wiped.
    /// </summary>
    /// <param name="newKey">The new 32-byte key (copied).</param>
    /// <param name="previousKeyValidUntilMicros">Deadline for tokens signed with the outgoing key (typically <c>now + graceMicros</c>).</param>
    /// <exception cref="ArgumentException"><paramref name="newKey"/> is not 32 bytes.</exception>
    /// <exception cref="ObjectDisposedException">The authority was disposed.</exception>
    public void RotateKey(ReadOnlySpan<byte> newKey, long previousKeyValidUntilMicros)
    {
        ThrowIfBadKey(newKey, nameof(newKey));
        byte[] copy = newKey.ToArray();
        lock (_lock)
        {
            KeySet old = _keys ?? throw new ObjectDisposedException(nameof(SessionTokenAuthority));
            Volatile.Write(ref _keys, new KeySet(copy, old.Current, previousKeyValidUntilMicros));
            if (old.Previous is not null)
            {
                CryptographicOperations.ZeroMemory(old.Previous);
            }
        }
    }

    /// <summary>Wipes the keys. Further calls throw <see cref="ObjectDisposedException"/>.</summary>
    public void Dispose()
    {
        lock (_lock)
        {
            KeySet? keys = _keys;
            if (keys is null)
            {
                return;
            }

            Volatile.Write(ref _keys, null);
            CryptographicOperations.ZeroMemory(keys.Current);
            if (keys.Previous is not null)
            {
                CryptographicOperations.ZeroMemory(keys.Previous);
            }
        }
    }

    private SessionTokenStatus Validate(ReadOnlySpan<byte> token, bool consume, out ulong sessionId, out uint epoch)
    {
        sessionId = 0;
        epoch = 0;
        KeySet keys = Volatile.Read(ref _keys) ?? throw new ObjectDisposedException(nameof(SessionTokenAuthority));
        if (token.Length != TokenLength || token[0] != FormatVersion)
        {
            return SessionTokenStatus.Malformed;
        }

        long now = _clock.NowMicros;
        if (!VerifyMac(keys, token, now))
        {
            return SessionTokenStatus.BadSignature;
        }

        long expiry = BinaryPrimitives.ReadInt64LittleEndian(token.Slice(ExpiryOffset));
        if (now >= expiry)
        {
            return SessionTokenStatus.Expired;
        }

        ulong lo = BinaryPrimitives.ReadUInt64LittleEndian(token.Slice(RandomOffset));
        ulong hi = BinaryPrimitives.ReadUInt64LittleEndian(token.Slice(RandomOffset + 8));
        SessionTokenStatus status;
        lock (_lock)
        {
            if (consume)
            {
                status = _replayCache.TryAdd(lo, hi, expiry, now) switch
                {
                    ReplayCacheResult.Added => SessionTokenStatus.Valid,
                    ReplayCacheResult.Duplicate => SessionTokenStatus.Replayed,
                    _ => SessionTokenStatus.ReplayCacheFull,
                };
            }
            else
            {
                status = _replayCache.Contains(lo, hi, now) ? SessionTokenStatus.Replayed : SessionTokenStatus.Valid;
            }
        }

        if (status == SessionTokenStatus.Valid)
        {
            sessionId = BinaryPrimitives.ReadUInt64LittleEndian(token.Slice(SessionIdOffset));
            epoch = BinaryPrimitives.ReadUInt32LittleEndian(token.Slice(EpochOffset));
        }

        return status;
    }

    /// <summary>Recomputes the MAC under the current key (and, before its deadline, the previous key) and compares in constant time.</summary>
    private static bool VerifyMac(KeySet keys, ReadOnlySpan<byte> token, long now)
    {
        Span<byte> expected = stackalloc byte[MacLength];
        ReadOnlySpan<byte> signed = token.Slice(0, MacOffset);
        ReadOnlySpan<byte> presented = token.Slice(MacOffset, MacLength);
        HMACSHA256.HashData(keys.Current, signed, expected);
        bool valid = CryptographicOperations.FixedTimeEquals(expected, presented);
        if (!valid && keys.Previous is not null && now < keys.PreviousValidUntilMicros)
        {
            HMACSHA256.HashData(keys.Previous, signed, expected);
            valid = CryptographicOperations.FixedTimeEquals(expected, presented);
        }

        CryptographicOperations.ZeroMemory(expected);
        return valid;
    }

    private static void ThrowIfBadKey(ReadOnlySpan<byte> key, string paramName)
    {
        if (key.Length != KeyLength)
        {
            throw new ArgumentException($"Session token keys are {KeyLength} bytes (got {key.Length}).", paramName);
        }
    }

    /// <summary>Immutable key pair, replaced atomically on rotation so validators never see a torn pair.</summary>
    private sealed class KeySet(byte[] current, byte[]? previous, long previousValidUntilMicros)
    {
        public byte[] Current { get; } = current;

        public byte[]? Previous { get; } = previous;

        public long PreviousValidUntilMicros { get; } = previousValidUntilMicros;
    }
}
