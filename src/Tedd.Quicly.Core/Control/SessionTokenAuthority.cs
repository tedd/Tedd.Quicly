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
/// Admission: check a presented token with <see cref="TryInspect"/> (does not consume it) and call
/// <see cref="TryValidate"/> only when the resume commits, i.e. after every other admission check (auth token, server
/// capacity, channel table, datagram support, session still in the registry) has passed. A resume refused after the
/// inspection therefore leaves the client's token usable, as PROTOCOL.md §4.1 requires (a token is spent only by a
/// resume that <em>succeeded</em>). <see cref="TryValidate"/> re-checks everything, so of two concurrent resumes with the
/// same token exactly one commits and the other sees <see cref="SessionTokenStatus.Replayed"/>.
/// </para>
/// <para>
/// Single use: <see cref="TryValidate"/> records the token's random part in a bounded replay cache (entries expire
/// with their token); presenting it again yields <see cref="SessionTokenStatus.Replayed"/>. When the cache holds
/// <c>replayCacheCapacity</c> unexpired entries a new token is refused with
/// <see cref="SessionTokenStatus.ReplayCacheFull"/> (fail closed). Invalidating older tokens once a newer one was issued
/// is the session registry's job: it compares the returned <c>epoch</c> with the session's current epoch and bumps the
/// epoch on every successful resume, which on its own already makes a token single-use; the replay cache is defence in
/// depth that keeps this class self-contained. Its exposure: every consumed token occupies a slot until it expires, so
/// one client that resumes faster than <c>replayCacheCapacity / grace</c> per second (about 546/s with the defaults and
/// a 30 s grace) can fill it and make every other client's resume fail closed until entries expire. The session layer
/// should therefore cap resumes per session (for example one per heartbeat interval) and charge refused resumes to
/// <see cref="AuthFailureRateLimiter"/>. While the cache is full and entries are expiring, an insert sweeps the whole
/// table (O(capacity), under the lock).
/// </para>
/// <para>
/// Rotation: <see cref="RotateKey"/> (or the two-key constructor) keeps accepting tokens signed with the previous key
/// until a deadline; new tokens are always signed with the current key. Only one previous key is kept: rotating again
/// before the previous key's deadline drops it immediately, so tokens it signed stop verifying early. MACs are compared
/// with <see cref="CryptographicOperations.FixedTimeEquals(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>.
/// </para>
/// <para>
/// Key lifetime: the keys in use form an immutable key set that owns private copies of its key bytes. Minting and
/// validation lease the current set (a reference count, re-checked after it is taken) for the duration of the HMAC.
/// <see cref="RotateKey"/> and <see cref="Dispose"/> publish the replacement first and then drop the authority's own
/// reference; a retired set's keys are wiped by whoever releases its last reference. Key bytes are therefore never
/// modified while a computation can still read them (an HMAC under a half-wiped or all-zero key would let anyone forge
/// a token). The clock is read before a lease is taken, so a clock that calls back into the authority never runs while
/// a lease is held.
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
    private readonly Lock _lock = new(); // guards the replay cache and serialises RotateKey / Dispose
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
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _keys) is null, this);
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
        KeySet keys = AcquireKeys();
        try
        {
            HMACSHA256.HashData(keys.Current, token.Slice(0, MacOffset), token.Slice(MacOffset, MacLength));
        }
        finally
        {
            keys.Release();
        }

        return TokenLength;
    }

    /// <summary>
    /// Validates <paramref name="token"/> and consumes it (single use): on <see cref="SessionTokenStatus.Valid"/> the same
    /// token is <see cref="SessionTokenStatus.Replayed"/> from then on.
    /// </summary>
    /// <remarks>
    /// Call this only when the resume commits. Use <see cref="TryInspect"/> for the admission checks that come first,
    /// so that a resume refused for another reason (server full, table mismatch, datagrams required, session gone)
    /// does not burn the client's token. This call repeats every check, so a token consumed concurrently since the
    /// inspection yields <see cref="SessionTokenStatus.Replayed"/>.
    /// </remarks>
    /// <param name="token">The presented token (untrusted).</param>
    /// <param name="sessionId">The session named by the token when valid, otherwise 0.</param>
    /// <param name="epoch">The epoch the token was issued in when valid, otherwise 0.</param>
    /// <returns>The outcome; report anything but <see cref="SessionTokenStatus.Valid"/> to the peer as <see cref="HelloStatus.Rejected"/>.</returns>
    /// <exception cref="ObjectDisposedException">The authority was disposed.</exception>
    public SessionTokenStatus TryValidate(ReadOnlySpan<byte> token, out ulong sessionId, out uint epoch) =>
        Validate(token, consume: true, out sessionId, out epoch);

    /// <summary>Validates <paramref name="token"/> like <see cref="TryValidate"/> but does not consume it.</summary>
    /// <remarks>This is the admission-time check; <see cref="TryValidate"/> follows only when the resume commits.</remarks>
    /// <param name="token">The presented token (untrusted).</param>
    /// <param name="sessionId">The session named by the token when valid, otherwise 0.</param>
    /// <param name="epoch">The epoch the token was issued in when valid, otherwise 0.</param>
    /// <returns>The outcome (<see cref="SessionTokenStatus.Replayed"/> when it was already consumed).</returns>
    /// <exception cref="ObjectDisposedException">The authority was disposed.</exception>
    public SessionTokenStatus TryInspect(ReadOnlySpan<byte> token, out ulong sessionId, out uint epoch) =>
        Validate(token, consume: false, out sessionId, out epoch);

    /// <summary>
    /// Makes <paramref name="newKey"/> the signing key; the current key becomes the previous key and keeps verifying
    /// until <paramref name="previousKeyValidUntilMicros"/>. The key it replaces as "previous" stops verifying at once
    /// and is wiped as soon as no in-flight mint or validation still uses it.
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

            // The new set gets its own copy of the outgoing key: every set owns (and eventually wipes) its arrays, so
            // retiring `old` never wipes bytes a newer set still verifies with. `old.Current` is not modified while
            // `old` is published, and only code holding this lock retires it.
            KeySet next = new(copy, old.Current.AsSpan().ToArray(), previousKeyValidUntilMicros);
            Interlocked.Exchange(ref _keys, next); // full fence: pairs with the re-check in AcquireKeys
            old.Release();
        }
    }

    /// <summary>
    /// Wipes the keys (at once, or when the last in-flight mint or validation finishes). Further calls to other members
    /// throw <see cref="ObjectDisposedException"/>.
    /// </summary>
    public void Dispose()
    {
        lock (_lock)
        {
            Interlocked.Exchange(ref _keys, null)?.Release();
        }
    }

    /// <summary>
    /// Leases the current key set; the caller must <see cref="KeySet.Release"/> it. The re-check after the reference is
    /// counted guarantees the set was still published at that point, so its retirement (which happens after it is
    /// unpublished) sees the lease and leaves the wipe to the lease's release.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The authority was disposed.</exception>
    internal KeySet AcquireKeys()
    {
        while (true)
        {
            KeySet keys = Volatile.Read(ref _keys) ?? throw new ObjectDisposedException(nameof(SessionTokenAuthority));
            keys.AddReference(); // full fence
            if (ReferenceEquals(Volatile.Read(ref _keys), keys))
            {
                return keys;
            }

            keys.Release(); // retired between the two reads: possibly already wiped, so never used
        }
    }

    private SessionTokenStatus Validate(ReadOnlySpan<byte> token, bool consume, out ulong sessionId, out uint epoch)
    {
        sessionId = 0;
        epoch = 0;
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _keys) is null, this);
        if (token.Length != TokenLength || token[0] != FormatVersion)
        {
            return SessionTokenStatus.Malformed;
        }

        long now = _clock.NowMicros; // read before the lease: the clock is caller code
        bool macValid;
        KeySet keys = AcquireKeys();
        try
        {
            macValid = VerifyMac(keys, token, now);
        }
        finally
        {
            keys.Release();
        }

        if (!macValid)
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

    /// <summary>
    /// Immutable key pair, replaced atomically on rotation so validators never see a torn pair. Owns its arrays
    /// exclusively and wipes them when the last reference (the authority's own, or an in-flight lease) is released.
    /// </summary>
    internal sealed class KeySet(byte[] current, byte[]? previous, long previousValidUntilMicros)
    {
        private int _references = 1; // the authority's reference, released when the set is retired
        private int _wiped;

        public byte[] Current { get; } = current;

        public byte[]? Previous { get; } = previous;

        public long PreviousValidUntilMicros { get; } = previousValidUntilMicros;

        /// <summary>Whether the keys have been wiped (diagnostics and tests).</summary>
        public bool IsWiped => Volatile.Read(ref _wiped) != 0;

        public void AddReference() => Interlocked.Increment(ref _references);

        public void Release()
        {
            if (Interlocked.Decrement(ref _references) != 0)
            {
                return;
            }

            // Idempotent: a lease taken on an already-retired set (and dropped after the failed re-check) can bring the
            // count back to zero a second time.
            CryptographicOperations.ZeroMemory(Current);
            if (Previous is not null)
            {
                CryptographicOperations.ZeroMemory(Previous);
            }

            Volatile.Write(ref _wiped, 1);
        }
    }
}
