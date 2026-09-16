using System.Buffers.Binary;
using System.Numerics;

namespace Tedd.Quicly.Server;

/// <summary>
/// The session tokens a server has spent, so a token cannot be resumed twice (PROTOCOL.md §4.1). A bounded, time-expiring
/// secondary guard that <b>never</b> fails closed: the session registry is the authority on single use (a resume advances
/// the session's epoch, so a replayed token no longer matches its record), and refusing resumes because a cache is full
/// would hand one authenticated client a denial-of-service lever over every other client.
/// </summary>
/// <remarks>
/// <para><b>Lifetime.</b> An entry only has to cover the window in which a replayed token could still match a live registry
/// record, so entries expire one <see cref="ServerSessionOptions.Grace"/> after they were spent — not after the token's
/// maximum age (<see cref="ServerSessionOptions.TokenLifetime"/>, 24 h by default), which would keep a day of resumes
/// around and make the cache the scarce resource.</para>
/// <para><b>Full.</b> The oldest entry is evicted and the new one stored; the eviction is counted
/// (<see cref="Evictions"/>, <see cref="ServerStatistics.ReplayCacheEvictions"/>) so operators can see the cache being
/// pressured and raise <see cref="ServerSessionOptions.ReplayCacheCapacity"/>. Since all entries get the same lifetime,
/// insertion order is expiry order: the oldest entry is both the first to expire and the first to evict, and expired
/// entries are swept from the front in O(1) each instead of scanning the table.</para>
/// <para><b>Key.</b> The last 16 bytes of a token are part of its HMAC-SHA256 (PROTOCOL.md §4.1), so they are a uniformly
/// random, tamper-evident 128-bit name for a token that only this server could have minted. Only tokens that already
/// verified (<see cref="Core.Control.SessionTokenAuthority.TryInspect"/>) are ever looked up or stored.</para>
/// <para>Game thread only (the admission policy runs inside <see cref="QuiclyServer.PollAll"/>); allocation-free after
/// construction.</para>
/// </remarks>
internal sealed class SessionReplayCache
{
    /// <summary>Largest capacity (the same bound as <see cref="ServerSessionOptions.ReplayCacheCapacity"/>).</summary>
    public const int MaxCapacity = 1 << 22;

    private readonly long _lifetimeMicros;
    private readonly ulong[] _lo;
    private readonly ulong[] _hi;
    private readonly long[] _expires;
    private readonly int[] _next;    // bucket chain, -1 for the end
    private readonly int[] _buckets; // entry index + 1, 0 for an empty bucket
    private readonly int _mask;
    private int _head;               // oldest entry
    private int _count;

    /// <summary>Creates a cache of <paramref name="capacity"/> entries, each kept for <paramref name="lifetimeMicros"/>.</summary>
    /// <param name="capacity">Spent tokens remembered at once (1 … 4 194 304).</param>
    /// <param name="lifetimeMicros">How long an entry is kept (the session grace period); at least 0.</param>
    public SessionReplayCache(int capacity, long lifetimeMicros)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(capacity, MaxCapacity);
        ArgumentOutOfRangeException.ThrowIfNegative(lifetimeMicros);
        _lifetimeMicros = lifetimeMicros;
        _lo = new ulong[capacity];
        _hi = new ulong[capacity];
        _expires = new long[capacity];
        _next = new int[capacity];
        int buckets = (int)BitOperations.RoundUpToPowerOf2((uint)capacity);
        _buckets = new int[buckets];
        _mask = buckets - 1;
        Capacity = capacity;
    }

    /// <summary>Spent tokens the cache can remember at once.</summary>
    public int Capacity { get; }

    /// <summary>Entries currently held (unexpired as of the last call that swept).</summary>
    public int Count => _count;

    /// <summary>Entries dropped because the cache was full when another token was spent (never a refused resume).</summary>
    public long Evictions { get; private set; }

    /// <summary>Whether <paramref name="token"/> was already spent and is still remembered.</summary>
    /// <param name="token">A token that verified (its bytes are not trusted for anything else).</param>
    /// <param name="nowMicros">Clock micros.</param>
    /// <returns><see langword="true"/> when the token was spent within the entry lifetime.</returns>
    public bool Contains(ReadOnlySpan<byte> token, long nowMicros)
    {
        KeyOf(token, out ulong lo, out ulong hi);
        return Find(lo, hi, nowMicros) >= 0;
    }

    /// <summary>
    /// Spends <paramref name="token"/>: remembers it for the entry lifetime. Evicts the oldest entry when the cache is
    /// full; never refuses.
    /// </summary>
    /// <param name="token">A token that verified.</param>
    /// <param name="nowMicros">Clock micros.</param>
    /// <returns><see langword="false"/> when the token was already spent (the resume is a replay and must be refused).</returns>
    public bool TryConsume(ReadOnlySpan<byte> token, long nowMicros)
    {
        KeyOf(token, out ulong lo, out ulong hi);
        if (Find(lo, hi, nowMicros) >= 0)
        {
            return false;
        }

        Sweep(nowMicros); // entries share one lifetime, so the expired ones are exactly the front of the queue
        if (_count == Capacity)
        {
            RemoveOldest(evicted: true);
        }

        int entry = _head + _count < Capacity ? _head + _count : _head + _count - Capacity;
        _lo[entry] = lo;
        _hi[entry] = hi;
        _expires[entry] = nowMicros + _lifetimeMicros;
        ref int bucket = ref _buckets[(int)lo & _mask];
        _next[entry] = bucket - 1;
        bucket = entry + 1;
        _count++;
        return true;
    }

    /// <summary>Drops the entries whose lifetime ended (front of the queue first, since they all share one lifetime).</summary>
    /// <param name="nowMicros">Clock micros.</param>
    public void Sweep(long nowMicros)
    {
        while (_count != 0 && _expires[_head] <= nowMicros)
        {
            RemoveOldest(evicted: false);
        }
    }

    /// <summary>The 128-bit name of a token: its last 16 bytes, which are part of its HMAC (PROTOCOL.md §4.1).</summary>
    private static void KeyOf(ReadOnlySpan<byte> token, out ulong lo, out ulong hi)
    {
        ReadOnlySpan<byte> tail = token.Slice(token.Length - 16);
        lo = BinaryPrimitives.ReadUInt64LittleEndian(tail);
        hi = BinaryPrimitives.ReadUInt64LittleEndian(tail.Slice(8));
    }

    /// <summary>The entry holding (<paramref name="lo"/>, <paramref name="hi"/>) and not expired, or -1.</summary>
    private int Find(ulong lo, ulong hi, long nowMicros)
    {
        for (int entry = _buckets[(int)lo & _mask] - 1; entry >= 0; entry = _next[entry])
        {
            if (_lo[entry] == lo && _hi[entry] == hi)
            {
                return _expires[entry] > nowMicros ? entry : -1;
            }
        }

        return -1;
    }

    private void RemoveOldest(bool evicted)
    {
        int entry = _head;
        ref int bucket = ref _buckets[(int)_lo[entry] & _mask];
        if (bucket - 1 == entry)
        {
            bucket = _next[entry] + 1;
        }
        else
        {
            for (int previous = bucket - 1; previous >= 0; previous = _next[previous])
            {
                if (_next[previous] == entry)
                {
                    _next[previous] = _next[entry];
                    break;
                }
            }
        }

        _next[entry] = -1;
        _expires[entry] = 0;
        _head = entry + 1 < Capacity ? entry + 1 : 0;
        _count--;
        if (evicted)
        {
            Evictions++;
        }
    }
}
