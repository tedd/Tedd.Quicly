using System.Numerics;

namespace Tedd.Quicly.Core.Control;

/// <summary>Outcome of <see cref="ReplayCache.TryAdd"/>.</summary>
internal enum ReplayCacheResult
{
    Added,
    Duplicate,
    Full,
}

/// <summary>
/// Bounded set of used session-token nonces (the 16 random bytes), each remembered until its token expires.
/// Open addressing with linear probing over power-of-two SoA arrays sized to twice the capacity (load ≤ 0.5),
/// backward-shift deletion (no tombstones). Expired entries met while probing are deleted on the spot; when the
/// table holds <c>capacity</c> entries an insert first sweeps out every expired entry, and if none has expired the
/// insert fails (<see cref="ReplayCacheResult.Full"/>: the caller refuses the token). The nonces are server-chosen
/// random values that an attacker cannot pick, so the low bits index the table directly. Not thread-safe; the
/// owner serialises access. Allocation-free after construction.
/// </summary>
internal sealed class ReplayCache
{
    public const int MaxCapacity = 1 << 22;

    private const long Empty = long.MinValue;

    private readonly ulong[] _lo;
    private readonly ulong[] _hi;
    private readonly long[] _expiry;
    private readonly int _mask;
    private int _count;

    // Lower bound on the smallest expiry in the table: no sweep can free anything before this time.
    private long _earliestExpiry = long.MaxValue;

    public ReplayCache(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(capacity, MaxCapacity);
        int slots = (int)BitOperations.RoundUpToPowerOf2((uint)capacity * 2);
        _lo = new ulong[slots];
        _hi = new ulong[slots];
        _expiry = new long[slots];
        Array.Fill(_expiry, Empty);
        _mask = slots - 1;
        Capacity = capacity;
    }

    public int Capacity { get; }

    /// <summary>Entries currently stored (some may have expired but not been swept yet).</summary>
    public int Count => _count;

    public ReplayCacheResult TryAdd(ulong lo, ulong hi, long expiry, long now)
    {
        int i = Probe(lo, hi, now, out bool found);
        if (found)
        {
            return ReplayCacheResult.Duplicate;
        }

        if (_count >= Capacity)
        {
            if (now < _earliestExpiry || Sweep(now) == 0)
            {
                return ReplayCacheResult.Full;
            }

            i = Probe(lo, hi, now, out _);
        }

        _lo[i] = lo;
        _hi[i] = hi;
        _expiry[i] = expiry;
        _count++;
        if (expiry < _earliestExpiry)
        {
            _earliestExpiry = expiry;
        }

        return ReplayCacheResult.Added;
    }

    public bool Contains(ulong lo, ulong hi, long now)
    {
        Probe(lo, hi, now, out bool found);
        return found;
    }

    /// <summary>
    /// Walks the probe sequence of (lo, hi), deleting expired entries met on the way. Returns the matching slot
    /// (<paramref name="found"/>) or the empty slot that ends the sequence.
    /// </summary>
    private int Probe(ulong lo, ulong hi, long now, out bool found)
    {
        int i = (int)lo & _mask;
        while (true)
        {
            long expiry = _expiry[i];
            if (expiry == Empty)
            {
                found = false;
                return i;
            }

            if (expiry <= now)
            {
                RemoveAt(i); // re-examine slot i: backward shift may have moved a later entry into it
                continue;
            }

            if (_lo[i] == lo && _hi[i] == hi)
            {
                found = true;
                return i;
            }

            i = (i + 1) & _mask;
        }
    }

    /// <summary>Deletes every expired entry; returns how many were deleted.</summary>
    private int Sweep(long now)
    {
        int removed = 0;
        long earliest = long.MaxValue;
        for (int i = 0; i <= _mask;)
        {
            long expiry = _expiry[i];
            if (expiry != Empty && expiry <= now)
            {
                RemoveAt(i);
                removed++;
                continue;
            }

            if (expiry != Empty && expiry < earliest)
            {
                earliest = expiry;
            }

            i++;
        }

        _earliestExpiry = earliest;
        return removed;
    }

    /// <summary>Backward-shift deletion: pulls later members of the probe run into the hole so lookups never need tombstones.</summary>
    private void RemoveAt(int hole)
    {
        int j = hole;
        while (true)
        {
            j = (j + 1) & _mask;
            long expiry = _expiry[j];
            if (expiry == Empty)
            {
                break;
            }

            int home = (int)_lo[j] & _mask;
            if (((j - home) & _mask) >= ((j - hole) & _mask))
            {
                _lo[hole] = _lo[j];
                _hi[hole] = _hi[j];
                _expiry[hole] = expiry;
                hole = j;
            }
        }

        _expiry[hole] = Empty;
        _count--;
    }
}
