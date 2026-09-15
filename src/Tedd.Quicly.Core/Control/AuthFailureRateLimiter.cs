using System.Buffers.Binary;
using System.Net;
using System.Numerics;
using System.Security.Cryptography;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Time;

namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Per-remote-address rate limiter for failed authentication / resume attempts (PROTOCOL.md §4.1, ADR 0009).
/// </summary>
/// <remarks>
/// <para>
/// Each address has a token bucket of <c>burst</c> failures refilled at one failure per <c>refillInterval</c>
/// (implemented as GCRA: one "theoretical arrival time" per address). The admission path asks
/// <see cref="IsAllowed(IPAddress)"/> before doing any work and calls <see cref="RecordFailure(IPAddress)"/> when an
/// auth token or session token is rejected. An address is refused once it has <c>burst</c> failures outstanding and is
/// allowed again one interval after its last recorded failure.
/// </para>
/// <para>
/// Keys: IPv4 addresses (and IPv4-mapped IPv6) by full address; IPv6 by prefix (default /64, since one host commonly
/// owns a whole /64).
/// </para>
/// <para>
/// Bounded memory: a table of <c>capacity</c> entries organised as 8-way buckets, indexed by a keyed (per-instance
/// random seed) xxHash64 so a remote party cannot aim addresses at one bucket. An entry whose bucket has fully
/// refilled is free for reuse. When an address has no entry and every entry of its bucket is still active, its
/// failures are charged to a single shared <em>overflow</em> bucket, and untracked addresses in full buckets are
/// admitted only while that overflow bucket allows it: under a many-address attack the limiter degrades to a global
/// limit for the addresses it cannot track, rather than forgetting the ones it does.
/// </para>
/// <para>Thread-safe (one short lock per call); allocation-free after construction.</para>
/// </remarks>
public sealed class AuthFailureRateLimiter
{
    /// <summary>Default number of tracked addresses.</summary>
    public const int DefaultCapacity = 4096;

    /// <summary>Default number of failures allowed in a burst.</summary>
    public const int DefaultBurst = 10;

    /// <summary>Default refill interval: one failure credit every 6 s (10 per minute sustained).</summary>
    public const long DefaultRefillIntervalMicros = 6_000_000;

    /// <summary>Default IPv6 aggregation prefix length.</summary>
    public const int DefaultIPv6PrefixLength = 64;

    private const int Ways = 8;
    private const ulong Unused = ulong.MaxValue; // ff..ff is multicast: never a remote source address
    private const ulong Ipv4MappedPrefix = 0x0000_FFFF_0000_0000UL;

    private readonly IClock _clock;
    private readonly Lock _lock = new();
    private readonly ulong[] _hi;
    private readonly ulong[] _lo;
    private readonly long[] _tat;
    private readonly int _bucketMask;
    private readonly long _interval;
    private readonly long _limit;     // burst × interval: how far ahead of now the arrival time may be pushed
    private readonly long _allowance; // (burst − 1) × interval: an attempt is allowed while tat ≤ now + allowance
    private readonly ulong _hiMask;
    private readonly ulong _loMask;
    private readonly ulong _seed;
    private long _overflowTat = long.MinValue;
    private long _overflowFailures;

    /// <summary>Creates a limiter.</summary>
    /// <param name="clock">Time source.</param>
    /// <param name="capacity">Tracked addresses (8 … 4 194 304; rounded up to a power of two).</param>
    /// <param name="burst">Failures allowed back to back (1 … 1 000 000).</param>
    /// <param name="refillIntervalMicros">Time to regain one failure credit (1 µs … 1 day).</param>
    /// <param name="ipv6PrefixLength">IPv6 addresses sharing this many leading bits share a bucket (1 … 128).</param>
    /// <exception cref="ArgumentOutOfRangeException">An argument is out of range.</exception>
    public AuthFailureRateLimiter(IClock clock, int capacity = DefaultCapacity, int burst = DefaultBurst,
        long refillIntervalMicros = DefaultRefillIntervalMicros, int ipv6PrefixLength = DefaultIPv6PrefixLength)
    {
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, Ways);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(capacity, 1 << 22);
        ArgumentOutOfRangeException.ThrowIfLessThan(burst, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(burst, 1_000_000);
        ArgumentOutOfRangeException.ThrowIfLessThan(refillIntervalMicros, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(refillIntervalMicros, 86_400_000_000L);
        ArgumentOutOfRangeException.ThrowIfLessThan(ipv6PrefixLength, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(ipv6PrefixLength, 128);

        _clock = clock;
        int slots = (int)BitOperations.RoundUpToPowerOf2((uint)capacity);
        _hi = new ulong[slots];
        _lo = new ulong[slots];
        _tat = new long[slots];
        Array.Fill(_hi, Unused);
        Array.Fill(_lo, Unused);
        Array.Fill(_tat, long.MinValue);
        _bucketMask = (slots / Ways) - 1;
        _interval = refillIntervalMicros;
        _limit = burst * refillIntervalMicros;
        _allowance = _limit - refillIntervalMicros;
        _hiMask = ipv6PrefixLength >= 64 ? ulong.MaxValue : ulong.MaxValue << (64 - ipv6PrefixLength);
        _loMask = ipv6PrefixLength <= 64 ? 0 : ipv6PrefixLength == 128 ? ulong.MaxValue : ulong.MaxValue << (128 - ipv6PrefixLength);
        Span<byte> seed = stackalloc byte[8];
        RandomNumberGenerator.Fill(seed);
        _seed = BinaryPrimitives.ReadUInt64LittleEndian(seed);
        Capacity = slots;
    }

    /// <summary>Number of table entries (the requested capacity rounded up to a power of two).</summary>
    public int Capacity { get; }

    /// <summary>Failures charged to the shared overflow bucket because the address's bucket was full (diagnostics).</summary>
    public long OverflowFailures
    {
        get
        {
            lock (_lock)
            {
                return _overflowFailures;
            }
        }
    }

    /// <summary>Addresses that currently have outstanding failures (diagnostics; scans the table).</summary>
    public int ActiveCount
    {
        get
        {
            long now = _clock.NowMicros;
            int active = 0;
            lock (_lock)
            {
                foreach (long tat in _tat)
                {
                    if (tat > now)
                    {
                        active++;
                    }
                }
            }

            return active;
        }
    }

    /// <summary>Returns whether an attempt from <paramref name="address"/> may proceed.</summary>
    /// <param name="address">The remote address.</param>
    /// <exception cref="ArgumentNullException"><paramref name="address"/> is null.</exception>
    public bool IsAllowed(IPAddress address)
    {
        Span<byte> bytes = stackalloc byte[16];
        return IsAllowed(bytes.Slice(0, WriteBytes(address, bytes)));
    }

    /// <summary>Returns whether an attempt from the address in <paramref name="address"/> may proceed.</summary>
    /// <param name="address">Network-order address bytes: 4 (IPv4) or 16 (IPv6).</param>
    /// <exception cref="ArgumentException"><paramref name="address"/> is neither 4 nor 16 bytes.</exception>
    public bool IsAllowed(ReadOnlySpan<byte> address)
    {
        Normalize(address, out ulong hi, out ulong lo);
        int first = BucketOf(hi, lo);
        long now = _clock.NowMicros;
        lock (_lock)
        {
            bool hasFreeSlot = false;
            for (int i = first; i < first + Ways; i++)
            {
                if (_hi[i] == hi && _lo[i] == lo)
                {
                    return _tat[i] <= now + _allowance;
                }

                hasFreeSlot |= _tat[i] <= now;
            }

            return hasFreeSlot || _overflowTat <= now + _allowance;
        }
    }

    /// <summary>Charges one failed attempt to <paramref name="address"/>.</summary>
    /// <param name="address">The remote address.</param>
    /// <exception cref="ArgumentNullException"><paramref name="address"/> is null.</exception>
    public void RecordFailure(IPAddress address)
    {
        Span<byte> bytes = stackalloc byte[16];
        RecordFailure(bytes.Slice(0, WriteBytes(address, bytes)));
    }

    /// <summary>Charges one failed attempt to the address in <paramref name="address"/>.</summary>
    /// <param name="address">Network-order address bytes: 4 (IPv4) or 16 (IPv6).</param>
    /// <exception cref="ArgumentException"><paramref name="address"/> is neither 4 nor 16 bytes.</exception>
    public void RecordFailure(ReadOnlySpan<byte> address)
    {
        Normalize(address, out ulong hi, out ulong lo);
        int first = BucketOf(hi, lo);
        long now = _clock.NowMicros;
        lock (_lock)
        {
            int free = -1;
            for (int i = first; i < first + Ways; i++)
            {
                if (_hi[i] == hi && _lo[i] == lo)
                {
                    _tat[i] = Charge(_tat[i], now);
                    return;
                }

                if (free < 0 && _tat[i] <= now)
                {
                    free = i;
                }
            }

            if (free >= 0)
            {
                _hi[free] = hi;
                _lo[free] = lo;
                _tat[free] = Charge(long.MinValue, now);
                return;
            }

            _overflowTat = Charge(_overflowTat, now);
            _overflowFailures++;
        }
    }

    // GCRA: each failure pushes the theoretical arrival time one interval further, never more than `limit` ahead.
    private long Charge(long tat, long now) => Math.Min(Math.Max(tat, now) + _interval, now + _limit);

    private static int WriteBytes(IPAddress address, Span<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(address);
        address.TryWriteBytes(destination, out int written);
        return written;
    }

    private void Normalize(ReadOnlySpan<byte> address, out ulong hi, out ulong lo)
    {
        if (address.Length == 4)
        {
            hi = 0;
            lo = Ipv4MappedPrefix | BinaryPrimitives.ReadUInt32BigEndian(address);
            return;
        }

        if (address.Length != 16)
        {
            throw new ArgumentException("An IP address is 4 (IPv4) or 16 (IPv6) bytes.", nameof(address));
        }

        hi = BinaryPrimitives.ReadUInt64BigEndian(address);
        lo = BinaryPrimitives.ReadUInt64BigEndian(address.Slice(8));
        if (hi == 0 && (lo >> 32) == 0xFFFF)
        {
            return; // IPv4-mapped: identical key to the plain IPv4 form
        }

        hi &= _hiMask;
        lo &= _loMask;
    }

    private int BucketOf(ulong hi, ulong lo)
    {
        Span<byte> key = stackalloc byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(key, hi);
        BinaryPrimitives.WriteUInt64LittleEndian(key.Slice(8), lo);
        return ((int)XxHash64.Hash(key, _seed) & _bucketMask) * Ways;
    }
}
