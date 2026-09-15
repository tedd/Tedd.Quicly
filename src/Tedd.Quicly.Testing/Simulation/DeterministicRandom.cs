namespace Tedd.Quicly.Testing.Simulation;

/// <summary>
/// xoshiro256** seeded through SplitMix64. Unlike <see cref="Random"/> its sequence is fixed by this source file,
/// so a seed reproduces the same simulation on every runtime.
/// </summary>
internal struct DeterministicRandom
{
    private ulong _s0, _s1, _s2, _s3;

    public DeterministicRandom(int seed)
    {
        ulong x = unchecked((ulong)(long)seed);
        _s0 = SplitMix64(ref x);
        _s1 = SplitMix64(ref x);
        _s2 = SplitMix64(ref x);
        _s3 = SplitMix64(ref x);
    }

    private static ulong SplitMix64(ref ulong x)
    {
        ulong z = x += 0x9E3779B97F4A7C15UL;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    public ulong NextUInt64()
    {
        ulong result = ulong.RotateLeft(_s1 * 5, 7) * 9;
        ulong t = _s1 << 17;
        _s2 ^= _s0;
        _s3 ^= _s1;
        _s1 ^= _s2;
        _s0 ^= _s3;
        _s2 ^= t;
        _s3 = ulong.RotateLeft(_s3, 45);
        return result;
    }

    /// <summary>Uniform value in [0, <paramref name="maxExclusive"/>); 0 when <paramref name="maxExclusive"/> is not positive.</summary>
    public long NextInt64(long maxExclusive)
    {
        if (maxExclusive <= 1)
            return 0;
        return (long)(ulong)(((UInt128)NextUInt64() * (ulong)maxExclusive) >> 64);
    }

    /// <summary>True with probability <paramref name="percent"/> / 100. Consumes one value only when 0 &lt; percent &lt; 100.</summary>
    public bool Chance(double percent)
    {
        if (percent <= 0)
            return false;
        if (percent >= 100)
            return true;
        return (NextUInt64() >> 11) * (1.0 / (1UL << 53)) * 100.0 < percent;
    }
}
