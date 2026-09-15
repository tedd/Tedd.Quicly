using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.State;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Benchmarks.State;

/// <summary>Shape of the key set used by <see cref="KeyHashingBench"/>.</summary>
public enum KeyPattern
{
    /// <summary>0, 1, 2, …: the identity hash's best case (every key in its own bucket, sequential memory).</summary>
    Sequential,
    /// <summary>i × 1024: entity ids with a stride, aligned handles. The identity hash's worst case.</summary>
    Strided,
    /// <summary>Uniformly random 64-bit keys.</summary>
    Random,
}

/// <summary>
/// ADR 0007 loop for the key-table hash: V0 = identity (archived <see cref="KeyTableV0"/>, bucket = low key bits)
/// against V1 = seeded fmix64 (shipping <see cref="KeyTable"/>). Same probing and layout, only the hash differs.
/// <see cref="Ops"/> lookups per invoke in random order over the key set; reported time is per lookup.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class KeyHashingBench
{
    private const int Ops = 65_536;

    private KeyTableV0 _v0 = null!;
    private KeyTable _v1 = null!;
    private ulong[] _hits = null!;
    private ulong[] _misses = null!;

    [Params(1_024, 65_536)]
    public int N { get; set; }

    [Params(KeyPattern.Sequential, KeyPattern.Strided, KeyPattern.Random)]
    public KeyPattern Pattern { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(42);
        var used = new HashSet<ulong>();
        ulong[] keys = new ulong[N];
        ulong[] absent = new ulong[N];
        for (int i = 0; i < N; i++)
        {
            keys[i] = Make(i, rng, used);
            used.Add(keys[i]);
        }

        for (int i = 0; i < N; i++)
        {
            absent[i] = Make(N + i, rng, used);
            used.Add(absent[i]);
        }

        _v0 = new KeyTableV0(N);
        _v1 = new KeyTable(N);
        foreach (ulong key in keys)
        {
            _v0.TryAdd(key, out _);
            _v1.TryAdd(key, out _);
        }

        _hits = new ulong[Ops];
        _misses = new ulong[Ops];
        for (int i = 0; i < Ops; i++)
        {
            _hits[i] = keys[rng.Next(N)];
            _misses[i] = absent[rng.Next(N)];
        }
    }

    private ulong Make(int i, Random rng, HashSet<ulong> used)
    {
        switch (Pattern)
        {
            case KeyPattern.Sequential:
                return (ulong)i;
            case KeyPattern.Strided:
                return (ulong)i << 10;
            default:
                ulong key;
                do
                    key = (ulong)rng.NextInt64() ^ ((ulong)rng.Next(2) << 63);
                while (used.Contains(key));
                return key;
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _v0.Dispose();
        _v1.Dispose();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Ops)]
    public int V0_Identity_Hit()
    {
        int sum = 0;
        ulong[] keys = _hits;
        KeyTableV0 table = _v0;
        for (int i = 0; i < keys.Length; i++)
        {
            table.TryGetSlot(keys[i], out int slot);
            sum += slot;
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int V1_Fmix64_Hit()
    {
        int sum = 0;
        ulong[] keys = _hits;
        KeyTable table = _v1;
        for (int i = 0; i < keys.Length; i++)
        {
            table.TryGetSlot(keys[i], out int slot);
            sum += slot;
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int V0_Identity_Miss()
    {
        int sum = 0;
        ulong[] keys = _misses;
        KeyTableV0 table = _v0;
        for (int i = 0; i < keys.Length; i++)
        {
            table.TryGetSlot(keys[i], out int slot);
            sum += slot;
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int V1_Fmix64_Miss()
    {
        int sum = 0;
        ulong[] keys = _misses;
        KeyTable table = _v1;
        for (int i = 0; i < keys.Length; i++)
        {
            table.TryGetSlot(keys[i], out int slot);
            sum += slot;
        }

        return sum;
    }
}
