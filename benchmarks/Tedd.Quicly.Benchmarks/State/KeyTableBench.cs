using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Benchmarks.State;

/// <summary>
/// <see cref="KeyTable"/> (seeded fmix64, linear probing, native SoA) and <see cref="DenseKeyTable"/> against
/// <see cref="Dictionary{TKey, TValue}"/> of <c>ulong → int</c>: lookups that hit, lookups that miss, and
/// remove+re-add churn, with N random 64-bit keys (dense table: keys 0 … N-1). <see cref="Ops"/> operations per
/// invoke in random key order, so the 64k case includes cache misses; reported time is per operation.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class KeyTableBench
{
    private const int Ops = 65_536;

    private KeyTable _table = null!;
    private DenseKeyTable _dense = null!;
    private Dictionary<ulong, int> _dictionary = null!;
    private ulong[] _hits = null!;
    private ulong[] _misses = null!;
    private ulong[] _denseHits = null!;

    [Params(1_024, 65_536)]
    public int N { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(7);
        var used = new HashSet<ulong>();
        ulong[] keys = new ulong[N];
        for (int i = 0; i < N; i++)
            keys[i] = Unique(rng, used);
        ulong[] absent = new ulong[N];
        for (int i = 0; i < N; i++)
            absent[i] = Unique(rng, used);

        _table = new KeyTable(N);
        _dense = new DenseKeyTable(N);
        _dictionary = new Dictionary<ulong, int>(N);
        for (int i = 0; i < N; i++)
        {
            _table.TryAdd(keys[i], out int slot);
            _dictionary.Add(keys[i], slot);
            _dense.TryAdd((ulong)i, out _);
        }

        _hits = new ulong[Ops];
        _misses = new ulong[Ops];
        _denseHits = new ulong[Ops];
        for (int i = 0; i < Ops; i++)
        {
            _hits[i] = keys[rng.Next(N)];
            _misses[i] = absent[rng.Next(N)];
            _denseHits[i] = (ulong)rng.Next(N);
        }
    }

    private static ulong Unique(Random rng, HashSet<ulong> used)
    {
        ulong key;
        do
            key = (ulong)rng.NextInt64() ^ ((ulong)rng.Next(2) << 63);
        while (!used.Add(key));
        return key;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _table.Dispose();
        _dense.Dispose();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Ops)]
    public int KeyTable_Hit()
    {
        int sum = 0;
        ulong[] keys = _hits;
        KeyTable table = _table;
        for (int i = 0; i < keys.Length; i++)
        {
            table.TryGetSlot(keys[i], out int slot);
            sum += slot;
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int Dictionary_Hit()
    {
        int sum = 0;
        ulong[] keys = _hits;
        Dictionary<ulong, int> dictionary = _dictionary;
        for (int i = 0; i < keys.Length; i++)
        {
            dictionary.TryGetValue(keys[i], out int slot);
            sum += slot;
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int DenseKeyTable_Hit()
    {
        int sum = 0;
        ulong[] keys = _denseHits;
        DenseKeyTable table = _dense;
        for (int i = 0; i < keys.Length; i++)
        {
            table.TryGetSlot(keys[i], out int slot);
            sum += slot;
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int KeyTable_Miss()
    {
        int sum = 0;
        ulong[] keys = _misses;
        KeyTable table = _table;
        for (int i = 0; i < keys.Length; i++)
        {
            table.TryGetSlot(keys[i], out int slot);
            sum += slot;
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int Dictionary_Miss()
    {
        int sum = 0;
        ulong[] keys = _misses;
        Dictionary<ulong, int> dictionary = _dictionary;
        for (int i = 0; i < keys.Length; i++)
        {
            dictionary.TryGetValue(keys[i], out int slot);
            sum += slot;
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int KeyTable_RemoveAdd()
    {
        int sum = 0;
        ulong[] keys = _hits;
        KeyTable table = _table;
        for (int i = 0; i < keys.Length; i++)
        {
            table.Remove(keys[i], out _);
            table.TryAdd(keys[i], out int slot);
            sum += slot;
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int Dictionary_RemoveAdd()
    {
        int sum = 0;
        ulong[] keys = _hits;
        Dictionary<ulong, int> dictionary = _dictionary;
        for (int i = 0; i < keys.Length; i++)
        {
            dictionary.Remove(keys[i], out int slot);
            dictionary.Add(keys[i], slot);
            sum += slot;
        }

        return sum;
    }
}
