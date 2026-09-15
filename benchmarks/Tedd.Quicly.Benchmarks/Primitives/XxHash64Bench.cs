using BenchmarkDotNet.Attributes;

namespace Tedd.Quicly.Benchmarks.Primitives;

/// <summary>XXH64 of 16 B / 1 KiB / 64 KiB: our implementation against <c>System.IO.Hashing.XxHash64</c>.</summary>
[Config(typeof(InProcessShortRunConfig))]
public class XxHash64Bench
{
    private byte[] _data = Array.Empty<byte>();

    [Params(16, 1024, 65536)]
    public int Size { get; set; }

    [GlobalSetup]
    public void Setup() => _data = BenchmarkData.Random(Size, 5);

    [Benchmark(Baseline = true)]
    public ulong SystemIoHashing() => System.IO.Hashing.XxHash64.HashToUInt64(_data);

    [Benchmark]
    public ulong Current() => Core.Primitives.XxHash64.Hash(_data);
}
