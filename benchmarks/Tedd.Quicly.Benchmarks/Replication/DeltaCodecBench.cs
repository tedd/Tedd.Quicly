using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Tedd.Quicly.Archive.Replication;
using Tedd.Quicly.Benchmarks.Primitives;
using Tedd.Quicly.Replication;

namespace Tedd.Quicly.Benchmarks.Replication;

/// <summary>
/// Snapshot delta encode / decode: the archived byte-at-a-time V0 against the shipped SIMD codec (Vector256 on this
/// hardware) and its forced Vector128 path. Baseline = game-like snapshot; current = the baseline with
/// <see cref="ChangedPercent"/> % of its bytes changed at uniformly random positions (the worst case for run finding:
/// short scattered literals).
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class DeltaCodecBench
{
    private byte[] _baseline = [];
    private byte[] _current = [];
    private byte[] _delta = [];
    private byte[] _encoded = [];
    private byte[] _output = [];

    [Params(1024, 16384)]
    public int Size { get; set; }

    [Params(5, 50)]
    public int ChangedPercent { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _baseline = BenchmarkData.GameLike(Size, 21);
        _current = (byte[])_baseline.Clone();
        Random random = new(Size + ChangedPercent);
        int changes = Size * ChangedPercent / 100;
        for (int i = 0; i < changes; i++)
        {
            int p;
            do
            {
                p = random.Next(Size);
            }
            while (_current[p] != _baseline[p]);

            _current[p] ^= (byte)random.Next(1, 256);
        }

        _delta = new byte[DeltaCodec.GetMaxEncodedLength(Size)];
        _output = new byte[Size];
        int n = DeltaCodec.Encode(_baseline, _current, _delta);
        _encoded = _delta.AsSpan(0, n).ToArray();
        int v0 = DeltaCodecV0.Encode(_baseline, _current, new byte[_delta.Length]);
        Console.WriteLine($"// Size={Size} Changed={ChangedPercent}%: delta {n} bytes (V0 {v0})");
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Encode")]
    public int Encode_V0() => DeltaCodecV0.Encode(_baseline, _current, _delta);

    [Benchmark]
    [BenchmarkCategory("Encode")]
    public int Encode_V1() => DeltaCodecV1.Encode(_baseline, _current, _delta);

    [Benchmark]
    [BenchmarkCategory("Encode")]
    public int Encode_Vector128() => DeltaCodec.Encode<Simd128>(_baseline, _current, _delta);

    [Benchmark]
    [BenchmarkCategory("Encode")]
    public int Encode_Current() => DeltaCodec.Encode(_baseline, _current, _delta);

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Decode")]
    public int Decode_V0() => DeltaCodecV0.Decode(_baseline, _encoded, _output);

    [Benchmark]
    [BenchmarkCategory("Decode")]
    public int Decode_V1() => DeltaCodecV1.Decode(_baseline, _encoded, _output);

    [Benchmark]
    [BenchmarkCategory("Decode")]
    public int Decode_Vector128() => DeltaCodec.Decode<Simd128>(_baseline, _encoded, _output);

    [Benchmark]
    [BenchmarkCategory("Decode")]
    public int Decode_Current() => DeltaCodec.Decode(_baseline, _encoded, _output);
}
