using BenchmarkDotNet.Attributes;
using K4os.Compression.LZ4;
using Tedd.Quicly.Archive.Primitives;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Benchmarks.Primitives;

/// <summary>
/// LZ4 block compress / decompress of game-like data (32-byte entity records, partially repetitive):
/// current implementation (V2), archived versions (V0, V1, and the rejected V3 decoder) and
/// K4os.Compression.LZ4 (fast level) as the reference.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class Lz4Bench
{
    private byte[] _input = Array.Empty<byte>();
    private byte[] _compressed = Array.Empty<byte>();
    private byte[] _compressedK4os = Array.Empty<byte>();
    private byte[] _output = Array.Empty<byte>();
    private byte[] _target = Array.Empty<byte>();
    private int[] _scratch4096 = Array.Empty<int>();
    private int[] _scratch16384 = Array.Empty<int>();

    [Params(1024, 65536)]
    public int Size { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _input = BenchmarkData.GameLike(Size, 11);
        _target = new byte[Lz4Block.GetMaxCompressedLength(Size)];
        _output = new byte[Size];
        _scratch4096 = new int[4096];
        _scratch16384 = new int[16384];

        int n = Lz4Block.Compress(_input, _target, _scratch4096);
        _compressed = _target.AsSpan(0, n).ToArray();
        byte[] k = new byte[LZ4Codec.MaximumOutputSize(Size)];
        int kn = LZ4Codec.Encode(_input, k, LZ4Level.L00_FAST);
        _compressedK4os = k.AsSpan(0, kn).ToArray();

        Console.WriteLine($"// Size={Size}: ours={n} bytes, K4os={kn} bytes");
    }

    [Benchmark(Baseline = true)]
    public int Compress_K4os() => LZ4Codec.Encode(_input, _target, LZ4Level.L00_FAST);

    [Benchmark]
    public int Compress_V0_4096() => Lz4BlockV0.Compress(_input, _target, _scratch4096);

    [Benchmark]
    public int Compress_V1_4096() => Lz4BlockV1.Compress(_input, _target, _scratch4096);

    [Benchmark]
    public int Compress_Current_4096() => Lz4Block.Compress(_input, _target, _scratch4096);

    [Benchmark]
    public int Compress_Current_16384() => Lz4Block.Compress(_input, _target, _scratch16384);

    [Benchmark]
    public int Compress_Current_ThreadStatic() => Lz4Block.Compress(_input, _target);

    [Benchmark]
    public int Decompress_K4os() => LZ4Codec.Decode(_compressedK4os, _output);

    [Benchmark]
    public int Decompress_V0() => Lz4BlockV0.Decompress(_compressed, _output);

    [Benchmark]
    public int Decompress_V1() => Lz4BlockV1.Decompress(_compressed, _output);

    [Benchmark]
    public int Decompress_V3() => Lz4BlockV3.Decompress(_compressed, _output);

    [Benchmark]
    public int Decompress_Current() => Lz4Block.Decompress(_compressed, _output);
}
