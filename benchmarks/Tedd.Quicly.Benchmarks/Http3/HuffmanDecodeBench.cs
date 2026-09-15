using System.Text;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.Http3;
using Tedd.Quicly.Http3.Qpack;

namespace Tedd.Quicly.Benchmarks.Http3;

/// <summary>Huffman decode of a 40-byte header value: V0 bit-by-bit tree walk vs V1 nibble transition table.</summary>
[Config(typeof(Http3BenchConfig))]
public class HuffmanDecodeBench
{
    private byte[] _encoded = [];
    private byte[] _decoded = [];

    [GlobalSetup]
    public void Setup()
    {
        byte[] value = Encoding.ASCII.GetBytes("Mozilla/5.0 (Windows NT 10.0; Win64; x64)"); // 41 bytes -> use 40
        value = value.AsSpan(0, 40).ToArray();
        _encoded = new byte[HpackHuffman.GetEncodedLength(value)];
        HpackHuffman.Encode(value, _encoded);
        _decoded = new byte[64];
    }

    [Benchmark(Baseline = true)]
    public int V0_TreeWalk() => HpackHuffmanV0.Decode(_encoded, _decoded);

    [Benchmark]
    public int V1_NibbleTable() => HpackHuffman.Decode(_encoded, _decoded);
}
