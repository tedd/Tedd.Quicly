using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Archive.Acme;

namespace Tedd.Quicly.Benchmarks.Acme;

/// <summary>
/// Base64url encode/decode: V0 (Convert.ToBase64String + Replace/TrimEnd) vs V1 (System.Buffers.Text.Base64Url).
/// Sizes: 32 B (a SHA-256 thumbprint / DNS TXT value), 256 B (an RSA-2048 modulus or signature), 1 KiB (a CSR).
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class Base64UrlBench
{
    private byte[] _data = [];
    private string _encoded = string.Empty;
    private char[] _buffer = [];

    [Params(32, 256, 1024)]
    public int Size { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _data = new byte[Size];
        new Random(42).NextBytes(_data);
        _encoded = Base64UrlV0.Encode(_data);
        _buffer = new char[Base64UrlCodec.GetEncodedLength(Size)];
    }

    [Benchmark(Baseline = true)]
    public string Encode_V0() => Base64UrlV0.Encode(_data);

    [Benchmark]
    public string Encode_V1() => Base64UrlCodec.Encode(_data);

    [Benchmark]
    public int Encode_V1_Span() => Base64UrlCodec.Encode(_data, _buffer);

    [Benchmark]
    public byte[] Decode_V0() => Base64UrlV0.Decode(_encoded);

    [Benchmark]
    public byte[] Decode_V1() => Base64UrlCodec.Decode(_encoded);
}
