using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Http3;
using Tedd.Quicly.Http3.Qpack;

namespace Tedd.Quicly.Benchmarks.Http3;

/// <summary>QPACK decode (and encode) of a Chrome-like WebTransport Extended CONNECT header block.</summary>
[Config(typeof(Http3BenchConfig))]
public class QpackDecodeBench
{
    private byte[] _block = [];
    private byte[] _encodeBuffer = [];
    private Http3HeaderCollection _headers = null!;
    private Http3HeaderCollection _source = null!;

    [GlobalSetup]
    public void Setup()
    {
        _source = new Http3HeaderCollection(1024, 16);
        _source.TryAdd(":method"u8, "CONNECT"u8);
        _source.TryAdd(":authority"u8, "game.example.com:4433"u8);
        _source.TryAdd(":scheme"u8, "https"u8);
        _source.TryAdd(":path"u8, "/session?token=abc123"u8);
        _source.TryAdd(":protocol"u8, "webtransport"u8);
        _source.TryAdd("origin"u8, "https://game.example.com"u8);
        _source.TryAdd("sec-webtransport-http3-draft02"u8, "1"u8);
        _source.TryAdd("user-agent"u8, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"u8);
        _encodeBuffer = new byte[1024];
        int n = QpackEncoder.Encode(_source, _encodeBuffer);
        _block = _encodeBuffer.AsSpan(0, n).ToArray();
        _headers = new Http3HeaderCollection(1024, 16);
    }

    [Benchmark]
    public QpackDecodeStatus Decode() => QpackDecoder.Decode(_block, _headers, 16384);

    [Benchmark]
    public int Encode() => QpackEncoder.Encode(_source, _encodeBuffer);
}
