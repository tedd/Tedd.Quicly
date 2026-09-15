using Tedd.Quicly.Http3.Qpack;
using Tedd.Quicly.Http3.WebTransport;

namespace Tedd.Quicly.Http3.Tests;

/// <summary>
/// Every hot path must not allocate on the GC heap in steady state (ADR 0007). Each test warms the path up
/// (type initialisers, tiering) and then measures <see cref="GC.GetAllocatedBytesForCurrentThread"/> around a loop.
/// </summary>
public class ZeroAllocationTests
{
    private const int Iterations = 2_000;

    private static void AssertNoAllocations(Action body)
    {
        for (int i = 0; i < 50; i++) body();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < Iterations; i++) body();
        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);
    }

    [Fact]
    public void Huffman_Encode_And_Decode()
    {
        byte[] plain = TestUtil.Ascii("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36");
        byte[] encoded = new byte[128];
        byte[] decoded = new byte[128];
        AssertNoAllocations(() =>
        {
            int n = HpackHuffman.Encode(plain, encoded);
            if (n != HpackHuffman.GetEncodedLength(plain)) throw new InvalidOperationException();
            if (HpackHuffman.Decode(encoded.AsSpan(0, n), decoded) != plain.Length) throw new InvalidOperationException();
        });
    }

    [Fact]
    public void Qpack_Encode_And_Decode()
    {
        Http3HeaderCollection src = TestUtil.ChromeConnectHeaders();
        var dst = new Http3HeaderCollection(2048, 32);
        byte[] block = new byte[2048];
        AssertNoAllocations(() =>
        {
            int n = QpackEncoder.Encode(src, block);
            if (n <= 0) throw new InvalidOperationException();
            if (QpackDecoder.Decode(block.AsSpan(0, n), dst, 16384) != QpackDecodeStatus.Ok) throw new InvalidOperationException();
            if (!dst.TryGet(":path"u8, out ReadOnlySpan<byte> path) || path.IsEmpty) throw new InvalidOperationException();
            int count = 0;
            foreach (Http3Header h in dst) count += h.Name.Length + h.Value.Length;
            if (count == 0) throw new InvalidOperationException();
        });
    }

    [Fact]
    public void Frame_Reader_And_Writer()
    {
        byte[] stream = new byte[4096];
        byte[] payload = new byte[500];
        int pos = 0;
        for (int i = 0; i < 6; i++) pos += Http3FrameWriter.WriteFrame(stream.AsSpan(pos), Http3FrameType.Data, payload);
        int total = pos;
        var reader = new Http3FrameReader(1 << 16);
        byte[] scratch = new byte[16];
        AssertNoAllocations(() =>
        {
            long bytes = 0;
            for (int offset = 0; offset < total; offset += 300)
            {
                ReadOnlySpan<byte> input = stream.AsSpan(offset, Math.Min(300, total - offset));
                while (!input.IsEmpty)
                {
                    Http3FrameReadStatus s = reader.Read(input, out int consumed, out ReadOnlySpan<byte> p);
                    bytes += p.Length;
                    input = input.Slice(consumed);
                    if (s == Http3FrameReadStatus.NeedMoreData) break;
                }
            }
            if (bytes != 6 * 500) throw new InvalidOperationException();
            if (Http3FrameWriter.WriteHeader(scratch, Http3FrameType.Headers, 12345) <= 0) throw new InvalidOperationException();
        });
    }

    [Fact]
    public void Settings_Encode_And_Decode()
    {
        Http3Settings settings = Http3Settings.CreateWebTransportServerDefaults(32);
        byte[] buffer = new byte[64];
        AssertNoAllocations(() =>
        {
            int n = settings.WriteFrame(buffer);
            if (!Http3FrameReader.TryReadFrame(buffer.AsSpan(0, n), out _, out ReadOnlySpan<byte> payload, out _)) throw new InvalidOperationException();
            if (Http3Settings.Decode(payload, out Http3Settings back) != Http3SettingsDecodeStatus.Ok || back != settings) throw new InvalidOperationException();
        });
    }

    [Fact]
    public void Datagram_Capsule_And_WebTransport_Framing()
    {
        byte[] buffer = new byte[128];
        byte[] payload = new byte[32];
        AssertNoAllocations(() =>
        {
            int n = HttpDatagram.Write(buffer, 8, payload);
            if (!HttpDatagram.TryRead(buffer.AsSpan(0, n), out ulong id, out _) || id != 8) throw new InvalidOperationException();
            n = CapsuleWriter.WriteCloseSession(buffer, 1, "bye"u8);
            if (!CapsuleReader.TryRead(buffer.AsSpan(0, n), out _, out ReadOnlySpan<byte> p, out _)) throw new InvalidOperationException();
            if (!CapsuleReader.TryParseCloseSession(p, out _, out _)) throw new InvalidOperationException();
            n = WebTransportFraming.WriteUnidirectionalPreamble(buffer, 4);
            if (WebTransportFraming.TryReadUnidirectionalPreamble(buffer.AsSpan(0, n), out _, out _) != WebTransportPreambleStatus.Ok) throw new InvalidOperationException();
            if (!WebTransportErrorCode.TryFromHttp3(WebTransportErrorCode.ToHttp3(77), out uint app) || app != 77) throw new InvalidOperationException();
        });
    }

    [Fact]
    public void WebTransport_Request_Helpers()
    {
        var headers = new Http3HeaderCollection(512, 8);
        byte[] block = new byte[512];
        AssertNoAllocations(() =>
        {
            headers.Clear();
            if (!WebTransportRequest.TryBuildConnectRequest(headers, "example.com"u8, "/game"u8, "https://example.com"u8)) throw new InvalidOperationException();
            if (WebTransportRequest.Validate(headers, out _) != WebTransportRequestStatus.Ok) throw new InvalidOperationException();
            if (WebTransportRequest.EncodeConnectRequest(block, "example.com"u8, "/game"u8, "https://example.com"u8) <= 0) throw new InvalidOperationException();
            if (WebTransportRequest.EncodeConnectResponse(block) <= 0) throw new InvalidOperationException();
            if (WebTransportRequest.EncodeResponse(block, 200, "text/plain"u8, 42) <= 0) throw new InvalidOperationException();
            headers.Clear();
            if (!WebTransportRequest.TryBuildResponse(headers, 404, "text/plain"u8, 0)) throw new InvalidOperationException();
        });
    }
}
