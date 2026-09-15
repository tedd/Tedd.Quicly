using Tedd.Quicly.Http3.Qpack;
using Tedd.Quicly.Http3.WebTransport;

namespace Tedd.Quicly.Http3.Tests;

public class WebTransportRequestTests
{
    [Fact]
    public void Build_And_Validate_Connect_Request()
    {
        var h = new Http3HeaderCollection(512, 8);
        Assert.True(WebTransportRequest.TryBuildConnectRequest(h, "example.com:4433"u8, "/game"u8, "https://example.com"u8));
        Assert.Equal(6, h.Count);
        Assert.Equal("CONNECT", TestUtil.AsciiString(h.GetValue(0)));
        Assert.Equal(":protocol", TestUtil.AsciiString(h.GetName(1)));
        Assert.Equal("webtransport", TestUtil.AsciiString(h.GetValue(1)));
        Assert.Equal("https", TestUtil.AsciiString(h.GetValue(2)));

        Assert.Equal(WebTransportRequestStatus.Ok, WebTransportRequest.Validate(h, out WebTransportConnectRequest req));
        Assert.Equal("example.com:4433", TestUtil.AsciiString(req.Authority));
        Assert.Equal("/game", TestUtil.AsciiString(req.Path));
        Assert.Equal("https://example.com", TestUtil.AsciiString(req.Origin));

        // Without origin (non-browser client).
        h.Clear();
        Assert.True(WebTransportRequest.TryBuildConnectRequest(h, "example.com"u8, "/"u8, ReadOnlySpan<byte>.Empty));
        Assert.Equal(5, h.Count);
        Assert.Equal(WebTransportRequestStatus.Ok, WebTransportRequest.Validate(h, out req));
        Assert.True(req.Origin.IsEmpty);
    }

    [Fact]
    public void Encode_Connect_Request_Decodes_To_Same_Fields()
    {
        byte[] block = new byte[512];
        int n = WebTransportRequest.EncodeConnectRequest(block, "example.com:4433"u8, "/game"u8, "https://example.com"u8);
        Assert.True(n > 2);
        var h = new Http3HeaderCollection(512, 8);
        Assert.Equal(QpackDecodeStatus.Ok, QpackDecoder.Decode(block.AsSpan(0, n), h));
        Assert.Equal(WebTransportRequestStatus.Ok, WebTransportRequest.Validate(h, out WebTransportConnectRequest req));
        Assert.Equal("/game", TestUtil.AsciiString(req.Path));
        Assert.Equal("https://example.com", TestUtil.AsciiString(req.Origin));

        // Static-table hits: :method CONNECT is index 15 -> 0xCF; :scheme https is 23 -> 0xD7.
        Assert.Equal(0xCF, block[2]);

        for (int size = 0; size < n; size++)
        {
            Assert.Equal(-1, WebTransportRequest.EncodeConnectRequest(block.AsSpan(0, size), "example.com:4433"u8, "/game"u8, "https://example.com"u8));
        }
        int noOrigin = WebTransportRequest.EncodeConnectRequest(block, "example.com:4433"u8, "/game"u8, ReadOnlySpan<byte>.Empty, useHuffman: false);
        Assert.True(noOrigin > 2 && noOrigin < n + 30);
        Assert.Equal(QpackDecodeStatus.Ok, QpackDecoder.Decode(block.AsSpan(0, noOrigin), h));
        Assert.Equal(5, h.Count);
    }

    [Fact]
    public void Build_Connect_Request_Fails_When_Collection_Full()
    {
        for (int capacity = 1; capacity <= 5; capacity++)
        {
            var h = new Http3HeaderCollection(512, capacity);
            Assert.False(WebTransportRequest.TryBuildConnectRequest(h, "a"u8, "/"u8, "o"u8));
        }
        var five = new Http3HeaderCollection(512, 5);
        Assert.True(WebTransportRequest.TryBuildConnectRequest(five, "a"u8, "/"u8, ReadOnlySpan<byte>.Empty));
        var six = new Http3HeaderCollection(512, 6);
        Assert.True(WebTransportRequest.TryBuildConnectRequest(six, "a"u8, "/"u8, "o"u8));
    }

    private static Http3HeaderCollection Request(params (string Name, string Value)[] fields)
    {
        var h = new Http3HeaderCollection(512, 8);
        foreach (var (name, value) in fields) Assert.True(h.TryAdd(TestUtil.Ascii(name), TestUtil.Ascii(value)));
        return h;
    }

    [Fact]
    public void Validate_Reports_Each_Failure()
    {
        Assert.Equal(WebTransportRequestStatus.NotConnect, WebTransportRequest.Validate(Request(), out _));
        Assert.Equal(WebTransportRequestStatus.NotConnect, WebTransportRequest.Validate(Request((":method", "GET"), (":protocol", "webtransport")), out _));
        Assert.Equal(WebTransportRequestStatus.NotWebTransport, WebTransportRequest.Validate(Request((":method", "CONNECT")), out _));
        Assert.Equal(WebTransportRequestStatus.NotWebTransport, WebTransportRequest.Validate(Request((":method", "CONNECT"), (":protocol", "websocket")), out _));
        Assert.Equal(WebTransportRequestStatus.InvalidScheme, WebTransportRequest.Validate(Request((":method", "CONNECT"), (":protocol", "webtransport")), out _));
        Assert.Equal(WebTransportRequestStatus.InvalidScheme, WebTransportRequest.Validate(Request((":method", "CONNECT"), (":protocol", "webtransport"), (":scheme", "http")), out _));
        Assert.Equal(WebTransportRequestStatus.MissingAuthority, WebTransportRequest.Validate(Request((":method", "CONNECT"), (":protocol", "webtransport"), (":scheme", "https")), out _));
        Assert.Equal(WebTransportRequestStatus.MissingAuthority, WebTransportRequest.Validate(Request((":method", "CONNECT"), (":protocol", "webtransport"), (":scheme", "https"), (":authority", "")), out _));
        Assert.Equal(WebTransportRequestStatus.MissingPath, WebTransportRequest.Validate(Request((":method", "CONNECT"), (":protocol", "webtransport"), (":scheme", "https"), (":authority", "a")), out _));
        Assert.Equal(WebTransportRequestStatus.MissingPath, WebTransportRequest.Validate(Request((":method", "CONNECT"), (":protocol", "webtransport"), (":scheme", "https"), (":authority", "a"), (":path", "")), out WebTransportConnectRequest req));
        Assert.True(req.Authority.IsEmpty);
        Assert.Equal(WebTransportRequestStatus.Ok, WebTransportRequest.Validate(Request((":method", "CONNECT"), (":protocol", "webtransport"), (":scheme", "https"), (":authority", "a"), (":path", "/")), out req));
        Assert.Equal("a", TestUtil.AsciiString(req.Authority));
        Assert.Throws<ArgumentNullException>(() => WebTransportRequest.Validate(null!, out _));
        Assert.Throws<ArgumentNullException>(() => WebTransportRequest.TryBuildConnectRequest(null!, "a"u8, "/"u8, ""u8));
        Assert.Throws<ArgumentNullException>(() => WebTransportRequest.TryBuildConnectResponse(null!));
        Assert.Throws<ArgumentNullException>(() => WebTransportRequest.TryBuildResponse(null!, 200, ""u8, 0));
    }

    [Fact]
    public void Connect_Response()
    {
        var h = new Http3HeaderCollection(128, 2);
        Assert.True(WebTransportRequest.TryBuildConnectResponse(h));
        Assert.Equal(2, h.Count);
        Assert.Equal("200", TestUtil.AsciiString(h.GetValue(0)));
        Assert.True(h.TryGet("sec-webtransport-http3-draft"u8, out ReadOnlySpan<byte> draft));
        Assert.Equal("draft02", TestUtil.AsciiString(draft));
        Assert.Equal("draft02", TestUtil.AsciiString(WebTransportRequest.LegacyDraftValue));

        h.Clear();
        Assert.True(WebTransportRequest.TryBuildConnectResponse(h, includeLegacyDraftHeader: false));
        Assert.Equal(1, h.Count);

        var one = new Http3HeaderCollection(128, 1);
        Assert.False(WebTransportRequest.TryBuildConnectResponse(one));
        Assert.False(WebTransportRequest.TryBuildConnectResponse(new Http3HeaderCollection(2, 4)));

        byte[] block = new byte[128];
        int n = WebTransportRequest.EncodeConnectResponse(block);
        Assert.Equal(0xD9, block[2]); // :status 200 = index 25
        var dec = new Http3HeaderCollection(128, 4);
        Assert.Equal(QpackDecodeStatus.Ok, QpackDecoder.Decode(block.AsSpan(0, n), dec));
        Assert.Equal(2, dec.Count);
        Assert.Equal("draft02", TestUtil.AsciiString(dec.GetValue(1)));
        for (int size = 0; size < n; size++) Assert.Equal(-1, WebTransportRequest.EncodeConnectResponse(block.AsSpan(0, size)));
        Assert.Equal(3, WebTransportRequest.EncodeConnectResponse(block, includeLegacyDraftHeader: false));
        Assert.True(WebTransportRequest.EncodeConnectResponse(block, useHuffman: false) > n);
    }

    [Fact]
    public void Plain_Response_Header_List()
    {
        var h = new Http3HeaderCollection(256, 4);
        Assert.True(WebTransportRequest.TryBuildResponse(h, 200, "text/html; charset=utf-8"u8, 1234));
        Assert.Equal(3, h.Count);
        Assert.Equal("200", TestUtil.AsciiString(h.GetValue(0)));
        Assert.Equal("text/html; charset=utf-8", TestUtil.AsciiString(h.GetValue(1)));
        Assert.Equal("1234", TestUtil.AsciiString(h.GetValue(2)));

        h.Clear();
        Assert.True(WebTransportRequest.TryBuildResponse(h, 404, ReadOnlySpan<byte>.Empty, 0));
        Assert.Equal(2, h.Count);
        Assert.Equal("404", TestUtil.AsciiString(h.GetValue(0)));
        Assert.Equal("content-length", TestUtil.AsciiString(h.GetName(1)));
        Assert.Equal("0", TestUtil.AsciiString(h.GetValue(1)));

        h.Clear();
        Assert.True(WebTransportRequest.TryBuildResponse(h, 999, "x/y"u8, -1));
        Assert.Equal(2, h.Count);
        Assert.Equal("999", TestUtil.AsciiString(h.GetValue(0)));
        Assert.Equal("content-type", TestUtil.AsciiString(h.GetName(1)));

        h.Clear();
        Assert.True(WebTransportRequest.TryBuildResponse(h, 100, ReadOnlySpan<byte>.Empty, long.MaxValue));
        Assert.Equal("9223372036854775807", TestUtil.AsciiString(h.GetValue(1)));

        Assert.False(WebTransportRequest.TryBuildResponse(h, 99, "x"u8, 1));
        Assert.False(WebTransportRequest.TryBuildResponse(h, 1000, "x"u8, 1));
        Assert.False(WebTransportRequest.TryBuildResponse(new Http3HeaderCollection(256, 1), 200, "x"u8, 1));
        Assert.False(WebTransportRequest.TryBuildResponse(new Http3HeaderCollection(256, 2), 200, "x"u8, 1));
        Assert.False(WebTransportRequest.TryBuildResponse(new Http3HeaderCollection(2, 3), 200, "x"u8, 1));
        Assert.True(WebTransportRequest.TryBuildResponse(new Http3HeaderCollection(256, 3), 200, "x"u8, 1));
    }

    [Fact]
    public void Plain_Response_Encoded()
    {
        byte[] block = new byte[256];
        int n = WebTransportRequest.EncodeResponse(block, 200, "text/html; charset=utf-8"u8, 1234);
        Assert.Equal(0xD9, block[2]); // :status 200
        Assert.Equal(0xF4, block[3]); // content-type text/html; charset=utf-8 = index 52
        var h = new Http3HeaderCollection(256, 4);
        Assert.Equal(QpackDecodeStatus.Ok, QpackDecoder.Decode(block.AsSpan(0, n), h));
        Assert.Equal(3, h.Count);
        Assert.Equal("1234", TestUtil.AsciiString(h.GetValue(2)));

        for (int size = 0; size < n; size++) Assert.Equal(-1, WebTransportRequest.EncodeResponse(block.AsSpan(0, size), 200, "text/html; charset=utf-8"u8, 1234));
        Assert.Equal(-1, WebTransportRequest.EncodeResponse(block, 42, "x"u8, 1));
        Assert.Equal(-1, WebTransportRequest.EncodeResponse(block, 1000, "x"u8, 1));

        n = WebTransportRequest.EncodeResponse(block, 503, ReadOnlySpan<byte>.Empty, -1, useHuffman: false);
        Assert.Equal(3, n);
        Assert.Equal(0xDC, block[2]); // :status 503 = index 28
        n = WebTransportRequest.EncodeResponse(block, 418, ReadOnlySpan<byte>.Empty, 7);
        Assert.Equal(QpackDecodeStatus.Ok, QpackDecoder.Decode(block.AsSpan(0, n), h));
        Assert.Equal(2, h.Count);
        Assert.Equal("418", TestUtil.AsciiString(h.GetValue(0)));
        Assert.Equal("7", TestUtil.AsciiString(h.GetValue(1)));
    }
}
