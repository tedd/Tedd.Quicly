using System.Text;

namespace Tedd.Quicly.Http3.Tests;

internal static class TestUtil
{
    public static byte[] Hex(string hex) => Convert.FromHexString(hex.Replace(" ", "", StringComparison.Ordinal));

    public static string ToHex(ReadOnlySpan<byte> bytes) => Convert.ToHexString(bytes).ToLowerInvariant();

    public static byte[] Ascii(string s) => Encoding.ASCII.GetBytes(s);

    public static string AsciiString(ReadOnlySpan<byte> bytes) => Encoding.ASCII.GetString(bytes);

    public static Http3HeaderCollection ChromeConnectHeaders()
    {
        var h = new Http3HeaderCollection(2048, 32);
        Assert.True(h.TryAdd(":method"u8, "CONNECT"u8));
        Assert.True(h.TryAdd(":authority"u8, "game.example.com:4433"u8));
        Assert.True(h.TryAdd(":scheme"u8, "https"u8));
        Assert.True(h.TryAdd(":path"u8, "/session?token=abc123"u8));
        Assert.True(h.TryAdd(":protocol"u8, "webtransport"u8));
        Assert.True(h.TryAdd("origin"u8, "https://game.example.com"u8));
        Assert.True(h.TryAdd("sec-webtransport-http3-draft02"u8, "1"u8));
        Assert.True(h.TryAdd("user-agent"u8, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"u8));
        return h;
    }

    public static Http3HeaderCollection ResponseHeaders()
    {
        var h = new Http3HeaderCollection(2048, 32);
        Assert.True(h.TryAdd(":status"u8, "200"u8));
        Assert.True(h.TryAdd("content-type"u8, "text/html; charset=utf-8"u8));
        Assert.True(h.TryAdd("content-length"u8, "1234"u8));
        Assert.True(h.TryAdd("cache-control"u8, "no-cache"u8));
        Assert.True(h.TryAdd("server"u8, "quicly"u8));
        Assert.True(h.TryAdd("x-custom-header"u8, "some value with spaces & symbols!"u8));
        Assert.True(h.TryAdd("set-cookie"u8, "session=deadbeef; Path=/; HttpOnly"u8));
        Assert.True(h.TryAdd("alt-svc"u8, "h3=\":443\"; ma=86400"u8));
        return h;
    }

    public static void AssertSameHeaders(Http3HeaderCollection expected, Http3HeaderCollection actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(AsciiString(expected.GetName(i)), AsciiString(actual.GetName(i)));
            Assert.Equal(AsciiString(expected.GetValue(i)), AsciiString(actual.GetValue(i)));
        }
    }
}
