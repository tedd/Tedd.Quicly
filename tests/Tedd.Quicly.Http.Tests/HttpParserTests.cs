using System.Text;
using Tedd.Quicly.Http.Parsing;

namespace Tedd.Quicly.Http.Tests;

public class HttpParserTests
{
    private static HttpParseStatus Parse(string request, out ParsedRequest parsed, int maxLine = 8192, int maxHeaders = 32768, int maxCount = 100)
    {
        parsed = new ParsedRequest(maxCount);
        return HttpParser.TryParse(Encoding.Latin1.GetBytes(request), maxLine, maxHeaders, ref parsed);
    }

    private static string Slice(string request, int start, int length) => request.Substring(start, length);

    [Fact]
    public void Parses_request_line_and_headers()
    {
        const string req = "GET /a/b?x=1 HTTP/1.1\r\nHost: example.com\r\nX-Empty:\r\nX-Trim:  \t v \t \r\n\r\nrest";
        var status = Parse(req, out var p);
        Assert.Equal(HttpParseStatus.Ok, status);
        Assert.Equal("GET", Slice(req, p.MethodStart, p.MethodLength));
        Assert.Equal("/a/b?x=1", Slice(req, p.TargetStart, p.TargetLength));
        Assert.Equal(HttpProtocolVersion.Http11, p.Version);
        Assert.Equal(3, p.HeaderCount);
        Assert.Equal("Host", Slice(req, p.Headers[0].NameStart, p.Headers[0].NameLength));
        Assert.Equal("example.com", Slice(req, p.Headers[0].ValueStart, p.Headers[0].ValueLength));
        Assert.Equal("X-Empty", Slice(req, p.Headers[1].NameStart, p.Headers[1].NameLength));
        Assert.Equal(0, p.Headers[1].ValueLength);
        Assert.Equal("v", Slice(req, p.Headers[2].ValueStart, p.Headers[2].ValueLength));
        Assert.Equal(req.Length - 4, p.Consumed);
    }

    [Fact]
    public void Parses_http10_and_skips_leading_crlf()
    {
        var status = Parse("\r\n\r\nGET / HTTP/1.0\r\n\r\n", out var p);
        Assert.Equal(HttpParseStatus.Ok, status);
        Assert.Equal(HttpProtocolVersion.Http10, p.Version);
        Assert.Equal(0, p.HeaderCount);
        Assert.Equal(4, p.MethodStart);
    }

    [Theory]
    [InlineData("GET / HTTP/1.1\r\n")]
    [InlineData("GET / HTTP/1.1\r\nHost: a")]
    [InlineData("GET / HTTP/1.1\r\nHost: a\r\n")]
    [InlineData("GET / HT")]
    [InlineData("")]
    public void Needs_more_for_incomplete(string req)
    {
        Assert.Equal(HttpParseStatus.NeedMore, Parse(req, out _));
    }

    [Theory]
    [InlineData("GET / HTTP/1.1\nHost: a\r\n\r\n")]            // bare LF request line
    [InlineData("GET / HTTP/1.1\r\nHost: a\n\r\n")]            // bare LF header line
    [InlineData("\nGET / HTTP/1.1\r\n\r\n")]                   // bare LF before request line
    [InlineData("GET / HTTP/1.1\r\nHost: a\r\n continued\r\n\r\n")] // obs-fold
    [InlineData("GET / HTTP/1.1\r\n\tfold\r\n\r\n")]           // obs-fold with tab
    [InlineData("GET / HTTP/1.1\r\nHost : a\r\n\r\n")]         // whitespace before colon
    [InlineData("GET / HTTP/1.1\r\n: a\r\n\r\n")]              // empty name
    [InlineData("GET / HTTP/1.1\r\nNoColon\r\n\r\n")]
    [InlineData("GET / HTTP/1.1\r\nBad(Name): a\r\n\r\n")]
    [InlineData("GET / HTTP/1.1\r\nHost: ab\r\n\r\n")]   // control char in value
    [InlineData("GET / HTTP/1.1\r\nHost: ab\r\n\r\n")]   // DEL in value
    [InlineData("GET / HTTP/1.1\r\nHost: a\rb\r\n\r\n")]       // CR in value
    [InlineData(" GET / HTTP/1.1\r\n\r\n")]                    // leading space
    [InlineData("GET  / HTTP/1.1\r\n\r\n")]                    // empty target
    [InlineData("GET / HTTP/1.1\r\n\r\n")]               // control in target
    [InlineData("GET / HTTP/1.1 extra\r\n\r\n")]
    [InlineData("GET / HTTP/1.\r\n\r\n")]
    [InlineData("GET / HTTP/x.1\r\n\r\n")]
    [InlineData("GET / HTTP/1x1\r\n\r\n")]
    [InlineData("GET / http/1.1\r\n\r\n")]
    [InlineData("GET /\r\n\r\n")]                              // HTTP/0.9 style
    [InlineData("G(ET / HTTP/1.1\r\n\r\n")]                    // bad method char
    [InlineData("GET\r\n\r\n")]
    [InlineData("\r\n\r\n\r\n")]                               // only empty lines (CRLF then bare header end)
    public void Rejects_malformed(string req)
    {
        Assert.Equal(HttpParseStatus.Invalid, Parse(req, out _));
    }

    [Theory]
    [InlineData("GET / HTTP/2.0\r\n\r\n")]
    [InlineData("GET / HTTP/0.9\r\n\r\n")]
    [InlineData("GET / HTTP/1.2\r\n\r\n")]
    public void Unsupported_version(string req)
    {
        Assert.Equal(HttpParseStatus.VersionNotSupported, Parse(req, out _));
    }

    [Fact]
    public void Request_line_too_long()
    {
        var req = "GET /" + new string('a', 100) + " HTTP/1.1\r\n\r\n";
        Assert.Equal(HttpParseStatus.RequestLineTooLong, Parse(req, out _, maxLine: 32));
        // exactly at the limit is fine
        var line = "GET /" + new string('a', 32 - "GET / HTTP/1.1".Length) + " HTTP/1.1";
        Assert.Equal(32, line.Length);
        Assert.Equal(HttpParseStatus.Ok, Parse(line + "\r\n\r\n", out _, maxLine: 32));
        // limit hit while still incomplete
        Assert.Equal(HttpParseStatus.RequestLineTooLong, Parse(new string('a', 40), out _, maxLine: 32));
        Assert.Equal(HttpParseStatus.NeedMore, Parse(new string('a', 10), out _, maxLine: 32));
    }

    [Fact]
    public void Headers_too_large_by_bytes()
    {
        var req = "GET / HTTP/1.1\r\nX: " + new string('v', 100) + "\r\n\r\n";
        Assert.Equal(HttpParseStatus.HeadersTooLarge, Parse(req, out _, maxHeaders: 64));
        // incomplete but already beyond the limit
        Assert.Equal(HttpParseStatus.HeadersTooLarge, Parse("GET / HTTP/1.1\r\nX: " + new string('v', 100), out _, maxHeaders: 64));
        // a header section that fits exactly
        string headers = "Host: a\r\n\r\n";
        Assert.Equal(HttpParseStatus.Ok, Parse("GET / HTTP/1.1\r\n" + headers, out _, maxHeaders: headers.Length));
        Assert.Equal(HttpParseStatus.HeadersTooLarge, Parse("GET / HTTP/1.1\r\n" + headers, out _, maxHeaders: headers.Length - 1));
        // terminating line exceeding the budget
        Assert.Equal(HttpParseStatus.HeadersTooLarge, Parse("GET / HTTP/1.1\r\nHost: abc\r\n\r\n", out _, maxHeaders: 11));
    }

    [Fact]
    public void Headers_too_large_by_count()
    {
        var req = "GET / HTTP/1.1\r\nA: 1\r\nB: 2\r\nC: 3\r\n\r\n";
        Assert.Equal(HttpParseStatus.HeadersTooLarge, Parse(req, out _, maxCount: 2));
        Assert.Equal(HttpParseStatus.Ok, Parse(req, out _, maxCount: 3));
    }

    [Fact]
    public void Obs_text_in_values_is_accepted()
    {
        var status = Parse("GET / HTTP/1.1\r\nX: café\r\n\r\n", out var p);
        Assert.Equal(HttpParseStatus.Ok, status);
        Assert.Equal(1, p.HeaderCount);
    }

    [Fact]
    public void Terminator_span_is_lf_cr_lf()
    {
        Assert.True(HttpParser.HeaderTerminator.SequenceEqual("\n\r\n"u8));
    }

    [Fact]
    public void Steady_state_parse_does_not_allocate()
    {
        var request = Encoding.ASCII.GetBytes(
            "GET /index.html?x=1 HTTP/1.1\r\nHost: example.com\r\nUser-Agent: test\r\nAccept: */*\r\n" +
            "Accept-Encoding: gzip, deflate, br\r\nConnection: keep-alive\r\nIf-None-Match: \"abc\"\r\n\r\n");
        var parsed = new ParsedRequest(32);

        static int Run(byte[] request, ref ParsedRequest parsed)
        {
            int sink = 0;
            var status = HttpParser.TryParse(request, 8192, 32768, ref parsed);
            if (status != HttpParseStatus.Ok)
                throw new InvalidOperationException(status.ToString());
            sink += KnownStrings.InternMethod(request.AsSpan(parsed.MethodStart, parsed.MethodLength)).Length;
            for (int i = 0; i < parsed.HeaderCount; i++)
            {
                ref var h = ref parsed.Headers[i];
                sink += KnownStrings.InternHeaderName(request.AsSpan(h.NameStart, h.NameLength)).Length;
            }
            // known values only (Accept, Accept-Encoding, Connection): the others are not interned
            sink += KnownStrings.InternHeaderValue(request.AsSpan(parsed.Headers[2].ValueStart, parsed.Headers[2].ValueLength)).Length;
            sink += KnownStrings.InternHeaderValue(request.AsSpan(parsed.Headers[3].ValueStart, parsed.Headers[3].ValueLength)).Length;
            sink += KnownStrings.InternHeaderValue(request.AsSpan(parsed.Headers[4].ValueStart, parsed.Headers[4].ValueLength)).Length;
            return sink;
        }

        for (int i = 0; i < 2000; i++)
            Run(request, ref parsed);

        long before = GC.GetAllocatedBytesForCurrentThread();
        int total = 0;
        for (int i = 0; i < 10_000; i++)
            total += Run(request, ref parsed);
        long after = GC.GetAllocatedBytesForCurrentThread();

        Assert.True(total > 0);
        Assert.Equal(0, after - before);
    }
}

public class KnownStringsTests
{
    [Theory]
    [InlineData("GET")]
    [InlineData("PUT")]
    [InlineData("HEAD")]
    [InlineData("POST")]
    [InlineData("PATCH")]
    [InlineData("TRACE")]
    [InlineData("DELETE")]
    [InlineData("OPTIONS")]
    [InlineData("CONNECT")]
    public void Known_methods_are_interned(string method)
    {
        var a = KnownStrings.InternMethod(Encoding.ASCII.GetBytes(method));
        var b = KnownStrings.InternMethod(Encoding.ASCII.GetBytes(method));
        Assert.Equal(method, a);
        Assert.Same(a, b);
    }

    [Theory]
    [InlineData("BREW")]
    [InlineData("GETX")]
    [InlineData("PUTS")]
    [InlineData("HEADS")]
    [InlineData("DELETX")]
    [InlineData("OPTIONZ")]
    [InlineData("XY")]
    public void Unknown_methods_are_copied(string method)
    {
        Assert.Equal(method, KnownStrings.InternMethod(Encoding.ASCII.GetBytes(method)));
    }

    [Theory]
    [InlineData("host", "Host")]
    [InlineData("DATE", "Date")]
    [InlineData("range", "Range")]
    [InlineData("accept", "Accept")]
    [InlineData("cookie", "Cookie")]
    [InlineData("expect", "Expect")]
    [InlineData("origin", "Origin")]
    [InlineData("pragma", "Pragma")]
    [InlineData("referer", "Referer")]
    [InlineData("upgrade", "Upgrade")]
    [InlineData("connection", "Connection")]
    [InlineData("user-agent", "User-Agent")]
    [InlineData("keep-alive", "Keep-Alive")]
    [InlineData("content-type", "Content-Type")]
    [InlineData("authorization", "Authorization")]
    [InlineData("cache-control", "Cache-Control")]
    [InlineData("if-none-match", "If-None-Match")]
    [InlineData("content-length", "Content-Length")]
    [InlineData("accept-charset", "Accept-Charset")]
    [InlineData("accept-encoding", "Accept-Encoding")]
    [InlineData("accept-language", "Accept-Language")]
    [InlineData("x-forwarded-for", "X-Forwarded-For")]
    [InlineData("transfer-encoding", "Transfer-Encoding")]
    [InlineData("if-modified-since", "If-Modified-Since")]
    [InlineData("upgrade-insecure-requests", "Upgrade-Insecure-Requests")]
    public void Known_header_names_are_interned_with_canonical_casing(string input, string canonical)
    {
        var a = KnownStrings.InternHeaderName(Encoding.ASCII.GetBytes(input));
        var b = KnownStrings.InternHeaderName(Encoding.ASCII.GetBytes(input.ToUpperInvariant()));
        Assert.Equal(canonical, a);
        Assert.Same(a, b);
    }

    [Theory]
    [InlineData("Hosx")]
    [InlineData("Rangx")]
    [InlineData("Accepx")]
    [InlineData("Referex")]
    [InlineData("Connectiox")]
    [InlineData("Content-Typx")]
    [InlineData("Authorizatiox")]
    [InlineData("Content-Lengtx")]
    [InlineData("Accept-Encodinx")]
    [InlineData("Transfer-Encodinx")]
    [InlineData("Upgrade-Insecure-Requestx")]
    [InlineData("X-Custom-Header-Name")]
    public void Unknown_header_names_are_copied(string input)
    {
        Assert.Equal(input, KnownStrings.InternHeaderName(Encoding.ASCII.GetBytes(input)));
    }

    [Theory]
    [InlineData("", "")]
    [InlineData("*", "*")]
    [InlineData("1", "1")]
    [InlineData("0", "0")]
    [InlineData("*/*", "*/*")]
    [InlineData("CLOSE", "close")]
    [InlineData("Chunked", "chunked")]
    [InlineData("Keep-Alive", "keep-alive")]
    [InlineData("100-Continue", "100-continue")]
    [InlineData("gzip, deflate, br", "gzip, deflate, br")]
    public void Known_values_are_interned(string input, string canonical)
    {
        var a = KnownStrings.InternHeaderValue(Encoding.ASCII.GetBytes(input));
        var b = KnownStrings.InternHeaderValue(Encoding.ASCII.GetBytes(input));
        Assert.Equal(canonical, a);
        Assert.Same(a, b);
    }

    [Theory]
    [InlineData("x")]
    [InlineData("a/b")]
    [InlineData("clos!")]
    [InlineData("chunkey")]
    [InlineData("keep-alivx")]
    [InlineData("100-continux")]
    [InlineData("gzip, deflate, bx")]
    [InlineData("café")]
    public void Unknown_values_are_copied_as_latin1(string input)
    {
        Assert.Equal(input, KnownStrings.InternHeaderValue(Encoding.Latin1.GetBytes(input)));
    }
}

public class PercentDecoderTests
{
    [Theory]
    [InlineData("/plain", "/plain")]
    [InlineData("/a%20b", "/a b")]
    [InlineData("/%C3%A9", "/é")]
    [InlineData("/%2F%2f", "///")]
    [InlineData("/%41%6a", "/Aj")]
    [InlineData("", "")]
    public void Decodes(string raw, string expected)
    {
        Assert.True(PercentDecoder.TryDecode(Encoding.Latin1.GetBytes(raw), out var decoded));
        Assert.Equal(expected, decoded);
    }

    [Fact]
    public void Decodes_raw_utf8_without_escapes()
    {
        Assert.True(PercentDecoder.TryDecode(Encoding.UTF8.GetBytes("/café"), out var decoded));
        Assert.Equal("/café", decoded);
        Assert.False(PercentDecoder.TryDecode([(byte)'/', 0xC3], out _));
    }

    [Theory]
    [InlineData("/%")]
    [InlineData("/%4")]
    [InlineData("/%zz")]
    [InlineData("/%4z")]
    [InlineData("/%00")]
    [InlineData("/%C3")]
    [InlineData("/%FF%FE")]
    public void Rejects_bad_escapes(string raw)
    {
        Assert.False(PercentDecoder.TryDecode(Encoding.Latin1.GetBytes(raw), out _));
    }

    [Fact]
    public void Handles_long_input_via_pool()
    {
        var raw = "/" + string.Concat(Enumerable.Repeat("%41", 1000));
        Assert.True(PercentDecoder.TryDecode(Encoding.ASCII.GetBytes(raw), out var decoded));
        Assert.Equal("/" + new string('A', 1000), decoded);
    }
}

public class HeaderTokensTests
{
    [Theory]
    [InlineData("close", "close", true)]
    [InlineData("Keep-Alive, Upgrade", "upgrade", true)]
    [InlineData(" keep-alive ,close", "close", true)]
    [InlineData("keep-alive", "close", false)]
    [InlineData("", "close", false)]
    [InlineData("closed", "close", false)]
    public void Contains(string value, string token, bool expected)
    {
        Assert.Equal(expected, HeaderTokens.Contains(value, token));
    }

    [Theory]
    [InlineData("\"a\"", "\"a\"", true)]
    [InlineData("W/\"a\"", "\"a\"", true)]
    [InlineData("\"b\", \"a\"", "\"a\"", true)]
    [InlineData("\"A\"", "\"a\"", false)]
    [InlineData("", "\"a\"", false)]
    public void ContainsOrdinal(string value, string token, bool expected)
    {
        Assert.Equal(expected, HeaderTokens.ContainsOrdinal(value, token));
    }

    [Theory]
    [InlineData("0", true, 0)]
    [InlineData("123", true, 123)]
    [InlineData("", false, 0)]
    [InlineData("-1", false, 0)]
    [InlineData("+1", false, 0)]
    [InlineData("1 ", false, 0)]
    [InlineData("1x", false, 0)]
    [InlineData("1234567890123456789", false, 0)]
    public void ParseContentLength(string value, bool ok, long expected)
    {
        Assert.Equal(ok, HeaderTokens.TryParseContentLength(value, out var v));
        Assert.Equal(expected, v);
    }
}

public class HttpDateTests
{
    [Fact]
    public void Current_bytes_are_imf_fixdate()
    {
        var a = Encoding.ASCII.GetString(HttpDate.GetCurrentBytes());
        Assert.Equal(29, a.Length);
        Assert.EndsWith(" GMT", a, StringComparison.Ordinal);
        Assert.True(HttpDate.TryParse(a, out var parsed));
        Assert.True((DateTime.UtcNow - parsed).Duration() < TimeSpan.FromSeconds(5));
        // cached within the same second
        var b = Encoding.ASCII.GetString(HttpDate.GetCurrentBytes());
        Assert.True(a == b || HttpDate.TryParse(b, out _));
    }

    [Fact]
    public void Format_and_parse_roundtrip()
    {
        var dt = new DateTime(2024, 2, 29, 13, 14, 15, DateTimeKind.Utc);
        var text = HttpDate.Format(dt);
        Assert.Equal("Thu, 29 Feb 2024 13:14:15 GMT", text);
        Assert.True(HttpDate.TryParse(text, out var back));
        Assert.Equal(dt, back);
        Assert.Equal(DateTimeKind.Utc, back.Kind);
        Assert.False(HttpDate.TryParse("yesterday", out _));
    }

    [Fact]
    public void Format_converts_local_to_utc()
    {
        var local = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Local);
        Assert.Equal(local.ToUniversalTime().ToString("r", System.Globalization.CultureInfo.InvariantCulture), HttpDate.Format(local));
    }
}

public class HttpReasonPhrasesTests
{
    [Fact]
    public void Known_and_unknown_codes()
    {
        Assert.Equal("OK", HttpReasonPhrases.Get(200));
        Assert.Equal("Not Found", HttpReasonPhrases.Get(404));
        Assert.Equal("HTTP Version Not Supported", HttpReasonPhrases.Get(505));
        Assert.Equal(string.Empty, HttpReasonPhrases.Get(299));
        foreach (int code in new[] { 100, 101, 201, 202, 204, 206, 301, 302, 303, 304, 307, 308, 400, 401, 403, 405, 406, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 426, 429, 431, 500, 501, 502, 503, 504 })
            Assert.NotEqual(string.Empty, HttpReasonPhrases.Get(code));
    }
}
