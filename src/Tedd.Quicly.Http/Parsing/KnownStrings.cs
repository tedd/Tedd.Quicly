using System.Text;

namespace Tedd.Quicly.Http.Parsing;

/// <summary>
/// Interning tables for method names, common header names and a few common header values so that a typical
/// request materialises with as few string allocations as possible. Matching is case-insensitive ASCII and
/// returns the canonical spelling.
/// </summary>
internal static class KnownStrings
{
    public static string InternMethod(ReadOnlySpan<byte> s)
    {
        switch (s.Length)
        {
            case 3:
                if (s.SequenceEqual("GET"u8)) return "GET";
                if (s.SequenceEqual("PUT"u8)) return "PUT";
                break;
            case 4:
                if (s.SequenceEqual("HEAD"u8)) return "HEAD";
                if (s.SequenceEqual("POST"u8)) return "POST";
                break;
            case 5:
                if (s.SequenceEqual("PATCH"u8)) return "PATCH";
                if (s.SequenceEqual("TRACE"u8)) return "TRACE";
                break;
            case 6:
                if (s.SequenceEqual("DELETE"u8)) return "DELETE";
                break;
            case 7:
                if (s.SequenceEqual("OPTIONS"u8)) return "OPTIONS";
                if (s.SequenceEqual("CONNECT"u8)) return "CONNECT";
                break;
        }
        return Encoding.ASCII.GetString(s);
    }

    public static string InternHeaderName(ReadOnlySpan<byte> s)
    {
        switch (s.Length)
        {
            case 4:
                if (Ascii.EqualsIgnoreCase(s, "Host"u8)) return "Host";
                if (Ascii.EqualsIgnoreCase(s, "Date"u8)) return "Date";
                break;
            case 5:
                if (Ascii.EqualsIgnoreCase(s, "Range"u8)) return "Range";
                break;
            case 6:
                if (Ascii.EqualsIgnoreCase(s, "Accept"u8)) return "Accept";
                if (Ascii.EqualsIgnoreCase(s, "Cookie"u8)) return "Cookie";
                if (Ascii.EqualsIgnoreCase(s, "Expect"u8)) return "Expect";
                if (Ascii.EqualsIgnoreCase(s, "Origin"u8)) return "Origin";
                if (Ascii.EqualsIgnoreCase(s, "Pragma"u8)) return "Pragma";
                break;
            case 7:
                if (Ascii.EqualsIgnoreCase(s, "Referer"u8)) return "Referer";
                if (Ascii.EqualsIgnoreCase(s, "Upgrade"u8)) return "Upgrade";
                break;
            case 10:
                if (Ascii.EqualsIgnoreCase(s, "Connection"u8)) return "Connection";
                if (Ascii.EqualsIgnoreCase(s, "User-Agent"u8)) return "User-Agent";
                if (Ascii.EqualsIgnoreCase(s, "Keep-Alive"u8)) return "Keep-Alive";
                break;
            case 12:
                if (Ascii.EqualsIgnoreCase(s, "Content-Type"u8)) return "Content-Type";
                break;
            case 13:
                if (Ascii.EqualsIgnoreCase(s, "Authorization"u8)) return "Authorization";
                if (Ascii.EqualsIgnoreCase(s, "Cache-Control"u8)) return "Cache-Control";
                if (Ascii.EqualsIgnoreCase(s, "If-None-Match"u8)) return "If-None-Match";
                break;
            case 14:
                if (Ascii.EqualsIgnoreCase(s, "Content-Length"u8)) return "Content-Length";
                if (Ascii.EqualsIgnoreCase(s, "Accept-Charset"u8)) return "Accept-Charset";
                break;
            case 15:
                if (Ascii.EqualsIgnoreCase(s, "Accept-Encoding"u8)) return "Accept-Encoding";
                if (Ascii.EqualsIgnoreCase(s, "Accept-Language"u8)) return "Accept-Language";
                if (Ascii.EqualsIgnoreCase(s, "X-Forwarded-For"u8)) return "X-Forwarded-For";
                break;
            case 17:
                if (Ascii.EqualsIgnoreCase(s, "Transfer-Encoding"u8)) return "Transfer-Encoding";
                if (Ascii.EqualsIgnoreCase(s, "If-Modified-Since"u8)) return "If-Modified-Since";
                break;
            case 25:
                if (Ascii.EqualsIgnoreCase(s, "Upgrade-Insecure-Requests"u8)) return "Upgrade-Insecure-Requests";
                break;
        }
        return Encoding.ASCII.GetString(s);
    }

    public static string InternHeaderValue(ReadOnlySpan<byte> s)
    {
        switch (s.Length)
        {
            case 0:
                return string.Empty;
            case 1:
                if (s[0] == (byte)'*') return "*";
                if (s[0] == (byte)'1') return "1";
                if (s[0] == (byte)'0') return "0";
                break;
            case 3:
                if (s.SequenceEqual("*/*"u8)) return "*/*";
                break;
            case 5:
                if (Ascii.EqualsIgnoreCase(s, "close"u8)) return "close";
                break;
            case 7:
                if (Ascii.EqualsIgnoreCase(s, "chunked"u8)) return "chunked";
                break;
            case 10:
                if (Ascii.EqualsIgnoreCase(s, "keep-alive"u8)) return "keep-alive";
                break;
            case 12:
                if (Ascii.EqualsIgnoreCase(s, "100-continue"u8)) return "100-continue";
                break;
            case 17:
                if (s.SequenceEqual("gzip, deflate, br"u8)) return "gzip, deflate, br";
                break;
        }
        return Encoding.Latin1.GetString(s);
    }
}
