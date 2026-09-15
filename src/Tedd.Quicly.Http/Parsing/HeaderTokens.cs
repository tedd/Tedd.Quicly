namespace Tedd.Quicly.Http.Parsing;

/// <summary>Helpers for comma-separated header token lists (<c>Connection</c>, <c>If-None-Match</c>, ...).</summary>
internal static class HeaderTokens
{
    /// <summary>Whether the comma-separated list <paramref name="value"/> contains <paramref name="token"/> (case-insensitive, whitespace ignored).</summary>
    public static bool Contains(string value, string token)
    {
        ReadOnlySpan<char> s = value;
        while (!s.IsEmpty)
        {
            int comma = s.IndexOf(',');
            ReadOnlySpan<char> item = comma < 0 ? s : s[..comma];
            item = item.Trim();
            if (item.Equals(token, StringComparison.OrdinalIgnoreCase))
                return true;
            if (comma < 0)
                break;
            s = s[(comma + 1)..];
        }
        return false;
    }

    /// <summary>Whether the comma-separated list <paramref name="value"/> contains <paramref name="token"/> compared ordinally (for entity tags).</summary>
    public static bool ContainsOrdinal(string value, string token)
    {
        ReadOnlySpan<char> s = value;
        while (!s.IsEmpty)
        {
            int comma = s.IndexOf(',');
            ReadOnlySpan<char> item = comma < 0 ? s : s[..comma];
            item = item.Trim();
            if (item.StartsWith("W/", StringComparison.Ordinal))
                item = item[2..];
            if (item.Equals(token, StringComparison.Ordinal))
                return true;
            if (comma < 0)
                break;
            s = s[(comma + 1)..];
        }
        return false;
    }

    private static readonly System.Buffers.SearchValues<char> TokenChars = System.Buffers.SearchValues.Create(
        "!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

    /// <summary>Whether <paramref name="value"/> is a non-empty RFC 9110 token (method names, header names).</summary>
    public static bool IsToken(string value) => value.Length > 0 && !value.AsSpan().ContainsAnyExcept(TokenChars);

    /// <summary>
    /// Whether <paramref name="value"/> can be written verbatim into a header value or reason phrase: no control
    /// characters other than HTAB, no DEL, nothing above U+00FF (the wire encoding is Latin-1).
    /// </summary>
    public static bool IsFieldValue(string value)
    {
        foreach (char c in value)
        {
            if ((c < 0x20 && c != '\t') || c == 0x7F || c > 0xFF)
                return false;
        }
        return true;
    }

    /// <summary>Parses a Content-Length value: digits only, no sign, no whitespace, fits in a non-negative <see cref="long"/>.</summary>
    public static bool TryParseContentLength(string value, out long length)
    {
        length = 0;
        if (value.Length == 0 || value.Length > 18)
            return false;
        long v = 0;
        foreach (char c in value)
        {
            if ((uint)(c - '0') > 9)
                return false;
            v = v * 10 + (c - '0');
        }
        length = v;
        return true;
    }
}
