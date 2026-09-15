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
