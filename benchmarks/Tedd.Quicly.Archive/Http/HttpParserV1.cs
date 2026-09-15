namespace Tedd.Quicly.Archive.Http;

/// <summary>Outcome of <see cref="HttpParserV1.TryParse"/> (mirror of the shipping enum so the archive stays self-contained).</summary>
public enum HttpParseStatusV1 : byte
{
    NeedMore,
    Ok,
    Invalid,
    RequestLineTooLong,
    HeadersTooLarge,
    VersionNotSupported,
}

/// <summary>Byte ranges of one header field inside the request buffer.</summary>
public struct HeaderRangeV1
{
    public int NameStart;
    public int NameLength;
    public int ValueStart;
    public int ValueLength;
}

/// <summary>Structural parse result, offsets only.</summary>
public struct ParsedRequestV1
{
    public int MethodStart;
    public int MethodLength;
    public int TargetStart;
    public int TargetLength;
    public byte Version; // 0 = HTTP/1.0, 1 = HTTP/1.1
    public HeaderRangeV1[] Headers;
    public int HeaderCount;
    public int Consumed;

    public ParsedRequestV1(int maxHeaderCount)
    {
        Headers = new HeaderRangeV1[maxHeaderCount];
    }
}

/// <summary>
/// Experiment V1 (docs/benchmarks/http.md): a single-pass, table-driven HTTP/1.x request parser. Every byte is
/// classified once through a 256-entry flag table (tchar / target char / field-value char) in a scalar loop, so no
/// line is scanned twice. Compared against V0 (the shipping parser), which finds line ends with vectorised
/// <c>IndexOf</c> and validates each slice with <c>SearchValues</c> - i.e. two to three vectorised passes per line.
/// </summary>
public static class HttpParserV1
{
    private const byte Token = 1;
    private const byte TargetChar = 2;
    private const byte ValueChar = 4;
    private const int MaxLeadingEmptyLines = 2;

    private static readonly byte[] Class = BuildTable();

    private static byte[] BuildTable()
    {
        var t = new byte[256];
        foreach (char c in "!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
            t[c] |= Token;
        for (int b = 0x21; b <= 0x7E; b++)
            t[b] |= TargetChar;
        for (int b = 0x80; b <= 0xFF; b++)
            t[b] |= TargetChar | ValueChar;
        for (int b = 0x20; b <= 0x7E; b++)
            t[b] |= ValueChar;
        t['\t'] |= ValueChar;
        return t;
    }

    public static HttpParseStatusV1 TryParse(ReadOnlySpan<byte> input, int maxRequestLineBytes, int maxHeadersBytes, ref ParsedRequestV1 result)
    {
        var table = Class;
        int pos = 0;
        int length = input.Length;

        for (int skipped = 0; skipped < MaxLeadingEmptyLines && length - pos >= 2 && input[pos] == (byte)'\r' && input[pos + 1] == (byte)'\n'; skipped++)
            pos += 2;

        // ---- request line ----
        int lineStart = pos;
        int lineLimit = Math.Min(length, lineStart + maxRequestLineBytes + 2);

        // method
        int i = pos;
        while (i < lineLimit && (table[input[i]] & Token) != 0)
            i++;
        if (i == lineLimit)
            return lineLimit - lineStart >= maxRequestLineBytes + 2 ? HttpParseStatusV1.RequestLineTooLong : HttpParseStatusV1.NeedMore;
        if (i == pos || input[i] != (byte)' ')
            return HttpParseStatusV1.Invalid;
        result.MethodStart = pos;
        result.MethodLength = i - pos;
        i++;

        // target
        int targetStart = i;
        while (i < lineLimit && (table[input[i]] & TargetChar) != 0)
            i++;
        if (i == lineLimit)
            return lineLimit - lineStart >= maxRequestLineBytes + 2 ? HttpParseStatusV1.RequestLineTooLong : HttpParseStatusV1.NeedMore;
        if (i == targetStart || input[i] != (byte)' ')
            return HttpParseStatusV1.Invalid;
        result.TargetStart = targetStart;
        result.TargetLength = i - targetStart;
        i++;

        // version + CRLF: exactly "HTTP/d.d\r\n"
        if (lineLimit - i < 10)
        {
            // Might still be incomplete, unless the line already exceeds the limit or a stray LF proves it malformed.
            int lf = input[i..lineLimit].IndexOf((byte)'\n');
            if (lf >= 0)
                return HttpParseStatusV1.Invalid;
            return lineLimit - lineStart >= maxRequestLineBytes + 2 ? HttpParseStatusV1.RequestLineTooLong : HttpParseStatusV1.NeedMore;
        }
        if (input[i] != (byte)'H' || input[i + 1] != (byte)'T' || input[i + 2] != (byte)'T' || input[i + 3] != (byte)'P' || input[i + 4] != (byte)'/'
            || !IsDigit(input[i + 5]) || input[i + 6] != (byte)'.' || !IsDigit(input[i + 7]) || input[i + 8] != (byte)'\r' || input[i + 9] != (byte)'\n')
            return HttpParseStatusV1.Invalid;
        if (input[i + 5] == (byte)'1' && input[i + 7] == (byte)'1')
            result.Version = 1;
        else if (input[i + 5] == (byte)'1' && input[i + 7] == (byte)'0')
            result.Version = 0;
        else
            return HttpParseStatusV1.VersionNotSupported;
        pos = i + 10;
        if (pos - lineStart - 2 > maxRequestLineBytes)
            return HttpParseStatusV1.RequestLineTooLong;

        // ---- header section ----
        int headersStart = pos;
        int headersLimit = Math.Min(length, headersStart + maxHeadersBytes);
        result.HeaderCount = 0;
        var headers = result.Headers;

        while (true)
        {
            if (headersLimit - pos < 2)
                return length - headersStart >= maxHeadersBytes ? HttpParseStatusV1.HeadersTooLarge : HttpParseStatusV1.NeedMore;
            if (input[pos] == (byte)'\r')
            {
                if (input[pos + 1] != (byte)'\n')
                    return HttpParseStatusV1.Invalid;
                result.Consumed = pos + 2;
                return HttpParseStatusV1.Ok;
            }

            // name
            int nameStart = pos;
            i = pos;
            while (i < headersLimit && (table[input[i]] & Token) != 0)
                i++;
            if (i == headersLimit)
                return length - headersStart >= maxHeadersBytes ? HttpParseStatusV1.HeadersTooLarge : HttpParseStatusV1.NeedMore;
            if (i == nameStart || input[i] != (byte)':')
                return HttpParseStatusV1.Invalid;
            int nameLength = i - nameStart;
            i++;

            // value: OWS, then value chars, then OWS, then CRLF
            while (i < headersLimit && (input[i] == (byte)' ' || input[i] == (byte)'\t'))
                i++;
            int valueStart = i;
            while (i < headersLimit && (table[input[i]] & ValueChar) != 0)
                i++;
            if (headersLimit - i < 2)
                return length - headersStart >= maxHeadersBytes ? HttpParseStatusV1.HeadersTooLarge : HttpParseStatusV1.NeedMore;
            if (input[i] != (byte)'\r' || input[i + 1] != (byte)'\n')
                return HttpParseStatusV1.Invalid;
            int valueEnd = i;
            while (valueEnd > valueStart && (input[valueEnd - 1] == (byte)' ' || input[valueEnd - 1] == (byte)'\t'))
                valueEnd--;

            if (result.HeaderCount == headers.Length)
                return HttpParseStatusV1.HeadersTooLarge;
            ref var h = ref headers[result.HeaderCount++];
            h.NameStart = nameStart;
            h.NameLength = nameLength;
            h.ValueStart = valueStart;
            h.ValueLength = valueEnd - valueStart;
            pos = i + 2;
        }
    }

    private static bool IsDigit(byte b) => (uint)(b - '0') <= 9;
}
