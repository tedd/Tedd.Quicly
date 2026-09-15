using System.Buffers;

namespace Tedd.Quicly.Http.Parsing;

/// <summary>Outcome of <see cref="HttpParser.TryParse"/>.</summary>
internal enum HttpParseStatus : byte
{
    /// <summary>The buffer does not yet contain a complete request line + header section.</summary>
    NeedMore,

    /// <summary>A complete, well-formed header section was parsed.</summary>
    Ok,

    /// <summary>Malformed input (400).</summary>
    Invalid,

    /// <summary>The request line exceeds the configured limit (414).</summary>
    RequestLineTooLong,

    /// <summary>The header section exceeds the configured byte or count limit (431).</summary>
    HeadersTooLarge,

    /// <summary>The request line is well-formed but names an HTTP version other than 1.0 / 1.1 (505).</summary>
    VersionNotSupported,
}

/// <summary>Byte ranges of one header field inside the request buffer.</summary>
internal struct HeaderRange
{
    public int NameStart;
    public int NameLength;
    public int ValueStart;
    public int ValueLength;
}

/// <summary>
/// Structural parse result: only offsets into the request buffer, no strings. Pre-allocate once per connection
/// and reuse; <see cref="HttpParser.TryParse"/> never allocates.
/// </summary>
internal struct ParsedRequest
{
    public int MethodStart;
    public int MethodLength;
    public int TargetStart;
    public int TargetLength;
    public HttpProtocolVersion Version;
    public HeaderRange[] Headers;
    public int HeaderCount;

    /// <summary>Total number of bytes consumed (through the CRLF that ends the header section).</summary>
    public int Consumed;

    public ParsedRequest(int maxHeaderCount)
    {
        Headers = new HeaderRange[maxHeaderCount];
    }
}

/// <summary>
/// Allocation-free HTTP/1.x request-line and header-section parser (RFC 9112). It validates token characters,
/// rejects bare LF, obsolete line folding, whitespace before the colon and control characters in values.
/// </summary>
internal static class HttpParser
{
    /// <summary>The byte sequence that terminates the header section when preceded by a CRLF-terminated line.</summary>
    public static ReadOnlySpan<byte> HeaderTerminator => "\n\r\n"u8;

    // tchar = "!" / "#" / "$" / "%" / "&" / "'" / "*" / "+" / "-" / "." / "^" / "_" / "`" / "|" / "~" / DIGIT / ALPHA
    private static readonly SearchValues<byte> TokenChars = SearchValues.Create(
        "!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"u8);

    // Request-target: any visible ASCII except SP, plus obs-text (0x80-0xFF) for leniency.
    private static readonly SearchValues<byte> TargetChars = SearchValues.Create(BuildTargetChars());

    // field-value bytes that are never allowed: CTLs other than HTAB, and DEL.
    private static readonly SearchValues<byte> InvalidValueChars = SearchValues.Create(BuildInvalidValueChars());

    private static byte[] BuildTargetChars()
    {
        var bytes = new byte[(0x7E - 0x21 + 1) + (0xFF - 0x80 + 1)];
        int n = 0;
        for (int b = 0x21; b <= 0x7E; b++)
            bytes[n++] = (byte)b;
        for (int b = 0x80; b <= 0xFF; b++)
            bytes[n++] = (byte)b;
        return bytes;
    }

    private static byte[] BuildInvalidValueChars()
    {
        var bytes = new byte[32];
        int n = 0;
        for (int b = 0; b < 0x20; b++)
        {
            if (b != '\t')
                bytes[n++] = (byte)b;
        }
        bytes[n++] = 0x7F;
        return bytes[..n];
    }

    /// <summary>
    /// Parses the request line and header section at the start of <paramref name="input"/>.
    /// </summary>
    /// <param name="input">Buffered bytes from the connection.</param>
    /// <param name="maxRequestLineBytes">Maximum request-line length excluding CRLF.</param>
    /// <param name="maxHeadersBytes">Maximum size of the header section (all header lines including the terminating CRLF).</param>
    /// <param name="result">Receives offsets; <see cref="ParsedRequest.Headers"/> must be pre-allocated and bounds the header count.</param>
    public static HttpParseStatus TryParse(ReadOnlySpan<byte> input, int maxRequestLineBytes, int maxHeadersBytes, ref ParsedRequest result)
    {
        int pos = 0;

        // RFC 9112 §2.2: ignore at least one empty line received before the request-line.
        while (input.Length - pos >= 2 && input[pos] == (byte)'\r' && input[pos + 1] == (byte)'\n')
            pos += 2;

        // ---- request line ----
        var window = input.Slice(pos, Math.Min(input.Length - pos, maxRequestLineBytes + 2));
        int lf = window.IndexOf((byte)'\n');
        if (lf < 0)
            return window.Length >= maxRequestLineBytes + 2 ? HttpParseStatus.RequestLineTooLong : HttpParseStatus.NeedMore;
        if (lf == 0 || window[lf - 1] != (byte)'\r')
            return HttpParseStatus.Invalid;

        var line = window[..(lf - 1)];
        int sp = line.IndexOf((byte)' ');
        if (sp <= 0 || line[..sp].ContainsAnyExcept(TokenChars))
            return HttpParseStatus.Invalid;
        result.MethodStart = pos;
        result.MethodLength = sp;

        var rest = line[(sp + 1)..];
        int sp2 = rest.IndexOf((byte)' ');
        if (sp2 <= 0 || rest[..sp2].ContainsAnyExcept(TargetChars))
            return HttpParseStatus.Invalid;
        result.TargetStart = pos + sp + 1;
        result.TargetLength = sp2;

        var version = rest[(sp2 + 1)..];
        if (version.Length != 8 || !version.StartsWith("HTTP/"u8) || !IsDigit(version[5]) || version[6] != (byte)'.' || !IsDigit(version[7]))
            return HttpParseStatus.Invalid;
        if (version[5] == (byte)'1' && version[7] == (byte)'1')
            result.Version = HttpProtocolVersion.Http11;
        else if (version[5] == (byte)'1' && version[7] == (byte)'0')
            result.Version = HttpProtocolVersion.Http10;
        else
            return HttpParseStatus.VersionNotSupported;

        pos += lf + 1;

        // ---- header section ----
        int headersStart = pos;
        int headersEndLimit = headersStart + maxHeadersBytes; // exclusive upper bound of bytes we may scan
        result.HeaderCount = 0;
        var headers = result.Headers;

        while (true)
        {
            int windowEnd = Math.Min(input.Length, headersEndLimit);
            if (windowEnd < pos)
                return HttpParseStatus.HeadersTooLarge;
            var hwindow = input[pos..windowEnd];
            int hlf = hwindow.IndexOf((byte)'\n');
            if (hlf < 0)
                return input.Length - headersStart >= maxHeadersBytes ? HttpParseStatus.HeadersTooLarge : HttpParseStatus.NeedMore;
            if (hlf == 0 || hwindow[hlf - 1] != (byte)'\r')
                return HttpParseStatus.Invalid;

            var hline = hwindow[..(hlf - 1)];
            int lineStart = pos;
            pos += hlf + 1;

            if (hline.Length == 0)
            {
                result.Consumed = pos;
                return HttpParseStatus.Ok;
            }

            // obs-fold (a line starting with whitespace) is rejected outright.
            if (hline[0] == (byte)' ' || hline[0] == (byte)'\t')
                return HttpParseStatus.Invalid;

            int colon = hline.IndexOf((byte)':');
            if (colon <= 0 || hline[..colon].ContainsAnyExcept(TokenChars))
                return HttpParseStatus.Invalid;

            int valueStart = colon + 1;
            int valueEnd = hline.Length;
            while (valueStart < valueEnd && (hline[valueStart] == (byte)' ' || hline[valueStart] == (byte)'\t'))
                valueStart++;
            while (valueEnd > valueStart && (hline[valueEnd - 1] == (byte)' ' || hline[valueEnd - 1] == (byte)'\t'))
                valueEnd--;
            if (hline[valueStart..valueEnd].ContainsAny(InvalidValueChars))
                return HttpParseStatus.Invalid;

            if (result.HeaderCount == headers.Length)
                return HttpParseStatus.HeadersTooLarge;

            ref var h = ref headers[result.HeaderCount++];
            h.NameStart = lineStart;
            h.NameLength = colon;
            h.ValueStart = lineStart + valueStart;
            h.ValueLength = valueEnd - valueStart;
        }
    }

    private static bool IsDigit(byte b) => (uint)(b - '0') <= 9;
}
