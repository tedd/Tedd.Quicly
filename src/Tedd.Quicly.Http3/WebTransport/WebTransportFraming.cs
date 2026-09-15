namespace Tedd.Quicly.Http3.WebTransport;

/// <summary>Outcome of a WebTransport stream preamble read.</summary>
public enum WebTransportPreambleStatus : byte
{
    /// <summary>The preamble was read; the session id is valid.</summary>
    Ok = 0,
    /// <summary>More bytes are needed.</summary>
    NeedMoreData,
    /// <summary>The first varint is not the expected WebTransport signal value; the stream belongs to something else.</summary>
    NotWebTransport,
    /// <summary>The session id is not a client-initiated bidirectional stream id (H3_ID_ERROR).</summary>
    InvalidSessionId,
}

/// <summary>
/// WebTransport stream preambles (draft-ietf-webtrans-http3 §4.2): a unidirectional stream starts with the stream
/// type 0x54 followed by the session id (the CONNECT stream id) as a varint; a bidirectional stream starts with
/// the signal value 0x41 followed by the session id.
/// </summary>
public static class WebTransportFraming
{
    /// <summary>Signal value opening a unidirectional WebTransport stream (the stream type 0x54).</summary>
    public const ulong UnidirectionalSignal = (ulong)Http3StreamType.WebTransport;

    /// <summary>Signal value opening a bidirectional WebTransport stream (the frame type 0x41).</summary>
    public const ulong BidirectionalSignal = (ulong)Http3FrameType.WebTransportStream;

    /// <summary>True when <paramref name="sessionId"/> is a client-initiated bidirectional stream id.</summary>
    public static bool IsValidSessionId(ulong sessionId) => HttpDatagram.IsValidStreamId(sessionId);

    /// <summary>Encoded size of a preamble for <paramref name="sessionId"/>, or -1 when the session id is invalid.</summary>
    public static int GetPreambleLength(ulong sessionId, bool bidirectional)
    {
        if (!IsValidSessionId(sessionId)) return -1;
        return Http3VarInt.GetLength(bidirectional ? BidirectionalSignal : UnidirectionalSignal) + Http3VarInt.GetLength(sessionId);
    }

    /// <summary>Writes <c>0x54, sessionId</c>. Returns bytes written or -1 on invalid session id / insufficient space.</summary>
    public static int WriteUnidirectionalPreamble(Span<byte> destination, ulong sessionId) => WritePreamble(destination, UnidirectionalSignal, sessionId);

    /// <summary>Writes <c>0x41, sessionId</c>. Returns bytes written or -1 on invalid session id / insufficient space.</summary>
    public static int WriteBidirectionalPreamble(Span<byte> destination, ulong sessionId) => WritePreamble(destination, BidirectionalSignal, sessionId);

    /// <summary>Reads a unidirectional preamble (<c>0x54, sessionId</c>) from the start of a stream.</summary>
    public static WebTransportPreambleStatus TryReadUnidirectionalPreamble(ReadOnlySpan<byte> source, out ulong sessionId, out int consumed) =>
        TryReadPreamble(source, UnidirectionalSignal, out sessionId, out consumed);

    /// <summary>Reads a bidirectional preamble (<c>0x41, sessionId</c>) from the start of a stream.</summary>
    public static WebTransportPreambleStatus TryReadBidirectionalPreamble(ReadOnlySpan<byte> source, out ulong sessionId, out int consumed) =>
        TryReadPreamble(source, BidirectionalSignal, out sessionId, out consumed);

    /// <summary>
    /// Reads the session id that follows an already-consumed signal value (for callers that dispatched on the
    /// stream type themselves).
    /// </summary>
    public static WebTransportPreambleStatus TryReadSessionId(ReadOnlySpan<byte> source, out ulong sessionId, out int consumed)
    {
        if (!Http3VarInt.TryRead(source, out sessionId, out consumed)) return WebTransportPreambleStatus.NeedMoreData;
        if (!IsValidSessionId(sessionId))
        {
            consumed = 0;
            return WebTransportPreambleStatus.InvalidSessionId;
        }
        return WebTransportPreambleStatus.Ok;
    }

    private static int WritePreamble(Span<byte> destination, ulong signal, ulong sessionId)
    {
        if (!IsValidSessionId(sessionId)) return -1;
        int a = Http3VarInt.Write(destination, signal);
        if (a < 0) return -1;
        int b = Http3VarInt.Write(destination.Slice(a), sessionId);
        if (b < 0) return -1;
        return a + b;
    }

    private static WebTransportPreambleStatus TryReadPreamble(ReadOnlySpan<byte> source, ulong signal, out ulong sessionId, out int consumed)
    {
        sessionId = 0;
        consumed = 0;
        if (!Http3VarInt.TryRead(source, out ulong first, out int a)) return WebTransportPreambleStatus.NeedMoreData;
        if (first != signal) return WebTransportPreambleStatus.NotWebTransport;
        WebTransportPreambleStatus s = TryReadSessionId(source.Slice(a), out sessionId, out int b);
        if (s == WebTransportPreambleStatus.Ok) consumed = a + b;
        return s;
    }
}

/// <summary>
/// Mapping between 32-bit WebTransport application error codes and the HTTP/3 error-code space
/// (draft-ietf-webtrans-http3 §4.3): <c>h3 = 0x52e4a40fa8db + n + floor(n / 0x1e)</c>.
/// </summary>
public static class WebTransportErrorCode
{
    private const ulong First = 0x52e4a40fa8db;
    private const ulong Last = 0x52e5ac983162;

    /// <summary>Converts a WebTransport application error code to its HTTP/3 error code.</summary>
    public static ulong ToHttp3(uint applicationErrorCode) => First + applicationErrorCode + applicationErrorCode / 0x1e;

    /// <summary>
    /// Converts an HTTP/3 error code back to a WebTransport application error code. Returns false when the value
    /// is outside the WebTransport range or lands on one of the reserved grease slots.
    /// </summary>
    public static bool TryFromHttp3(ulong http3ErrorCode, out uint applicationErrorCode)
    {
        applicationErrorCode = 0;
        if (http3ErrorCode < First || http3ErrorCode > Last) return false;
        ulong shifted = http3ErrorCode - First;
        if (shifted % 0x1f == 0x1e) return false;
        applicationErrorCode = (uint)(shifted - shifted / 0x1f);
        return true;
    }
}
