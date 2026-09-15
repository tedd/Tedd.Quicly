namespace Tedd.Quicly.Http3;

/// <summary>
/// HTTP Datagram framing (RFC 9297 §2.1): every QUIC DATAGRAM carrying HTTP data starts with the Quarter Stream
/// ID (the associated client-initiated bidirectional stream id divided by 4) as a varint.
/// </summary>
public static class HttpDatagram
{
    /// <summary>True when <paramref name="streamId"/> is a client-initiated bidirectional stream (the only kind that can own datagrams).</summary>
    public static bool IsValidStreamId(ulong streamId) => (streamId & 0x3) == 0 && streamId <= Http3VarInt.MaxValue;

    /// <summary>Returns the Quarter Stream ID for <paramref name="streamId"/>.</summary>
    public static ulong ToQuarterStreamId(ulong streamId) => streamId >> 2;

    /// <summary>Returns the stream id for <paramref name="quarterStreamId"/>.</summary>
    public static ulong FromQuarterStreamId(ulong quarterStreamId) => quarterStreamId << 2;

    /// <summary>Encoded size of the prefix for <paramref name="streamId"/>, or -1 when the stream id is invalid.</summary>
    public static int GetPrefixLength(ulong streamId) => IsValidStreamId(streamId) ? Http3VarInt.GetLength(streamId >> 2) : -1;

    /// <summary>
    /// Writes the Quarter Stream ID prefix for <paramref name="streamId"/>. Returns bytes written, or -1 when the
    /// stream id is not a client-initiated bidirectional stream or the destination is too small.
    /// </summary>
    public static int WritePrefix(Span<byte> destination, ulong streamId)
    {
        if (!IsValidStreamId(streamId)) return -1;
        return Http3VarInt.Write(destination, streamId >> 2);
    }

    /// <summary>
    /// Writes a complete HTTP Datagram (prefix + payload). Returns bytes written or -1 on invalid stream id or
    /// insufficient space.
    /// </summary>
    public static int Write(Span<byte> destination, ulong streamId, ReadOnlySpan<byte> payload)
    {
        int n = WritePrefix(destination, streamId);
        if (n < 0 || destination.Length - n < payload.Length) return -1;
        payload.CopyTo(destination.Slice(n));
        return n + payload.Length;
    }

    /// <summary>
    /// Parses a received HTTP Datagram. Returns false when the prefix is truncated (H3_DATAGRAM_ERROR).
    /// </summary>
    public static bool TryRead(ReadOnlySpan<byte> datagram, out ulong streamId, out ReadOnlySpan<byte> payload)
    {
        if (!Http3VarInt.TryRead(datagram, out ulong quarter, out int n))
        {
            streamId = 0;
            payload = default;
            return false;
        }
        streamId = quarter << 2;
        payload = datagram.Slice(n);
        return true;
    }
}
