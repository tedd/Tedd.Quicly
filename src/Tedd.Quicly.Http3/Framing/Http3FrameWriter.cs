namespace Tedd.Quicly.Http3;

/// <summary>Writes HTTP/3 frame headers (type varint + length varint) and unidirectional stream preambles.</summary>
public static class Http3FrameWriter
{
    /// <summary>Largest possible frame header (two 8-byte varints).</summary>
    public const int MaxHeaderLength = 2 * Http3VarInt.MaxLength;

    /// <summary>Returns the encoded size of a frame header, or -1 when either value exceeds the varint range.</summary>
    public static int GetHeaderLength(ulong type, ulong payloadLength)
    {
        int a = Http3VarInt.GetLength(type);
        int b = Http3VarInt.GetLength(payloadLength);
        if (a < 0 || b < 0) return -1;
        return a + b;
    }

    /// <summary>Returns the encoded size of a frame header for a known frame type.</summary>
    public static int GetHeaderLength(Http3FrameType type, ulong payloadLength) => GetHeaderLength((ulong)type, payloadLength);

    /// <summary>
    /// Writes a frame header. Returns the number of bytes written, or -1 when <paramref name="destination"/> is too
    /// small or a value exceeds the varint range.
    /// </summary>
    public static int WriteHeader(Span<byte> destination, ulong type, ulong payloadLength)
    {
        int a = Http3VarInt.Write(destination, type);
        if (a < 0) return -1;
        int b = Http3VarInt.Write(destination.Slice(a), payloadLength);
        if (b < 0) return -1;
        return a + b;
    }

    /// <summary>Writes a frame header for a known frame type. See <see cref="WriteHeader(Span{byte}, ulong, ulong)"/>.</summary>
    public static int WriteHeader(Span<byte> destination, Http3FrameType type, ulong payloadLength) => WriteHeader(destination, (ulong)type, payloadLength);

    /// <summary>
    /// Writes a complete frame (header followed by <paramref name="payload"/>).
    /// Returns the number of bytes written or -1 when <paramref name="destination"/> is too small.
    /// </summary>
    public static int WriteFrame(Span<byte> destination, ulong type, ReadOnlySpan<byte> payload)
    {
        int h = WriteHeader(destination, type, (ulong)payload.Length);
        if (h < 0 || destination.Length - h < payload.Length) return -1;
        payload.CopyTo(destination.Slice(h));
        return h + payload.Length;
    }

    /// <summary>Writes a complete frame for a known frame type. See <see cref="WriteFrame(Span{byte}, ulong, ReadOnlySpan{byte})"/>.</summary>
    public static int WriteFrame(Span<byte> destination, Http3FrameType type, ReadOnlySpan<byte> payload) => WriteFrame(destination, (ulong)type, payload);

    /// <summary>
    /// Writes the stream-type varint that starts every unidirectional stream (RFC 9114 §6.2).
    /// Returns the number of bytes written or -1 when <paramref name="destination"/> is too small.
    /// </summary>
    public static int WriteStreamType(Span<byte> destination, Http3StreamType type) => Http3VarInt.Write(destination, (ulong)type);

    /// <summary>
    /// Reads the stream-type varint that starts a unidirectional stream. Returns false when more bytes are needed.
    /// </summary>
    public static bool TryReadStreamType(ReadOnlySpan<byte> source, out Http3StreamType type, out int consumed)
    {
        bool ok = Http3VarInt.TryRead(source, out ulong raw, out consumed);
        type = (Http3StreamType)raw;
        return ok;
    }
}
