using System.Buffers.Binary;
using System.Text.Unicode;

namespace Tedd.Quicly.Http3;

/// <summary>Capsule types (RFC 9297 §3.2, draft-ietf-webtrans-http3).</summary>
public enum CapsuleType : ulong
{
    /// <summary>DATAGRAM (0x00): an HTTP Datagram carried on the stream when QUIC datagrams are unavailable.</summary>
    Datagram = 0x00,
    /// <summary>CLOSE_WEBTRANSPORT_SESSION (0x2843): u32 application error code (big-endian) + UTF-8 reason (at most 1024 bytes).</summary>
    CloseWebTransportSession = 0x2843,
    /// <summary>DRAIN_WEBTRANSPORT_SESSION (0x78ae): empty payload.</summary>
    DrainWebTransportSession = 0x78ae,
}

/// <summary>Writes capsules (type varint, length varint, payload) onto a CONNECT stream's DATA payload.</summary>
public static class CapsuleWriter
{
    /// <summary>Maximum reason length in the CLOSE_WEBTRANSPORT_SESSION capsule.</summary>
    public const int MaxCloseReasonLength = 1024;

    /// <summary>Encoded size of a capsule with the given payload length, or -1 when out of range.</summary>
    public static int GetLength(ulong type, ulong payloadLength)
    {
        int h = Http3FrameWriter.GetHeaderLength(type, payloadLength);
        return h < 0 ? -1 : h + (int)payloadLength;
    }

    /// <summary>Writes a capsule header. Returns bytes written or -1 when the destination is too small.</summary>
    public static int WriteHeader(Span<byte> destination, ulong type, ulong payloadLength) => Http3FrameWriter.WriteHeader(destination, type, payloadLength);

    /// <summary>Writes a capsule header for a known type. Returns bytes written or -1 when the destination is too small.</summary>
    public static int WriteHeader(Span<byte> destination, CapsuleType type, ulong payloadLength) => WriteHeader(destination, (ulong)type, payloadLength);

    /// <summary>Writes a complete capsule. Returns bytes written or -1 when the destination is too small.</summary>
    public static int Write(Span<byte> destination, ulong type, ReadOnlySpan<byte> payload) => Http3FrameWriter.WriteFrame(destination, type, payload);

    /// <summary>Writes a complete capsule of a known type. Returns bytes written or -1 when the destination is too small.</summary>
    public static int Write(Span<byte> destination, CapsuleType type, ReadOnlySpan<byte> payload) => Write(destination, (ulong)type, payload);

    /// <summary>
    /// Writes a CLOSE_WEBTRANSPORT_SESSION capsule. Returns bytes written, or -1 when the destination is too small
    /// or <paramref name="reason"/> is longer than <see cref="MaxCloseReasonLength"/> bytes.
    /// </summary>
    public static int WriteCloseSession(Span<byte> destination, uint errorCode, ReadOnlySpan<byte> reason)
    {
        if (reason.Length > MaxCloseReasonLength) return -1;
        int h = WriteHeader(destination, CapsuleType.CloseWebTransportSession, (ulong)(4 + reason.Length));
        if (h < 0 || destination.Length - h < 4 + reason.Length) return -1;
        BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(h), errorCode);
        reason.CopyTo(destination.Slice(h + 4));
        return h + 4 + reason.Length;
    }

    /// <summary>Writes a DRAIN_WEBTRANSPORT_SESSION capsule. Returns bytes written or -1 when the destination is too small.</summary>
    public static int WriteDrainSession(Span<byte> destination) => WriteHeader(destination, CapsuleType.DrainWebTransportSession, 0);
}

/// <summary>
/// Reads capsules. For contiguous data use <see cref="TryRead"/>; for data arriving in chunks keep an
/// <see cref="Http3FrameReader"/> (the wire shape is identical) and interpret its frames as capsules.
/// </summary>
public static class CapsuleReader
{
    /// <summary>
    /// Parses one complete capsule from the start of <paramref name="source"/>. Returns false when the buffer
    /// does not hold a complete capsule.
    /// </summary>
    public static bool TryRead(ReadOnlySpan<byte> source, out ulong type, out ReadOnlySpan<byte> payload, out int consumed) =>
        Http3FrameReader.TryReadFrame(source, out type, out payload, out consumed);

    /// <summary>
    /// Parses the payload of a CLOSE_WEBTRANSPORT_SESSION capsule. Returns false when the payload is shorter than 4
    /// bytes, the reason exceeds <see cref="CapsuleWriter.MaxCloseReasonLength"/> bytes or is not valid UTF-8.
    /// </summary>
    public static bool TryParseCloseSession(ReadOnlySpan<byte> payload, out uint errorCode, out ReadOnlySpan<byte> reason)
    {
        errorCode = 0;
        reason = default;
        if (payload.Length < 4) return false;
        ReadOnlySpan<byte> r = payload.Slice(4);
        if (r.Length > CapsuleWriter.MaxCloseReasonLength) return false;
        if (!Utf8.IsValid(r)) return false;
        errorCode = BinaryPrimitives.ReadUInt32BigEndian(payload);
        reason = r;
        return true;
    }
}
