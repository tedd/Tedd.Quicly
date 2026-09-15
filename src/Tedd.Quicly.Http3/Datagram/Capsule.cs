using System.Buffers.Binary;
using System.Text.Unicode;

namespace Tedd.Quicly.Http3;

/// <summary>Capsule types (RFC 9297 §3.2, draft-ietf-webtrans-http3 §4.5 and §5).</summary>
/// <remarks>
/// The WebTransport flow-control capsule codepoints (<c>0x190B4D3D..0x190B4D44</c>) are those of
/// draft-ietf-webtrans-http3-09 and later, matching the <c>0xc671706a</c> SETTINGS family pinned by
/// docs/PROTOCOL.md §5. Every flow-control capsule carries a single varint payload.
/// </remarks>
public enum CapsuleType : ulong
{
    /// <summary>DATAGRAM (0x00): an HTTP Datagram carried on the stream when QUIC datagrams are unavailable.</summary>
    Datagram = 0x00,
    /// <summary>CLOSE_WEBTRANSPORT_SESSION (0x2843): u32 application error code (big-endian) + UTF-8 reason (at most 1024 bytes).</summary>
    CloseWebTransportSession = 0x2843,
    /// <summary>DRAIN_WEBTRANSPORT_SESSION (0x78ae): empty payload.</summary>
    DrainWebTransportSession = 0x78ae,
    /// <summary>WT_MAX_DATA (0x190B4D3D): raises the session-level data limit (varint, bytes).</summary>
    WebTransportMaxData = 0x190B4D3D,
    /// <summary>WT_MAX_STREAMS for bidirectional streams (0x190B4D3F): cumulative stream limit (varint, at most 2^60).</summary>
    WebTransportMaxStreamsBidi = 0x190B4D3F,
    /// <summary>WT_MAX_STREAMS for unidirectional streams (0x190B4D40): cumulative stream limit (varint, at most 2^60).</summary>
    WebTransportMaxStreamsUni = 0x190B4D40,
    /// <summary>WT_DATA_BLOCKED (0x190B4D41): the sender is blocked at this session data limit (varint, bytes).</summary>
    WebTransportDataBlocked = 0x190B4D41,
    /// <summary>WT_STREAMS_BLOCKED for bidirectional streams (0x190B4D43): blocked at this stream limit (varint, at most 2^60).</summary>
    WebTransportStreamsBlockedBidi = 0x190B4D43,
    /// <summary>WT_STREAMS_BLOCKED for unidirectional streams (0x190B4D44): blocked at this stream limit (varint, at most 2^60).</summary>
    WebTransportStreamsBlockedUni = 0x190B4D44,
}

/// <summary>Helpers for <see cref="CapsuleType"/> values.</summary>
public static class CapsuleTypeExtensions
{
    /// <summary>True for the six WebTransport flow-control capsules, whose payload is a single varint.</summary>
    public static bool IsWebTransportFlowControl(this CapsuleType type)
    {
        switch (type)
        {
            case CapsuleType.WebTransportMaxData:
            case CapsuleType.WebTransportMaxStreamsBidi:
            case CapsuleType.WebTransportMaxStreamsUni:
            case CapsuleType.WebTransportDataBlocked:
            case CapsuleType.WebTransportStreamsBlockedBidi:
            case CapsuleType.WebTransportStreamsBlockedUni:
                return true;
            default:
                return false;
        }
    }

    /// <summary>True for the four stream-count capsules (WT_MAX_STREAMS_*, WT_STREAMS_BLOCKED_*), whose value may not exceed 2^60.</summary>
    public static bool IsWebTransportStreamCount(this CapsuleType type)
    {
        switch (type)
        {
            case CapsuleType.WebTransportMaxStreamsBidi:
            case CapsuleType.WebTransportMaxStreamsUni:
            case CapsuleType.WebTransportStreamsBlockedBidi:
            case CapsuleType.WebTransportStreamsBlockedUni:
                return true;
            default:
                return false;
        }
    }
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

    /// <summary>
    /// Writes one of the WebTransport flow-control capsules (WT_MAX_DATA, WT_MAX_STREAMS_*, WT_DATA_BLOCKED,
    /// WT_STREAMS_BLOCKED_*), whose payload is <paramref name="value"/> as a single varint. Returns bytes written,
    /// or -1 when <paramref name="type"/> is not a flow-control capsule, the value is not encodable (above 2^62-1,
    /// or above 2^60 for the stream-count capsules) or the destination is too small.
    /// </summary>
    public static int WriteFlowControl(Span<byte> destination, CapsuleType type, ulong value)
    {
        if (!type.IsWebTransportFlowControl()) return -1;
        if (type.IsWebTransportStreamCount() && value > Http3Settings.MaxStreamCredit) return -1;
        int valueLength = Http3VarInt.GetLength(value);
        if (valueLength < 0) return -1;
        int h = WriteHeader(destination, type, (ulong)valueLength);
        if (h < 0) return -1;
        int v = Http3VarInt.Write(destination.Slice(h), value);
        if (v < 0) return -1;
        return h + v;
    }

    /// <summary>Encoded size of a flow-control capsule for <paramref name="value"/>, or -1 when it cannot be written (see <see cref="WriteFlowControl"/>).</summary>
    public static int GetFlowControlLength(CapsuleType type, ulong value)
    {
        if (!type.IsWebTransportFlowControl()) return -1;
        if (type.IsWebTransportStreamCount() && value > Http3Settings.MaxStreamCredit) return -1;
        int valueLength = Http3VarInt.GetLength(value);
        return valueLength < 0 ? -1 : GetLength((ulong)type, (ulong)valueLength);
    }
}

/// <summary>
/// Reads capsules. For contiguous data use <see cref="TryRead"/>; for data arriving in chunks keep an
/// <see cref="Http3FrameReader"/> (the wire shape is identical) and interpret its frames as capsules.
/// </summary>
public static class CapsuleReader
{
    /// <summary>
    /// The largest capsule payload a QUICLY endpoint accepts on a CONNECT stream (docs/PROTOCOL.md §5: capsules
    /// ≤ 32 KiB). Pass it as the <c>maxPayloadLength</c> of the <see cref="Http3FrameReader"/> that parses the
    /// CONNECT stream's DATA payload; a larger capsule is a connection error (H3_EXCESSIVE_LOAD).
    /// </summary>
    public const ulong MaxCapsuleLength = 32 * 1024;

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

    /// <summary>
    /// Parses the payload of a WebTransport flow-control capsule (a single varint). Returns false when
    /// <paramref name="type"/> is not a flow-control capsule, the payload is not exactly one varint (truncated or
    /// trailing bytes) or a stream-count value exceeds 2^60 — all H3_GENERAL_PROTOCOL_ERROR / H3_FRAME_ERROR
    /// conditions for the caller to act on.
    /// </summary>
    public static bool TryParseFlowControl(CapsuleType type, ReadOnlySpan<byte> payload, out ulong value)
    {
        value = 0;
        if (!type.IsWebTransportFlowControl()) return false;
        if (!Http3VarInt.TryRead(payload, out ulong v, out int n) || n != payload.Length) return false;
        if (type.IsWebTransportStreamCount() && v > Http3Settings.MaxStreamCredit) return false;
        value = v;
        return true;
    }
}
