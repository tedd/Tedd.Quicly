namespace Tedd.Quicly.Http3;

/// <summary>
/// HTTP/3 error codes (RFC 9114 §8.1), QPACK error codes (RFC 9204 §6), the HTTP Datagram error (RFC 9297 §5)
/// and the WebTransport error codes (draft-ietf-webtrans-http3).
/// </summary>
public enum Http3ErrorCode : ulong
{
    /// <summary>H3_DATAGRAM_ERROR (0x33): datagram or capsule protocol parse failure (RFC 9297).</summary>
    DatagramError = 0x33,
    /// <summary>H3_NO_ERROR (0x0100).</summary>
    NoError = 0x0100,
    /// <summary>H3_GENERAL_PROTOCOL_ERROR (0x0101).</summary>
    GeneralProtocolError = 0x0101,
    /// <summary>H3_INTERNAL_ERROR (0x0102).</summary>
    InternalError = 0x0102,
    /// <summary>H3_STREAM_CREATION_ERROR (0x0103).</summary>
    StreamCreationError = 0x0103,
    /// <summary>H3_CLOSED_CRITICAL_STREAM (0x0104).</summary>
    ClosedCriticalStream = 0x0104,
    /// <summary>H3_FRAME_UNEXPECTED (0x0105).</summary>
    FrameUnexpected = 0x0105,
    /// <summary>H3_FRAME_ERROR (0x0106).</summary>
    FrameError = 0x0106,
    /// <summary>H3_EXCESSIVE_LOAD (0x0107).</summary>
    ExcessiveLoad = 0x0107,
    /// <summary>H3_ID_ERROR (0x0108).</summary>
    IdError = 0x0108,
    /// <summary>H3_SETTINGS_ERROR (0x0109).</summary>
    SettingsError = 0x0109,
    /// <summary>H3_MISSING_SETTINGS (0x010a).</summary>
    MissingSettings = 0x010a,
    /// <summary>H3_REQUEST_REJECTED (0x010b).</summary>
    RequestRejected = 0x010b,
    /// <summary>H3_REQUEST_CANCELLED (0x010c).</summary>
    RequestCancelled = 0x010c,
    /// <summary>H3_REQUEST_INCOMPLETE (0x010d).</summary>
    RequestIncomplete = 0x010d,
    /// <summary>H3_MESSAGE_ERROR (0x010e).</summary>
    MessageError = 0x010e,
    /// <summary>H3_CONNECT_ERROR (0x010f).</summary>
    ConnectError = 0x010f,
    /// <summary>H3_VERSION_FALLBACK (0x0110).</summary>
    VersionFallback = 0x0110,
    /// <summary>QPACK_DECOMPRESSION_FAILED (0x0200).</summary>
    QpackDecompressionFailed = 0x0200,
    /// <summary>QPACK_ENCODER_STREAM_ERROR (0x0201).</summary>
    QpackEncoderStreamError = 0x0201,
    /// <summary>QPACK_DECODER_STREAM_ERROR (0x0202).</summary>
    QpackDecoderStreamError = 0x0202,
    /// <summary>WEBTRANSPORT_SESSION_GONE (0x170d7b68).</summary>
    WebTransportSessionGone = 0x170d7b68,
    /// <summary>WEBTRANSPORT_BUFFERED_STREAM_REJECTED (0x3994bd84).</summary>
    WebTransportBufferedStreamRejected = 0x3994bd84,
}
