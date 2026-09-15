namespace Tedd.Quicly.Core.Control;

/// <summary>
/// QUICLY application error codes (PROTOCOL.md §6), used for QUIC CONNECTION_CLOSE / RESET_STREAM /
/// STOP_SENDING and in <see cref="Close.Code"/>, <see cref="BulkCancel.Code"/> and <see cref="BulkReject.Code"/>
/// (which carry the value as a 32-bit field on the wire).
/// </summary>
public enum QuiclyErrorCode : ulong
{
    /// <summary>Orderly close.</summary>
    NoError = 0x00,

    /// <summary>Malformed control frame, malformed message on a persistent ordered stream, second Hello, 0-RTT data, non-minimal varint.</summary>
    ProtocolViolation = 0x01,

    /// <summary>A PROTOCOL.md §7 limit was hit where the rule says "close".</summary>
    LimitExceeded = 0x02,

    /// <summary>A stream preamble names a channel that cannot be carried on a stream, or an unknown channel.</summary>
    UnsupportedChannel = 0x03,

    /// <summary>The HelloAck status was not <see cref="HelloStatus.Accepted"/>.</summary>
    AdmissionRejected = 0x04,

    /// <summary>Admission timeout or heartbeat timeout.</summary>
    Timeout = 0x05,

    /// <summary>A session resume replaced this connection.</summary>
    SessionReplaced = 0x06,

    /// <summary>Implementation fault.</summary>
    InternalError = 0x07,

    /// <summary>A bulk transfer was cancelled by either side.</summary>
    BulkCanceled = 0x10,

    /// <summary>A bulk request was not authorised or named an invalid range.</summary>
    BulkRejected = 0x11,
}
