namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Result of a <see cref="ControlCodec"/> read or parse. Every value other than <see cref="Ok"/> and
/// <see cref="NeedMoreData"/> means the frame is malformed: on the control stream that is a connection-level
/// <see cref="QuiclyErrorCode.ProtocolViolation"/>, for a control datagram the datagram is dropped and counted
/// (PROTOCOL.md §6). <see cref="UnsupportedVersion"/> is the exception for Hello: the server answers with
/// <see cref="HelloStatus.VersionMismatch"/>.
/// </summary>
public enum ControlParseStatus : byte
{
    /// <summary>The input is well-formed.</summary>
    Ok = 0,

    /// <summary>
    /// Stream form only: the input holds an incomplete but so far valid frame; retry once more bytes have
    /// arrived. Not an error.
    /// </summary>
    NeedMoreData = 1,

    /// <summary>A field extends past the end of the frame or body.</summary>
    Truncated = 2,

    /// <summary>Bytes remain after the last field of the message.</summary>
    TrailingBytes = 3,

    /// <summary>A varint uses more bytes than its value needs (PROTOCOL.md preamble).</summary>
    NonMinimalVarint = 4,

    /// <summary>Datagram form: the datagram does not start with channel 0.</summary>
    NotControlChannel = 5,

    /// <summary>The type byte is not a defined control type (0x06–0x0F are reserved).</summary>
    UnknownType = 6,

    /// <summary>The type is defined but not allowed in this carrier (a stream-only type in a control datagram).</summary>
    TypeNotAllowed = 7,

    /// <summary>Stream form: the <c>Length</c> field is outside [1, 16384].</summary>
    InvalidFrameLength = 8,

    /// <summary>Hello does not start with the magic <c>"QLCY"</c>.</summary>
    InvalidMagic = 9,

    /// <summary>
    /// Hello announces a protocol version other than <see cref="ControlCodec.ProtocolVersion"/>. Only
    /// <see cref="Hello.Version"/> is set; the rest of the body is not interpreted.
    /// </summary>
    UnsupportedVersion = 10,

    /// <summary>A length-prefixed field exceeds its bound (token 4 096, reason 512, channel name 64 bytes).</summary>
    FieldTooLong = 11,

    /// <summary>A reason or channel name is not valid UTF-8.</summary>
    InvalidUtf8 = 12,

    /// <summary>A channel id field is outside [2, 16383].</summary>
    InvalidChannel = 13,

    /// <summary>An entry count is larger than the remaining bytes can possibly hold.</summary>
    CountTooLarge = 14,

    /// <summary>
    /// A field holds a value outside its defined set (undefined HelloAck status or reject reason, a
    /// <c>tableIncluded</c> byte other than 0/1, a bulk range whose end exceeds 2^62 − 1).
    /// </summary>
    InvalidValue = 15,
}
