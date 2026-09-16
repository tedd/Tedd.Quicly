namespace Tedd.Quicly.Core.Framing;

/// <summary>
/// Outcome of a QUICLY frame parser. Every parser in this namespace reports malformed input through this enum and
/// never throws. <see cref="ControlChannel"/> and <see cref="ContainerChannel"/> are not errors: they tell a datagram
/// parser's caller to dispatch the datagram elsewhere.
/// </summary>
public enum ParseStatus : byte
{
    /// <summary>The frame was parsed.</summary>
    Ok = 0,

    /// <summary>The input ended inside a header (or a container entry / table field).</summary>
    Truncated,

    /// <summary>A varint was not minimally encoded (PROTOCOL.md: protocol error).</summary>
    NonMinimalVarint,

    /// <summary>The channel id is not in the table (or is ≥ 16384).</summary>
    UnknownChannel,

    /// <summary>A datagram names a channel whose mode is carried only on streams.</summary>
    ChannelNotDatagram,

    /// <summary>A stream preamble names channel 1, a datagram-only channel, or channel 0 on a unidirectional stream.</summary>
    ChannelNotStream,

    /// <summary>Not an error: the datagram is a control datagram (channel 0); the payload offset points at <c>Type</c>.</summary>
    ControlChannel,

    /// <summary>Not an error: the datagram is a packed container (channel 1); parse it with <see cref="PackedContainer.TryParse"/>.</summary>
    ContainerChannel,

    /// <summary><c>FragCount</c> outside 1…8, <c>FragIndex</c> ≥ <c>FragCount</c>, or an empty fragment.</summary>
    BadFragment,

    /// <summary><c>RawLength</c> exceeds the effective maximum message size.</summary>
    RawLengthTooLarge,

    /// <summary>The (uncompressed) payload or declared length exceeds the effective maximum message size or chunk size.</summary>
    MessageTooLarge,

    /// <summary>A packed container holds another packed container.</summary>
    NestedContainer,

    /// <summary>
    /// A length field is inconsistent: zero where ≥ 1 is required, larger than the remaining bytes, a compressed
    /// payload that is not shorter than its <c>RawLength</c>, a control frame length outside 1…16384, or bytes after
    /// the end of a bulk body.
    /// </summary>
    BadLength,

    /// <summary>A packed container holds no message.</summary>
    ContainerEmpty,

    /// <summary>A second message in a ReliableLatest group stream (a container's message count is bounded only by its length).</summary>
    TooManyMessages,

    /// <summary>Reserved flag bits are set (container flags, bulk flags, unsupported bulk hash algorithm).</summary>
    BadFlags,

    /// <summary>A value does not fit its field's range (container tick or request id above 2^32−1, ReliableLatest group id above 2^32−1).</summary>
    ValueOutOfRange,

    /// <summary>A ReliableLatest group stream message's sequence differs from the stream's group id (the version).</summary>
    SequenceMismatch,

    /// <summary>A stream preamble is valid but names a channel of another stream kind than the parser was reset for.</summary>
    RoleMismatch,

    /// <summary>A bulk header has <c>Length</c> = 0 or <c>Offset + Length &gt; TotalLength</c>.</summary>
    BadBulkRange,

    /// <summary><see cref="PackedContainer.TryParse"/> was given a datagram that does not start with channel 1.</summary>
    NotContainer,
}
