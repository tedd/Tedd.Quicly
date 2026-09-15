namespace Tedd.Quicly.Core.Channels;

/// <summary>Delivery mode of an application channel (PROTOCOL.md §1). The numeric values are wire values.</summary>
public enum ChannelMode : byte
{
    /// <summary>May be lost, may arrive in any order. Carried in QUIC DATAGRAM frames.</summary>
    UnreliableUnordered = 0,

    /// <summary>May be lost; only messages newer than the last accepted one (per key) are delivered. DATAGRAM frames with a 16/32-bit sequence.</summary>
    UnreliableSequenced = 1,

    /// <summary>Every message, in order, on one persistent unidirectional stream per channel and direction.</summary>
    ReliableOrdered = 2,

    /// <summary>Every message; one unidirectional stream per flush group, groups are independent.</summary>
    ReliableUnordered = 3,

    /// <summary>Only the latest version per key matters; versioned DATAGRAMs with acks and retries, large values on group streams.</summary>
    ReliableLatest = 4,

    /// <summary>Large objects, one stream per transfer, with progress, cancel and resume.</summary>
    Bulk = 5,
}

/// <summary>Payload compression codec of a channel (PROTOCOL.md §1). Values 2 and 3 are reserved.</summary>
public enum ChannelCompression : byte
{
    /// <summary>Payloads are never compressed.</summary>
    None = 0,

    /// <summary>Payloads of at least <see cref="ChannelDefinition.MinCompressSize"/> bytes may be sent as an LZ4 block.</summary>
    Lz4 = 1,
}
