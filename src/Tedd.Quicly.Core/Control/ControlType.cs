namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Type byte of a control message (PROTOCOL.md §2.3 and §3.4). Types 0x01–0x05 may be carried in a control
/// datagram or as a control-stream message; types 0x10–0x17 are control-stream only. 0x06–0x0F are reserved.
/// </summary>
public enum ControlType : byte
{
    /// <summary>Clock/RTT probe (<see cref="Control.Ping"/>).</summary>
    Ping = 0x01,

    /// <summary>Answer to a <see cref="Ping"/> (<see cref="Control.Pong"/>).</summary>
    Pong = 0x02,

    /// <summary>Cumulative ReliableLatest acknowledgements (<see cref="LatestAckBatchReader"/>, <see cref="LatestAckBatchWriter"/>).</summary>
    LatestAck = 0x03,

    /// <summary>ReliableLatest versions dropped locally by the receiver (<see cref="LatestRejectBatchReader"/>, <see cref="LatestRejectBatchWriter"/>).</summary>
    LatestReject = 0x04,

    /// <summary>Bulk transfer progress (<see cref="Control.BulkProgress"/>).</summary>
    BulkProgress = 0x05,

    /// <summary>Client handshake (<see cref="Control.Hello"/>); stream only, client to server.</summary>
    Hello = 0x10,

    /// <summary>Server handshake answer (<see cref="Control.HelloAck"/>); stream only, server to client.</summary>
    HelloAck = 0x11,

    /// <summary>Orderly or error close (<see cref="Control.Close"/>); stream only.</summary>
    Close = 0x12,

    /// <summary>Request for a bulk object range (<see cref="Control.BulkRequest"/>); stream only.</summary>
    BulkRequest = 0x13,

    /// <summary>Cancels a bulk transfer (<see cref="Control.BulkCancel"/>); stream only.</summary>
    BulkCancel = 0x14,

    /// <summary>Rejects a bulk request (<see cref="Control.BulkReject"/>); stream only.</summary>
    BulkReject = 0x15,

    /// <summary>Asks the server for its channel table (empty body); stream only, client to server.</summary>
    ChannelTableRequest = 0x16,

    /// <summary>The sender will not use a key again in this epoch (<see cref="Control.KeyRetired"/>); stream only.</summary>
    KeyRetired = 0x17,
}
