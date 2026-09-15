namespace Tedd.Quicly.Core.Control;

/// <summary>Ping (type 0x01, PROTOCOL.md §2.3).</summary>
/// <param name="TimeMicros">The sender's monotonic microseconds relative to its connection start, truncated to 32 bits.</param>
public readonly record struct Ping(uint TimeMicros);

/// <summary>Pong (type 0x02, PROTOCOL.md §2.3), the answer to a <see cref="Ping"/>.</summary>
/// <param name="EchoedTimeMicros">The <see cref="Ping.TimeMicros"/> being answered.</param>
/// <param name="ReceiveTimeMicros">The responder's relative microseconds when the Ping was received.</param>
/// <param name="SendTimeMicros">The responder's relative microseconds when this Pong was sent.</param>
public readonly record struct Pong(uint EchoedTimeMicros, uint ReceiveTimeMicros, uint SendTimeMicros);

/// <summary>One entry of a LatestAck batch (type 0x03, PROTOCOL.md §2.3): the highest version accepted for a key.</summary>
/// <param name="Channel">Channel id, 2–16383.</param>
/// <param name="Key">Key, at most 2^62 − 1.</param>
/// <param name="Version">Cumulative: every version up to this one (serial arithmetic) is covered.</param>
public readonly record struct LatestAckEntry(ushort Channel, ulong Key, uint Version);

/// <summary>One entry of a LatestReject batch (type 0x04, PROTOCOL.md §2.3): a version the receiver dropped locally.</summary>
/// <param name="Channel">Channel id, 2–16383.</param>
/// <param name="Key">Key, at most 2^62 − 1.</param>
/// <param name="Version">The dropped version.</param>
/// <param name="Reason">Why it was dropped.</param>
public readonly record struct LatestRejectEntry(ushort Channel, ulong Key, uint Version, LatestRejectReason Reason);

/// <summary>BulkProgress (type 0x05, PROTOCOL.md §2.3).</summary>
/// <param name="TransferId">The transfer, at most 2^62 − 1.</param>
/// <param name="BytesAccepted">Bytes of the transfer accepted so far, at most 2^62 − 1.</param>
public readonly record struct BulkProgress(ulong TransferId, ulong BytesAccepted);

/// <summary>BulkRequest (type 0x13, PROTOCOL.md §3.4). Every field except <paramref name="Channel"/> is a varint (at most 2^62 − 1).</summary>
/// <param name="RequestId">Request id chosen by the requester.</param>
/// <param name="Channel">Bulk channel id, 2–16383.</param>
/// <param name="ObjectId">Requested object.</param>
/// <param name="ObjectVersion">Requested object version.</param>
/// <param name="Offset">First byte of the requested range.</param>
/// <param name="Length">Length of the requested range; <c>Offset + Length</c> must not exceed 2^62 − 1.</param>
public readonly record struct BulkRequest(ulong RequestId, ushort Channel, ulong ObjectId, ulong ObjectVersion, ulong Offset, ulong Length);

/// <summary>BulkCancel (type 0x14, PROTOCOL.md §3.4).</summary>
/// <param name="TransferId">The transfer to cancel, at most 2^62 − 1.</param>
/// <param name="Code">Error code; carried as a 32-bit field, so at most <see cref="uint.MaxValue"/>.</param>
public readonly record struct BulkCancel(ulong TransferId, QuiclyErrorCode Code);

/// <summary>BulkReject (type 0x15, PROTOCOL.md §3.4).</summary>
/// <param name="RequestId">The rejected request, at most 2^62 − 1.</param>
/// <param name="Code">Error code; carried as a 32-bit field, so at most <see cref="uint.MaxValue"/>.</param>
public readonly record struct BulkReject(ulong RequestId, QuiclyErrorCode Code);

/// <summary>KeyRetired (type 0x17, PROTOCOL.md §3.4): the sender will not use this key again in this epoch.</summary>
/// <param name="Channel">Channel id, 2–16383.</param>
/// <param name="Key">The retired key, at most 2^62 − 1.</param>
public readonly record struct KeyRetired(ushort Channel, ulong Key);
