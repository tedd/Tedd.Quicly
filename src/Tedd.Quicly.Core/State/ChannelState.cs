using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.State;

/// <summary>Flags in <see cref="ChannelSendState.Flags"/>. Game thread.</summary>
[Flags]
public enum ChannelSendFlags : uint
{
    /// <summary>No flags.</summary>
    None = 0,
    /// <summary>The channel's ordered stream has been opened (<see cref="ChannelSendState.Stream"/> is valid).</summary>
    StreamOpen = 1,
    /// <summary>The next stream send must carry the Start flag.</summary>
    StreamStartPending = 2,
    /// <summary>An <see cref="SendEntryFlags.Immediate"/> send asked for a flush of this channel.</summary>
    FlushRequested = 4,
    /// <summary>The peer's stream limit blocked the last open; wait for streams-available.</summary>
    Blocked = 8,
}

/// <summary>Flags in <see cref="ChannelRecvState.Flags"/>. Transport thread.</summary>
[Flags]
public enum ChannelRecvFlags : uint
{
    /// <summary>No flags.</summary>
    None = 0,
    /// <summary>The peer's ordered stream for this channel is open.</summary>
    StreamOpen = 1,
    /// <summary>Receive on the stream is pended (ring full); resume from Poll.</summary>
    Pended = 2,
    /// <summary>The first message on an unkeyed sequenced channel has been accepted (<see cref="ChannelRecvState.LastAccepted"/> is valid).</summary>
    HasAccepted = 4,
}

/// <summary>
/// Per-channel send-side hot state: exactly one cache line, native memory, <b>game thread only</b>.
/// Counters live in <see cref="ChannelSendCounters"/> (own array, own cache line) so a statistics snapshot never
/// touches this line.
/// </summary>
/// <remarks>
/// Layout: <see cref="NextSequence"/> (0), <see cref="QueueHead"/> (4), <see cref="QueueTail"/> (8),
/// <see cref="QueueCount"/> (12), <see cref="QueueBytes"/> (16), <see cref="Stream"/> (24),
/// <see cref="CurrentGroup"/> (32), <see cref="InFlight"/> (36), <see cref="Flags"/> (40),
/// <see cref="RetryPending"/> (44), <see cref="NextDeadlineMicros"/> (48), <see cref="BudgetBytes"/> (56).
/// </remarks>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct ChannelSendState
{
    /// <summary>Size in bytes.</summary>
    public const int Size = 64;

    /// <summary>Sequence number assigned to the next message.</summary>
    [FieldOffset(0)] public uint NextSequence;
    /// <summary>First queued <see cref="SendEntry"/> slot (-1 = empty); entries chain through <see cref="SendEntryTable.Next"/>.</summary>
    [FieldOffset(4)] public int QueueHead;
    /// <summary>Last queued slot (-1 = empty).</summary>
    [FieldOffset(8)] public int QueueTail;
    /// <summary>Number of queued entries.</summary>
    [FieldOffset(12)] public int QueueCount;
    /// <summary>Payload bytes queued (checked against the channel's queue limit).</summary>
    [FieldOffset(16)] public long QueueBytes;
    /// <summary>The channel's ordered stream, when open.</summary>
    [FieldOffset(24)] public TransportStreamId Stream;
    /// <summary>Group being filled (group-stream channels), or -1.</summary>
    [FieldOffset(32)] public int CurrentGroup;
    /// <summary>Entries published to the transport and not yet completed.</summary>
    [FieldOffset(36)] public int InFlight;
    /// <summary>Flags.</summary>
    [FieldOffset(40)] public ChannelSendFlags Flags;
    /// <summary>Keys with a retry scheduled (ReliableLatest).</summary>
    [FieldOffset(44)] public int RetryPending;
    /// <summary>Earliest retry/expiry deadline among the channel's entries, in clock micros (0 = none).</summary>
    [FieldOffset(48)] public long NextDeadlineMicros;
    /// <summary>Bandwidth token-bucket balance in bytes.</summary>
    [FieldOffset(56)] public long BudgetBytes;
}

/// <summary>
/// Per-channel receive-side hot state: one cache line, native memory, <b>transport thread only</b> (the current
/// callback). Counters live in <see cref="ChannelRecvCounters"/>.
/// </summary>
/// <remarks>
/// Layout: <see cref="LastAccepted"/> (0), <see cref="HighestSeen"/> (4), <see cref="Stream"/> (8),
/// <see cref="Reassemblies"/> (16), <see cref="ActiveGroups"/> (20), <see cref="LastReceiveMicros"/> (24),
/// <see cref="Parser"/> (32), <see cref="Flags"/> (36), <see cref="PendingAcks"/> (40),
/// <see cref="PendingAckVersion"/> (44), <see cref="StagedBytes"/> (48), reserved (56).
/// </remarks>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct ChannelRecvState
{
    /// <summary>Size in bytes.</summary>
    public const int Size = 64;

    /// <summary>Last accepted sequence for unkeyed sequenced channels.</summary>
    [FieldOffset(0)] public uint LastAccepted;
    /// <summary>Highest sequence seen (statistics / reorder detection).</summary>
    [FieldOffset(4)] public uint HighestSeen;
    /// <summary>The peer's ordered stream for this channel, when open.</summary>
    [FieldOffset(8)] public TransportStreamId Stream;
    /// <summary>Fragment reassemblies in progress.</summary>
    [FieldOffset(16)] public int Reassemblies;
    /// <summary>Peer group streams currently open on this channel.</summary>
    [FieldOffset(20)] public int ActiveGroups;
    /// <summary>Clock micros of the last accepted message.</summary>
    [FieldOffset(24)] public long LastReceiveMicros;
    /// <summary>Index of the channel's stream-frame parser in the stream table, or -1.</summary>
    [FieldOffset(32)] public int Parser;
    /// <summary>Flags.</summary>
    [FieldOffset(36)] public ChannelRecvFlags Flags;
    /// <summary>Latest-acks queued for coalesced sending.</summary>
    [FieldOffset(40)] public int PendingAcks;
    /// <summary>Version of the most recent queued ack (unkeyed ReliableLatest).</summary>
    [FieldOffset(44)] public uint PendingAckVersion;
    /// <summary>Bytes held in staging leases (partial stream messages, reassembly).</summary>
    [FieldOffset(48)] public long StagedBytes;
}

/// <summary>
/// Send-side counters of one channel: eight <see cref="long"/>s in one cache line, incremented only by the game
/// thread and snapshotted into statistics (ADR 0008 invariant 13). Kept apart from <see cref="ChannelSendState"/>
/// so the two never share a line with receive-side data.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct ChannelSendCounters
{
    /// <summary>Size in bytes.</summary>
    public const int Size = 64;

    /// <summary>Messages handed to the transport.</summary>
    [FieldOffset(0)] public long Sent;
    /// <summary>Payload bytes handed to the transport.</summary>
    [FieldOffset(8)] public long Bytes;
    /// <summary>Pending messages replaced by a newer value of the same key.</summary>
    [FieldOffset(16)] public long Superseded;
    /// <summary>Messages dropped because their expiry elapsed before sending.</summary>
    [FieldOffset(24)] public long Expired;
    /// <summary>Sends rejected because the channel queue or the send table was full.</summary>
    [FieldOffset(32)] public long QueueFull;
    /// <summary>Sends rejected because the message exceeded the channel's size limit.</summary>
    [FieldOffset(40)] public long TooLarge;
    /// <summary>Retransmissions (ReliableLatest).</summary>
    [FieldOffset(48)] public long Retries;
    /// <summary>Sends rejected because the key table was full.</summary>
    [FieldOffset(56)] public long KeyTableFull;
}

/// <summary>
/// Receive-side counters of one channel: eight <see cref="long"/>s in one cache line, incremented only by the
/// transport thread.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct ChannelRecvCounters
{
    /// <summary>Size in bytes.</summary>
    public const int Size = 64;

    /// <summary>Messages accepted.</summary>
    [FieldOffset(0)] public long Received;
    /// <summary>Payload bytes accepted.</summary>
    [FieldOffset(8)] public long Bytes;
    /// <summary>Messages dropped as stale (sequence/version not newer) or malformed.</summary>
    [FieldOffset(16)] public long Dropped;
    /// <summary>Mailbox values replaced before the game thread saw them.</summary>
    [FieldOffset(24)] public long Superseded;
    /// <summary>Messages dropped because the receive ring was full.</summary>
    [FieldOffset(32)] public long RingDrops;
    /// <summary>Messages dropped because the key table was full.</summary>
    [FieldOffset(40)] public long KeyTableFull;
    /// <summary>Messages dropped because they exceeded a size limit.</summary>
    [FieldOffset(48)] public long TooLarge;
    /// <summary>Messages dropped because no receive buffer was available.</summary>
    [FieldOffset(56)] public long OutOfBuffers;
}
