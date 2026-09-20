using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>How the payload of a <see cref="SendRequest"/> is supplied (ARCHITECTURE.md §4.1).</summary>
internal enum SendPayloadKind : byte
{
    /// <summary><see cref="SendRequest.Source"/> is copied into a slab lease now.</summary>
    Copy = 0,

    /// <summary><see cref="SendRequest.Lease"/> (rented from the peer) holds <see cref="SendRequest.Length"/> bytes; ownership moves on admission.</summary>
    Owned = 1,

    /// <summary><see cref="SendRequest.Pointer"/> is pinned or native memory valid until the BufferReleased completion.</summary>
    Pinned = 2,

    /// <summary><see cref="SendRequest.Borrowed"/> is pinned by the engine (the handle is kept in the entry's pin side table).</summary>
    Borrowed = 3,

    /// <summary><see cref="SendRequest.Gather"/> lists existing payload pages.</summary>
    Gather = 4,

    /// <summary>
    /// <see cref="SendRequest.Shared"/> is a reference-counted lease of <see cref="SendRequest.SharedTable"/>: the entry
    /// takes one reference when admission commits and releases it when its payload is released (zero copy, never
    /// compressed; <see cref="QuiclyPeer.SendShared"/>).
    /// </summary>
    Shared = 5,
}

/// <summary>
/// One admission request handed from a public <c>Send*</c> call to the owning engine (game thread). The peer has already
/// resolved the channel and checked that the session is <see cref="PeerState.Connected"/>; the engine validates size,
/// key and queue limits, fills a send entry and queues it. It never calls the transport (that happens in
/// <see cref="ChannelEngine.Flush"/>) except for <see cref="SendMode.Immediate"/> sends, which it may flush at the end of
/// the call. On any status other than <see cref="SendStatus.Admitted"/> the caller keeps ownership of the payload.
/// </summary>
internal unsafe ref struct SendRequest
{
    /// <summary>The channel.</summary>
    public ChannelDefinition Channel;

    /// <summary>Dense index of the channel in the table (counters, handlers, <see cref="PeerCore.SendCounters"/>).</summary>
    public int ChannelIndex;

    /// <summary>Key (keyed channels).</summary>
    public ulong Key;

    /// <summary>Caller options.</summary>
    public SendOptions Options;

    /// <summary>How the payload is supplied.</summary>
    public SendPayloadKind Kind;

    /// <summary><see cref="SendPayloadKind.Copy"/>: the bytes to copy.</summary>
    public ReadOnlySpan<byte> Source;

    /// <summary><see cref="SendPayloadKind.Owned"/>: the lease.</summary>
    public BufferLease Lease;

    /// <summary>Payload length in bytes (every kind but <see cref="SendPayloadKind.Gather"/>).</summary>
    public int Length;

    /// <summary><see cref="SendPayloadKind.Pinned"/>: the payload.</summary>
    public byte* Pointer;

    /// <summary><see cref="SendPayloadKind.Borrowed"/>: the payload.</summary>
    public ReadOnlyMemory<byte> Borrowed;

    /// <summary><see cref="SendPayloadKind.Gather"/>: the pages (at most <see cref="QuiclyPeer.MaxGatherSegments"/>).</summary>
    public ReadOnlySpan<BufferLease> Gather;

    /// <summary><see cref="SendPayloadKind.Shared"/>: the table that counts the lease's references.</summary>
    public SharedLeaseTable? SharedTable;

    /// <summary><see cref="SendPayloadKind.Shared"/>: the shared payload.</summary>
    public SharedLease Shared;

    /// <summary>Request id for request/response channels (0 = plain message).</summary>
    public uint RequestId;

    /// <summary>Set by the engine for a tracked, admitted send (<see cref="PeerCore.TryTrack"/>).</summary>
    public SendToken Token;
}

/// <summary>Per-flush inputs and outputs shared by the engines of one peer (game thread).</summary>
internal struct FlushContext
{
    /// <summary>Clock micros of this flush (read once).</summary>
    public long NowMicros;

    /// <summary>The host's tick passed to <see cref="QuiclyPeer.Flush"/> (container Tick field; 0 = none).</summary>
    public uint Tick;

    /// <summary>Current maximum datagram payload (read at pack time, never cached from the handshake).</summary>
    public int MaxDatagramPayload;

    /// <summary>Whether datagrams can be sent right now.</summary>
    public bool DatagramsEnabled;

    /// <summary>Earliest time-driven deadline so far (clock micros, <see cref="long.MaxValue"/> = none); engines lower it.</summary>
    public long NextDeadline;

    /// <summary>
    /// Bytes the send cap (<see cref="PeerOptions.MaxSendBytesPerSecond"/>, a token bucket) still allows in this pass;
    /// <see cref="long.MaxValue"/> without a cap. Whoever hands bytes to the transport checks that it is positive first
    /// and subtracts what it takes: the last message of a pass may overdraw, and later refills repay the debt. The packer
    /// does this for datagrams; a stream engine does it for its stream sends.
    /// </summary>
    public long BudgetBytes;

    /// <summary>Set by whoever held work back because <see cref="BudgetBytes"/> ran out; the scheduler then lowers <see cref="NextDeadline"/> to the refill time.</summary>
    public bool BudgetExhausted;

    /// <summary>Bytes handed to the transport in this pass (datagrams and stream data); charged to the send cap when the pass ends.</summary>
    public long BytesSubmitted;

    /// <summary>
    /// Unreliable datagrams may carry <see cref="TransportSendFlags.CancelOnBlocked"/> in this pass (PROTOCOL.md §4.5
    /// <c>DropWhenBlocked</c>): true only when the transport reports that it honours the flag.
    /// </summary>
    public bool CancelBlockedDatagrams;
}

/// <summary>Kind of completion carried by a <see cref="CompletionEntry"/>.</summary>
internal enum CompletionKind : byte
{
    /// <summary><c>OnStreamSendCompleted</c>.</summary>
    Stream = 0,

    /// <summary><c>OnDatagramSendStateChanged</c>.</summary>
    Datagram = 1,

    /// <summary>
    /// Queued by the game thread itself (<see cref="PeerCore.QueueLocalCompletion"/>): the entry was never handed to the
    /// transport, or the transport refused it (expiry at scheduling time, a cancellation, a refused submission).
    /// <see cref="CompletionEntry.Status"/> says how it ends.
    /// </summary>
    Local = 2,
}

/// <summary>
/// A completion of one send entry, drained on the game thread and routed to the entry's owner
/// (<see cref="ChannelEngine.OnSendCompleted"/>, the packer's fan-out, or the peer's control traffic). Transport
/// completions come through the completion ring: per entry at most one non-final <see cref="DatagramSendState.Sent"/>
/// notice (for every datagram entry, tracked or not, so its payload block is released at <c>Sent</c> — ADR 0008
/// invariant 1) and exactly one final completion; the ring holds <c>2 × send table</c> items — two per entry — so it
/// cannot overflow. <see cref="CompletionKind.Local"/> completions are queued by the
/// game thread itself (<see cref="PeerCore.QueueLocalCompletion"/>) and are always final. A member of a packed container
/// receives a copy of its container's completion.
/// </summary>
internal struct CompletionEntry
{
    /// <summary>Send entry slot.</summary>
    public int Slot;

    /// <summary>The entry generation the completion was validated against.</summary>
    public uint Generation;

    /// <summary>Stream or datagram.</summary>
    public CompletionKind Kind;

    /// <summary>Datagram: the reported state (a final state, or <see cref="DatagramSendState.Sent"/>).</summary>
    public DatagramSendState DatagramState;

    /// <summary>The data was not delivered (stream canceled, datagram canceled).</summary>
    public bool Canceled;

    /// <summary>
    /// True for the final completion: the entry is in state <c>Completed</c> and the engine must eventually call
    /// <see cref="PeerCore.CompleteEntry"/>. False for the early <see cref="DatagramSendState.Sent"/> notice (the entry
    /// stays in flight; the payload may be released with <see cref="PeerCore.ReleasePayload"/>).
    /// </summary>
    public bool Final;

    /// <summary>
    /// <see cref="CompletionKind.Local"/>: the delivery status the entry ends with (<see cref="DeliveryStatus.Pending"/> on
    /// transport completions). <see cref="PeerCore.MapCompletion"/> reads it.
    /// </summary>
    public DeliveryStatus Status;
}

/// <summary>What the peer does with a peer-opened stream after <see cref="ChannelEngine.OnStreamOpened"/>.</summary>
internal enum StreamAcceptAction : byte
{
    /// <summary>The engine takes the stream.</summary>
    Accept = 0,

    /// <summary>Reset the stream (RESET_STREAM/STOP_SENDING) with the code; the connection survives.</summary>
    Reset = 1,

    /// <summary>Close the connection with the code (for example a duplicate persistent ordered stream).</summary>
    CloseConnection = 2,
}

/// <summary>Answer of <see cref="ChannelEngine.OnStreamOpened"/>.</summary>
internal readonly struct StreamAccept
{
    private StreamAccept(StreamAcceptAction action, QuiclyErrorCode code, long cookie)
    {
        Action = action;
        ResetCode = code;
        Cookie = cookie;
    }

    /// <summary>The action.</summary>
    public StreamAcceptAction Action { get; }

    /// <summary>The engine takes the stream.</summary>
    public bool Accepted => Action == StreamAcceptAction.Accept;

    /// <summary>When refused: the RESET_STREAM/STOP_SENDING (or connection close) code.</summary>
    public QuiclyErrorCode ResetCode { get; }

    /// <summary>Engine-owned per-stream value handed back in every <see cref="StreamMessageContext"/>.</summary>
    public long Cookie { get; }

    /// <summary>Takes the stream.</summary>
    /// <param name="cookie">Engine-owned per-stream value.</param>
    public static StreamAccept Accept(long cookie = 0) => new(StreamAcceptAction.Accept, QuiclyErrorCode.NoError, cookie);

    /// <summary>Refuses the stream; the peer resets it with <paramref name="code"/>.</summary>
    /// <param name="code">The reset code.</param>
    public static StreamAccept Reject(QuiclyErrorCode code) => new(StreamAcceptAction.Reset, code, 0);

    /// <summary>Refuses the stream and closes the connection with <paramref name="code"/> (queued to Poll).</summary>
    /// <param name="code">The connection close code.</param>
    public static StreamAccept CloseConnection(QuiclyErrorCode code) => new(StreamAcceptAction.CloseConnection, code, 0);
}

/// <summary>What the peer does after <see cref="ChannelEngine.OnStreamMessage"/>.</summary>
internal enum StreamConsumeAction : byte
{
    /// <summary>Keep parsing.</summary>
    Continue = 0,

    /// <summary>
    /// Back-pressure: the event is un-read (the parser is restored to its state before the event), the transport holds the
    /// bytes back (<see cref="ReceiveResult.PendingAfter"/>) and the stream is resumed from <see cref="QuiclyPeer.Poll"/>
    /// (<see cref="PeerCore.NotePendedStream"/>). Intended for <see cref="StreamMessagePhase.Start"/> and
    /// <see cref="StreamMessagePhase.Chunk"/>: an engine reserves its receive-ring slot at the start of a message
    /// (<see cref="PeerCore.TryReserveReceive"/>), so publishing at the end never fails.
    /// </summary>
    Pend = 1,

    /// <summary>Reset this stream with the code (group/bulk streams); the connection survives.</summary>
    ResetStream = 2,

    /// <summary>Close the connection with the code (malformed persistent ordered stream).</summary>
    CloseConnection = 3,
}

/// <summary>Answer of <see cref="ChannelEngine.OnStreamMessage"/>.</summary>
internal readonly struct StreamConsume
{
    private StreamConsume(StreamConsumeAction action, QuiclyErrorCode code)
    {
        Action = action;
        Code = code;
    }

    /// <summary>The action.</summary>
    public StreamConsumeAction Action { get; }

    /// <summary>Error code for <see cref="StreamConsumeAction.ResetStream"/> / <see cref="StreamConsumeAction.CloseConnection"/>.</summary>
    public QuiclyErrorCode Code { get; }

    /// <summary>Keep parsing.</summary>
    public static StreamConsume Continue => default;

    /// <summary>Back-pressure (see <see cref="StreamConsumeAction.Pend"/>).</summary>
    public static StreamConsume Pend => new(StreamConsumeAction.Pend, QuiclyErrorCode.NoError);

    /// <summary>Reset the stream.</summary>
    /// <param name="code">The reset code.</param>
    public static StreamConsume ResetStream(QuiclyErrorCode code) => new(StreamConsumeAction.ResetStream, code);

    /// <summary>Close the connection.</summary>
    /// <param name="code">The close code.</param>
    public static StreamConsume CloseConnection(QuiclyErrorCode code) => new(StreamConsumeAction.CloseConnection, code);
}

/// <summary>Phase of a stream message event.</summary>
internal enum StreamMessagePhase : byte
{
    /// <summary>A message begins: <see cref="StreamMessageContext.Header"/> is valid, <see cref="StreamMessageContext.Chunk"/> is empty.</summary>
    Start = 0,

    /// <summary>Payload bytes: <see cref="StreamMessageContext.Chunk"/> is a slice of the transport's buffer (valid during the call).</summary>
    Chunk = 1,

    /// <summary>The message is complete.</summary>
    End = 2,

    /// <summary>A bulk stream header was parsed: <see cref="StreamMessageContext.Bulk"/>.</summary>
    BulkHeader = 3,

    /// <summary>
    /// A whole message in one event, in place of <see cref="Start"/>, <see cref="Chunk"/> and <see cref="End"/>:
    /// <see cref="StreamMessageContext.Header"/> is valid and <see cref="StreamMessageContext.Chunk"/> is the complete payload
    /// (empty for an empty message). The peer uses it only for engines that set
    /// <see cref="ChannelEngine.AcceptsWholeMessages"/>, when a message's payload lies wholly in the current receive segment.
    /// <see cref="StreamConsumeAction.Pend"/> un-reads the whole message.
    /// </summary>
    Whole = 4,

    /// <summary>
    /// A bulk transfer's checksum trailer was parsed: <see cref="StreamMessageContext.BulkChecksum"/>. It follows the
    /// range's last body byte, so every byte has already been delivered by the time the engine can check it.
    /// </summary>
    BulkChecksum = 5,
}

/// <summary>One stream event for an engine (transport thread). Payload is never copied by the peer.</summary>
internal ref struct StreamMessageContext
{
    /// <summary>The transport stream.</summary>
    public TransportStreamId Id;

    /// <summary>The preamble's channel.</summary>
    public ushort Channel;

    /// <summary>Dense index of <see cref="Channel"/>.</summary>
    public int ChannelIndex;

    /// <summary>Group id (group streams; the version for large ReliableLatest values).</summary>
    public ulong GroupId;

    /// <summary>Phase.</summary>
    public StreamMessagePhase Phase;

    /// <summary>Header of the current message.</summary>
    public StreamMessageHeader Header;

    /// <summary>Bulk header (bulk streams).</summary>
    public BulkHeader Bulk;

    /// <summary>The sender's range checksum for <see cref="StreamMessagePhase.BulkChecksum"/>.</summary>
    public ulong BulkChecksum;

    /// <summary>Payload slice for <see cref="StreamMessagePhase.Chunk"/>.</summary>
    public ReadOnlySpan<byte> Chunk;

    /// <summary>The engine's per-stream value (from <see cref="StreamAccept.Cookie"/>); writable.</summary>
    public ref long Cookie;

    /// <summary>Clock micros of the receive callback.</summary>
    public long NowMicros;
}
