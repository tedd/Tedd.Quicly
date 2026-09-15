# Session layer internal design (Tedd.Quicly.Core)

This is the implementation-level companion to ARCHITECTURE.md / PROTOCOL.md / ADR 0008. It fixes file
layout, data structures and algorithms so that several people can implement the session layer in parallel
without drifting. Anything not stated here is decided by the documents above.

## 0. Folder map (Core)

```
Transport/      ITransport, ITransportSink, ITransportConnector/Listener, TransportSegment, types   (done, frozen)
Primitives/     VarInt, SerialNumber, XxHash64, Lz4Block                                              (stage A)
Memory/         SlabAllocator, BufferLease, SharedLeaseTable                                          (stage A)
Threading/      SpscRing, MpscRing, CompletionTable, SendToken, DeliveryStatus, CacheLine             (stage A)
Time/           IClock, MonotonicClock, VirtualClock                                                  (stage A)
Channels/       ChannelMode, ChannelDefinition, ChannelTable(+Builder), ChannelTableCodec (canonical encoding + hash)
Framing/        MessageHeader, DatagramFraming, StreamFraming, StreamFrameParser, PackedContainer, ControlCodec, ControlMessages
State/          NativeArray<T>, SendEntry (+cold SoA), SegmentArena, ReceiveEntry, KeyTable, DenseKeyTable, Mailboxes, ChannelSendState, ChannelRecvState
Session/        QuiclyPeer (partial: .cs .Send .Flush .Poll .Receive .Control .Completion), PeerCore (engine façade), PeerOptions,
                SendHeader/SendOptions/SendResult/SendStatus, ReceiveHeader/ReceivedMessage/ReceiveLease/MessageHandler,
                PeerStatistics/ChannelStatistics, IPeerAdmission/AdmissionResult/HelloInfo, PeerRole/PeerState/CloseReason/
                CompletionMode, BulkDescriptor/IBulkSource/BulkTransfer/BulkRangeRequest (C2 shapes), PingClock, TokenBucket,
                TransportControlPool, StreamTable, ReceiveQueues, ReceiveMailbox, PeerCounters
Session/Engines/ ChannelEngine (the boundary), ChannelEngines (per-mode registry), EngineTypes (SendRequest, FlushContext,
                CompletionEntry, StreamAccept, StreamConsume, StreamMessageContext), PlaceholderEngine; the mode engines
```

## 1. Channels

```csharp
public enum ChannelMode : byte { UnreliableUnordered = 0, UnreliableSequenced = 1, ReliableOrdered = 2, ReliableUnordered = 3, ReliableLatest = 4, Bulk = 5 }
public enum ChannelCompression : byte { None = 0, Lz4 = 1 }

public sealed class ChannelDefinition   // immutable after table build
{
    ushort Id; string Name; ChannelMode Mode; bool Keyed; byte SequenceBits /*16|32*/; bool Fragmentation; ChannelCompression Compression;
    bool RequestResponse; bool CoalesceOnReceive; byte Priority; int MaxMessageSize; int QueueLimitBytes; long ExpiryMicros;
    int MaxKeys; int MaxReassemblies; int MaxGroups; int GroupMaxBytes; KeySpace KeySpace /*Dense(max) | Hashed*/; int MinCompressSize;
    // derived, precomputed: bool HasSequence; int FixedHeaderBytesWithoutKey; bool IsDatagramMode; bool IsStreamMode
}
public sealed class ChannelTable { ChannelDefinition? this[int id]; int Count; ulong Hash; ReadOnlySpan<ChannelDefinition> All; static ChannelTableBuilder Create(); }
```

Validation at build: ids unique in 2..16383; ReliableLatest ⇒ SequenceBits 32 and Keyed and CoalesceOnReceive;
RequestResponse ⇒ ReliableOrdered; Fragmentation ⇒ mode ∈ {0,1}; MaxMessageSize ≤ 1 200 (unreliable, no
fragmentation), ≤ 8×1 100 (fragmentation), ≤ 16 MiB (bulk), ≤ 1 MiB otherwise; CoalesceOnReceive ⇒ Keyed.
`ChannelTableCodec.WriteCanonical(table, span)` / `Hash(table)` = XxHash64(seed 0) per PROTOCOL §1;
`WriteWithNames` / `TryParseWithNames` for HelloAck.

## 2. Framing (pure codecs over spans; no state except `StreamFrameParser`)

```csharp
public struct MessageHeader { ushort Channel; uint Sequence; ulong Key; byte FragCount; byte FragIndex; int RawLength; bool Compressed => RawLength > 0; }
static class DatagramFraming
{
    static int GetHeaderLength(in ChannelDefinition ch, in MessageHeader h);
    static int WriteHeader(Span<byte> dst, in ChannelDefinition ch, in MessageHeader h);          // returns bytes written
    static ParseStatus TryParse(ReadOnlySpan<byte> datagram, ChannelTable table, out ushort channel, out MessageHeader h, out int payloadOffset);
}
static class PackedContainer { int WriteHeader(Span<byte>, uint tick); ... iterator struct over (offset,length) of inner messages; validation rules of PROTOCOL §2.2 }
static class StreamFraming   // PROTOCOL §3.1/3.2 message framing (Length, Key, RequestId, RawLength)
{
    static int GetFrameHeaderLength(in ChannelDefinition ch, in StreamMessageHeader h);
    static int WriteFrameHeader(Span<byte> dst, in ChannelDefinition ch, in StreamMessageHeader h);
    static int WritePreamble(Span<byte> dst, ushort channel, ulong? groupId);
}
public struct StreamFrameParser   // incremental, resumable; holds at most 32 bytes of partial header; never copies payload
{
    void Reset(in ChannelDefinition ch, StreamRole role /*Ordered|Group|Bulk|Control*/);
    // Feed(segment) returns events: NeedMore, PreambleParsed, MessageHeader(h, payloadLength) then PayloadBytes(span) ... MessageComplete, Error(code)
    // A message's payload is delivered as zero or more PayloadBytes callbacks straight from the transport's segments.
}
static class ControlCodec  // every §2.3 and §3.4 message: TryWriteX(Span<byte>, in X, out int written) / TryParseX(ReadOnlySpan<byte>, out X, out int consumed); all bounds from PROTOCOL enforced here
```

`ParseStatus`: Ok, Truncated, NonMinimalVarint, UnknownChannel, ChannelNotDatagram, BadFragment, RawLengthTooLarge,
NestedContainer, BadLength, ...

## 3. State tables (native, reference-free)

* `NativeArray<T> : IDisposable where T : unmanaged` — `NativeMemory.AlignedAlloc(count*sizeof(T), 64)`, `Span`, `ref T this[int]`, `T* Pointer`.
* `SendEntry` (hot, 64 B, explicit layout): `int State` (Free/Filling/InFlight/Cancelling/Completed), `uint Generation`, `ushort Channel`,
  `byte Flags` (Tracked, Datagram, Container, Immediate, Fin, Retry), `byte HeaderLength`, `TransportSegment Header` (points to
  `HeaderScratch`), `TransportSegment Payload`, `fixed byte HeaderScratch[16]`. The `Header`+`Payload` pair is the contiguous
  `QUIC_BUFFER[2]` given to the transport. Cold SoA arrays indexed by slot: `BufferLease[] Leases`, `ulong[] Keys`, `uint[] Sequences`,
  `ulong[] Contexts`, `long[] Deadlines`, `int[] Next` (intrusive queue links), `int[] BatchHead/Count` (container membership),
  `nint[] PinHandles`.
  *Amended (review of the framing and state modules):* the header bytes move to a cold `NativeArray` of 32-byte
  blocks indexed by slot (the maximum datagram header is 24 bytes, so a 16-byte in-entry scratch is too small),
  and stream gathers use per-submission contiguous segment arrays from a native `SegmentArena` because entries
  are not adjacent `QUIC_BUFFER`s (ADR 0008 invariant 1).
  *As built:* offsets 48–63 of the entry are two engine-owned scratch words (`Aux0`, `Aux1`); `SendEntryTable.GetHeaderBlock`
  / `SetHeaderLength` / `SetHeader` fill the header block; `SendEntryFlags` adds `Pinned` (64, the pin handle in `PinHandles`
  must be freed) and `EngineCompletes` (128, see §7). `SegmentArena` (State/) hands out circular runs of segments that may be
  freed in any order and are reclaimed oldest first.
* `ReceiveEntry` (≤ 64 B): `ushort Channel`, `ReceiveFlags Flags`, `uint Sequence`, `ulong Key`, `BufferLease Lease`, `int Length`,
  `int RawLength`, `uint ReceivedMicrosDelta`, `uint SenderTick`, `uint RequestId`. *As built:* `ReceivedMicrosDelta` holds the low
  32 bits of the receive time (`PeerCore.StampReceive(now)`); Poll restores the full value relative to its own `now`.
* `KeyTable`: open addressing, linear probing, SoA `ulong[] keys`, `int[] slots`, power-of-two capacity, load ≤ 0.5, `fmix64`,
  backward-shift deletion (no tombstones). `DenseKeyTable`: direct index for `KeySpace.Dense(max)`.
* Per-key send slot (`KeySendSlot`, 64 B): current version, acked version, lease of current value (ReliableLatest), retry deadline,
  attempts, entry index in flight, large-value stream id. Per-key receive slot (`KeyRecvSlot`): last accepted sequence,
  mailbox lease index (int, −1 empty), reassembly index, flags.
* `Mailboxes`: `int[] mailbox` per key slot + `ulong[] dirty` bitset per channel; transport thread `Interlocked.Exchange` in,
  game thread `Interlocked.Exchange(-1)` out; `PopDirty(Span<int> keys)` scans the bitset with `BitOperations.TrailingZeroCount`.
* `ChannelSendState` / `ChannelRecvState` (64 B each, per channel): counters, next sequence, queue head/tail/bytes, stream id,
  group state, statistics fields (sent/received/dropped/superseded/expired/queueFull/tooLarge/ringDrops/retries/keyTableFull).

## 4. QuiclyPeer (as built: wave C1, step 1 — peer core)

`public sealed partial class QuiclyPeer : IDisposable`. Transport callbacks are **not** public API: a private nested
`Sink : ITransportSink` forwards them and is exposed as `peer.TransportSink` (a server's `AcceptCallback` returns it; a client
peer hands it to `ITransportConnector.Connect` itself). Creation: `QuiclyPeer.Connect(connector, endpoint, serverName, table,
options, authToken)` (client) and `QuiclyPeer.CreateServerPeer(transport, in info, table, options, admission)` (server).

| File | Owns |
|---|---|
| `QuiclyPeer.cs` | construction, immutable option values, public properties (`Index`, `Tag`, `Role`, `State`, `Epoch`, `SessionId`, `SessionToken`, `HandshakeStatus`, `CloseReason`, `RemoteEndPoint`, `Capabilities`, `Channels`, `RemoteChannelTable`, `LastCallbackFault`), `GetStatistics`, `GetChannelStatistics`, `EstimatedRemoteMicros`, `NextDeadline`/`NextDeadlineMicros`, `Dispose` with deferred native free, the transport→game signal word |
| `QuiclyPeer.Send.cs` | `RentBuffer`/`GetBufferSpan`/`ReturnBuffer`, `SendCopy/SendOwned/SendPinned/SendBorrowed/SendGather/SendAsync`, `SendRequestAsync`, `Respond`, `RetireKey`, `BeginBulkSendAsync`, `RequestBulk`: resolve the channel, require `Connected`, build a `SendRequest`, call the channel's engine |
| `QuiclyPeer.Flush.cs` | `Flush(tick)`, `FlushAsync`, the scheduler seam `FlushEngines`, `NextDeadline` bookkeeping |
| `QuiclyPeer.Poll.cs` | `Poll(maxItems)`, `Drain`, `Release`, `Retain`, `RegisterHandler`/`UnregisterHandler`, per-channel drain queues (`ReceiveQueues`), mailbox dispatch, LZ4 decode in Poll, pended-stream resume, the final Closed step |
| `QuiclyPeer.Control.cs` | game thread: handshake (client Hello, server checks + `IPeerAdmission` + HelloAck, `CompleteAdmission`, channel-table answer), `Close`, close linger, ping schedule + `PingClock`, heartbeat, admission timeout, the `StateChanged` queue |
| `QuiclyPeer.Receive.cs` | transport thread: the `Sink`, datagram / container / control-datagram receive, the stream table driver (control stream parsing, preamble → engine, parser events → engine, back-pressure un-read), stream error rules, Pong from the transport thread |
| `QuiclyPeer.Completion.cs` | `WaitAsync`/`Wait`/`GetDeliveryStatus`/`TryCancel`, completion-ring drain and routing |
| `PeerCore.cs` | the engine-facing façade (§7) |
| `Engines/*.cs` | the engine boundary, the registry, the placeholder, the mode engines |

**Hand-offs** (ADR 0008). Transport → game thread: a signal word (`Interlocked.Or` / `Exchange`) with the bits Connected, Hello,
HelloAck, PeerClose, TransportClosed, CloseRequest, TableRequest, TableInfo — the data of a bit is written before it is set
(Hello/HelloAck/Close bodies are copied to arrays: handshake and close only); SPSC rings `CompletionRing` (`CompletionEntry`,
capacity 2 × send table + 1), `ReceiveRing` (`ReceiveEntry`), `PendedStreams` (capacity = the peer's unidirectional allowance + 2),
pong samples (16) and stream-ping requests (8); per-key mailboxes; `PeerCore.RequestClose(code)` (first request wins), executed by
the game thread in Poll/Flush. Game → transport thread: `PeerCore.IsAdmitted` (volatile), the control stream id (volatile word) and
`PeerCore.SessionMaxMessageSize` (server: its option; client: set by the transport thread from an accepted HelloAck before it
publishes admission). Transport-thread code calls only `AbortStream`, `CloseStream` (after shutdown complete) and `SendDatagram`
(Pong); never `Close`. Every `Sink` callback is wrapped: an exception is counted (`CallbackFaults`), kept (`LastCallbackFault`)
and turned into a queued `InternalError` close (`PeerOptions.FailFastOnCallbackException` → `Environment.FailFast`). After the
transport's `OnClosed` no callback runs.

**Lifetime.** Native memory (tables, rings, the private allocator) is freed exactly once, by whichever step completes the
lifetime word: close seen (the `Sink` sets it in the `finally` of `OnClosed`; `Dispose` sets it for a peer that never got a
transport), dispose requested (`Dispose`, which also closes and disposes the transport) and no game-thread call in progress (a bit
held from the outermost `Poll`/`Flush` entry to its exit, handler re-entry included). `Dispose` may therefore run inside a
`StateChanged` or receive handler: the memory stays valid until that `Poll` returns. After `Dispose`, `Release` is a no-op,
statistics and `Capabilities` read as default, and callbacks that still arrive are ignored (`IsFreed`).

### 4.1 Send path (game thread)

1. The peer: channel lookup (`InvalidChannel`), `State == Connected` (`NotConnected`), argument checks (length vs lease, pinned
   null, ≤ 8 gather pages), then `engine.Admit(ref SendRequest)` (placeholders answer `NotSupported`).
2. The engine (steps 2/3): size against `PeerCore.EffectiveMaxMessageSize(channel)`, key, queue limits;
   `PeerCore.TryAllocateEntry(channel, flags, out slot)` (`QueueFull`); header bytes into the slot's 32-byte block
   (`Entries.GetHeaderBlock(slot)` + `Entries.SetHeaderLength`); payload with `TryRentSend` + `AttachLease` (copy / owned), `SetPayload`
   (pinned) or a pin handle in `Entries.PinHandles` (borrowed: `GCHandle.ToIntPtr`; `ReleasePayload` frees it); tracked sends with
   `PeerCore.TryTrack(slot, options.Context, out request.Token)`; queue on the channel (intrusive `Entries.Next`). Any failure after
   allocation: `PeerCore.DiscardEntry(slot)` (releases token as Canceled, payload, slot). Nothing reaches the transport in `Admit`
   except `Immediate` sends.

### 4.2 Flush (game thread)

1. `now` once; `DrainCompletions()` (frees slots before new work); `RunTimers(now)`: admission deadline (Connecting/Handshaking),
   ping schedule + `PingClock.Advance` + heartbeat (Connected), close linger (Closing).
2. While `Connected`: `FlushContext { NowMicros, Tick, MaxDatagramPayload (current), DatagramsEnabled, NextDeadline }`; every
   engine's `Tick(now, ref flush.NextDeadline)`; then `FlushEngines(ref flush)` — **the step-2 seam**: it calls every engine's
   `Flush` once in mode order today; step 2 replaces its body with the scheduler (channel priority order precomputed, admission order
   within a channel, expiry at scheduling time, token bucket `MaxSendBytesPerSecond`) and the per-peer packer.
3. `NextDeadlineMicros = min(timer deadline, engine deadline)`.

Submission primitives (`PeerCore`): `SubmitDatagram(slot, flags)` sends the entry's header+payload pair (1 or 2 segments; sets
`SendEntryFlags.Datagram`); `SubmitStream(stream, segments, count, slot, flags)` sends a segment array whose completion carries the
slot's context. Both publish before the call; on a failed call the entry is back in `Filling` and still owned by the caller.

### 4.3 Receive path (transport thread)

* Datagram: `DatagramFraming.TryParse(datagram, table, SessionMaxMessageSize)`.
  * Channel 0: `ControlCodec.TryReadDatagram`, control-rate token, admitted only: Ping → Pong through `TransportControlPool`
    (8 × 32-byte native buffers, `Priority`, 4/s + burst 32; a full pool counts `ControlSendFailures`), Pong → pong ring (t4
    stamped at arrival), LatestAck/LatestReject → `OnControl` of the ReliableLatest engine, BulkProgress → Bulk engine (batch
    structure validated first; no engine of that mode ⇒ malformed).
  * Channel 1: `PackedContainer.TryParse`, then every member as control or application frame; `PeerCore.CurrentSenderTick` holds the
    container tick while its members are dispatched.
  * Application: before admission ⇒ `DroppedBeforeAdmission`; else `engine.OnDatagram(header, payload, now)`.
  * Anything else ⇒ `MalformedDatagrams` + the channel's `Dropped`.
* Streams: one `StreamRecord` per peer stream in `PeerCore.Streams` (`Control`, `Preamble`, `Engine`, `Discard`).
  * Bidirectional: the server's first is the control stream, a second is a connection `ProtocolViolation`; the client resets any
    server-opened one with `UnsupportedChannel`; the client's own control stream gets its record on its first data.
  * Unidirectional before admission ⇒ reset `AdmissionRejected` + `DroppedBeforeAdmission`.
  * Preamble (`StreamFrameParser`, `StreamRole.Unknown`, session cap): unknown channel, channel 0/1 or a datagram-only channel ⇒
    reset `UnsupportedChannel`; malformed ⇒ reset `ProtocolViolation`; otherwise `engine.OnStreamOpened(id, channel, groupId)` →
    `Accept(cookie)` / `Reject(code)` (reset) / `CloseConnection(code)`.
  * Events ⇒ `engine.OnStreamMessage(ref StreamMessageContext)` (`Start`, `Chunk`, `End`, `BulkHeader`) → `Continue`, `Pend` (the
    parser is restored to its snapshot before the event, the call returns `PendingAfter(bytes before the event)`, the stream id goes to
    `PendedStreams` and Poll calls `ResumeStreamReceive(id, 0)`), `ResetStream(code)` or `CloseConnection(code)`.
    The context (`Chunk`, `Header`, `Bulk` and the `Cookie` ref) is valid only during the call (the peer declares it `scoped`);
    copy what you keep.
  * A parser error or a FIN inside a message: ordered stream ⇒ connection `ProtocolViolation`; group/bulk ⇒ reset `ProtocolViolation`.
    The owning engine gets `OnStreamClosed` exactly once per accepted stream (reset by either side, error, or shutdown complete);
    events of locally opened streams (peer STOP_SENDING, shutdown complete) are broadcast to every engine. `CloseStream` follows
    shutdown complete.
  * Receive results: the peer consumes a whole indication (`Consumed(all)`) or returns `PendingAfter(n)`; it never returns a partial
    `Consumed` and never `Consumed(0)` for a non-empty indication, because both mean back-pressure in the `ReceiveResult` contract.
    `ResumeStreamReceive` is called only from Poll on the game thread, never from inside the receive callback; a resume that races
    the returning callback takes effect when it returns.
* Control stream (both directions start with `0x00`; `StreamFrameParser(Control)`; bodies used in place or assembled in a lazily
  grown array; control-rate limit): server — the first frame must be Hello, a second Hello or a HelloAck is a violation, the Hello body
  is copied and signalled (malformed ⇒ violation; version ≠ 1 ⇒ answered with status 1); client — the first frame must be a HelloAck
  (Informational, or Accepted with epoch 0 ⇒ violation), later only Informational HelloAcks (table answers); Close anywhere; after
  admission Ping (answered on the stream by the game thread), Pong, Latest*/Bulk*/KeyRetired (routed to engines), ChannelTableRequest
  (server); wrong-direction or undefined types and a FIN without Close ⇒ `ProtocolViolation`.
* After Close was sent or received, or a local violation was queued, all further input is ignored (PROTOCOL.md §6).

### 4.4 Poll (game thread)

1. `now` once; `DrainCompletions()` — with `CompletionMode.PollOnly` tracked-send continuations run here.
2. `ProcessSignals(now)`, in this order: Connected (Connecting → Handshaking; the client opens the control stream and sends Hello),
   the queued local close (before the Hello so a violating client is never admitted), peer Close, Hello (server), HelloAck (client),
   table info / table request, transport closed; then pong samples (matched against the last eight pings; unmatched are counted) and
   stream-ping requests.
3. `RunTimers(now)`; raise queued `StateChanged` events, holding back the change to Closed.
4. Dispatch while Connected or Closing (and Closed not yet raised): drain-queued messages of channels that now have a handler, the
   held entry, then the ring — a channel with a handler is dispatched, others go to `ReceiveQueues` (a node pool of the ring's
   capacity; when it is full one entry is held and the ring is left alone) — then the mailboxes of channels with handlers.
   Compressed messages are decoded with `Lz4Block.DecompressExact` into a second lease (decoded-bytes budget; a failure drops and
   counts `DecodeFailures`). The lease is released after the handler unless `Retain` was called; handler exceptions propagate.
5. `ResumePendedStreams()`.
6. Closed: engines' `OnPeerClosed`, every receive lease released, the Closed event raised last; afterwards Poll/Flush do nothing and
   no handler or event runs.

### 4.5 Control protocol

* Client: Connected ⇒ `OpenStream(Bidirectional, PeerCore.ControlStreamContext, 65535)`; the first send carries `0x00` + Hello
  (`Start | Priority`; table hash, flags, last epoch, session and auth tokens, `MaxReceiveDatagram`, caps). Accepted HelloAck ⇒
  epoch, session id and token, optional table (`RemoteChannelTable`), `UpdatePeerStreamLimits(0, uni)`, Connected. Refused ⇒
  `HandshakeStatus`, close `AdmissionRejected` with the sanitized reason (Source Peer).
* Server: Hello ⇒ version (status 1), table hash (2), datagrams (7: the table has a mode 0/1/4 channel and the client's caps or the
  transport lack datagrams), then `IPeerAdmission.Admit(in HelloInfo, peer)`: Accept ⇒ `IsAdmitted = true`, then
  `UpdatePeerStreamLimits(1, uni)`, then HelloAck (epoch ≥ 1, random session id when 0, the session token, the table when requested
  and within the frame budget), Connected; Reject ⇒ HelloAck(status, reason), then the transport closes with `AdmissionRejected`
  when that send completes (or after `CloseLinger`); Pending ⇒ `CompleteAdmission(result)` later; an exception from `Admit` ⇒
  status 6 and it propagates from Poll. ChannelTableRequest ⇒ informational HelloAck (status 0xFF) with the table.
* `uni` = Σ max(MaxGroups, 1) over stream-capable channels, capped at 4 096 (`PeerCore.PeerUnidirectionalStreamLimit`).
* Timers: admission deadline = creation + `AdmissionTimeout` for both roles (Hello, admission and HelloAck must fit) ⇒ close
  `Timeout`; heartbeat (Connected): `max(last receive, connected at) + HeartbeatTimeout` ⇒ close `Timeout` (0 disables); pings at once
  on Connected, every `FastPingInterval` (100 ms) during `FastLockDuration` (3 s), then every `PingInterval` (1 s) — as a Priority
  datagram when datagrams are enabled, otherwise on the control stream.
* Time sync: 32-bit connection-relative micros (`PeerCore.ConnectionStartMicros` = local clock at `OnConnected`). `PingClock`: RFC 6298
  smoothing, offset = the sample with the minimum RTT of the last eight (the newest on ties), stepped until the fast lock ends, then
  slewed by at most elapsed/16; one-way jitter from consecutive Pong timestamps. `EstimatedRemoteMicros()` = local
  connection-relative now + published offset.
* Rate limits: control messages 200/s (burst 200) ⇒ close `LimitExceeded`; Pongs 4/s (burst 32) ⇒ excess `PingsIgnored`.
* Close: `Close(reason)` ⇒ Closing, a Close frame on the control stream (when open), the transport close with the same code when
  that frame is delivered or after `CloseLinger` (0 = at once); a received Close ⇒ Closing and an immediate transport close with the
  same code; a transport close without Close ⇒ Closed with Source Peer/Local/Transport. The first reason wins.
* LatestAck/LatestReject coalescing (`AckDelay`) and KeyRetired sending belong to the engines of wave C2.

### 4.6 Engines

* Wave C1 step 2: `UnreliableUnordered` and `UnreliableSequenced` engines (per-key acceptance with `KeyTable`/`DenseKeyTable`,
  coalescing mailboxes), the scheduler and the packer.
* Wave C1 step 3: the `ReliableOrdered` engine (lazy persistent stream, gather send, progressive receive, back-pressure).
* Wave C2: `ReliableLatestEngine`, `GroupStreamEngine` (`ReliableUnordered`), `BulkEngine`, fragmentation in the unreliable engines,
  request/response in the ordered engine.
* `PingClock` is peer-level (`Session/PingClock.cs`), not an engine.

## 5. SimulatedTransport (Tedd.Quicly.Testing)

Namespace `Tedd.Quicly.Testing.Simulation`. `SimulatedNetwork(VirtualClock, seed)` owns one deterministic event queue (min-heap by due
time, FIFO among equal times) and one seeded random generator: the same seed and the same calls give the same callbacks.
`CreatePair(sinkA, sinkB, LinkOptions)` returns two `SimulatedTransport` ends (A is the client). `SimulatedConnector` /
`SimulatedListener` implement `ITransportConnector` / `ITransportListener` (PreHandshake reject, Accept returning null), so client and
server layers run without MsQuic. `RecordingSink` records every callback with payload copies (thread-safe, with wait helpers).

`LinkOptions` (copied at link creation): `DelayMicros` (one way), `JitterMicros`, `LossPercent` (datagrams), `StreamLossPercent` (stream
packets, modelled as a retransmission delay of `RetransmitDelayMicros`, never as missing data), `ReorderPercent`, `BandwidthBitsPerSecond`
(0 = unlimited; a per-direction serialization queue served highest priority first: priority datagrams, then datagrams, then streams
by priority), `MaxQueueBytes` (datagrams beyond it are dropped), `MaxDatagramPayload` (default 1200, also the stream packet size),
`DatagramsEnabled`, `DatagramSendStateReporting`, `PeerUnidiStreams` / `PeerBidiStreams` (default 0 / 1, like MsQuic before admission),
`MtuChanges` (list of `(AtMicros, MaxDatagramPayload)`), `DisconnectAtMicros`, `ConnectDelayMicros` (default one round trip; the
connector ignores it: the client connects after one RTT, the server half an RTT later).

Timing. `network.Advance(micros)`, `AdvanceTo` and `RunUntilIdle(maxMicros)` deliver due events in order on the calling thread (the
simulated transport thread). Every sink callback happens inside them. Unlike MsQuic, a callback never runs inline in an API call: a
completion due "now" arrives at the next `Advance(0)`. `CreatePair` raises `OnDatagramCapabilityChanged` on both ends before
`OnConnected` on either.
- Datagrams: `Sent` at the next advance step (or when the serializer starts on the datagram), then exactly one final state:
  `Acknowledged` at delivery + one-way delay, `LostDiscarded` one RTT after a loss, or `Canceled` (on close, when an MTU drop makes a
  queued datagram too large, or with `CancelOnBlocked` when the serializer is busy). `TooLarge` is returned synchronously.
- Streams: `OnStreamStarted` locally at the next step, `OnPeerStreamStarted` one one-way delay later. Data is cut into packets of the
  current payload size, reassembled in order and indicated one segment per packet. A send completes at delivery of it and of every
  byte before it + one-way delay. `PendingAfter` holds bytes back until `ResumeStreamReceive`; consuming part of an indication has the
  rest indicated again at the next step, consuming nothing of a non-empty indication counts as `PendingAfter(0)`, and aborting a local
  stream that was never started releases it like `CloseStream` (the `ITransport` contract as clarified by the MsQuic transport wave).
  Aborts are causal: the peer keeps delivering what it holds until the reset arrives. `OnStreamShutdownComplete` comes once both
  directions are done, and `CloseStream` then bumps the slot generation. A stream's peer credit returns to its opener when the peer side has shut down.
- `Close`: in-flight sends complete canceled, streams shut down, then `OnClosed` (Local). One one-way delay later the peer does the same
  (Peer, code, reason). Data already on the wire still reaches the peer before its close, as in QUIC. Data queued behind a bandwidth
  limit, or in flight when the link is cut, is lost. No callbacks follow `OnClosed`.

Fault injection: `DropNextDatagrams(n)` and `LoseNextStreamPackets(n)` on a transport hit specific packets (lost final update, lost ack,
lost fragment) without seed hunting. `GetLinkStatistics` exposes per-direction counters.

Notes for session-layer tests over the simulator:
1. After an API call, `Advance(0)` before expecting its callback.
2. Payloads are copied when a send is accepted, so the simulator cannot catch a caller that reuses a buffer before its completion
   (ADR 0008 invariant 1). Cover that rule in the session layer's own tests or end to end over MsQuic.
3. Stream flow control is not modelled. Sends complete while the receiver stays Pending, and the receive buffer is unbounded, so
   sender-side back-pressure cannot be exercised here yet.
4. Stream limits count concurrently open streams. The defaults are 1 bidi and 0 uni until `UpdatePeerStreamLimits`.
5. A partial consume without `Pending` (at least one byte) keeps the remainder and indicates it again at the next step, together with
   any data that arrived meanwhile, without waiting for new data (MsQuic re-indicates it right after the callback). Consuming nothing of
   a non-empty indication without `Pending` counts as `PendingAfter(0)`: the stream waits for `ResumeStreamReceive` (`ReceiveResult`
   contract, identical on both transports).
6. Zero-allocation tests need a warm-up that reaches the run's peak of concurrent events, streams and sends; the tables grow to that peak
   and then stay. A stream-per-message workload under jitter needs about 15,000 messages.
7. `OnIdealSendBufferSize` and `OnPeerAddressChanged` are never raised. `IdealSendBufferSize` and `AppOwnedReceiveBuffers` are false.
8. Link options are fixed at creation. There is no mid-run change of loss, delay or bandwidth; use the targeted drops or a new link.
9. `CloseStream` before shutdown aborts both directions with code 0. Pending completions are still reported (canceled), but the shutdown
   callback is not.

## 6. Tests that must exist (Core)

Framing: every header shape, every ParseStatus, container rules, non-minimal varints. Channels: validation matrix, hash stability
vectors. State: KeyTable (insert/find/remove/backward shift/grow/dense), mailboxes concurrency, NativeArray, SegmentArena. Session over
SimulatedTransport: handshake (accept/reject/timeout/table mismatch/datagrams required), every mode end to end, coalescing on/off,
key reuse after RetireKey, ReliableLatest lost final update / lost ack / rollover / retry budget / large value stream supersede,
fragmentation loss, expiry under a bandwidth cap, group streams with a stream limit of 1, bulk transfer with resume, request/response
with timeout, ring overflow policies, ping/RTT/offset, close/epoch/reconnect resync, zero-allocation steady state for send/poll.

The peer-core tests live in `tests/Tedd.Quicly.Core.Tests/Session/`: `SessionTestKit.cs` (fixtures: `SessionHarness` peer↔peer,
`ServerHarness` peer server + `RawClient`, `ClientHarness` peer client + raw server, `Frames` builders, `TestEngine` exercising every
engine seam, `OffsetClock`, `AllocationAssert`), `HandshakeTests`, `CloseTests`, `PingTests`, `PlumbingTests`,
`SessionZeroAllocationTests`, `SessionUnitTests` (fake-transport tests of the support types) and `SessionEdgeTests` (control
frames assembled across packets, engine-bound control routing, control-stream aborts, input after a violation, data on a reset
stream, stream pong limits, bulk headers, engine faults in stream callbacks, drain paths, `Dispose` from a handler, callbacks after
the peer was freed).

## 7. Engine boundary and implementation waves

The peer never contains mode-specific logic. It owns the shared tables (send entries, receive ring, mailboxes,
key tables, stream table, completion table) and dispatches to **one engine instance per delivery mode per
peer** — created only for modes present in the table. An engine owns the SoA state of *all* channels of its mode
(indexed by a dense per-mode channel index it derives in `Initialize`), which keeps the data layout ECS-like and lets
engines be written in parallel without touching the peer's files. A mode is implemented by adding its engine file and
changing its case in `ChannelEngines.Create`; `PeerOptions.EngineFactory` (internal) lets tests substitute an engine.

```csharp
internal abstract class ChannelEngine : IDisposable     // Session/Engines/ChannelEngine.cs
{
    public abstract ChannelMode Mode { get; }
    public abstract void Initialize(PeerCore core, ReadOnlySpan<ChannelDefinition> channelsOfMode);   // constructor time

    // game thread
    public abstract SendStatus Admit(ref SendRequest request);          // validate, fill entry, enqueue (no transport call except Immediate)
    public abstract void Flush(ref FlushContext flush);                 // submit queued work (called from the scheduler seam)
    public abstract void Tick(long nowMicros, ref long nextDeadline);    // retries, expiry, timers (every Flush while Connected)
    public abstract void OnSendCompleted(int entrySlot, in CompletionEntry completion);  // final completion, or the early Sent notice
    public abstract void OnEpochReset(bool resumed);                     // when the session becomes Connected (resumed = epoch > 1)
    public virtual bool TryCancel(int entrySlot);                        // default false
    public virtual void OnPeerClosed();                                  // after Closed: complete queued entries Disconnected
    public virtual SendStatus RetireKey(ChannelDefinition, ulong key);   // C2 hooks: default NotSupported / NotSupportedException
    public virtual ValueTask<ReceiveLease> SendRequestAsync(ref SendRequest, long timeoutMicros, CancellationToken);
    public virtual SendStatus Respond(in ReceiveHeader request, ref SendRequest response);
    public virtual ValueTask<BulkTransfer> BeginBulkSendAsync(ChannelDefinition, in BulkDescriptor, IBulkSource, CancellationToken);
    public virtual void RequestBulk(ChannelDefinition, in BulkRangeRequest);

    // transport thread
    public abstract void OnDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros);   // admitted only
    public abstract StreamAccept OnStreamOpened(TransportStreamId id, ushort channel, ulong groupId);       // Accept/Reject/CloseConnection
    public abstract StreamConsume OnStreamMessage(ref StreamMessageContext message);   // Start/Chunk/End/BulkHeader → Continue/Pend/ResetStream/CloseConnection
    public abstract void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode);   // once per accepted stream; local stream events broadcast
    public virtual bool OnControl(ControlType type, ReadOnlySpan<byte> body, bool onStream, long nowMicros);   // validated, admitted; false = violation / drop
    public virtual void Dispose();                                       // after the transport can no longer call back
}
```

`PeerCore` (Session/PeerCore.cs) is the engine-facing façade. Game thread: `TryAllocateEntry`, `TryTrack`, `AttachLease`,
`SetPayload`, `Entries` (header blocks, cold SoA, `PinHandles`, `Next`/`BatchHead`), `Segments` (`SegmentArena`), `SubmitDatagram`,
`SubmitStream`, `OpenStream`, `CompleteStage`, `CompleteEntry` (accepts Completed, InFlight and Filling entries), `DiscardEntry`,
`ReleasePayload`, `TryRentSend`/`ReturnSend`, `SendCounters(ci)`, `MapDatagramState`, `MapStreamCompletion`, `GetToken`,
`GetUserContext`, `CreateMailbox` (from `Initialize`), `CreateKeyTable(channel)`. Transport thread: `TryRentReceive`,
`TryEnqueueReceive`, `TryReserveReceive`/`PublishReserved`/`CancelReservation`, `NotePendedStream`, `RecvCounters(ci)`,
`CountDatagramDropped`, `CurrentSenderTick`, `Streams`. Any thread: `RequestClose(code)`, `ReturnReceive`, `GetPointer`/`GetSpan`,
`ChannelIndexOf`, `GetChannel`, `GetEngine`, `EffectiveMaxMessageSize`, `SessionMaxMessageSize`, `MaxDatagramPayload`,
`DatagramsEnabled`, `DatagramStatesReported`, `StreamCreditGeneration`, `IsAdmitted`, `IsTransportClosing`, `Epoch`, `Clock`,
`ConnectionStartMicros`, `StampReceive`/`RestoreReceive`, `Counters`.

Completion rules. The transport thread validates the generation-tagged context (`Entries.TryTransitionContext`), moves the entry to
`Completed` on a final state (datagram `Acknowledged`/`AcknowledgedSpurious`/`LostDiscarded`/`Canceled`, or `Sent` when the transport
does not report states; any stream completion) and pushes a `CompletionEntry { Final = true }`; a tracked or container datagram
entry also gets one early `Sent` notice (`Final = false`, entry still in flight). Stale contexts are counted. The game thread routes
by the entry's channel: 0 → the peer's control traffic, 1 → `PeerCore.OnContainerCompleted` (packer seam), otherwise the channel's
engine. With `CompletionMode.ThreadPool` the transport thread also completes the token of a tracked, non-container entry with the
default mapping (unless the entry has `SendEntryFlags.EngineCompletes`); the game-thread `CompleteEntry` is then a no-op for the token
and only frees the slot.

### 7.1 Seams for step 2 (datagram engines, scheduler, packer)

* Register `UnreliableUnordered` and `UnreliableSequenced` engines in `ChannelEngines.Create`. `SessionSupportTests.
  Every_Mode_Has_A_Registered_Engine` asserts placeholders only for the C2 modes.
* Replace the body of `QuiclyPeer.FlushEngines(ref FlushContext)` (QuiclyPeer.Flush.cs) with the scheduler; keep engine `Tick`
  where it is. Put the token bucket (`TokenBucket`, `PeerOptions.MaxSendBytesPerSecond`) in the scheduler.
* The packer: container entries use channel id `PeerCore.ContainerChannelId` (1) and `SendEntryFlags.Container`, member entries join
  with `Entries.AddToBatch` and are never submitted themselves; replace `PeerCore.OnContainerCompleted` with the fan-out (release on
  the early Sent notice, finish members with `CompleteEntry` — it accepts their `Filling` state). In ThreadPool mode containers are not
  signalled by the transport thread; fan out on the game thread (or extend `PeerCore.SignalFinal`).
* Send flags per PROTOCOL.md §4.5 go through `SubmitDatagram(slot, flags)`; datagram size is `FlushContext.MaxDatagramPayload`.
* Receive: `TryRentReceive` (count `Counters.OutOfReceiveBuffers` and the channel's `OutOfBuffers` on failure), copy, fill a
  `ReceiveEntry` (`ReceivedMicrosDelta = PeerCore.StampReceive(now)`, `SenderTick = CurrentSenderTick`, `RawLength`,
  `ReceiveFlags.Compressed` when `header.Compressed`), then `TryEnqueueReceive` (false ⇒ return the lease, count the channel's
  `RingDrops`) or, for coalescing keyed channels, a mailbox from `CreateMailbox` (`TryPost`; return the displaced lease and count
  `Superseded`). Poll decodes LZ4, dispatches and releases; engines never touch the game-thread side of a received message.
* Send flags: set `TransportSendFlags.CancelOnBlocked` (PROTOCOL.md §4.5 `DropWhenBlocked` for unreliable datagrams) only when the
  transport reports `TransportCapabilities.CancelOnBlocked`; without it the datagram is queued instead of being dropped and counted
  `Expired`. Do not set `DelaySend` for tick bursts: it measured 16-19 % slower per datagram for bursts of 32 on MsQuic loopback.
* `SessionTestKit.TestEngine` is a working example of every one of these calls.

### 7.2 Seams for step 3 (ordered-stream engine, async completion APIs)

* Register the `ReliableOrdered` engine in `ChannelEngines.Create`.
* Send: `PeerCore.OpenStream(StreamKind.Unidirectional, context, priority)` (context ≠ `PeerCore.ControlStreamContext`), the first
  `SubmitStream` with `TransportSendFlags.Start`; retry opens that hit `StreamLimitReached` when `StreamCreditGeneration` changes.
  Gathers: `Segments.TryAllocate(count ≤ 64, out start)`, copy each entry's pair from `Entries.GetSegments(slot)`, `SubmitStream(stream,
  Segments.GetPointer(start), count, contextSlot, flags)`, keep `start` in the context entry (`Aux0`) and `Segments.Free(start)` when
  its completion is drained; finish the covered entries with `CompleteEntry`.
* Receive: a second persistent stream for a channel ⇒ `StreamAccept.CloseConnection(ProtocolViolation)`; `Start` ⇒
  `TryReserveReceive` then `TryRentReceive(Header.Length)` (either failing ⇒ `StreamConsume.Pend`; the peer un-reads the event and
  resumes the stream from Poll); `Chunk` ⇒ copy; `End` ⇒ `PublishReserved`; `OnStreamClosed` ⇒ cancel the reservation and return the
  lease. `MaxMessageSize` (session cap included) is enforced by `StreamFrameParser` (`MessageTooLarge`), which the peer turns into a
  connection `ProtocolViolation` on an ordered stream. Local-stream events (`OnStreamClosed` broadcast) arrive on the transport thread:
  hand them to the game thread with an `Interlocked` flag per channel.
* Local streams: one whose first `SubmitStream` (with `Start`) failed was never started — release it with `PeerCore.Transport.CloseStream`
  (`AbortStream` does the same for it), expect no callback and treat the id as stale; only a started stream reports
  `OnStreamShutdownComplete`. Back-pressure is `StreamConsume.Pend`: never call `ResumeStreamReceive` from inside a receive callback,
  the peer resumes pended streams from Poll.
* Async APIs: `QuiclyPeer.SendAsync` (today one synchronous admission attempt), `FlushAsync` (today `Flush()` + a completed task),
  `WaitAsync`/`Wait`/`GetDeliveryStatus` (wired to `CompletionTable`) and `TryCancel` (wired to `ChannelEngine.TryCancel` through
  `PeerCore.EntryOfToken`). `PeerOptions.ThreadSafeSend` is reserved: add the Vyukov MPSC front in QuiclyPeer.Send.cs and drain it at
  the start of `Flush`/`Poll`.

Waves:

| Wave | Content | Depends on |
|---|---|---|
| C1 step 1 (done) | `PeerCore`, `QuiclyPeer` public API, handshake/control/ping/close, `ChannelEngine` base + registry + placeholder, stream table, completions, Poll/Drain/handlers, statistics | State, Framing, Channels, Control, SimulatedTransport |
| C1 step 2 | packer + scheduler, engines for `UnreliableUnordered`, `UnreliableSequenced` (incl. coalescing mailboxes), all send paths, tracked sends, compression on send | step 1 |
| C1 step 3 | `ReliableOrdered` engine (persistent stream, gather send, progressive receive, back-pressure), `SendAsync`/`FlushAsync`/`ThreadSafeSend`, benchmarks | steps 1–2 |
| C2 (parallel) | `ReliableLatestEngine`; `GroupStreamEngine` (`ReliableUnordered`); `BulkEngine`; fragmentation in the unreliable engines; request/response in the ordered engine | C1 |
| C3 | `MsQuicTransport` (ITransport over the MsQuic wrappers) + listener/connector; `QuiclyServer` / `QuiclyClient`; admission; reconnect | C1, msquic bindings |
| C4 | WebTransport-over-HTTP/3 carrier (opt-in), HTTP/3 static responder | C3, Http3 |
| C5 | End-to-end tests (MsQuic loopback, ACME mock CA + HTTP server + QUIC listener cert swap), samples, E2E benchmarks | all |
