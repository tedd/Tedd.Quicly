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
Session/        QuiclyPeer (partial: .cs .Send .Flush .Poll .Receive .Control .Completion .Work .Shared .Reconnect), PeerCore (engine façade), PeerOptions,
                SendHeader/SendOptions/SendResult/SendStatus, ReceiveHeader/ReceivedMessage/ReceiveLease/MessageHandler,
                PeerStatistics/ChannelStatistics, IPeerAdmission/AdmissionResult/HelloInfo, PeerRole/PeerState/CloseReason/
                CompletionMode, BulkDescriptor/IBulkSource/BulkTransfer/BulkRangeRequest (C2 shapes), PingClock, TokenBucket,
                TransportControlPool, StreamTable, ReceiveQueues, ReceiveMailbox, PeerCounters, IPeerWorkSignal
Session/Engines/ ChannelEngine (the boundary), ChannelEngines (per-mode registry), EngineTypes (SendRequest, FlushContext,
                CompletionEntry, StreamAccept, StreamConsume, StreamMessageContext), EnginePayload (the shared send paths),
                PlaceholderEngine; the mode engines (DatagramEngine with UnreliableUnordered/UnreliableSequenced,
                ReliableOrderedEngine, ReliableLatestEngine + .Receive)
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
    void GetMark(out Mark mark); void Rewind(in Mark mark);   // un-reads the last message event cheaply (the Pend of the session layer)
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
  mailbox lease index (int, −1 empty), flags, and a reserved `Reassembly` field that no engine uses — fragments are not
  reassembled per key but per `(channel, key, sequence)` in the datagram engine's own table (§7.8).
* `Mailboxes`: `int[] mailbox` per key slot + `ulong[] dirty` bitset per channel; transport thread `Interlocked.Exchange` in,
  game thread `Interlocked.Exchange(-1)` out; `PopDirty(Span<int> keys)` scans the bitset with `BitOperations.TrailingZeroCount`.
* `ChannelSendState` / `ChannelRecvState` (64 B each, per channel): counters, next sequence, queue head/tail/bytes, stream id,
  group state, statistics fields (sent/received/dropped/superseded/expired/queueFull/tooLarge/ringDrops/retries/keyTableFull).

## 4. QuiclyPeer (as built: wave C1, steps 1–3)

`public sealed partial class QuiclyPeer : IDisposable`. Transport callbacks are **not** public API: a private nested
`Sink : ITransportSink` forwards them and is exposed as `peer.TransportSink` (a server's `AcceptCallback` returns it; a client
peer hands it to `ITransportConnector.Connect` itself). Creation: `QuiclyPeer.Connect(connector, endpoint, serverName, table,
options, authToken)` (client) and `QuiclyPeer.CreateServerPeer(transport, in info, table, options, admission)` (server).

| File | Owns |
|---|---|
| `QuiclyPeer.cs` | construction, immutable option values, public properties (`Index`, `Tag`, `Role`, `State`, `IsDisposed`, `Epoch`, `SessionId`, `SessionToken`, `HandshakeStatus`, `CloseReason`, `RemoteEndPoint`, `Capabilities`, `Channels`, `RemoteChannelTable`, `LastCallbackFault`), `GetStatistics`, `GetChannelStatistics`, `EstimatedRemoteMicros`, `NextDeadline`/`NextDeadlineMicros` and the split `NextPollDeadlineMicros`/`NextFlushDeadlineMicros` (§4.7), `Dispose` with deferred native free, the transport→game signal word |
| `QuiclyPeer.Send.cs` | `RentBuffer`/`GetBufferSpan`/`ReturnBuffer`, `SendCopy/SendOwned/SendPinned/SendBorrowed/SendGather/SendAsync`, `SendRequestAsync`, `Respond`, `RetireKey`, `BeginBulkSendAsync`, `RequestBulk`: resolve the channel, require `Connected`, build a `SendRequest`, call the channel's engine; the `SendAsync` waiters and the `ThreadSafeSend` front (§7.3) |
| `QuiclyPeer.Flush.cs` | `Flush(tick)`, `FlushAsync` and its watermark (§7.3), the scheduler seam `FlushEngines`, `NextDeadline` bookkeeping |
| `QuiclyPeer.Poll.cs` | `Poll(maxItems)`, `Drain`, `Release`, `Retain`, `RegisterHandler`/`UnregisterHandler`, per-channel drain queues (`ReceiveQueues`), mailbox dispatch, LZ4 decode in Poll, pended-stream resume, the final Closed step |
| `QuiclyPeer.Control.cs` | game thread: handshake (client Hello, server checks + `IPeerAdmission` + HelloAck, `CompleteAdmission`, channel-table answer), `Close`, close linger, ping schedule + `PingClock`, heartbeat, admission timeout, the `StateChanged` queue |
| `QuiclyPeer.Receive.cs` | transport thread: the `Sink`, datagram / container / control-datagram receive, the stream table driver (control stream parsing, preamble → engine, parser events → engine, back-pressure un-read), stream error rules, Pong from the transport thread |
| `QuiclyPeer.Completion.cs` | `WaitAsync`/`Wait`/`GetDeliveryStatus`/`TryCancel`, completion-ring drain and routing |
| `QuiclyPeer.Work.cs` | `HasPendingWork` and the `PeerOptions.WorkSignal` edge (`NoteWork`, re-armed by `Poll`; §4.7) |
| `QuiclyPeer.Shared.cs` | `SendShared` over a reference-counted `SharedLease` (§4.1) |
| `QuiclyPeer.Reconnect.cs` | `Reconnect` and the two resets it drives (§4.8) |
| `PeerCore.cs` | the engine-facing façade (§7) |
| `Engines/*.cs` | the engine boundary, the registry, the placeholder, the mode engines |

**Hand-offs** (ADR 0008). Transport → game thread: a signal word (`Interlocked.Or` / `Exchange`) with the bits Connected, Hello,
HelloAck, PeerClose, TransportClosed, CloseRequest, TableRequest, TableInfo — the data of a bit is written before it is set
(Hello/HelloAck/Close bodies are copied to arrays: handshake and close only); SPSC rings `CompletionRing` (`CompletionEntry`,
capacity 2 × send table: two per entry), `ReceiveRing` (`ReceiveEntry`, 64 B each), `PendedStreams` (capacity = the peer's
unidirectional allowance + 2), pong samples (16) and stream-ping requests (8) — every ring's elements live in a `NativeArray`
(ADR 0008 invariants 5 and 12) and the peer disposes them with its other native tables; per-key mailboxes; `PeerCore.RequestClose(code)` (first request wins), executed by
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
   null, ≤ 8 gather pages, a key above 2^62 − 1 on a keyed channel ⇒ `ArgumentOutOfRangeException`), then
   `engine.Admit(ref SendRequest)` (placeholders answer `NotSupported`). An admitted `Immediate` send then runs one scheduler pass
   (`FlushImmediate`, §4.2) before the call returns (PROTOCOL.md §4.5: eligible now, together with what is buffered).
2. The engine: size against `PeerCore.EffectiveMaxMessageSize(channel)`, key, queue limits;
   `PeerCore.TryAllocateEntry(channel, flags, out slot)` (`QueueFull`); header bytes into the slot's 32-byte block
   (`Entries.GetHeaderBlock(slot)` + `Entries.SetHeaderLength`); payload with `TryRentSend` + `AttachLease` (copy / owned), `SetPayload`
   (pinned, shared), a pin handle in `Entries.PinHandles` (borrowed: `GCHandle.ToIntPtr`; `ReleasePayload` frees it) or a reference on a
   `SharedLeaseTable` (`AttachShared`, see below; `ReleasePayload` drops it); tracked sends with
   `PeerCore.TryTrack(slot, options.Context, out request.Token)`; queue on the channel (intrusive `Entries.Next`). Any failure after
   allocation: `PeerCore.DiscardEntry(slot)` (releases token as Canceled, payload, slot). Nothing reaches the transport in `Admit`.
   The datagram engines' admission as built is in §7.1, the ordered engine's in §7.2.
3. With `PeerOptions.ThreadSafeSend`, a send from a thread other than the game thread (the thread that last entered Poll or
   Flush) takes the path of §7.3 instead: its payload is copied into a send lease and a 64-byte request is queued for the game
   thread.

**`SendShared`** (`QuiclyPeer.Shared.cs`; ARCHITECTURE.md §4.1). `SendResult SendShared(in SendHeader header, SharedLeaseTable table,
in SharedLease lease, int length, SendOptions options = default)` is `SendPinned` over a reference-counted block, so a server
serialises a snapshot once and sends it to N peers with no copy. `SendPayloadKind.Shared` takes the pointer as it is and is **never**
compressed — the caller compresses once before sharing, and compressing per peer would undo the point of the path, so `RawLength` is
0 on the wire — which makes it work on every channel mode that exists today. `EnginePayload.Commit` calls `PeerCore.AttachShared`,
which does one `SharedLeaseTable.Retain` and records the pair in a cold per-slot side table; `ReleasePayload` does the single matching
`Release`: at a datagram's `Sent` notice, when the packer has copied the message into a container, when an ordered channel's carrier
is acknowledged, when the entry is discarded (expired, canceled, refused), when the session closes, and when the peer is disposed
with the send in flight. A rejected admission never retained, so the caller keeps its own count; an empty payload takes no reference
at all. From another thread (`ThreadSafeSend`) the bytes are copied at the call and no reference is taken, like every other
foreign-thread path.

### 4.2 Flush (game thread)

1. `now` once; `DrainCompletions()` (the transport's completion ring, then the completions the game thread queued itself with
   `PeerCore.QueueLocalCompletion`; frees slots before new work); the `ThreadSafeSend` front is admitted and the waiting `SendAsync`
   calls are retried (§7.3); `RunTimers(now)`: admission deadline (Connecting/Handshaking), ping schedule + `PingClock.Advance` +
   heartbeat (Connected), close linger (Closing).
2. While `Connected`: `FlushContext { NowMicros, Tick, MaxDatagramPayload (current), DatagramsEnabled, NextDeadline, BudgetBytes,
   BudgetExhausted, BytesSubmitted, CancelBlockedDatagrams }`; every engine's `Tick(now, ref flush.NextDeadline)`; then
   `FlushEngines(ref flush)`, the scheduler (§7.1): send-cap refill, every channel's `FlushChannel` in `PeerCore.ScheduleOrder`, every
   engine's `Flush`, `Packer.Finish`, the cap charged with the bytes submitted, then the pass's local completions routed.
3. `NextDeadlineMicros = min(timer deadline, engine deadline)`; a pass held back by the send cap lowers the engine deadline to the
   cap's refill time. Then the `FlushAsync` calls whose watermark was reached complete (§7.3); an `Immediate` send's pass does the
   same.

The same pass runs at the end of an admitted `Immediate` send (`FlushImmediate`): no `Tick`, the container tick of the last
`Flush(tick)`, wrapped in the lifetime in-call bit because a continuation run by its local completions may dispose the peer.

Submission primitives (`PeerCore`): `SubmitDatagram(slot, flags)` sends the entry's header+payload pair (1 or 2 segments; sets
`SendEntryFlags.Datagram`); for datagram channels its only caller is the packer (§7.1). `SubmitStream(stream, segments, count, slot,
flags)` sends a segment array whose completion carries the slot's context. Both publish before the call; on a failed call the entry
is back in `Filling` and still owned by the caller.

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
    parser is restored from a `StreamFrameParser.Mark` taken before the event, a whole copy on bulk streams; the call returns `PendingAfter(bytes before the event)`, the stream id goes to
    `PendedStreams` and Poll calls `ResumeStreamReceive(id, 0)`), `ResetStream(code)` or `CloseConnection(code)`.
    The context (`Chunk`, `Header`, `Bulk` and the `Cookie` ref) is valid only during the call (the peer declares it `scoped`);
    copy what you keep. An engine that sets `ChannelEngine.AcceptsWholeMessages` (the ordered and group engines) gets a message
    whose payload lies wholly in the current segment as **one** `Whole` event instead of `Start`, `Chunk`, `End`
    (`StreamFrameParser.TryTakeWholePayload` right after the header): the same size check, reservation-then-lease order and ring
    entry as the three events, and a `Pend` rewinds to the mark taken before the header, so the whole message is un-read. The
    ReliableLatest and bulk engines keep the three-event path (docs/benchmarks/session.md, "Hot-path pass").
  * A parser error or a FIN inside a message: ordered stream ⇒ connection `ProtocolViolation`; group/bulk ⇒ reset `ProtocolViolation`.
    The owning engine gets `OnStreamClosed` **exactly once per stream**, peer-opened or locally opened (reset by either side,
    error, or shutdown complete); events of locally opened streams (peer STOP_SENDING, shutdown complete) are broadcast to every
    engine. `CloseStream` follows shutdown complete.
  * **One close notice per stream, local streams included.** A started stream the peer stops raises *two* transport events — the
    STOP_SENDING and the shutdown that always follows it, because MsQuic completes every started stream with SHUTDOWN_COMPLETE
    and the simulator does the same. A peer stream's own `StreamRecord` collapses them (the tag flips to `Discard` on the first),
    and `HandleStreamAborted` now leaves the same `Discard` record behind for a **locally opened** stream, so the shutdown tells
    no engine a second time. Every engine may therefore release a stream's resources — its group record, its staging lease, its
    stream slot — on the first notice, which is what ADR 0008 ("released exactly once on every path") requires of it. An engine
    that also keys its own state by a serial per stream (the ordered and group engines do) is then protected twice over.
  * Receive results: the peer consumes a whole indication (`Consumed(all)`) or returns `PendingAfter(n)`; it never returns a partial
    `Consumed` and never `Consumed(0)` for a non-empty indication, because both mean back-pressure in the `ReceiveResult` contract.
    `ResumeStreamReceive` is called only from Poll on the game thread, never from inside the receive callback; a resume that races
    the returning callback takes effect when it returns.
  * **Mid-message idle** (PROTOCOL.md §7, `PeerOptions.StreamIdleTimeout`, default 30 s). The transport thread arms
    `StreamRecord.MidMessageMicros` on every accepted message event and clears it at the message's end (and when the stream is
    given up), so a stream is watched only while a half-received message pins its staging lease and its ring reservation.
    `RunTimers` (game thread, `now` read once) resets every stream whose stamp is older than the timeout with
    `AbortStream(Timeout, Both)`, counts `PeerStatistics.StreamIdleTimeouts` and lowers `NextDeadline` to the oldest stamp's
    expiry. It does **not** free anything itself: the transport's shutdown-complete takes the ordinary `OnStreamClosed` path, where
    the owning engine releases the lease and cancels the reservation on the transport thread (ADR 0008 invariant 4), which covers
    every mode's engine without a timer of its own. This sweep is the only game-thread reader of the stream table: per slot it reads
    one 64-bit stamp and one packed id from a snapshot of the record array and disarms an expired stamp with a compare-exchange.
  * **`CloseStream` is deferred by the transport.** The peer calls it from the transport thread after shutdown complete
    (`HandleStreamShutdownComplete`), and three engines call it from inside a scheduler pass:
    `ReliableOrderedEngine.AbandonStream`, `GroupStreamEngine` on a synchronously refused start, and
    `ReliableLatestEngine.TrySendLarge` on its refusal path (a `SubmitStream` the peer's stream limit refused releases the
    stream it had just opened). ADR 0008
    invariant 7 wants `StreamClose` off callback threads and outside passes, so the session layer relies on the transport deferring
    the real close: the MsQuic transport moves every `StreamClose` onto its cleanup work item (docs/benchmarks/msquic-transport.md
    R1/R2) and the simulator does the equivalent. A transport that closed the stream inline from those calls would break the
    invariant, so this is a requirement on new transports, not an accident.
* Control stream (both directions start with `0x00`; `StreamFrameParser(Control)`; bodies used in place or assembled in a lazily
  grown array; control-rate limit): server — the first frame must be Hello, a second Hello or a HelloAck is a violation, the Hello body
  is copied and signalled (malformed ⇒ violation; version ≠ 1 ⇒ answered with status 1); client — the first frame must be a HelloAck
  (Informational, or Accepted with epoch 0 ⇒ violation), later only Informational HelloAcks (table answers); Close anywhere; after
  admission Ping (answered on the stream by the game thread), Pong, Latest*/Bulk*/KeyRetired (routed to engines), ChannelTableRequest
  (server); wrong-direction or undefined types and a FIN without Close ⇒ `ProtocolViolation`.
* After Close was sent or received, or a local violation was queued, all further input is ignored (PROTOCOL.md §6).

### 4.4 Poll (game thread)

1. `now` once; `DrainCompletions()` — with `CompletionMode.PollOnly` tracked-send continuations run here; the `ThreadSafeSend`
   front is admitted (an `Immediate` request among them runs one scheduler pass here, on the game thread) and the waiting
   `SendAsync` calls are retried (§7.3).
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
6. Closed: engines' `OnPeerClosed`, requests still in the `ThreadSafeSend` front dropped (leases returned), every receive lease
   released, waiting `SendAsync` calls completed `NotConnected` and `FlushAsync` calls completed, the Closed event raised last;
   afterwards Poll/Flush do nothing and no handler or event runs.

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
* Rate limits: control messages 2 000/s (burst 2 000) ⇒ close `LimitExceeded`; Pongs 4/s (burst 32) ⇒ excess `PingsIgnored`.
* Close: `Close(reason)` ⇒ Closing, a Close frame on the control stream (when open), the transport close with the same code when
  that frame is delivered or after `CloseLinger` (0 = at once); a received Close ⇒ Closing and an immediate transport close with the
  same code; a transport close without Close ⇒ Closed with Source Peer/Local/Transport. The first reason wins.
* LatestAck/LatestReject coalescing (`AckDelay`) and KeyRetired sending belong to the ReliableLatest engine (§7.6), which
  reaches the control path through `PeerCore.SendControlFrame`.

### 4.6 Engines

* Wave C1 step 2 (done, §7.1): `UnreliableUnorderedEngine` and `UnreliableSequencedEngine` over the shared `DatagramEngine` base
  (per-key acceptance with `ReceiveKeyTracker` over `KeyTable`/`DenseKeyTable` and LRU eviction, coalescing mailboxes), the
  scheduler (`QuiclyPeer.FlushEngines`) and the packer (`DatagramPacker`).
* Wave C1 step 3 (done, §7.2): the `ReliableOrdered` engine (lazy persistent stream, carrier gathers, refused-start retry,
  progressive receive, back-pressure); the async APIs and the `ThreadSafeSend` front (§7.3).
* Wave C2a (done, §7.6): the `ReliableLatest` engine (per-key values, retransmission, group streams for large values,
  coalesced acks, key retirement). Wave C2b (done, §7.5): the `GroupStreamEngine` (`ReliableUnordered`). Wave C2c (done,
  §7.7): the `BulkEngine`. Wave C2d (done, §7.8): fragmentation in the shared datagram engine and request/response in the
  ordered engine. Wave C2 is complete.
* `PingClock` is peer-level (`Session/PingClock.cs`), not an engine.

### 4.7 Host hooks (work signal, split deadlines, robustness)

* **Work signal.** `PeerOptions.WorkSignal` (`IPeerWorkSignal { void OnWork(QuiclyPeer peer); }`) is called by
  `QuiclyPeer.NoteWork()` the first time the peer publishes game-thread work after each `Poll`, so a host can wake a sleeping game
  thread instead of polling idle peers. It is an **edge with set-once semantics**: one `Interlocked.Exchange` on a word guards the
  call and `Poll` re-arms it at entry, so a burst of a thousand messages costs one host call — and work that Poll does not consume (a
  message of a channel without a handler, engine work that needs a `Flush`) raises no second call, because `HasPendingWork` is the
  level. Raised from the transport thread by `Signal(bit)` (every handshake, close and table signal), `TryEnqueueReceive`,
  `PublishReserved`, a coalescing mailbox post (the datagram engine's and the ReliableLatest engine's: a value, whether it came
  in a datagram or on a group stream, and a key retirement), `PushCompletion`, `NotePendedStream`, the pong / stream-ping rings,
  the ReliableLatest engine's pass work (an ack or reject it owes, a LatestAck / LatestReject / stream notice to apply —
  §7.6) and the Bulk engine's (a control frame from the peer, a notice of one of its own streams, bytes accepted, a retired
  receive — §7.7); and from the game thread by `SetState` (a queued `StateChanged` is work, which covers `Close` and `CompleteAdmission` without a hook of their
  own), `QueueLocalCompletion`, a send queued by another thread and a bulk transfer the application started; and from any thread by
  `BulkTransfer.Cancel`. Allocation-free — two field reads when no signal is configured —
  and a host exception is wrapped like a transport callback fault (counted, kept in `LastCallbackFault`, turned into a queued
  `InternalError` close), so it never reaches the transport. **Inside a transport callback the signal is raised once per
  callback, not once per publication**: the sink's receive and completion callbacks are wrapped in
  `PeerCore.BeginTransportCallback`/`EndTransportCallback` (nesting counted, `End` in a `finally`), transport-thread publications
  call `NoteTransportWork`, which only sets a pending flag while a callback is open, and `EndTransportCallback` makes the one
  `NoteWork` after the last publication. Every publication still precedes the exchange, so the game thread's re-arm-then-probe
  handshake is unchanged; a plain-read "exchange only when clear" shortcut was rejected because the read could pass the ring store
  (x86 store-load reordering) and lose a wake-up. Outside a callback `NoteTransportWork` is `NoteWork`.
* **`HasPendingWork`** answers whether anything is really waiting: the signal word, the completion ring and the local completions,
  the receive ring, the per-channel drain queues and the held entry, the mailbox dirty bitsets (`Mailboxes.HasDirty`),
  `PendedStreams`, the pong and stream-ping rings, the `ThreadSafeSend` front, a state transition that has not been raised, a due
  deadline (`NextDeadlineMicros` passed), and — while `Connected` — the engines' pass work: the ReliableLatest engine's
  (`ReliableLatestEngine.HasUnsentControl`: acks and rejects owed, notices to apply) and the Bulk engine's
  (`BulkEngine.HasPassWork`: control frames and stream notices to apply, a transfer started or a notice applied since the pass
  last served the send lists, a cancel asked for, progress owed for bytes accepted or for a retired receive). `false` once the
  session is closed or the peer disposed. The rings, the bitsets and the signal word are read with acquire semantics; the game
  thread's own bookkeeping is read plainly, so a foreign caller gets an advisory answer — which is why a host wakes on the edge and
  decides on this probe from its game thread. Every publication that raises the edge is in the level, so a host that woke never
  finds the probe clear with the edge still set (it would then skip the `Poll` that re-arms it and sleep through the next
  publication; `QuiclyServer.PollAll` instead keeps such a slot marked and probes it on every call until the peer's next poll
  deadline, which is what a Bulk peer did before 2026-09-19). The pass work stays in the level until a `Flush` consumes it (owed
  acks can wait out `AckDelay` there, owed bulk progress its 100 ms window); `QuiclyServer.PollAll` therefore keeps a peer marked
  after its Poll only for what a Poll serves (`HasPendingPollWork`: the queues, and the *poll* deadline only), and the flush
  deadline that Poll brought forward (below) gets the pass work served.
* **Split deadlines.** `NextPollDeadlineMicros` is the peer's own timers (ping schedule, heartbeat, admission timeout, close linger,
  the mid-message stream idle sweep). `NextFlushDeadlineMicros` is the engine work only a scheduler pass can serve (retries, expiry,
  the send cap's refill time), and is `long.MaxValue` unless the session is `Connected`. A pass computes it from what it saw, so
  every `Poll` of a `Connected` peer brings it forward to the pass work left since (`ReliableLatestEngine.LowerControlDeadline`,
  `BulkEngine.LowerPassDeadline`). Owed ReliableLatest acks and rejects are due `AckDelay` after the first Poll that saw them —
  the longest delay `PeerOptions.AckDelay` allows — and not before the coalescing window of the last transmission ends, so a
  host flush that comes sooner (its tick) still carries them with its other traffic instead of costing a packet of their own;
  acks the last due pass could not send at all (no carrier fits a batch) are left out, because bringing the flush forward for
  them would make a host that sleeps until the deadline spin on a Flush that cannot send them. A bulk transfer's discrete
  events — a range request, cancel or reject from the peer, a stream notice, a transfer the application started or cancelled,
  a retired receive's final progress — are due now, and a live receive's progress at its window; the peer's `BulkProgress`
  frames (every 64 KiB) and LatestAck / LatestReject notices do not move it (the send pump does not wait on progress, and the
  pass that would retransmit a value applies its ack first). Before this (up to 2026-09-18) owed acks waited for the host's
  next `Flush` — at 60 Hz up to 16.7 ms, against `AckDelay` 5 ms and a 20 ms minimum retry at the sender — and a peer that only
  polled never sent them. `NextDeadline`/`NextDeadlineMicros` stay the minimum of both, so a host with one loop is
  unaffected; a host that polls on network wake-ups and flushes on its own tick sleeps the polling loop on the poll deadline and
  brings a flush forward to the flush deadline, because `Poll` does not run the scheduler.
* **`PollAll` flushes the due peers only.** The server keeps every slot's flush deadline in a dense array next to the poll
  deadlines (`UpdateDeadlines`); when the earliest is due, `PollAll` scans that array (vectorised, like the poll deadlines) and
  flushes just the peers whose deadline passed (`FlushDue`), with the tick the host last passed to `FlushAll`. Before, a due
  deadline made `PollAll` call `FlushAll` — every peer through the gate — which was acceptable for rare retries but not once a
  received ReliableLatest value can make an ack due between two ticks. `FlushAll` itself (the host's tick, and
  `AutoFlushInterval`) is unchanged.
* **Flush gate (`QuiclyServer.FlushAll`).** A Flush of an idle peer costs about a microsecond of cache misses at 60 Hz, so the
  server asks each peer first (`QuiclyPeer.CanSkipFlush`, internal, `QuiclyPeer.FlushGate.cs`). Every Flush records whether its
  scheduler pass left every engine empty and the admission stamp it saw; the peer is skipped only when since then nothing was
  admitted, no completion reached an engine outside a Flush, no local completion, send from another thread, `SendAsync`/`FlushAsync`
  waiter, queued transition, due deadline, ReliableLatest ack/reject/notice (`ReliableLatestEngine.HasUnsentControl`) or canceled
  request (`ReliableOrderedEngine.HasCanceledRequests`) is pending, the caller is the game thread already, and a fragmenting
  table's RTT has not moved. A peer whose only pending item is the completion ring gets it drained first (its own ping traffic
  touches no engine) and is asked again. A skipped peer still records the tick, the pass clock and the ping clock's slew — the only
  things an empty Flush changes. The gate is off for tables with a Bulk channel and when `StreamIdleTimeout` is under twice the
  `PingInterval`. A send returned before `FlushAll` is transmitted by that call; a send racing it from another thread goes out in
  this call or the next, as with an unconditional Flush. On real MsQuic loopback with 1 000 idle peers `FlushAll` fell from
  ~1.0–1.5 µs to ~0.3–0.4 µs per peer per tick.
* **Activation marks the slot.** `QuiclyServer.ActivatePending` marks every slot it activates: a `PollAll` that read the activation
  count just before the listener queued a peer, then took the slot's work bit, found an empty slot and dropped the bit — and the
  peer's work signal fires only once until its first Poll, so it was then polled only at its admission deadline, after the client's
  own admission timeout (seen as connection-storm timeouts).
* **`IsDisposed`** lets a host skip a peer it disposed from inside a handler without catching `ObjectDisposedException`.
* **A throwing `StateChanged` handler** cannot stall the state machine: `RaiseTransitions` takes a transition out of the queue
  *before* invoking the handlers, so the same change can never be raised again by the next `Poll`; the exception is counted as
  `PeerStatistics.CallbackFaults`, kept in `LastCallbackFault` and swallowed, and the transitions behind it still go out. Receive
  handlers keep propagating: they run on the host's own dispatch and hold no session state.

### 4.8 In-place resume (`Reconnect`, PROTOCOL.md §4.1)

`void Reconnect(ITransportConnector connector, EndPoint endpoint, string? serverName, ReadOnlySpan<byte> authToken)` resumes a
**client** peer's session over a new transport after the connection was lost (transport close, link loss, heartbeat timeout).
Preconditions: client role, not inside `Poll`/`Flush`, and the lost transport has reported its close
(`PeerCore.IsTransportClosed`), so no callback can race the reset.

1. `SettleLostConnection()` does what the Closed step of `Poll` does, for a host that has not polled that far: completions drained,
   every engine's `OnPeerClosed` (queued sends complete `Disconnected`), receive leases and the `ThreadSafeSend` front released,
   waiting `SendAsync`/`FlushAsync` calls completed. Sends that were in flight are already finished, because the `ITransport`
   contract completes every accepted send before `OnClosed`.
2. The auth token is replaced, the resume token becomes the token of the last HelloAck and `LastEpoch` the current `Epoch`.
3. `PeerCore.ResetForReconnect()`: every engine's `OnReconnecting()` (stream ids, phases, half-received messages, queued notices),
   then any entry still allocated completes `Disconnected` (`AbandonEntries`), the completion ring and `PendedStreams` are emptied,
   the transport reference, admission, the datagram capabilities, a client's session message cap and the receive reservations go back
   to their pre-handshake values, and the stream table, the container packer and the segment arena are cleared. Kept: the channel
   table, the engines, every counter, the budgets and `Epoch` — the epoch is what the Hello presents as `LastEpoch`.
4. The peer's own per-connection state starts over (signals, handshake bodies, control stream id, close reason, the ping clock and
   its RTT window, the admission deadline) and the lifetime word's ClosedSeen bit is cleared: the new transport will call back again,
   so `Dispose` must not free the native memory early. The rate limiters keep their state on purpose — they are per peer, not per
   connection, and refill with time.
5. `PeerState.Reconnecting` is queued and the new transport attached. `OnTransportConnected` treats `Reconnecting` like
   `Connecting`, so `Reconnecting → Handshaking` opens a control stream and sends Hello with the session token and `LastEpoch`; the
   admission deadline covers `Reconnecting` too. On acceptance the engines get `OnEpochReset(resumed: epoch > 1)` and the
   PROTOCOL.md §4.1 channel rules apply (`UnreliableSequenced` tables reset, in-flight `ReliableOrdered` sends already completed
   `Disconnected`, live `ReliableLatest` keys re-queued and resumable `Bulk` transfers re-requested by their engines in wave C2).

Kept across the resume: the registered handlers, `Index`, `Tag`, the `StateChanged` subscribers, the channel table and every
statistic.

**A failed attempt leaves the peer re-armable.** When the connector throws, returns no transport (`InvalidOperationException`) or
the attempt fails for any other reason *before* a new transport is attached, `RestoreLostConnection` puts the peer back where the
lost connection left it and the exception reaches the caller — the peer is never left half re-armed, and never left waiting in
`Reconnecting` for a close callback that cannot come. Restored: the state and the queued transitions, `CloseReason`,
`HandshakeStatus`, the remote table, `RemoteEndPoint`, the ping clock `GetStatistics` publishes, the deadlines, the connection
start and the session fields step 2 replaced. `PeerCore.MarkTransportClosed` and the lifetime word's ClosedSeen bit are set again
— the mirror of what step 4 cleared for a transport that never arrived, so `Dispose` still frees at the end of the call — and the
close is replayed through `OnTransportClosed`, the routine the transport's own close signal runs, so a host that had not polled
the loss yet still gets its `Closed` transition exactly once and a host that had polled it gets no second one. Not restored,
because a closed peer no longer uses them and the next attempt would clear them again: the lost transport (disposed), the
per-connection tables of step 3, and the signals, handshake bodies and pong samples of the lost connection. What step 1 finished
stays finished.

**`bool CanReconnect`** is the cheap probe for that state: `true` for a **client** peer whose connection is closed with its
transport close observed (`PeerCore.IsTransportClosed`), whose native state is alive (not disposed, not freed) and with no
`Poll`/`Flush` on the stack — exactly the preconditions above, so a host asks instead of catching. Allocation-free (field reads
only, nothing touched), so a reconnect policy may ask it every pass. It stays `true` after a failed attempt. It says nothing about
whether the *session* is still resumable: the server's registry decides that from the presented token (grace period, replay,
resume rate), and the host's policy decides whether the close is worth retrying. A **server** peer always answers `false`, for the
protocol reason below.

**The server cannot do this, and that is a protocol consequence, not an omission.** A listener's `AcceptCallback` must return an
`ITransportSink` synchronously, before a single QUICLY byte was read, so the server does not yet know which session the connection
belongs to: the token arrives in the Hello, which is parsed *by the sink it already had to hand out*. A resumed connection therefore
always gets a new server peer (`CreateServerPeer`), and the host carries the session identity over when `IPeerAdmission.Admit`
matches the token — copy `Index`, `Tag` and the per-session application state onto the new peer, register the same handlers, answer
`AdmissionResult.Accept` with the session's id and the next epoch, and close the replaced connection's peer with `SessionReplaced`.
Per-peer statistics do not carry over (they belong to the peer object); a host that reports per session aggregates them itself.
`Reconnect` throws `InvalidOperationException` for a server peer.

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
`DatagramsEnabled`, `DatagramSendStateReporting`, `IdealSendBufferReporting` (note 7), `PeerUnidiStreams` / `PeerBidiStreams` (default 0 / 1, like MsQuic before admission),
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
3. Stream flow control is modelled only with `LinkOptions.StreamReceiveWindowBytes` (per stream, like QUIC's MAX_STREAM_DATA): the
   sender holds back packets beyond the receiver's consumed offset plus the window, the receiver raises the limit once its
   application consumed a quarter of the window (one one-way delay later), and a send completes only when all of its bytes were
   delivered, so back-pressure reaches the sender. Without it sends complete while the receiver stays Pending and the receive buffer
   is unbounded.
4. Stream limits count concurrently open streams. The defaults are 1 bidi and 0 uni until `UpdatePeerStreamLimits`.
5. A partial consume without `Pending` (at least one byte) keeps the remainder and indicates it again at the next step, together with
   any data that arrived meanwhile, without waiting for new data (MsQuic re-indicates it right after the callback). Consuming nothing of
   a non-empty indication without `Pending` counts as `PendingAfter(0)`: the stream waits for `ResumeStreamReceive` (`ReceiveResult`
   contract, identical on both transports).
6. Zero-allocation tests need a warm-up that reaches the run's peak of concurrent events, streams and sends; the tables grow to that peak
   and then stay. A stream-per-message workload under jitter needs about 15,000 messages.
7. `OnPeerAddressChanged` is never raised and `AppOwnedReceiveBuffers` is false. `OnIdealSendBufferSize` is raised only with
   `LinkOptions.IdealSendBufferReporting` (default off; the `IdealSendBufferSize` capability follows it), the way MsQuic raises
   it with send buffering disabled: each local stream hears the connection's ideal right after it starts, and every started
   stream still sending hears it again when it grows. The ideal starts at 128 KiB and, whenever the bytes in flight — counted
   up to the reported congestion window, as MsQuic counts only bytes on the wire — reach a new maximum, becomes the first
   value of 128 KiB × 1.5ⁿ above it (at most 128 MiB); it never shrinks, and a stream is never told the same value twice.
8. Link options are fixed at creation. There is no mid-run change of loss, delay or bandwidth; use the targeted drops or a new link.
9. `CloseStream` before shutdown aborts both directions with code 0. Pending completions are still reported (canceled), but the shutdown
   callback is not.
10. A start beyond the peer's stream limit is refused the way MsQuic refuses it: the call returns Success, then
    `OnStreamStarted(StreamLimitReached)`, the canceled completion of a send made with Start and `OnStreamShutdownComplete` follow at
    the next step; the stream never starts (a later start returns InvalidState).
11. The simulator honours `CancelOnBlocked` (`TransportCapabilities.CancelOnBlocked` is true): a datagram that finds its direction's
    serializer busy is canceled, so a session's unreliable datagrams are dropped (`Expired`) while stream data keeps the link busy.
    Tests that want queueing wrap the connector (`FlagRecordingConnector(connector, cancelOnBlocked: false)`).

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

Step 2 (datagram delivery) adds: `DatagramTestKit.cs` (`DatagramTables.Main`, `DatagramKit` — quiet options without pings or
heartbeat, link statistics, wire parsing of containers, hand-written frames — `FlagRecordingTransport`/`FlagRecordingConnector`,
which record send flags and can refuse datagrams, wired through the new `SessionHarness(connector:)` parameter, and a
`NativeMemoryManager` for borrowed memory that is not an array); `SchedulerTests` (packing counted with the simulator's link
statistics, a message sent alone, container tick, container size after an MTU change, a queued message failing after the limit
shrank, priority and admission order, starvation of low priority under the cap, the cap's byte rate and refill deadline, no message
older than its expiry delivered under the cap, `Immediate` sends, send flags, refused datagrams, datagrams unavailable);
`DatagramDeliveryTests` (unordered under loss/reorder/jitter, sequenced newest-only per key under reorder, the 16-bit wrap,
coalescing on vs off at 60 Hz, unordered coalescing, ring overflow, receive budget, LRU eviction and the replay window, dense key
spaces, a second epoch on one engine); `DatagramSendPathTests` (every send path alone and packed, borrowed pins released only when
the transport is done, owned leases and refusals, tracked Delivered/Lost/Expired/Canceled/Disconnected, a transport-canceled
datagram completing Expired, container members completing in both completion modes, compression round trip and the RawLength
rules on the wire, admission refusals, send budget); `DatagramZeroAllocationTests` (60 Hz traffic of 64-byte messages over
unordered, sequenced, coalescing, compressed and tracked channels; a capped scheduler with expiring messages); `SchedulerUnitTests`
(`TokenBucket` overdraft, `ReceiveKeyTracker`, local completions, the idle packer) and `Framing/PackedContainerWriterResumeTests`.

Step 3 (ordered delivery, async APIs, sends from other threads) adds: `OrderedTestKit.cs` (`OrderedTables.Main`, `OrderedKit` —
roomy budgets, payload patterns, engine phase accessors — and `AsyncRefusalTransport`/`AsyncRefusalConnector`, which refuse a stream
start the MsQuic way or synchronously); `OrderedDeliveryTests` (10 000 messages of 1 B … 64 KiB in order and byte-exact under
datagram loss, jitter, reordering, stream packet loss, a bandwidth limit and flow control; one flush of 100 messages = four stream
sends; a message cut into 600 packets; keys, request ids and compression in the frame; gathered pages; movement datagrams flowing
during a large ordered transfer with DropWhenBlocked); `OrderedStreamTests` (stream credit, asynchronous and synchronous refusals
re-sent on a new stream, STOP_SENDING closes the channel, a duplicate persistent stream and an oversized or FIN-cut frame are
protocol violations, the sender's size limit, a full ring and an exhausted receive budget hold the stream back, a message that can
never fit closes with LimitExceeded, expiry before the stream, close completes queued messages Disconnected, the send-table reserve,
the queue limit counts bytes in flight, back-pressure from a receiver that stops polling to QueueFull and back, a segment arena of
eight entries, an Immediate send without a Flush, a message that expires behind the head left out of the stream send);
`AsyncApiTests` (SendAsync at once, waiting in call order, waiting for the send budget, canceled, unreliable, NotConnected and
disposed, from another thread while the ThreadSafeSend front is full; FlushAsync at once, under the send cap,
waiting for stream credit; WaitAsync / Wait / GetDeliveryStatus of both stages; ThreadPool completion; TryCancel while queued);
`ThreadSafeSendTests` (four producer threads, a foreign Immediate send's pass on the game thread, tracked sends refused, every send
path, drops counted, the checks made before a request is queued); `OrderedZeroAllocationTests` (ordered traffic both ways on a
clean link — where the simulator itself allocates nothing, so a window measures only the session layer — the same workload under
jitter and 2 % stream loss as a delivery test, the synchronous paths of SendAsync and FlushAsync, admitting sends queued by another
thread); the CancelOnBlocked capability tests in `SchedulerTests`; and, in the simulator's own suite, `FlowControlTests`.

The hooks wave (§4.7, §4.8) adds `HookTestKit.cs` (`RecordingWorkSignal`, `SharedPool` — a host-owned allocator with its
`SharedLeaseTable` — and `QuietOptions`), `WorkSignalTests` (one signal per Poll for a burst of messages, an idle peer that never
signals, a due timer in `HasPendingWork`, work created by `Close` / `CompleteAdmission` / a foreign-thread send, the
coalescing-mailbox path, a throwing signal, and no allocation in the steady state), `SharedSendTests` (the shared reference released
exactly once on every path: delivered, packed into a container, an ordered carrier acknowledged, a rejected admission, canceled, the
session closed with the send queued, the peer disposed with the send in flight, a foreign-thread copy, argument validation, and zero
allocation), `PeerSurfaceTests` (the split deadlines and their minimum, a send cap's flush deadline that Poll does not serve, a host
loop driven by the two deadlines, `PeerOptions.Clone`/`Validate`, `IsDisposed`, and a throwing `StateChanged` handler that is not
re-raised) and `ReconnectTests` (a full client resume with a new epoch on the same session id; handlers, `Index`, `Tag` and
statistics kept; the lost connection's sends completing `Disconnected` with and without the host polling to Closed; an in-flight
carrier's segment run reclaimed; the engines' second `OnEpochReset`; a refused resume that becomes a fresh session; and the
refusals — a server peer, a live connection, a disposed peer, bad arguments, a call from inside `Poll`, and a connector that throws).

The two review waves add `ReviewContractTests` (an unreliable send is never `Delivered` on a carrier without datagram send state,
a stream stalled mid-message stops pinning the receive budget) and `ReviewPerfTests` (the receive ring is `capacity × 64 B` of
64-byte-aligned native memory, the completion ring holds at most two completions per send entry, and receiving on a channel without
a handler does not allocate in `Poll`), plus `OrderedDeliveryTests.Request_And_Response_Ids_Reach_The_Handler_On_A_RequestResponse_Channel`
for the receive side of PROTOCOL.md §3.1 request ids.

Wave C2a (ReliableLatest) adds `LatestTestKit.cs` (`LatestTables.Main` — hashed, dense, compressed, four-key,
high-priority and single-group latest channels next to an unreliable and an ordered one — quiet and roomy options, engine
accessors including the version-counter test hook, hand-written datagram and group-stream values, and the ack/reject
readers of a raw endpoint); `LatestDeliveryTests` (a value delivered once and acknowledged, the lost final update, the
lost ack recovered by the re-ack, only the latest value delivered, a supersede while in flight, the 32-bit roll-over, the
transmission budget ending in `Failed`, the per-peer retry budget, the epoch resync, key retirement and reuse, retirement
of an unacknowledged value, and every send path keeping its own copy); `LatestStreamTests` (a 16 KiB value byte-exact on a
group stream, a newer version aborting the older stream, `MaxGroups` on both sides, compression deciding datagram vs
stream, stream loss); `LatestReceiveTests` (ack coalescing into one datagram, one ack per key at its highest version with
re-acks for duplicates, a `LatestReject` for every local drop reason, an ack for a channel of another mode as a violation
or a counted drop, and the control-stream fallback); `LatestEdgeTests` (the `FlushAsync` watermark, cancellation, the close
path, every admission refusal, an immediate value, the 30-second version budget, a supersede from the middle of the queue, a
datagram limit that shrinks below a queued value, a large-value stream refused synchronously and asynchronously, the
receive-side rejections of a group stream, the ack sweep after the hand-off ring overflows, and the checks the engine makes
on channels that are not its own); and `LatestZeroAllocationTests` (1 000 keys at 60 Hz, and superseding plus retiring keys).

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
    public virtual void FlushChannel(int channelIndex, ref FlushContext flush);   // scheduler pass over one channel (priority order): hand queued work over
    public abstract void Flush(ref FlushContext flush);                 // engine-level work after every channel's pass (retries, bulk)
    public abstract void Tick(long nowMicros, ref long nextDeadline);    // retries, expiry, timers (every Flush while Connected)
    public abstract void OnSendCompleted(int entrySlot, in CompletionEntry completion);  // final completion, or the early Sent notice
    public abstract void OnEpochReset(bool resumed);                     // when the session becomes Connected (resumed = epoch > 1)
    public virtual bool TryCancel(int entrySlot);                        // default false
    public virtual long OldestQueuedStamp();                             // FlushAsync watermark (default long.MaxValue)
    public virtual void AddStatistics(int channelIndex, ref ChannelStatistics statistics);   // queued / in-flight numbers
    public virtual void OnPeerClosed();                                  // after Closed: complete queued entries Disconnected
    public virtual void OnReconnecting();                                // Reconnect (§4.8): drop transport-bound state (streams, phases, notices)
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
    public virtual void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status);   // a stream this engine opened (routed by the context's mode)
    public virtual bool OnControl(ControlType type, ReadOnlySpan<byte> body, bool onStream, long nowMicros);   // validated, admitted; false = violation / drop
    public virtual void Dispose();                                       // after the transport can no longer call back
}
```

`PeerCore` (Session/PeerCore.cs) is the engine-facing façade. Game thread: `TryAllocateEntry`, `TryTrack`, `AttachLease`,
`SetPayload`, `Entries` (header blocks, cold SoA, `PinHandles`, `Next`/`BatchHead`), `Segments` (`SegmentArena`), `SubmitDatagram`,
`SubmitStream`, `OpenStream`, `CompleteStage`, `CompleteEntry` (accepts Completed, InFlight and Filling entries), `DiscardEntry`,
`ReleasePayload`, `AttachShared` (one `SendShared` reference, dropped by `ReleasePayload`), `ResetForReconnect` (§4.8),
`TryRentSend`/`ReturnSend`, `SendCounters(ci)`, `MapDatagramState`, `MapStreamCompletion`, `MapCompletion`,
`MapSubmitFailure`, `QueueLocalCompletion`/`TryDequeueLocalCompletion`, `Packer`, `ScheduleOrder`, `GetToken`,
`GetUserContext`, `CreateMailbox` (from `Initialize`), `CreateKeyTable(channel)`, `StampAdmission`/`GetAdmissionStamp`/
`LastAdmissionStamp`, `CurrentPassMicros`/`NotePass` (the clock stamp of the current Poll/Flush/Immediate pass, so engines never
read the clock per admitted message: ADR 0008 invariant 9). Transport thread: `TryRentReceive`,
`TryEnqueueReceive`, `TryReserveReceive`/`PublishReserved`/`CancelReservation`, `NotePendedStream`, `RecvCounters(ci)`,
`CountDatagramDropped`, `CurrentSenderTick`, `Streams`. Any thread: `RequestClose(code)`, `ReturnReceive`, `GetPointer`/`GetSpan`,
`ChannelIndexOf`, `GetChannel`, `GetEngine`, `EffectiveMaxMessageSize`, `SessionMaxMessageSize`, `MaxDatagramPayload`,
`DatagramsEnabled`, `DatagramStatesReported`, `StreamCreditGeneration`, `IsAdmitted`, `IsTransportClosing`, `Epoch`, `Clock`,
`ConnectionStartMicros`, `StampReceive`/`RestoreReceive`, `Counters`, `MakeEngineStreamContext`/`TryDecodeEngineStreamContext`,
`CancelOnBlockedHonoured`, `ReceiveBudgetBytes`, `NoteWork` (the host work signal, §4.7). The send paths every engine shares (copy,
owned, pinned, shared, borrowed, gather, LZ4) are `EnginePayload.TryPrepare`/`Commit`/`Release`.

Completion rules. The transport thread validates the generation-tagged context (`Entries.TryTransitionContext`), moves the entry to
`Completed` on a final state (datagram `Acknowledged`/`AcknowledgedSpurious`/`LostDiscarded`/`Canceled`, or `Sent` when the transport
does not report states; any stream completion) and pushes a `CompletionEntry { Final = true }`; **every** datagram entry also gets one
early `Sent` notice (`Final = false`, entry still in flight), tracked or not, because the payload block is released at `Sent`
(ADR 0008 invariant 1) — hence two ring items per entry. Stale contexts are counted. `PeerCore.MapDatagramState` maps an
acknowledgement to `Delivered` and a final `Sent` (a carrier without per-datagram send state) to `DeliveryStatus.Sent`, never to
`Delivered`: PROTOCOL.md §4.3 allows `Delivered` only when the transport reports datagram state. The game thread routes
(`QuiclyPeer.RouteCompletion`) by the entry's channel: 0 → the peer's control traffic, 1 → the packer's fan-out
(`PeerCore.OnContainerCompleted` → `DatagramPacker.OnContainerCompleted`, which routes every member back through `RouteCompletion`),
otherwise the channel's engine. Completions the game thread decides itself — expiry at scheduling time, a cancellation, a refused
submission — are queued with `PeerCore.QueueLocalCompletion` (`CompletionKind.Local` with a `Status`) and routed the same way at the
end of the scheduler pass or at the start of the next Poll/Flush; `PeerCore.MapCompletion` maps any final completion. With
`CompletionMode.ThreadPool` the transport thread also completes the token of a tracked, non-container entry with the default mapping
(unless the entry has `SendEntryFlags.EngineCompletes`); the game-thread `CompleteEntry` is then a no-op for the token and only frees
the slot.

### 7.1 Datagram delivery (as built: wave C1, step 2)

**Scheduler** (`QuiclyPeer.FlushEngines`, QuiclyPeer.Flush.cs). A pass runs in every `Flush` while Connected (after the engines'
`Tick`) and at the end of every admitted `Immediate` send (`FlushImmediate`):

1. the send cap refills — a `TokenBucket` of `PeerOptions.MaxSendBytesPerSecond` bytes per second (0, or 2·10⁹ and more, means no
   cap) whose burst is two flush intervals' worth (the interval clamped to 10 … 500 ms) — and `FlushContext.BudgetBytes = Available(now)`;
2. `FlushContext.CancelBlockedDatagrams = TransportHonoursCancelOnBlocked()` (see send flags);
3. `Packer.Begin`: the current `MaxDatagramPayload` (0 while datagrams are off) and the tick;
4. every channel in `PeerCore.ScheduleOrder` (dense indices by priority descending, id ascending among equals; computed at
   construction) gets `engine.FlushChannel(channelIndex, ref flush)`; within a channel the engine keeps admission order;
5. every engine's `Flush(ref flush)`: work PROTOCOL.md §4.5 schedules after fresh real-time traffic (retries, bulk); the datagram
   engines have none;
6. `Packer.Finish` submits the open container or the held message;
7. `flush.BytesSubmitted` is charged to the bucket (`Consume`; the balance may go negative and later refills repay it); when work was
   held back (`BudgetExhausted`) `NextDeadline` becomes the refill time;
8. after the pass, the local completions it queued are routed (`DrainLocalCompletions`).

The pass is not re-entrant (`_inScheduler`). No user code runs inside it, because every completion it decides is queued, so only a
continuation run in step 8 can start another pass, after the first ended. Budget rule for every consumer: hand bytes over only while
`BudgetBytes > 0`, subtract them, add them to `BytesSubmitted`, set `BudgetExhausted` when stopping for the budget; the last message of
a pass may overdraw.

**Send flags** (`DatagramPacker.SendFlags`, PROTOCOL.md §4.5). `Priority` for messages of channels with priority ≥ 192
(`DatagramPacker.PriorityThreshold`) and for `Immediate` sends; a container carries it when any member does. `CancelOnBlocked` for
unreliable datagrams (a container only when every member is unreliable) and only when the pass allows it:
`QuiclyPeer.TransportHonoursCancelOnBlocked()` is the one place that decides: it returns `TransportCapabilities.CancelOnBlocked`,
which `PeerCore` records at `OnConnected` and again at every `OnDatagramCapabilityChanged` (MsQuic 2.4 and later and the simulator
honour the flag; otherwise blocked datagrams are queued by the transport). Within a pass the transport-call order keeps the priority
order: a stream engine hands the packer's pending container over (`DatagramPacker.SubmitPending`) before its own stream sends, so
datagrams of channels the scheduler reached earlier never wait behind stream data. `DelaySend` is never set: it measured 16-19 %
slower per datagram for tick bursts on MsQuic loopback and a later re-run found no measurable effect either way (the original step
text asked for it on buffered sends). PROTOCOL.md §4.5 and ARCHITECTURE.md §7 record that measurement and that the flag is unused.

**Packer** (`DatagramPacker`, `PeerCore.Packer`; PROTOCOL.md §2.2, ADR 0008 invariant 14). Engines hand it filled `Filling` entries
(`Add(slot, hints, ref flush)`); it answers `Accepted` (it owns the entry for this pass), `Blocked` (budget), `Unavailable` (no
datagrams: keep it queued) or `TooLarge` (header + payload above the current limit). It holds the first message; when a second one
fits next to it, a container opens — its own entry on channel 1 (`PeerCore.ContainerChannelId`, `SendEntryFlags.Container`) with a
send lease of `MaxDatagramPayload` bytes, written with a `PackedContainerWriter` resumed for every message (the new
`PackedContainerWriter(buffer, length, count)` constructor), with the tick flag when `Flush(tick)` passed one — and later messages are
appended while they fit (bytes, and at most 64 messages). A message that fits next to nothing goes out alone, zero copy; so does
everything when no container lease or entry is available. Members join with `Entries.AddToBatch` (reusing `Next`), stay `Filling`,
are never submitted, and their payload is returned as soon as it was copied (their BufferReleased stage still completes at the
container's Sent). The container's `Aux0` counts tracked members. A refused `SubmitDatagram` queues a local completion
(`MapSubmitFailure`: `Disconnected` while closing, otherwise `Failed`).

Fan-out (`PeerCore.OnContainerCompleted` → `DatagramPacker.OnContainerCompleted`): the Sent notice returns the container's lease
and, when a member is tracked, passes the notice to every member's owner; the final completion (transport or local) is copied to every
member (slot and generation replaced) and routed through `QuiclyPeer.RouteCompletion`, so each owner finishes its member with
`CompleteEntry` (which accepts `Filling`); then the container entry is finished. In ThreadPool mode the transport thread signals neither
containers nor members; the fan-out on the game thread completes their tokens.

**Datagram engines** (`Engines/DatagramEngine.cs`, `UnreliableUnorderedEngine`, `UnreliableSequencedEngine`, `ReceiveKeyTracker`;
registered in `ChannelEngines.Create`).

* State per channel of the mode (engine-local index): `ChannelSendState` (FIFO head/tail/count/bytes over `Entries.Next`,
  `NextSequence`) and `ChannelRecvState` (unkeyed sequenced: `LastAccepted`, `HasAccepted`) in native arrays, the resolved default
  expiry, the send hints, a `ReceiveKeyTracker` for keyed sequenced and for coalescing channels, a mailbox (`CreateMailbox(dense,
  MaxKeys)`) for coalescing channels. Entry scratch: `Aux0` = admitted length, `Aux1` = queued, handed to the packer, or finished.
* `Admit`: raw length ≤ `EffectiveMaxMessageSize` (`TooLarge`); a key beyond a dense key space (`KeyTableFull`); `QueueLimitBytes`
  (`QueueFull`); an entry (`QueueFull`); the payload (`OutOfBuffers`): Copy → one lease, compressed into it when that shrinks the
  payload; Owned → the caller's lease (zero copy) or a compressed lease (the caller's lease then returns to the pool at commit);
  Pinned → the pointer (zero copy) or compressed; Borrowed → an array pinned with a `GCHandle` (`SendEntryFlags.Pinned`, freed with
  the payload) or compressed, while memory that is not an array is copied; Gather → the pages copied into one lease (compressed when
  that shrinks it), the pages returned at commit. LZ4 applies when `Compression == Lz4`, the length is at least max(2,
  `MinCompressSize`) and the block is strictly shorter (the destination is one byte short), giving `RawLength = length`; otherwise
  `RawLength` is 0. Encoded size above the current `MaxDatagramPayload` ⇒ `TooLarge` (fragmenting channels: the `AdmitFragmented`
  hook, `NotSupported` until C2). Tracking (`QueueFull` when the completion table is full). Commit: the sequence from the channel
  counter (16/32-bit wrap), header, payload, `Sequences`/`Keys`/`Deadlines` (admission clock + `SendOptions.ExpiryMicros`, or the
  channel's resolved default; 0 = never), the `Immediate` flag, FIFO append. On any rejection the caller keeps its lease or pages.
* `FlushChannel`: from the head — `now > deadline` ⇒ `Expired` counter + local completion `Expired`; `Packer.Add` ⇒ `Accepted`
  (`Sent`, `Bytes`), `TooLarge` (`TooLarge` counter + local `Failed`: the limit shrank after admission), `Blocked`/`Unavailable` ⇒ stop
  with the entry at the head. The queue link is read before `Add` (the packer reuses it).
* `OnSendCompleted`: the Sent notice ⇒ `ReleasePayload` + BufferReleased; final ⇒ `CompleteEntry(MapCompletion)`; a local completion
  of an entry the packer had taken (a refused datagram) un-counts `Sent`/`Bytes`.
* `TryCancel`: only while queued; unlinked, local completion `Canceled` (finished at the next Poll/Flush). `OnPeerClosed`: queued
  entries ⇒ `Disconnected`. `OnEpochReset`: the first call opens the first epoch; a later one on the same engine restarts
  `NextSequence` and has the transport thread clear the receive tables before its next datagram (PROTOCOL.md §4.1). `Tick` and `Flush`
  do nothing: expiry is evaluated when the scheduler reaches an entry.
* `OnDatagram` (transport thread): fragments ⇒ the `OnFragment` hook (dropped and counted until C2); acceptance — unordered: always
  (coalescing: the key's slot); sequenced unkeyed: newer than the channel's last (serial, 16 or 32 bits); sequenced keyed:
  `ReceiveKeyTracker.TryAcceptSequence` (stale ⇒ `Dropped`, a dense key out of range ⇒ `KeyTableFull`); the receive lease
  (`OutOfBuffers`, peer `OutOfReceiveBuffers`); the `ReceiveEntry` (sequence, key, `RawLength`, `Compressed`, `StampReceive(now)`,
  `CurrentSenderTick`); coalescing ⇒ `ReceiveMailbox.TryPost(…, out displaced, out replaced)` (`Superseded`), otherwise
  `TryEnqueueReceive` (full ⇒ the newest is dropped: `RingDrops`, peer `ReceiveRingDrops`); `Received`/`Bytes`.
* `ReceiveKeyTracker`: the channel's key table (a hashed `KeyTable(MaxKeys, 64)` that grows, or a `DenseKeyTable`), the last accepted
  sequence per slot and, for hashed tables, a least-recently-updated list: a full table evicts the key updated longest ago (stale
  values do not refresh a key), and an evicted key re-accepts any sequence when it returns (the PROTOCOL.md §7 replay window). The
  per-slot arrays grow with the slots in use.

**Statistics.** `PeerStatistics.DatagramsSent`, `DatagramBytesSent`, `ContainersSent` and `MessagesPacked` (application datagrams:
messages sent alone and containers; control datagrams are not included); per channel the send counters `Sent`, `BytesSent`,
`Expired`, `QueueFull`, `TooLarge`, `SendKeyTableFull` and the receive counters `Received`, `BytesReceived`, `Dropped`,
`ReceiveSuperseded`, `RingDrops`, `ReceiveKeyTableFull`, `OutOfBuffers`.

### 7.2 Reliable ordered delivery (as built: wave C1, step 3)

**Engine** (`Engines/ReliableOrderedEngine.cs`, registered in `ChannelEngines.Create`). One instance per peer owns every
`ReliableOrdered` channel: a 64-byte `OrderedSendState` per channel in native memory, owned by the game thread (the FIFO over
`Entries.Next`, queued and in-flight counts and bytes, the send stream with its serial and phase, the stream credit generation
read before the last open, the carriers outstanding, the carrier of an unconfirmed start), and a 64-byte `OrderedRecvState` per
channel owned by the transport thread (the peer's stream, the lease and header fields of the message being received, flags).

* `Admit`: `ChannelClosed` once the channel's stream failed; raw length ≤ `EffectiveMaxMessageSize` (`TooLarge`);
  `QueueLimitBytes` bounds the bytes admitted and not yet acknowledged, queued plus in flight (`QueueFull`; a message larger than
  the limit still goes alone); the entry table keeps a reserve of one entry per ordered channel plus one for the carriers that
  drain the queues (`QueueFull` before it is touched, so a full table cannot deadlock); the payload by every send path
  (`EnginePayload`, `OutOfBuffers`; a single gathered page is taken as it is); tracking (`QueueFull`). Commit: the frame header
  (Length, Key, RequestId, RawLength — PROTOCOL.md §3.1, at most 24 bytes) in the entry's header block, the payload segment, the
  expiry deadline (none by default on reliable channels), the admission stamp, FIFO append. `Aux0` = admitted length, `Aux1` =
  queued, in a carrier, or finished.
* `FlushChannel` (every scheduler pass, in priority order): the notices of the channel's streams are applied first; a closed
  channel fails its queue; `Starting` and `Refused` wait; `Blocked` waits until `StreamCreditGeneration` changes; expired messages
  at the head are dropped (`Expired`, local completion). Then carriers go out. A **carrier** is an untracked send entry of the
  channel whose `Aux0` is a run of `PeerCore.Segments` — at most 64 segments: the preamble, then each message's header/payload pair
  (ADR 0008 invariant 1) — and whose batch list (`BatchHead`, the members linked through `Next` in admission order) names the
  messages it carries. The members stay `Filling`, are never submitted themselves, and keep their header blocks and payloads until
  the carrier completes. Stream bytes (preamble, headers, payloads) follow the budget rule of §7.1, and the packer's pending
  container is handed over first (`DatagramPacker.SubmitPending`) so datagrams of channels the scheduler reached earlier never
  queue behind stream data. One `SubmitStream` per carrier, 32 messages per call on an open stream (ADR 0008 invariant 14). The
  stream is opened lazily (`OpenStream(Unidirectional, PeerCore.MakeEngineStreamContext(ReliableOrdered, channel, serial),
  priority × 257)`); the first carrier carries the preamble (in its own header block) and `Start`, and nothing more is sent until
  the start is confirmed.
* **Stream lifecycle** (`StreamPhase`): `NoStream` → (first carrier accepted with `Start`) `Starting` → (`OnStreamStarted(Success)`)
  `Open`. A start the peer's stream limit refuses never reached the peer, so its messages are sent again, first, on a new stream:
  refused synchronously (the send returns `StreamLimitReached`; the stream is released with `CloseStream`, the carrier unwound and
  the messages put back at the head) → `Blocked`; refused asynchronously, as MsQuic and the simulator do
  (`OnStreamStarted(StreamLimitReached)`, then the carrier's canceled completion, then `OnStreamShutdownComplete`, after which the
  peer's shutdown handling calls `CloseStream`) → `Refused` → (the carrier completes canceled: its messages go back to the head and
  their `Sent` counts are taken back) → `Blocked` → (`OnStreamsAvailable` changed `StreamCreditGeneration`) `NoStream` → a new
  stream with the next serial. A stream the peer stops (STOP_SENDING, `OnStreamClosed(aborted)`), one that fails to start for
  another reason, and one that shuts down while open close the channel for the connection (`Closed`): queued messages complete
  `Failed` (`Disconnected` when the connection goes down), in-flight ones with their canceled carriers, and `Admit` answers
  `ChannelClosed`. An `OpenStream` failure other than the limit (`OutOfMemory` while a closed slot is still being released,
  `InvalidState` after shutdown) is retried at the next pass.
* **Notices.** Local-stream events arrive on the transport thread. `OnStreamStarted` (routed by the context's mode; it records the
  stream id and serial per channel in transport-thread arrays) and the broadcast `OnStreamClosed` (matched by stream id) post a
  notice `(channel, serial, kind)` into an SPSC ring (16 per channel); the game thread drains it before every decision that depends
  on it (`Admit`, `FlushChannel`, a carrier's completion, `OnPeerClosed`) and ignores the notices of earlier serials.
* **Completions.** A carrier's final completion frees its segment run. Not canceled ⇒ every member `CompleteEntry(Delivered)`, in
  admission order; both stages complete together, since BufferReleased and Delivered are the same event for reliable streams
  (PROTOCOL.md §4.3). Canceled while the transport closes ⇒ `Disconnected`; canceled after a refusal of the current stream ⇒
  re-queued; otherwise ⇒ `Failed`, and the channel closes. Local completions finish members that never went out (expired,
  canceled, failed). `TryCancel` works while a message is queued (unlinked, local `Canceled`). `OnPeerClosed`: queued ⇒
  `Disconnected`.
* **Receive** (transport thread). `OnStreamOpened` accepts one stream per channel per connection; a second is
  `CloseConnection(ProtocolViolation)` (PROTOCOL.md §3). `Start`: a message that could never be buffered (larger than the receive
  budget or the largest pool block) ⇒ `CloseConnection(LimitExceeded)`; otherwise `TryReserveReceive`, then `TryRentReceive(Length)`
  (nothing for an empty message) — either failing ⇒ `Pend` (the peer un-reads the event and Poll resumes the stream;
  `PeerStatistics.StreamReceivePends`). `Chunk` ⇒ copied into the lease. `End` ⇒ `PublishReserved` (`Compressed` and `RawLength`
  for LZ4, decoded in Poll; `IsRequest`/`IsResponse` when a request id is present). `OnStreamClosed` ⇒ the reservation is cancelled
  and the lease returned, and the peer's mid-message idle sweep (§4.3) resets a stream that stops half way through a message.
  `MaxMessageSize` (session cap included), frame errors and a FIN inside a message are enforced by the
  peer's parser: a connection `ProtocolViolation` on ordered streams.
* **Receive-budget sizing rule.** The staging lease of an ordered message is rented whole, so the engine's limit is
  `min(PeerOptions.ReceiveBudgetBytes, allocator's largest block)`: a message that is within its channel's `MaxMessageSize` but
  above that limit could never be buffered and closes the connection with `LimitExceeded`. A peer's receive budget **and** its
  pool's largest size class must therefore be at least the largest `MaxMessageSize` of any ordered (or group) channel it accepts.
  The defaults satisfy it with room to spare (256 KiB budget, a 256 KiB largest block, 64 KiB reliable `MaxMessageSize`); a host
  that raises `MaxMessageSize`, shrinks `ReceiveBudgetBytes` or supplies a pool without a large size class must keep the rule.
* **Statistics.** Per channel: `Sent`/`BytesSent` at hand-off (taken back after a refusal), `Received`/`BytesReceived` at the end
  of a message, `Expired`, `QueueFull`, `TooLarge`, `ReceiveTooLarge`, and the engine's `QueuedMessages`, `QueuedBytes`,
  `InFlightMessages`, `InFlightBytes` (`ChannelEngine.AddStatistics`). Per peer: `StreamSends`, `StreamBytesSent`,
  `StreamReceivePends`, `StreamIdleTimeouts`.

### 7.3 Async APIs and sends from other threads (as built: wave C1, step 3)

* `SendAsync(header, payload, options, ct)` makes one synchronous `SendCopy`. `QueueFull` or `OutOfBuffers` on a reliable channel
  of a connected session makes it a waiter (a `TaskCompletionSource`, allocated only then) that is retried after the completions
  were drained in every Poll and Flush, in call order per channel (a channel stops at its first send that still does not fit).
  While a channel has waiters, synchronous sends on it answer `QueueFull`, so nothing overtakes them. The task completes with the
  admission result (continuations inline with `PollOnly`, on the thread pool with `ThreadPool`), with `NotConnected` when the
  session closes, and faults with `ObjectDisposedException` on `Dispose`; the token cancels the wait (a canceled waiter is never
  admitted). The synchronous path allocates nothing.
* `FlushAsync(ct)` runs `Flush()`, then compares a watermark: the stamp of the last admitted message (`PeerCore.LastAdmissionStamp`)
  against the oldest stamp any engine still holds (`ChannelEngine.OldestQueuedStamp`: queue heads, and for an ordered channel whose
  start is unconfirmed the first member of its start carrier, because a refused start comes back). Nothing older left ⇒ a
  completed `ValueTask`, no allocation; otherwise a waiter completed after the scheduler pass that hands the last of them over
  (a Flush or an Immediate send), or when the session closes; `ObjectDisposedException` on `Dispose`.
* `WaitAsync`/`Wait`/`GetDeliveryStatus` read the `CompletionTable`; `TryCancel` goes through `PeerCore.EntryOfToken` to the
  channel's engine.
* `ThreadSafeSend`. The game thread is the thread that last entered Poll or Flush (before that, the thread that created the peer).
  A send from any other thread copies the payload into a send lease (`TryRentSend`, whose budget accounting becomes atomic; an
  owned lease moves as it is), writes a 56-byte `ForeignSend` (lease, key, context, expiry, length, channel, mode) into a Vyukov
  `MpscRing` (with its sequence word a 64-byte slot; capacity `SendTableCapacity`) and answers `Admitted` without a token
  (`QueueFull` when the ring is full; tracked sends answer `NotSupported`, because tokens belong to the game thread). The game
  thread admits the requests at the start of Poll and Flush as owned sends, oldest first; a request that does not fit yet stays
  first for the next call, and one refused for good is dropped and counted (`PeerStatistics.ThreadSafeSendDrops`). An `Immediate`
  request never runs a pass on the producing thread: Poll runs one after it drained the ring. A `SendAsync` from another thread
  retries every millisecond while the ring is full. Requests of one thread keep their order; there is no order between threads.

### 7.4 Seams for wave C2

* A mode engine replaces its placeholder in `ChannelEngines.Create` and reuses what the ordered engine uses: `EnginePayload` for the
  send paths; carriers over `Segments` runs for stream gathers; `PeerCore.MakeEngineStreamContext` for every stream it opens (the
  peer routes their `OnStreamStarted` by the context's mode); an SPSC ring of notices for local-stream events; `QueueLocalCompletion`
  inside passes; `StampAdmission` at commit with `OldestQueuedStamp` for `FlushAsync`; `AddStatistics`.
* Group streams (`ReliableUnordered`: §7.5, as built; large `ReliableLatest` values): one stream per group, and the refusal
  handling above applies per stream (a refused group waits for credit and goes out on a new stream: PROTOCOL.md §3.2, "the group
  waits, it never fails"). The peer grants Σ max(MaxGroups, 1) unidirectional streams.
* Request/response (ordered channels with `RequestResponse`): the frame's RequestId is written from `SendRequest.RequestId` and
  parsed into `ReceiveEntry.RequestId` with `IsRequest`/`IsResponse`; `SendRequestAsync`/`Respond` are the engine hooks
  (as built: §7.8, which added `ChannelEngine.RunPollDeadlines` and `ChannelEngine.TryTakeResponse` next to them).
* Fragmentation: the send and receive halves of `DatagramEngine` (§7.1, as built: §7.8).
* Bulk: `PeerOptions.BulkShareOfEstimatedBandwidth`/`BulkMaxBytesPerSecond` and `OnIdealSendBufferSize` are not used yet; bulk
  streams share the send cap and the budget rule of `FlushChannel`.

### 7.5 Reliable unordered delivery: group streams (as built: wave C2b)

**Engine** (`Engines/GroupStreamEngine.cs`, registered in `ChannelEngines.Create`). One instance per peer owns every
`ReliableUnordered` channel. Three native, 64-byte, reference-free structs: a `GroupSendState` per channel (the channel's list of
live groups, the group being filled, queued/in-flight counts and bytes, the group-id counter, the churn deadline, the number of
groups holding a stream), a `GroupState` per group (its message FIFO over `Entries.Next`, its byte count, its group id, its
stream with serial and credit generation, its carriers outstanding, its phase) taken from a free list of
`Σ (3 × max(MaxGroups, 1) + 4)` records, and a `GroupRecv` per accepted peer stream (staging lease, message header fields) from a
free list of `Σ max(MaxGroups, 1)` records. The peer's files are untouched; the only new shared seam is the churn bound
(`PeerOptions.GroupMinInterval` → `PeerCore.GroupMinIntervalMicros`).

* **Groups.** The messages a channel admits between two scheduler passes form one group (PROTOCOL.md §3.2). `Admit` appends to
  the channel's open group and seals it as soon as one more message would pass `GroupMaxBytes` (default 64 KiB), opening the
  next one — so a group fills without waiting for a flush, like a container (PROTOCOL.md §4.5). A message larger than the bound
  is a group of its own. With no free group record the open group simply keeps growing (a send is never failed for that), and a
  channel that has no group at all answers `QueueFull`. Checks and commit are the ordered engine's (`EnginePayload` for every
  send path, `EffectiveMaxMessageSize`, `QueueLimitBytes` over queued plus in-flight bytes, the carrier reserve in the entry
  table, `CurrentPassMicros` for the expiry deadline, `StampAdmission`), with the §3.1 frame header written **without** a request
  id.
* **`FlushChannel`.** The open group is sealed unless `GroupMinIntervalMicros` (default 1 ms) has not passed since the channel
  last opened a stream — then it keeps filling and `NextDeadline` drops to the moment the next stream may open, which is what
  bounds stream churn; a message sent `Immediate` seals its group at once. Then every group of the channel is offered a stream,
  oldest first: expired messages at the **head** of a group that has no stream yet are dropped (`Expired`, PROTOCOL.md §4.5 —
  once a group's stream is open its messages are committed to it). The drop stops at the first message whose expiry has not
  passed, because a group is a FIFO and its messages must reach the peer in admission order: a message given a shorter per-send
  `ExpiryMicros` behind a longer-lived one is therefore dropped only once it reaches the head (it goes out if its group's stream
  opens first). Then a group opens one `OpenStream(Unidirectional,
  MakeEngineStreamContext(ReliableUnordered, **group slot**, serial), priority × 257)`. The context carries the engine's group
  slot where the ordered engine carries a channel index: the peer routes `OnStreamStarted` by the context's mode only. Carriers
  are the ordered engine's (a run of `PeerCore.Segments`, at most 64 segments, the batch list naming the members in admission
  order, `DatagramPacker.SubmitPending` first, the §7.1 budget rule); the first carries the preamble `ChannelId, GroupId` and
  `Start`, the one that takes the group's last message carries `Fin`.
* **Waiting, never failing.** Phases: `Filling` → `Waiting` → `Starting` → `Open`, with `Refused` while a refused start's
  carrier comes back and `Closed` when the group is finished. A start the peer's stream limit refuses (synchronously: the call
  returns `StreamLimitReached` and the stream is released with `CloseStream`; asynchronously: `OnStreamStarted(StreamLimitReached)`,
  the carrier's canceled completion, then the shutdown the peer's handling closes) never reached the peer, so the group's
  messages return to its head in admission order and go out on a **new** stream (a new serial, the same group id) once
  `StreamCreditGeneration` changes. A group never fails for want of credit, and the first refusal in a pass stops the pass from
  opening more streams on that channel. A `SubmitStream` that fails for any *other* reason after the open succeeded leaves the
  group unrefused (no stream limit turned it away), so the next pass opens a new stream for it at once instead of waiting for a
  credit generation that may never change.
* **Transport requirement: a refusal is reported no later than the carrier's completion.** The engine decides what happens to a
  Start carrier's messages when that carrier's completion arrives — re-queue them (the start was refused) or fail the group
  (anything else) — so `OnStreamStarted(StreamLimitReached)` must reach the sink **before** the canceled completion of the send
  that carried `Start`, which is the order the `ITransport` contract states (refusal, canceled sends, shutdown complete). A
  transport that reported the refusal afterwards would make this engine fail a group the peer never even saw. Together with the
  deferred-`CloseStream` rule of §4.3 (the transport must not close a stream inline from a callback or a pass) these are the two
  things a new transport has to get right for the stream engines.
* **`MaxGroups` bounds both directions.** Both ends hold the same table, so the sender keeps at most `max(MaxGroups, 1)` streams
  open per channel: exceeding it would have the receiver reset its own live groups. The slot (like the peer's stream credit)
  returns only when the stream **shuts down**, not when its last carrier completes — the receiver frees its group slot at that
  same event, so this end can never open the stream that would push the peer past the limit. A group therefore keeps its record
  until its stream is gone; `OnPeerClosed` hands the slots back itself, since nothing shuts a stream down after the connection
  is.
* **Completions.** A carrier's completion completes both stages of every member `Delivered` (PROTOCOL.md §4.3),
  `Disconnected` while the connection closes, and re-queues them after a refusal. Anything else fails **that group**
  (`Failed`), and so does a stream the peer stops (STOP_SENDING) or one that fails to start: its queued messages complete
  `Failed`, its record is released and the channel keeps admitting and sending — groups are independent, so nothing here closes
  a channel (unlike §7.2). `TryCancel` unlinks a message that is still queued in its group. Local-stream events reach the game
  thread through an SPSC ring of notices keyed by group slot and serial, drained before every decision that depends on them.
  A record is returned to the free list **exactly once**: the release is idempotent, and it bumps the record's serial as it frees
  it, so no notice and no carrier tag of the stream that record just had can match its next occupant (the serial also rises on
  every open). The peer reports each stream's close once (§4.3), so the two rules are belt and braces — and they are what keeps a
  stopped group from freeing its record twice, which would underflow the channel's group count and hand every later group of
  every channel the same record. A carrier's completion resolves its channel from the carrier's own entry rather than from the
  record, so even a stale carrier could only ever touch its own channel's counters. The channel's list of live groups is doubly
  linked, so a finished group leaves it in one step: a pass that finishes many groups of a channel with a large `MaxGroups` costs
  no walk per release.
* **Receive** (transport thread). `OnStreamOpened` accepts at most `MaxGroups` concurrent peer streams per channel and resets
  the rest with `LimitExceeded` (PROTOCOL.md §7); each accepted stream gets a `GroupRecv` record whose index is the stream's
  cookie. `Start` reserves a receive-ring slot and rents a lease of the frame's `Length` (either failing ⇒ `Pend`), `Chunk`
  copies, `End` publishes — so a group's messages are delivered as they complete, not at its FIN. A message within
  `MaxMessageSize` but above `min(ReceiveBudgetBytes, largest pool block)` resets **its** stream with `LimitExceeded` (the
  §7.2 sizing rule applies, but a group never closes the connection for it). A malformed group is reset
  `ProtocolViolation` by the peer's parser and a stalled one `Timeout` by the peer's mid-message idle sweep (§4.3);
  both reach `OnStreamClosed`, which returns the staging lease and cancels the reservation on the transport thread.
* **Statistics.** Per channel: `Sent`/`BytesSent` at hand-off (taken back after a refusal), `Received`/`BytesReceived` at the
  end of a message, `Expired`, `QueueFull`, `TooLarge`, `ReceiveTooLarge`, and `QueuedMessages`/`QueuedBytes`/
  `InFlightMessages`/`InFlightBytes`. Per peer: `StreamSends`, `StreamBytesSent`, `StreamReceivePends`, `StreamIdleTimeouts`,
  `StreamsReset`.
* **Tests** (`Session/GroupTestKit.cs` — `GroupTables.Main`, `OneStream`/`FourGroups` (the same hash with different
  `MaxGroups`, which the hash does not cover, so one end grants one stream while the other tries four), hand-written group
  streams): `GroupDeliveryTests` (loss in one group never delays another, through the simulator's targeted
  `LoseNextStreamPackets`; 3 000 messages of 4 B … 64 KiB byte-exact under 5 % datagram loss, 3 % stream loss, reordering,
  jitter and a bandwidth cap; a group's messages delivered while its stream is still open; one flush of a hundred messages as
  one group in four stream sends; the `GroupMaxBytes` seal; keys, LZ4 and empty payloads), `GroupStreamTests` (a refused group
  waiting for credit and going out on a new stream, asynchronous and synchronous refusals, the receive-side `MaxGroups` reset,
  a malformed group reset while other groups keep flowing, the mid-message idle timeout, the churn bound with and without an
  interval, a message the receiver could never buffer resetting only its group, a group the peer stops, the queue limit, the
  size limit, cancellation, expiry before the stream opens, close, and a full receive ring holding a group back) and
  `GroupZeroAllocationTests` (a stream opened and closed every tick on a clean link, 0 B per window), and `GroupEngineTests`
  (the `FlushAsync` watermark over a group whose start is still unconfirmed, in-place reconnect dropping the lost connection's
  groups, a segment arena too small for a whole group, an immediate send sealing its group inside the call, the send cap handing
  a group over in pieces, the entry-table reserve, a channel whose group records all wait answering `QueueFull`, receive-budget
  back-pressure, cancelling inside a group, closing with carriers in flight, and the transport-thread paths peer input cannot
  reach, including a `Chunk` outside a staged message). `ReviewGroupTests` holds the wave C2b review findings: a group stream the
  peer stops releasing its record **exactly once** (the send cap leaves the group holding its stream with nothing in flight,
  which is the state the double free needed and the delivery suites never reached), a locally opened stream the peer stops
  reaching the engines exactly once, and a shared lease on a group channel retained and released once per peer (the guard that
  the engine commits every payload through `EnginePayload.Commit`, which is what takes the reference). `GroupStreamTests` adds a
  start that fails for a reason other than the stream limit and is retried at the next pass.

### 7.6 ReliableLatest delivery (as built: wave C2a)

**Engine** (`Engines/ReliableLatestEngine.cs` + `.Receive.cs`, registered in `ChannelEngines.Create`). One instance per peer
owns every `ReliableLatest` channel: a 64-byte `LatestSendState` per channel in native memory (the 32-bit version counter, the
fresh queue, the retry queue, the queued/in-flight counts and bytes, the earliest armed retry deadline, the large-value stream
serial and the credit generation), a `KeySendSlot` per key plus an intrusive list of the keys with a live value (so the retry
sweep visits only those), and on the receive side a `KeyRecvSlot` per key, the channel's coalescing mailbox and the staging
state of the peer's group streams. Every per-key array grows with the slots in use up to `MaxKeys`.

* **Values and transmissions.** A key has at most one *live value*: a send entry that is **never submitted** and owns the
  value's payload lease, its datagram header block, its tracking token, its version (`Sequences`), its key (`Keys`), its raw
  length (`BatchCount`) and its 30-second budget deadline (`Deadlines`). Each *transmission* is a separate untracked entry
  whose payload segment points at the value's bytes (`Aux0` = the value's slot, `Aux1` = the version), so a retry costs no
  copy (ADR 0008 invariant 1). The value's `Aux1` packs its key slot, its outstanding transmissions, its flags
  (`Queued`, `Retrying`, `Superseded`, `Finish`, `Transmitted`) and, once known, the terminal status it still owes.
  Because caller memory is only promised until BufferReleased while a version may live for 30 s, `Admit` always takes the
  bytes into memory the value may still be read from: a copy (or its LZ4 block) in a send lease, or the caller's owned lease;
  pinned, borrowed and gathered payloads are copied. The hand-over itself goes through the shared
  `EnginePayload.TryPrepare` / `Commit` pair, so a `SendShared` block is **retained** rather than copied (one
  `SharedLeaseTable` reference, released exactly once in `ReleasePayload` when the value is acknowledged, superseded,
  canceled or fails — §4.1) and a payload kind added later cannot be dropped here silently. A refused admission gives back
  only what was rented (`EnginePayload.Release`), so the caller keeps its own lease, its pages and its own references.
* **Admission.** Key slot (`KeyTableFull`, never an eviction — PROTOCOL.md §7), size against
  `EffectiveMaxMessageSize` (`TooLarge`), the channel's `QueueLimitBytes` over queued + in-flight bytes (`QueueFull`), one
  entry reserved per channel for the transmissions (`QueueFull`), the payload (`OutOfBuffers`), tracking (`QueueFull`). The
  version comes from the channel's counter; 0 is skipped on the wrap, so 0 always means "no version".
* **Supersede.** `Admit` replaces the key's live value: with no transmission outstanding the old value completes
  `Superseded` at once (a local completion, so no continuation runs inside a pass); with one still in flight it is marked and
  completed when its last transmission completes, because the transport may still be reading those bytes. A still-open
  large-value stream of that key is aborted (`AbortStream`, send direction).
* **Retransmission** (PROTOCOL.md §4.4). `DeliveryStatus.Sent` and a transport acknowledgement both mean only "the datagram
  left this host": the `LatestAck` is the sole source of `Delivered`. A transmission reported `Lost`, `Expired`, `Canceled`
  or refused queues an immediate retry; the backstop is `clamp(1.5 × RTT, MinRetry 20 ms, MaxRetry 1 s)` doubled per attempt
  (`PeerCore.ApplicationRttMicros`, 0 before the first Pong ⇒ `MinRetry`). 16 transmissions or 30 s per version ⇒ `Failed`.
  Retries are handed over in the engine's `Flush`, after every channel's `FlushChannel` (PROTOCOL.md §4.5: after fresh
  real-time traffic), bounded by a per-peer `TokenBucket` of `PeerOptions.MaxRetryBytesPerSecond`, or
  `RetryShareOfEstimatedBandwidth` × the estimated bandwidth — the congestion window ÷ RTT, or `MaxSendBytesPerSecond` when
  the transport reports no window, as `RetryShareOfEstimatedBandwidth` documents — with a 16 KiB/s floor so a stalled key
  always progresses; only with neither a window nor a send cap is there no aggregate budget and the per-version budget alone
  applies. A rate derived from the window moves on every pass, so a rate change carries the bucket's **level** over
  (`TokenBucket.SetRate`) instead of refilling it, or the cap would be reset to a full burst each pass and never bind.
  `Tick` moves due keys to the retry queue and keeps each channel's earliest deadline, so a pass that is not due costs one
  comparison per channel. A value that has had **no** transmission at all (an occupied stream slot, datagrams unavailable, a
  starved send cap) has no retry timer, so `Admit` arms the channel's timer for that value's 30-second budget and `Tick`
  watches that deadline for it — otherwise the budget would never be checked and the value would pin its entry and its
  payload lease for the life of the session.
* **Large values.** A value whose header + payload exceeds the pass's `MaxDatagramPayload` goes out as one group stream
  (PROTOCOL.md §3.2, §8 item 8): `WriteGroupPreamble(channel, version)` + the frame `Length, Sequence = version, Key,
  RawLength` in the transmission's own header block, the value's payload as the second segment of the entry's own pair, one
  `SubmitStream` with `Start | Fin`. The packer's pending container is handed over first, as on ordered channels. A refused
  start (`StreamLimitReached`) releases the stream with `CloseStream` and the value waits until `StreamCreditGeneration`
  changes; a newer version aborts the older stream of the same key.
* **The channel's stream cap binds the sender too.** Both peers read the same channel table, so opening more than
  `MaxGroups` streams on a channel would make the *receiver* reset a live stream of ours (PROTOCOL.md §7) — silent loss of a
  value we believe is on its way. The engine therefore counts its own open streams per channel and holds a large value back
  until one shuts down. The slot is released at **shutdown**, not when the send completes: the receiver frees its own slot at
  the same point, so counting the completion would allow one stream more than it still holds. The transport thread posts the
  shutdown (and a start the peer refused) as a notice, which the game thread applies together with the acks. The pairing is
  by **stream id**, not by count: the slot is recorded against the id of the stream whose `SubmitStream` succeeded, and only
  that id's own shutdown gives it back, so a transport that reports `OnStreamStarted` for a start it then refused cannot
  release a slot the refused stream never took — which would silently let the sender exceed the channel's cap. The peer reports
  each stream's close **exactly once** (§4.3), so the slot is released on that one notice; the release is idempotent by
  construction (the id is gone from the table afterwards) as defence in depth. A `SubmitStream` that fails for a reason *other*
  than the stream limit after the open succeeded does **not** park the channel on the current credit generation — nothing took
  credit, so no credit event is coming — and the value simply goes out on a new stream in the next pass, with its 30-second
  budget as the backstop.
* **Reconnect** (`OnReconnecting`, session-layer.md §4.8). Everything bound to the lost transport is dropped: the per-key
  large-value streams, the per-channel stream counts and credit generation, both queues, the notices the transport thread had
  handed over, the pending ack versions and the half-received values of the peer's group streams (their staging leases go
  back). The peer completes every live value `Disconnected` before the hook runs, so the re-queue of PROTOCOL.md §4.1 applies
  to whatever is still live when `OnEpochReset(resumed: true)` runs — on an in-place resume that is nothing, and the
  application resends; the re-queue matters for a host that drives a new epoch without losing the values.
* **Receive** (transport thread). Only a version newer than the key's last accepted one is accepted (serial arithmetic, 32
  bits), into the key's mailbox — ReliableLatest always coalesces, so it uses **no receive-ring entry and no reservation**
  (PROTOCOL.md §7, ADR 0008 invariant 6) and the `TryReserveReceive`/`PublishReserved` protocol does not apply. Without a ring
  publication to raise the peer's work signal, the engine raises it itself (`NoteTransportWork`, so once per transport callback
  however many values, acks, rejects and notices it published): for a value or key retirement posted into a mailbox, an ack
  or reject it now owes, and a LatestAck / LatestReject / stream notice queued for its pass; `HasUnsentControl` is the level
  behind the pass part (§4.7). Before this (up to 055d68d) none of them raised it, so on an idle `QuiclyServer` peer a received
  value waited for the peer's next poll deadline — its ping, up to `PingInterval` — and a sender that slept on the edge
  completed a value only at its retry timer. An older or duplicate version re-acks the current one, so a lost ack cannot stall
  completion. Local drops answer `LatestReject`:
  1 (`RingFull`) when no receive lease was available or the mailbox had no record, 2 (`TooLarge`) for a group-stream value
  above `min(ReceiveBudgetBytes, largest pool block)`, 3 (`DecodeError`) for a compressed value whose *decoded* size is above
  that same limit (the receiver could never produce those bytes; the wire-format rules themselves are enforced by the framing
  layer), 4 (`KeyTableFull`). At most `MaxGroups` (default 4) peer group streams are accepted per channel; further ones are
  reset `LimitExceeded`. A stream that stops mid-value is reset by the peer's idle sweep (§4.3) and its staging lease is
  released in `OnStreamClosed`.
* **Acks this end owes.** The version to acknowledge lives in the key's `PendingAckVersion` word, exchanged atomically: a
  non-zero previous value means the key is already queued, so the SPSC ring of (channel, key slot) holds at most one entry per
  key; the ring is bounded at 1 024 keys (8 KiB) whatever `MaxKeys` is, and when it overflows a per-channel sweep flag makes
  the game thread walk that channel's slots instead, so no ack is stranded. The game thread sends them in the engine's `Flush`: at most one coalesced transmission per peer per
  `PeerOptions.AckDelay` (default 5 ms), LatestAck first and then LatestReject, as high-priority control datagrams,
  de-duplicated per key by construction (one pending version per key, the highest). The carrier is chosen *before* the batch
  is written, because the two carriers frame a control message differently: a datagram the transport refuses costs that batch
  (the sender's timer retransmits and the duplicate is re-acked) and the next transmission goes on the control stream. A
  transmission whose batch does not fit one datagram continues in further datagrams, at most
  `MaxAckDatagramsPerPass` = 8 per transmission (about 1 300 keys), which is what a 1 000-key channel needs. The `AckDelay`
  window starts only when a transmission really went out, so a pass that could send nothing (no carrier, or a datagram limit
  too small for any batch) does not silently skip that window's acks. `NextDeadline` is lowered to the next ack time so a
  host that sleeps still acks in time — but only while that time is still in the future and `AckDelay` is not zero: with no
  delay the acks go out in every pass, and a deadline at or before `now` would make a host that sleeps until the deadline
  spin instead of sleep.
* **Control-rate sizing rule.** One ack datagram carries about 170 keys (a 1-byte channel, a short key and the 4-byte
  version), so a channel of *N* keys updated at *F* Hz makes the peer receive roughly `N·F / 170` control messages per
  second — about 360/s for 1 000 keys at 60 Hz. That measurement is what sized `PeerOptions.ControlMessagesPerSecond`:
  PROTOCOL.md §7's default is **2 000/s**, which covers that workload with room to spare (one coalesced transmission per
  `AckDelay` — 200/s at the 5 ms default — spanning at most 8 datagrams), so the tests and the benchmark raise nothing. A
  host whose channels hold more keys, or update them faster, sizes the limit with this rule and raises it on **both** ends,
  or the receiving peer answers the ack traffic with `LimitExceeded`. This is the ReliableLatest counterpart of the
  receive-budget sizing rule of §7.2.
* **Acks the peer sent.** `OnControl` validates that every entry names a ReliableLatest channel of this engine (otherwise the
  message is a violation on the control stream and a counted drop as a datagram) and hands the entries to the game thread
  through an SPSC ring, drained before every send-side decision (`Admit`, `FlushChannel`, `Flush`, `Tick`). An ack is
  cumulative per key: one covering the current version completes the value `Delivered` and frees the key; a `LatestReject`
  re-arms the timer (back off, then retry). An ack is evidence only for a version that really left this host, so an entry
  above the highest version ever **transmitted** for that key is ignored: nothing the peer can have received names it.
* **Key retirement.** `RetireKey(channel, key)` completes the key's live value `Canceled`, stops its retries, aborts its
  stream, frees the send slot and sends `KeyRetired` (0x17) on the control stream. A received `KeyRetired` posts a message
  with `ReceiveFlags.KeyRetired` (empty payload, the last accepted version) into the key's mailbox and marks the receive slot
  retired. The slot is **kept until that notice has been delivered**: freeing it at once would let a new holder of the same
  key id post into the same mailbox and displace the retirement, and the application would never hear that the key was
  retired — so a value that arrives for a retired key while its notice is still waiting is answered `LatestReject(1)` and
  the sender retries it. Once the notice is gone the slot is taken over and forgets its versions, so the new holder's first
  value is newer than anything remembered; a key retired and never reused gives its slot back when the table is full (a live
  key is still never evicted, PROTOCOL.md §7).
* **Epoch reset.** A resumed session (`OnEpochReset(resumed: true)`) restarts the channel counter (PROTOCOL.md §1: counters
  are scoped to the epoch) and re-queues every live key at its current *value* under a fresh version — the free full-state
  resync of §4.1 — and asks the transport thread to forget its receive keys before the next value, so the lower versions are
  accepted again. That request is consumed at **every** receive entry point (`OnDatagram`, `OnStreamOpened` and a stream
  message's `Start`), because the first value of a key in the new epoch may well be a large one that never touches the
  datagram path: a reset consumed only there would have such a value dropped as "not newer" against the *previous* epoch's
  version and re-acked, and that re-ack looks cumulative on the sender, which would complete a value `Delivered` the peer
  never received (PROTOCOL.md §4.3, §5). The acks this end still owed are dropped with the epoch, together with the peer's
  acks not yet applied and every key's highest-transmitted version, so no ack of a closed epoch can be sent or believed.
* **Statistics.** Per channel: `Sent`/`BytesSent` per transmission, `Retries` (retransmissions), `SendSuperseded`,
  `Received`/`BytesReceived`, `Dropped` (stale or duplicate), `ReceiveSuperseded` (mailbox replacements), `RingDrops`,
  `ReceiveKeyTableFull`, `ReceiveTooLarge`, `OutOfBuffers`, and the engine's `QueuedMessages`/`QueuedBytes`/
  `InFlightMessages`/`InFlightBytes`.
* **Shared seams added for it** (one region in each file): `PeerCore.SendControlFrame(frame, carrier)` →
  `QuiclyPeer.SendEngineControl` (a coalesced ack batch as a high-priority control datagram — `SendControlDatagram` now rents
  a lease for a frame that does not fit the entry's 32-byte header block — or any control message on the control stream),
  `PeerCore.ApplicationRttMicros` → `QuiclyPeer.ApplicationRttMicros` (the retry timer's input), and
  `PeerCore.AckDelayMicros`/`RetryShareOfEstimatedBandwidth`/`MaxRetryBytesPerSecond`/`MaxSendBytesPerSecond` (option
  values), `ReceiveMailbox.HasPending` over the existing `Mailboxes.Peek` (whether a value posted earlier is still
  unclaimed) and `TokenBucket.SetRate` (a rate change that carries the level). `PeerOptions` gained
  `RetryShareOfEstimatedBandwidth` and `MaxRetryBytesPerSecond`.

### 7.7 Bulk transfers (as built: wave C2c)

**Engine** (`Engines/BulkEngine.cs` + `.Receive.cs`, registered in `ChannelEngines.Create`). One instance per peer owns
every `Bulk` channel. Native, reference-free state: a 64-byte `BulkSendState` per channel (its list of live transfers and
how many hold a stream), a 128-byte `BulkSend` per send transfer (the range, the bytes read / completed / acknowledged,
the wire bytes outstanding, the stream with its serial and credit generation, the phase) and a 192-byte `BulkRecv` per
receive transfer, whose first two cache lines belong to the transport thread and whose third holds the game thread's
progress and request bookkeeping. Both record tables hold `PeerOptions.BulkTransfersPerDirection` entries (PROTOCOL.md §7: two per
direction). Cold side arrays carry the managed references a transfer needs — its `IBulkSource`, its `BulkTransfer`, its
`IBulkSink`, its `IncrementalHash` and the 32-byte hash of its object.

* **Starting a transfer.** `BeginBulkSendAsync(channel, descriptor, source, ct)` validates the range before anything else
  (`Length > 0`, `Offset + Length ≤ TotalLength ≤ 2^62−1`, `Length ≤` the channel's `MaxMessageSize` — PROTOCOL.md §8
  bounds *one transfer*, not the object — and a hash of 0 or 32 bytes), then registers it and answers a `BulkTransfer`
  with progress, a completion and `Cancel()`. A session that is not `Connected`, or one already running
  `BulkTransfersPerDirection` transfers, gets a transfer that is already finished `Rejected` rather than a queue: the
  application decides when to ask again, exactly as a peer's refused `BulkRequest` does. Nothing is read from the source
  until a scheduler pass reaches it. A 64 MiB object is therefore four 16 MiB transfers, two at a time.
* **Scheduling.** All send work happens in the engine's `Flush`, after every channel's `FlushChannel`, because
  PROTOCOL.md §4.5 schedules bulk *after* fresh real-time traffic; `FlushChannel` itself does nothing. A transfer with no
  stream opens one at **priority band 0** (§4.5), below every real-time channel, and the packer's pending container is
  handed over first so datagrams never queue behind bulk bytes. Each piece is one send entry with one pooled block: the
  first carries the preamble and the §3.3 header, the last carries FIN. No segment arena run is needed, because a piece is
  a single buffer rather than a gather.
* **What wakes the pass.** The engine's discrete events are level, edge and flush deadline (§4.7): the peer's control frames
  and this end's stream notices are handed over on the transport thread with one work signal per transport callback
  (`NoteTransportWork`); a transfer the application starts, a notice applied outside a pass (a completion routed by `Poll`
  drains the notices first, so a confirmed start there lets the body go), a cancel, and progress owed are in
  `BulkEngine.HasPassWork`, and a `Poll` brings the flush deadline forward to all of them but the peer's `BulkProgress` frames
  (`_urgentControl` marks the other control frames). The data pump itself — more pieces as the transport completes earlier ones
  and the window reopens — stays driven by the host's flushes, as it was: a flush per completion, or per progress frame, would
  cost a pass per piece. The `FlushAll` gate stays off for tables with a Bulk channel for the same reason.
* **Three gates, and what they bound.** A piece goes out only while the pass's send cap (`FlushContext.BudgetBytes`), a
  per-peer rate bucket and the transfer's send window all allow it. The rate is
  `BulkMaxBytesPerSecond` — an explicit cap, taken as it is — or `BulkShareOfEstimatedBandwidth` × an estimate, with a
  **16 KiB/s floor** under anything derived. The estimate is the congestion window ÷ the RTT; a transport that reports **no
  congestion window** falls back to `MaxSendBytesPerSecond`, and with no send cap either there is no estimate at all and
  **the floor is the rate**: an unmeasured link is not assumed to be a fast one, and a transfer still always progresses.
  A window reported with an RTT **below a microsecond** (a loopback or in-memory carrier) is the one case with no gate: the
  window crosses in no time, so the estimate has no bound, and the pass's send cap plus the transfer's send window are
  what bound the traffic. Every rate is clamped to `TokenBucket.MaxRatePerSecond` (about 2.5 GB/s, the largest rate the
  bucket's integer refill represents after an hour's gap), far above any real link. A rate derived from a moving window
  carries the bucket's **level** across a change (`TokenBucket.SetRate`) instead of refilling it, or the cap would never
  bind. The congestion window and the RTT are read from the transport **once per pass** and
  shared by the window and the rate. The window is `min(IdealSendBufferSize for that stream when the transport reports it, else
  PeerOptions.BulkSendWindowBytes, BulkShareOfCongestionWindow × the congestion window)` — ARCHITECTURE.md §7: stream
  priority alone cannot protect datagram latency, because datagrams and streams share one congestion window. MsQuic
  (send buffering off) reports the ideal per stream right after the start and again whenever it grows with the bytes in
  flight (128 KiB × 1.5ⁿ), so it replaces `BulkSendWindowBytes` from the first report on and the congestion window's share
  caps it; the simulator reports it the same way under `LinkOptions.IdealSendBufferReporting` (§5 note 7), which is what the
  windowing tests run against. The gates count **wire** bytes and the piece is sized from the **object** (`min(remaining, BulkChunkBytes − 128)`): sizing the
  piece from a wire allowance would cut a compressible 64 KiB chunk — a few hundred bytes on the wire — down to a few
  hundred object bytes and turn one transfer into thousands of chunks. Each gate is then charged what the piece really
  cost, and the last piece of a pass may overdraw, as §7.1's budget rule allows.
* **Body.** Raw bytes read straight from the application's source into the block, or, with `Compress`, one
  `ChunkLength, RawLength, bytes` chunk per piece (PROTOCOL.md §3.3), LZ4-compressed into a destination one byte short so
  a chunk that does not shrink is stored uncompressed with `RawLength = 0`. The raw bytes of a chunk are read into a
  native scratch block of `BulkChunkBytes`, rented lazily so a peer that never compresses never holds one.
* **The channel's stream cap binds the sender too**, as on group and large-latest streams: both ends read the same table,
  so opening more than `max(MaxGroups, 1)` streams on a Bulk channel would make the *receiver* reset a live transfer of
  ours (PROTOCOL.md §7). The slot belongs to the transfer whose stream it is and is returned on that stream's **one**
  close notice (§4.3), which is also when the peer frees its own slot and returns credit — never earlier, because a
  transfer that gave its slot back while its stream was still alive would let the next pass open one stream too many.
  Every terminal path of a send transfer therefore goes through **one** function (`TerminateSend`): it resets the stream
  when this end never sent FIN on it, finishes the application's transfer exactly once, and returns the record and the
  slot exactly once. A path that released the slot without ending the stream would leak that stream for the life of the
  connection while the engine's own accounting said the channel was free. A start the peer's stream limit
  refused never reached the peer, so the transfer rewinds to its last completed byte, forgets that its header went out and
  opens a **new** stream once `StreamCreditGeneration` changes. The refusal notice and the canceled completion of the send
  that carried the start may arrive in either order, and one function (`RewindRefused`) runs on whichever is last; a `SubmitStream` that fails for any *other* reason after
  the open succeeded does **not** park the transfer on the current generation — nothing took credit, so no credit event is
  coming — and the next pass simply opens a new stream.
* **Completions.** A piece's completion returns its pooled block (PROTOCOL.md §4.3 releases a bulk buffer per chunk) and
  frees that much window. `Delivered` is **not** a transport acknowledgement: §4.3 defines it for this mode as the peer's
  `BulkProgress` reaching the range's length. That is a *claim* by the peer, so the transfer completes only when **both**
  ends are done with it — the peer confirmed the whole range *and* this end read the range, put every byte on the wire
  with FIN and saw every piece complete; whichever arrives last completes it. The claim is bounded twice, for two
  different questions (ADR 0009: a client parses hostile servers too). What is *reported* is clamped to the bytes whose
  sends completed, which are the bytes the peer can have seen, so `BulkTransfer.BytesTransferred` never counts a byte this
  end did not send. What counts as a *violation* is a claim above the bytes handed to the transport at all: a peer cannot
  have accepted bytes that were never submitted, while a claim running ahead of a completion still in flight is ordinary
  (both are triggered by the same round trip). A violation is a field no honest peer can produce, so the frame is handled
  exactly as its sibling control frames with impossible fields are (PROTOCOL.md §3.4, control-message bounds): as a
  control datagram it is dropped **whole** — neither its bytes nor the confirmation it would imply apply — and on the
  control stream it closes the connection `ProtocolViolation`; either way `PeerStatistics.BulkProgressOverClaims`
  counts it. Without both halves of this a peer could report an object as delivered, with its full byte count, before one body byte
  had left the host and while the application's `IBulkSource` had never been read. A piece canceled while the transport
  is closing ends the transfer `Disconnected`; one canceled after a refused start re-queues; a piece canceled for any
  *other* reason decides nothing, because the transport reports a canceled send before the stop that caused it as often as
  after — the stream's close notice is the verdict (a stop with `BulkCanceled` → `Canceled`, any other stop → `Failed`,
  and a shutdown before FIN with no stop at all → `Disconnected`: only the connection takes a stream down that way, and
  the transport shuts every stream down *before* it reports its own close, so the transport-closing flag is not set yet),
  and the peer delivers exactly one such notice per stream. `OldestQueuedStamp` stays `long.MaxValue`: a bulk object is minutes of
  rate-capped traffic, so `FlushAsync` must not wait for it.
* **Receive** (transport thread). A peer stream is accepted while a record is free and neither the per-direction limit nor
  the channel's `MaxGroups` is reached; the rest are reset `LimitExceeded` (PROTOCOL.md §7). The header is validated by
  the framing layer before the engine sees it, and the engine then checks its own two rules **before any state is
  created**: the transfer id must be free and the application's receive router
  (`PeerOptions.BulkRouter`) must accept the descriptor — with no router every peer-initiated transfer is refused, which
  is the §3.3 default. Bytes are then written **progressively** into the application's `IBulkSink` as they arrive:
  a bulk transfer never becomes a `ReceiveEntry`, so it takes **no receive-ring entry and no reservation** (ADR 0008
  invariant 6, like a coalescing channel's mailbox and unlike every other stream mode), and nothing is ever sized from
  `TotalLength`, `Offset` or `Length`, which are untrusted. A pooled lease is taken only to stage a *compressed* chunk and
  the block it decodes into, both returned at the chunk's end. A transfer whose stream shuts down before its last byte with
  no reset in either direction and no cancel of ours ends `Disconnected`: an early FIN is a parser error, which resets, so
  only the connection going away ends a stream that way.
* **Transfer ids, and the tolerance (PROTOCOL.md §8).** §3.3 makes a transfer id unique per (peer, direction) for the
  epoch. The receiver enforces that over every transfer it still holds any trace of — a running one, and a finished one
  whose record has not travelled back through the retire/recycle rings yet (the record is recycled once its final
  `BulkProgress` is out) — and **tolerates** a reuse after that: remembering every id an epoch has seen would be unbounded
  state the peer controls, and a sender numbers its transfers upward and never reuses one, so a reuse can only confuse the
  peer that made it. `BulkRulesTests.A_Transfer_Id_Stays_Taken_Until_Its_Record_Is_Recycled` pins both halves.
* **Receive sizing rule.** Staging a compressed chunk costs one pooled block of its wire length and one of its decoded
  length, so a peer that accepts chunked transfers needs `min(PeerOptions.BulkMaxChunk, the pool's largest block)` to be at
  least the largest chunk the peer sends, and a receive budget of at least twice that. A chunk above that limit resets its
  stream `LimitExceeded` (the transfer fails, the connection survives); a chunk that merely finds the budget exhausted is
  `Pend`ed and resumed from `Poll`. This end never sends a chunk larger than `BulkChunkBytes` (default 64 KiB, one pooled
  block), which is the counterpart of §7.2's receive-budget rule.
* **Hashing, and precisely what a resumed range does.** The hash of PROTOCOL.md §3.3 covers the **whole object**, so only
  an end that saw every byte of it, in order, can check it. A transfer whose range *is* the whole object
  (`Offset == 0 && Length == TotalLength`) is hashed as it arrives and verified at its last byte: `Verified`, or
  `Mismatch`, which ends the transfer `Failed` even though every byte arrived. A transfer that carries only **part** of the
  object — every resumed range — is **not hashed by the engine at all**: it reports
  `BulkHashState.DeferredToApplication` and hands the sender's hash to the application, which is the only party holding
  the assembled object and can verify it once the last range has landed. Resume is a new transfer for the remaining range
  with the same `ObjectId`/`ObjectVersion`, and the hash value is repeated on every range's header, so it stays comparable
  across them. The sender's own transfer is unaffected by the verdict: it saw every byte accepted and completes normally,
  because acting on a mismatch is the receiver's business.
* **Progress this end owes.** `BulkProgress` (0x05) is sent at most every 64 KiB or 100 ms per transfer, and on
  completion (PROTOCOL.md §2.3). The decision is the game thread's, in the engine's `Flush`: the carrier is chosen
  *before* the frame is encoded (the two framings differ), and the 64 KiB / 100 ms window advances **only when a frame
  really went out**, so a pass that could send nothing does not silently skip a window. `Tick` lowers `NextDeadline` to
  the next frame a transfer owes and never to a time at or before `now`, which would make a sleeping host spin. The final
  frame is exact: the transport thread hands a finished record to the game thread through a ring and the record is
  recycled only once its last frame is out, while the periodic frames read a live record's byte count advisorily (the
  transfer id is read on both sides of it, and progress is cumulative, so a stale read is skipped or harmless). The
  window's timestamp has **one owner**, the game thread (ADR 0008 invariant 4): the transport thread stamps nothing when
  the header arrives, and the first pass that sees the record starts its 100 ms window. A frame that cannot go out — the
  carrier refused it — is held, not skipped: a retired transfer stays pending and the pass stops, and a live one keeps its
  window, so both are tried again on the next pass and the sender completes on the exact final count.
* **Cancelling, and why only one end emits the frame.** `BulkCancel` (0x14) is **receiver-to-sender only**
  (PROTOCOL.md §3.4): it means "stop sending the transfer you are sending to me", so it always resolves against the
  recipient's *send* records — which is what the engine does with it. Transfer ids are scoped per (peer, direction), so id
  1 exists in both directions of one session and the frame has no direction field; a sender that emitted one would name
  the peer's own unrelated outbound transfer and cancel it. `QuiclyPeer.CancelBulk(channel, transferId)` is therefore the
  **only** emitter: the receiving end stops the peer's sending side with STOP_SENDING and sends the frame so the peer also
  stops reading its source. Stopping a peer's stream comes back to *this* end as an ordinary shutdown rather than an abort,
  so the record remembers the code we cancelled with; without that the transfer we cancelled ourselves would end `Failed`.
  `BulkTransfer.Cancel()` (the **sending** end, any thread) sets the flag the application's own transfer object holds, sets the
  engine's cancel level (exchanged back by the next pass before it reads the flags) and wakes the host; the next pass resets the stream with `BulkCanceled` and completes the transfer `Canceled`, and the reset
  *is* the signal — the receiver turns it into a cancelled transfer through that stream's single close notice, and no
  control frame is needed or sent. A sender that abandons a peer-requested transfer before any stream exists — its header
  never went out, or its start was refused and rewound; in practice a provider's source that runs dry on the first read,
  which happens before the stream is opened — answers `BulkReject` (0x15) with the request's id instead, because there is
  no stream to reset (`TerminateSend` decides it, so no exit path can miss it; a disconnect sends nothing). No case is
  left needing a direction bit in the frame. A `BulkCancel` naming no transfer this end is sending — including one that
  crossed its transfer's completion on the wire — is ignored and counted (`PeerStatistics.BulkCancelsIgnored`).
* **Requests and authorisation.** `RequestBulk` sends `BulkRequest` (0x13) and keeps the range in a small table of
  `BulkTransfersPerDirection + 1` entries — three by default, which is what PROTOCOL.md §7's limit row spells out, so a
  further range can be asked for while both transfers are still running. An incoming request is validated on
  the transport thread and applied on the game thread, where `IBulkAuthorizer` decides and `IBulkProvider` supplies the
  object: **both default to deny**, so serving bulk objects is opt-in (ADR 0009), and a refusal — no authorizer, no
  provider, an invalid descriptor or no free transfer slot — is answered `BulkReject` (0x15). A `BulkReject` for a range
  this end asked for is reported to the router. An entry leaves the table when it is answered: by that `BulkReject`, or by
  the transfer that answers it finishing — matched on channel, object identity and first byte (PROTOCOL.md §3.4; the bulk
  header carries no request id, and a provider may shorten a range but never move its start), on the game thread once the
  transfer's final progress is out (`SettleRequest`). The application learns the outcome from its sink. Until this was
  added only a reject or a closed connection released an entry, so after three served requests every further
  `RequestBulk` was silently refused.
* **Epochs and reconnect.** PROTOCOL.md §4.1: every resumable outbound request whose transfer did not complete is asked
  again for the bytes still missing under a fresh request id when the session resumes, and it is the end that *asked* for
  a range that re-asks. "The bytes still missing" is exact: when the lost connection finishes a transfer that answered a
  resumable request `Disconnected`, the request advances past the bytes that arrived instead of being released, and the
  close reports to the router only the requests nothing had answered yet. Transfer ids are scoped to the epoch, so the receive side is told to forget the ids it has seen;
  that request is consumed at **every** receive entry point — a stream's open, its messages and a control message —
  because the first thing a resumed session sees may be any of the three, and the control messages of the closed epoch are
  dropped with it. `OnReconnecting` drops everything bound to the lost transport: streams, phases, records, notices and
  half-received transfers, with their staging leases returned.
* **Dispose.** A peer disposed without having been polled to `Closed` gets no `FinishClosed`, yet every await it handed
  out must still end (ADR 0008; PROTOCOL.md §4.3 gives a transfer terminal states so that its caller always is). The peer
  therefore calls `ChannelEngine.OnDisposing` — **not** `OnPeerClosed`, which runs once the transport has reported its
  close and completes in-flight entries: here the transport is still live, may still be reading a payload it was handed and
  may still be writing into a receive record, so releasing either would give a block the transport is using back to the
  pool. The bulk engine finishes every `BulkTransfer` it is sending `Disconnected` (game-thread state only: streams are
  forgotten, not reset, since the transport is closed right after) and drops its pending requests without calling the
  router back. What the transport thread owns — half-received transfers and their staging leases — is finished when the
  peer frees its memory, which it does only once the transport has reported its close and no Poll or Flush is running:
  the engine's `Dispose` then finishes each such sink `Disconnected`, keeping `IBulkSink.Finish`'s "exactly once".
  Tracked sends' `WaitAsync` complete `Disconnected` from the peer's completion table, and `SendAsync`/`FlushAsync`
  waiters were failed just before. The ordered engine uses the same hook to fail a pending `SendRequestAsync` with
  `ObjectDisposedException` (§7.8); engines run in mode order, so requests fail before bulk transfers finish, and each
  await completes exactly once. `DisposingPeerTests` disposes an unclosed peer with a bulk transfer, a request and a
  tracked send all pending at once. A wait released this way may be consumed only after the peer has freed its memory
  (awaited late, or a thread-pool continuation racing the transport's close callback); consuming it releases its slot,
  and the completion table then skips the return to its already freed free list instead of writing into it
  (`CompletionTable.Dispose`: a return in progress and the dispose agree on which of them frees the ring). The merge of
  waves C2c and C2d found that use-after-free with the test above.
* **Statistics.** Per channel: `Sent`/`BytesSent` per piece (object bytes), `Received`/`BytesReceived` as bytes reach the
  application, `Dropped` (a refused, duplicate or corrupt transfer), `TooLarge`, `QueueFull` (a transfer or request the
  limits refused), `ReceiveTooLarge`, `OutOfBuffers`, and `QueuedBytes`/`InFlightBytes` from the live transfers. Per peer:
  `StreamSends`, `StreamBytesSent`, `StreamsReset`, `BulkProgressOverClaims` and `BulkCancelsIgnored`.
* **Shared seams added for it** (one region in each file): `PeerOptions.BulkRouter`/`BulkAuthorizer`/`BulkProvider`/
  `BulkTransfersPerDirection`/`BulkSendWindowBytes`/`BulkShareOfCongestionWindow`/`BulkChunkBytes`/`BulkMaxChunk` with
  their `PeerCore` accessors (next to the existing `BulkShareOfEstimatedBandwidth` and `BulkMaxBytesPerSecond`),
  `ChannelEngine.OnIdealSendBufferSize` (broadcast like the other local-stream events, so an engine ignores ids it does
  not own — and an **invalid** id is refused before the match, because a cleared slot of the engine's stream table holds
  exactly that and would otherwise "match" record zero) wired from the peer's sink, `ChannelEngine.OnDisposing` (the
  dispose hook above; default nothing), and `QuiclyPeer.Bulk.cs` with `CancelBulk`. `QuiclyPeer.BeginBulkSendAsync` and
  `RequestBulk` already routed to the engine.
* **Tests** (`Session/BulkTestKit.cs` — tables, an object generated from a pattern so a 64 MiB transfer costs no memory
  and is still checked byte for byte, memory sources and sinks, accepting and denying routers, authorizers, a provider and
  hand-written bulk streams): `BulkDeliveryTests` (a byte-exact object whose progress completes the transfer; a 64 MiB
  object as four 16 MiB ranges over a bandwidth-capped link while 60 Hz movement datagrams keep flowing with bounded
  latency; chunked compression shrinking the wire; the whole-object hash verified and a mismatch failing the transfer; a
  partial range deferring the hash; the send window bounding outstanding bytes; both concurrency caps; a start the peer's
  stream limit refused going out on a new stream; an unrouted transfer and a peer with no router at all; a source that
  runs dry), `BulkStreamTests` (cancel from each side, an unauthorised request, a request with no authorizer, an
  authorised request served from a provider, a resumable request asked again after an epoch change, a disconnected object
  resumed byte-exact as a new transfer for the remaining range, hostile and malformed headers creating no state, a huge
  declared object size that never becomes an allocation, a duplicate transfer id, the receive-side `MaxGroups` reset, a
  chunk this peer could never stage, and the session closing under a running transfer), `BulkZeroAllocationTests` (the
  raw and the chunked streaming paths, measured in the middle of one transfer), `BulkLimitTests` (what bounds a transfer and
  what happens when a bound bites: a chunked object over a lossy, reordered, jittery link with its whole-object hash
  verified; a cancellation from each side racing that loss; the pass's send cap holding a pass back and setting the refill
  deadline; an exhausted send table and an exhausted send budget only *delaying* a transfer; a chunk pended because the
  receive budget is held by another transfer's half-arrived one, and a chunk whose decode block cannot be rented resetting
  its stream `LimitExceeded`; the rate rule — an explicit cap taken as it is, a derived estimate floored at 16 KiB/s, the
  gate off for an estimate without a bound, and the floor as the whole rate, really pacing a transfer, on a transport
  that measures nothing — and an explicit cap really pacing a transfer; the send window following the ideal send buffer
  the transport reports, and keeping the configured window without reports; a range request the transfer limit answers
  `BulkReject`; an over-claimed progress frame dropped and counted while an early honest one is not; a progress frame that
  beats the last completion; progress that cannot go out held and sent later; a refused start on the synchronous path and
  on the asynchronous path in the order the simulator does not produce by itself; a provider answering with the wrong
  descriptor; an overflowing notice ring; the structs' declared layout; and a tracked send's wait released on
  `Dispose`), `BulkRulesTests` (the rules the review tightened: an over-claim on the control stream closing
  `ProtocolViolation`; a late progress frame for a finished transfer leaving its published count alone; a `BulkCancel`
  naming nothing this end sends ignored and counted; a served range whose source runs
  dry before its stream exists answered `BulkReject`; a disposed receiver finishing its sink once the transport closed; a
  transfer id held until its record is recycled and free after; the request table's size; a served request leaving the
  table; and a resumable request cut by a disconnect asked again, after a real resume, for exactly the missing part) and
  `ReviewBulkTests` (the wave's review findings: a forged progress claim completing nothing, a sender's cancel leaving the
  peer's own outbound transfer alone, and a disposed peer finishing the transfer it was sending). The simulator's ideal
  send buffer model has its own tests (`Tedd.Quicly.Testing.Tests/Simulation/IdealSendBufferTests`).
* **Follow-up, recorded rather than done.** A ring's `out` value is now undefined when `TryDequeue` returns false
  (ADR 0008 invariant 5), which is what removed the one live instance of this class of bug — a failed dequeue writing
  `default(int)`, a perfectly valid record index, into the field that then "retired" a live transfer. The stronger form,
  storing an integer ring's elements **offset by one** so that no value a failed dequeue can leave behind is a valid index,
  was deliberately not applied: it touches every engine that hands record indices between threads, and no surviving
  instance needs it. The audit was repeated after waves C2c and C2d merged: every `TryDequeue` call site in `src` (the
  fragmentation and request/response code takes nothing out of a ring) either loops on the result or dequeues into a
  local it reads only after a `true` return, and none passes a field as the `out` argument. This is the next step if an
  instance is ever found.

### 7.8 Fragmentation and request/response (as built: wave C2d)

Two features that ride on engines that already exist: fragmentation is the second half of the shared
`DatagramEngine` (`Engines/DatagramEngine.Fragmentation.cs`), request/response the second half of the ordered engine
(`Engines/ReliableOrderedEngine.RequestResponse.cs`). No new engine, no new mode.

#### Fragmentation (PROTOCOL.md §2.1, §7)

* **When.** `Admit` prepares the payload as usual and, when the encoded message does not fit the *current*
  `MaxDatagramPayload` on a channel with `Fragmentation`, gives back the entry of that attempt and hands the **prepared
  payload** to `AdmitFragmented` (a channel without it answers `TooLarge`). The payload is prepared exactly once for both
  shapes: the fragments point into those very bytes, so a compressed message runs LZ4 once into one send lease and a
  borrowed array is pinned once. Because the single-datagram header is one byte shorter than a fragment's (no
  `FragIndex`), a message that gets here always needs at least two fragments.
* **Layout.** `count = ceil(wire / capacity)` with `capacity = MaxDatagramPayload − header(FragCount > 1)`, and
  `size = ceil(wire / count)`, so every fragment but the last has the size of fragment 0 and the last carries the rest
  (1 … size) — the shape PROTOCOL.md §2.1 requires so that a receiver can bound the total from any single fragment.
  `count > 8` ⇒ `TooLarge` (the per-channel `MaxMessageSize` of a fragmenting channel is 8 × 1 100 for exactly this
  reason). `wire` is the *compressed* size when the channel compresses: LZ4 runs once over the whole message and
  `RawLength` (the reassembled size) is repeated in every fragment.
* **Entries.** One **owner** entry holds the message: its payload (a copy, the caller's own lease, pinned or shared
  memory — whatever `EnginePayload.TryPrepare`/`Commit` produced, so a `SendShared` block is retained once and released
  once), its tracking token, its admission stamp, the admitted length in `Aux0` and, packed into `Aux1`, the fragments
  still outstanding, the payload references still held and the worst status seen so far. Each **fragment** is an
  untracked entry whose payload segment points into the owner's bytes — no fragment copies anything — with the base
  engine's own `Aux0`/`Aux1` (queue bytes and phase), so the scheduler, the packer and expiry treat it exactly like any
  other datagram message. A per-peer `int[]` by send-entry slot says which owner a fragment belongs to (-1 = an ordinary
  message, the slot itself = an owner); every allocation this engine makes writes it, so a recycled slot never inherits a
  stale owner.
* **Completions.** A fragment's `Sent` notice gives back one payload reference; when the last one is gone the owner's
  payload is released and the message's **BufferReleased** stage completes — which is what makes pinned, borrowed,
  gathered and shared payloads safe to reuse. A fragment that travelled inside a packed container may get the container's
  `Sent` notice as well: the packer forwards the non-final notice to *every* member of the container as soon as **any** of
  them is tracked (it keeps that count in the container entry's `Aux0`), and a fragment is never tracked itself, so whether
  it is notified depends on the neighbours it shared the datagram with. The reference is given back exactly once either way,
  because a fragment carries a *payload released* bit in `Aux1` that both the notice and the final completion test first: a
  fragment that was notified returns the reference there, one that was not returns it at its final completion.
  Each final completion folds into the owner, worst outcome wins (`Disconnected` > `Failed` > `Lost` > `Expired` >
  `Canceled` > `Sent` > `Delivered`), and the last one completes the message. So a fragmented message is `Delivered` only
  if every fragment was, and one lost datagram makes the whole message `Lost`.
* **The trade-off, on purpose.** Delivery probability is (1 − loss)^FragCount: at 2 % loss a two-fragment message
  arrives 96 % of the time and an eight-fragment one 85 %. Fragmentation exists so that a slightly oversized unreliable
  message still works, not so that large ones become normal — that is what the reliable modes are for — and the cap of 8
  is the bound that keeps the worst case visible.
* **Cancel, expiry, close.** `TryCancel` names the owner (the token's entry) and works only while *every* fragment is
  still queued; it unlinks them all and they complete `Canceled`. Expiry is per fragment at scheduling time
  (PROTOCOL.md §4.5), so a message whose deadline passes is dropped as a whole in one pass. `OnPeerClosed` finishes the
  queued fragments and then completes what is left of their messages `Disconnected`; `OnReconnecting` additionally
  forgets the owner map.
* **Receive** (transport thread). `MaxReassemblies` records per channel (default 16, a contiguous range of one native
  array, 64 B each) hold the partial messages, and the **reassembly scope is `(channel, key, sequence)`** exactly as
  PROTOCOL.md §2.1 says: a fragment belongs to the partial of its own key *and* sequence, so the channel reassembles as
  many messages at once as the cap allows, whether it is keyed or not. That is the normal case, not an unusual one — a
  fragmented message is several datagrams, and §4.5 paces them, so any jitter or reordering between two consecutive
  messages interleaves their fragments.
  The §7 rule that "a newer sequence for the same key abandons the older partial" therefore applies **only where the
  sequence carries ordering**: on an `UnreliableSequenced` channel, where a newer message of a key makes the older one
  obsolete by definition (and a fragment of an older sequence of that key is dropped for the same reason). On
  `UnreliableUnordered` the sequence is nothing but a reassembly id (§2.1), so neither rule applies there and the cap with
  its oldest-first eviction is the whole concurrency bound.
  Order of work, all of it before a buffer is chosen: the framing layer has already bounded the implied total against the
  channel's `MaxMessageSize`; the engine bounds it again against `min(ReceiveBudgetBytes, largest pool block)`; expired
  partials of the channel are swept; the ordering verdict above is applied; `FragCount`, `RawLength` and the fragment sizes
  must agree with what the partial already knows; a duplicate index is ignored. Only then is a buffer rented —
  `min(size × count, limit)` when fragment 0's size is known, the channel's limit when the *last* fragment arrived first
  (its size says nothing about the others; its bytes wait at the front of the buffer and move once the size is known, a
  single memmove in that one case). The clamp matters: a fragment's own §8 bound can pass while `size × count` is far above
  what the channel promised, and renting that would charge the receive budget — and count `OutOfReceiveBuffers` — for bytes
  no message of that channel may ever have.
  One bound needs *both* sizes and so cannot be checked per fragment: the two §8 bounds are lower bounds, so each can hold
  for a set whose real total does not. Whichever of the two sizes arrives second, the engine checks
  `FragmentSize × (count − 1) + LastLength` against the limit and drops the fragment with its partial when it exceeds it
  (PROTOCOL.md §8 says so now; before that it was enforced in one arrival order only).
* **Publishing.** The completed message needs no copy: the partial's own buffer *is* the payload. It goes through the
  mode's `Accept` (so a stale sequenced message is dropped exactly as an unfragmented one would be) and then into the
  key's mailbox or the receive ring, with `ReceiveFlags.Fragmented` and, for a compressed message, `Compressed` +
  `RawLength` — which `Poll` decodes like any other compressed message. Coalescing channels use no ring entry and no
  reservation (ADR 0008 invariant 6); no other path here reserves either, because a datagram message is published in one
  step.
* **Expiry is swept lazily.** 2 × RTT + 100 ms (PROTOCOL.md §7). The window is recomputed by the engine's `Tick` on the
  game thread and read by the transport thread; the sweep itself runs when a fragment of the channel arrives, because the
  table belongs to the transport thread (ADR 0008 invariant 4) and a deadline for it would be a timer nothing else
  needs. A channel that goes quiet therefore holds its partials' buffers until a fragment of it arrives again, until the
  engine consumes an epoch reset (below), or until the peer reconnects or is disposed — a close alone does not free them —
  bounded by `MaxReassemblies × MaxMessageSize` per channel, which is the sizing rule in ARCHITECTURE.md §9.
* **Epoch reset.** `OnEpochReset(resumed: true)` restarts the send counters and asks the transport thread to forget its
  receive tables before the next datagram; the partials go with them (their buffers are returned), because their
  sequences belong to the epoch that ended. The request is a flag, not work: the reset happens inside the *next* datagram
  of any channel of the engine, so an engine that hears nothing after the resume keeps the old epoch's partial buffers
  until it does. **That is accepted, deliberately.** The receive tables belong to the transport thread (ADR 0008
  invariant 4), so clearing them from the game thread would need a handshake for a case that costs nothing: the buffers are
  bounded by `MaxReassemblies × MaxMessageSize` per channel either way (ARCHITECTURE.md §9), they are already counted in
  the peer's receive budget, no fragment of the new epoch can be mixed into them (the reset runs *before* that fragment is
  processed), and the two ways a peer really stops — `OnReconnecting` and `Dispose` — clear the table directly, because no
  callback can arrive then. A host that wants the memory back sooner closes the peer.
* **Statistics.** Per peer: `FragmentedMessagesSent`, `FragmentsSent`, `FragmentsReceived`,
  `FragmentedMessagesReceived`, `FragmentsDropped` (duplicates, inconsistent fields, limits, no buffer),
  `ReassembliesAbandoned` (a newer sequence of a key on a *sequenced* channel, or the cap evicting the oldest) and
  `ReassembliesExpired`. `Reassemblies(channelIndex)` reads the transport thread's count with one volatile read and is
  diagnostics only (ADR 0008 invariant 13). Per channel the
  ordinary counters apply, with one twist worth knowing: **each fragment is one `Sent`** of its channel, because each is
  a datagram the scheduler hands over separately, while `Received` counts reassembled messages.

#### Request/response (PROTOCOL.md §3.1, §4.3)

* **Ids.** Per channel, odd, from a counter that starts at 1 and skips `0xFFFFFFFF` (its response would not fit 32
  bits); the response carries `RequestId + 1`. `Respond` refuses anything that is not a request — id 0, an even id, that
  one odd id, or a channel without `RequestResponse` — with `SendStatus.NotSupported`, and otherwise admits the response
  as an ordinary message of the channel, so requests, responses and plain messages share the channel's order.
* **The table.** At most `MaxOutstandingRequests` = 256 requests per peer wait at once (a 257th is refused). A slot is a
  pooled `IValueTaskSource<ReceiveLease>` created on first use and reused for the life of the peer, so the synchronous
  path allocates nothing: no state machine, no `Task`, no registration unless the caller passes a cancellable token. A
  slot leaves the live set when it is completed and returns to the free list when its value task has been *consumed* —
  the `CompletionTable` contract, for the same reason: the source's version is what invalidates an earlier occupant's
  value task. The free list is a lock-free stack because `CompletionMode.ThreadPool` consumes a value task on a pool
  thread. Matching a response is a scan of the 256 slots, which happens once per response and never per message.
* **Timeouts without a timer.** `ChannelEngine.RunPollDeadlines(now, ref nextDeadline)` is called from the peer's timer
  pass, which both `Poll` and `Flush` run, so a request times out for a host that only polls as well as for one that only
  flushes (ADR 0008 invariant 9). The engine keeps the earliest deadline and publishes it only while it is **in the
  future**: a deadline at or before `now` would make a host that sleeps on `NextPollDeadlineMicros` spin. A timeout
  faults the value task with `TimeoutException` and counts `RequestsTimedOut`; `timeout` 0 means "wait until the response
  arrives, the wait is canceled or the session ends". A positive timeout is never that sentinel: `PeerOptions.ToMicros`
  rounds any positive duration up to at least one microsecond, for every caller — each of them reads 0 as "off" or
  "none" (a disabled heartbeat or stream-idle check, no linger, no group interval, immediate acks), so a truncated
  500 ns value would have switched the feature off rather than made it short.
* **Where a response is matched.** On the game thread, where the message is dispatched: `QuiclyPeer.Poll` (and `Drain`)
  offer every message flagged `IsResponse` to the channel's engine through `ChannelEngine.TryTakeResponse(in
  ReceiveLease)` *before* a handler or a drain queue sees it — a compressed response is decoded first, exactly as a
  dispatched message would be. The engine takes the lease for the awaiter (the application releases it with
  `Release(in ReceiveLease)`); a response no request matches is dropped and counted (`ResponsesUnmatched`). The request
  table is send-side state, so nothing of this happens on the transport thread.
* **Cancellation, close, reconnect, dispose.** Cancelling the wait never cancels the send (ADR 0004): the request still
  goes out and its response is dropped and counted when it arrives. `OnPeerClosed`, `OnReconnecting` and `Dispose` fail
  every outstanding request with a clear reason (`InvalidOperationException` for a closed or lost session,
  `ObjectDisposedException` for a disposed peer), so a request never hangs on a session that ended. `QuiclyPeer.Dispose`
  fails them **synchronously**, through the same `ChannelEngine.OnDisposing` hook the bulk engine uses (§7.7): the
  ordered engine's override hands its request table an `ObjectDisposedException`. The sequence is one for every await:
  `SendAsync`/`FlushAsync` waiters first, then each engine's `OnDisposing` in mode order (requests, then bulk
  transfers), then every tracked send's wait from the completion table; a peer already polled to `Closed` skips it all,
  because `OnPeerClosed` released the same awaits. The engine's own `Dispose` would fail them too, but that runs from
  `FreeResources`, which waits until the transport has reported its close — for a peer disposed without being closed first
  (ordinary teardown) that is much later, and for a transport that reports no close it is never; and after `Dispose` no
  Poll, Flush or Drain runs that could match a response or serve a timeout. An `await` must not outlive the peer that
  handed it out. The request table is the only wait this feature hands out: a tracked fragmented message's
  `WaitAsync`/`Wait` is a wait on the peer's completion table like every tracked send's, with nothing specific to
  fragmentation.
* **Shared seams added for it** (one region each): `ChannelEngine.RunPollDeadlines` and `ChannelEngine.TryTakeResponse`
  (virtual no-ops), the engine loop in `QuiclyPeer.RunTimers`, `QuiclyPeer.TakeResponse` in the Poll/Drain paths,
  the ordered engine's override of `ChannelEngine.OnDisposing` (the hook wave C2c added), and ten counters in
  `PeerCounters`/`PeerStatistics` (seven for fragmentation, three for requests).
* **Where responses are intercepted.** Two places, not five: `Route` (Poll) and the ring loop of `Drain`. Those are the only
  paths that take a message out of the receive ring, and everything else the peer holds — a per-channel queue, the single
  held entry — can only receive what already passed one of them, so neither can ever contain a response. The queue and held
  paths assert that invariant (`Debug.Assert`) instead of testing it again, which is why no channel handler and no `Drain`
  caller can see a response even though only two checks exist. Two tests pin it with both of those structures really in
  use: the drain queues full, a plain message held and the response waiting in the ring behind it, then `Drain` (queue →
  held → ring) and, separately, `Poll` with a newly registered handler (handler-queue loop → held → ring) must hand over
  only the plain messages while the response completes its request.
* **Statistics.** `RequestsSent`, `RequestsTimedOut`, `ResponsesUnmatched`; a request and its response also count as
  ordinary messages of their channel.

**Tests** (`Session/FragmentTestKit.cs` — `FragmentTables.Main` with fragmenting unordered, sequenced keyed, compressed,
small-limit, two-reassembly and coalescing channels, a hand-written fragment writer that can break every rule, and a
splitter that produces a sender's own fragments): `FragmentDeliveryTests` (every fragment count 2 … 8 byte-exact, a lost
fragment dropping only its own message, reordering and duplicates, the last fragment arriving first, partials of
different keys side by side, the newer-sequence abandon on a sequenced channel, compression on top of fragmentation, a
reassembled message in a coalescing mailbox, 80 messages over a reordering, jittering link with the losses **chosen**,
and 150 tracked messages of 2 … 6 fragments over a link that loses 3 % of datagrams from a **fixed seed**, where the
link itself says which messages were whole — `Delivered` versus `Lost` — and the receiver must deliver exactly that set,
byte-exact: in neither test can a tolerance hide a reassembly bug behind link loss); `FragmentEdgeTests` (hostile
`FragCount`/`FragIndex`/empty-payload/oversized-bound frames dropped without closing the connection, fragments that
disagree about their message, a bound above what the receive budget could ever hold, a fragment whose size × count is
above the limit while its own bound is not — the rent is clamped, nothing counts `OutOfReceiveBuffers`, the message that
fits is delivered and the one whose exact total does not is dropped — the cap evicting the oldest partial, the expiry
sweep, a message that would need more than eight fragments refused, every send path fragmenting — including a
`SendShared` block retained and released exactly once, alone and packed beside a tracked message whose container forwards
its `Sent` notice — a tracked message ending `Delivered` and, with one fragment lost, `Lost`, a carrier without
per-datagram send states ending it `Sent`, a message the send cap splits across passes, a refusal for the tracking token
leaving no owner marks, cancel, expiry at scheduling time, close, an epoch reset returning a partial's buffer, a quiet
engine keeping the closed epoch's partial until its next datagram, and a resume that fragments again after the owner map
was reset); `RequestResponseTests` (the happy path, keyed and compressed requests, 64 concurrent requests answered in
reverse order, 32 of them over a lossy, jittering link with plain messages interleaved, timeouts served by Poll and by
Flush, the published deadline moving on to the next request's timeout, a response after the timeout, cancellation that
does not cancel the send, a cancellation that loses to the response, an unmatched response, a response
drained rather than polled — which is also what reclaims a canceled slot — a `Drain` that hands the caller the plain
message and not the response, a response waiting in the ring behind full drain queues and a held message, taken by
`Drain` and by `Poll`, close, a lost connection followed by a resume, a dispose that fails every request synchronously
with `ObjectDisposedException`, a positive sub-microsecond duration that never becomes the zero sentinel, the refusal
matrix and the table's bound); `ReviewFragmentRequestTests` (the review's own three: the real total against
`MaxMessageSize`, interleaved messages both reassembled, and a sub-microsecond timeout that still elapses); and
`FragmentRequestZeroAllocationTests` (fragmented traffic at 60 Hz, and four request round trips per cycle, 0 B per
window).

Waves:

| Wave | Content | Depends on |
|---|---|---|
| C1 step 1 (done) | `PeerCore`, `QuiclyPeer` public API, handshake/control/ping/close, `ChannelEngine` base + registry + placeholder, stream table, completions, Poll/Drain/handlers, statistics | State, Framing, Channels, Control, SimulatedTransport |
| C1 step 2 (done) | packer + scheduler (§7.1), engines for `UnreliableUnordered`, `UnreliableSequenced` (incl. coalescing mailboxes and LRU key tables), all send paths, tracked sends, compression on send, send cap, `Immediate` sends | step 1 |
| C1 step 3 (done) | `ReliableOrdered` engine (persistent stream, carrier gathers, refused-start retry, progressive receive, back-pressure), `SendAsync`/`FlushAsync`/`ThreadSafeSend`, simulator flow control, benchmarks (§7.2, §7.3) | steps 1–2 |
| C2a (done) | `ReliableLatestEngine` (§7.6): per-key values and versions, supersede, retransmission and budgets, large values on group streams, coalescing receive with cumulative acks, ack coalescing, epoch resync, key retirement | C1 |
| C2b (done) | `GroupStreamEngine` (`ReliableUnordered`, §7.5): groups and carriers, refused starts, per-group failure, progressive receive | C1 |
| C2c (done) | `BulkEngine` (§7.7): a stream per transfer at the lowest priority, the send window and rate cap, chunked compression, progress, cancel, resume, request authorisation, the whole-object hash | C1 |
| C2d (done) | fragmentation in the shared datagram engine and request/response in the ordered engine (§7.8): at most 8 fragments per message with a bounded reassembly table, correlated requests with a pooled value-task source and timeouts served by Poll and Flush | C1 |
| C3 (done) | `MsQuicTransport` (ITransport over the MsQuic wrappers) + listener/connector; `QuiclyServer` / `QuiclyClient`; admission; reconnect | C1, msquic bindings |
| C4 | WebTransport-over-HTTP/3 carrier (opt-in), HTTP/3 static responder | C3, Http3 |
| C5 | End-to-end tests (MsQuic loopback, ACME mock CA + HTTP server + QUIC listener cert swap), samples, E2E benchmarks | C1–C3 (C4 is opt-in and nothing depends on it, so C5 comes first) |
