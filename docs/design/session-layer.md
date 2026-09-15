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
State/          NativeArray<T>, SendEntry (+cold SoA), ReceiveEntry, KeyTable, DenseKeyTable, Mailboxes, DirtyBitset, ChannelSendState, ChannelRecvState
Session/        QuiclyPeer (partial, one file per concern), PeerOptions, SendHeader/SendOptions/SendResult, ReceiveHeader, ReceivedMessage, PeerStatistics, IReceiveRouter/ReceiveTarget, SessionTokens, HandshakeState
Session/Engines/ Scheduler, DatagramSequencer, ReliableLatestEngine, OrderedStreamChannel, GroupStreamChannel, Fragmenter, BulkEngine, RequestTable, PingClock
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
* `ReceiveEntry` (≤ 64 B): `ushort Channel`, `ReceiveFlags Flags`, `uint Sequence`, `ulong Key`, `BufferLease Lease`, `int Length`,
  `int RawLength`, `uint ReceivedMicrosDelta`, `uint SenderTick`, `uint RequestId`.
* `KeyTable`: open addressing, linear probing, SoA `ulong[] keys`, `int[] slots`, power-of-two capacity, load ≤ 0.5, `fmix64`,
  backward-shift deletion (no tombstones). `DenseKeyTable`: direct index for `KeySpace.Dense(max)`.
* Per-key send slot (`KeySendSlot`, 64 B): current version, acked version, lease of current value (ReliableLatest), retry deadline,
  attempts, entry index in flight, large-value stream id. Per-key receive slot (`KeyRecvSlot`): last accepted sequence,
  mailbox lease index (int, −1 empty), reassembly index, flags.
* `Mailboxes`: `int[] mailbox` per key slot + `ulong[] dirty` bitset per channel; transport thread `Interlocked.Exchange` in,
  game thread `Interlocked.Exchange(-1)` out; `PopDirty(Span<int> keys)` scans the bitset with `BitOperations.TrailingZeroCount`.
* `ChannelSendState` / `ChannelRecvState` (64 B each, per channel): counters, next sequence, queue head/tail/bytes, stream id,
  group state, statistics fields (sent/received/dropped/superseded/expired/queueFull/tooLarge/ringDrops/retries/keyTableFull).

## 4. QuiclyPeer

`public sealed partial class QuiclyPeer : ITransportSink, IDisposable` with files:

| File | Owns |
|---|---|
| `QuiclyPeer.cs` | construction from `PeerOptions` + `ChannelTable` + `ITransport` (server) or connector (client), `State`, `Dispose`, `Index/Tag/Epoch`, statistics snapshot |
| `QuiclyPeer.Send.cs` | `RentBuffer`, `SendCopy/SendOwned/SendPinned/SendBorrowed/SendGather/SendAsync`, `TryCancel`, admission (size/queue/budget checks), entry allocation and enqueue into per-channel queues; `ThreadSafeSend` MPSC front |
| `QuiclyPeer.Flush.cs` | `Flush(tick)`: run engines' time-driven work, scheduler, packing, gather submission, `NextDeadline`, `FlushAsync` watermark |
| `QuiclyPeer.Receive.cs` | `ITransportSink` datagram/stream receive → parsers → per-mode acceptance → pooled lease/mailbox/ring; limits (PROTOCOL §7); stream table (per transport stream: role, channel, parser, group/bulk state) |
| `QuiclyPeer.Poll.cs` | `Poll`, `Drain`, `Release`, handler dispatch, decompression, completion ring drain → `CompletionTable`, state-change events, `CompletionMode` |
| `QuiclyPeer.Control.cs` | handshake state machine (Hello/HelloAck), control stream parser, ping/pong + RTT/clock sync, ack coalescing, KeyRetired, Close |
| `QuiclyPeer.Completion.cs` | `WaitAsync/Wait`, completion routing from transport events (contexts → slots, containers → members, datagram send states) |
| `Engines/*.cs` | one class per engine, owning its slice of the state tables, called from Send/Flush/Receive |

Threading per ADR 0008; the transport-thread methods live in `Receive.cs`/`Completion.cs` and never touch game-thread-owned
arrays except through the rings/mailboxes/`Interlocked` state transitions.

### 4.1 Send path (game thread)

1. Validate channel, connected state, size (`MaxMessageSize`, compression), key requirement; compute effective mode.
2. Allocate entry slot (`SendEntry` table; `QueueFull` when none), and a payload lease when copying (`OutOfBuffers`).
3. Fill: header bytes into `HeaderScratch` via `DatagramFraming.WriteHeader`/`StreamFraming.WriteFrameHeader` (sequence assigned now from
   the channel counter), `Payload` segment over the lease/pinned memory, cold fields, tracked token from `CompletionTable`.
4. Publish (`State = InFlight` release-store) and enqueue on the channel queue (intrusive `Next` links, tail pointer). `Immediate` ⇒ mark
   the peer "flush requested" and run the scheduler for that channel at the end of the call.
5. `ReliableLatest`: supersede the pending entry of the same key (complete it `Superseded`, release lease if not in flight; if in flight the
   lease is released on completion), update `KeySendSlot`.

### 4.2 Flush (game thread)

1. `now = clock.NowMicros` once. Run time-driven work: ReliableLatest retries due, expiry scan (SoA `Deadlines`), ping schedule, ack flush,
   handshake timeout, group timers.
2. Scheduler: iterate channels sorted by priority (precomputed order); per channel drain its queue into submissions:
   * datagram modes: pack into a container lease (size = current `MaxDatagramPayload`) while ≥ 2 fit, else send single; flags per
     PROTOCOL §4.5; each container is its own `SendEntry` (Flags.Container) whose completion fans out to member entries (`BatchHead`
     list); `CancelOnBlocked` for unreliable modes; `Priority` for control/immediate.
   * ordered stream: ensure stream open (lazy, `Start` flag on first send), gather contiguous entries' segments into one `SendStream`
     (limit 64 segments per call); context = the first entry's slot; the completion covers the whole gather (store range in the entry).
   * group stream: open a stream per flush group with `Start`, send gathered frames, `Fin` at the end; stream limit ⇒ leave queued.
   * bulk: feed transfers up to their window (`IdealSendBufferSize`, cwnd share).
   * bandwidth budget: token bucket per peer (`MaxSendBytesPerSecond`) and bulk share; unreliable over budget ⇒ `Expired` + count.
3. Update `NextDeadline` (min of retry/expiry/ping deadlines).

### 4.3 Receive path (transport thread)

* Datagram: `DatagramFraming.TryParse` (or container iteration) → channel 0 ⇒ control handler; else mode logic:
  * `UnreliableUnordered`: accept.
  * `UnreliableSequenced`: `SerialNumber.IsNewer(seq, last)` per key (or per channel when unkeyed) else drop+count.
  * `ReliableLatest`: newer than accepted version ⇒ accept, record ack; else queue re-ack of current version.
  * Fragmentation: reassembly table per channel (`MaxReassemblies`), first fragment fixes total size (checked against limits).
  * Pooled target: rent lease of payload size (or RawLength when compressed — stage compressed, decode in Poll), copy, then either
    mailbox (coalescing keyed channels) or `ReceiveRing.TryEnqueue`; on ring full ⇒ drop + count (datagram) — never block.
* Stream: stream table entry → `StreamFrameParser` → for each message header: limits, target (pooled lease sized from `Length`),
  progressive copy from segments, on complete ⇒ ring; ring full ⇒ return `PendingAfter(consumedSoFar)` and remember the stream to
  resume from Poll. Control stream ⇒ `ControlCodec` messages. Bulk stream ⇒ `BulkEngine` sink.
* Every drop/violation increments a counter; protocol violations close the connection (queued as a "close request" the game thread
  executes in Poll — never `Close` from the callback).

### 4.4 Poll (game thread)

1. Drain `CompletionRing` → `CompletionTable.Complete` (+ release leases/pin handles of completed entries; free slots).
2. Drain `ReceiveRing` up to `maxItems`: decompress if needed (into a second lease), dispatch to the channel handler or leave for `Drain`;
   release lease unless retained. Then mailboxes: for each dirty channel, `PopDirty` keys, exchange out the lease, dispatch.
3. Resume streams that were pended (`ResumeStreamReceive`).
4. Execute queued state changes (close requests, handshake results), raise `StateChanged`.
5. `CompletionMode.PollOnly`: `CompletionTable` continuations run here.

### 4.5 Control protocol

* Client: on `OnConnected` open the control bidi stream, send preamble + Hello (with `tableHash`, tokens, caps). State `Handshaking`.
  On HelloAck accepted ⇒ `Connected`; store session token/epoch; raise stream limits are the server's job.
* Server: `OnPeerStreamStarted` (bidi, first) ⇒ control stream; parse Hello (bounds!), run `IAdmissionPolicy.Admit` (callback executed
  on the game thread via Poll: the transport thread queues the Hello; Poll runs the policy and sends HelloAck) ⇒ `UpdatePeerStreamLimits`,
  `Connected`. `AdmissionTimeout` enforced in Flush/Poll.
* Ping every `PingInterval` (fast lock first 3 s); Pong immediately from the transport thread through the control pool (small dedicated
  slab class) with `Priority` flag. RTT/offset filter per PROTOCOL §4.6.
* Ack coalescing: pending LatestAck/LatestReject entries in a small table, flushed at most every `AckDelay` from Flush/Poll or when full.
* Close: send `Close` on the control stream then `transport.Close(code)` from the game thread.

### 4.6 Engines (each a class with `Init(peer state)`, `OnTick(now)`, and mode-specific hooks)

* `DatagramSequencer` — per-channel/per-key acceptance and coalescing (receive side).
* `ReliableLatestEngine` — send side (versions, retries per PROTOCOL §4.4, budgets, large-value streams) and receive side (accept/ack/reject).
* `OrderedStreamChannel` — lazy stream per channel per direction; send gather; receive parser state.
* `GroupStreamChannel` — groups, stream credit handling, receive of peer groups (bounded by `MaxGroups`).
* `Fragmenter` — split on send (≤ 8 fragments), reassemble on receive with timeout/eviction.
* `BulkEngine` — `BeginBulkSendAsync`/`RequestBulk`, transfer table (2 per direction), chunking, SHA-256, progress, resume, authorizer.
* `RequestTable` — request ids (odd), timeouts, response leases, `SendRequestAsync`/`Respond`.
* `PingClock` — RTT stats, offset, jitter.

## 5. SimulatedTransport (Tedd.Quicly.Testing)

`SimulatedNetwork(VirtualClock, seed)` creates `SimulatedLink` pairs; each end is an `ITransport` whose sink is set at creation
(`CreatePair(sinkA, sinkB, LinkOptions)`). `LinkOptions`: `DelayMicros`, `JitterMicros`, `LossPercent`, `ReorderPercent`, `BandwidthBitsPerSecond`,
`MaxDatagramPayload`, `DatagramsEnabled`, `DatagramSendStateReporting`, `PeerUnidiStreams`, `MtuChangeAt`, `DisconnectAt`.
`network.Advance(micros)` delivers due events in order on the calling thread (simulating the transport thread); every sink callback
happens inside `Advance`. Stream sends complete (acknowledged) at delivery + one-way delay; datagram states go Sent immediately,
then Acknowledged/LostDiscarded at delivery time. Also `RecordingTransport` decorator (optional, later).

## 6. Tests that must exist (Core)

Framing: every header shape, every ParseStatus, container rules, non-minimal varints. Channels: validation matrix, hash stability
vectors. State: KeyTable (insert/find/remove/backward shift/grow/dense), mailboxes concurrency, NativeArray. Session over
SimulatedTransport: handshake (accept/reject/timeout/table mismatch/datagrams required), every mode end to end, coalescing on/off,
key reuse after RetireKey, ReliableLatest lost final update / lost ack / rollover / retry budget / large value stream supersede,
fragmentation loss, expiry under a bandwidth cap, group streams with a stream limit of 1, bulk transfer with resume, request/response
with timeout, ring overflow policies, ping/RTT/offset, close/epoch/reconnect resync, zero-allocation steady state for send/poll.

## 7. Engine boundary and implementation waves

The peer never contains mode-specific logic. It owns the shared tables (send entries, receive ring, mailboxes,
key tables, stream table, completion table) and dispatches to **one engine instance per delivery mode per
peer**. An engine owns the SoA state of *all* channels of its mode (indexed by a dense per-mode channel
index), which keeps the data layout ECS-like and lets engines be written in parallel without touching the
peer's files.

```csharp
internal abstract class ChannelEngine            // one sealed subclass per ChannelMode
{
    public abstract ChannelMode Mode { get; }
    public abstract void Initialize(PeerCore core, ReadOnlySpan<ChannelDefinition> channelsOfMode);

    // game thread
    public abstract SendStatus Admit(ref SendRequest request);        // validate, fill entry, enqueue (no transport call)
    public abstract void Flush(ref FlushContext flush);               // submit queued work within the flush budget
    public abstract void Tick(long nowMicros, ref long nextDeadline);  // retries, expiry, timers
    public abstract void OnSendCompleted(int entrySlot, in CompletionEntry completion);  // drained from the CompletionRing
    public abstract void OnEpochReset(bool resumed);                   // reconnect semantics (PROTOCOL 4.1)

    // transport thread
    public abstract void OnDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros);
    public abstract StreamAccept OnStreamOpened(TransportStreamId id, ushort channel, ulong groupId);
    public abstract StreamConsume OnStreamMessage(ref StreamMessageContext message);   // start / chunk / end
    public abstract void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode);
}
```

`PeerCore` is the engine-facing façade: allocate/publish/free send entries, rent/return leases, enqueue
receive entries or mailbox exchanges, open/send/abort streams through the transport, submit datagrams through
the packer, record counters, read `now`, request a connection close (executed in `Poll`).

Waves:

| Wave | Content | Depends on |
|---|---|---|
| C1 | `PeerCore`, `QuiclyPeer` public API, handshake/control/ping, packer + scheduler, `ChannelEngine` base, engines for `UnreliableUnordered`, `UnreliableSequenced` (incl. coalescing mailboxes), `ReliableOrdered` (persistent stream, gather send), Poll/Drain/handlers, completions, statistics | State, Framing, Channels, Control, SimulatedTransport |
| C2 (parallel) | `ReliableLatestEngine`; `GroupStreamEngine` (`ReliableUnordered`); `BulkEngine`; fragmentation in the unreliable engines; request/response in the ordered engine | C1 |
| C3 | `MsQuicTransport` (ITransport over the MsQuic wrappers) + listener/connector; `QuiclyServer` / `QuiclyClient`; admission; reconnect | C1, msquic bindings |
| C4 | WebTransport-over-HTTP/3 carrier (opt-in), HTTP/3 static responder | C3, Http3 |
| C5 | End-to-end tests (MsQuic loopback, ACME mock CA + HTTP server + QUIC listener cert swap), samples, E2E benchmarks | all |
