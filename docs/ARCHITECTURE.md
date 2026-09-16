# Tedd.QUICLY — Architecture

QUICLY is a game-session transport library for .NET built directly on QUIC (MsQuic). It gives a game the
primitives it actually needs — tiny lossy state updates, guaranteed events, "only the latest matters" state,
and large world-data transfers — with explicit buffer ownership, a minimal wire header and a data-oriented,
allocation-free hot path.

This document describes the layering, the threading model, the memory model and the public API surface.
The wire format lives in [PROTOCOL.md](PROTOCOL.md). Decisions and their rationale live in [adr/](adr/);
ADR 0008 (hot-path memory and threading contract) and ADR 0009 (security and limits) are the two most
important companions to this document.

## 1. Projects

| Project | Runs where | Purpose |
|---|---|---|
| `Tedd.Quicly.Core` | anywhere (browser-safe: no P/Invoke, no sockets) | Channels, delivery modes, framing, scheduler, buffer pools, completions, session handshake, control protocol, diagnostics, time. Defines `ITransport`. |
| `Tedd.Quicly.Client` | anywhere | `QuiclyClient`: connect, authenticate, reconnect policy, browser-suspend handling. Produces a `QuiclyPeer`. |
| `Tedd.Quicly.Server` | native | `QuiclyServer`: listener, admission policy, per-client limits, peer sets / broadcast, HTTP hosting glue, certificate provisioning glue (ACME). Produces `QuiclyPeer`s. |
| `Tedd.Quicly.Http3` | anywhere | Pure-managed HTTP/3 framing, QPACK (static table + Huffman), HTTP Datagrams (RFC 9297), WebTransport session framing. Unit-testable without a network. Used only when HTTP/3 is enabled. |
| `Tedd.Quicly.Transport.MsQuic` | native (Windows / Linux / macOS) | Hand-written MsQuic bindings (validated against the runtime-bundled library), wrappers, raw-QUIC transport (ALPN `quicly/1`) and, opt-in, WebTransport-over-HTTP/3 (ALPN `h3`) sharing one UDP port. Client and server sides. |
| `Tedd.Quicly.Transport.Browser` | browser-wasm (optional, experimental, later) | JS WebTransport adapter via `[JSImport]`. Same `ITransport` contract. |
| `Tedd.Quicly.Http` | native | Small HTTP/1.1 server over TCP (plain + TLS via `SslStream` with a ClientHello peek for ALPN): routing, static files, ACME `http-01` responder and `tls-alpn-01` responder. |
| `Tedd.Quicly.Acme` | native | RFC 8555 ACME v2 client: any directory URL (Let's Encrypt, ZeroSSL, Buypass, Google Trust Services, SSL.com, …), External Account Binding, `http-01` / `tls-alpn-01` / `dns-01` challenge providers, renewal scheduling. |
| `Tedd.Quicly.Testing` | anywhere | `SimulatedTransport`: in-memory link with deterministic loss / reorder / delay / jitter / bandwidth / disconnect models driven by a virtual clock. Certificate helpers. |
| `Tedd.Quicly.Replication` | anywhere | Optional game-layer helpers: entity generations, snapshot ring with acknowledged baselines, delta encoding, input sequencing / reconciliation, event de-duplication window. |
| `benchmarks/Tedd.Quicly.Benchmarks` | native | BenchmarkDotNet micro and macro benchmarks; compares against `Tedd.Quicly.Archive`. |
| `benchmarks/Tedd.Quicly.Archive` | native | Superseded implementations of hot-path code, kept so every optimisation is measurable against its predecessor. |

Dependency direction is strictly downward: `Core` depends on nothing; transports depend on `Core` (+ `Http3`);
`Client`/`Server` depend on `Core` and are transport-agnostic (a transport factory is injected).

Target frameworks: `net11.0` (primary) and `net10.0` for every library and test project.

## 2. Layering

```
 game code
 ───────────────────────────────────────────────────────────────────────
 QuiclyServer / QuiclyClient      accept / connect, admission, peer sets, PollAll
 QuiclyPeer  (Core)               Send*, Poll, Flush, Drain, completions, channels
   ├─ ChannelTable                static per-app channel definitions
   ├─ SendScheduler               priority / bandwidth budget / packing / expiry / retries
   ├─ Delivery engines            DatagramSequencer, ReliableLatestEngine, OrderedStreamChannel,
   │                              GroupStreamChannel, BulkTransferEngine, Fragmenter, RequestTable
   ├─ ReceivePipeline             header parse → limits → target selection → fill → publish (SPSC ring / key mailboxes)
   ├─ ControlProtocol             hello/ack, ping/RTT/clock, latest-acks, key retirement, close
   └─ Memory                      SlabAllocator (native, size classes, lock-free), leases, shared refcounts
 ───────────────────────────────────────────────────────────────────────
 ITransport (Core)                datagrams + streams + capabilities + events
   ├─ MsQuicRawTransport          ALPN quicly/1 : QUIC streams & DATAGRAM frames directly
   ├─ WebTransportH3Transport     ALPN h3       : HTTP/3 + Extended CONNECT + WT framing (opt-in)
   ├─ BrowserWebTransport         JS WebTransport (optional, later)
   └─ SimulatedTransport          tests / benchmarks
 ───────────────────────────────────────────────────────────────────────
 MsQuic (msquic.dll / libmsquic)  bundled with the .NET runtime on Windows; LibraryImport + function pointers
```

### 2.1 `ITransport` contract (Core)

The transport is deliberately thin; all game semantics are above it. `TransportSegment` is bit-identical
to MsQuic's `QUIC_BUFFER` (`uint Length` at offset 0, `byte* Buffer` at offset 8, 16 bytes) so gather arrays
are handed to the native library without translation and receive arrays are reinterpreted in place.

```csharp
public interface ITransport : IDisposable
{
    TransportCapabilities Capabilities { get; }   // Datagrams, DatagramSendState, MaxDatagramPayload (dynamic), StreamPriority, AppOwnedReceiveBuffers
    TransportState State { get; }
    // Datagrams: one QUIC DATAGRAM frame per call. Buffers and the segment array MUST stay valid until OnDatagramSendStateChanged(Sent).
    TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags);
    // Streams. Buffers and the segment array MUST stay valid until OnStreamSendCompleted(context).
    TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id);
    TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags); // flags: Fin, DelaySend, CancelOnLoss, Start
    void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection dir);
    void SetStreamPriority(TransportStreamId id, ushort priority);
    void ResumeStreamReceive(TransportStreamId id, int bytesConsumed);   // after a receive returned Pending
    void UpdatePeerStreamLimits(ushort bidi, ushort uni);                // raised after admission
    void Close(ulong errorCode, ReadOnlySpan<byte> reason);
    void GetStatistics(out TransportStatistics stats);                   // RTT/minRTT/variance, cwnd, bytes in flight, loss, path MTU — struct, no allocation
}

public interface ITransportSink   // implemented by Core; called on transport threads, serialised per connection
{
    void OnConnected(in TransportConnectedInfo info);                    // RemoteAddress, NegotiatedAlpn, SessionResumed, HandshakeInfo
    void OnDatagramReceived(ReadOnlySpan<byte> payload);                 // valid only during the call
    void OnPeerStreamStarted(TransportStreamId id, StreamKind kind);
    ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> buffers, ulong absoluteOffset, bool fin); // Consumed(n) | Pending(n)
    void OnStreamSendCompleted(ulong context, bool canceled);            // segment array + buffers released
    void OnDatagramSendStateChanged(ulong context, DatagramSendState state); // Sent (buffer released) … Acknowledged/Lost*/Canceled (final)
    void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection dir);
    void OnStreamShutdownComplete(TransportStreamId id);
    void OnDatagramCapabilityChanged(bool enabled, int maxPayload);
    void OnIdealSendBufferSize(TransportStreamId id, ulong bytes);
    void OnClosed(TransportCloseReason reason, ulong errorCode, int status);
}
```

Contexts are `(generation << 32) | slot`; every callback validates the generation and ignores stale events
(MsQuic keeps reporting datagram send state for a context after `Sent`). `DatagramSendState` is the full
MsQuic enum (`Unknown, Sent, LostSuspect, LostDiscarded, Acknowledged, AcknowledgedSpurious, Canceled`):
the payload block is released at `Sent`/`Canceled`, the slot only at a final state (`LostDiscarded`,
`Acknowledged`, `AcknowledgedSpurious`, `Canceled`).

Send-call semantics: if `SendDatagram`/`SendStream`/`OpenStream` returns a failure status **no completion
follows** and the caller releases its lease; on success a completion **always** follows (`canceled = true`
on abort/close) and it may arrive on the transport thread **before the call returns** on the game thread.
Callers therefore publish everything the completion needs (state = InFlight, token, lease) *before* the
call and never touch the slot afterwards. `TransportStreamId` is a local slot index valid immediately; the
QUIC stream id (`QuicStreamId`, needed by the WebTransport carrier) is valid after the start-complete event.
Group and bulk streams are started with the first send (`Start` flag) and closed with `Fin` on the last.

Stream receive: a frame header may be split across the buffers of one receive event as well as across
events; the sink parses across the buffer array, consumes whole frames when it can, returns
`Consumed(n)` with `n` at a frame boundary otherwise, and uses `Pending` only for genuine back-pressure (while
a receive is pending no further receive events arrive for that stream and unconsumed bytes are not credited
back to the peer's flow control). Datagram buffers are valid only during the callback, so Direct-mode
target selection for datagrams completes inline.

## 3. Threading model

* **Transport thread**: MsQuic serialises all callbacks of one connection (and its streams) onto one worker
  at a time (the worker identity may change; ownership is "the current callback", never a thread id).
  Receive-side state is mutated there **without locks**.
* **Game thread**: calls `Send*`, `Flush`, `Poll`, `Drain`. Send-side state (queues, per-key send tables,
  request table, completion slots) is owned by the game thread. All time-driven work (retries, expiry,
  pings, heartbeat, group flush) runs inside `Flush`/`Poll`; there are no timers. `NextDeadline` tells a host
  how long it may sleep. `now` is read once per `Flush`/`Poll`.
* **Hand-off**: per peer, pre-allocated single-producer/single-consumer rings in native memory (the transport
  side has one producer by construction): `ReceiveRing` (descriptors of complete messages, 64 bytes each) and
  `CompletionRing` (sized to two per send entry — one early `Sent` notice plus one final completion — so it can
  never overflow). Keyed channels with
  `CoalesceOnReceive` bypass the ring: each key has a **mailbox** slot exchanged atomically (the transport
  thread frees the lease the game thread never saw) and a per-channel dirty bitset the game thread scans.
* **Send entries** cross threads: the game thread submits, the transport thread completes. The entry's
  state is an `int` driven only by CAS transitions (Free → Submitted → [Cancelling] → Completed → Free); the
  game thread reuses a slot only after observing its completion through the `CompletionRing`.
* **Completion delivery** (`PeerOptions.CompletionMode`): `PollOnly` (default for games) completes
  awaiters inside `Poll` with continuations inline on the game thread; `ThreadPool` signals from the transport
  thread with `RunContinuationsAsynchronously` for hosts that never poll.
* **Multi-producer sends** (`PeerOptions.ThreadSafeSend`): foreign threads enqueue 64-byte send requests
  into a Vyukov MPSC ring drained by the game thread at `Flush`/`Poll`; `Immediate` from a foreign thread
  means "at the next Flush".
* **Callbacks never throw**: every `[UnmanagedCallersOnly]` body is wrapped; an escaping exception is
  recorded, the peer is poisoned and shut down asynchronously (strict mode: fail fast). Application code on
  the transport thread (`SelectTarget`) has a documented budget: no allocation, no locks, ≤ 1 µs.
* Nothing in the hot path takes a lock; nothing in the hot path allocates on the GC heap after warm-up.

## 4. Memory model

* `SlabAllocator` (Core): native memory (`NativeMemory.AlignedAlloc`, 64-byte aligned) partitioned into
  size classes (default 64 B, 256 B, 1 536 B, 4 KiB, 16 KiB, 64 KiB, 256 KiB) with lock-free free lists.
  Default reserve is ~16 MiB per process; per-peer budgets bound what one peer can hold. Exhaustion is an
  explicit, counted outcome (`SendStatus.OutOfBuffers`), never a GC allocation. Per-thread magazines with
  SPSC return rings are the planned refinement once the benchmark in `docs/benchmarks/memory.md` justifies
  them.
* `BufferLease` (16-byte struct: class, block, offset, length, generation) is the currency of ownership.
  `SharedLease` adds a padded atomic reference count so a server can serialise a snapshot once and send it
  to N peers with zero copies (`SendShared`).
* All hot state lives in **reference-free struct arrays in native memory**, split by owning thread and
  padded to 64 bytes: `ChannelSendState[]`, `KeySendSlot[]`, `SendEntry[]` (game thread);
  `ChannelRecvState[]`, `KeyRecvSlot[]`, `ReceiveEntry[]` (transport thread). Fields that are scanned every
  tick (expiry deadlines, retry timestamps, dirty bits) are stored structure-of-arrays so `Vector256` scans
  and popcounts apply. Keys are hashed with linear probing over SoA `keys[]`/`slots[]` (fmix64, power-of-two
  capacity, load ≤ 0.5) or indexed directly for `KeySpace.Dense`.
* `SendEntry` is 64 bytes and contains its adjacent `QUIC_BUFFER` pair (header + payload segments); the
  header bytes live in a cold array of 32-byte blocks, and stream gathers copy the segment pairs into a
  per-submission contiguous segment array from a native arena (ADR 0008 invariant 1). Nothing the transport was given a pointer to moves or is reused before the matching completion.

### 4.1 Send ownership

| API | Copy? | Buffer released when | Use for |
|---|---|---|---|
| `SendCopy(header, ReadOnlySpan<byte>)` | one memcpy into a slab block | the caller's span immediately; the slab block after the transport is done | locked / mutable game structures: lock → copy → unlock |
| `RentBuffer(size)` + `SendOwned(header, lease, length)` | none | after the transport is done (datagram: sent; stream: acknowledged) | serialise directly into library memory — the true zero-copy path |
| `SendPinned(header, byte* ptr, int length)` / `SendPinned(header, PinnedMemory)` | none, no handle | `BufferReleased` completion | native memory or `GC.AllocateArray(pinned: true)` |
| `SendBorrowed(header, ReadOnlyMemory<byte>)` | none, but pins (`MemoryHandle` stored as `nint` in a side table) | `BufferReleased` completion | convenience path for ordinary arrays |
| `SendShared(header, table, sharedLease, length)` | none | this peer's reference is dropped when its own send completed (or was discarded); the block returns to the pool when the last peer released it | broadcast one serialisation to many peers — never compressed, so the caller compresses once instead of once per peer |
| `SendGather(header, ReadOnlySpan<BufferLease>)` / `(header, TransportSegment*, count)` | none | `BufferReleased` | header + existing payload pages (≤ 8 segments) |

The library never promises "no copy anywhere": it promises **at most one application-level copy**. MsQuic
performs its own copy of send bytes into the packet buffer during encryption, and copies stream data from
the decrypted packet into its stream receive buffer before indicating it (datagrams are indicated straight
from the packet buffer). MsQuic non-buffered sends are used by default (`SendBufferingEnabled = false`), which
removes MsQuic's send-side buffer copy; the cost is that a reliable channel's blocks stay in flight for
≥ 1 RTT, which bounds reliable throughput per peer to budget/RTT. App-owned stream receive buffers (MsQuic
2.5 preview) are the future path to landing stream data directly in caller-chosen memory.

### 4.2 Receive targets

Two-phase receive: the header is parsed on the transport thread, limits are checked, then the destination
is chosen:

* **Pooled mode** (default): the payload is copied into a slab lease and delivered via `Poll()` to the
  channel's `MessageHandler(in ReceiveHeader, ReadOnlySpan<byte>)`, or drained in batches with
  `Drain(channel, Span<ReceivedMessage>)` + `Release(...)` for data-oriented consumers. The lease is
  released when the handler returns unless retained (`ReceiveLease Retain(in ReceiveHeader)`).
* **Direct mode** (Bulk and large objects): `IReceiveRouter.SelectTarget(in ReceiveHeader)` runs on the
  transport thread and returns a `ReceiveTarget` — pre-sized caller `Memory<byte>` (never grown, not touched
  by the game thread until `OnMessage`), an `IBufferWriter<byte>` that never grows, a pooled lease, or
  `Reject`. Payload bytes are written there as they arrive; `OnMessage` is raised on the game thread when
  the message is complete; `OnAborted` on cancel. Compressed messages are staged compressed and decoded on
  the game thread inside `Poll`; targets receive decoded bytes.
* **Coalescing** (`CoalesceOnReceive`, forced on for `ReliableLatest`): only the newest queued version per
  key is delivered; superseded versions are counted and their leases recycled without a ring entry.
* **Overflow policy**: datagram channels drop the newest message and count it; stream channels return
  `Pending` to the transport (QUIC flow control then bounds the peer) and resume from `Poll`.

```csharp
public readonly struct ReceiveHeader
{
    public ushort Channel; public ulong Key; public uint Sequence; public int Length; public int RawLength;
    public long ReceivedMicros; public uint SenderTick; public uint Epoch; public int PeerIndex; public uint RequestId;
    public ReceiveFlags Flags;   // Fragmented, Compressed, Superseded, IsRequest, IsResponse, KeyRetired
}
```

## 5. Channels and delivery modes

Channels are declared once in a `ChannelTable` (both sides must agree; the handshake exchanges a hash).

| Mode | Contract | Implementation |
|---|---|---|
| `UnreliableUnordered` | may be lost, may arrive in any order | QUIC DATAGRAM |
| `UnreliableSequenced` | may be lost; only messages newer than the last accepted one per key are delivered | DATAGRAM + 16/32-bit serial sequence (per-channel counter, compared per key) |
| `ReliableOrdered` | every message, in order, per channel; optional request/response correlation | one persistent unidirectional QUIC stream per channel per direction; length-prefixed frames |
| `ReliableUnordered` | every message; no message waits for another's retransmission | one unidirectional stream per *flush group*; groups are independent |
| `ReliableLatest` | intermediate versions may be discarded; the latest version is eventually delivered while the epoch lives | versioned DATAGRAMs + application acks + loss-driven/timed retries (large values: per-key stream that aborts the previous one) |
| `Bulk` | large objects with progress / cancel / resume | one stream per transfer, chunked, low priority, bounded concurrency, SHA-256 identity |

Ordering scope: session + direction + channel. Replacement scope: session + direction + channel + key.
Channels are typed by the application (one channel per message kind); there is no message-type field.
Keys can be retired (`RetireKey`) so per-key state is freed on both sides; sequence counters are per channel,
so a reused key is never mistaken for its previous holder.

Completion points: **BufferReleased** (always), **Delivered** (transport ack for datagrams/streams when the
transport reports it; `LatestAck` for ReliableLatest; transfer complete for Bulk), and — the only
application-level acknowledgement in v1 — the **response** on a `RequestResponse` channel.

## 6. Public API (Core)

```csharp
public sealed class QuiclyPeer : IDisposable
{
    public int Index { get; }              // dense, generation-tagged slot in the owning server/client
    public ulong Tag { get; set; }         // application data
    public PeerState State { get; }        // Connecting, Handshaking, Connected, Reconnecting, Closing, Closed
    public uint Epoch { get; }
    public IPEndPoint RemoteEndPoint { get; }
    public TransportCapabilities Capabilities { get; }
    public ChannelTable Channels { get; }
    public void GetStatistics(out PeerStatistics stats);          // fixed-layout struct: RTT (smoothed/min/max/variance, transport + application), one-way jitter, datagram loss %, bytes/packets per second each way, cwnd, bytes in flight, max datagram payload, ring occupancy high-water marks, per-channel counters (sent, received, dropped, superseded, expired, out-of-buffers, queue-full, too-large, ring-drops, retries, key-table-full)
    public long EstimatedRemoteMicros();
    public TimeSpan NextDeadline { get; }
    public long NextDeadlineMicros { get; }            // = min(poll, flush), for a host with one loop
    public long NextPollDeadlineMicros { get; }         // the peer's own timers: ping, heartbeat, admission, close linger, stream idle
    public long NextFlushDeadlineMicros { get; }        // engine work only a Flush can serve (retries, expiry, send-cap refill)
    public bool HasPendingWork { get; }                 // anything waiting for Poll: rings, mailboxes, transitions, a due timer
    public bool IsDisposed { get; }

    public BufferLease RentBuffer(int size);
    public SendResult SendCopy(in SendHeader h, ReadOnlySpan<byte> payload, SendOptions o = default);
    public SendResult SendOwned(in SendHeader h, BufferLease lease, int length, SendOptions o = default);
    public unsafe SendResult SendPinned(in SendHeader h, byte* payload, int length, SendOptions o = default);
    public SendResult SendBorrowed(in SendHeader h, ReadOnlyMemory<byte> payload, SendOptions o = default);
    public SendResult SendGather(in SendHeader h, ReadOnlySpan<BufferLease> segments, SendOptions o = default);
    public SendResult SendShared(in SendHeader h, SharedLeaseTable table, in SharedLease lease, int length, SendOptions o = default); // fan-out: retains on admission, releases exactly once
    public ValueTask<SendResult> SendAsync(in SendHeader h, ReadOnlyMemory<byte> payload, SendOptions o, CancellationToken ct); // waits for admission when a reliable queue is full
    public ValueTask<ReceiveLease> SendRequestAsync(in SendHeader h, ReadOnlyMemory<byte> payload, TimeSpan timeout, CancellationToken ct);
    public SendResult Respond(in ReceiveHeader request, ReadOnlySpan<byte> payload);
    public void RetireKey(ushort channel, ulong key);

    public ValueTask<BulkTransfer> BeginBulkSendAsync(BulkDescriptor d, IBulkSource source, CancellationToken ct);
    public void RequestBulk(in BulkRequest request);

    public void Flush(uint tick = 0);                // transmit everything buffered; run time-driven work
    public ValueTask FlushAsync(CancellationToken ct);
    public int Poll(int maxItems = int.MaxValue);    // dispatch receives + completions on the calling thread
    public int Drain(ushort channel, Span<ReceivedMessage> into);   // batch alternative to handlers
    public void Release(ReadOnlySpan<ReceivedMessage> messages);

    public ValueTask<DeliveryStatus> WaitAsync(SendToken t, CompletionStage stage, CancellationToken ct);
    public DeliveryStatus Wait(SendToken t, CompletionStage stage, TimeSpan timeout);   // native hosts only
    public bool TryCancel(SendToken t);              // best effort; never releases the payload by itself

    public void Close(CloseReason reason);           // graceful; completes through Poll with PeerState.Closed
    public void Reconnect(ITransportConnector connector, EndPoint endpoint, string? serverName, ReadOnlySpan<byte> authToken); // client: resume this session over a new transport (PROTOCOL §4.1)
    public event Action<QuiclyPeer, PeerState, PeerState>? StateChanged;   // raised from Poll; a throwing handler never sees the same transition twice
}

public readonly record struct SendHeader(ushort Channel, ulong Key = 0);
public readonly struct SendOptions { public SendMode Mode; /* Buffered | Immediate */ public bool Track; public ulong Context; public long ExpiryMicros; }
public readonly record struct SendResult(SendStatus Status, SendToken Token);
public enum SendStatus { Admitted, QueueFull, TooLarge, OutOfBuffers, ChannelClosed, NotConnected, KeyTableFull, InvalidChannel, NotSupported }
public enum DeliveryStatus { Pending, Delivered, Superseded, Failed, Canceled, Expired, Lost, Disconnected, Sent }

// PeerOptions.WorkSignal: told once per Poll that the peer has game-thread work, so a host wakes instead of polling idle peers.
// Non-blocking, allocation-free, must not re-enter the peer; HasPendingWork is the level behind this edge.
public interface IPeerWorkSignal { void OnWork(QuiclyPeer peer); }

// PeerOptions also exposes Clone() (an independent copy sharing the clock, pool and signal) and Validate() (the same checks the
// peer constructors run), so hosts neither copy options by reflection nor build a throw-away peer to check them.
```

### 6.1 Server and client

```csharp
public sealed class QuiclyServer : IAsyncDisposable
{
    public QuiclyServer(ServerOptions options, ITransportListenerFactory listener);
    public ValueTask StartAsync(CancellationToken ct);
    public int PollAll(int maxItems = int.MaxValue);          // polls the peers the work signal marked (gated on HasPendingWork) and those whose poll deadline is due; idle peers cost nothing
    public void FlushAll(uint tick = 0);                       // PollAll also brings a flush forward to NextFlushDeadlineMicros
    public long NextPollDeadlineMicros { get; }                // sleep the polling loop on this (peers' timers + session expiry)
    public long NextFlushDeadlineMicros { get; }               // engine work and the auto-flush schedule
    public ReadOnlySpan<PeerSlot> Peers { get; }               // dense, generation-tagged
    public QuiclyPeer? GetPeer(int index);
    public PeerSet CreateSet();                                 // bitset over peer indices
    public SharedSendResult SendShared(PeerSet set, in SendHeader h, SharedLease lease, int length, SendOptions o = default); // admitted/rejected masks; each peer retains and releases its own reference
    public event Action<QuiclyPeer>? PeerAdmitted;             // raised from PollAll
    public event Action<QuiclyPeer, CloseReason>? PeerClosed;  // raised from PollAll
}

public interface IAdmissionPolicy
{
    PreHandshakeDecision PreHandshake(in NewConnectionInfo info);      // remote address, SNI, ALPN — before TLS work
    AdmissionDecision Admit(in HelloInfo hello, QuiclyPeer peer);      // auth token, session resume, capacity
}

public sealed class QuiclyClient
{
    public ValueTask<QuiclyPeer> ConnectAsync(EndPoint endpoint, ClientOptions options, CancellationToken ct);
    public QuiclyPeer? Peer { get; }                 // kept across a resumed reconnect: handlers, Index, Tag and statistics survive
    public int Poll(int maxItems = int.MaxValue);     // polls the current peer and drives the reconnect attempts
    public void Flush(uint tick = 0);
    // ReconnectPolicy: attempts, back-off, browser-suspend awareness. A resume reconnects the same peer in place
    // (QuiclyPeer.Reconnect: Closed → Reconnecting → Handshaking → Connected, epoch + 1); a refused resume falls back to a
    // fresh session on a new peer.
}
```

## 7. Transport defaults (MsQuic)

| Setting | Default | Why |
|---|---|---|
| `PeerBidiStreamCount` | 1 before admission | the control stream only |
| `PeerUnidiStreamCount` | 0 before admission; after: channels + Σ MaxGroups + bulk concurrency (≤ 4 096) | per-channel streams and flush groups |
| `StreamRecvWindowUnidiDefault` | 2 MiB | one Bulk stream per RTT must not be capped at 64 KiB |
| `ConnFlowControlWindow` | 16 MiB | bulk throughput |
| `IdleTimeoutMs` | 30 000 | dead-client detection when nothing is in flight |
| `KeepAliveIntervalMs` | 0 on the server; 10 000 on the client (below common NAT binding timeouts; the 1 s application ping usually makes it moot) | |
| `DisconnectTimeoutMs` | 6 000 | detection when data is in flight |
| `HandshakeIdleTimeoutMs` | 5 000 | |
| `MinimumMtu` / `MaximumMtu` | 1 248 / 1 500 | datagram payload starts near 1 200 B and grows with PMTUD; it can shrink on migration |
| `MaxAckDelayMs` | 5 | faster loss detection and `BufferReleased` |
| `PacingEnabled` | true (benchmarked; configurable) | |
| `MigrationEnabled` | true | Wi-Fi → LTE without reconnect |
| `DatagramReceiveEnabled` | true | |
| `SendBufferingEnabled` | false | zero-copy sends |
| `ServerResumptionLevel` | NO_RESUME | see PROTOCOL §4.2 |
| `CongestionControlAlgorithm` | CUBIC (BBR option) | |
| `StreamSchedulingScheme` | ROUND_ROBIN | Bulk must not starve ordered channels |
| execution profile | LOW_LATENCY (REAL_TIME option) | |
| datagram flags | Immediate/high priority → `DGRAM_PRIORITY`; unreliable → `CANCEL_ON_BLOCKED`. `DELAY_SEND` is **not** used: measured on MsQuic loopback it made a tick's burst 16–19 % slower per datagram, and a later re-run found no measurable effect either way, so the session layer never sets it — batching comes from packing (PROTOCOL §2.2) instead | packing and stale-data control |

Bulk sends are windowed on the transport's `IdealSendBufferSize` and additionally capped to a fraction of
the congestion window (`BulkShareOfCongestionWindow`, default 50 %, re-evaluated per completion) — stream
priority alone cannot protect datagram latency because datagrams and streams share one congestion window.
Group/bulk streams are started with `FailBlocked | ShutdownOnFail`; a stream-limit failure surfaces as
`SendStatus.QueueFull` and `PeerNeedsStreams` raises the local limit at runtime. Stream credit is returned
to the peer only at shutdown-complete, so the receive engine consumes FIN promptly and closes streams
immediately. `PeerAddressChanged` (migration, NAT rebind) re-runs the per-address admission check and
treats path MTU and congestion state as reset. Platforms: Windows 11 / Server 2022+ (Schannel build bundled
with .NET: no ChaCha20, no 0-RTT), Linux with `libmsquic` (OpenSSL build; PKCS12/file credentials), macOS
only with a user-installed `libmsquic`. The `Microsoft.Native.Quic.MsQuic.OpenSSL` package is an alternative
on Windows when OpenSSL features are wanted.
Version gating: the binding reads the library version at open, refuses < 2.4, and exposes API-table entries
beyond the 2.2 set only when the loaded library has them; preview entries (app-owned receive buffers,
execution polling) are behind an explicit opt-in.

## 8. Security defaults

TLS 1.3 is mandatory in QUIC; "encryption as an option" therefore means certificate validation policy:
`SystemRoots` (default), `PinnedSpki` (SHA-256 of SubjectPublicKeyInfo; requires `AcmeOptions.ReuseKey` on
the server), `Callback` (portable DER chain, platform validation still performed unless the callback opts
out), `DangerousAcceptAnyServerCertificate` (throws outside DEBUG unless an environment variable is set,
logs a warning per connection). Session/auth token rules, admission timeouts, receive-side limits,
0-RTT policy and error handling are specified in PROTOCOL §4, §6, §7 and ADR 0009.

## 9. Memory sizing

| Item | Per | Default | Notes |
|---|---|---|---|
| slab reserve | process | 16 MiB | shared by all peers, bounded per peer by the budgets below |
| receive byte budget | peer | 256 KiB | pooled leases + reassembly + stream staging |
| send byte budget | peer | 256 KiB | blocks in flight; reliable throughput ≤ budget / RTT |
| send table | peer | 1 024 entries × 64 B | tracked and untracked sends in flight |
| rings | peer | 4 096 × 64 B receive (256 KiB), 2 048 × 16 B completion (32 KiB) | native memory; `ReceiveEntry` is 64 B (52 of them in use) and a send entry produces at most two completions |
| drain queues | peer | min(receive ring, 1 024) × 68 B (68 KiB) | per-channel queues for `Drain` consumers: a 64 B node plus its link, native, built with the peer |
| segment arena | peer | 1 024 × 16 B | per-submission gather arrays for stream sends |
| channel state | peer × channel | 2 × 64 B | send + receive halves |
| group records | peer × group channel | `(3 × max(MaxGroups, 1) + 4) × 64 B` send + `max(MaxGroups, 1) × 64 B` receive | `ReliableUnordered`: one record per live group (filling, waiting, or holding a stream) and one per accepted peer stream; the engine's notice ring adds `4 × its send records + 8` × 12 B |
| bulk transfers | peer (both directions) | `BulkTransfersPerDirection` × 128 B send + × 192 B receive | `Bulk`: one record per transfer in each direction (2 + 2 by default = 640 B), plus the engine's rings — stream notices `(4 × transfers + 8) × 24 B`, peer control messages 64 × 48 B, and two `transfers + 8` slot rings of 4 B — about 4 KiB per peer in total. A transfer that **compresses** also rents one `BulkChunkBytes` scratch block (64 KiB, lazily, per peer); the staging of a received compressed chunk comes from the receive budget, not from here |
| key slots | peer × keyed channel | `MaxKeys` × 64 B (+ mailbox) | dense or hashed |

With the defaults a peer's fixed native tables are therefore about **560 KiB**: 256 KiB receive ring, 32 KiB
completion ring, 64 KiB send entries plus ~120 KiB of their header blocks and cold side arrays, 68 KiB drain
queues and 16 KiB segment arena. The 256 KiB receive and 256 KiB send figures above are *payload* budgets drawn
from the shared slab reserve, not additional per-peer allocations. `ServerOptions.ExpectedPeers` scales all of
it — 1 000 peers at the defaults would be ~550 MiB of tables alone, so a server with many peers lowers
`ReceiveRingCapacity`, `SendTableCapacity` and the byte budgets (the defaults target tens to a few hundred peers
per process). `MaxGroups` is the one channel option that can dominate this: at its default of 8 a group channel costs about
1.8 KiB of send records, 0.5 KiB of receive records and 1.4 KiB of notice ring, but a channel raised to `MaxGroups = 1024` costs
about **192 KiB** of send records (3 076 of them), 64 KiB of receive records and ~144 KiB of ring — roughly 400 KiB for that one
channel, per peer — so raise it only for a channel that really needs that many groups in flight at once. The numbers are
published from the benchmark in `docs/benchmarks/memory.md`.

## 10. Testing & measurement

* Unit tests target ~100 % line coverage of `Core`, `Http3`, `Acme`, `Http`, `Testing`, `Replication`.
* `SimulatedTransport` shares the peer's `IClock`, so every delivery-mode property is deterministic: lost
  final update, lost ack, sequence roll-over, key reuse after retirement, disconnect during a borrowed send,
  cancel mid-receive, coalescing on/off, movement latency while a bulk transfer saturates a capped link.
* End-to-end tests run real MsQuic loopback (raw and, when enabled, WebTransport), the ACME client against
  an in-process mock CA, and `HttpClient` (HTTP/3) against our HTTP/3 server.
* Every optimisation follows hypothesis → benchmark (BenchmarkDotNet, `Archive` baseline, both runtimes) →
  decision, recorded in `docs/benchmarks/`. E2E benchmarks also report GC pause totals and DatagramSend calls
  per tick.
