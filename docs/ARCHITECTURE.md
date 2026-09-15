# Tedd.QUICLY — Architecture

QUICLY is a game-session transport library for .NET built directly on QUIC (MsQuic). It gives a game the
primitives it actually needs — tiny lossy state updates, guaranteed events, "only the latest matters" state,
and large world-data transfers — with explicit buffer ownership, a minimal wire header and a data-oriented,
allocation-free hot path.

This document describes the layering, the threading model, the memory model and the public API surface.
The wire format lives in [PROTOCOL.md](PROTOCOL.md). Decisions and their rationale live in [adr/](adr/).

## 1. Projects

| Project | Runs where | Purpose |
|---|---|---|
| `Tedd.Quicly.Core` | anywhere (browser-safe: no P/Invoke, no sockets) | Channels, delivery modes, framing, scheduler, buffer pools, completions, session handshake, control protocol, diagnostics, time. Defines `ITransport`. |
| `Tedd.Quicly.Client` | anywhere | `QuiclyClient`: connect, authenticate, reconnect policy, browser-suspend handling. Produces a `QuiclyPeer`. |
| `Tedd.Quicly.Server` | native | `QuiclyServer`: listener, admission policy, per-client limits, HTTP hosting glue, certificate provisioning glue (ACME). Produces `QuiclyPeer`s. |
| `Tedd.Quicly.Http3` | anywhere | Pure-managed HTTP/3 framing, QPACK (static table + Huffman), HTTP Datagrams (RFC 9297), WebTransport session framing (draft-ietf-webtrans-http3). Unit-testable without a network. |
| `Tedd.Quicly.Transport.MsQuic` | native (Windows / Linux / macOS) | Hand-written MsQuic bindings (validated against the runtime-bundled library), safe handle wrappers, raw-QUIC transport (ALPN `quicly/1`) and WebTransport-over-HTTP/3 transport (ALPN `h3`) sharing one UDP port. Both client and server sides. |
| `Tedd.Quicly.Transport.Browser` | browser-wasm (optional, experimental) | JS WebTransport adapter via `[JSImport]`. Same `ITransport` contract. |
| `Tedd.Quicly.Http` | native | Small HTTP/1.1 server over TCP (plain + TLS via `SslStream`): routing, static files, ACME `http-01` responder and `tls-alpn-01` responder. |
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
 QuiclyPeer  (Core)          Send*, Poll, Flush, completions, channels
   ├─ ChannelTable            static per-app channel definitions (ECS-style struct arrays)
   ├─ SendScheduler           priority / budget / batching / packing / expiry
   ├─ Delivery engines        DatagramSequencer, ReliableLatestEngine, OrderedStreamChannel,
   │                          GroupStreamChannel, BulkTransferEngine, Fragmenter
   ├─ ReceivePipeline         header parse → target selection → fill → publish (MPSC ring)
   ├─ ControlProtocol         hello/ack, ping/RTT/clock, receipts, latest-acks, close
   └─ Memory                  SlabAllocator (native, size classes, lock-free), leases, shared refcounts
 ───────────────────────────────────────────────────────────────────────
 ITransport (Core)            datagrams + streams + capabilities + events
   ├─ MsQuicRawTransport      ALPN quicly/1 : QUIC streams & DATAGRAM frames directly
   ├─ WebTransportH3Transport ALPN h3       : HTTP/3 + Extended CONNECT + WT stream/datagram framing
   ├─ BrowserWebTransport     JS WebTransport (optional)
   └─ SimulatedTransport      tests / benchmarks
 ───────────────────────────────────────────────────────────────────────
 MsQuic (msquic.dll / libmsquic)   bundled with the .NET runtime on Windows; LibraryImport + function pointers
```

### 2.1 `ITransport` contract (Core)

The transport is deliberately thin; all game semantics are above it.

```csharp
public interface ITransport : IAsyncDisposable
{
    TransportCapabilities Capabilities { get; }        // datagrams? max datagram payload (dynamic), gather send?, receive-in-place?, stream priority?
    TransportState State { get; }
    // Datagrams
    TransportSendStatus SendDatagram(ReadOnlySpan<TransportSegment> segments, ulong context, TransportSendFlags flags);
    // Streams
    TransportStreamId OpenStream(StreamKind kind /*uni|bidi*/, ulong context, StreamPriority priority);
    TransportSendStatus SendStream(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong context, bool fin, TransportSendFlags flags);
    void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection dir);
    void SetStreamPriority(TransportStreamId id, StreamPriority p);
    void ResumeStreamReceive(TransportStreamId id);   // after a receive sink applied backpressure
    // Connection
    void Close(ulong errorCode, ReadOnlySpan<byte> reason);
    TransportStatistics GetStatistics();              // RTT, cwnd, bytes, loss — struct, no allocation
    // Sink: the transport calls back into ITransportSink (implemented by Core) on its own threads
}

public interface ITransportSink
{
    void OnConnected(in TransportConnectedInfo info);
    void OnDatagramReceived(ReadOnlySpan<byte> payload);                      // buffer valid only during the call
    void OnPeerStreamStarted(TransportStreamId id, StreamKind kind);
    ReceiveConsumed OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> buffers, ulong absoluteOffset, bool fin);
    void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled);  // buffer released
    void OnDatagramSendStateChanged(ulong context, DatagramSendState state);       // Sent / Lost / Acked / Canceled
    void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection dir);
    void OnStreamShutdownComplete(TransportStreamId id);
    void OnDatagramCapabilityChanged(bool enabled, int maxPayload);
    void OnClosed(TransportCloseReason reason, ulong errorCode, int status);
}
```

`TransportSegment` is a `(byte* pointer, int length)` pair over pinned/native memory — this is what enables
scatter/gather sends straight from the application's leases into MsQuic's `QUIC_BUFFER[]` with no copy.

## 3. Threading model

* **Transport threads** (MsQuic workers): MsQuic serialises all callbacks of one connection (and its streams)
  onto one worker at a time, so per-connection receive-side state is mutated **without locks** on the
  transport thread.
* **Game thread**: calls `Send*`, `Flush`, `Poll`. The send-side state (queues, per-key latest tables,
  completion slots) is owned by the game thread. Send admission is single-producer by default;
  `PeerOptions.ThreadSafeSend = true` puts a lock-free MPSC ring in front for multi-threaded producers.
* **Cross-thread hand-off**: two bounded lock-free rings per peer, both pre-allocated:
  * `ReceiveRing` (MPSC → game thread): completed message descriptors (`ReceiveEntry`, 32 bytes).
  * `CompletionRing` (MPSC → game thread): send completions (`CompletionEntry`, 16 bytes).
  `Poll()` drains both and invokes the router / completes awaiters **on the game thread**.
* **Acks / receipts** are transport-level; the receive side may emit them directly from the transport
  thread through a dedicated small pool so the game thread's send arena is never touched from another
  thread.
* Nothing in the hot path takes a lock; nothing in the hot path allocates on the GC heap after warm-up.
  The transport's SEND_COMPLETE / DATAGRAM state events update slab free lists (lock-free stacks).
* `Poll` never blocks. `FlushAsync` and completion awaits are `ValueTask`s backed by pooled
  `ManualResetValueTaskSourceCore` slots. Synchronous waits (`WaitBufferReleased`) spin then park on a
  lazily created event; they are unavailable in the browser (throw `PlatformNotSupportedException`).

## 4. Memory model

* `SlabAllocator` (Core): a set of native memory slabs (`NativeMemory.AlignedAlloc`, 64-byte aligned)
  partitioned into size classes (default: 64 B, 256 B, 1 536 B, 4 KiB, 16 KiB, 64 KiB, 256 KiB).
  Each class keeps a lock-free free list (Treiber stack over indices with an ABA tag). Default reserve is
  ~16 MiB per process; the server can size per-peer budgets. Exhaustion is an explicit, counted outcome
  (`SendStatus.OutOfBuffers`), never a GC allocation.
* `BufferLease` (struct: slab class, block index, offset, length) is the currency of ownership.
  `SharedLease` adds an atomic reference count in a side table so a server can serialise a snapshot once
  and send it to N peers with zero copies (`SendShared`).
* Receive descriptors, send entries, key tables, channel state are all **struct arrays indexed by id**
  (ECS style): `ChannelState[]`, `KeySlot[]` (open-addressing hash on `ulong` key → dense slot),
  `SendEntry[]`, `ReceiveEntry[]`. Iteration is sequential; hot fields are packed together.
* Sequence comparison, varint codec and header parsing are branch-light and vectorisable where it matters;
  benchmarks keep them honest.

### 4.1 Send ownership

| API | Copy? | Buffer released when | Use for |
|---|---|---|---|
| `SendCopy(header, ReadOnlySpan<byte>)` | one memcpy into a slab block | immediately (caller's span is not retained) | locked / mutable game structures: lock → copy → unlock |
| `RentBuffer(size)` + `SendOwned(header, lease)` | none | after the transport is done with it (datagram: sent; stream: acknowledged) | serialise directly into library memory — the true zero-copy path |
| `SendBorrowed(header, ReadOnlyMemory<byte>)` | none (memory is pinned) | `BufferReleased` completion | stable caller memory (pinned arrays / native memory) |
| `SendShared(peers, header, sharedLease)` | none | when every peer's send completed | broadcast one serialisation to many peers |
| `SendGather(header, segments)` | none | `BufferReleased` | header + existing payload pages without concatenation |

The library never promises "no copy anywhere": the receive side has exactly one copy (from the transport's
receive buffer into the destination the application selected). MsQuic non-buffered sends are used by default
(`SendBufferingEnabled = false`), so send is zero-copy down to the kernel.

### 4.2 Receive targets

Two-phase receive: the header is parsed on the transport thread, then the destination is chosen:

* **Pooled mode** (default, simplest): the payload is copied into a slab lease and delivered via
  `Poll()` to the channel's `MessageHandler(in ReceiveHeader, ReadOnlySpan<byte>)`; the lease is released
  when the handler returns (or retained via `header.Retain()`).
* **Direct mode**: the channel's `IReceiveRouter.SelectTarget(in ReceiveHeader)` runs on the transport
  thread and returns a `ReceiveTarget` — caller `Memory<byte>`, an `IBufferWriter<byte>`, a pooled lease,
  or `Reject`. The payload is written straight into that destination (for streams, progressively as bytes
  arrive), and `OnMessage` is raised on the game thread when the message is complete. Partial messages
  never reach live game state: on abort/cancel the router gets `OnAborted`.
* **Latest semantics at the application boundary**: for sequenced / latest channels, if several versions
  of the same key are queued before the game thread polls, only the newest is delivered; older entries are
  marked superseded and their buffers recycled.

## 5. Channels and delivery modes

Channels are declared once in a `ChannelTable` (both sides must agree; the handshake exchanges a hash).

| Mode | Contract | Implementation |
|---|---|---|
| `UnreliableUnordered` | may be lost, may arrive in any order | QUIC DATAGRAM |
| `UnreliableSequenced` | may be lost; only messages newer than the last accepted one per key are delivered | DATAGRAM + 16/32-bit serial sequence (per channel or per key) |
| `ReliableOrdered` | every message, in order, per channel | one persistent unidirectional QUIC stream per channel per direction; length-prefixed frames |
| `ReliableUnordered` | every message; no message waits for another's retransmission | one unidirectional stream per *flush group*; groups are independent |
| `ReliableLatest` | intermediate versions may be discarded; the latest version is eventually delivered while the session lives | versioned DATAGRAMs + application acks + retries (large values fall back to a per-key stream that aborts the previous one) |
| `Bulk` | large objects with progress / cancel / resume | one stream per transfer, chunked, low priority, bounded concurrency |

Ordering scope: session + direction + channel. Replacement scope: session + direction + channel + key.
Channels are typed by the application (one channel per message kind); there is no message-type field.

Per-channel options: `Keyed`, `SequenceBits (16|32)`, `Priority`, `MaxMessageSize`, `QueueLimit`,
`Expiry`, `Fragmentation` (unreliable messages larger than one datagram), `Compression`
(`None | Lz4 | Brotli`, self-contained per message), `Receipts` (remote-accepted completions).

Every send has three separable completion points: **BufferReleased**, **RemoteAccepted** (the remote
library validated the complete message; requires receipts or latest-acks), **Applied** (explicit
application acknowledgement). Only the first is guaranteed to occur; the others are opt-in per channel.

## 6. Public API (Core)

```csharp
public sealed class QuiclyPeer : IAsyncDisposable
{
    public PeerId Id { get; }
    public PeerState State { get; }
    public TransportCapabilities Capabilities { get; }
    public ChannelTable Channels { get; }
    public PeerStatistics Statistics { get; }                    // struct snapshot, no allocation
    public TimeSpan Rtt { get; }  public long ClockOffsetMicros { get; }

    public BufferLease RentBuffer(int size);
    public SendResult SendCopy(in SendHeader h, ReadOnlySpan<byte> payload, SendOptions o = default);
    public SendResult SendOwned(in SendHeader h, BufferLease lease, int length, SendOptions o = default);
    public SendResult SendBorrowed(in SendHeader h, ReadOnlyMemory<byte> payload, SendOptions o = default);
    public SendResult SendGather(in SendHeader h, ReadOnlySpan<ReadOnlyMemory<byte>> segments, SendOptions o = default);
    public ValueTask<SendResult> SendAsync(in SendHeader h, ReadOnlyMemory<byte> payload, SendOptions o, CancellationToken ct); // waits for admission when a reliable queue is full

    public ValueTask<BulkTransfer> BeginBulkSendAsync(BulkDescriptor d, CancellationToken ct);
    public void RequestBulk(BulkRequest request);

    public void Flush();                     // make everything buffered eligible for transmission now
    public ValueTask FlushAsync(CancellationToken ct);   // completes when everything admitted before the call was handed to the transport
    public int Poll(int maxItems = int.MaxValue);        // dispatch receives + completions on the calling thread

    public ValueTask WaitBufferReleasedAsync(SendToken t);   public void WaitBufferReleased(SendToken t);
    public ValueTask<DeliveryStatus> WaitRemoteAcceptedAsync(SendToken t, CancellationToken ct);
    public void AcknowledgeApplied(in ReceiveHeader h);
    public bool TryCancel(SendToken t);      // best effort; never releases the payload by itself

    public ValueTask CloseAsync(CloseReason reason, CancellationToken ct);
    public event Action<QuiclyPeer, PeerState>? StateChanged;
}

public readonly record struct SendHeader(ushort Channel, ulong Key = 0);
public readonly struct SendOptions { SendMode Mode /*Buffered|Immediate*/; bool Track; TimeSpan Expiry; }
public readonly record struct SendResult(SendStatus Status, SendToken Token);
public enum SendStatus { Admitted, QueueFull, TooLarge, OutOfBuffers, ChannelClosed, NotConnected, Superseded, InvalidChannel }
```

## 7. Server / client

* `QuiclyServer` owns one listener (raw QUIC + HTTP/3/WebTransport on one port), an `IAdmissionPolicy`
  (auth token, per-IP limits, capacity), per-peer limits, and an optional HTTP/1.1 endpoint for ACME
  `http-01` / static files / health. `ICertificateSource` supplies the TLS certificate; `AcmeCertificateSource`
  obtains and renews it (any ACME v2 directory) using the HTTP endpoint or `tls-alpn-01` or a DNS hook.
* `QuiclyClient` connects with a transport factory, performs the session handshake, and applies a
  `ReconnectPolicy` (new epoch, resumable bulk transfers, pending reliable operations fail with `Disconnected`).

## 8. Security defaults

TLS 1.3 is mandatory in QUIC; "encryption as an option" therefore means: certificate validation policy
(system roots / pinned hash / custom callback / insecure-for-dev), client authentication token, origin
validation for browser sessions, 0-RTT **disabled** by default, admission and resource limits, decompression
bombs bounded by `MaxMessageSize` before any buffer is chosen.

## 9. Testing & measurement

* Unit tests target ~100 % line coverage of `Core`, `Http3`, `Acme`, `Http`, `Testing`, `Replication`.
* `SimulatedTransport` makes every delivery-mode property testable deterministically: lost final update,
  lost ack, sequence roll-over, disconnect during a borrowed send, cancel mid-receive, movement latency while a
  bulk transfer saturates the link.
* End-to-end tests run real MsQuic loopback (raw and WebTransport), the ACME client against an in-process
  mock CA, and `HttpClient` (HTTP/3) against our HTTP/3 server.
* Every optimisation follows hypothesis → benchmark (BenchmarkDotNet, `Archive` baseline) → decision, recorded
  in `docs/benchmarks/`.
