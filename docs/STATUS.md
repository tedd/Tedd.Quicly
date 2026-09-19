# Implementation status

Last updated 2026-09-19. Test counts are totals across both target frameworks (net11.0 + net10.0), measured on `main` after
the hot-path performance pass (docs/benchmarks/session.md, "Hot-path pass") and the engine pass-work fixes (Bulk work signal and
level, owed ReliableLatest acks in the flush deadline, `PollAll` flushing only due peers; session-layer.md §4.7). Line coverage figures are from the module reviews (Microsoft.Testing.Extensions.CodeCoverage,
cobertura; the Core rows were re-measured at the wave C2 merge) and were not re-measured after the hot-path pass. Every test
project passes on both frameworks at this commit. (The certificate/TLS tests had failed on this machine since test roots with
fixed names piled up in the Windows intermediate store past its 50-per-name limit; roots are now unique and removed after use.)

## Merged into `main`

| Module | Content | Tests | Line coverage | Benchmarks |
|---|---|---|---|---|
| Core · Transport | `ITransport` / `ITransportSink` contract, `TransportSegment` (QUIC_BUFFER layout) | in Core | 100 % | — |
| Core · Primitives | QUIC varints (minimal-encoding check), RFC 1982 serials, xxHash64, LZ4 block codec | in Core | ~100 % (big-endian arms unreachable on x64) | [primitives](benchmarks/primitives.md) |
| Core · Memory | lock-free sharded slab allocator, `BufferLease`, shared leases | in Core | 99.1 % | [memory](benchmarks/memory.md) |
| Core · Threading / Time | SPSC and MPSC rings (native memory), `CompletionTable` (a wait may be consumed after the table is disposed; its free list lives on the pinned object heap), clocks | in Core | 99.4 % (two layout accessors of `MpscRing`) | [threading](benchmarks/threading.md) |
| Core · State | native arrays, send-entry table, key tables, mailboxes, channel state, segment arena | in Core | 99.8 % | [state](benchmarks/state.md) |
| Core · Channels / Framing | channel table + canonical hash, datagram/container/stream framing, incremental stream parser | in Core | 99.9 % | [framing](benchmarks/framing.md) |
| Core · Control | control-protocol codec, session tokens, auth-failure limiter | in Core | 100 % | control numbers in the module notes |
| Core · Session | `QuiclyPeer`, handshake and admission, control stream, ping and clock sync, scheduler and packer; engines for all six delivery modes (unreliable, sequenced, ordered, group-stream, latest-value, bulk); datagram fragmentation with bounded reassembly; correlated request/response; bulk transfers with progress, cancel, resume and whole-object hash; async completion APIs, thread-safe send, Poll/Drain, per-stream idle timeout; one dispose sequence that releases every await exactly once; plus the host hooks (work signal and `HasPendingWork` — both covering the engines' work for the next pass: ReliableLatest acks owed and notices, Bulk control, notices, progress, starts and cancels — peer-level `SendShared`, split poll/flush deadlines with a Poll bringing the flush deadline forward to that work (owed acks at most `AckDelay`), in-place reconnect with session resume, re-armable after a failed attempt and probed with `CanReconnect` — `PeerOptions.Clone`/`Validate`, `IsDisposed`) | in Core | 97.1 % | [session](benchmarks/session.md) |
| **Core total** | | **3 854** | | |
| Http3 | HTTP/3 frames, QPACK (static + Huffman), HTTP datagrams, WebTransport framing and capsules | 504 | 100 % | [http3](benchmarks/http3.md) |
| Transport.MsQuic | layout-validated MsQuic interop and wrappers; MsQuic-backed `ITransport`, connector, listener with reference-counted certificate hot swap | 722 (4 skipped here: one test needs machine-key rights, one runs off Windows only) | 98.4 % bindings, 93.5 % transport | [msquic-transport](benchmarks/msquic-transport.md) |
| Testing | deterministic simulated network and transport (with the ideal-send-buffer model MsQuic reports), recording sink, test certificates, in-process fake ACME CA, transport conformance suite (24 scenarios, run against the simulator and MsQuic) | 380 | 97.6 % | [simulation](benchmarks/simulation.md) |
| Acme | RFC 8555 client for any CA, EAB, http-01 / dns-01 / tls-alpn-01, renewal (ARI aware) | 430 | ~98 % (Unix-only branches) | [acme](benchmarks/acme.md) |
| Server | certificate provisioning (static, file, ACME) with consumer binding and grace-period disposal; admission with per-address limits and an auth-failure limiter; sessions with token resume, a grace-period replay guard and epoch bumps; dense peer table, peer sets and shared broadcast; work-signal driven `PollAll` that flushes only the peers whose flush deadline is due, and a `FlushAll` that skips peers whose Flush would do nothing; activation marks the peer's slot (a work bit taken before the peer was queued is no longer lost); graceful shutdown; optional HTTP side endpoint | 700 | 99.6 % | — |
| Client | connect with cancellation, reconnect with back-off and jitter, in-place session resume on the same peer (gated on `QuiclyPeer.CanReconnect`, so an attempt whose connector failed is retried in place), fresh-session fallback when the server refuses the resume; a positive sub-microsecond auto-flush interval rounds up instead of switching auto-flush off | 104 | 100 % | — |
| EndToEnd · Certificates | real-network suite: fake ACME CA to provisioner to MsQuic listener to TLS 1.3 handshake; hot swap, SPKI pinning, tls-alpn-01, restart reuse | 30 | — | — |
| Http | hardened HTTP/1.1 server, ACME responders, ClientHello peek for tls-alpn-01, separate TLS handshake timeout, cached certificate contexts, static files | 788 (2 skipped) | 99.7 % | [http](benchmarks/http.md) |
| Replication | entity ids, bit packing, quantisation, SIMD delta codec, snapshot history, inputs, prediction, interpolation, dedup, tick clock | 474 | 100 % | [replication](benchmarks/replication.md) |

## In progress

| Module | Content |
|---|---|

## Remaining

| Wave | Content |
|---|---|
| C5 | End-to-end tests over real MsQuic loopback with the session layer, samples, end-to-end benchmarks; plan and verified warnings in [design/wave-c5-plan.md](design/wave-c5-plan.md) |
| C4 | WebTransport-over-HTTP/3 carrier (opt-in) |
| later | Browser (WebAssembly) transport — deferred: optional, waits for .NET 11 browser tooling to mature |
