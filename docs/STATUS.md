# Implementation status

Last updated 2026-09-16. Test counts are totals across both target frameworks (net11.0 + net10.0).
Line coverage figures are from the module reviews (Microsoft.Testing.Extensions.CodeCoverage, cobertura).

## Merged into `main`

| Module | Content | Tests | Line coverage | Benchmarks |
|---|---|---|---|---|
| Core · Transport | `ITransport` / `ITransportSink` contract, `TransportSegment` (QUIC_BUFFER layout) | in Core | 100 % | — |
| Core · Primitives | QUIC varints (minimal-encoding check), RFC 1982 serials, xxHash64, LZ4 block codec | in Core | ~100 % (big-endian arms unreachable on x64) | [primitives](benchmarks/primitives.md) |
| Core · Memory | lock-free sharded slab allocator, `BufferLease`, shared leases | in Core | 99.1 % | [memory](benchmarks/memory.md) |
| Core · Threading / Time | SPSC and MPSC rings (native memory), `CompletionTable`, clocks | in Core | 100 % | [threading](benchmarks/threading.md) |
| Core · State | native arrays, send-entry table, key tables, mailboxes, channel state, segment arena | in Core | 99.8 % | [state](benchmarks/state.md) |
| Core · Channels / Framing | channel table + canonical hash, datagram/container/stream framing, incremental stream parser | in Core | 99.9 % | [framing](benchmarks/framing.md) |
| Core · Control | control-protocol codec, session tokens, auth-failure limiter | in Core | 100 % | control numbers in the module notes |
| Core · Session | `QuiclyPeer`, handshake and admission, control stream, ping and clock sync, scheduler and packer, unreliable / sequenced / ordered / group-stream / latest-value engines, datagram fragmentation and correlated request/response, async completion APIs, thread-safe send, Poll/Drain, per-stream idle timeout, plus the host hooks (work signal and `HasPendingWork`, peer-level `SendShared`, split poll/flush deadlines, in-place reconnect with session resume, `PeerOptions.Clone`/`Validate`, `IsDisposed`) | in Core | 96.6 % | [session](benchmarks/session.md) |
| **Core total** | | **3 426** | | |
| Http3 | HTTP/3 frames, QPACK (static + Huffman), HTTP datagrams, WebTransport framing and capsules | 504 | 100 % | [http3](benchmarks/http3.md) |
| Transport.MsQuic | layout-validated MsQuic interop and wrappers; MsQuic-backed `ITransport`, connector, listener with reference-counted certificate hot swap | 722 (4 skipped off-Windows) | 98.4 % bindings, 93.5 % transport | [msquic-transport](benchmarks/msquic-transport.md) |
| Testing | deterministic simulated network and transport, recording sink, test certificates, in-process fake ACME CA, transport conformance suite (24 scenarios, run against the simulator and MsQuic) | 366 | 97.6 % | [simulation](benchmarks/simulation.md) |
| Acme | RFC 8555 client for any CA, EAB, http-01 / dns-01 / tls-alpn-01, renewal (ARI aware) | 430 | ~98 % (Unix-only branches) | [acme](benchmarks/acme.md) |
| Server | certificate provisioning (static, file, ACME) with consumer binding and grace-period disposal; admission with per-address limits and an auth-failure limiter; sessions with token resume, a grace-period replay guard and epoch bumps; dense peer table, peer sets and shared broadcast; work-signal driven `PollAll`; graceful shutdown; optional HTTP side endpoint | 662 | 99.6 % | — |
| Client | connect with cancellation, reconnect with back-off and jitter, in-place session resume on the same peer, fresh-session fallback | 84 | 100 % | — |
| EndToEnd · Certificates | real-network suite: fake ACME CA to provisioner to MsQuic listener to TLS 1.3 handshake; hot swap, SPKI pinning, tls-alpn-01, restart reuse | 30 | — | — |
| Http | hardened HTTP/1.1 server, ACME responders, ClientHello peek for tls-alpn-01, separate TLS handshake timeout, cached certificate contexts, static files | 788 (2 skipped) | 99.7 % | [http](benchmarks/http.md) |
| Replication | entity ids, bit packing, quantisation, SIMD delta codec, snapshot history, inputs, prediction, interpolation, dedup, tick clock | 474 | 100 % | [replication](benchmarks/replication.md) |

## In progress

| Module | Content |
|---|---|
| Core · Bulk (wave C2) | large-object transfers: per-transfer streams, chunking and compression, progress, cancel, resume, request authorisation, whole-object hash |

## Remaining

| Wave | Content |
|---|---|
| Core follow-up | in-place reconnect must leave the peer re-armable when the transport connector throws |
| C4 | WebTransport-over-HTTP/3 carrier (opt-in) |
| C5 | End-to-end tests over real MsQuic loopback with the session layer, samples, end-to-end benchmarks |
| later | Browser (WebAssembly) transport — optional, waits for .NET 11 browser tooling to mature |
