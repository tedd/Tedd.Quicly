# Implementation status

Last updated 2026-09-15. Test counts are totals across both target frameworks (net11.0 + net10.0).
Line coverage figures are from the module reviews (Microsoft.Testing.Extensions.CodeCoverage, cobertura).

## Merged into `main`

| Module | Content | Tests | Line coverage | Benchmarks |
|---|---|---|---|---|
| Core · Transport | `ITransport` / `ITransportSink` contract, `TransportSegment` (QUIC_BUFFER layout) | in Core | 100 % | — |
| Core · Primitives | QUIC varints (minimal-encoding check), RFC 1982 serials, xxHash64, LZ4 block codec | in Core | ~100 % (big-endian arms unreachable on x64) | [primitives](benchmarks/primitives.md) |
| Core · Memory | lock-free sharded slab allocator, `BufferLease`, shared leases | in Core | 99.1 % | [memory](benchmarks/memory.md) |
| Core · Threading / Time | SPSC and MPSC rings, `CompletionTable`, clocks | in Core | 100 % | [threading](benchmarks/threading.md) |
| Core · State | native arrays, send-entry table, key tables, mailboxes, channel state | in Core | 99.8 % | [state](benchmarks/state.md) |
| Core · Channels / Framing | channel table + canonical hash, datagram/container/stream framing, incremental stream parser | in Core | 99.9 % | [framing](benchmarks/framing.md) |
| Core · Control | control-protocol codec, session tokens, auth-failure limiter | in Core | 100 % | control numbers in the module notes |
| **Core total** | | **2 492** | | |
| Http3 | HTTP/3 frames, QPACK (static + Huffman), HTTP datagrams, WebTransport framing and capsules | 504 | 100 % | [http3](benchmarks/http3.md) |
| Transport.MsQuic | layout-validated MsQuic interop and wrappers; MsQuic-backed `ITransport`, connector, listener with reference-counted certificate hot swap | 722 (4 skipped off-Windows) | 98.4 % bindings, 93.5 % transport | [msquic-transport](benchmarks/msquic-transport.md) |
| Testing | deterministic simulated network and transport, recording sink, test certificates, in-process fake ACME CA, transport conformance suite (24 scenarios, run against the simulator and MsQuic) | 360 | 97.6 % | [simulation](benchmarks/simulation.md) |
| Acme | RFC 8555 client for any CA, EAB, http-01 / dns-01 / tls-alpn-01, renewal (ARI aware) | 430 | ~98 % (Unix-only branches) | [acme](benchmarks/acme.md) |
| Server · Certificates | provisioning from a static certificate, a file or ACME; consumer binding with grace-period disposal; reverse-DNS tls-alpn-01 responder | 308 | 99.5 % | — |
| EndToEnd · Certificates | real-network suite: fake ACME CA -> provisioner -> MsQuic listener -> TLS 1.3 handshake; hot swap, SPKI pinning, tls-alpn-01, restart reuse | 30 | — | — |
| Http | hardened HTTP/1.1 server, ACME responders, ClientHello peek for tls-alpn-01, static files | 772 (2 skipped) | 99.9 % | [http](benchmarks/http.md) |
| Replication | entity ids, bit packing, quantisation, SIMD delta codec, snapshot history, inputs, prediction, interpolation, dedup, tick clock | 474 | 100 % | [replication](benchmarks/replication.md) |

## In progress

| Module | Content |
|---|---|
| Core · Session (wave C1) | `QuiclyPeer`, handshake and admission, control stream, ping/clock sync, scheduler and packer, unreliable and ordered engines, Poll/Drain, completions |

## Remaining

| Wave | Content |
|---|---|
| C2 | `ReliableLatest`, `ReliableUnordered` (group streams), `Bulk`, fragmentation, request/response engines |
| C3 | `QuiclyServer` and `QuiclyClient` (admission, peer sets and broadcast, reconnect) |
| C4 | WebTransport-over-HTTP/3 carrier (opt-in) |
| C5 | End-to-end tests over real MsQuic loopback, samples, end-to-end benchmarks |
| later | Browser (WebAssembly) transport — optional, waits for .NET 11 browser tooling to mature |
