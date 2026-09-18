# Wave C5 plan: end-to-end verification, samples, end-to-end benchmarks

Status: **not started** (parked 2026-09-18). All six delivery modes, fragmentation, request/response, the server and the
client are merged on `main`, so every precondition is met. Wave C4 (the opt-in WebTransport carrier) is not a
prerequisite. See [STATUS.md](../STATUS.md).

Coordinate before starting: benchmarks on a shared machine invalidate each other, so take turns with any other session
that is measuring. C5 touches `tests/Tedd.Quicly.EndToEnd.Tests`, `samples/`, a new `benchmarks/Tedd.Quicly.Benchmarks/EndToEnd`
folder and `docs/benchmarks/end-to-end.md`, and may fix library defects it exposes in `src/`.

## Starting point

- `samples/Tedd.Quicly.Samples.Server` and `.Client` are six-line placeholders. Both target `net11.0`; the server sample
  references Server and Transport.MsQuic, the client sample Client and Transport.MsQuic.
- `tests/Tedd.Quicly.EndToEnd.Tests` already references every `src` project. Today it covers certificates and TLS only
  (hot swap, certificate paths, client validation, restart, tls-alpn-01 provisioning, a smoke test) plus reusable
  infrastructure for a real MsQuic server and client. Session-layer tests go in a new `Session/` folder and reuse
  `EndToEndCollection`, which serialises the suite.
- The server does not serve HTTP/3 on the game port. The HTTP/3 codec is consumed only by wave C4, so HTTP coexistence
  in C5 means the HTTP/1.1 side endpoint and its ACME responders.

## Track 1: session-layer end-to-end tests over real MsQuic loopback

Real `QuiclyServer` and `QuiclyClient` over MsQuic on 127.0.0.1 with ephemeral ports. No simulator. Both target frameworks.

1. **All six modes on one connection**, each guarantee observed on the receiver: sequenced never delivers an older serial,
   ordered arrives in order, latest converges to the final value, group streams do not block each other. Statistics
   prove unreliable modes travelled as datagrams and reliable ones as streams.
2. **At least 50 concurrent clients** through the work-signal driven `PollAll`/`FlushAll` loop: isolation, completion of
   every exchange, and a `PeerSet` broadcast reaching exactly its members.
3. **Reconnect with session resume mid-traffic**, asserting the PROTOCOL 4.1 channel rules, plus the refusal path that
   falls back to a fresh session.
4. **Bulk beside 60 Hz movement**: the transfer completes with its hash verified on the receiver, and the movement p99
   inter-arrival stays within a generous multiple of the send interval. This is the headline claim of the Bulk design.
5. **Fragmentation on a real path**, including two fragmented messages in flight at once.
6. **Request/response on a real path**: a response larger than one frame, a timeout with no responder, a late response.
7. **Shutdown and failure**: graceful shutdown, abrupt server disposal, client dispose mid-bulk, each followed by full
   resource accounting and every outstanding await completed.
8. **Zero GC allocations on the game thread** in a warmed 60 Hz loop with all six modes active.
9. **HTTP/1.1 side endpoint** answering health and the ACME http-01 responder while game traffic flows.

## Track 2: the two samples

A small real game rather than an echo: a 60 Hz server sending world data over Bulk on connect, positions over
UnreliableSequenced, chat over ReliableOrdered, an inventory request over request/response and match state over
ReliableLatest. The console client shows positions, bulk progress, request round-trip time and reconnect status, and
survives a server restart. Flags for address, port, client count, bulk size and a `--duration` for unattended runs.
Allocation-free hot loop, clean Ctrl+C, and a `samples/README.md`.

## Track 3: end-to-end benchmarks

Over real MsQuic loopback, following ADR 0007: connection setup, round-trip latency per mode, bulk throughput raw and
compressed, movement packet rate with one and many peers, `PollAll` plus `FlushAll` cost at as many peers as loopback
sustains (aim for 1 000; never extrapolate), and game-thread allocation per second. State what loopback flatters: no
real round-trip time, no loss, kernel-local copies. Measure on a quiet machine only.

## Verified warnings from the module reviews

These come from adversarial reviews and fix passes. Each one was checked, not guessed.

1. Assert on the receiver, never on the sender's status alone. A sender's `Completed` is a claim about the peer.
2. Assert reassembled fragment sizes against the channel limit, not only against the sent length.
3. Watch stream credit over long sessions. An accounting slip surfaces on MsQuic as stream-limit exhaustion, not as a
   bulk error.
4. Close before dispose, or put a timeout on every await in teardown, so a regression fails instead of hanging.
5. The receive byte budget must be at least twice the effective bulk chunk size, which is the smaller of the chunk option
   and the pool's largest block. Otherwise chunked transfers reset with a limit error.
6. Request timeouts are served only while connected and only from Poll or Flush, and requests are buffered, so a loop
   that never flushes never sends them. The request table has 256 slots and costs one per unconsumed value task.
7. Every existing session benchmark is single-core with both peers and the simulator on one thread. MsQuic loopback
   numbers differ in kind and must not be compared with them directly.
8. A closed peer that is never disposed keeps its partial reassembly buffers.
9. A carrier that reports no congestion window moves bulk at the 16 KiB/s floor unless a rate is configured.

## Questions only a real MsQuic transport can answer

The Bulk fix pass could not settle these in the simulator. Track 1 should settle each one explicitly.

- Does MsQuic raise `IDEAL_SEND_BUFFER_SIZE` for a bulk stream after `START_COMPLETE`, and how often? The engine matches
  the report by the stream id it records at start, so an earlier report would be missed until the ideal grows.
- Does a connection teardown reach a stream as shutdown-complete without a preceding abort? That decides whether a
  transfer cut by a lost connection ends `Disconnected` and can be resumed, or ends `Failed`. Test it with a resumable
  range request across a real connection loss and reconnect.
- Is `OnClosed` still delivered after `QuiclyPeer.Dispose` closes the transport? Receive sinks are finished, and staging
  leases returned, only when it is.

## Follow-ups recorded elsewhere

- Encoding "none" unambiguously in the integer rings was deferred. See session-layer section 7.7. The repo-wide audit found
  no read of a ring's out value after a false return.
- The completion-table dispose fix adds two interlocked operations and two volatile reads to every tracked-send release.
  It has not been benchmarked yet.
