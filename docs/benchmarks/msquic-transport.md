# MsQuic transport — `MsQuicTransport` over loopback

`src/Tedd.Quicly.Transport.MsQuic/Transport/` implements the Core `ITransport` contract over the in-repo MsQuic wrapper:
`MsQuicTransport` (one connection: datagrams, a generation-tagged stream table, event translation),
`MsQuicTransportConnector` (client), `MsQuicTransportListener` (server: pre-handshake admission, accept, certificate hot
swap) and `MsQuicTransportOptions` (ARCHITECTURE §7 settings, certificate validation). This page records the MsQuic
behaviour the design depends on (measured before and while writing it, and by the performance review), which calls wait
for the MsQuic worker, the zero-allocation evidence, and the loopback benchmarks with the decisions taken (ADR 0007).
It was revised after the performance and contract reviews (2026-09-15); claims those reviews corrected are marked.

## 1. Measured MsQuic behaviour

Loopback experiments at the wrapper level (msquic.dll 2.5.10 from the .NET shared framework, Schannel, Windows 11). The
exploration tests have been removed; every row that the transport relies on is now asserted by a transport test or a
conformance scenario.

| # | Experiment | Observation | Consequence in `MsQuicTransport` |
|---|---|---|---|
| E1 | RECEIVE consumes 10 of 1 000 bytes (no PENDING); more data follows | No further RECEIVE, not even for new data, until `StreamReceiveSetEnabled(TRUE)`; then the remainder and the new data arrive from offset 10 | A partial consume must re-enable receives, or the stream stalls |
| E2 | Return PENDING, later `StreamReceiveComplete(10)` | Paused exactly as in E1 until `StreamReceiveSetEnabled(TRUE)`; the next indication starts at offset 10, so the argument is the total consumed from the held indication | Superseded by R4: the transport no longer leaves a receive pending for another thread to complete |
| E3 | Return PENDING, later `StreamReceiveComplete(all)` | Delivery continues on its own | As E2 |
| R4 | Return PENDING, and `StreamReceiveComplete(0)` from another thread **before the callback has returned to MsQuic**, then `StreamReceiveSetEnabled(TRUE)` (second review of the group-stream fix, 2026-09-30; read from the release/2.5 source: `MsQuicStreamReceiveComplete`, `QuicStreamRecvFlush`, `QuicStreamRecvSetEnabledState`) | While a receive call is active the API only adds the length to the stream's completion counter and queues nothing; after a PENDING return MsQuic completes the receive only if that counter is not zero; the re-enable finds a read still pending and queues no flush. The stream is never indicated again. A completion of at least one byte is not lost, and neither is any completion issued after the callback has returned | A hold is answered as a **partial consumption** (success with the bytes the sink took, possibly 0): MsQuic completes it itself and pauses the stream as in E1, and `ResumeStreamReceive` only calls `StreamReceiveSetEnabled(TRUE)`, which is always a queued operation. Bytes a resume credits are skipped at the head of the next indication. An indication the sink consumed whole and held keeps one byte back (skipped later) so that the completion is partial; an indication that carries only the FIN stays pending and is indicated again by the re-enable alone (MsQuic has no read pending for it). Asserted by the conformance scenario `HeldStreamIsIndicatedAgainAfterEveryResume` and a transport stress test; on the old code a stream held and resumed with zero bytes stalled after 2 to 2 000 cycles |
| E6 | Partial consume of an indication that carries FIN | Paused; after re-enabling, the remainder is indicated again with FIN | Same rule at the FIN |
| E7 | `StreamReceiveSetEnabled(TRUE)` inside RECEIVE, then return a partial count | The remainder is indicated again immediately (without new data), repeatedly while each call consumes something | How a partial `Consumed(n)` is re-indicated; `Consumed(0)` must not do this (it would spin) |
| E4 | `StreamStart(FAIL_BLOCKED \| SHUTDOWN_ON_FAIL)` without peer credit, off the worker | Returns PENDING; START_COMPLETE(STREAM_LIMIT_REACHED); a send queued behind the start completes canceled; SEND_SHUTDOWN_COMPLETE; SHUTDOWN_COMPLETE | A stream-limit refusal is reported as `OnStreamStarted(StreamLimitReached)`, the canceled completion, then `OnStreamShutdownComplete`; the stream never starts (a later `StartStream` is `InvalidState`) — the simulator now reports the same sequence |
| E11 | The same start issued inline from the CONNECTED callback | Still PENDING; the failure again arrives through START_COMPLETE | The synchronous `StreamLimitReached` path is kept (and reported once) but practically unused on MsQuic |
| E5 | `DatagramSend` before CONNECTED | PENDING (MsQuic queues it) | The transport refuses it itself (`InvalidState`), as the contract and the simulator do |
| E5 | Datagram of MaxSendLength + 1 bytes; of exactly MaxSendLength | INVALID_PARAMETER; accepted | Size pre-check against the cached maximum; INVALID_PARAMETER on a datagram send maps to `TooLarge` (a path-MTU drop between check and call) |
| E5 | `QUIC_PARAM_CONN_CLOSE_REASON_PHRASE` without / with a NUL terminator | INVALID_PARAMETER / SUCCESS | `Close` copies the reason NUL-terminated |
| E8 | Abort of a stream that was never started; connection shutdown with such a stream open | No event at all | `AbortStream` of a never-started stream releases it like `CloseStream`; at connection shutdown the transport raises `OnStreamShutdownComplete` for such streams itself, before `OnClosed` |
| R1 | `StreamClose` of a never-started stream, off the worker (performance review; **corrects** "inline" in the first version of this page) | MsQuic runs the close on the connection's worker and **waits for it**: 14–16 µs with an idle worker, as long as a running callback takes otherwise; SHUTDOWN_COMPLETE for the stream is indicated during that close | Every native `StreamClose` runs on the transport's cleanup work item (a thread-pool thread), never on the calling thread; events that arrive while the transport closes a stream natively are ignored |
| R2 | `StreamClose` of a stream that already reported SHUTDOWN_COMPLETE, off the worker (review) | About 10 µs on the calling thread, no wait | Also moved to the cleanup work item |
| R3 | Any `SetParam` / `GetParam` (stream priority, settings, close reason, statistics) off the connection's worker (review) | Queued to the worker and waited for: 13–16 µs idle, a running callback's duration otherwise; from a callback of **another** connection on the same worker it deadlocks | `OpenStream`'s priority is stored and applied from START_COMPLETE (where the call runs inline); the members that still wait are listed in §3 |
| E9 | 200 datagrams queued, then an immediate connection shutdown | All 200 reach a final state (the queued ones CANCELED) before the connection's SHUTDOWN_COMPLETE | "Every accepted datagram reaches a final state before `OnClosed`" holds without per-datagram tracking |
| E10 | 64 × 64 KiB stream sends in flight (receiver holding back), connection shutdown | 63 SEND_COMPLETE(canceled), then the stream's SHUTDOWN_COMPLETE, then the connection's SHUTDOWN_COMPLETE | "In-flight sends complete canceled before `OnClosed`" holds without per-send tracking |
| E13 | `StreamReceiveComplete` after a local ABORT_RECEIVE or a peer reset of a pending stream | Harmless | A resume racing an abort cannot crash the process (MsQuic asserts only on over-completion, which `ResumeStreamReceive` validates) |
| — | STREAMS_AVAILABLE | Indicated before CONNECTED, with the initial allowance | `OnStreamsAvailable` is only raised once connected (contract: "Only raised after OnConnected") |
| — | A listener's NEW_CONNECTION returns a configuration | The wrapper applies it (`ConnectionSetConfiguration`) only after the callback returns, holding no reference in between | The listener takes a per-connection reference on the configuration before returning it and drops it when the handshake ends or the connection is refused, so a certificate swap never closes a configuration inside that window (stress-tested: parallel handshakes during 20 swaps, no failed handshake, no retired certificate validated after its retirement) |

## 2. Contract decisions that came out of the measurements and the reviews

The shared conformance suite (`src/Tedd.Quicly.Testing/Conformance`, 25 scenarios) runs unchanged against the simulator
(clean link and a 10 ms / 3 ms jitter / 10 % stream-loss link) and against `MsQuicTransport` over loopback. Where MsQuic
could not do what the simulator documented, the contract was clarified and the simulator aligned, so both behave the same:

* **Partial consumption** (`ReceiveResult` XML doc): consuming at least one byte but fewer than indicated gets the rest
  indicated again without waiting for new data (MsQuic: right after the callback, via inline `StreamReceiveSetEnabled`;
  simulator: next advance step). Consuming nothing of a non-empty indication without `Pending` counts as
  `PendingAfter(0)` (MsQuic cannot "wait for more data" while receives are disabled, and re-enabling would spin).
* **Resume racing the callback**: a `ResumeStreamReceive` issued on another thread while the receive callback is still
  returning `Pending` is applied when it returns (a per-stream state machine: idle → in-callback → pending, with an
  early-resume state). Core's "remember the stream, resume from Poll" pattern needs this on a real transport; a
  conformance scenario now races it on both transports.
* **Refused streams** (`ITransport.StartStream`, `TransportStatus.StreamLimitReached`): a start the peer's stream limit
  refuses never starts. MsQuic reports it asynchronously (E4) and the simulator now does exactly the same: the call
  returns `Success`, then `OnStreamStarted(StreamLimitReached)`, a canceled completion for a send made with the start, and
  `OnStreamShutdownComplete`; `StartStream` on the stream stays `InvalidState` even after `OnStreamsAvailable`. The
  simulator used to return `StreamLimitReached` synchronously and let the stream be started again later. The session layer
  releases the refused stream with `CloseStream` and sends again on a **new** stream after `OnStreamsAvailable`.
* **Streams that never started**: `AbortStream` releases them like `CloseStream` (no callback); at connection close they
  get `OnStreamShutdownComplete` before `OnClosed`.
* **`CloseStream` before shutdown** aborts both directions with code 0, still reports pending completions (canceled) and
  suppresses the shutdown callback.
* **Refused connections**: the transport an accept callback refuses (null or an exception) is `Closed` at once and raises
  no callback (MsQuic keeps the native connection); the accept callback must not call members of the new transport
  (MsQuic can lose events then) — the simulator's conformance harness now raises its stream limits after the callback.
* **Close codes**: for `TransportCloseReason.Transport` the error code and status are transport-specific (MsQuic: the QUIC
  transport error code and the `QUIC_STATUS`; the simulator: 0 and its `Status*` constants). When both ends close at once
  each end reports `Local` (its own close won) or `Peer` (the other close arrived first).
* **`Connect` may throw** when the attempt cannot even be started (MsQuic refuses `ConnectionStart`); every later failure
  is an `OnClosed(Transport)`, and the simulator reports all failures that way.
* **Lowering a stream limit** takes nothing back (QUIC never revokes stream credit): MsQuic only limits credit it grants
  later, the simulator ignores a lower value once the old one can have reached the peer.
* **`TransportCapabilities.CancelOnBlocked`** reports whether `TransportSendFlags.CancelOnBlocked` is honoured (MsQuic
  2.4+; the simulator always).

The harness abstraction gained what the new scenarios need: `ITransportTestHarness.Connect` (a client against a listener
with the scenario's own pre-handshake and accept callbacks), `ConformancePairOptions.TransportCloseAfter` (MsQuic: the idle
timeout with keep-alives off; simulator: a link cut) and `ConformancePairOptions.FailHandshake` (MsQuic: a client pin that
matches no certificate; simulator: `LinkOptions.FailHandshake`). The scenarios added after the reviews cover refused
connections, a failed handshake, `Close` while connecting, both ends closing at once, a transport-initiated close, datagrams
in flight at close, a refused start made inside a callback, aborting never-started streams and the racing resume.

## 3. What waits for the MsQuic worker

MsQuic executes an API call inline when it is made on the connection's worker thread (inside one of its callbacks) and
otherwise either queues it and returns (most calls) or queues it and **waits** for the worker (parameter calls, and
`StreamClose` of a never-started stream). A waiting call made from a callback of *another* connection that shares the
worker deadlocks. The transport keeps the game thread off the waiting paths wherever the contract allows it:

| Member | Waits for the worker? | Notes |
|---|---|---|
| `OpenStream` (any priority) | No | `StreamOpen` does not wait; a non-default priority is stored and applied from START_COMPLETE (was a 13 µs blocking `SetParam` per open) |
| `StartStream`, `SendStream`, `SendDatagram`, `AbortStream`, `ResumeStreamReceive` | No | Queued MsQuic calls |
| `CloseStream` | No | The native `StreamClose` runs on the cleanup work item; every close queued while it is pending is handled by that one work item (batched per connection) |
| `SetStreamPriority` | Before the start: no (stored). On a started stream: yes | `QUIC_PARAM_STREAM_PRIORITY`, about 15 µs with an idle worker |
| `UpdatePeerStreamLimits` | Yes | `QUIC_PARAM_CONN_SETTINGS` |
| `Close` | Only with a reason | `QUIC_PARAM_CONN_CLOSE_REASON_PHRASE`; the shutdown itself is queued |
| `GetStatistics` | Yes | `QUIC_PARAM_CONN_STATISTICS_V2`, about 16 µs with an idle worker |
| `Dispose` | No | The handles are closed on a thread-pool thread once SHUTDOWN_COMPLETE arrived (`WaitForHandlesClosed`) |

## 4. Zero allocation in steady state

`tests/Tedd.Quicly.Transport.MsQuic.Tests/Transport/MsQuicTransportAllocationTests.cs` (own collection with parallelisation
disabled, so the process-wide counter sees only this test): after 2 000 warm-up messages, 10 000 more are sent through an
allocation-free counting sink.

| Path | Sending thread (`GC.GetAllocatedBytesForCurrentThread`) | MsQuic worker threads, between callbacks | Whole process (`GC.GetTotalAllocatedBytes(true)`) |
|---|---|---|---|
| Datagrams, 64 B, `SendDatagram` → DATAGRAM_RECEIVED / DATAGRAM_SEND_STATE_CHANGED | 0 B | 0 B (client and server) | 0 B (asserted < 64 KiB) |
| Stream, 1 KiB sends on one stream → RECEIVE / SEND_COMPLETE | 0 B | 0 B (client and server) | 0 B (asserted < 64 KiB) |

Measured on .NET 10.0.12 and .NET 11 preview 7, again after the review fixes. The 64 KiB budget leaves room for runtime
and test-runner background work; the observed value was 0 B. What makes it hold: segments go to MsQuic as `QUIC_BUFFER*`
unchanged (64-bit processes only: `TransportSegment` matches `QUIC_BUFFER` there, and the connector, listener and transport
refuse to run in a 32-bit process), contexts are passed through as MsQuic client contexts, flags map through two 64-entry
tables, the datagram capability is one packed field read once per send, the stream-table lookup is an array index plus a
generation compare and two interlocked operations (the per-slot guard), a stream's slot is found from `MsQuicStream.Tag`,
receive buffers are reinterpreted in place as `ReadOnlySpan<TransportSegment>`, and the deferred-close list and the cleanup
work item are intrusive (the transport itself is the `IThreadPoolWorkItem`).

Allocations by design, none per message: per connection the transport object, its stream table (grown on demand up to
`MaxStreams`, slots reused) and the `IPEndPoint`s of `TransportConnectedInfo`; per stream open one `MsQuicStream` wrapper
(~100 B) plus a `GCHandle`; per accepted connection the `NewConnectionInfo` strings. `GetStatistics` reads
`QUIC_STATISTICS_V2` into a stack buffer and allocates nothing (asserted).

## 5. Benchmarks

`benchmarks/Tedd.Quicly.Benchmarks/Transport/MsQuicTransportBench.cs` (`MsQuicTransportBench`,
`MsQuicTransportDelaySendBench`), `[Config(typeof(InProcessShortRunConfig))]`. One loopback pair (127.0.0.1, pinned
self-signed certificate, default `MsQuicTransportOptions`: LOW_LATENCY profile, pacing on, CUBIC, MaxAckDelay 5 ms). The
pair shares one borrowed registration, which the benchmark now creates with `MsQuicTransportOptions.ExecutionProfile`
(it used the wrapper's default before — the same LOW_LATENCY profile, so earlier numbers are comparable). Waiting is a spin
on counters the sinks update on MsQuic worker threads, so every number includes MsQuic's own scheduling.

```
dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*MsQuicTransport*'
```

| Benchmark | Workload | Reported per |
|---|---|---|
| `DatagramPingPong64` | the client sends a 64-byte datagram (`Priority`); the server sink echoes it from inside `OnDatagramReceived`; the client spins until the echo arrives; 200 round trips per invocation | round trip |
| `DatagramThroughput64` / `DatagramThroughput1K` | 4 000 datagrams of 64 B / 1 KiB sent back to back, then wait until the server received all of them | datagram (messages/s = 1 / mean) |
| `StreamThroughput64K` | 64 sends of 64 KiB (each gathered from two 32 KiB segments) on one open stream, then wait for all 64 completions (the peer's acknowledgements) | 64 KiB send |
| `DatagramBurst32` (`DelaySend` = false / true) | 50 bursts of 32 × 64 B, `DelaySend` on the first 31 datagrams of each burst when true, each burst waited for at the receiver (a game tick's worth) | datagram |

Hardware / software (2026-09-15, re-run after the review fixes):

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.9445/25H2/2025Update/HudsonValley2)
AMD Ryzen 9 5950X 3.40GHz, 1 CPU, 32 logical and 16 physical cores
.NET SDK 11.0.100-preview.7.26381.103
  [Host]   : .NET 10.0.12 (10.0.12, 10.0.1226.42308), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.12 (10.0.12, 10.0.1226.42308), X64 RyuJIT x86-64-v3
Job=ShortRun  IterationCount=3  LaunchCount=1  WarmupCount=3
```

On .NET 10 `Program.CreateConfig` adds the default out-of-process ShortRun job next to the class's in-process one, hence
two rows per method. ShortRun has N = 3, so the *Error* column is wide; read *Mean* and *StdDev*. Other agents' test runs
shared the machine, which shows in the ping-pong spread.

### Results (net10.0)

| Method               | Toolchain              | Mean         | Error        | StdDev       | Allocated |
|--------------------- |----------------------- |-------------:|-------------:|-------------:|----------:|
| DatagramPingPong64   | Default                | 108,575.6 ns | 190,679.5 ns | 10,451.79 ns |         - |
| DatagramThroughput64 | Default                |     844.2 ns |     349.3 ns |     19.15 ns |         - |
| DatagramThroughput1K | Default                |  19,322.1 ns |  13,364.2 ns |    732.54 ns |         - |
| StreamThroughput64K  | Default                | 308,116.4 ns | 365,195.9 ns | 20,017.62 ns |         - |
| DatagramPingPong64   | InProcessEmitToolchain | 102,888.5 ns | 209,551.6 ns | 11,486.23 ns |         - |
| DatagramThroughput64 | InProcessEmitToolchain |     916.2 ns |     521.9 ns |     28.61 ns |         - |
| DatagramThroughput1K | InProcessEmitToolchain |  21,301.9 ns |  23,620.9 ns |  1,294.74 ns |         - |
| StreamThroughput64K  | InProcessEmitToolchain | 301,350.2 ns | 213,162.0 ns | 11,684.13 ns |       2 B |

| Headline | Out-of-process | In-process | First run, before the fixes (out / in) |
|---|---:|---:|---:|
| Datagram round trip, 64 B | 109 µs | 103 µs | 89 / 98 µs |
| One-way datagrams, 64 B | 1.18 M msg/s (76 MB/s of payload) | 1.09 M msg/s | 1.25 / 1.15 M msg/s |
| One-way datagrams, 1 KiB | 52 k msg/s (53 MB/s) | 47 k msg/s | 47 / 47 k msg/s |
| Stream, 64 KiB gathered sends | 213 MB/s | 217 MB/s | 238 / 189 MB/s |

The differences from the first run are inside the run-to-run spread of ShortRun on a shared machine; the only hot-path
change in the fixes is that `SendDatagram` reads the packed datagram capability once. The in-process ping-pong iterations
ranged from 96 to 116 µs; the review's distribution measurement (see Reading) is the better latency reference.

Delivery during the runs: no datagram was lost or canceled in any phase and no ping timed out (9.2 M datagrams of 64 B
per throughput phase, 508 k and 316 k of 1 KiB). In the in-process 1 KiB phase about 13 k datagrams (13 MB, the size of the
congestion window MsQuic reaches) were still unacknowledged when the counters were printed right after the last
invocation, so their final states were pending, not missing. The 2 B/op in the in-process stream row is BenchmarkDotNet's
process-wide counter picking up runtime background work (about 128 B per invocation of 64 sends); the dedicated allocation
tests of §4 measure 0 B.

`DatagramBurst32` (N = 3):

| Method          | Toolchain              | DelaySend | Mean     | Error     | StdDev    | Allocated |
|---------------- |----------------------- |---------- |---------:|----------:|----------:|----------:|
| DatagramBurst32 | Default                | False     | 2.996 us | 3.4882 us | 0.1912 us |         - |
| DatagramBurst32 | InProcessEmitToolchain | False     | 3.150 us | 0.3387 us | 0.0186 us |         - |
| DatagramBurst32 | Default                | True      | 2.960 us | 4.2114 us | 0.2308 us |         - |
| DatagramBurst32 | InProcessEmitToolchain | True      | 2.895 us | 3.1272 us | 0.1714 us |         - |

The first version of this page reported an N = 15 re-run in which the bursts took 2.46 / 2.56 µs per datagram without the
flag and 2.92 / 3.01 µs with it; neither this run nor the review's alternating rounds reproduce such a difference.

### Reading

* **Round trip ≈ 90 µs is real, and the transport's share of it is not measurable.** The performance review measured the
  distribution directly: p50 77 µs, mean 83–85 µs, about 37 µs per direction. Plain .NET UDP sockets on the same machine
  take p50 38 µs per loopback round trip with both ends blocking in `Receive` and 25 µs with a spinning client; MsQuic's
  own minimum RTT estimate (about 50 µs) equals the transport's minimum round trip, so what `MsQuicTransport` adds is
  below the noise. The benchmark's `SpinWait` (with its occasional yield) adds about 8 µs at p50 compared with a busy spin.
  LOW_LATENCY is the right default: MAX_THROUGHPUT measured 97 µs p50; REAL_TIME the same p50 with a worse tail.
* **Datagram throughput is bounded by per-packet CPU, not by congestion control or pacing** (**corrects** the first version
  of this page). MsQuic spends about 24 µs of worker CPU per 1 KiB datagram (one packet each), which caps 1 KiB datagrams
  near 64 k packets/s; 64-byte datagrams pack 12.0 DATAGRAM frames per 1 200-byte packet (not about 18), about 90 k
  packets/s, hence roughly a million 64-byte messages per second. Pacing on or off makes no difference, and the congestion
  window grows to 17–20 MB, far beyond anything these runs keep in flight.
* **Streams move 4 to 5 times more bytes than 1 KiB datagrams**: stream data leaves the send queue in large batches and
  costs one completion per 64 KiB send, while every datagram costs two send-state callbacks (SENT and the final state).
* **DelaySend: no measurable effect on loopback** (**corrects** "hypothesis rejected"). The earlier N = 15 run that showed
  bursts 16–19 % slower with the flag does not hold up: alternating rounds with and without the flag on one connection show
  no consistent difference (p50 58–75 µs per burst either way; 1.0–1.17 M datagrams/s unpaced), and the N = 3 rows above
  point in no consistent direction either.

### Decisions

* Keep the `TransportSendFlags` → MsQuic flag mapping as specified (`DelaySend` → `DELAY_SEND`). It buys nothing on
  loopback and costs nothing measurable; the session layer need not set it for tick bursts (packing messages into
  containers, PROTOCOL §2.2, already removes most small-datagram bursts). Revisit on a real network (pacing, GSO).
* Keep the ARCHITECTURE §7 defaults (LOW_LATENCY, pacing, CUBIC): the review's profile comparison supports LOW_LATENCY,
  and the throughput figures are loopback ceilings set by MsQuic's per-packet CPU, for capacity planning rather than tuning.
* No optimisation round (nothing moves to `benchmarks/Tedd.Quicly.Archive`): the transport adds no allocation, its CPU
  cost is not separable from MsQuic's, and the review fixes left the hot paths unchanged apart from reading the packed
  datagram capability once per send.
