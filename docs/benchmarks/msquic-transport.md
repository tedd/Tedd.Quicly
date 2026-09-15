# MsQuic transport — `MsQuicTransport` over loopback

`src/Tedd.Quicly.Transport.MsQuic/Transport/` implements the Core `ITransport` contract over the in-repo MsQuic wrapper:
`MsQuicTransport` (one connection: datagrams, a generation-tagged stream table, event translation),
`MsQuicTransportConnector` (client), `MsQuicTransportListener` (server: pre-handshake admission, accept, certificate hot
swap) and `MsQuicTransportOptions` (ARCHITECTURE §7 settings, certificate validation). This page records the MsQuic
behaviour the design depends on (measured before and while writing it), the zero-allocation evidence, and the loopback
benchmarks with the decisions taken (ADR 0007).

## 1. Measured MsQuic behaviour

Loopback experiments at the wrapper level (msquic.dll 2.5.10 from the .NET shared framework, Schannel, Windows 11). The
exploration tests have been removed; every row that the transport relies on is now asserted by a transport test or a
conformance scenario.

| # | Experiment | Observation | Consequence in `MsQuicTransport` |
|---|---|---|---|
| E1 | RECEIVE consumes 10 of 1 000 bytes (no PENDING); more data follows | No further RECEIVE, not even for new data, until `StreamReceiveSetEnabled(TRUE)`; then the remainder and the new data arrive from offset 10 | A partial consume must re-enable receives, or the stream stalls |
| E2 | Return PENDING, later `StreamReceiveComplete(10)` | Paused exactly as in E1 until `StreamReceiveSetEnabled(TRUE)`; the next indication starts at offset 10, so the argument is the total consumed from the held indication | `ResumeStreamReceive(n)` calls `StreamReceiveComplete(consumedBeforePending + n)`, plus `StreamReceiveSetEnabled(TRUE)` when bytes remain |
| E3 | Return PENDING, later `StreamReceiveComplete(all)` | Delivery continues on its own | No re-enable after a full completion |
| E6 | Partial consume of an indication that carries FIN | Paused; after re-enabling, the remainder is indicated again with FIN | Same rule at the FIN |
| E7 | `StreamReceiveSetEnabled(TRUE)` inside RECEIVE, then return a partial count | The remainder is indicated again immediately (without new data), repeatedly while each call consumes something | How a partial `Consumed(n)` is re-indicated; `Consumed(0)` must not do this (it would spin) |
| E4 | `StreamStart(FAIL_BLOCKED \| SHUTDOWN_ON_FAIL)` without peer credit, off the worker | Returns PENDING; START_COMPLETE(STREAM_LIMIT_REACHED); a send queued behind the start completes canceled; SEND_SHUTDOWN_COMPLETE; SHUTDOWN_COMPLETE | A stream-limit failure is reported through `OnStreamStarted(StreamLimitReached)` followed by `OnStreamShutdownComplete` |
| E11 | The same start issued inline from the CONNECTED callback | Still PENDING; the failure again arrives through START_COMPLETE | The synchronous `StreamLimitReached` path is kept but practically unused on MsQuic |
| E5 | `DatagramSend` before CONNECTED | PENDING (MsQuic queues it) | The transport refuses it itself (`InvalidState`), as the contract and the simulator do |
| E5 | Datagram of MaxSendLength + 1 bytes; of exactly MaxSendLength | INVALID_PARAMETER; accepted | Size pre-check against the cached maximum; INVALID_PARAMETER on a datagram send maps to `TooLarge` (a path-MTU drop between check and call) |
| E5 | `QUIC_PARAM_CONN_CLOSE_REASON_PHRASE` without / with a NUL terminator | INVALID_PARAMETER / SUCCESS | `Close` copies the reason NUL-terminated |
| E8 | Abort of a stream that was never started; connection shutdown with such a stream open | No event at all | `AbortStream` of a never-started stream releases it like `CloseStream`; at connection shutdown the transport raises `OnStreamShutdownComplete` for such streams itself, before `OnClosed` |
| — | `StreamClose` of a never-started stream (found by a failing transport test) | SHUTDOWN_COMPLETE is indicated **inline**, on the thread calling `StreamClose` | Events that arrive while the transport closes a stream natively are ignored; before the fix the event re-queued the slot for closing and the slot was freed twice |
| E9 | 200 datagrams queued, then an immediate connection shutdown | All 200 reach a final state (the queued ones CANCELED) before the connection's SHUTDOWN_COMPLETE | "Every accepted datagram reaches a final state before `OnClosed`" holds without per-datagram tracking |
| E10 | 64 × 64 KiB stream sends in flight (receiver holding back), connection shutdown | 63 SEND_COMPLETE(canceled), then the stream's SHUTDOWN_COMPLETE, then the connection's SHUTDOWN_COMPLETE | "In-flight sends complete canceled before `OnClosed`" holds without per-send tracking |
| E13 | `StreamReceiveComplete` after a local ABORT_RECEIVE or a peer reset of a pending stream | Harmless | A resume racing an abort cannot crash the process (MsQuic asserts only on over-completion, which `ResumeStreamReceive` validates) |
| — | STREAMS_AVAILABLE | Indicated before CONNECTED, with the initial allowance | `OnStreamsAvailable` is only raised once connected (contract: "Only raised after OnConnected") |

## 2. Contract decisions that came out of the measurements

The shared conformance suite (`src/Tedd.Quicly.Testing/Conformance`, 13 scenarios) runs unchanged against the simulator
(clean link and a 10 ms / 3 ms jitter / 10 % stream-loss link) and against `MsQuicTransport` over loopback. Where MsQuic
could not do what the simulator documented, the contract was clarified and the simulator aligned, so both behave the same:

* **Partial consumption** (`ReceiveResult` XML doc): consuming at least one byte but fewer than indicated gets the rest
  indicated again without waiting for new data (MsQuic: right after the callback, via inline `StreamReceiveSetEnabled`;
  simulator: next advance step). Consuming nothing of a non-empty indication without `Pending` counts as
  `PendingAfter(0)` (MsQuic cannot "wait for more data" while receives are disabled, and re-enabling would spin). The
  simulator previously re-delivered such a remainder only with the next data and stalled for good after the FIN.
* **Resume racing the callback**: a `ResumeStreamReceive` issued on another thread while the receive callback is still
  returning `Pending` is applied when it returns (a per-stream state machine: idle → in-callback → pending, with an
  early-resume state). Core's "remember the stream, resume from Poll" pattern needs this on a real transport.
* **Streams that never started**: `AbortStream` releases them like `CloseStream` (no callback); at connection close they
  get `OnStreamShutdownComplete` before `OnClosed` (simulator already did the latter).
* **`CloseStream` before shutdown** aborts both directions with code 0, still reports pending completions (canceled) and
  suppresses the shutdown callback — now written into the `ITransport.CloseStream` doc.
* **`TransportCapabilities.CancelOnBlocked`** reports whether `TransportSendFlags.CancelOnBlocked` is honoured (MsQuic
  2.4+; the simulator always).

## 3. Zero allocation in steady state

`tests/Tedd.Quicly.Transport.MsQuic.Tests/Transport/MsQuicTransportAllocationTests.cs` (own collection with parallelisation
disabled, so the process-wide counter sees only this test): after 2 000 warm-up messages, 10 000 more are sent through an
allocation-free counting sink.

| Path | Sending thread (`GC.GetAllocatedBytesForCurrentThread`) | MsQuic worker threads, between callbacks | Whole process (`GC.GetTotalAllocatedBytes(true)`) |
|---|---|---|---|
| Datagrams, 64 B, `SendDatagram` → DATAGRAM_RECEIVED / DATAGRAM_SEND_STATE_CHANGED | 0 B | 0 B (client and server) | 0 B (asserted < 64 KiB) |
| Stream, 1 KiB sends on one stream → RECEIVE / SEND_COMPLETE | 0 B | 0 B (client and server) | 0 B (asserted < 64 KiB) |

Measured on .NET 10.0.12; the same tests pass on .NET 11 preview 7. The 64 KiB budget leaves room for runtime and
test-runner background work; the observed value was 0 B. What makes it hold: segments go to MsQuic as `QUIC_BUFFER*`
unchanged, contexts are passed through as MsQuic client contexts, flags map through two 64-entry tables, the stream-table
lookup is an array index plus a generation compare and two interlocked operations (the per-slot guard), a stream's slot is
found from `MsQuicStream.Tag`, receive buffers are reinterpreted in place as `ReadOnlySpan<TransportSegment>`, and the
deferred-close list and the cleanup work item are intrusive (the transport itself is the `IThreadPoolWorkItem`).

Allocations by design, none per message: per connection the transport object, its stream table (grown on demand up to
`MaxStreams`, slots reused) and the `IPEndPoint`s of `TransportConnectedInfo`; per stream open one `MsQuicStream` wrapper
(~100 B) plus a `GCHandle`; per accepted connection the `NewConnectionInfo` strings. `GetStatistics` reads
`QUIC_STATISTICS_V2` into a stack buffer and allocates nothing (asserted).

## 4. Benchmarks

`benchmarks/Tedd.Quicly.Benchmarks/Transport/MsQuicTransportBench.cs` (`MsQuicTransportBench`,
`MsQuicTransportDelaySendBench`), `[Config(typeof(InProcessShortRunConfig))]`. One loopback pair (127.0.0.1, pinned
self-signed certificate, default `MsQuicTransportOptions`: LOW_LATENCY profile, pacing on, CUBIC, MaxAckDelay 5 ms).
Waiting is a spin on counters the sinks update on MsQuic worker threads, so every number includes MsQuic's own scheduling.

```
dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*MsQuicTransport*'
```

| Benchmark | Workload | Reported per |
|---|---|---|
| `DatagramPingPong64` | the client sends a 64-byte datagram (`Priority`); the server sink echoes it from inside `OnDatagramReceived`; the client spins until the echo arrives; 200 round trips per invocation | round trip |
| `DatagramThroughput64` / `DatagramThroughput1K` | 4 000 datagrams of 64 B / 1 KiB sent back to back, then wait until the server received all of them | datagram (messages/s = 1 / mean) |
| `StreamThroughput64K` | 64 sends of 64 KiB (each gathered from two 32 KiB segments) on one open stream, then wait for all 64 completions (the peer's acknowledgements) | 64 KiB send |
| `DatagramBurst32` (`DelaySend` = false / true) | 50 bursts of 32 × 64 B, `DelaySend` on the first 31 datagrams of each burst when true, each burst waited for at the receiver (a game tick's worth) | datagram |

Hardware / software (2026-09-15):

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.9445/25H2/2025Update/HudsonValley2)
AMD Ryzen 9 5950X 3.40GHz, 1 CPU, 32 logical and 16 physical cores
.NET SDK 11.0.100-preview.7.26381.103
  [Host]   : .NET 10.0.12 (10.0.12, 10.0.1226.42308), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.12 (10.0.12, 10.0.1226.42308), X64 RyuJIT x86-64-v3
Job=ShortRun  IterationCount=3  LaunchCount=1  WarmupCount=3
```

On .NET 10 `Program.CreateConfig` adds the default out-of-process ShortRun job next to the class's in-process one, hence
two rows per method. ShortRun has N = 3, so the *Error* column is wide; read *Mean* and *StdDev*.

### Results (net10.0)

| Method               | Toolchain              | Mean         | Error        | StdDev       | Allocated |
|--------------------- |----------------------- |-------------:|-------------:|-------------:|----------:|
| DatagramPingPong64   | Default                |  89,210.7 ns |  51,098.1 ns |  2,800.86 ns |         - |
| DatagramThroughput64 | Default                |     798.0 ns |     158.5 ns |      8.69 ns |         - |
| DatagramThroughput1K | Default                |  21,091.8 ns |  26,222.4 ns |  1,437.34 ns |         - |
| StreamThroughput64K  | Default                | 275,103.8 ns | 383,459.9 ns | 21,018.73 ns |         - |
| DatagramPingPong64   | InProcessEmitToolchain |  97,956.7 ns |  28,486.0 ns |  1,561.41 ns |         - |
| DatagramThroughput64 | InProcessEmitToolchain |     866.9 ns |     291.7 ns |     15.99 ns |         - |
| DatagramThroughput1K | InProcessEmitToolchain |  21,163.6 ns |   6,006.4 ns |    329.23 ns |         - |
| StreamThroughput64K  | InProcessEmitToolchain | 346,418.8 ns |  71,710.8 ns |  3,930.71 ns |       2 B |

| Headline | Out-of-process | In-process |
|---|---:|---:|
| Datagram round trip, 64 B | 89 µs | 98 µs |
| One-way datagrams, 64 B | 1.25 M msg/s (80 MB/s of payload) | 1.15 M msg/s |
| One-way datagrams, 1 KiB | 47 k msg/s (49 MB/s) | 47 k msg/s |
| Stream, 64 KiB gathered sends | 238 MB/s | 189 MB/s |

Delivery during the runs: no datagram was lost or canceled in any phase (9.2 M datagrams of 64 B and 508 k of 1 KiB
delivered in the throughput phases, every send reported a final state), and no ping timed out. The 2 B/op in the
in-process stream row is BenchmarkDotNet's process-wide counter picking up runtime background work (about 128 B per
invocation of 64 sends); the dedicated allocation tests of §3 measure 0 B.

`DatagramBurst32`, first as part of the run above (N = 3), then re-run alone with
`--iterationCount 15 --warmupCount 5` because the first run's error bars overlapped:

| Method          | Toolchain              | DelaySend | Mean (N = 3) | StdDev    | Mean (N = 15) | Error     | StdDev    | Allocated |
|---------------- |----------------------- |---------- |-------------:|----------:|--------------:|----------:|----------:|----------:|
| DatagramBurst32 | Default                | False     |     2.910 us | 0.0742 us |      2.461 us | 0.0470 us | 0.0440 us |         - |
| DatagramBurst32 | InProcessEmitToolchain | False     |     2.668 us | 0.0020 us |      2.560 us | 0.0652 us | 0.0610 us |         - |
| DatagramBurst32 | Default                | True      |     2.494 us | 0.0381 us |      2.920 us | 0.2084 us | 0.1949 us |         - |
| DatagramBurst32 | InProcessEmitToolchain | True      |     2.673 us | 0.1357 us |      3.007 us | 0.2312 us | 0.2162 us |         - |

### Reading

* **Round trip ≈ 90 µs.** Each direction crosses an MsQuic worker wake-up and the loopback UDP path; the transport's own
  work per message is an array lookup, a flag-table read and a few interlocked operations, far below the noise here
  (compare the simulator's 1.5 µs per datagram round of pure harness cost, `simulation.md`).
* **Small datagrams are packet-bound, not message-bound.** 64-byte datagrams go 26× faster per message than 1 KiB ones
  because MsQuic packs about 18 DATAGRAM frames of 64 B into one 1 200-byte packet: 1.25 M messages/s is about 70 k
  packets/s, of the same order as the 47 k packets/s of 1 KiB datagrams (one per packet). The packet rate is set by
  congestion control and pacing with 5 ms acknowledgement clocking on loopback.
* **Streams move 4 to 5 times more bytes than 1 KiB datagrams** (238 MB/s ≈ 200 k packets/s): stream data leaves the
  send queue in large batches and costs one completion per 64 KiB send, while every datagram costs two send-state
  callbacks (SENT and the final state).
* **DelaySend: hypothesis rejected for receiver-paced bursts.** With N = 15 the bursts take 16 to 19 % *longer* per
  datagram with `DELAY_SEND` on the first 31 datagrams (2.92 vs 2.46 µs out of process, 3.01 vs 2.56 µs in process;
  the N = 3 run pointed the other way inside overlapping error bars). Without the flag MsQuic already starts packetising
  while the application is still queuing the burst (queuing 32 datagrams takes a few µs), so the burst's last datagram
  leaves earlier; with the flag everything waits for the 32nd call. For a burst that must arrive before the next tick, the
  latency of the last datagram dominates, not the number of worker wake-ups.

### Decisions

* Keep the `TransportSendFlags` → MsQuic flag mapping as specified (`DelaySend` → `DELAY_SEND`), but the session layer
  should not set `DelaySend` on latency-bound tick bursts; packing messages into containers (PROTOCOL §2.2) already
  removes most small-datagram bursts. Revisit on a real network (pacing, GSO) and with bursts larger than one packet.
* Keep the ARCHITECTURE §7 defaults (LOW_LATENCY, pacing, CUBIC); nothing here argues for changing them, and the
  throughput figures are loopback ceilings for capacity planning rather than tuning targets.
* No optimisation round (nothing moves to `benchmarks/Tedd.Quicly.Archive`): the transport adds no allocation and its CPU
  cost is not separable from MsQuic's in these measurements.
