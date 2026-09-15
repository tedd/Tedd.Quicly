# Session: what a message costs end to end

Method per [ADR 0007](../adr/0007-measurement-method.md): measure the straightforward implementation (V0), state a
hypothesis about a hot path, implement it (V1), measure again, keep the winner and archive the loser in
`benchmarks/Tedd.Quicly.Archive/Session` so the comparison stays runnable. The session layer is specified in
[docs/design/session-layer.md](../design/session-layer.md) §4 and §7.

Benchmarks: `benchmarks/Tedd.Quicly.Benchmarks/Session/*.cs`, every class with `[Config(typeof(InProcessShortRunConfig))]`.
Run with

```
dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*Session*'
```

(the filter also runs `Control/SessionTokenBench`, reported in the control notes). On .NET 10, `Program.CreateConfig` adds an
out-of-process ShortRun job next to the in-process one, so every case has two rows: *Default* (its own process) and
*InProcessEmitToolchain*. ShortRun has N = 3, so the *Error* column is wide: read *Mean* and *StdDev*, and treat differences
under ~20 % as noise. Other agents were building and testing on the machine during the runs.

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.9445/25H2/2025Update/HudsonValley2)
AMD Ryzen 9 5950X 3.40GHz, 1 CPU, 32 logical and 16 physical cores
.NET SDK 11.0.100-preview.7.26381.103
  [Host]   : .NET 10.0.12 (10.0.12, 10.0.1226.42308), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.12 (10.0.12, 10.0.1226.42308), X64 RyuJIT x86-64-v3
Job=ShortRun  IterationCount=3  LaunchCount=1  WarmupCount=3
SessionPassBench: InvocationCount=1  IterationCount=60  LaunchCount=1  WarmupCount=10
```

Measured 2026-09-16 (branch `quicly/c1-session`).

## Workloads

Every case runs connected client/server `QuiclyPeer` pairs over zero-delay `SimulatedTransport` links (`SessionFixture`:
pings, heartbeat and the fast lock off, so a measured cycle is only the traffic under test). Channels: 2 unordered,
3 sequenced keyed, 4 ordered. One thread does all of it: both peers, and the simulator, which raises the transport callbacks
synchronously. The simulator's own cost is in [simulation.md](simulation.md) (1.5 µs per round of two 600-byte datagrams over
a 1 ms link, about 12 µs per 64 KiB stream send).

| Benchmark | Workload | Reported per |
|---|---|---|
| `SessionEndToEndBench.Unreliable64Packed` | 100 × `SendCopy` of 64 B on the unordered channel, then one cycle: client `Flush` (the scheduler packs containers), `Advance(0)`, server `Poll` (dispatch to a counting handler), `Advance(0)`, client `Poll` (completions) | message |
| `SessionEndToEndBench.Unreliable64Loose` | 100 × (one `SendCopy` of 64 B, one cycle): every message is a datagram of its own | message |
| `SessionEndToEndBench.Sequenced64Keyed` | 100 keyed sequenced messages of 64 B (keys 0..99), one cycle (per-key acceptance on receive) | message |
| `SessionEndToEndBench.Ordered64` | 100 × 64 B on the persistent ordered stream, one cycle: four stream sends (carrier gathers), progressive receive into pooled leases, dispatch, the carriers' completions | message |
| `SessionEndToEndBench.Ordered4K` | 16 × 4 KiB on the ordered stream, one cycle (64 KiB of stream data, about 55 packets) | message |
| `SessionPassBench.Flush100Buffered` | one `Flush` of a peer holding 100 buffered 64-byte messages: the scheduler pass alone (packing into containers, or gathering into stream sends); four pairs, the queues filled in the iteration setup, delivery in the iteration cleanup | flush |
| `SessionPassBench.SendCopy` | the admission of one 64-byte message (channel lookup, entry, lease, copy, header, queue); 4 × 100 per invocation, delivery in the iteration cleanup | message |
| `StreamReceiveLoopBench` | the ADR 0007 loop below | message |

`SessionPassBench` times one invocation per iteration (its setup and cleanup must stay outside the measurement), so
BenchmarkDotNet's own warm-up calls each method only a few times. Its global setup therefore runs the paths 600 times per
case, with pauses, until tiered compilation has promoted them: without that, a first run reported 563 ns per `SendCopy` out
of process against 131 ns in process, which was tier-0 code.

## Results (net10.0)

### End to end

| Method             | Toolchain              | Mean       | Error     | StdDev   | Messages/s (derived) | Allocated |
|------------------- |----------------------- |-----------:|----------:|---------:|---------------------:|----------:|
| Unreliable64Packed | Default                |   256.1 ns |  45.29 ns |  2.48 ns |               3.90 M |         - |
| Unreliable64Loose  | Default                |   632.1 ns |  64.61 ns |  3.54 ns |               1.58 M |         - |
| Sequenced64Keyed   | Default                |   273.5 ns |  31.49 ns |  1.73 ns |               3.66 M |         - |
| Ordered64          | Default                |   257.8 ns |  69.40 ns |  3.80 ns |               3.88 M |         - |
| Ordered4K          | Default                | 1,361.7 ns | 479.23 ns | 26.27 ns |  734 k (3.0 GB/s)    |         - |
| Unreliable64Packed | InProcessEmitToolchain |   251.8 ns | 201.54 ns | 11.05 ns |               3.97 M |         - |
| Unreliable64Loose  | InProcessEmitToolchain |   631.3 ns | 280.59 ns | 15.38 ns |               1.58 M |         - |
| Sequenced64Keyed   | InProcessEmitToolchain |   288.4 ns | 182.85 ns | 10.02 ns |               3.47 M |         - |
| Ordered64          | InProcessEmitToolchain |   262.5 ns |  18.71 ns |  1.03 ns |               3.81 M |         - |
| Ordered4K          | InProcessEmitToolchain | 1,427.5 ns | 112.43 ns |  6.16 ns |  701 k (2.9 GB/s)    |         - |

Messages/s = 10⁹ / Mean, for one core doing both ends and the simulator.

### The two halves of the send path

| Method           | Toolchain              | Ordered | Mean         | Error      | StdDev       | Allocated |
|----------------- |----------------------- |-------- |-------------:|-----------:|-------------:|----------:|
| Flush100Buffered | Default                | False   |  6,362.02 ns |  69.375 ns |   143.272 ns |         - |
| SendCopy         | Default                | False   |     81.82 ns |   1.810 ns |     3.778 ns |         - |
| Flush100Buffered | InProcessEmitToolchain | False   | 10,099.53 ns | 699.384 ns | 1,459.876 ns |    1668 B |
| SendCopy         | InProcessEmitToolchain | False   |    115.73 ns |   5.452 ns |    11.501 ns |      18 B |
| Flush100Buffered | Default                | True    |  3,124.06 ns |  82.543 ns |   172.298 ns |         - |
| SendCopy         | Default                | True    |     75.27 ns |   1.285 ns |     2.567 ns |         - |
| Flush100Buffered | InProcessEmitToolchain | True    |  5,513.36 ns | 309.577 ns |   679.529 ns |    1752 B |
| SendCopy         | InProcessEmitToolchain | True    |    116.60 ns |   5.577 ns |    12.006 ns |      15 B |

### Reading

* **No allocation per message, end to end.** Every end-to-end row, in both jobs, allocates nothing: admission, the scheduler
  pass, the transport, the receive path, dispatch and the completions, for loose datagrams, containers, keyed sequenced
  messages and the ordered stream.
* **Packing.** A 64-byte unreliable message costs 256 ns when 100 share a flush (packed into containers) and 632 ns when each
  goes out alone; the ~375 ns difference is the per-datagram work of the peer (a submission and its completion) and of the
  simulator (a delivery and an acknowledgement per datagram).
* **Sequenced keyed** messages cost 20–35 ns more than unordered ones: the per-key acceptance on receive (key table and
  sequence comparison).
* **Ordered 64 B** (258 ns) costs the same as packed datagrams: the carrier gathers the messages' own header and payload
  segments into four stream sends (no container copy), and the receiver copies each payload into a pooled lease sized from the
  frame's length.
* **Ordered 4 KiB**: 1.36–1.43 µs per message, about 730 k messages or 3 GB of payload per second on one core. The simulator's
  stream path accounts for roughly 0.8 µs of that (12–13 µs per 64 KiB); the rest is the two copies of 4 KiB (`SendCopy` into a
  send lease, the receive copy into a pooled lease), framing, dispatch and completions.
* **The send side.** Out of process, admitting a 64-byte message costs 75–82 ns; the scheduler pass costs 31 ns per message on
  the ordered channel (3.1 µs for 100 messages gathered into four stream sends) and 64 ns per message on the unordered channel
  (6.4 µs: 100 messages copied into containers and one datagram submission per container). For an ordered 64-byte message that
  is about 106 of the 258 ns end to end; the other ~150 ns are the transport, the receive path, dispatch and completions.
* **The in-process rows of `SessionPassBench`** are slower and report 15–1 752 B. With one invocation per iteration the
  in-process toolchain's own work lands in the measurement; the out-of-process rows of the same code report no allocation, and
  the unit tests below assert zero allocation for these operations. Read the *Default* rows.

## ADR 0007 loop: the ordered receive loop

**Why this path.** The split above leaves about 150 of the 258 ns of an ordered 64-byte message after the hand-off to the
transport, more than admission (75 ns) or the flush (31 ns); the simulator's stream path takes about 13 ns of it. Every byte
and every event of those messages passes the peer's engine-stream loop (`QuiclyPeer.ReceiveEngineStream`) on the transport
thread: it feeds the stream's `StreamFrameParser`, hands each message event (`Start`, `Chunk`, `End`) to the engine and, when
the engine answers `Pend` (receive ring full or receive budget exhausted), must un-read that event so that `Poll` can resume
the stream later.

**V0** (`benchmarks/Tedd.Quicly.Archive/Session/StreamReceiveLoopV0.cs`): before every event the loop copies the whole parser
(about 190 bytes: the bulk header, the 32-byte partial-header buffer and the configuration) so that a `Pend` can restore it. A
64-byte message is three events, so that is about 570 bytes copied per message, more than the message itself.

**Hypothesis.** A message event changes only the message header, the bulk-decoded count, the remaining length, the state, the
partial-header length and the control type: 40 bytes. Saving just those before each event (`StreamFrameParser.Mark`, restored
with `Rewind`) and copying the whole parser only on bulk streams (whose header decode touches more state) should take most of
the copy's cost out of the loop.

**Measurement.** `StreamReceiveLoopBench`: 1 000 frames of 64 bytes on an unkeyed, uncompressed ordered channel, received in
1 200-byte segments (three events per message); the engine call is replaced by a consumer that sums what it sees, the same in
both versions, and the setup checks that both loops produce the same sum. Per message. The .NET 11 row comes from
`dotnet run -c Release -f net11.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*StreamReceiveLoopBench*'`
(in process only: BenchmarkDotNet 0.15.8 has no .NET 11 moniker).

**Result.**

| Method                | Runtime, toolchain                  | Mean     | Error     | StdDev   | Ratio | Allocated |
|---------------------- |------------------------------------ |---------:|----------:|---------:|------:|----------:|
| V0_CopyParserPerEvent | .NET 10.0.12, Default               | 41.57 ns | 12.201 ns | 0.669 ns |  1.00 |         - |
| V1_MarkPerEvent       | .NET 10.0.12, Default               | 20.84 ns | 16.685 ns | 0.915 ns |  0.50 |         - |
| V0_CopyParserPerEvent | .NET 10.0.12, InProcessEmitToolchain | 35.27 ns |  5.604 ns | 0.307 ns |  1.00 |         - |
| V1_MarkPerEvent       | .NET 10.0.12, InProcessEmitToolchain | 20.18 ns |  5.227 ns | 0.286 ns |  0.57 |         - |
| V0_CopyParserPerEvent | .NET 11.0 preview 7, InProcessEmitToolchain | 35.15 ns | 9.083 ns | 0.498 ns | 1.00 |      - |
| V1_MarkPerEvent       | .NET 11.0 preview 7, InProcessEmitToolchain | 19.79 ns | 8.464 ns | 0.464 ns | 0.56 |      - |

An earlier run of the same pair on .NET 10 gave 33.30 → 20.19 ns out of process and 35.44 → 19.65 ns in process.

The **first V1 was refuted**: a `readonly struct Mark` returned by value from `GetMark()` through its constructor measured
41.64 ns against V0's 32.95 ns out of process (ratio 1.26) and 42.64 ns against 36.30 ns in process (1.17). Saving less state
did not pay once taking the mark became a call plus a copy of the returned struct. The kept V1 writes the mark in place
(`GetMark(out Mark mark)`, a mutable struct with internal fields), with `GetMark` and `Rewind` aggressively inlined.

**Decision.** Keep V1: `StreamFrameParser.Mark`, `GetMark(out Mark)`, `Rewind(in Mark)`; the peer's loop takes a mark per
event and a whole copy only on bulk streams. The loop costs half as much per message on both runtimes. V0 stays runnable in
the archive as `StreamReceiveLoopBench`'s baseline. The `Pend` path is covered by `Framing/StreamFrameParserMarkTests`
(rewinding every message event replays the same messages in any segmentation, a header straddling segments is restored) and
by the ordered engine's ring-full and receive-budget tests.

End to end the saving (15–20 ns of the ~260 ns an ordered 64-byte message costs) is below what ShortRun resolves on a shared
machine: `Ordered64` measured 331 / 398 ns (out of process / in process) with V0, 280 / 293 ns with the refuted V1 and
258 / 263 ns with the kept one, but `Unreliable64Packed`, which never enters the stream loop, moved from 283 / 334 ns to
256 / 252 ns between the same runs.

## Allocation

Besides the *Allocated* columns, the steady-state tests assert zero bytes allocated with
`GC.GetAllocatedBytesForCurrentThread` over five windows of work after a warm-up (`WindowedAllocation`: at most one window
may show a one-off runtime event such as a tier-up). The simulator raises the transport callbacks on the test thread, so the
receive paths are measured too.

* `OrderedZeroAllocationTests.Steady_Ordered_Traffic_Of_64_Byte_Messages_Does_Not_Allocate`: 10 ms link with 2 ms jitter and
  2 % stream packet loss; per 60 Hz tick 16 ordered 64-byte messages, a keyed ordered one, an LZ4-compressed one, a tracked one
  and one in the other direction; 3 000 ticks of warm-up, windows of 120 ticks. The warm-up is that long because the simulator
  itself allocates on the test thread until its buffer pool has reached the link's in-flight high-water mark: a per-call probe
  of the ticks after the old 1 200-tick warm-up found only 64- and 128-byte pinned buffers rented inside `SendStream`
  (ticks 1 382 and 1 428, then 5 422), never an allocation in the session layer.
* `OrderedZeroAllocationTests.The_Synchronous_Paths_Of_SendAsync_And_FlushAsync_Do_Not_Allocate`: 8 × `SendAsync` and one
  `FlushAsync` per cycle, all completing synchronously; windows of 200 cycles.
* `OrderedZeroAllocationTests.Admitting_Sends_Queued_By_Another_Thread_Does_Not_Allocate_On_The_Game_Thread`: a producer thread
  queues 10 sends per cycle through `ThreadSafeSend`; the game thread's `Poll`/`Flush` admits them; windows of 200 cycles.
* The datagram and idle paths: `DatagramZeroAllocationTests` and `SessionZeroAllocationTests` (steps 1 and 2).
