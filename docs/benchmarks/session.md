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

Measured 2026-09-16 (branch `quicly/c1-session`) and **re-measured on the same machine after the wave C1 review fixes** moved the
receive, completion and hand-off rings into native memory (`NativeArray<T>`), sized the completion ring to two entries per send
slot instead of four, built the drain queues eagerly in native memory, and gave the receive producer a cached-index room check.
Every table below is the re-run unless a row says otherwise.

**The measurement machine was not idle:** another agent was building and running test suites throughout, as during the original
run. That is the normal condition for these numbers and one more reason to treat differences under ~20 % as noise.

## Workloads

Every case runs connected client/server `QuiclyPeer` pairs over zero-delay `SimulatedTransport` links (`SessionFixture`:
pings, heartbeat and the fast lock off, so a measured cycle is only the traffic under test). Channels: 2 unordered,
3 sequenced keyed, 4 ordered. One thread does all of it: both peers, and the simulator, which raises the transport callbacks
synchronously. The simulator's own cost is in [simulation.md](simulation.md) (1.5 µs per round of two 600-byte datagrams over
a 1 ms link, about 12 µs per 64 KiB stream send).

**Caveat: every figure here is single-core.** Both peers and the simulator run on the one thread, so the transport-thread →
game-thread hand-offs (receive ring, completion ring, mailboxes) are produced and consumed by the same core with warm cache
lines and no cross-thread coherence traffic whatsoever. These benchmarks therefore *cannot* show what it costs when a producer
touches the consumer's cache line — the defect behind ADR 0008 invariant 5's cached-index rule, which the wave C1 performance
review found in `TryEnqueueReceive`/`TryReserveReceive` (they read `SpscRing.Count`, i.e. both indices, per received message) —
and they show no lock or contention cost either. Read them as "what one core does with the whole session", not as a model of a
real client or server where the transport worker and the game thread are different cores.

| Benchmark | Workload | Reported per |
|---|---|---|
| `SessionEndToEndBench.Unreliable64Packed` | 100 × `SendCopy` of 64 B on the unordered channel, then one cycle: client `Flush` (the scheduler packs containers), `Advance(0)`, server `Poll` (dispatch to a counting handler), `Advance(0)`, client `Poll` (completions) | message |
| `SessionEndToEndBench.Unreliable64Loose` | 100 × (one `SendCopy` of 64 B, one cycle): every message is a datagram of its own | message |
| `SessionEndToEndBench.Sequenced64Keyed` | 100 keyed sequenced messages of 64 B (keys 0..99), one cycle (per-key acceptance on receive) | message |
| `SessionEndToEndBench.Ordered64` | 100 × 64 B on the persistent ordered stream, one cycle: four stream sends (carrier gathers), progressive receive into pooled leases, dispatch, the carriers' completions | message |
| `SessionEndToEndBench.Ordered4K` | 16 × 4 KiB on the ordered stream, one cycle (64 KiB of stream data, about 55 packets) | message |
| `SessionPassBench.Flush100Buffered` | one `Flush` of a peer holding 100 buffered 64-byte messages: the scheduler pass alone (packing into containers, or gathering into stream sends); four pairs, the queues filled in the iteration setup, delivery in the iteration cleanup | flush |
| `SessionPassBench.SendCopy` | the admission of one 64-byte message (channel lookup, entry, lease, copy, header, queue); 4 × 100 per invocation, delivery in the iteration cleanup | message |
| `LatestBench.Latest1000Keys` | 1 000 keys of a `ReliableLatest` channel updated once per 60 Hz tick: `SendCopy` per key, client `Flush`, one tick of virtual time, server `Poll` (dispatch) + `Flush` (the coalesced `LatestAck` batch), client `Poll` (the acks and the completions) | value |
| `LatestBench.Latest64Keys` | the same cycle with 64 keys, so the per-value cost can be separated from the per-pass cost | value |
| `StreamReceiveLoopBench` | the ADR 0007 loop below | message |

`SessionPassBench` times one invocation per iteration (its setup and cleanup must stay outside the measurement), so
BenchmarkDotNet's own warm-up calls each method only a few times. Its global setup therefore runs the paths 600 times per
case, with pauses, until tiered compilation has promoted them: without that, a first run reported 563 ns per `SendCopy` out
of process against 131 ns in process, which was tier-0 code.

## Results (net10.0)

### End to end

| Method             | Toolchain              | Mean       | Error     | StdDev   | Messages/s (derived) | Allocated |
|------------------- |----------------------- |-----------:|----------:|---------:|---------------------:|----------:|
| Unreliable64Packed | Default                |   239.8 ns |  19.34 ns |  1.06 ns |               4.17 M |         - |
| Unreliable64Loose  | Default                |   624.2 ns |  29.13 ns |  1.60 ns |               1.60 M |         - |
| Sequenced64Keyed   | Default                |   278.3 ns | 127.85 ns |  7.01 ns |               3.59 M |         - |
| Ordered64          | Default                |   262.0 ns | 152.05 ns |  8.33 ns |               3.82 M |         - |
| Ordered4K          | Default                | 1,343.2 ns |  50.40 ns |  2.76 ns |  744 k (3.0 GB/s)    |         - |
| Unreliable64Packed | InProcessEmitToolchain |   248.3 ns |  38.96 ns |  2.14 ns |               4.03 M |         - |
| Unreliable64Loose  | InProcessEmitToolchain |   664.7 ns |  99.25 ns |  5.44 ns |               1.50 M |         - |
| Sequenced64Keyed   | InProcessEmitToolchain |   288.5 ns |  41.43 ns |  2.27 ns |               3.47 M |         - |
| Ordered64          | InProcessEmitToolchain |   263.1 ns |  17.97 ns |  0.98 ns |               3.80 M |         - |
| Ordered4K          | InProcessEmitToolchain | 1,409.8 ns | 710.42 ns | 38.94 ns |  709 k (2.9 GB/s)    |         - |

**Did the native-memory rings move throughput?** No: every row is within ShortRun noise of the numbers measured before the
change (256.1 / 632.1 / 273.5 / 257.8 / 1 361.7 ns out of process). Four rows moved down and four up, the largest being
`Unreliable64Packed` at −6.4 % (256.1 → 239.8 ns) and `Unreliable64Loose` in process at +5.3 % (631.3 → 664.7 ns). That is the
expected result: the rings were already flat arrays of the same structs, so moving them from the GC heap to
`NativeMemory.AlignedAlloc` changes where they live, not how they are read — the gain is that a 256 KiB per-peer ring no longer
lands on the large-object heap and is no longer scanned. Nothing here measures the cached-index change either, for the reason in
the caveat above.

Messages/s = 10⁹ / Mean, for one core doing both ends and the simulator.

### The two halves of the send path

| Method           | Toolchain              | Ordered | Mean        | Error      | StdDev     | Median      | Allocated |
|----------------- |----------------------- |-------- |------------:|-----------:|-----------:|------------:|----------:|
| Flush100Buffered | Default                | False   | 6,375.46 ns |  61.885 ns | 130.537 ns | 6,387.50 ns |         - |
| SendCopy         | Default                | False   |    77.49 ns |   0.398 ns |   0.830 ns |    77.38 ns |         - |
| Flush100Buffered | InProcessEmitToolchain | False   | 9,060.85 ns | 295.623 ns | 617.075 ns | 9,150.00 ns |    1752 B |
| SendCopy         | InProcessEmitToolchain | False   |   110.59 ns |   5.042 ns |  10.412 ns |   107.00 ns |      18 B |
| Flush100Buffered | Default                | True    | 3,664.36 ns | 348.657 ns | 680.028 ns | 3,300.00 ns |         - |
| SendCopy         | Default                | True    |    76.25 ns |   0.413 ns |   0.834 ns |    76.00 ns |         - |
| Flush100Buffered | InProcessEmitToolchain | True    | 5,301.34 ns |  99.695 ns | 214.604 ns | 5,250.00 ns |    1332 B |
| SendCopy         | InProcessEmitToolchain | True    |   116.07 ns |   3.826 ns |   8.398 ns |   114.00 ns |      17 B |

`SendCopy` is the steadiest case in the suite (StdDev under 1 ns out of process) and it moved from 81.82 to 77.49 ns unordered
and 75.27 to 76.25 ns ordered — flat to slightly faster after the change. `Flush100Buffered` on the ordered channel reports a
higher mean than before (3 124 → 3 664 ns) with a *median* of 3 300 ns and a StdDev of 680 ns: this case times one invocation per
iteration, so its distribution is skewed by the occasional long pass, and the median is the number to compare. It is not a
regression in the pass itself.

### Reading

* **No allocation per message, end to end.** Every end-to-end row, in both jobs, allocates nothing: admission, the scheduler
  pass, the transport, the receive path, dispatch and the completions, for loose datagrams, containers, keyed sequenced
  messages and the ordered stream.
* **Packing.** A 64-byte unreliable message costs 240 ns when 100 share a flush (packed into containers) and 624 ns when each
  goes out alone; the ~384 ns difference is the per-datagram work of the peer (a submission and its completion) and of the
  simulator (a delivery and an acknowledgement per datagram).
* **Sequenced keyed** messages cost about 40 ns more than unordered ones (278 vs 240 ns out of process, 289 vs 248 in process):
  the per-key acceptance on receive (key table and sequence comparison).
* **Ordered 64 B** (262 ns) costs within ~10 % of a packed datagram (240 ns): the carrier gathers the messages' own header and
  payload segments into four stream sends (no container copy), and the receiver copies each payload into a pooled lease sized
  from the frame's length.
* **Ordered 4 KiB**: 1.34–1.41 µs per message, about 710–745 k messages or 2.9–3.0 GB of payload per second on one core. The
  simulator's stream path accounts for roughly 0.8 µs of that (12–13 µs per 64 KiB); the rest is the two copies of 4 KiB
  (`SendCopy` into a send lease, the receive copy into a pooled lease), framing, dispatch and completions.
* **The send side.** Out of process, admitting a 64-byte message costs 76–77 ns; the scheduler pass costs 33–37 ns per message
  on the ordered channel (3.3 µs median for 100 messages gathered into four stream sends) and 64 ns per message on the unordered
  channel (6.4 µs: 100 messages copied into containers and one datagram submission per container). For an ordered 64-byte
  message that is about 110 of the 262 ns end to end; the other ~150 ns are the transport, the receive path, dispatch and
  completions.
* **The in-process rows of `SessionPassBench`** are slower and report 17–1 752 B. With one invocation per iteration the
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
| V0_CopyParserPerEvent | .NET 10.0.12, Default               | 33.12 ns |  2.627 ns | 0.144 ns |  1.00 |         - |
| V1_MarkPerEvent       | .NET 10.0.12, Default               | 24.08 ns | 55.667 ns | 3.051 ns |  0.73 |         - |
| V0_CopyParserPerEvent | .NET 10.0.12, InProcessEmitToolchain | 35.96 ns |  1.904 ns | 0.104 ns |  1.00 |         - |
| V1_MarkPerEvent       | .NET 10.0.12, InProcessEmitToolchain | 19.17 ns |  6.910 ns | 0.379 ns |  0.53 |         - |
| V0_CopyParserPerEvent | .NET 11.0 preview 7, InProcessEmitToolchain | 35.15 ns | 9.083 ns | 0.498 ns | 1.00 |      - |
| V1_MarkPerEvent       | .NET 11.0 preview 7, InProcessEmitToolchain | 19.79 ns | 8.464 ns | 0.464 ns | 0.56 |      - |

The two .NET 10 rows are the re-run; the **.NET 11 rows are the earlier measurement and were not re-measured after the
native-ring change** (the re-run used `-f net10.0`, and BenchmarkDotNet 0.15.8 has no .NET 11 moniker, so those rows need their
own `-f net11.0` invocation). The loop itself does not touch the rings, so they are expected to stand.

Earlier runs of the same pair on .NET 10 gave 41.57 → 20.84 ns and 33.30 → 20.19 ns out of process and 35.27 / 35.44 → 20.18 /
19.65 ns in process; the wave C1 review's independent re-run measured 32.65 → 20.24 ns out of process (ratio 0.62) and 21.41 ns
in process. Pooling every run, V0 sits at 32.6–41.6 ns and V1 at 19.2–24.1 ns, so **the honest headline is ~33–36 ns down to
~19–24 ns — about a third less per message (38 % at the midpoints), not a halving.** The doc originally claimed "41.6 → 20.8, a
halving", which took the single highest V0 sample against a low V1 one: no single ShortRun ratio is stable enough to quote alone
(0.73 and 0.53 in the re-run above, 0.62 and 0.57 in the review's, 0.50 and 0.57 originally), while the direction of the result
has never been in doubt.

The **first V1 was refuted**: a `readonly struct Mark` returned by value from `GetMark()` through its constructor measured
41.64 ns against V0's 32.95 ns out of process (ratio 1.26) and 42.64 ns against 36.30 ns in process (1.17). Saving less state
did not pay once taking the mark became a call plus a copy of the returned struct. The kept V1 writes the mark in place
(`GetMark(out Mark mark)`, a mutable struct with internal fields), with `GetMark` and `Rewind` aggressively inlined.

**Decision.** Keep V1: `StreamFrameParser.Mark`, `GetMark(out Mark)`, `Rewind(in Mark)`; the peer's loop takes a mark per
event and a whole copy only on bulk streams. The loop costs about a third less per message on both runtimes (~33–36 ns down to ~19–24 ns across runs and jobs; see the range
below rather than any single ratio). V0 stays runnable in
the archive as `StreamReceiveLoopBench`'s baseline. The `Pend` path is covered by `Framing/StreamFrameParserMarkTests`
(rewinding every message event replays the same messages in any segmentation, a header straddling segments is restored) and
by the ordered engine's ring-full and receive-budget tests.

End to end the saving (15–20 ns of the ~260 ns an ordered 64-byte message costs) is below what ShortRun resolves on a shared
machine: `Ordered64` measured 331 / 398 ns (out of process / in process) with V0, 280 / 293 ns with the refuted V1 and
258 / 263 ns with the kept one, but `Unreliable64Packed`, which never enters the stream loop, moved from 283 / 334 ns to
256 / 252 ns between the same runs.

## ReliableLatest: what a keyed value costs (wave C2a)

`LatestBench` (added with the `ReliableLatest` engine, docs/design/session-layer.md §7.5) measures the mode's own cycle on
the same machine and with the same `InProcessShortRunConfig` as the tables above, on net10.0 (measured 2026-09-16, re-run
after the session hooks were merged). One
operation is one *value*: the key's slot lookup, the version from the channel's counter, the value entry with its payload
lease, one transmission handed to the packer (packed with the other keys' values into containers), the transport, the
receiving side's per-key acceptance and mailbox post, dispatch to the handler, the peer's coalesced `LatestAck` batch and the
completion it produces. Channel 2 is a `ReliableLatest` channel with a dense key space; both peers leave
`ControlMessagesPerSecond` at its 2 000/s default, which covers this workload's ack traffic (see the caveat below).

| Method         | Toolchain              | Mean     | Error     | StdDev   | Values/s (derived) | Allocated |
|--------------- |----------------------- |---------:|----------:|---------:|-------------------:|----------:|
| Latest1000Keys | Default                | 606.4 ns | 186.69 ns | 10.23 ns |             1.65 M |         - |
| Latest64Keys   | Default                | 560.9 ns | 295.61 ns | 16.20 ns |             1.78 M |         - |
| Latest1000Keys | InProcessEmitToolchain | 479.8 ns |  85.52 ns |  4.69 ns |             2.08 M |         - |
| Latest64Keys   | InProcessEmitToolchain | 463.8 ns | 176.42 ns |  9.67 ns |             2.16 M |         - |

An earlier valid run of the same benchmark — before the session hooks were merged and before the engine started limiting
its own large-value streams — reported 471.8 / 470.9 ns out of process and 610.4 / 564.4 ns in process: the two runs
straddle each other and the toolchains swapped places, with error bars of ±86 … ±633 ns. **The honest reading is therefore
0.46 … 0.61 µs per value, about 1.6 … 2.2 M values per second on one core, with no allocation** — not a figure to quote to
three digits. What both runs agree on is the shape of the cost, below.

### Reading

* **No allocation per value, end to end**, in both jobs and both runs: admission with the per-key slot, the transmission, the
  mailbox receive, the ack batch and the completions. `LatestZeroAllocationTests` asserts the same thing over five windows of
  60 ticks with 1 000 keys (and a second workload that supersedes and retires 32 keys every tick).
* **The per-value cost dominates**: 1 000 keys cost within 8 % of 64 keys per value in every row of both runs, so the
  per-pass work (the scheduler pass, the container submissions, the ack batches) is already amortised at 64 keys.
* **About 2 × a packed unreliable datagram** (0.46–0.61 µs vs 239.8 ns for `Unreliable64Packed`). The extra work per value is the
  key slot and version bookkeeping, the second send entry (the value keeps its payload for retransmission while the
  transmission carries it), the receiver's per-key version check and mailbox post instead of a ring entry, and the ack: one
  entry written into the peer's batch, one control datagram per ~170 keys, and the completion it decides.
* **At 60 Hz most values complete `Superseded`, not `Delivered`** — and that is the mode working as specified (PROTOCOL.md
  §4.3). The ack of a value written in tick *t* arrives during tick *t+1*, by which time the application has already written
  the next value for that key; every value still reached the peer (the receive counter matches) and no retransmission fires
  (the 20 ms `MinRetry` backstop is longer than the 16.7 ms tick). The benchmark asserts both in its setup and cleanup, so a
  cycle that stopped delivering could not produce numbers.
* **Caveat: the control-message limit is part of this workload.** One `LatestAck` datagram carries about 170 keys, so 1 000
  keys at 60 Hz make the peer receive ~360 control messages per second. Against the **200/s** default this benchmark
  originally ran with, that closed the session: the first run was **discarded** because the peers closed with
  `LimitExceeded` part-way through and the remaining cycles measured a closed session (413.6 / 388.9 ns, i.e. ~13 % too
  fast); the kept run raised the limit to 8 000/s on both ends. That measurement is exactly what made PROTOCOL.md §7 raise
  the default to **2 000/s with a sizing rule**, so the benchmark now runs at the default and raises nothing. Its `Check()`
  fails the run if either peer is not connected, if any value was retransmitted, or if fewer values reached the peer's
  handler than the cycles sent — so a run that started refusing admission or closing the session cannot produce numbers.
  Hosts running channels with more keys or a higher rate than this still size the limit themselves (session-layer.md §7.5).
* The single-core caveat of the tables above applies unchanged: both peers and the simulator run on one thread, so the
  transport-thread → game-thread hand-offs (mailboxes, the ack ring, the completion ring) cost no cross-core coherence here.
  Another agent was building and testing on the machine during the run.

## Allocation

Besides the *Allocated* columns, the steady-state tests assert zero bytes allocated with
`GC.GetAllocatedBytesForCurrentThread` over five windows of work after a warm-up (`WindowedAllocation`: at most one window
may show a one-off runtime event such as a tier-up). The simulator raises the transport callbacks on the test thread, so the
receive paths are measured too.

* `OrderedZeroAllocationTests.Steady_Ordered_Traffic_Of_64_Byte_Messages_Does_Not_Allocate`: a **clean** 10 ms link (delay only);
  per 60 Hz tick 16 ordered 64-byte messages, a keyed ordered one, an LZ4-compressed one, a tracked one and one in the other
  direction; 1 200 ticks of warm-up, windows of 120 ticks. The link is deliberately clean: on a lossy or jittery link the
  simulator's own buffer pool rents a pinned 64- or 128-byte array inside `SendStream` whenever more sends are in flight than
  ever before — with seed 1 at ticks 1 382, 1 428, 5 422 and 5 429 — so a measurement window can straddle one of those events and
  fail for a reason outside the session layer (the old 3 000-tick warm-up only happened to sit between two of them). A per-phase
  probe on a clean link found 0 bytes across 8 000 ticks in all three phases (sends + client Flush, `network.Advance`, server
  Poll + Flush + client Poll). Loss, jitter and retransmission are covered by
  `Steady_Ordered_Traffic_Under_Loss_And_Jitter_Delivers_Every_Message_In_Order`, which asserts order and byte-exactness for
  ~9 600 messages instead of allocation.
* `OrderedZeroAllocationTests.The_Synchronous_Paths_Of_SendAsync_And_FlushAsync_Do_Not_Allocate`: 8 × `SendAsync` and one
  `FlushAsync` per cycle, all completing synchronously; windows of 200 cycles.
* `OrderedZeroAllocationTests.Admitting_Sends_Queued_By_Another_Thread_Does_Not_Allocate_On_The_Game_Thread`: a producer thread
  queues 10 sends per cycle through `ThreadSafeSend`; the game thread's `Poll`/`Flush` admits them; windows of 200 cycles.
* The datagram and idle paths: `DatagramZeroAllocationTests` and `SessionZeroAllocationTests` (steps 1 and 2).
