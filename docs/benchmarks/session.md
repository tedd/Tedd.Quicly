# Session: what a message costs end to end

Method per [ADR 0007](../adr/0007-measurement-method.md): measure the straightforward implementation (V0), state a
hypothesis about a hot path, implement it (V1), measure again, keep the winner and archive the loser in
`benchmarks/Tedd.Quicly.Archive/Session` so the comparison stays runnable. The session layer is specified in
[docs/design/session-layer.md](../design/session-layer.md) §4 and §7.

Benchmarks: `benchmarks/Tedd.Quicly.Benchmarks/Session/*.cs`, every class with `[Config(typeof(InProcessShortRunConfig))]` except
`BulkBench`, which runs 30 iterations (`InProcessMeasuredConfig`; see its section).
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
| `GroupStreamBench.Group64` | 100 × `SendCopy` of 64 B on a `ReliableUnordered` channel, then passes until the **server has dispatched all 100** — the group's stream has opened, been confirmed and carried every message with FIN, but its carrier completions and its shutdown are still outstanding when the measurement stops, so each iteration also pays the previous one's teardown | message |
| `GroupStreamBench.Group4K` | 16 × 4 KiB as one group, same cycle (64 KiB on one stream), likewise ending at the server's dispatch | message |
| `SessionPassBench.Flush100Buffered` | one `Flush` of a peer holding 100 buffered 64-byte messages: the scheduler pass alone (packing into containers, or gathering into stream sends); four pairs, the queues filled in the iteration setup, delivery in the iteration cleanup | flush |
| `SessionPassBench.SendCopy` | the admission of one 64-byte message (channel lookup, entry, lease, copy, header, queue); 4 × 100 per invocation, delivery in the iteration cleanup | message |
| `LatestBench.Latest1000Keys` | 1 000 keys of a `ReliableLatest` channel updated once per 60 Hz tick: `SendCopy` per key, client `Flush`, one tick of virtual time, server `Poll` (dispatch) + `Flush` (the coalesced `LatestAck` batch), client `Poll` (the acks and the completions) | value |
| `LatestBench.Latest64Keys` | the same cycle with 64 keys, so the per-value cost can be separated from the per-pass cost | value |
| `FragmentBench.Fragment3` | 20 × `SendCopy` of 2 400 B on an unreliable **fragmenting** channel (three fragments each), then passes until the server has dispatched all 20: admission of one owner entry plus its fragments, one datagram per fragment, reassembly into one pooled lease, dispatch, and the fragments' completions | message |
| `FragmentBench.Fragment8` | the same cycle with 8 375 B, which is eight fragments — the most PROTOCOL.md §2.1 allows | message |
| `RequestResponseBench.RequestRoundTrip` | 16 `SendRequestAsync` calls in flight at once on a request/response ordered channel, answered by the peer's handler with `Respond`, then consumed: the request's id and table slot, both carriers, the peer's dispatch, the matching that completes the value task | request |
| `RequestResponseBench.RequestSingle` | one request at a time through the same cycle, so the per-pass cost is not shared | request |
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

### Group streams (ReliableUnordered, wave C2b)

Measured 2026-09-16 and **re-measured the same day after the wave C2b review fixes** (the release path, the group list, the
carrier completion and the receive chunk branch all changed) with
`dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*Group*' '*Ordered64*'`
on the machine above (again not idle: another agent was building and testing throughout). The table below is the re-run.
`Ordered64` was included in that invocation as an **in-run control**: it came out at 262.9 ns (Default) and 275.2 ns (in
process), within noise of the 262.0 / 263.1 ns in the end-to-end table above, so the machine state is comparable to the run that
produced that table. Every batch is **one group on one
stream**, so a row includes a stream's whole lifetime — `OpenStream`, the preamble and the `Start` flag, the confirmation, the
gathered carriers, FIN, the shutdown and the credit coming back — not just the messages. That lifetime is *not* contained in one
iteration, though: `Deliver` stops as soon as the server has dispatched the batch, which is before that group's carrier
completions, its FIN's shutdown and the returning credit have been processed, so those land inside the next iteration's window.
Over a run of many iterations each row therefore still pays exactly one stream's cost per batch — the previous batch's teardown
instead of its own — which is what makes the steady-state figures comparable; only a single iteration in isolation would be
mis-attributed. The peers run with
`PeerOptions.GroupMinInterval = 0` because the benchmark's virtual clock does not advance between cycles; the interval bounds
how often a channel opens a stream in wall-clock time (PROTOCOL.md §3.2) and would otherwise seal one group for the whole run.

| Method  | Toolchain              | Mean       | Error      | StdDev    | Messages/s (derived) | Allocated |
|-------- |----------------------- |-----------:|-----------:|----------:|---------------------:|----------:|
| Group64 | Default                |   284.8 ns |   88.04 ns |   4.83 ns |  3.51 M (225 MB/s)   |         - |
| Group4K | Default                | 1,423.4 ns |  394.37 ns |  21.62 ns |   703 k (2.9 GB/s)   |         - |
| Group64 | InProcessEmitToolchain |   308.6 ns |   90.54 ns |   4.96 ns |  3.24 M (207 MB/s)   |         - |
| Group4K | InProcessEmitToolchain | 1,466.4 ns |  286.31 ns |  15.69 ns |   682 k (2.8 GB/s)   |         - |

The wave C2b run of the same four rows was 365.0 / 1,560.4 / 341.0 / 1,807.8 ns, so every row came out 12–25 % faster. **Do not
read that as a speed-up from the review fixes:** on the measured path they are neutral at best and slightly more work at worst
(one extra branch per received chunk, one channel lookup per carrier completion), and the O(1) group release only pays off with
many live groups, of which this benchmark has one. The earlier run's own error bars say what happened — `Group4K` was ±1.6 µs
then against ±0.39 µs now — so the first numbers were inflated by whatever else the machine was doing, and the control row above
is the evidence that today's state is the cleaner one. What matters here is that nothing regressed.

**Reading.** A 64-byte message on a group stream costs 285–309 ns against the same run's 263–275 ns on the persistent ordered
stream: about 22–33 ns per message more, which is the per-group stream lifetime spread over the batch's 100 messages (a group of
100 costs a couple of µs of stream setup and teardown — the teardown being the previous batch's, as the workload note above
explains) plus the extra pass every group needs before its second carrier, since nothing else goes out until its start is
confirmed. The wave C2b run put that premium at about 100 ns per message; with both modes measured in one invocation it is far
smaller, and the difference is in the group rows, not the ordered control. That is the price of the mode's promise: no message
waits for another's retransmission (PROTOCOL.md §3.2). At 4 KiB the difference disappears into the error bars (1.42–1.47 µs
against the ordered stream's 1.34–1.41 µs from the end-to-end table above, which was not re-run; `Group4K`'s ShortRun error is
±0.29–0.39 µs, so read it as indicative only) because the stream's fixed cost is amortised over 64 KiB of payload. Nothing allocates in either row, which is the same result the unit tests assert over
windows of ticks. As everywhere in this file, both peers and the simulator share one core, so these are single-core figures
with no cross-thread coherence cost.

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

`LatestBench` (added with the `ReliableLatest` engine, docs/design/session-layer.md §7.6) measures the mode's own cycle on
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
straddle each other and the toolchains swapped places, with error bars of ±86 … ±633 ns. A **third** run, after the wave C2a
review fixes (the shared payload committed through `EnginePayload.Commit`, the ack measured against the highest version
transmitted, the epoch reset consumed on every receive path, the counted stream slots), reported 495.4 / 442.1 ns out of
process and 558.5 / 491.0 ns in process — inside the band of the first two, with the toolchains swapping places for the third
time. **The honest reading is therefore 0.46 … 0.61 µs per value, about 1.6 … 2.2 M values per second on one core, with no
allocation** — not a figure to quote to three digits, and unchanged by the fixes: the table above is kept as measured rather
than replaced, because no run separates the three. What they all agree on is the shape of the cost, below.

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
  Hosts running channels with more keys or a higher rate than this still size the limit themselves (session-layer.md §7.6).
* The single-core caveat of the tables above applies unchanged: both peers and the simulator run on one thread, so the
  transport-thread → game-thread hand-offs (mailboxes, the ack ring, the completion ring) cost no cross-core coherence here.
  Another agent was building and testing on the machine during the run.

## Bulk: what a megabyte costs (wave C2c)

`BulkBench` (added with the `Bulk` engine, docs/design/session-layer.md §7.7) transfers a 4 MiB object end to end over a
zero-delay simulated link, with and without chunked LZ4 compression, on the machine above. Run with

```
dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*BulkBench*'
dotnet run -c Release -f net11.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*BulkBench*'
```

`OperationsPerInvoke` is the object's size in MiB, so **`Mean` is the time per MiB** and throughput is
`1.048576e9 / Mean(ns)` MB/s. One operation is a whole transfer's share of a megabyte: the stream's open at priority
band 0, the reads from the application's `IBulkSource` into pooled blocks, the §3.3 header and the body framing, the
transport, the progressive write into the application's `IBulkSink`, the receiver's `BulkProgress` (one per 64 KiB
accepted) and the completions — and, because the measurement runs until the transfer's record is released, the stream's
FIN and shutdown as well.

### Re-measured with 30 iterations (2026-09-18, after the wave C2c review fixes)

The first publication (2026-09-16) used the three-iteration ShortRun and headlined **1 081 MB/s** raw. That figure did not
reproduce (a re-run gave 969 MB/s), and it could not have been expected to: BenchmarkDotNet's *Error* is half the 99.9 %
confidence interval, a Student-t quantile over the iteration count, and with three iterations the quantile is about 31 —
the published row itself had an *Error* of 470 µs on a 970 µs mean. `BulkBench` now runs `InProcessMeasuredConfig`
(30 iterations after 5 warm-ups, quantile about 3.7): an in-process job, plus an out-of-process one on .NET 10, where
BenchmarkDotNet can spawn the child. `Program.CreateConfig` still adds its ShortRun job to every class; its rows are kept
below as the demonstration of the problem (an *Error* up to 3.2 ms on a 1.4 ms mean) and are not used for any figure.

Three runs, each on a quiet machine at its start (processor load 2–9 %); other agents build and test here, and the load
had risen to 73 % by the end of run C. A fourth run, started at 79 % load, came out bimodal and is **discarded**.

| Run | Runtime | Job            | Method         | Mean (ms/MiB) | Error (ms) | StdDev (ms) | MB/s (mean) | MB/s (mean ± error) | Allocated |
|-----|---------|----------------|--------------- |--------------:|-----------:|------------:|------------:|--------------------:|----------:|
| A   | net10.0 | InProcess30    | BulkRaw        |         1.110 |      0.110 |       0.165 |         945 |          859 – 1 049 |      72 B |
| A   | net10.0 | InProcess30    | BulkCompressed |         1.501 |      0.053 |       0.079 |         699 |            675 – 724 |      79 B |
| A   | net10.0 | OutOfProcess30 | BulkRaw        |         1.127 |      0.051 |       0.076 |         930 |            890 – 974 |      72 B |
| A   | net10.0 | OutOfProcess30 | BulkCompressed |         1.297 |      0.093 |       0.139 |         808 |            754 – 871 |      72 B |
| B   | net11.0 | InProcess30    | BulkRaw        |         1.096 |      0.051 |       0.076 |         957 |          914 – 1 003 |      72 B |
| B   | net11.0 | InProcess30    | BulkCompressed |         1.359 |      0.092 |       0.137 |         772 |            723 – 828 |      80 B |
| C   | net10.0 | InProcess30    | BulkRaw        |         1.192 |      0.045 |       0.068 |         880 |            848 – 914 |      72 B |
| C   | net10.0 | InProcess30    | BulkCompressed |         1.460 |      0.052 |       0.076 |         718 |            694 – 745 |      76 B |
| C   | net10.0 | OutOfProcess30 | BulkRaw        |         1.168 |      0.040 |       0.060 |         898 |            868 – 930 |      72 B |
| C   | net10.0 | OutOfProcess30 | BulkCompressed |         1.461 |      0.032 |       0.048 |         718 |            702 – 734 |      72 B |

The ShortRun rows of the same runs (N = 3), for comparison only:

| Run | Runtime | Method         | Mean (ms/MiB) | Error (ms) | StdDev (ms) |
|-----|---------|--------------- |--------------:|-----------:|------------:|
| A   | net10.0 | BulkRaw        |         1.184 |      0.615 |       0.034 |
| A   | net10.0 | BulkCompressed |         1.407 |      3.229 |       0.177 |
| B   | net11.0 | BulkRaw        |         1.119 |      0.348 |       0.019 |
| B   | net11.0 | BulkCompressed |         1.447 |      0.143 |       0.008 |
| C   | net10.0 | BulkRaw        |         1.079 |      1.347 |       0.074 |
| C   | net10.0 | BulkCompressed |         1.159 |      0.911 |       0.050 |

### Reading

* **Roughly 0.85–1.05 GB/s of object bytes on one core, raw**, for both peers and the simulator together — every
  30-iteration raw interval above lies inside 848–1 049 MB/s, and the means are 880–957 MB/s. The single-core caveat of
  the tables above applies unchanged. A bulk transfer is two copies of every byte (the source into a pooled block, the
  block into the application's target) plus the framing and the stream, which is what puts it below the ordered stream's
  2.9–3.0 GB/s for 4 KiB messages: bulk pays a per-object stream lifetime and a progress frame per 64 KiB where the ordered
  channel amortises one persistent stream over everything. The 1 081 MB/s of the first publication sits at the top edge of
  this range, not in its middle.
* **Compression costs about 13–26 % of throughput here — 18–20 % in three of the five pairs**, measured as
  `1 − raw mean ÷ compressed mean` within one run and job (A in process 26 %, A out of process 13 %, B 19 %, C in process
  18 %, C out of process 20 %); compressed throughput is 0.70–0.81 GB/s. That is the expected direction on this link. The
  chunked body puts roughly a tenth of the bytes on the wire, but the wire is an in-memory simulator with no delay and no
  bandwidth limit, so LZ4 compressing every 64 KiB chunk on the send side and decompressing it on the receive side is pure
  added CPU. The trade only pays where the bytes saved are bytes that would have queued:
  `A_Chunked_Compressed_Body_Round_Trips_And_Shrinks_The_Wire` asserts the wire really shrinks (`StreamBytesSent` below
  half the object), and it is a capped or metered link that turns that into time saved. The spread between jobs is larger
  than the intervals, so it is run-to-run variation of the machine rather than a property of either toolchain.
* **72–80 B per MiB is per *object*, not per byte**: one `BulkTransfer` and its `TaskCompletionSource` per 4 MiB transfer
  (about 290–320 B), which amortises to the figure above. The streaming path itself allocates nothing, which
  `BulkZeroAllocationTests` asserts over five windows of passes in the middle of a transfer, for the raw and the chunked
  body alike. (The discarded busy run showed 89 B once, in process; no quiet run did.)
* **The review fixes changed no hot path of this benchmark.** On this zero-delay link the transport reports a congestion
  window with an RTT under a microsecond, which leaves the rate gate off exactly as before (session-layer.md §7.7: the
  16 KiB/s floor now applies when the transport reports *no* window, which the simulator never does); a progress frame is
  now bounded by the bytes handed to the transport, and the per-pass work is otherwise unchanged.
* **Caveat: the control-message limit is part of this workload, and both peers keep the 2 000/s default.** The receiver
  owes one `BulkProgress` per 64 KiB accepted (PROTOCOL.md §2.3), so the benchmark advances the virtual clock 4 ms per
  pass and holds 256 KiB outstanding, which keeps that traffic near 1 000/s. The first run of this benchmark was
  **discarded**: it gave each peer its own `VirtualClock`, copied from `GroupStreamBench` where it is harmless because
  group streams send no control traffic at all. A peer whose clock never advances can never refill its control-message
  bucket, so the session closed with `LimitExceeded` part way through and the remaining invocations measured a dead
  session — `BulkRaw`, which sends the most progress frames, failed outright while `BulkCompressed` still reported a
  number. Both peers now share the network's clock, and `Transfer` fails the run if a transfer ends in any state but
  `Completed` or leaves its record behind, so a run that stopped transferring cannot produce numbers.

## Fragmentation and request/response (wave C2d)

`FragmentBench` and `RequestResponseBench` (added with the two features of docs/design/session-layer.md §7.8) measure them
on the same machine as the tables above. As everywhere in this file both peers and the simulator share one thread, so these
are single-core figures with no cross-thread coherence cost, and other agents were building and testing on the machine
throughout.

The table is the final code of the wave (after the review fixes), measured 2026-09-18 on net10.0 with a longer job than
ShortRun — 15 iterations after 5 warm-ups, in 2 launches, so 30 measured iterations per row:
`dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*FragmentBench*'
'*RequestResponseBench*' --job short --iterationCount 15 --warmupCount 5 --launchCount 2`.

| Method           | Toolchain              | Mean       | Error     | StdDev    | Derived                         | Allocated |
|----------------- |----------------------- |-----------:|----------:|----------:|--------------------------------:|----------:|
| Fragment3        | Default                |   2.594 µs | 0.0507 µs | 0.0759 µs | 0.86 µs per fragment, 925 MB/s  |         - |
| Fragment8        | Default                |   7.876 µs | 0.1014 µs | 0.1517 µs | 0.98 µs per fragment, 1.06 GB/s |         - |
| Fragment3        | InProcessEmitToolchain |   2.944 µs | 0.0748 µs | 0.1096 µs | 0.98 µs per fragment, 815 MB/s  |         - |
| Fragment8        | InProcessEmitToolchain |   8.870 µs | 0.3736 µs | 0.5591 µs | 1.11 µs per fragment, 944 MB/s  |         - |
| RequestRoundTrip | Default                |   776.4 ns |  15.23 ns |  21.35 ns | 1.29 M requests/s               |         - |
| RequestSingle    | Default                | 2,083.6 ns | 106.55 ns | 159.48 ns |   480 k requests/s              |         - |
| RequestRoundTrip | InProcessEmitToolchain |   824.1 ns |  74.44 ns | 111.42 ns | 1.21 M requests/s               |         - |
| RequestSingle    | InProcessEmitToolchain | 2,250.1 ns | 170.90 ns | 250.50 ns |   444 k requests/s              |         - |

**How much a number here is worth.** The same rows were measured three times on this machine over two days, with ShortRun
(N = 3) the first two times: `Fragment3` out of process read 2.416 µs (first measurement), 2.245 µs (after the review
fixes) and 2.594 µs (this table); `RequestRoundTrip` read 811.6, 647.8 and 776.4 ns — on code whose request path did not
change in between. Ambient load moves a row by 15 … 30 % from one session to the next, so a comparison of two sessions says
nothing below that, and a change is only judged by an A/B run back to back (below). Within one session the StdDevs above
are 3 … 7 %.

### Preparing a fragmented message's payload once (ADR 0007)

**V0** (the wave's first version): `Admit` prepares the payload for its single-datagram attempt, finds that the message does
not fit, **releases** that payload and gives up its entry, and `AdmitFragmented` prepares the payload all over again. For
`SendCopy` that is two rents of a send lease and two copies of the whole message; for a compressed channel two LZ4 passes;
for `SendBorrowed` two `GCHandle` pin/unpin cycles. **V1** hands the prepared payload from `Admit` to `AdmitFragmented`.

**Hypothesis.** Nothing about the payload depends on the fragment layout — the fragments point into those bytes and only the
header differs — so V1 removes one payload copy and one lease rent per fragmented message: a fixed saving per message of
roughly one memcpy of 2 400 or 8 375 bytes (tens to a couple of hundred nanoseconds), not a saving per fragment.

**Measurement.** `FragmentBench`, both arms on the same code except V0's second preparation, which is reproduced exactly by
inserting at the top of `AdmitFragmented` (after `SendEntryTable entries = _core.Entries;`):

```csharp
EnginePayload.Release(_core, in payload);
payload = default;
if (!EnginePayload.TryPrepare(_core, ref request, channel, length, takeSinglePage: false, ref payload))
{
    return SendStatus.OutOfBuffers;
}
```

Run as **A-B-B-A** (V1, V0, V0, V1) back to back, so a linear drift in ambient load cancels out, with the job of the table
above on net10.0 (both toolchains) and, in-process only because BenchmarkDotNet 0.15 cannot launch a net11.0 child, on
net11.0 (`-f net11.0`, without `--job short`). 2026-09-18, other agents building and testing on the box throughout.

**Result** (means of the two runs of each arm):

| Method    | Runtime, toolchain     | V1 runs           | V0 runs           | V1 mean  | V0 mean  | V1 / V0 |
|---------- |----------------------- |------------------:|------------------:|---------:|---------:|--------:|
| Fragment3 | net10.0, Default       | 2.638, 2.594 µs   | 2.747, 2.608 µs   | 2.616 µs | 2.678 µs |    0.98 |
| Fragment8 | net10.0, Default       | 7.798, 7.876 µs   | 8.031, 8.280 µs   | 7.837 µs | 8.156 µs |    0.96 |
| Fragment3 | net10.0, InProcessEmit | 2.926, 2.944 µs   | 2.963, 3.063 µs   | 2.935 µs | 3.013 µs |    0.97 |
| Fragment8 | net10.0, InProcessEmit | 8.607, 8.870 µs   | 8.783, 8.625 µs   | 8.739 µs | 8.704 µs |    1.00 |
| Fragment3 | net11.0, InProcessEmit | 2.154, 2.316 µs   | 2.380, 2.059 µs   | 2.235 µs | 2.220 µs |    1.01 |
| Fragment8 | net11.0, InProcessEmit | 7.048, 6.937 µs   | 6.499, 6.374 µs   | 6.993 µs | 6.437 µs |    1.09 |

On net10.0 V1 is 0 … 4 % faster, which is the size and sign the hypothesis predicts but no larger than the StdDev of a
single run (3 … 6 %); on net11.0 the StdDevs are 15 % and the arms point the other way. **The change is below what this
machine resolves.** An earlier record of this A/B (two ShortRun sessions minutes apart, V1 measured first) showed V1 at
0.81 and 0.85 of V0 out of process; this A-B-B-A run with ten times the iterations does not reproduce it, so that record
was ambient drift between the two sessions and has been withdrawn.

**Decision.** Keep V1 — not for speed, which is not measurably different, but because it is the simpler contract: one owner
of the prepared payload, released on exactly one refusal path, one LZ4 pass and one pin for compressed and borrowed
fragmenting channels (which no benchmark here measures; `FragmentEdgeTests.Every_Send_Path_Can_Fragment` covers them
functionally). **No Archive copy:** ADR 0007 archives the implementation an *optimisation* supersedes so its win stays
checkable, and this is not recorded as one. The superseded behaviour is the six lines above, so the comparison stays
runnable from this page.

### Reading

* **A fragment costs about what a datagram of its size costs: 0.86 … 1.11 µs.** Three fragments take 2.6 … 2.9 µs and
  eight take 7.9 … 8.9 µs, so the per-fragment cost is flat in the count — there is no per-message penalty that grows
  with the number of fragments. The cost is the larger copy on each side: the sender's `SendCopy` into the owner's lease
  (one copy of the message) and the receiver's copy of each fragment into the partial message's buffer, which then *is*
  the delivered payload.
* **About 1 GB/s of fragmented payload on one core** (815 MB/s … 1.06 GB/s), with both peers and the simulator on that
  same core. The fragments of one message are never packed together — by construction two of them cannot share a
  datagram — so this is one `SendDatagram` and one completion per fragment.
* **Nothing allocates**, in either job, for either feature: the owner entry, the fragment entries, the reassembly
  records and the request table's value-task sources are all pooled. `FragmentRequestZeroAllocationTests` asserts the
  same thing over five windows of 60 Hz traffic and of request round trips.
* **A request round trip costs 0.78 … 0.82 µs with 16 in flight** — in the region of two `Ordered64` messages (262 ns each
  in the end-to-end table, measured in another session) plus the request id, the table slot, the pooled value-task source
  and the matching scan. It is *one* round trip, not two messages: the application learns that the peer applied the
  message, which is the only application-level acknowledgement v1 has (PROTOCOL.md §4.3).
* **One request at a time costs 2.1 … 2.3 µs**, about 1.3 … 1.4 µs more. That difference is the per-pass cost of the
  cycle (both peers' `Flush`/`Poll`, the simulator's delivery) which 16 requests share and one request pays alone — the
  same shape as the packing difference between `Unreliable64Packed` and `Unreliable64Loose`. A game that issues its
  requests together per tick gets the first number.

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
* `GroupZeroAllocationTests.Steady_Group_Traffic_Of_64_Byte_Messages_Does_Not_Allocate`: a clean 10 ms link, 60 Hz ticks of 16
  plain 64-byte messages plus a keyed, an LZ4-compressed and a tracked one and one the other way — so **three group streams are
  opened, FIN'd and shut down every tick** in each direction; 1 200 ticks of warm-up (long enough for the peak of concurrent
  streams, group records and simulator slots), windows of 120 ticks. Group records, stream notices and receive records are
  pooled, so a stream per group costs no allocation.
* `OrderedZeroAllocationTests.The_Synchronous_Paths_Of_SendAsync_And_FlushAsync_Do_Not_Allocate`: 8 × `SendAsync` and one
  `FlushAsync` per cycle, all completing synchronously; windows of 200 cycles.
* `OrderedZeroAllocationTests.Admitting_Sends_Queued_By_Another_Thread_Does_Not_Allocate_On_The_Game_Thread`: a producer thread
  queues 10 sends per cycle through `ThreadSafeSend`; the game thread's `Poll`/`Flush` admits them; windows of 200 cycles.
* The datagram and idle paths: `DatagramZeroAllocationTests` and `SessionZeroAllocationTests` (steps 1 and 2).

## Hot-path pass (2026-09-18): main before and after

A pass run by the OPTIMIZE handbook's method: profile and stage-time the session, assign one specialist per area (admission,
ordered-stream receive, datagram path, atomics and cross-thread hand-off, real-MsQuic attribution, then the server's `FlushAll`
and the cross-core receive path), keep a mechanism only on paired evidence, review the result adversarially (three reviewers,
ten failing tests), fix, and measure **main before (`691f20d`) against main after** on every session benchmark. The method is the
[ADR 0007 addendum](../adr/0007-measurement-method.md): both builds loaded into one pinned process, alternating 0.4 s windows, 12
pairs per launch, launches alternating which build is loaded first; *B/A* is the geometric mean of the per-launch ratios with its
95 % t interval (identical builds give 0.994 [0.962 .. 1.028] over six launches). `B/A < 1` means main after is faster. Same
machine as above; .NET 10.0.12 (10 launches) and .NET 11 preview 7 (6 launches); other sessions' builds and tests ran on the box
at times, which the pairing absorbs but which widens the per-launch spread.

### Result

| Benchmark | net10.0 B/A [95 % CI] | net11.0 B/A [95 % CI] |
|---|---|---|
| `SessionEndToEndBench.Unreliable64Packed` | **0.894** [0.862 .. 0.928] | **0.898** [0.867 .. 0.930] |
| `SessionEndToEndBench.Ordered64` | **0.871** [0.850 .. 0.894] | **0.855** [0.835 .. 0.876] |
| `SessionEndToEndBench.Sequenced64Keyed` | **0.924** [0.893 .. 0.956] | — |
| `SessionEndToEndBench.Unreliable64Loose` | **0.941** [0.922 .. 0.959] | — |
| `SessionEndToEndBench.Ordered4K` | 0.983 [0.956 .. 1.011] (no change) | — |
| `GroupStreamBench.Group64` | **0.848** [0.813 .. 0.884] | **0.869** [0.850 .. 0.889] |
| `RequestResponseBench.RequestRoundTrip` | **0.864** [0.849 .. 0.880] | — |
| `RequestResponseBench.RequestSingle` | **0.951** [0.940 .. 0.962] | — |
| `LatestBench.Latest1000Keys` | **0.974** [0.964 .. 0.984] | — |
| `FragmentBench.Fragment3` | 1.006 [0.988 .. 1.025] (no change) | — |
| `BulkBench.BulkRaw` | 0.997 [0.989 .. 1.005] (no change) | — |
| `BulkBench.BulkCompressed` | 0.998 [0.995 .. 1.002] (no change) | — |

Where it moved (stage timing, `stages.<workload>`, ns per 64-byte message, mean of the per-launch medians, net10.0):

| Workload | send | flush | deliver | spoll | cpoll | total | B/A [95 % CI] |
|---|---|---|---|---|---|---|---|
| Packed | 78.3 → 70.3 | 56.1 → 38.8 | 59.6 → 59.8 | 22.9 → 21.5 | 27.0 → 14.2 | 244 → 205 | 0.839 [0.819 .. 0.860] |
| Keyed | 79.8 → 71.2 | 57.4 → 39.9 | 81.1 → 81.2 | 26.1 → 21.7 | 27.4 → 14.8 | 272 → 229 | 0.843 [0.832 .. 0.854] |
| Ordered64 | 71.4 → 66.8 | 22.8 → 22.9 | 104.3 → 73.7 | 25.3 → 21.5 | 25.7 → 25.3 | 250 → 211 | 0.849 [0.829 .. 0.869] |

On net11.0 `stages.Packed` is 0.845 [0.836 .. 0.855] and `stages.Ordered64` 0.839 [0.821 .. 0.857]. Nothing allocates, before or
after (the zero-allocation tests pass on both frameworks). The plain `Sequenced64Keyed` benchmark (0.924) gains less than the
stage-timed Keyed cycle (0.843) for the same work; both are reported, the plain benchmark is the headline.

### What changed

| Mechanism | Commits | Phase | Its own evidence (paired, net10.0) |
|---|---|---|---|
| A container's plain datagram members are completed by the packer directly (`DatagramHints.DirectCompletion`) instead of each routed through channel, engine, virtual call and fragment check | 86ef092 | cpoll | cpoll 27.3 → 16.2 ns (−40 %) in every launch; `stages.Packed` 0.955 [0.939 .. 0.970] |
| A container entry is written in place (the fit is already checked; one 16-byte vector move for the header); Tier-1 `Append` 1 341 → 587 bytes | 911a9ce | flush | flush 56.7 → 45.7 ns; `stages.Packed` 0.952 [0.928 .. 0.977] |
| The packer returns its members' blocks in one chain (`SlabAllocator.ReturnMany`) and releases each member's send budget when it copies it | 3cd268e, 77f465c, 549cc2c | flush | flush 47.6 → 41.4 ns; `stages.Packed` 0.962 [0.947 .. 0.976] |
| Admission: the uncompressed `SendCopy` path inlined into the engines' `Admit` (one lease address lookup), `SendEntryTable.TryAllocate` inlined, the stream engines' notice ring checked inline | 9f6c058, 944ec1a, c0b8a80 | send | send −10 % Packed, −6 % Ordered64, −12 % Keyed; `Unreliable64Packed` 0.960 [0.947 .. 0.973], `Ordered64` 0.969 [0.965 .. 0.973] |
| A stream message whose payload lies wholly in the segment is **one** engine event (`StreamMessagePhase.Whole`) for the ordered and group engines instead of Start, Chunk, End | 3c57427 | deliver | deliver −31 %; `Ordered64` 0.883 [0.858 .. 0.909], `Group64` 0.894 [0.872 .. 0.916], `RequestRoundTrip` 0.874 [0.821 .. 0.931]; `Ordered4K` and `BulkRaw` unchanged |
| One work signal per transport callback instead of one per published message (`PeerCore.Begin/EndTransportCallback`, `NoteTransportWork`) | 2536ce3, c2e7c2c, 9a79b72 | two threads | invisible single-core (no signal configured); `PeerReceiveHandoffBench` *Pipelined* busy 0.712 [0.681 .. 0.744] same CCD, 0.692 [0.665 .. 0.720] across CCDs (docs/benchmarks/threading.md §6) |
| The `CompletionTable` free list on the pinned object heap (no Dekker guard on dispose) | b480f19, b8db68e, d0f821d | tracked sends | ~2 ns per released slot; a whole tracked send within noise (threading.md §6) |
| `QuiclyServer.FlushAll` skips peers whose Flush would do nothing | c4dbefe, aa73c6a, f9bf52f | server tick | real MsQuic loopback, 60 Hz, 1 000 idle peers: ~1.0–1.5 → ~0.3–0.4 µs per peer per tick (session-layer.md §4.7) |

`StreamReceiveLoopBench` (the ADR 0007 loop above) gained a V2 (the whole-message event), with V1 archived as
`benchmarks/Tedd.Quicly.Archive/Session/StreamReceiveLoopV1.cs`: 33.9 / 28.0 / 21.7 ns per message for V0 / V1 / V2 out of process
(ShortRun, so V1 and V2 are within noise of each other): the loop's parser share is small, and the end-to-end gain comes from the
engine-side per-event work.

A correctness fix came out of the pass as well: `QuiclyServer.ActivatePending` marks every slot it activates (e827203). A work
bit taken before the peer was queued was lost, so such a peer was polled only at its admission deadline and its client timed out
in connection storms. It is a defect of the unchanged code, not of this pass.

### Tried and not kept

* **Receive-lease recycling** (dispatch parks a lease for the transport thread to reissue): the largest two-thread gain
  (cross-CCD busy 0.53×), reverted after review because parked leases starve game-thread rents (sends refused `OutOfBuffers`, a
  compressed reliable message lost at decode). threading.md §6.
* **Reading a whole frame without the parser's state machine**: `Ordered64` deliver 0.898, but `Ordered4K` deliver +4–6 % with the
  cause not found; rejected.
* **Filling the stream event context once per callback**: no measurable effect once the whole-message event existed (0.9999).
* **Sizing a datagram header exactly only near the limit**: no effect (PGO had already inlined the sizing).
* **A game-thread block magazine in the allocator**: at most 2 % available, protocol cost too high (memory.md).
* **"Read the work flag before exchanging"**: rejected without measuring; a plain read can pass the ring store and lose a wake-up.

### What a real peer gets out of it

The figures above are single-core simulator figures (both peers on one thread). A profile over **real MsQuic loopback** (two
processes, per-thread cycle counts; the harness is not in the repo) put QUICLY at 20–30 % of the machine's CPU per 64-byte message
and MsQuic plus the kernel at 70–80 %: a 15 % cheaper QUICLY is a few percent of machine CPU per message, and a connection's
message rate is limited by the sender's MsQuic worker, not by QUICLY. Where it counts is the **game thread**: QUICLY is 82–95 % of
the sender game thread's networking time (186–295 ns per message), and at 60 Hz with 1 000 peers the game thread spent 9.4–9.6 ms
of the 16.7 ms tick on networking before the pass, about half of it QUICLY and about 1 ms of it `FlushAll` over idle peers.
Placement matters more than any of this: a game thread on the other CCD from the MsQuic workers costs ~50 % more QUICLY time than
one on the same CCD.

### Hand-over items of the C2 wave

* The `CompletionTable` dispose guard of 68035f8 costs ~2 ns per released slot and nothing measurable per tracked send; the
  pinned free list above replaced it.
* The rings' `TryDequeue` contract (`out` undefined on false, ADR 0008 invariant 5) is unchanged.
* Bulk on a quiet machine (load 10–14 %, `InProcessMeasuredConfig`, net10.0, main 691f20d): `BulkRaw` 950.6 µs/MiB out of process
  (±9.7 µs, ≈ 1 103 MB/s) and 988.9 in process (≈ 1 060 MB/s); `BulkCompressed` 1 135.2 / 1 153.0 µs/MiB (≈ 924 / 909 MB/s). That is
  10–30 % above the Bulk section's figures, which were taken at up to 73 % machine load: the difference is the machine, not the code.
  The pass left both unchanged (table above).
