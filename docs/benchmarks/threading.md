# Threading: rings and completion table

Method per [ADR 0007](../adr/0007-measurement-method.md): write the simple correct version (V0), benchmark it,
state a hypothesis, implement it (V1), measure, keep the winner, archive the loser in
`benchmarks/Tedd.Quicly.Archive/Threading` so the comparison stays runnable.

Benchmarks: `benchmarks/Tedd.Quicly.Benchmarks/Threading/*.cs`. Run with

```
dotnet run -c Release --project benchmarks/Tedd.Quicly.Benchmarks -f net11.0 -- --filter '*Threading*'
dotnet run -c Release --project benchmarks/Tedd.Quicly.Benchmarks -f net10.0 -- --filter '*Threading*'
```

The Threading benchmarks carry their own config (`ThreadingBenchmarkConfig`: `Job.ShortRun`, in-process emit
toolchain, `MemoryDiagnoser`). In-process because BenchmarkDotNet 0.15.8 cannot validate or spawn a `net11.0`
child process (`GetRuntimeVersion not implemented for NotRecognized`); passing `--job short` on the command line
would add a second, out-of-process job and hit that error again. The runtime under test is whatever `-f` selects.

Hardware / software for every table below (2026-09-15):

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.9445/25H2/2025Update/HudsonValley2)
AMD Ryzen 9 5950X 3.40GHz, 1 CPU, 32 logical and 16 physical cores
.NET SDK 11.0.100-preview.7.26381.103
  [Host] : .NET 11.0.0 (11.0.0-preview.7.26381.103, 11.0.26.38203), X64 RyuJIT x86-64-v3   (section 1-3)
  [Host] : .NET 10.0.12, X64 RyuJIT x86-64-v3                                              (section 4)
Job=ShortRun-InProcess  Toolchain=InProcessEmitToolchain  IterationCount=3  LaunchCount=1  WarmupCount=3
```

ShortRun = 1 launch, 3 warm-up, 3 measured iterations: the *Error* column (99.9 % CI half-width from N = 3) is
wide, so read *Mean* and *StdDev* and treat differences under ~20 % as noise. Every decision below rests on a
gap of 1.6x or more.

All ring benchmarks move 2^20 `long`s through a 1024-slot ring (or an unbounded `ConcurrentQueue`) from producer
thread(s) to the benchmark thread; the reported time is **per item** (`OperationsPerInvoke`), and the 0-byte
allocation column is the per-item steady state (thread start-up is amortised over the 2^20 items and shows up as a
few bytes per item at most).

## 1. SpscRing (single producer, single consumer)

**V0** (`Archive.Threading.SpscRingV0<T>`): `head`/`tail` as two plain `long` fields next to each other, read
with `Volatile.Read` on every operation. Correct and 20 lines long.

**Hypothesis.** Two things dominate V0's cost: (a) `head` and `tail` share a cache line, so every enqueue
(writes `tail`) invalidates the line the consumer is spinning on and vice versa; (b) every enqueue reads `head`
and every dequeue reads `tail`, i.e. each operation touches the *other* thread's line even when the ring is far
from full/empty. Putting each index on its own cache-line pair (128-byte stride, so the adjacent-line prefetcher
does not pair them) removes (a); keeping a private snapshot of the other side's index and only refreshing it
when the snapshot says full/empty removes (b) in steady state (the producer then reads `head` about once per
`capacity` enqueues instead of once per enqueue). Expected: 2 to 4x fewer cache-line transfers per item, hence
2 to 4x throughput; identical semantics.

**V1** (`Core.Threading.SpscRing<T>`): `SpscIndices` explicit-layout struct with `Tail`+`CachedHead` at offset
128 and `Head`+`CachedTail` at offset 256, buffer index masked (capacity is a power of two).

**Measurement (net11.0).**

| Method              | Mean       | Error      | StdDev     | Ratio | RatioSD | Allocated | Alloc Ratio |
|-------------------- |-----------:|-----------:|-----------:|------:|--------:|----------:|------------:|
| SpscRing_V1         |   5.816 ns |  29.107 ns |  1.5954 ns |  1.06 |    0.40 |         - |          NA |
| SpscRing_V0_Archive |  19.495 ns |  42.468 ns |  2.3278 ns |  3.56 |    1.07 |         - |          NA |
| ConcurrentQueue     |   6.706 ns |   3.035 ns |  0.1664 ns |  1.23 |    0.35 |         - |          NA |
| BoundedChannel      | 163.250 ns | 840.024 ns | 46.0445 ns | 29.85 |   11.30 |         - |          NA |

**Decision.** Hypothesis confirmed: V1 is 3.4x faster per item than V0 (5.8 ns vs 19.5 ns) with no semantic
change; V0 is archived. `ConcurrentQueue<T>` is within noise of V1 for one producer (6.7 ns) but is unbounded,
allocates segments as it grows (the queue object is reused across iterations here, so segment churn does not
show up in the per-item column), and offers no back-pressure; the bounded ring is kept because the peer's
rings must be pre-allocated and bounded (ADR 0005). The bounded `Channel<T>` costs 160 ns per item through its
synchronous API because `TryWrite`/`TryRead` take a lock and manage waiter lists; it is not a candidate for
the hot path.

## 2. MpscRing (multi producer, single consumer)

**V0** (`Archive.Threading.MpscRingV0<T>`): the same ring with a monitor `lock` around enqueue and dequeue.
Trivially correct under any number of producers.

**Hypothesis.** A lock serialises producers *and* the consumer through one cache line and one critical section;
with several producers the lock convoy dominates. Vyukov's bounded MPMC queue gives every slot its own sequence
number: a producer claims a slot with one compare-exchange on the enqueue position, then publishes by writing the
slot's sequence, so producers only contend on the enqueue position (one CAS per item) and the consumer never
touches it at all (it reads the slot sequence, which is on the slot's own line). With the enqueue and dequeue
positions on separate cache-line pairs, the consumer's line is written only by the consumer. Expected: with 1
producer roughly the SPSC numbers plus one CAS (~10 to 20 ns); with 4 producers a large win over the lock,
whose cost grows with contention. `ConcurrentQueue<T>` (segmented, unbounded, allocates segments) and a bounded
`Channel<T>` (lock + `Deque` internally, synchronous `TryWrite`/`TryRead`) are the framework references.

**V1** (`Core.Threading.MpscRing<T>`): Vyukov bounded queue, `MpscPositions` padded struct, single-consumer
dequeue uses a plain write instead of a CAS on the dequeue position, `TryDequeueBatch` amortises the position
write over a batch.

**Measurement (net11.0).**

| Method              | Producers | Mean      | Error      | StdDev    | Ratio | RatioSD | Allocated | Alloc Ratio |
|-------------------- |---------- |----------:|-----------:|----------:|------:|--------:|----------:|------------:|
| MpscRing_V1         | 1         |  16.43 ns |   6.851 ns |  0.376 ns |  1.00 |    0.03 |         - |          NA |
| MpscRing_V0_Archive | 1         | 115.43 ns | 358.335 ns | 19.642 ns |  7.03 |    1.04 |         - |          NA |
| ConcurrentQueue     | 1         |  13.73 ns |  68.677 ns |  3.764 ns |  0.84 |    0.20 |         - |          NA |
| BoundedChannel      | 1         | 180.28 ns | 174.874 ns |  9.585 ns | 10.97 |    0.55 |         - |          NA |
|                     |           |           |            |           |       |         |           |             |
| MpscRing_V1         | 4         | 126.09 ns |  57.242 ns |  3.138 ns |  1.00 |    0.03 |         - |          NA |
| MpscRing_V0_Archive | 4         | 470.18 ns | 530.104 ns | 29.057 ns |  3.73 |    0.22 |         - |          NA |
| ConcurrentQueue     | 4         | 160.06 ns | 190.549 ns | 10.445 ns |  1.27 |    0.08 |         - |          NA |
| BoundedChannel      | 4         | 593.05 ns | 375.491 ns | 20.582 ns |  4.71 |    0.17 |         - |          NA |

**Decision.** Hypothesis confirmed: V1 beats the locked V0 by 7x with one producer (16 ns vs 115 ns) and by
3.7x with four (126 ns vs 470 ns); V0 is archived. With one producer V1 costs about the SPSC ring plus one CAS
(16 ns vs 6 ns), as predicted. With four producers the per-item cost of every candidate rises because all four
producers hammer one enqueue position while a single consumer drains a 1024-slot ring (the ring is full most of
the time, so producers spin); V1 still leads `ConcurrentQueue<T>` by 1.27x and the channel by 4.7x. The
1-producer `ConcurrentQueue` number (13.7 ns, within noise of V1) does not change the decision for the reasons
given in section 1 (unbounded, segment allocation, no back-pressure).

## 3. CompletionTable (allocate / complete / await round trip)

**V0** (`Archive.Threading.CompletionTableV0`): every transition takes the slot's monitor lock; the free list is
a `Stack<int>` under its own lock. Same contract (two stages, per-stage `ManualResetValueTaskSourceCore`,
generation-checked tokens, auto-release when both stages are done and no wait is outstanding).

**Hypothesis.** The round trip is a handful of state transitions on a single 32-bit word; a monitor
enter/exit pair per transition (uncontended, ~20 ns each on this hardware) is most of the cost. Packing
`Allocated | Done0/1 | Pending0/1 | Armed0/1` into one `int` and driving every transition with one
compare-exchange (or `Interlocked.And`) removes the locks; the free list becomes an `MpscRing<int>` (completion
threads push, the owner pops), which also removes the second lock. Expected: 2x or better on both round trips,
zero allocations in both.

**V1** (`Core.Threading.CompletionTable`): as described; see the class remarks for the state machine.

Two round trips are measured: `CompleteThenAwait` (both stages complete before the wait, so `WaitAsync` returns a
completed `ValueTask` and the slot auto-releases on the second completion) and `AwaitThenComplete` (a continuation
is attached first, so completion runs the `IValueTaskSource` path and the continuation inline, and the slot is
released when the result is read). Per-round time includes allocate + 2 completes + wait + release.

**Measurement (net11.0).**

| Method                             | Mean     | Error     | StdDev    | Ratio | RatioSD | Allocated | Alloc Ratio |
|----------------------------------- |---------:|----------:|----------:|------:|--------:|----------:|------------:|
| Table_V1_CompleteThenAwait         | 30.08 ns |  70.69 ns |  3.875 ns |  1.01 |    0.16 |         - |          NA |
| Table_V0_Archive_CompleteThenAwait | 77.40 ns |  81.26 ns |  4.454 ns |  2.60 |    0.31 |         - |          NA |
| Table_V1_AwaitThenComplete         | 58.97 ns | 278.20 ns | 15.249 ns |  1.98 |    0.49 |         - |          NA |
| Table_V0_Archive_AwaitThenComplete | 96.76 ns | 235.26 ns | 12.896 ns |  3.25 |    0.51 |         - |          NA |

**Decision.** Hypothesis confirmed: the lock-free table is 2.6x faster on the complete-then-await round trip
(30 ns vs 77 ns, i.e. allocate + two completes + wait + release in 30 ns) and 1.6x faster on the
await-then-complete round trip (59 ns vs 97 ns, where the extra ~30 ns in both versions is the
`ManualResetValueTaskSourceCore` continuation registration, inline invocation and reset, which the two versions
share). Neither version allocates. V0 is archived. The remaining cost of V1 is dominated by the two interlocked
operations per stage plus the free-list ring round trip; a further step (batching releases, or a per-owner-thread
free list without interlocked operations) is not worth taking until a real peer-level benchmark shows the table on
the profile.

**V1.1 (review fix, same day).** Review found an ABA race in V1: `CompletionTable.Complete` checked the slot's
generation and then `Slot.Complete` read the 32-bit state, wrote `_status` and compare-exchanged the state as
three separate steps. If the owner released the token and re-allocated the same slot in between (`Allocate`
reset the state to exactly `Allocated`, so the stale CAS succeeded), the late completion landed on the new
occupant; a losing duplicate `Complete` could also overwrite `_status` although its transition was ignored.
Reproduced by `CompletionTableReviewTests` (275 corrupted fresh tokens in 3.2M iterations).

*Fix.* The whole slot state is now one 64-bit word, `generation << 32 | status << 8 | stage bits`; every
transition is a single compare-exchange whose expected value carries the token's generation, so a stale call
can never succeed, and the status travels in the same CAS as the stage bit, so an ignored completion never
touches it. Release changed from a CAS loop that any thread could attempt into an exclusive step: exactly one
transition per occupancy makes `Done0 | Done1` true with no `Pending` bit set, and the thread that installed
that word releases the slot with two plain writes (released status, then the free word with the next
generation). Generations stay odd, step 2. V1 is not archived: it has a correctness defect, so it is not a
candidate to keep runnable. `TryAllocate` also now waits out a release that another thread has claimed but
not yet published on the free-list ring instead of reporting exhaustion (`Available > 0` implies success).

*Hypothesis for the numbers.* One interlocked operation fewer on the release path (plain write instead of CAS)
and one fewer volatile read/write pair for the status; expected: complete-then-await noticeably faster,
await-then-complete unchanged within noise (its cost is dominated by the core's continuation machinery).

**Measurement (net11.0), two consecutive runs.**

| Method                             | Mean      | Error      | StdDev    | Ratio | RatioSD | Allocated | Alloc Ratio |
|----------------------------------- |----------:|-----------:|----------:|------:|--------:|----------:|------------:|
| Table_V1_CompleteThenAwait         |  15.05 ns |  34.18 ns  | 1.874 ns  |  1.01 |    0.15 |         - |          NA |
| Table_V0_Archive_CompleteThenAwait |  49.37 ns |  60.47 ns  | 3.315 ns  |  3.31 |    0.39 |         - |          NA |
| Table_V1_AwaitThenComplete         |  77.24 ns | 107.02 ns  | 5.866 ns  |  5.18 |    0.62 |         - |          NA |
| Table_V0_Archive_AwaitThenComplete | 100.48 ns |  51.43 ns  | 2.819 ns  |  6.74 |    0.70 |         - |          NA |
|                                    |           |            |           |       |         |           |             |
| Table_V1_CompleteThenAwait         |  16.70 ns |   7.354 ns |  0.403 ns |  1.00 |    0.03 |         - |          NA |
| Table_V0_Archive_CompleteThenAwait |  48.62 ns |  47.113 ns |  2.582 ns |  2.91 |    0.15 |         - |          NA |
| Table_V1_AwaitThenComplete         |  50.42 ns |  82.325 ns |  4.513 ns |  3.02 |    0.24 |         - |          NA |
| Table_V0_Archive_AwaitThenComplete | 125.88 ns | 213.646 ns | 11.711 ns |  7.54 |    0.63 |         - |          NA |

**Decision.** Complete-then-await halved (30 ns to 15 to 17 ns, consistent across both runs) as predicted;
await-then-complete is 50 to 77 ns against 59 ns before, i.e. within this config's run-to-run noise (the
archived V0 moved 77 to 49 ns and 97 to 126 ns between the same runs without any code change), so it is treated
as unchanged. Still zero allocations. The fixed version ships; no follow-up performance work was triggered.

## 4. net10.0 cross-check

Same machine, same day, `-f net10.0` (.NET 10.0.12). Every decision above holds on net10.0; the V1/V0 ratios are
3.1x (SPSC), 7.4x / 3.8x (MPSC, 1 / 4 producers) and 4.3x / 1.8x (table, after the V1.1 fix in section 3). The framework references move around
between runtimes (`ConcurrentQueue` is slower and `Channel` faster on net10.0 than on net11.0), our rings do not.

| Method              | Mean      | Error      | StdDev    | Ratio | RatioSD | Allocated | Alloc Ratio |
|-------------------- |----------:|-----------:|----------:|------:|--------:|----------:|------------:|
| SpscRing_V1         |  5.530 ns |   4.618 ns | 0.2531 ns |  1.00 |    0.06 |         - |          NA |
| SpscRing_V0_Archive | 16.994 ns |  26.068 ns | 1.4289 ns |  3.08 |    0.25 |         - |          NA |
| ConcurrentQueue     |  6.284 ns |   1.571 ns | 0.0861 ns |  1.14 |    0.05 |         - |          NA |
| BoundedChannel      | 98.248 ns | 104.271 ns | 5.7154 ns | 17.79 |    1.13 |         - |          NA |

| Method              | Producers | Mean      | Error      | StdDev    | Ratio | RatioSD | Allocated | Alloc Ratio |
|-------------------- |---------- |----------:|-----------:|----------:|------:|--------:|----------:|------------:|
| MpscRing_V1         | 1         |  13.96 ns |   0.910 ns |  0.050 ns |  1.00 |    0.00 |         - |          NA |
| MpscRing_V0_Archive | 1         | 103.54 ns | 373.988 ns | 20.500 ns |  7.42 |    1.27 |         - |          NA |
| ConcurrentQueue     | 1         |  23.74 ns |  23.547 ns |  1.291 ns |  1.70 |    0.08 |         - |          NA |
| BoundedChannel      | 1         |  86.48 ns |  68.265 ns |  3.742 ns |  6.19 |    0.23 |         - |          NA |
|                     |           |           |            |           |       |         |           |             |
| MpscRing_V1         | 4         | 129.47 ns |  40.792 ns |  2.236 ns |  1.00 |    0.02 |         - |          NA |
| MpscRing_V0_Archive | 4         | 493.32 ns | 226.520 ns | 12.416 ns |  3.81 |    0.10 |         - |          NA |
| ConcurrentQueue     | 4         | 136.17 ns | 249.904 ns | 13.698 ns |  1.05 |    0.09 |         - |          NA |
| BoundedChannel      | 4         | 497.00 ns | 467.328 ns | 25.616 ns |  3.84 |    0.18 |         - |          NA |

| Method                             | Mean      | Error      | StdDev    | Ratio | RatioSD | Allocated | Alloc Ratio |
|----------------------------------- |----------:|-----------:|----------:|------:|--------:|----------:|------------:|
| Table_V1_CompleteThenAwait         |  18.51 ns |   8.067 ns |  0.442 ns |  1.00 |    0.03 |         - |          NA |
| Table_V0_Archive_CompleteThenAwait |  80.26 ns | 133.833 ns |  7.336 ns |  4.34 |    0.35 |         - |          NA |
| Table_V1_AwaitThenComplete         |  63.95 ns |  20.903 ns |  1.146 ns |  3.46 |    0.09 |         - |          NA |
| Table_V0_Archive_AwaitThenComplete | 118.14 ns | 548.675 ns | 30.075 ns |  6.38 |    1.41 |         - |          NA |

## 5. Notes

* Allocation is asserted by unit tests too: `SpscRingTests.Steady_State_Does_Not_Allocate`,
  `MpscRingTests.Steady_State_Does_Not_Allocate`,
  `CompletionTableTests.Allocate_Complete_Await_Completed_Loop_Does_Not_Allocate` and
  `CompletionTableTests.Await_Registered_Before_Completion_Does_Not_Allocate` measure
  `GC.GetAllocatedBytesForCurrentThread()` around warmed-up loops and assert 0 bytes.
* One runtime behaviour worth knowing: if a continuation is attached to a `ValueTask` *after* its stage
  completed (the awaiter lost the race between `IsCompleted` and `OnCompleted`),
  `ManualResetValueTaskSourceCore` queues the continuation to the thread pool, which allocates a work item.
  That never happens when completions are delivered on the owner thread through `Poll()`, which is the design
  in ARCHITECTURE section 3.

## 6. Hot-path pass (2026-09-18): the completion table's dispose guard, and what crossing a core costs

Measured on the machine above (.NET 10.0.12, Release, TieredPGO) with the paired in-process method of the
[ADR 0007 addendum](../adr/0007-measurement-method.md): both builds in one pinned process, alternating 0.4 s windows, repeated over
launches; *B/A* is the geometric mean of the per-launch ratios with its 95 % interval.

**The dispose guard of `68035f8`.** That commit fixed a use-after-free (a wait consumed after `Dispose` returned its slot into the
freed native free list) with a Dekker handshake: two `lock`-prefixed increments/decrements and two volatile reads per slot return.
Tier-1 disassembly: `ReturnToFreeList` carried 3 `lock` instructions (2 `xadd`, 1 `cmpxchg`), the ring's own CAS included.
Removing only the guard (a diagnostic ablation, same source otherwise) moved `CompletionTableRoundTripBench` complete-then-await
from 16.5 to 14.5 ns (0.857 [0.788 .. 0.933], 4 launches) — about 2 ns per released slot — and a whole tracked send not
measurably (`TrackedSendBench.TrackedOrdered64` 0.986 [0.951 .. 1.022], `AwaitedOrdered64` 0.971 [0.954 .. 0.988], 6 launches).

**Kept: the free list on the pinned object heap.** `MpscRing<T>` gained a pinned-object-heap storage mode (one 64-byte aligned
reference-free block that `Dispose` leaves alone), and `CompletionTable` uses it: a late return reaches the ring through the slot
it releases, so the memory is alive for as long as such a return can happen and the return is a plain enqueue again. It is as
fast as having no guard (micro 0.992 [0.948 .. 1.037] against the ablation; `AwaitedOrdered64` 0.971 [0.931 .. 1.013], 10 launches)
and simpler to reason about; the cost is 16 B per slot on the pinned heap (64 KiB at 4 096 slots) released by the GC rather than
by `Dispose`, and a capacity bounded by the largest array (2^26 slots; ADR 0008 invariant 5). The tests fault (access violation)
against a native list freed by `Dispose` without the guard.

**Crossing a core.** `CrossCoreHandoffBench` passes received 64-byte messages from a producer thread to a consumer in lockstep
(each side's busy time timed separately; consumer on CPU 4, producer on CPU 6 = same CCD or CPU 20 = other CCD). Extra busy ns per
message over the single-core figure:

| Kernel | Batch | Single-core busy | + same CCD | + other CCD |
|---|---|---|---|---|
| Ring only (`Ring64`) | 10 | 8 | 10–18 | 34–55 |
| Ring only | 100 | 4 | 3–4 | 14–15 |
| Ring + receive lease path (`RingLease`) | 10 | 38 | 44–53 | 154–180 |
| Ring + receive lease path | 100 | 33 | 23–27 | 82–83 |

The ring itself is cheap at batch 100 (cached indices on separate lines, no false sharing); the lease path — the allocator's shard
line and the receive-budget word written by both threads per message — is what crosses. Lockstep hands the lines over once per
pass and understates contention: `Session/PeerReceiveHandoffBench` replays real packed containers into a peer's transport sink on
one thread while the game thread polls, and its *Pipelined* mode (both threads at once, as in a real peer) is the one to quote.
There, one work signal per transport callback instead of one per published message (session-layer.md §4.7) took the busy time per
message to 0.712 [0.681 .. 0.744] of 691f20d on one CCD and 0.692 [0.665 .. 0.720] across CCDs (wall 195 → 162 and 556 → 435 ns/msg;
3 ABBA rounds of separate processes), and the work signals from ~5 M to ~0.5 M per run.

**Rejected after review: receive-lease recycling.** Letting dispatch park a received lease for the transport thread to reissue
(no pool or budget atomics per message) cut the pipelined figures further (same CCD wall 158 → 106 ns/msg with the signal change,
cross-CCD busy 0.53×), but parked leases can only be reclaimed by the transport thread: two adversarial reviews showed a send from
the same private pool refused `OutOfBuffers` and a compressed reliable message lost at decode near a full budget, and on a server's
shared pool one peer's parked blocks cannot be reclaimed by another. It was reverted; a design that returns dispatched leases in one
chain per Poll (before any game-thread rent) is the open follow-up.

## 7. The blocking `Wait` under CPU load (2026-10-02)

**Finding.** `CompletionTable.Wait` (behind `QuiclyPeer.Wait`) spins ten `SpinWait` rounds, the ones that never yield
(a few µs), then parks on the slot's `ManualResetEventSlim`. That event had the default spin count, 35, so its own
`Wait` spun again before it blocked: ten more busy rounds, then 25 rounds that alternate busy spins with `Thread.Yield` and
`Thread.Sleep(0)`. On an idle machine a yield returns at once, and the whole phase ended between 10 and 20 µs into the
wait (measured below). On a machine with no free core every yield hands the core to a ready thread for the rest of its
quantum (about 31 ms on this Windows build), and a completion that arrives meanwhile cannot wake the waiter: `Set`
signals only a thread that has parked. The load-independence work on Core.Tests (branch `tests/load-independent`) found it:
`CompletionTableTests.Stress_Transport_Thread_Completes_While_Owner_Awaits`, whose mode 2 is this `Wait`, took 22–26 s
instead of 3–4 s on four cores shared with eight busy loops.

**Hypothesis.** Creating the event with spin count 0 removes the yielding phase and keeps the table's own spin. A wait
that is not complete when that spin ends then parks, and the completion wakes it (with the wake-up's priority boost)
instead of the waiter getting the core back when the busy thread's quantum ends. Expected: under load, wake-ups in
microseconds instead of up to a quantum. Idle: unchanged, except for completions that land inside the old event's spin
(after the table's spin, up to 10–20 µs into the wait), which now pay one kernel wake-up. `Complete` and every
`WaitAsync` path are untouched; they never use the event's spin count.

**Change.** `new ManualResetEventSlim(false, spinCount: 0)` in `Slot.Wait`. Any count up to 10 never yields (`SpinWait`
yields from round 10 on), and 10 measured the same as 0 in every case below, so the event does not spin at all.

**Measurement.** `CompletionTableWaitBench` (new): the owner allocates a slot, hands the token to a completer thread and
calls `Wait`; the completer completes the stage `DelayUs` after it saw the token; the owner releases the slot. Time per
operation is one round: delay + hand-off + wake-up. net11.0, `ThreadingBenchmarkConfig` (ShortRun, in-process; on net11.0
the run also gets the global ShortRun job, so each run reports two in-process jobs), process pinned to CPUs 8–11 with
`DOTNET_PROCESSOR_COUNT=4`, normal priority. *Idle*: nothing else pinned there (ambient desktop load 20–25 %); *loaded*:
eight `powershell -Command "while(1){}"` pinned to the same four CPUs. A = main (`5ab528b`), B = spin count 0, C = spin count
10; runs interleaved A, B, C, A, B. Mean per round, range over all jobs of all runs:

| DelayUs | A idle | B idle | C idle | A loaded | B loaded |
|--------:|-------:|-------:|-------:|---------:|---------:|
| 0 | 0.37–0.40 µs | 0.37–0.39 µs | 0.37 µs | 0.41–1.14 µs | 0.35–0.39 µs |
| 10 | 10.6–11.1 µs | 21.2–21.6 µs | 21.1–21.6 µs | 680–3 301 µs | 16.0–17.2 µs |
| 20 | 32.9–47.8 µs | 32.0–32.6 µs | | 659–3 720 µs | 26.5–27.5 µs |
| 40 | 52.0 µs | 52.6–53.6 µs | | 662–3 673 µs | 46.7–48.0 µs |
| 100 | 112.8–125.1 µs | 112.4–113.7 µs | 112.6–113.3 µs | 748–4 098 µs | 106.6–107.9 µs |

(Loaded: three runs of A and three of B, interleaved A, B, A, B, A, B. The other sessions' load varied: A's first two runs
read 2.3–4.1 ms per round, its third 0.66–0.76 ms; B did not move.)

Loaded, a round of A costs 0.7–4 ms as soon as the completion comes after the table's spin: in many rounds the waiter has
handed its core to a busy loop and gets it back only when that quantum ends. B pays the delay plus 6–8 µs, less than idle,
because cores that never go idle wake a parked thread faster than cores in a sleep state. Idle, the old event's yielding
spin catches a completion 10 µs into the wait (0.7 µs over the delay); without it the waiter has parked and pays one
kernel wake-up, about 11 µs. By 20 µs the old event has parked too, and both pay the same ~12 µs (one of the two jobs of
A at 20 µs read 47.8 µs: a yield that lost its core to the ambient load). The single-thread table benchmarks (sections 3
and 6) do not touch the event and stay within ShortRun noise, idle (A / B, two jobs each):
`Table_V1_CompleteThenAwait` 15.4–15.8 / 15.5–16.0 ns, `Table_V1_AwaitThenComplete` 53.3–54.7 / 52.7–54.9 ns,
`CompleteThenAwaitPeerSized` 16.8–17.0 / 16.7–17.1 ns, `CompletionTableRoundTripBench.AwaitThenComplete` 56.1–56.3 /
52.8–67.2 ns (one job of B high, the other low; nothing on that path changed), zero allocations in both. Loaded, the
same rows read 18.1–18.3 / 18.4–18.5 ns, 76.4 / 77.6–78.2 ns, 19.3–19.5 / 19.6 ns and 77.4–77.7 / 75.0–78.6 ns.

The stress test, wall time of a fresh test process (`--filter-method`, including ~0.4 s start-up), on the test version
that `tests/load-independent` ships (`7f43be8`, whose own hand-offs no longer yield), CPUs 8–11, `DOTNET_PROCESSOR_COUNT=4`,
runs of A and B interleaved:

| | A (main) | B (spin count 0) |
|---|---:|---:|
| idle, 5 runs | 1.42–1.52 s | 1.49–1.56 s |
| eight busy loops, 7 runs | 31.6–47.3 s (median 33.3) | 10.2–13.9 s (median 12.7) |

The machine also carried other sessions' builds and test runs on other cores during the loaded runs, which is why both
columns are slower than the 22–26 s and 3–4 s of the original report; the two slowest A runs (47.3 and 41.7 s) coincided
with a full Core.Tests run of this branch on CPUs 16–31, a few seconds of it by mistake unpinned. On main's own version of
that test, whose spin awaiters also yield, one loaded run each took 484 s (A) and 397 s (B): the test itself was then the
bigger problem.

`CompletionTableBlockingWaitTests` (new) pins the waiter and a busy loop of equal (highest) priority to one CPU and
compares each table wait with a reference wait on an event that parks at once, in the round next to it. Table minus
reference, median of 21 pairs: A 26.6–31.6 ms (the busy loop's quantum; it fails 20 of 20 runs idle and 10 of 10 with eight
busy loops on its four CPUs, net10.0 and net11.0), B 0.000–0.001 ms (passes all of them). Two simpler designs failed in
full Core.Tests runs: an absolute bound on the wake-up (a parked waiter at normal priority waited for other tests' boosted
threads for a quantum) and the same bound at the highest priority (garbage collections and other pauses of the test
process delayed 18 of 21 rounds by 1.5–70 ms). With the reference, the whole project passed three runs per framework on four
CPUs (on `7f43be8` with this change) and one per framework on CPUs 16–31 (on this branch), 2 376 of 2 376 each.

**Decision.** Kept. Under load a blocking wait no longer loses a quantum per yield; idle it costs about 11 µs only when the
completion lands after the table's spin and within the old event's (10–20 µs into the wait), which a send's stages
practically never do (`BufferReleased` follows the transport's send completion, `RemoteAccepted` a round trip). Spin count
10 bought nothing over 0.
