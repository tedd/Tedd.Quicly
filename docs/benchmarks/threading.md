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

**Finding.** `CompletionTable.Wait` (behind `QuiclyPeer.Wait`) spun ten `SpinWait` rounds, the ones that never yield (a
few µs), then parked on the slot's `ManualResetEventSlim`. That event had the default spin count, 35, so its own `Wait`
spun again before it blocked: ten more busy rounds, then 25 rounds that alternate busy spins with `Thread.Yield` and
`Thread.Sleep(0)`. On an idle machine a yield returns at once, and the whole phase ended 15–20 µs into the wait. On a
machine with no free core every yield hands the core to a ready thread for the rest of its quantum (about 31 ms on this
Windows build), and a completion that arrives meanwhile cannot wake the waiter: `Set` signals only a thread that has
parked. The load-independence work on Core.Tests (branch `tests/load-independent`) found it:
`CompletionTableTests.Stress_Transport_Thread_Completes_While_Owner_Awaits`, whose mode 2 is this `Wait`, took 22–26 s
instead of 3–4 s on four cores shared with eight busy loops.

**First change, and what its review found.** `3d60a10` created the event with spin count 0, so the waiter parked as soon
as the table's spin ended. Under load that removed the lost quanta. Idle, though, every completion that landed about
3–14 µs into the wait now paid a kernel wake-up: +6–8 µs (+1.6 µs at 3 µs, nothing from 15 µs on). The first version of
this section claimed a send's stages "practically never" land there. That was wrong for datagrams. Over MsQuic loopback a
tracked datagram's `BufferReleased` completes 7–17 µs after `Flush` (p05 7.2, p20 8.9, p50 10.8–11.3, p75 13.7–17.4 µs;
65–72 % within 13 µs). With spin count 0, `Flush` + `QuiclyPeer.Wait(BufferReleased)` went from a median of 10.9–11.3 µs
to 17.6–20.3 µs, B above A in 30 of 30 blocks.

The perf review measured all of this with a paired in-process harness on CPUs 16–23. It seeded every slot's event by
reflection and alternated the variants per block; its A/A offset was ±0.15 µs; this was on Windows, Zen 3. The contract
review found an older defect next to it. Two blocking waits on the two stages of one token shared the slot's one event,
and the second-stage waiter's `Reset` could swallow the first stage's wake-up, which then came up to the whole timeout
late. That hit about a quarter of the rounds, the same on main (`ReviewCwaitcontractTwoWaiterTests`).

**Hypothesis.** The yields were the problem, not the spinning. Spinning on with `Thread.SpinWait` alone, until about
20 µs into the wait, keeps the old event's idle profile (it had parked by 15–20 µs) and still never hands the core away.
Under load the spin costs at most 20 µs of CPU for each wait that completes later, against a quantum per yield. The
review had measured exactly that from the caller's side (a pre-spin on `IsCompleted`, then the spin-0 event): within
0.3–0.5 µs of main over MsQuic loopback, and equal to spin count 0 under eight busy loops.

**Change.** After its ten `SpinWait` rounds, `Slot.Wait` spins with `Thread.SpinWait(8)`, checking the state after each
call, until 20 µs (Stopwatch) since the wait began. The spin is skipped for a zero timeout, and on a single processor,
where the completing thread cannot run meanwhile. Then it parks on an event with spin count 0. Each stage has its own
lazily created event, so a waiter only ever resets its own.

**Measurement, idle.** `CompletionTableWaitBench` (new): the owner allocates a slot, hands the token to a completer
thread and calls `Wait`; the completer completes the stage `DelayUs` after it saw the token; the owner releases the slot.
The time per operation is one round: delay + hand-off + wake-up. No quiet machine could be had, so both builds ran in
one PairHost process. That is the ADR 0007 addendum method; PairHost now takes `Class.Method:DelayUs=10`. The process was
pinned to CPUs 16–23, which other sessions' builds and test runs shared, with alternating 0.4 s windows, 12 pairs per
launch and 6 launches. A = main (`5ab528b`), B = this branch; A/A = two copies of main. Columns: per-launch medians per
round, and the combined B/A ratio with its 95 % interval.

| DelayUs | A | B | B/A | A/A |
|--------:|---:|---:|---:|---:|
| 0 | 355–411 ns | 347–374 ns | 0.95 [0.92 .. 0.97] | |
| 5 | 5.65–5.81 µs | 5.62–5.67 µs | 0.92 [0.87 .. 0.98] | 1.08 [0.94 .. 1.24] |
| 10 | 10.8–21.0 µs | 10.65–10.78 µs | 0.70 [0.44 .. 1.11] | 0.96 [0.89 .. 1.03] |
| 15 | 47–66 µs | 15.8–15.9 µs | 0.28 [0.25 .. 0.32] | |
| 20 | 29.1–80.9 µs | 23.2–24.6 µs | 0.65 [0.46 .. 0.93] | 1.03 [0.98 .. 1.09] |
| 40 | 54–113 µs | 50.5–52.0 µs | 0.56 [0.38 .. 0.83] | |
| 100 | 112–131 µs | 111–113 µs | 0.91 [0.85 .. 0.97] | |

B is never slower here, and its medians barely move between launches. Up to about 10 µs both builds catch the completion
while spinning. From there A's spin yields, and on these shared cores some of its windows lost the core, so its medians
wander between launches. At 15 µs, deep in the old event's yielding rounds, A took 47–66 µs in every launch. At 20 µs B
catches part of the rounds before its spin ends. From 40 µs on, B parks and pays one wake-up (about 11 µs). A pays that
too, and on these cores sometimes a lost core as well. The `WaitAsync` and `Complete` paths
do not touch the spin, and `Complete` now picks one of two event fields:
- `CompletionTableRoundTripBench.CompleteThenAwait` B/A 0.97 [0.90 .. 1.04] (its A/A 1.00 [0.97 .. 1.03]);
- `CompleteThenAwaitPeerSized` 0.99 [0.96 .. 1.02];
- `AwaitThenComplete` 1.040 [1.028 .. 1.052], then 1.013 [0.995 .. 1.032] when repeated, with its A/A at
  1.043 [1.018 .. 1.068]. That is the method's arm offset for this workload, not the change.

The recheck review repeated the table-level pairs at finer delays: 4 launches × 12 blocks × 200 rounds on CPUs 16–23, with
A/A differences of at most 0.43 µs.
- At 0–12 µs B was within −0.3 to +0.1 µs of A.
- At 15, 18 and 20 µs B was faster: 15.6 vs 18.3–25.3 µs, 18.6 vs 26.1–27.6 µs, and 22.1–22.5 vs 27.9–28.8 µs.
- At 22 µs, just past B's spin, B cost +0.45 to +0.55 µs in 3 of 4 launches (A/A in the same launches up to +0.43 µs).
- From 25 µs on the two were equal.

So "never slower" holds to within about 0.5 µs.

**Measurement, idle, MsQuic loopback.** This is the path the first review flagged, re-measured by the recheck review on
the final design. Each arm (A = main, B = this branch, a = a second copy of main) ran in its own AssemblyLoadContext with
its own client and server over MsQuic loopback; the client used `CompletionMode.ThreadPool`. Each op was
`SendCopy(64 B, UnreliableUnordered, Tracked)`, then the timed `Flush` + `QuiclyPeer.Wait(BufferReleased)`, then `Poll`.
Blocks rotated between the arms, 30 blocks × 400 ops per arm per launch. There were 16 launches on net10.0 and net11.0,
pinned to CPUs 16–23, which carried 15–55 % load from other sessions.

| | A (main) | a (main again) | B (this branch) |
|---|---:|---:|---:|
| p50 per launch | 14.1–28.6 µs (typically 21–25) | 13.6–31.9 µs | 17.2–19.1 µs |
| p90 per launch | 45.3–74.4 µs | | 46.4–49.5 µs |

- The paired B−A block-median difference had a median of −5.15 µs (range −8.47 to +4.15).
- B was lower than A in 15 of 16 launches. The one launch above had A/A at +8.43 µs.
- The A/A difference had a median magnitude of 2.0 µs.
- On this busier machine 37–41 % of B's completions landed after 20 µs, and p05–p30 landed at 7.5–15 µs.
- With eight busy threads in the same process, A's p50 was 1.75–3.79 ms (p90 up to 10 ms) and B's 14.0–20.5 µs (p90
  about 45–50 µs).

The first review's +6.7–8.2 µs regression of spin count 0 on this path is gone: B is typically about 5 µs faster than main.

**Measurement, loaded.** The same benchmark under BenchmarkDotNet (ShortRun, in-process; on net11.0 each run reports two
in-process jobs), pinned to CPUs 8–11 with `DOTNET_PROCESSOR_COUNT=4` and eight `powershell -Command "while(1){}"` on the
same four CPUs, A and B interleaved three times each. Mean per round, range over all jobs. The spin-0 column is
`04cf395`, measured earlier in the day; main read 0.66–4.1 ms per round then.

| DelayUs | A (main) | B (this branch) | spin count 0 |
|--------:|---:|---:|---:|
| 0 | 0.51–0.65 µs | 0.36–0.41 µs | 0.35–0.39 µs |
| 5 | 256–651 µs | 5.63–5.71 µs | |
| 10 | 502–709 µs | 10.6–10.9 µs | 16.0–17.2 µs |
| 15 | 641–687 µs | 15.6–15.8 µs | |
| 20 | 401–679 µs | 23.8–24.7 µs | 26.5–27.5 µs |
| 40 | 358–720 µs | 46.3–46.9 µs | 46.7–48.0 µs |
| 100 | 390–821 µs | 106.6–107.2 µs | 106.6–107.9 µs |

B never loses a quantum: up to 15 µs it pays the delay plus 0.6–0.9 µs, after that the delay plus 4–7 µs. Busy cores wake
a parked thread faster than idle cores in a sleep state.

**The stress test.** Wall time of a fresh test process (`--filter-method`, including ~0.4 s start-up). It ran the test
version that `tests/load-independent` ships (`7f43be8`, whose own hand-offs no longer yield), on CPUs 8–11 with
`DOTNET_PROCESSOR_COUNT=4`, runs interleaved. Each batch ran at a different time with its own main column, and other
sessions loaded the rest of the machine differently each time.

| batch | | main | change |
|---|---|---:|---:|
| spin count 0 (`04cf395`) | idle, 5 runs | 1.42–1.52 s | 1.49–1.56 s |
| | eight busy loops, 7 runs | 31.6–47.3 s (median 33.3) | 10.2–13.9 s (median 12.7) |
| this branch | idle, 5 runs | 1.66–1.83 s | 1.64–2.72 s (one run 2.72, the others ≤ 2.10) |
| | eight busy loops, 6 runs | 9.3–12.9 s (median 11.6) | 2.7–3.3 s (median 2.9) |

On main's own version of that test, whose spin awaiters also yield, one loaded run took 484 s on main and 397 s with spin
count 0: the test itself was then the bigger problem.

**Tests.** `CompletionTableBlockingWaitTests` (new):
- *The_Events_A_Blocking_Wait_Parks_On_Do_Not_Spin* reads both stage events by reflection after a parked wait and
  requires spin count 0. It is timing-free and runs on every platform.
- *A_Blocking_Wait_Parks_Instead_Of_Handing_Its_Cpu_To_A_Busy_Thread* is Windows only. It pins the waiter and a busy
  loop of equal (highest) priority to one CPU. It compares each table wait, completed 200 µs in, with a reference wait on
  an event that parks at once, in the round next to it: table minus reference must have a median under 5 ms. Main reads
  26.6–31.6 ms (the busy loop's quantum), this branch 0.000 ms. Two simpler designs failed in full Core.Tests runs: an
  absolute bound on the wake-up, and the same bound at the highest priority. Other tests' boosted threads and pauses of
  the whole test process delayed even a parked waiter.

The review added two tests:
- `ReviewCwaitperfIdleWakeTests` completes 6 µs in and compares against a spinning reference. 04cf395 read 7.2–10.3 µs
  behind and failed; main and this branch read 0.2–0.3 µs.
- `ReviewCwaitcontractTwoWaiterTests` covers the shared event.

Mutations, each failing the test that guards it:
- the event's default spin count fails both new tests;
- a `Thread.Yield` in place of the `Thread.SpinWait` fails the Windows test;
- parking at once (`04cf395`) fails the 6 µs test, 7 of 7;
- one shared event fails the two-waiter test, 6 of 6.

The whole Core.Tests project passed 2 379 of 2 379:
- on this branch, on CPUs 16–31, net10.0 and net11.0;
- on `7f43be8` with this change, on four CPUs (0xF00), in three of four runs. The fourth, on net10.0, failed only
  `ReviewCreditThreadingTests.Guard_A_Receiver_On_Its_Own_Thread_Gets_Every_Message(seed 22)`, whose coverage guard saw
  no credit hold (every message arrived; that test does not use the blocking wait).

**Decision.** Kept: spin without yielding until 20 µs into the wait, then park on a spin-0 event per stage. Idle it is as
fast as main (within about 0.5 µs) or faster in every paired run, and about 5 µs faster on a loopback datagram's
`BufferReleased`. Under load a wait never hands its core away: rounds that cost main 0.3–0.8 ms take the delay plus at
most 7 µs. Spin count 0 alone (`04cf395`) was rejected for its idle cost to datagram
completions. Spin count 10 measured the same as 0.
