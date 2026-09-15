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

## 4. net10.0 cross-check

Same machine, same day, `-f net10.0` (.NET 10.0.12). Every decision above holds on net10.0; the V1/V0 ratios are
3.1x (SPSC), 7.4x / 3.8x (MPSC, 1 / 4 producers) and 2.7x / 1.8x (table). The framework references move around
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

| Method                             | Mean     | Error     | StdDev    | Ratio | RatioSD | Allocated | Alloc Ratio |
|----------------------------------- |---------:|----------:|----------:|------:|--------:|----------:|------------:|
| Table_V1_CompleteThenAwait         | 25.22 ns |  94.62 ns |  5.186 ns |  1.03 |    0.25 |         - |          NA |
| Table_V0_Archive_CompleteThenAwait | 67.19 ns | 293.58 ns | 16.092 ns |  2.73 |    0.72 |         - |          NA |
| Table_V1_AwaitThenComplete         | 54.46 ns |  44.32 ns |  2.429 ns |  2.22 |    0.36 |         - |          NA |
| Table_V0_Archive_AwaitThenComplete | 96.61 ns |  77.86 ns |  4.267 ns |  3.93 |    0.64 |         - |          NA |

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
