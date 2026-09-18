# Memory — `SlabAllocator` rent/return

Method per [ADR 0007](../adr/0007-measurement-method.md): a simple correct version first (V0), a benchmark,
a hypothesis, the optimised version, a measurement, a decision — twice, because the first hypothesis was only
half right. Losers live in `benchmarks/Tedd.Quicly.Archive/Memory/` (`SlabAllocatorV0`, `SlabAllocatorV1`)
so every comparison stays runnable.

## Benchmark

`benchmarks/Tedd.Quicly.Benchmarks/Memory/SlabAllocatorBenchmarks.cs` — `[MemoryDiagnoser]`, ShortRun job.

Workload: rent a 1 536-byte block (the MTU-sized class), write one byte, in batches of 16 in flight, then
return the batch; 1 000 000 rent/return pairs per thread. `Threads = 1` runs on the calling thread;
`Threads = 8` starts 8 threads and joins them (thread start/join is inside the measured region and amortised
over 1 M operations). `OperationsPerInvoke = 1 000 000`, so the reported **Mean is wall time per operation
per thread**: with perfect scaling the 8-thread number equals the 1-thread number, and any increase is
contention (cache-line ping-pong on the free-list head, lock convoying, …). Allocated bytes for the 8-thread
rows are the eight `Thread` objects plus their start state, not per-operation allocations.

Competitors:

| Name | What |
|---|---|
| `Slab_V2_Sharded` | shipping `Tedd.Quicly.Core.Memory.SlabAllocator`: 8 Treiber stacks per class, `ValidateLeases = false` (Release default) |
| `Slab_V2_Sharded_Validated` | same, `ValidateLeases = true` (per-block state byte + generation check on return) |
| `Slab_V1_SingleStack` | archived `SlabAllocatorV1`: one Treiber stack + one global rented counter per class |
| `Slab_V0_Lock` | archived `SlabAllocatorV0`: same native slabs, `lock` around an array-backed free stack |
| `ArrayPool_Shared` | `ArrayPool<byte>.Shared.Rent(1536)` / `Return` (GC arrays, per-thread caches + shared partitions) |
| `NativeMemory_AllocFree` | `NativeMemory.Alloc(1536)` / `Free` (the CRT heap) |

Run with:

```
dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*SlabAllocatorBenchmarks*'
dotnet run -c Release -f net11.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*SlabAllocatorBenchmarks*'
```

BenchmarkDotNet 0.15.8 has no runtime moniker for the .NET 11 preview and its SDK validator throws for an
unrecognised host, so `Program.cs` defines the ShortRun job itself: the normal out-of-process toolchain on
.NET 10, the in-process toolchain on .NET 11. Numbers between the two runtimes are therefore not strictly
comparable (in-process skips a process boundary but shares the host's JIT state); compare rows within one table.

Hardware: AMD Ryzen 9 5950X (16 cores / 32 threads, two 8-core CCDs — a cache line moving between CCDs
costs far more than one moving inside a CCD), Windows 11 25H2.

## V0 — lock-based free stack

Per class: native slab, `int[] FreeStack` + `Top`, `ushort[] Generations`, counters, all under one
`System.Threading.Lock`. `TryRent` and `Return` each take the lock once. Correct and trivially so.

## Hypothesis 1 (V1): one lock-free Treiber stack per class

A `lock` costs two interlocked operations (enter + exit) even uncontended, and under contention it parks
threads. A Treiber stack over block indices with the ABA tag packed into the same 64-bit word as the top index
needs **one** `Interlocked.CompareExchange` per rent and one per return and never blocks. Expected: roughly
half the single-thread cost and a smaller degradation at 8 threads.

### Measurement 1 (.NET 10, out-of-process, ShortRun; first run, before V2 existed)

`Slab_V1_LockFree` in this table is the benchmark that is now called `Slab_V1_SingleStack`.

```
| Method                     | Threads | Mean        | Error        | StdDev     | Ratio | RatioSD | Allocated | Alloc Ratio |
|--------------------------- |-------- |------------:|-------------:|-----------:|------:|--------:|----------:|------------:|
| Slab_V1_LockFree           | 1       |    13.85 ns |     8.616 ns |   0.472 ns |  1.00 |    0.04 |         - |          NA |
| Slab_V1_LockFree_Validated | 1       |    18.37 ns |    31.299 ns |   1.716 ns |  1.33 |    0.11 |         - |          NA |
| Slab_V0_Lock               | 1       |    23.47 ns |    15.553 ns |   0.853 ns |  1.70 |    0.07 |         - |          NA |
| ArrayPool_Shared           | 1       |    38.34 ns |    13.978 ns |   0.766 ns |  2.77 |    0.09 |         - |          NA |
| NativeMemory_AllocFree     | 1       |    42.30 ns |    24.220 ns |   1.328 ns |  3.06 |    0.12 |         - |          NA |
|                            |         |             |              |            |       |         |           |             |
| Slab_V1_LockFree           | 8       | 2,438.68 ns |   486.128 ns |  26.646 ns |  1.00 |    0.01 |         - |          NA |
| Slab_V1_LockFree_Validated | 8       | 2,406.40 ns | 2,054.013 ns | 112.587 ns |  0.99 |    0.04 |         - |          NA |
| Slab_V0_Lock               | 8       | 1,508.87 ns |   587.008 ns |  32.176 ns |  0.62 |    0.01 |         - |          NA |
| ArrayPool_Shared           | 8       |    82.44 ns |    42.756 ns |   2.344 ns |  0.03 |    0.00 |         - |          NA |
| NativeMemory_AllocFree     | 8       |   202.51 ns |   88.999 ns  |   4.878 ns |  0.08 |    0.00 |         - |          NA |
```

Result: the single-thread half of the hypothesis held (13.9 ns vs 23.5 ns for the lock, 2.8× faster than
`ArrayPool`, 3× faster than `malloc`/`free`). The contention half was wrong: at 8 threads V1 is **176× slower
than itself single-threaded and 1.6× slower than the lock**. Every thread hits the same head line for its
CAS *and* the same counter line for `Rented`, both lines migrate between the two CCDs on every operation, and
failed CASes retry immediately, so the retry storm feeds itself. The lock is better precisely because it parks
losers instead of letting them retry. `ArrayPool` is 30× faster at 8 threads because its hot path is
thread-local.

## Hypothesis 2 (V2): shard the free list, give each shard its own counters, back off on CAS failure

Contention is per cache line, not per class. Splitting each class into `FreeListShards` (default 8)
independent Treiber stacks, assigning threads to shards round-robin, popping from the own shard first and
stealing from the others only when it is empty, and returning a block to the shard it came from (recorded in
the lease's spare byte) should let 8 uncoordinated threads run on 8 different lines, i.e. close to the
single-thread number. `Rented` and `Peak` become per-shard too, so the second globally contended line of V1
disappears; each shard header is 128 bytes with the head on the first line and the counters on the second,
so a CAS retry storm on the head does not also stall the counter increment (and vice versa). Exponential
back-off (`Thread.SpinWait`, 1…32) turns a retry storm into a queue when two threads do share a shard.
Cost: a per-class `Peak` becomes the sum of per-shard peaks (an upper bound; exact from one thread), and the
exhaustion path scans all shards. Expected: 1-thread unchanged (one extra thread-static read and an
`&`), 8-thread within a small factor of 1-thread.

### Measurement 2 — .NET 10.0.12 (out-of-process, ShortRun)

```
| Method                    | Threads | Mean        | Error        | StdDev     | Ratio | RatioSD | Allocated | Alloc Ratio |
|-------------------------- |-------- |------------:|-------------:|-----------:|------:|--------:|----------:|------------:|
| Slab_V2_Sharded           | 1       |    18.01 ns |    17.079 ns |   0.936 ns |  1.00 |    0.06 |         - |          NA |
| Slab_V2_Sharded_Validated | 1       |    20.17 ns |    20.215 ns |   1.108 ns |  1.12 |    0.07 |         - |          NA |
| Slab_V1_SingleStack       | 1       |    16.09 ns |    31.955 ns |   1.752 ns |  0.89 |    0.09 |         - |          NA |
| Slab_V0_Lock              | 1       |    33.56 ns |    62.708 ns |   3.437 ns |  1.87 |    0.18 |         - |          NA |
| ArrayPool_Shared          | 1       |    50.17 ns |    79.142 ns |   4.338 ns |  2.79 |    0.24 |         - |          NA |
| NativeMemory_AllocFree    | 1       |    61.69 ns |    63.286 ns |   3.469 ns |  3.43 |    0.22 |         - |          NA |
|                           |         |             |              |            |       |         |           |             |
| Slab_V2_Sharded           | 8       |    29.35 ns |     6.511 ns |   0.357 ns |  1.00 |    0.01 |         - |          NA |
| Slab_V2_Sharded_Validated | 8       |    31.44 ns |     3.333 ns |   0.183 ns |  1.07 |    0.01 |         - |          NA |
| Slab_V1_SingleStack       | 8       | 2,291.76 ns | 2,497.905 ns | 136.919 ns | 78.10 |    4.12 |         - |          NA |
| Slab_V0_Lock              | 8       | 2,212.52 ns | 3,758.737 ns | 206.029 ns | 75.40 |    6.13 |         - |          NA |
| ArrayPool_Shared          | 8       |    87.03 ns |    68.041 ns |   3.730 ns |  2.97 |    0.11 |         - |          NA |
| NativeMemory_AllocFree    | 8       |   177.64 ns |    80.425 ns |   4.408 ns |  6.05 |    0.14 |         - |          NA |
```

### Measurement 2 — .NET 11 preview 7 (in-process, ShortRun)

```
| Method                    | Threads | Mean        | Error        | StdDev     | Ratio | RatioSD | Allocated | Alloc Ratio |
|-------------------------- |-------- |------------:|-------------:|-----------:|------:|--------:|----------:|------------:|
| Slab_V2_Sharded           | 1       |    28.58 ns |    47.771 ns |   2.618 ns |  1.01 |    0.11 |         - |          NA |
| Slab_V2_Sharded_Validated | 1       |    27.23 ns |    49.013 ns |   2.687 ns |  0.96 |    0.11 |         - |          NA |
| Slab_V1_SingleStack       | 1       |    21.48 ns |    26.124 ns |   1.432 ns |  0.76 |    0.08 |         - |          NA |
| Slab_V0_Lock              | 1       |    34.24 ns |   131.941 ns |   7.232 ns |  1.20 |    0.24 |         - |          NA |
| ArrayPool_Shared          | 1       |    67.98 ns |   142.193 ns |   7.794 ns |  2.39 |    0.31 |         - |          NA |
| NativeMemory_AllocFree    | 1       |    61.67 ns |   186.035 ns |  10.197 ns |  2.17 |    0.36 |         - |          NA |
|                           |         |             |              |            |       |         |           |             |
| Slab_V2_Sharded           | 8       |    32.28 ns |     5.958 ns |   0.327 ns |  1.00 |    0.01 |         - |          NA |
| Slab_V2_Sharded_Validated | 8       |    34.47 ns |     5.498 ns |   0.301 ns |  1.07 |    0.01 |         - |          NA |
| Slab_V1_SingleStack       | 8       | 2,441.43 ns | 1,824.570 ns | 100.011 ns | 75.64 |    2.77 |         - |          NA |
| Slab_V0_Lock              | 8       | 2,231.25 ns |   541.557 ns |  29.685 ns | 69.13 |    1.00 |         - |          NA |
| ArrayPool_Shared          | 8       |   161.04 ns |    72.378 ns |   3.967 ns |  4.99 |    0.12 |         - |          NA |
| NativeMemory_AllocFree    | 8       |   245.63 ns |   262.981 ns |  14.415 ns |  7.61 |    0.39 |         - |          NA |
```

(The .NET 11 single-thread rows carry ±2–10 ns error bars at N = 3: the in-process job shares the host's
JIT and GC state, so read them as "the same order as .NET 10", not as a regression. The 8-thread rows are
tight on both runtimes.)

Result: the hypothesis held on both runtimes. At 8 threads V2 costs **29 ns** per operation per thread on
.NET 10 (32 ns on .NET 11) against 2 292 ns for the single stack and 2 213 ns for the lock — a **78×** win
over V1 and **75×** over V0 — and it is 3× faster than `ArrayPool<byte>.Shared` (87 ns) and 6× faster than
`malloc`/`free` (178 ns). The 8-thread number is 1.6× the 1-thread number, not 1.0×: the eight benchmark
threads start together and take shard slots round-robin, but any earlier thread of the process (thread pool,
BenchmarkDotNet's own) already consumed slots, so two of the eight can land on one shard and pay the
back-off queue; the remaining gap is the shard head line staying in one core's cache versus L3. Single
thread, V2 is 2 ns (12 %) slower than V1: one `[ThreadStatic]` read and one `&` to pick the shard, exactly
the predicted cost. Lease validation (per-block state byte + generation check on return) costs 2 ns at
either thread count.

## Decision

**V2 (sharded Treiber stacks, per-shard counters, back-off) ships** as `Tedd.Quicly.Core.Memory.SlabAllocator`;
V0 and V1 are archived. The 2 ns single-thread cost buys a 78× improvement under contention, which is the
case that matters: the transport thread returns receive buffers while the game thread rents send buffers,
and a server has several of each. `ValidateLeases` stays on in Debug and off in Release — 2 ns for
double-return and stale-lease detection is worth it while developing, not in production.

Per-thread magazines with SPSC return rings (ADR 0008) would turn the remaining 1.6× 8-thread gap into a
thread-local hit with no atomic at all, at the cost of blocks parked in magazines and a return ring per
thread pair. At 29 ns per operation that is not yet the bottleneck of a send (the MsQuic call is
microseconds); it stays on the list until a profile of a full peer says otherwise.

## Hot-path pass (2026-09-18): what the allocator's atomics cost a message, and chained returns

Method: the paired in-process comparison of the [ADR 0007 addendum](../adr/0007-measurement-method.md) (.NET 10.0.12).

**Share of the atomics.** A 64-byte message end to end does 10 `lock`-prefixed instructions in the allocator and the budgets
(Tier-1 disassembly, lease validation off): send rent 2 (`cmpxchg` on the shard head, `xadd` on the shard's rented count), send
return 2, receive rent 2 + the receive budget's `xadd`, receive return 2 + the budget. Replacing every allocator atomic with a plain
operation (a diagnostic ablation, single thread, uncontended) moved `stages.Ordered64` 0.968 [0.946 .. 0.991] and `stages.Packed`
0.963 [0.938 .. 0.988] (6 launches each; send −3.7 / −4.8 ns, the rest in deliver and spoll): 3–4 % of a message. **The game-thread
magazine suggested above stays rejected**: it could remove only the send side's share (≤ 4 ns, ≤ 2 %) and would need a new owner
API, statistics that count cached blocks, a `ThreadSafeSend` bypass and a flush on teardown. The cross-core cost of the same lines
is larger (docs/benchmarks/threading.md §6).

**Kept: `ReturnMany`.** The packer used to return each member's payload lease the moment it copied it into a container: about 17 ×
(counter decrement + CAS) per container. `SlabAllocator.ReturnMany` returns a run of same-class, same-shard blocks with one
validation pass, one counter add and one CAS; the packer releases each member's *send budget* when it copies the member in (so the
budget looks exactly as before to anything renting in the same pass) and returns the blocks in one chain when the container
closes, on `Reset` and on `Dispose`. `stages.Packed` flush fell from 47.6 to 41.4 ns per message (0.962 [0.947 .. 0.976] total, 10
launches). With `ValidateLeases`, a stale or double-returned lease rejects its whole run and leaves every lease of it rented.
Side effect: until the container closes, the member blocks are still out of the pool, so a same-class rent in that window sees a
slightly emptier pool than before (it matters only when that class is exhausted).
