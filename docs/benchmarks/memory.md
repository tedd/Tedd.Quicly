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

## Hypothesis 2 (V2): shard the free list, keep the counters on the head line, back off on CAS failure

Contention is per cache line, not per class. Splitting each class into `FreeListShards` (default 8)
independent Treiber stacks, assigning threads to shards round-robin, popping from the own shard first and
stealing from the others only when it is empty, and returning a block to the shard it came from (recorded in
the lease's spare byte) should let 8 uncoordinated threads run on 8 different lines, i.e. close to the
single-thread number. Moving `Rented` and `Peak` onto the shard's head line removes the second contended
line (after a successful CAS the line is already owned, so the increment is nearly free), and exponential
back-off (`Thread.SpinWait`, 1…32) turns a retry storm into a queue when two threads do share a shard.
Cost: a per-class `Peak` becomes the sum of per-shard peaks (an upper bound; exact from one thread), and the
exhaustion path scans all shards. Expected: 1-thread unchanged (one extra thread-static read and an
`&`), 8-thread within a small factor of 1-thread.

### Measurement 2 — .NET 10 (out-of-process, ShortRun)

RESULTS_NET10

### Measurement 2 — .NET 11 preview 7 (in-process, ShortRun)

RESULTS_NET11

## Decision

DECISION
