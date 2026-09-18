# ADR 0007 — Scientific method for performance work

**Status:** accepted (2026-09-15)

Every optimisation is recorded in `docs/benchmarks/` as: hypothesis (what and why it should be faster),
measurement (BenchmarkDotNet job, hardware, both .NET 10 and .NET 11 where relevant), result, decision.
The previous implementation moves to `benchmarks/Tedd.Quicly.Archive` so the comparison stays runnable.
Allocation is asserted (`MemoryDiagnoser` = 0 B on the hot path) and unit tests guard it with
`GC.GetAllocatedBytesForCurrentThread()` around steady-state operations.

## Addendum (2026-09-18): attribution and paired comparisons

Found during the hot-path pass (baseline `691f20d`) and binding for later performance work:

- **Attribution.** EventPipe sampling (`dotnet-trace`, `dotnet-sampled-thread-time`) is safe-point biased on this code:
  `Thread.PollGCWorker` collects 10–33 % of the samples and large copies are over-attributed, so a sampled profile is a
  coarse inclusive ranking only. Per-phase costs come from stage timing:
  `dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- stages <Packed|Keyed|Ordered64|Ordered4K|Loose>`
  (send / flush / deliver / spoll / ack / cpoll in ns per message; `Profiling/Stages.cs`). `-- sustain <Class> <Method>`
  runs one benchmark method in a loop for a profiler to attach to.
- **Separate processes are too noisy for single-digit effects.** Two builds of the same commit, each run in its own
  process (pinned to two cores at high priority), differed by up to ±8 %: every process — and every loaded copy of the
  code — gets its own code and memory layout. It is not dynamic PGO (`DOTNET_TieredPGO=0` leaves it at ~3 %). ShortRun
  (N = 3) and single-process ratios below ~10 % are therefore not evidence.
- **Paired in-process comparison.** `benchmarks/scripts/build-arm.sh` freezes a build of a checkout as an *arm*;
  `benchmarks/scripts/pair.sh <armA> <armB> <Class.Method | stages.<workload>>` loads both arms into one PairHost process
  (`benchmarks/Tedd.Quicly.PairHost`, two `AssemblyLoadContext`s), alternates 0.4 s windows A,B / B,A on one pinned thread,
  and repeats that over several launches. The launch is the unit of evidence (per-launch sd ~2–3 %); `COMBINED` is the
  geometric mean of the per-launch ratios with a 95 % t interval. Identical arms gave 0.994 [0.962 .. 1.028] over six
  launches. A change counts as measured when its interval over ≥ 10 launches excludes 1.0 and the stage column that moved
  agrees with the mechanism (disassembly or an ablation).
- **Real threads.** Every simulator benchmark runs both peers on one thread. Cross-core costs need two-thread benchmarks
  (`Threading/CrossCoreHandoffBench`, `Session/PeerReceiveHandoffBench` — use its *Pipelined* mode for claims) or a real
  MsQuic run with pinned game threads: which CCD the game thread and the MsQuic workers share moves QUICLY's cost by ~50 %.
