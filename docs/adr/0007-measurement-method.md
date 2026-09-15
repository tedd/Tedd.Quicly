# ADR 0007 — Scientific method for performance work

**Status:** accepted (2026-09-15)

Every optimisation is recorded in `docs/benchmarks/` as: hypothesis (what and why it should be faster),
measurement (BenchmarkDotNet job, hardware, both .NET 10 and .NET 11 where relevant), result, decision.
The previous implementation moves to `benchmarks/Tedd.Quicly.Archive` so the comparison stays runnable.
Allocation is asserted (`MemoryDiagnoser` = 0 B on the hot path) and unit tests guard it with
`GC.GetAllocatedBytesForCurrentThread()` around steady-state operations.
