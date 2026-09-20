# Development guide

## Prerequisites

* .NET SDK 11.0.100-preview.7 or later (see `global.json`; `net10.0` is also built, so the .NET 10 runtime
  must be present).
* Windows 11 / Server 2022+ (MsQuic via Schannel is bundled with the runtime) — or Linux/macOS with
  `libmsquic` 2.x installed.
* No Python, no Node required.

## Build

```bash
dotnet build Tedd.Quicly.slnx -c Release
```

## Tests

Tests use xunit.v3 on the Microsoft.Testing.Platform runner (`global.json` opts `dotnet test` into it).

```bash
dotnet test Tedd.Quicly.slnx -c Release
```

Single project (both target frameworks):

```bash
dotnet test tests/Tedd.Quicly.Core.Tests -c Release
```

Filter by name (MTP syntax):

```bash
dotnet test tests/Tedd.Quicly.Core.Tests -c Release -- --filter-method "*VarInt*"
```

## Coverage

```bash
dotnet test tests/Tedd.Quicly.Core.Tests -c Release -- --coverage --coverage-output-format cobertura --coverage-output coverage.cobertura.xml
```

The report lands under the test project's `bin/.../TestResults/`. Summarise with ReportGenerator if installed:

```bash
reportgenerator -reports:"**/coverage.cobertura.xml" -targetdir:coverage-report -reporttypes:TextSummary
```

## Benchmarks

```bash
dotnet run -c Release --project benchmarks/Tedd.Quicly.Benchmarks -- --filter "*VarInt*" --job short
```

Every benchmark change is recorded in `docs/benchmarks/<area>.md` with hypothesis, numbers and decision
(ADR 0007). Superseded implementations live in `benchmarks/Tedd.Quicly.Archive` so comparisons stay runnable.

## Conventions

* Root namespace = project name. File-scoped namespaces. Nullable warnings are errors.
* `src/` projects are AOT-compatible (`IsAotCompatible=true`): no reflection, no dynamic code.
* Hot path: no GC allocations, no LINQ, no exceptions for control flow, no per-operation delegates.
  Guard it with a test that asserts `GC.GetAllocatedBytesForCurrentThread()` does not change across a
  warmed-up loop, measured with `WindowedAllocation.AssertNone` (`AllocationAssert.NoAllocations` in the Server
  tests): the test host's runtime now and then allocates a few kilobytes on the test thread in one window, which
  those helpers tolerate and a hand-rolled single measurement does not. Test projects do not reference one
  another, so each one that needs it carries its own copy of `WindowedAllocation`; keep the copies in step.
  A check whose assertions count the work done takes the window count the helper returns, and one that measures
  a one-time cost (rather than a steady-state loop) takes the smallest of several samples instead.
* Public API gets XML docs.
