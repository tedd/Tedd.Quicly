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

## Releasing

Pushing to the `deploy` branch runs `.github/workflows/nuget-publish.yml`: it packs every `src/` project, which builds the
solution, and pushes the packages and their symbol packages to nuget.org; a failed build stops the publish. The same
push runs `.github/workflows/pages.yml`, so the project site and the packages on nuget.org always describe the same
version. Ordinary builds and pull requests are covered by `.github/workflows/ci.yml`; nothing is published from them.

**Nothing is tested on the way to nuget.org.** The publish workflow used to carry a report-only test job that ran the
whole suite on a Windows runner — it gated nothing, it hung often enough that it needed a 40-minute cap, and Windows
minutes bill at twice the Linux rate, so it cost up to 80 billable minutes per deploy for no safety at all. It is gone.
What protects a release is, in order: the local per-project runs (the real gate — a full-solution run flakes on shared
ports, the machine certificate store and HttpListener prefixes), then CI on `main`. Do not push to `deploy` from a tree
you have not run the suites on.

CI itself is kept cheap on purpose: it skips pushes that only touch docs, the site or markdown; a newer push cancels the
run it superseded; it tests one target framework rather than both; and every job has an explicit `timeout-minutes`,
because without one a hung run bills GitHub's six-hour default.

It also runs only the fast, deterministic suites — Http3, Testing and Acme, about 25 seconds together. Core,
Transport.MsQuic, EndToEnd, Client, Server, Http and Replication bind sockets, build certificates and drive real
MsQuic; they are the slow ones and the flaky-under-sharing ones, and they run locally, per project, before a push.
The build step still compiles every project and every test project on both target frameworks, so an API break or a
bad merge fails CI whatever the test selection is.

* **Version.** `Directory.Build.props` holds the base version, `Major.Minor.Patch`. Every package is published as
  `Major.Minor.(Patch + workflow run number)`, so each push to `deploy` gets a higher version than the last and a
  rerun of the same run keeps its version. Bump `Major.Minor` in `Directory.Build.props` to start a new line. The
  published commit is tagged `v<version>`.
* **Credentials.** There is no API key secret. The workflow exchanges its GitHub OIDC token for a short-lived
  nuget.org key (trusted publishing) under the nuget.org user named in `nuget-publish.yml`.
* **One-time setup on nuget.org.** Under the account's *Trusted Publishing*, add a policy for repository owner
  `tedd`, repository `Tedd.QUICLY`, workflow file `nuget-publish.yml` (file name only, no environment). It must allow
  publishing new packages as well as new versions. While the repository is private the policy is only
  temporarily active: it turns permanent after the first successful publish and lapses after 7 days without one
  (it can be restarted from the same page).

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
