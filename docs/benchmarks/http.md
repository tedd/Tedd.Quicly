# HTTP/1.1 — request-head parser

Method per ADR 0007: simple correct V0 → hypothesis → V1 → measure → keep the winner, archive the loser in
`benchmarks/Tedd.Quicly.Archive/Http` (namespace `Tedd.Quicly.Archive.Http`, class suffixed with its version)
so the comparison stays runnable.

`Tedd.Quicly.Http` is not on the game data path (ADR 0008 governs the QUIC path); it serves ACME `http-01`,
health and static files on a public port. What matters here is the cost of the **worst request a hostile
client can make us parse** within the ADR 0009 limits (request line ≤ 2 KiB, headers ≤ 8 KiB / 32 fields),
more than the typical request.

## Measurement setup

* Hardware: AMD Ryzen 9 5950X 3.40 GHz (16C/32T), Windows 11 (10.0.26200), AVX2 (`VectorSize=256`).
* SDK 11.0.100-preview.7; runtimes .NET 10.0.12 and .NET 11.0.0-preview.7.26381.103, X64 RyuJIT x86-64-v3.
* BenchmarkDotNet 0.15.8, `InProcessEmitToolchain` (0.15.8 cannot spawn `net11.0` children), `MemoryDiagnoser`.
  Two in-process jobs ran per case (BDN's default job and `Job.ShortRun`); they agree within 5 % and the
  tables below show the `ShortRun` rows.
* Command (per runtime):
  `dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*HttpParser*' --inProcess`
  (and `-f net11.0`).
* Every case allocates 0 B.

## Benchmark

`HttpParserBenchmarks` parses one complete request head (request line + header section) into a
pre-allocated `ParsedRequest` (offsets only, no strings). Shapes:

| Shape     | Size    | What it models |
|-----------|--------:|----------------|
| `acme`    | ~0.2 KiB | Let's Encrypt's `http-01` validator: short request line, 3 headers |
| `browser` | ~0.6 KiB | A browser page load: 12 headers incl. a long `User-Agent`, `Accept`, `Cookie` |
| `wide`    | ~6 KiB  | 30 fields of ~200 B: the largest head a client can send inside the 8 KiB limit |

### V0 — shipping (`src/Tedd.Quicly.Http/Parsing/HttpParser.cs`)

Per line: a vectorised `IndexOf((byte)'\n')` inside a window bounded by the remaining limit, then
`SearchValues` validation of the slice (`ContainsAnyExcept(tchar)` for method and field names,
`ContainsAnyExcept(target chars)` for the target, `ContainsAny(CTL)` for values). Two to three vectorised
passes over every byte.

### Hypothesis for V1 (`benchmarks/Tedd.Quicly.Archive/Http/HttpParserV1.cs`)

Classify every byte exactly once through a 256-entry flag table (tchar / target char / field-value char) in a
single scalar loop, so no byte is scanned twice and no vector setup is paid per line. Expected to win on the
short lines that dominate real requests, and to lose little on long ones.

### Measurement

Mean time per parse (`ShortRun`, in-process). Ratio is V1 / V0.

| Shape     | .NET 10 V0 | .NET 10 V1 | ratio | .NET 11 V0 | .NET 11 V1 | ratio |
|-----------|-----------:|-----------:|------:|-----------:|-----------:|------:|
| `acme`    |   147.4 ns |   140.0 ns |  0.95 |    80.6 ns |   140.4 ns |  1.74 |
| `browser` |   446.4 ns |   348.1 ns |  0.78 |   207.5 ns |   353.5 ns |  1.70 |
| `wide`    | 1 172.5 ns | 3 276.9 ns |  2.79 |   572.6 ns | 3 224.4 ns |  5.63 |

BDN's default in-process job, for reference: .NET 10 `browser` 457.1 / 348.9 ns, `wide` 1 238.0 /
3 276.6 ns; .NET 11 `acme` 80.8 / 138.4 ns, `browser` 207.7 / 350.4 ns, `wide` 571.2 / 3 248.8 ns.

### Result

* The hypothesis holds only partly, and only on .NET 10: V1 is ~22 % faster on the `browser` shape
  (≈ 100 ns per request) and level on `acme`.
* It fails where it matters. On the near-limit `wide` shape V1 costs ~0.55 ns per byte regardless of
  runtime, while V0 streams long values through 32-byte vectors: V1 is 2.8× slower on .NET 10 and 5.6×
  slower on .NET 11.
* .NET 11 roughly halves V0 on every shape (better `SearchValues` / `IndexOf` code generation). V1's
  scalar loop does not move, so on .NET 11 V0 is faster on every shape, typical requests included.

### Decision

**Keep V0.** A parser on a public port is sized by the adversarial case, and V0's worst case is 2.8–5.6×
cheaper. V1's only win, about 100 ns per request on .NET 10, is irrelevant at this server's request rates and
disappears on .NET 11. V1 stays in the archive, and the benchmark keeps both runnable.

## Allocation and memory bounds (not timed)

* The parser, the header collection's lookups and the ClientHello reassembly are allocation-free in steady
  state. Unit tests guard this with `GC.GetAllocatedBytesForCurrentThread()`:
  `HttpParserTests.Steady_state_parse_does_not_allocate`,
  `HttpHeaderCollectionTests.Steady_state_lookup_does_not_allocate`,
  `ByteBufferWriterTests.Steady_state_header_serialisation_does_not_allocate` and
  `ClientHelloAssemblyTests.Assembly_does_not_allocate`.
* Materialising a request allocates the strings handed to handlers: the path, the raw target, and header names
  and values that are not interned (methods, common names and common values are interned). This is by design
  for a string-based handler API off the hot path.
* **TLS ClientHello peek.** The first implementation rented 80 KiB of peek buffer plus 64 KiB of reassembly
  buffer for every TLS connection before the first byte arrived, so 2 048 slow-loris connections pinned about
  288 MiB of pooled arrays. The peek buffer now starts at 4 KiB and doubles only as bytes arrive (bounded by
  `ClientHelloParser.MaxPeekBytes`). The reassembly buffer (`ClientHelloReader`) starts at the size of the
  first bytes and grows only as handshake payload arrives, bounded by `ClientHelloParser.MaxClientHelloLength`.
  A connection that trickles a hello therefore holds memory in proportion to what it has actually sent.
* **TLS ClientHello CPU.** Reassembly used to restart from the first record on every read, so a hello sent
  as thousands of one-byte records, one per TCP segment, cost CPU quadratic in the record count (about
  n²/2 record visits per connection, ~450 million for a 12 KiB hello). The resumable
  `ClientHelloParser.TryAssemble(..., ref ClientHelloAssemblyState, ...)` walks each record once, so the cost is
  linear in the bytes received. `ReviewFollowUpTests.Client_hello_reader_is_linear_for_a_hello_trickled_as_one_byte_records`
  feeds such a hello one byte per call (~73 000 calls) and finishes in milliseconds. This is a
  complexity fix, not a micro-optimisation, so it was not benchmarked; the request parser measured above is
  unchanged.
* Connections rejected by the per-address limit are refused before any per-connection state is created. No
  buffers or linked cancellation sources are allocated, so a connect flood from one address costs only the
  accept.
