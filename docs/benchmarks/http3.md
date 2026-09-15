# HTTP/3 codec benchmarks (`Tedd.Quicly.Http3`)

Method: ADR 0007. Benchmarks live in `benchmarks/Tedd.Quicly.Benchmarks/Http3`, superseded implementations in
`benchmarks/Tedd.Quicly.Archive/Http3`. All three classes share `Http3BenchConfig`
(`Job.ShortRun.WithToolchain(InProcessEmitToolchain.Instance)` + `MemoryDiagnoser`). Run with:

```
dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*Http3*'
dotnet run -c Release -f net11.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*Http3*'
```

Hardware / runtime of the numbers below: X64 RyuJIT x86-64-v3, Windows 11, BenchmarkDotNet 0.15.8, in-process
short run (3 iterations, hence the wide error bars), `[MemoryDiagnoser]`. Two runtimes were measured:
.NET 10.0.12 (10.0.1226.42308) and .NET 11.0.0-preview.7.26381.103. The in-process toolchain is what makes the
`net11.0` run possible (BenchmarkDotNet 0.15.8 cannot spawn a `net11.0` child process); the code is identical
for both TFMs and, as expected, so are the numbers within noise.

## 1. Huffman decode of a 40-byte header value (`HuffmanDecodeBench`)

### V0 — bit-by-bit code-tree walk (`Tedd.Quicly.Archive.Http3.HpackHuffmanV0`)

The obvious correct implementation: a binary tree built from the RFC 7541 Appendix B code table, walked one
input bit at a time; padding validity is tracked with a "bits since last symbol" counter and an all-ones flag.
Cost: 8 branches + 8 dependent array loads per input byte.

### Hypothesis

Consuming the input 4 bits at a time through a precomputed transition table (256 internal tree nodes = 256
states × 16 nibble values, each entry = next state + optional emitted symbol + accept/fail flags) should cut the
per-byte work to 2 table loads and 2 flag tests. Because the shortest code is 5 bits, at most one symbol is
emitted per nibble, so an entry fits in one `uint`. The 16 KiB table is built once at type initialisation from
the same code table — the only allocation, at startup. Expected: 3–4× faster, identical output, 0 B/op.

### V1 — nibble transition table (`Tedd.Quicly.Http3.Qpack.HpackHuffman`, shipped)

Padding rules fall out of the table: an entry is "accepting" iff its destination node lies on the all-ones
path from the root at depth ≤ 7 (RFC 7541 §5.2: padding must be a prefix of EOS and at most 7 bits); reaching
the EOS leaf sets the fail flag.

### Measurement

.NET 10.0.12:

| Method         | Mean     | Error       | StdDev   | Ratio | RatioSD | Allocated | Alloc Ratio |
|--------------- |---------:|------------:|---------:|------:|--------:|----------:|------------:|
| V0_TreeWalk    | 566.3 ns | 1,147.14 ns | 62.88 ns |  1.01 |    0.14 |         - |          NA |
| V1_NibbleTable | 168.4 ns |    54.98 ns |  3.01 ns |  0.30 |    0.03 |         - |          NA |

.NET 11.0.0-preview.7:

| Method         | Mean     | Error       | StdDev   | Ratio | RatioSD | Allocated | Alloc Ratio |
|--------------- |---------:|------------:|---------:|------:|--------:|----------:|------------:|
| V0_TreeWalk    | 645.4 ns | 1,095.14 ns | 60.03 ns |  1.01 |    0.12 |         - |          NA |
| V1_NibbleTable | 169.2 ns |    53.04 ns |  2.91 ns |  0.26 |    0.02 |         - |          NA |

(An earlier out-of-process `[ShortRunJob]` run on .NET 10 gave 592.0 ns / 163.5 ns — the same picture.)

Both variants produce byte-identical output for every RFC 7541 Appendix C vector, every single symbol and
2000 random strings; `ZeroAllocationTests.Huffman_Encode_And_Decode` asserts 0 bytes allocated in steady state.

### Decision

Hypothesis confirmed: 3.6× faster (592 → 164 ns for 40 bytes ≈ 4.1 ns/byte), no allocation. V1 ships; V0 is
archived as `HpackHuffmanV0` with its own frozen copy of the code table so the comparison stays runnable.

## 2. QPACK codec, Chrome-like WebTransport CONNECT block (`QpackDecodeBench`)

Eight fields (`:method CONNECT`, `:authority`, `:scheme https`, `:path`, `:protocol webtransport`, `origin`,
`sec-webtransport-http3-draft02`, a 111-byte `user-agent`), static table + literals, Huffman where shorter
(encoded block ≈ 200 bytes). Decoded into a pre-allocated `Http3HeaderCollection`.

.NET 10.0.12:

| Method | Mean     | Error     | StdDev    | Allocated |
|------- |---------:|----------:|----------:|----------:|
| Decode | 1.025 us | 0.1480 us | 0.0081 us |         - |
| Encode | 1.671 us | 3.9288 us | 0.2153 us |         - |

.NET 11.0.0-preview.7:

| Method | Mean     | Error     | StdDev    | Allocated |
|------- |---------:|----------:|----------:|----------:|
| Decode | 1.082 us | 0.3663 us | 0.0201 us |         - |
| Encode | 1.718 us | 0.8835 us | 0.0484 us |         - |

Baseline (V0) only; ~1 µs per request header block is far below anything on the connection path. The encoder's
extra cost is the linear 99-entry static-table scan per field plus Huffman length computation — a candidate for
a later hypothesis (name-length bucketed lookup) if profiling ever shows it.

## 3. Frame reader throughput (`FrameReaderBench`)

A 1 MiB stream of 1200-byte DATA frames interleaved with 40-byte HEADERS frames, consumed as one contiguous
buffer and in 1200-byte receive chunks (payload fragments delivered without copying).

.NET 10.0.12:

| Method      | Mean     | Error     | StdDev   | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------ |---------:|----------:|---------:|------:|--------:|----------:|------------:|
| Contiguous  | 21.05 us | 20.654 us | 1.132 us |  1.00 |    0.07 |         - |          NA |
| Chunked1200 | 29.23 us |  7.904 us | 0.433 us |  1.39 |    0.07 |         - |          NA |

.NET 11.0.0-preview.7:

| Method      | Mean     | Error     | StdDev   | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------ |---------:|----------:|---------:|------:|--------:|----------:|------------:|
| Contiguous  | 22.48 us |  2.554 us | 0.140 us |  1.00 |    0.01 |         - |          NA |
| Chunked1200 | 26.71 us | 88.896 us | 4.873 us |  1.19 |    0.19 |         - |          NA |

≈ 50 GB/s (contiguous) / 38 GB/s (packet-sized chunks) of framed stream: the reader touches only the headers,
so the cost is ~20–30 ns per frame. Baseline only; no optimisation attempted.
