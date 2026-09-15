# HTTP/3 codec benchmarks (`Tedd.Quicly.Http3`)

Method: ADR 0007. Benchmarks live in `benchmarks/Tedd.Quicly.Benchmarks/Http3`, superseded implementations in
`benchmarks/Tedd.Quicly.Archive/Http3`. Run with:

```
dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*Http3*' --job short
```

Hardware / runtime of the numbers below: X64 RyuJIT x86-64-v3, .NET 10.0.12 (10.0.1226.42308),
BenchmarkDotNet 0.15.8, `[ShortRunJob]` (3 iterations, hence the wide error bars), `[MemoryDiagnoser]`.

> BenchmarkDotNet 0.15.8 does not recognise the `net11.0` runtime moniker
> (`GetRuntimeVersion not implemented for NotRecognized`), so the runs use the `net10.0` target. Re-run on
> `net11.0` (e.g. `--inProcess`) once BenchmarkDotNet supports it; the code is identical for both TFMs.

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

| Method         | Mean     | Error     | StdDev   | Ratio | RatioSD | Allocated | Alloc Ratio |
|--------------- |---------:|----------:|---------:|------:|--------:|----------:|------------:|
| V0_TreeWalk    | 592.0 ns | 965.85 ns | 52.94 ns |  1.01 |    0.11 |         - |          NA |
| V1_NibbleTable | 163.5 ns |  16.33 ns |  0.90 ns |  0.28 |    0.02 |         - |          NA |

Both variants produce byte-identical output for every RFC 7541 Appendix C vector, every single symbol and
2000 random strings; `ZeroAllocationTests.Huffman_Encode_And_Decode` asserts 0 bytes allocated in steady state.

### Decision

Hypothesis confirmed: 3.6× faster (592 → 164 ns for 40 bytes ≈ 4.1 ns/byte), no allocation. V1 ships; V0 is
archived as `HpackHuffmanV0` with its own frozen copy of the code table so the comparison stays runnable.

## 2. QPACK codec, Chrome-like WebTransport CONNECT block (`QpackDecodeBench`)

Eight fields (`:method CONNECT`, `:authority`, `:scheme https`, `:path`, `:protocol webtransport`, `origin`,
`sec-webtransport-http3-draft02`, a 111-byte `user-agent`), static table + literals, Huffman where shorter
(encoded block ≈ 200 bytes). Decoded into a pre-allocated `Http3HeaderCollection`.

| Method | Mean     | Error     | StdDev    | Allocated |
|------- |---------:|----------:|----------:|----------:|
| Decode | 1.085 us | 0.4173 us | 0.0229 us |         - |
| Encode | 1.873 us | 3.1825 us | 0.1744 us |         - |

Baseline (V0) only; ~1 µs per request header block is far below anything on the connection path. The encoder's
extra cost is the linear 99-entry static-table scan per field plus Huffman length computation — a candidate for
a later hypothesis (name-length bucketed lookup) if profiling ever shows it.

## 3. Frame reader throughput (`FrameReaderBench`)

A 1 MiB stream of 1200-byte DATA frames interleaved with 40-byte HEADERS frames, consumed as one contiguous
buffer and in 1200-byte receive chunks (payload fragments delivered without copying).

| Method      | Mean     | Error    | StdDev   | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------ |---------:|---------:|---------:|------:|--------:|----------:|------------:|
| Contiguous  | 19.56 us | 5.041 us | 0.276 us |  1.00 |    0.02 |         - |          NA |
| Chunked1200 | 27.64 us | 4.979 us | 0.273 us |  1.41 |    0.02 |         - |          NA |

≈ 50 GB/s (contiguous) / 38 GB/s (packet-sized chunks) of framed stream: the reader touches only the headers,
so the cost is ~20–30 ns per frame. Baseline only; no optimisation attempted.
