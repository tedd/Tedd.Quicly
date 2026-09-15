# Benchmarks — Tedd.Quicly.Acme

The ACME client is not on the game hot path (a handful of HTTPS round trips every ~60 days), so correctness and
clarity win over micro-optimisation. The one primitive that is called for every JWS header, payload, signature, JWK
coordinate, thumbprint, DNS TXT value and CSR is base64url encoding/decoding, so that is what was measured
(method per [ADR 0007](../adr/0007-measurement-method.md)).

## Base64url codec: V0 (string surgery) vs V1 (`System.Buffers.Text.Base64Url`)

**V0** (`benchmarks/Tedd.Quicly.Archive/Acme/Base64UrlV0.cs`): `Convert.ToBase64String(...)`, then
`.TrimEnd('=')`, `.Replace('+', '-')`, `.Replace('/', '_')`; decoding re-pads and reverses the replacements before
`Convert.FromBase64String`. This is the textbook implementation found in most ACME clients.

**V1** (`src/Tedd.Quicly.Acme/Base64UrlCodec.cs`): the in-box `System.Buffers.Text.Base64Url` (.NET 9+), which
encodes straight to the URL-safe alphabet without padding and offers span overloads.

### Hypothesis

V0 allocates up to four intermediate strings per encode (the padded base64 string plus one per `TrimEnd`/`Replace`
that changes something) and walks the string up to three extra times. V1 produces the final alphabet in a single
vectorised pass, so it should be **2–4× faster and allocate exactly one string** (or nothing with the span overload).
Decoding should show a similar but smaller gain (V0 always allocates the re-padded string before decoding).

### Measurement

* `dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*Base64UrlBench*'`
* `[Config(typeof(InProcessShortRunConfig))]`: ShortRun (3 warm-up + 3 measured iterations, 1 launch), in-process
  emit toolchain, memory diagnoser. BenchmarkDotNet 0.15.8 cannot spawn `net11.0` children, so the run is on .NET 10;
  the code paths are identical on .NET 11.
* Sizes: 32 B (a SHA-256 thumbprint / DNS TXT value), 256 B (an RSA-2048 modulus or signature), 1 KiB (a CSR).
* Machine: AMD Ryzen 9 5950X (16 cores / 32 threads), Windows 11 25H2, .NET 10.0.12 (RyuJIT x86-64-v3), 2026-09-15.
  Other agents were running builds and tests on the same machine, so the ShortRun error margins are wide; the
  ratios are what matter, and they were stable across both toolchain rows BenchmarkDotNet produced.

### Results (in-process toolchain)

| Method         | Size   |        Mean | Ratio | Allocated | Alloc ratio |
|----------------|-------:|------------:|------:|----------:|------------:|
| Encode_V0      |   32 B |   120.09 ns |  1.00 |     448 B |        1.00 |
| Encode_V1      |   32 B |    37.31 ns |  0.31 |     112 B |        0.25 |
| Encode_V1_Span |   32 B |    17.80 ns |  0.15 |       0 B |        0.00 |
| Decode_V0      |   32 B |   194.49 ns |  1.62 |     392 B |        0.88 |
| Decode_V1      |   32 B |    47.22 ns |  0.39 |      56 B |        0.12 |
| Encode_V0      |  256 B |   415.47 ns |  1.00 |    2848 B |        1.00 |
| Encode_V1      |  256 B |   120.74 ns |  0.29 |     712 B |        0.25 |
| Encode_V1_Span |  256 B |    41.81 ns |  0.10 |       0 B |        0.00 |
| Decode_V0      |  256 B |   926.78 ns |  2.24 |    2416 B |        0.85 |
| Decode_V1      |  256 B |   124.65 ns |  0.30 |     280 B |        0.10 |
| Encode_V0      | 1024 B | 1,077.66 ns |  1.00 |   11040 B |        1.00 |
| Encode_V1      | 1024 B |   322.63 ns |  0.30 |    2760 B |        0.25 |
| Encode_V1_Span | 1024 B |   120.10 ns |  0.11 |       0 B |        0.00 |
| Decode_V0      | 1024 B | 3,528.84 ns |  3.28 |    9328 B |        0.84 |
| Decode_V1      | 1024 B |   323.99 ns |  0.30 |    1048 B |        0.09 |

(Ratios are relative to `Encode_V0` of the same size, the benchmark baseline. The out-of-process `Default` toolchain
rows from the same run agree within noise: e.g. 1 KiB encode 1,148.74 ns → 299.98 ns, decode 3,474.21 ns → 326.47 ns.)

### Conclusion

* **Encode: hypothesis confirmed.** V1 is 3.2–3.4× faster at every size and allocates exactly the result string
  (alloc ratio 0.25: V0's three extra intermediate strings are gone). The span overload is 6.7–9.9× faster and
  allocation-free.
* **Decode: hypothesis wrong in magnitude — the gain is larger, not smaller.** V1 is 4.1× (32 B), 7.4× (256 B) and
  10.9× (1 KiB) faster and allocates only the output array (V0 allocates the re-padded string, the two `Replace`
  copies and the output). The gap grows with size because V0's `Replace`/re-pad passes and `Convert.FromBase64String`
  are scalar while `Base64Url.DecodeFromChars` is vectorised.
* **V1 ships** (`Base64UrlCodec`); V0 stays archived in `Tedd.Quicly.Archive` for this comparison. In absolute terms a
  full ACME order spends well under 50 µs in base64url either way, so the choice is about allocations and clarity
  (one call, no hand-rolled alphabet fix-ups), not wall-clock time; the ACME client is otherwise not benchmarked.
