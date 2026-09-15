# Replication — DeltaCodec, BitWriter/BitReader, quantization

Method per ADR 0007: simple correct V0 → hypothesis → V1 → measure → keep the winner, archive the loser in
`benchmarks/Tedd.Quicly.Archive/Replication` (namespace `Tedd.Quicly.Archive.Replication`, class suffixed with its
version) so every comparison below stays runnable.

## Measurement setup

* Hardware: AMD Ryzen 9 5950X (16C/32T, AVX2 + BMI2, `VectorSize=256`), Windows 11 Pro. Other agents were building and testing on the same machine during the
  runs, so ShortRun error bars are wide; differences under ~10 % are treated as noise.
* SDK 11.0.100-preview.7, host runtime .NET 10 (`-f net10.0`). BenchmarkDotNet 0.15.8, `Job.ShortRun`
  (3 warm-up + 3 measured iterations, 1 launch) with `MemoryDiagnoser`. Every class carries
  `[Config(typeof(InProcessShortRunConfig))]` and `Program` adds the repository-wide out-of-process ShortRun job,
  so each case is measured twice (in-process and out-of-process); both numbers are reported as `a / b`.
* Command: `dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*DeltaCodecBench*' '*BitStreamBench*' '*QuantizationBench*'`
* Every benchmark allocates 0 B (the `Allocated` column is `-` throughout and is omitted); the unit tests in
  `tests/Tedd.Quicly.Replication.Tests/AllocationTests.cs` assert `GC.GetAllocatedBytesForCurrentThread()` does
  not move across thousands of steady-state iterations of every hot path.

## DeltaCodec

Benchmark: `DeltaCodecBench`. Baseline = `BenchmarkData.GameLike` (32-byte entity records: ids, drifting float
positions, small-alphabet bytes, zero padding) of 1 KiB and 16 KiB; current = the baseline with 5 % or 50 % of its
bytes changed at uniformly random positions. Random scattered single-byte changes are the worst case for run
finding (the most ops per byte); real snapshots change in clustered 4–12-byte fields and produce fewer, longer ops.
Delta sizes: 1 KiB/5 % → 153 B, 1 KiB/50 % → 902 B, 16 KiB/5 % → 2 363 B, 16 KiB/50 % → 14 300 B
(V0, V1 and V2 produce byte-identical output — the unit tests compare every SIMD width of the shipped codec
against a reference encoder, and the benchmark setup prints the V0 size next to the shipped one).

### V0 — simple and correct (archived as `DeltaCodecV0`)

One byte at a time: `X(i) = current[i] ^ (i < overlap ? baseline[i] : 0)` with bounds checks, a loop that skips
zero X bytes, a loop that extends the literal until three consecutive zero X bytes, `VarInt.Write` into a slice for
both varints, and a byte loop writing the XOR literals. Decode reads both varints with `VarInt.TryReadMinimal`
and copies skipped baseline bytes and XOR literals one byte at a time.

### Hypothesis for V1

1. *Scanning:* the encoder's time goes into deciding, per byte, whether X is zero. One `Vector256` load + XOR +
   compare + `ExtractMostSignificantBits` answers that for 32 bytes, giving a 32-bit "zero" mask. Unchanged runs are
   then skipped 32 bytes per step (`~mask`, `tzcnt`), and the end of a literal (the first 3-zero run) is found
   with integer ops on a 64-bit mask built from two adjacent chunks: `m & (m >> 1) & (m >> 2)`. The chunks that
   straddle the baseline end or the input end use a scalar mask, and positions past the end count as zero, which
   removes every end-of-input special case from the vector loop. Expected: several times faster at 5 % (long
   unchanged runs), smaller gain at 50 % (short runs, per-op overhead dominates).
2. *XOR / copy:* literals are XORed 32 (then 16) bytes at a time; skipped baseline ranges are one `Span.CopyTo`
   (vectorised `memmove`), the implicit zero-extension one `Clear`. Varints below 64 (almost every skip and
   count) take a one-byte fast path on both sides.
3. The SIMD width is a generic struct parameter (`ISimdLevel`), so the JIT compiles one specialised body per width
   with no runtime branching; the public entry points pick the widest accelerated one (a JIT-time constant).

### Measurement of V1 (net10.0, run 1)

Mean, in-process / out-of-process. "V1 Vector128" is the same code forced to the 128-bit path.

| Operation | Size / changed | V0                    | V1 (Vector256)        | V1 Vector128          |
|-----------|----------------|----------------------:|----------------------:|----------------------:|
| Encode    | 1 KiB / 5 %    | 1 332 / 1 789 ns      |   841 /   856 ns      | 1 130 / 1 105 ns      |
| Encode    | 1 KiB / 50 %   | 2 987 / 2 026 ns      | 1 325 / 1 331 ns      | 1 677 / 1 365 ns      |
| Encode    | 16 KiB / 5 %   | 25.5 / 33.7 µs        | 11.9 / 12.1 µs        | 17.6 / 15.9 µs        |
| Encode    | 16 KiB / 50 %  | 61.6 / 47.4 µs        | 17.9 / 20.9 µs        | 28.1 / 25.3 µs        |
| Decode    | 1 KiB / 5 %    | 1 362 / 1 074 ns      |   582 /   656 ns      |   637 /   600 ns      |
| Decode    | 1 KiB / 50 %   | 1 545 / 1 629 ns      | 1 293 / 1 157 ns      | 1 063 / 1 243 ns      |
| Decode    | 16 KiB / 5 %   | 23.8 / 23.2 µs        |  6.6 /  8.2 µs        | 11.4 /  6.3 µs        |
| Decode    | 16 KiB / 50 %  | 30.5 / 34.0 µs        | 22.7 / 18.5 µs        | 20.7 / 17.1 µs        |

V1 confirmed the hypothesis where runs are long (encode 2.1–3.4×, decode at 5 % 2–3.6×), but the gains at
50 % and at 1 KiB were smaller than expected, and decode at 50 % gained only 1.2–1.8×. Per op, V1 still costs
about 16 ns on top of the scan (1 KiB / 5 %: ~51 ops in 841 ns): every op recomputes three 32-byte masks (one to
find the change, two to find the end of the literal), and on the decode side every skip is a `Span.CopyTo` call
(slice + `memmove` dispatch) and every short literal a scalar loop.

### Hypothesis for V2

1. *Encode:* keep one sliding 64-position window of zero bits (`bit k ⇔ X[chunk + k] == 0`) and advance it 32
   positions at a time. Both searches become shifts and `tzcnt` on that window, so each chunk mask is computed
   exactly once per encode instead of about three times per op. The end-of-literal search may miss a run that
   crosses the window end (bits shifted in are 0) but can never invent one, so it just advances and retries.
   Skip and count below 64 are written as two plain byte stores.
2. *Decode:* a skip of at most 32 (16) bytes is one `Vector256` (`Vector128`) load/store of baseline bytes, and a
   literal of at most 16 bytes is one 16-byte XOR store. Both may write past their op (never past
   `CurrentLength`): every later position is rewritten by a later op or the final copy, so the over-write is
   harmless. The over-copied skip bytes are baseline bytes, so it is also correct in place; the XOR spill is not,
   so it is disabled when the destination is the baseline. Expected: most of the 50 % decode gap closes.

### Measurement: V0 vs V1 vs V2 (net10.0, run 2)

All four implementations measured in one session. Mean, in-process / out-of-process; "V2" is the shipped
`DeltaCodec` (Vector256 on this CPU), "V2 Vector128" the same code forced to the 128-bit path.

| Operation | Size / changed | V0                | V1                | V2 (shipped)      | V2 Vector128      |
|-----------|----------------|------------------:|------------------:|------------------:|------------------:|
| Encode    | 1 KiB / 5 %    | 1 291 / 1 203 ns  |   903 /   876 ns  |   453 /   429 ns  |   464 /   409 ns  |
| Encode    | 1 KiB / 50 %   | 2 274 / 2 018 ns  | 1 276 / 1 164 ns  | 1 007 /   724 ns  |   985 /   759 ns  |
| Encode    | 16 KiB / 5 %   | 30.6 / 32.3 µs    | 14.6 / 11.9 µs    | 10.5 /  9.6 µs    | 11.4 /  8.7 µs    |
| Encode    | 16 KiB / 50 %  | 43.2 / 43.4 µs    | 19.5 / 20.5 µs    | 12.5 / 14.6 µs    | 13.5 / 16.0 µs    |
| Decode    | 1 KiB / 5 %    | 1 608 /   894 ns  |   432 /   395 ns  |   238 /   309 ns  |   275 /   295 ns  |
| Decode    | 1 KiB / 50 %   | 1 796 / 1 207 ns  | 1 112 /   970 ns  |   381 /   429 ns  |   467 /   449 ns  |
| Decode    | 16 KiB / 5 %   | 19.0 / 18.5 µs    |  6.4 /  5.4 µs    |  3.9 /  3.3 µs    |  4.3 /  3.7 µs    |
| Decode    | 16 KiB / 50 %  | 20.6 / 19.6 µs    | 12.5 / 10.5 µs    |  6.0 /  4.8 µs    |  6.9 /  4.7 µs    |

Error bars (StdDev) were below 5 % for most rows; the exceptions (V0 decode 1 KiB in-process ±12 %, V1 decode
1 KiB / 50 % ±22–27 %, V2 encode 1 KiB / 50 % in-process ±15 %) do not change any ordering. Run-1 and run-2
numbers for the same code differ by up to ~30 % (machine shared with other builds), so only same-session ratios
are compared.

### Decision

* **V1 over V0: confirmed.** Encode 1.4–2.6×, decode 1.6–3.7× faster in the same session; largest where
  unchanged runs are long, as predicted.
* **V2 over V1: confirmed, and it ships as `DeltaCodec`.** Encode 1.3–2.1× faster than V1 (1 KiB / 5 %: ~450 ns vs
  ~890 ns — the per-op mask recomputation was indeed most of V1's cost), decode 1.6–2.9× faster (1 KiB / 50 %:
  ~400 ns vs ~1 040 ns — the per-op `CopyTo` call and the scalar XOR tail were the gap). Against V0 the shipped
  codec is 2.3–3.5× faster at encoding and 3.2–6.8× faster at decoding. V1 is archived as `DeltaCodecV1`, V0 as
  `DeltaCodecV0`.
* **Vector256 vs Vector128:** within noise at 1 KiB; Vector256 is ahead at 16 KiB in-process (5–15 %), the
  out-of-process numbers go either way. Both paths ship: the public entry points use Vector256 where it is
  hardware-accelerated and Vector128 (SSE2 / ARM64 NEON) otherwise, with a scalar fallback; the choice is a
  JIT-time constant.
* For scale: a 60 Hz server sending a 16 KiB world snapshot with 5 % of its bytes changed spends ~10 µs per peer
  per tick encoding it; a client decodes it in ~3–4 µs.

## BitWriter / BitReader

Benchmark: `BitStreamBench` — 256 fields of pseudo-random widths 1..32 bits per invocation (a quantized entity
record's shape), and 256 ZigZag bit-varints of values in ±5 000.

Mean per invocation, in-process / out-of-process (net10.0, 0 B allocated):

| Benchmark     | 256 operations        | per operation |
|---------------|----------------------:|--------------:|
| WriteFields   |   483.7 /   634.7 ns  | 1.9 – 2.5 ns  |
| ReadFields    |   683.4 /   591.9 ns  | 2.3 – 2.7 ns  |
| WriteVarInts  | 1 345.8 /   868.5 ns  | 3.4 – 5.3 ns  |
| ReadVarInts   | 1 572.6 / 1 177.9 ns  | 4.6 – 6.1 ns  |

(ReadVarInts in-process had ±26 % StdDev; the out-of-process number is the steadier one.)

The writer gathers bits in a 64-bit accumulator and stores 8 bytes per spill (`BZHI` masks the value where BMI2
is available); the reader does one unaligned 8-byte load per field plus a shift (a ninth byte only for a 64-bit
field that starts mid-byte). No alternative was benchmarked: at ~2 ns per field both are within a few cycles of
the load/shift/store floor, far below any other per-message cost in the pipeline.

## Quantization

Benchmark: `QuantizationBench` — 256 values per invocation: smallest-three quaternions at 10 bits per component
(32 bits total), octahedral unit vectors at 12 bits per component, range-quantized floats at 16 bits.

Mean per invocation, in-process / out-of-process (net10.0, 0 B allocated):

| Benchmark             | 256 values            | per value     |
|-----------------------|----------------------:|--------------:|
| QuantizeQuaternion    | 3 250.1 / 3 221.0 ns  | 12.6 ns       |
| DequantizeQuaternion  | 2 154.4 / 3 150.0 ns  | 8.4 – 12.3 ns |
| QuantizeUnitVector    | 1 820.4 / 1 852.6 ns  | 7.1 – 7.2 ns  |
| DequantizeUnitVector  | 1 923.2 / 1 924.2 ns  | 7.5 ns        |
| QuantizeFloat         |   394.1 /   378.4 ns  | 1.5 ns        |

The quaternion encoder's cost is dominated by the normalising square root and division plus three
round-to-nearest conversions (latency-bound, ~50 cycles per value); a 60 Hz server encoding 1 000 rotations per
tick spends ~13 µs per tick on it, so no further work was done.

Both encoders are branch-light (conditional selects for the largest-component index and the octahedral fold, no
data-dependent loops). The documented error bounds (`Quantization` XML docs) are proven by dense sweeps in
`QuantizationTests`: measured maxima are 2.12 / M rad for unit vectors (bound 2.25 / M, all widths) and 2.36 / M rad
for quaternions (bound 2.5 / M, 4 bits and up), with M = 2^(bits−1) − 1.
