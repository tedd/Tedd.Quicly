# Primitives — VarInt, SerialNumber, XxHash64, Lz4Block

Method per ADR 0007: simple correct V0 → hypothesis → V1 → measure → keep the winner, archive the loser in
`benchmarks/Tedd.Quicly.Archive/Primitives` (namespace `Tedd.Quicly.Archive.Primitives`, class suffixed with
its version) so every comparison below stays runnable.

## Measurement setup

* Hardware: AMD Ryzen 9 5950X (16C/32T), Windows 11 Pro.
* SDK 11.0.100-preview.7; runtime as noted per table. BenchmarkDotNet 0.15.8, `Job.ShortRun`
  (3 warm-up + 3 measured iterations, 1 launch) on the `InProcessEmitToolchain` with `MemoryDiagnoser`
  (BDN 0.15.8 cannot spawn `net11.0` child processes; the in-process job applies to whichever TFM hosts it).
* Command: `dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*Primitives*'`
* Every benchmark allocates 0 B (`Allocated` column omitted from the tables below where it was `-` throughout).
* ShortRun has wide error bars; differences under ~5 % are treated as noise and decided by code simplicity.

## VarInt (RFC 9000 §16)

Benchmark: `VarIntBench` — 256 values per invocation, `Set` = all values of one encoded length (1/2/4/8) or a
pseudo-random mix of the four lengths (defeats the branch predictor on the length selection, which is the
realistic case for a frame parser). Read benchmarks walk a buffer of the 256 encodings.

### V0 — simple and correct

Write: `if (v <= 63) … else if (v <= 16383) … else if …` chain, each branch doing one bounds check and one
big-endian store of the right width. Read: `PeekLength(first)`, bounds check, then a `switch` on the length
with a 1/2/4/8-byte big-endian load and mask.

### Hypothesis for V1

1. *Write:* the four-way compare chain mispredicts on mixed input; deriving the length code from
   `lzcnt(value | 1)` through a 65-entry lookup table, then forming the whole encoding in a register
   (`(value << shift) | (code << 62)`, byte-swapped) with a single width-dispatching store should be
   branch-free up to the final store and faster on the mixed set.
2. *Read:* when at least 8 bytes are readable (the common case inside a datagram), one unaligned 8-byte
   big-endian load gives the prefix in the top two bits and the value in the top `length` bytes:
   `value = (raw & MaxValue) >> (64 - 8 * length)`. No switch, no data-dependent branch; the short path
   (fewer than 8 bytes left) keeps the V0 switch.

### Measurement (net10.0)

Mean ns per invocation (256 values). "Current" is the shipped `VarInt`.

**Write (`TryWrite` into a 16-byte span)**

| Set   | V0 if-chain | V1 lzcnt + LUT | Current (if-chain, `BinaryPrimitives` stores) |
|-------|------------:|---------------:|----------------------------------------------:|
| 1     |       303.0 |          787.9 |                                         305.7 |
| 2     |       618.3 |        1 089.4 |                                         577.2 |
| 4     |       783.1 |        1 135.6 |                                         788.5 |
| 8     |       835.9 |        1 070.3 |                                         509.7 |
| mixed |       411.0 |          723.1 |                                         441.8 |

An intermediate form of the if-chain that hoisted `ref byte dst = ref GetReference(destination)` and stored
with `Unsafe.WriteUnaligned` measured consistently slower than V0 in two runs (8-byte set 702 / 1 179 vs
515 / 843; mixed 579 / 877 vs 412 / 668); returning to plain `BinaryPrimitives.WriteUIntNNBigEndian(span, …)`
stores (whose length checks the JIT folds into ours) removed the gap, so that is what ships.

**GetLength** (two runs, first / second, to show ShortRun variance)

| Set   | V0 if-chain | V1 lzcnt + LUT | Current (if-chain) |
|-------|------------:|---------------:|-------------------:|
| 1     |   170 / 157 |      425 / 412 |          166 / 158 |
| 2     |   514 / 376 |      304 / 402 |          351 / 643 |
| 4     |   749 / 765 |      281 / 432 |          539 / 766 |
| 8     |   544 / 741 |      370 / 414 |          509 / 960 |
| mixed |   576 / 652 |      323 / 438 |          692 / 729 |

**Read** (walk 256 encodings; `TryRead` span and pointer overloads, plus `TryReadMinimal`)

| Set   | V0 switch (span) | Current wide load (span) | Current `TryReadMinimal` | V0 (pointer) | Current (pointer) |
|-------|-----------------:|-------------------------:|-------------------------:|-------------:|------------------:|
| 1     |          1 515.3 |                    805.2 |                    908.1 |      1 226.2 |             742.6 |
| 2     |          1 226.6 |                    782.0 |                    891.5 |      1 142.0 |             706.7 |
| 4     |          1 194.9 |                    779.7 |                    792.6 |      1 189.7 |             726.4 |
| 8     |          1 289.4 |                    787.7 |                    805.0 |      1 208.9 |             726.5 |
| mixed |          1 930.4 |                    789.0 |                    934.9 |      1 416.6 |             758.1 |

(V1's read is the same wide-load code as Current and measured within noise of it: 810.6 / 803.2 / 793.5 /
784.6 / 791.4 ns.)

### Decision

* **Read: hypothesis confirmed — keep the wide load.** 1.5–2.4× faster than the switch, and flat across
  lengths (≈ 3.1 ns per varint including loop overhead); the mixed set, which mispredicts the switch, gains
  the most (1 930 → 789 ns). The pointer overload is the same code and ~5 % faster still (no span
  construction).
* **Write: hypothesis rejected — keep the if-chain.** The lzcnt + lookup-table + register-formed encoding is
  1.3–2.6× *slower* on every set: it always does the 64-bit byte swap and a width-dispatching switch on the
  store, whereas the compare chain predicts well (values of one length dominate real traffic) and each branch
  is a single narrow store. V1 is archived as `VarIntV1`; V0's write form is what ships.
* **GetLength alone:** the lzcnt + LUT form is 1.4–2× faster on the multi-byte and mixed sets but 2.6× slower
  for 1-byte values, which dominate protocol headers (channel ids, small lengths, keys). Kept the if-chain
  for consistency with `TryWrite`; revisit only if profiling shows `GetLength` on large mixed values matters.
* `TryReadMinimal` costs 1–18 % over `TryRead` (one shift, one OR, one compare); parsers use it everywhere
  PROTOCOL.md requires minimal encodings, and it is still ~2× faster than the V0 lenient read.

`TryReadMinimal` (PROTOCOL.md: receivers MUST reject non-minimal encodings) is the wide-load read plus one
branch-free check: an encoding of length L is minimal iff `value >> (4L - 2)` is non-zero, or L = 1. Its cost
over `TryRead` is in the table above.

## XxHash64

Benchmark: `XxHash64Bench` — 16 B / 1 KiB / 64 KiB random input, ours vs `System.IO.Hashing.XxHash64.HashToUInt64`.
There is a single version: the classic XXH64 algorithm has one canonical structure (4 lanes of 8 bytes,
merge, tail of 8/4/1-byte steps, avalanche), implemented over `Unsafe.ReadUnaligned` with no allocation.
The reference package is the correctness oracle (tests cover every length 0..300, random lengths to 70 000
and random seeds) and the performance yardstick.

### Measurement (net10.0)

| Size   | `System.IO.Hashing.XxHash64` | `Tedd.Quicly.Core.Primitives.XxHash64` | Ratio |
|--------|-----------------------------:|---------------------------------------:|------:|
| 16 B   |                     14.44 ns |                               13.33 ns |  0.93 |
| 1 KiB  |                    237.42 ns |                              147.99 ns |  0.63 |
| 64 KiB |                 15 799.45 ns |                             5 240.21 ns |  0.33 |

64 KiB at 5.24 µs is ≈ 12.5 GB/s, i.e. the four-lane loop runs at the algorithm's natural rate on this core;
the framework's static `HashToUInt64` goes through its incremental state object and pays for that on long
inputs. Both are 0 B allocated.

No optimisation loop was run: the implementation is within noise of the framework's own (which is the same
scalar algorithm; XXH64 does not vectorise), so there is no hypothesis worth testing.

## Lz4Block

Benchmark: `Lz4Bench` — game-like data (32-byte entity records: incrementing ids, drifting floats,
small-alphabet flags, zero padding; 1 KiB and 64 KiB), compress with a 4096-entry (16 KiB) scratch table
and decompress; `K4os.Compression.LZ4` (`L00_FAST`) as the reference on the same input. Compressed sizes are
printed by the setup: at 1 KiB ours = 813 B and K4os = 813 B (identical output size), at 64 KiB see below.

### V0 — simple and correct

Reference LZ4 greedy parse (skip-trigger acceleration, `MFLIMIT`/`LASTLITERALS` end rules) with three
simplifications: the hash table is cleared with `Span.Clear()` at the start of every call, matches are
extended one byte at a time, and the decoder copies literals with `CopyBlockUnaligned` but every match
byte-by-byte.

### Hypothesis for V1

Clearing the scratch table costs 16 KiB of stores per call — a large fixed cost for 1 KiB messages. If every
candidate read from the table is validated before use (`0 <= ip - 1 - match <= min(ip - 1, 65534)`, one
unsigned compare) and then verified by comparing the 4 bytes at the candidate, stale entries from earlier
calls (or arbitrary garbage) can at worst cost a missed match, never a wrong one. So: drop the clear.
Expected: a large win at 1 KiB, small at 64 KiB.

### Hypothesis for V2

Match extension and match copy are byte loops. (a) Compare 8 bytes at a time and locate the first
difference with `tzcnt` (the reference `LZ4_count`). (b) In the decoder, copy short literal runs and short
non-overlapping matches as two 8-byte loads/stores when there is 16 bytes of slack, and copy overlapping
matches in 8-byte strides once the first 16 bytes have been primed (the stride is rounded down to a multiple
of the offset so every 8-byte read touches only bytes already written). Expected: faster compression of
repetitive data (long matches) and a noticeably faster decoder.

### Hypothesis for V3 (decoder only)

After V2 the decoder was still ~1.5× slower than K4os. V2 calls `CopyBlockUnaligned` (a memmove) for every
literal run or match longer than 16 bytes and primes short-offset overlaps byte by byte; the reference decoder
instead uses *wild copies* — fixed 8/16-byte chunks that may overshoot the logical end — whenever there is
slack, and handles offsets 1..7 with a four-byte-then-four-byte step from an adjusted source
(`inc32table`/`dec64table`) so the rest of the match copies in 8-byte chunks. V3 adopts that: while at least
16 bytes of slack remain after the copy in both spans, literals and offset ≥ 16 matches copy in 16-byte
chunks, offsets 8..15 in 8-byte chunks and offsets 1..7 via the table trick; the last 16 bytes of the
destination keep V2's exact-length paths. Every overshoot stays inside the destination span and every read
of the destination stays at or below the bytes already written (the source trails the output by ≥ 8 bytes).
Expected: the decoder closes most of the gap to K4os; the compressor is untouched.

### Measurement (net10.0)

Compressed sizes on the benchmark input: 1 KiB → ours 813 B, K4os 813 B; 64 KiB → ours 49 057 B,
K4os 48 186 B (K4os's fast level uses a 16 K-entry table by default; with `Compress_Current_16384` ours
matches its ratio but see the cache note below).

**Compress** (mean; three runs are shown for the versions that were present in each: run 1 / run 2 / final)

| Size   | K4os `L00_FAST`         | V0 (clear table)        | V1 (no clear)           | V2 = Current (8-byte count) | V3 (V2 compressor)  |
|--------|------------------------:|------------------------:|------------------------:|----------------------------:|--------------------:|
| 1 KiB  | 3 673 / 3 718 / 4 087 ns | 2 726 / 3 027 / 3 081 ns | 2 588 / 2 586 / 2 547 ns | 2 164 / 2 312 / 2 012 ns     | 2 207 ns            |
| 64 KiB | 191.7 / 191.8 / 197.9 µs | 196.0 / 193.9 / 189.5 µs | 171.6 / 196.7 / 198.5 µs | 167.6 / 184.8 / 193.0 µs     | 195.3 µs            |

Scratch-table size and the convenience overload (Current, final run): 1 KiB — 4 096 entries 2 012 ns,
16 384 entries 2 459 ns, `[ThreadStatic]` 4 096 entries 2 376 ns; 64 KiB — 4 096 entries 193.0 µs,
16 384 entries 251.7 µs, `[ThreadStatic]` 193.8 µs.

**Decompress** (mean; run 1 / run 2 / final)

| Size   | K4os                     | V0 (byte-wise match copy) | V1                       | V2 = Current               | V3 (wild copy)         |
|--------|-------------------------:|--------------------------:|-------------------------:|---------------------------:|-----------------------:|
| 1 KiB  | 396 / 398 / 410 ns       | 971 / 910 / 968 ns        | 794 / 914 / 884 ns       | 553 / 538 / 505 ns         | 608 / 574 ns           |
| 64 KiB | 28.7 / 29.7 / 28.8 µs    | 90.7 / 79.4 / 85.3 µs     | 84.0 / 91.5 / 92.8 µs    | 43.5 / 41.1 / 42.9 µs      | 51.0 / 51.0 µs         |

Notes: the 1–2 B "Allocated" reported for every 64 KiB *compress* row (K4os included, and never for
decompress) is an artefact of the in-process toolchain's measurement at that iteration count, not an
allocation in the code path — the unit tests assert `GC.GetAllocatedBytesForCurrentThread()` does not move
across 200 compress + decompress iterations. ShortRun error bars on the 64 KiB compress rows are ±5–10 %,
so V0/V1/V2 are indistinguishable there; the 1 KiB rows separate cleanly in all three runs.

### Decision

* **V1 (no table clear): confirmed at 1 KiB** (−5 … −15 % vs V0 in every run; the 16 KiB clear is a fixed
  cost that dominates small messages), within noise at 64 KiB as predicted. Kept; V0 archived as `Lz4BlockV0`.
* **V2 (8-byte match counting, 16-byte short copies, 8-byte overlap strides): confirmed.** Compression
  another −10 … −20 % at 1 KiB; decompression 1.5–2.1× faster than V1 at both sizes. Kept as the shipped
  `Lz4Block`; V1 archived as `Lz4BlockV1`.
* **V3 (reference-style wild-copy decoder): rejected.** 7–20 % *slower* than V2 at both sizes in two
  independent runs. The game-like data produces many 17–64-byte literal runs and matches; .NET's
  `CopyBlockUnaligned` already handles those with one or two vector moves, so replacing it with a 16-byte
  chunk loop plus two extra slack checks per sequence only added work, and the inc32/dec64 dance is no faster
  than V2's 16-byte priming for the short-offset matches (zero padding) this data has. Archived as
  `Lz4BlockV3` so the comparison stays runnable. K4os remains ~1.3–1.5× faster on decompression; the
  remaining gap is its fully unchecked fast loop, which we do not want on a hostile-input path
  (ADR 0009: every parser is bounded).
* **Scratch table: 4 096 entries (16 KiB) is the default.** 16 384 entries (64 KiB) is 20–30 % *slower* on
  this CPU (32 KiB L1D: the larger table thrashes it) and buys 1.8 % ratio on 64 KiB inputs; callers that
  compress large bulk chunks may still pass 16 384. The `[ThreadStatic]` convenience overload costs ~5–15 %
  at 1 KiB (thread-static access plus the null check) and nothing measurable at 64 KiB; hot paths should pass
  their own scratch span.
* **Against the reference:** compression is 25–50 % faster than K4os at 1 KiB (identical output size) and
  equal at 64 KiB; decompression is 1.3–1.5× slower, bounded-input safety included.

### In-place decode (2026-10-02, RC2-1)

`Lz4Block.TryDecompressInPlace` decodes a block in the buffer it was staged in: the compressed bytes move to the
buffer's end (one memmove), then the decode writes from the start towards them with the same parsing and bounds
checks as `Decompress`, plus one compare per copy that its write ends before the first unread source byte. It is a
separate method so the out-of-place decoder's code is unchanged. A receiver needs it so that a compressed reliable
message never waits for a second buffer (session-layer.md, "Drain and compressed messages").

Measured with a scratch probe, not BenchmarkDotNet: one process, pinned to one core, best of nine, game-like data
(16 of every 64 bytes random), net10.0, three runs on a machine shared with other sessions' stress loops; separate
runs, so read only the order of magnitude (ADR 0007).

| Raw size | Out of place (`DecompressExact`) | In place (move + decode) |
|---|---|---|
| 1 KiB | 646–677 ns | 673–727 ns |
| 16 KiB | 9.3–9.7 µs | 9.7–10.3 µs |
| 64 KiB | 29–44 µs | 33–47 µs |
| 200 000 B | 87–159 µs | 95–108 µs |

**Decision: kept.** In place is 0–15 % slower than the out-of-place decode alone (the memmove of the compressed bytes
and the extra compare), and it saves the second buffer's rent and return — and, which is the point, the wait for that
buffer. Decoding still runs at well over 1 GB/s, far above the 8 MiB/s `DecodedBytesPerSecond` default.
