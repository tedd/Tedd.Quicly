# Framing — datagram headers, packed containers, stream parser

Method per ADR 0007: simple correct V0 → hypothesis → V1 → measure → keep the winner, archive the loser in
`benchmarks/Tedd.Quicly.Archive/Framing` (namespace `Tedd.Quicly.Archive.Framing`) so the comparison stays runnable.

## Measurement setup

* Hardware: AMD Ryzen 9 5950X (16C/32T), Windows 11 Pro (10.0.26200).
* SDK 11.0.100-preview.7; host runtime .NET 10.0.12 (x64 RyuJIT, x86-64-v3). BenchmarkDotNet 0.15.8, `Job.ShortRun`
  (3 warm-up + 3 measured iterations, 1 launch), `MemoryDiagnoser`.
* Command: `dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*.Framing.*'`
* Every class carries `[Config(typeof(InProcessShortRunConfig))]` and `Program` adds the global ShortRun job, so each
  case runs twice: `Default` (out-of-process on .NET 10) and `InProcessEmitToolchain`. Both are reported; they agree
  within ShortRun noise. ShortRun error bars are wide — differences under ~10 % are treated as noise.
* Allocation: 0 B everywhere in the `Default` toolchain. The in-process toolchain reports 14 B/op on the 1 ms
  stream-parser invocations only; that is harness overhead (the unit test
  `FramingZeroAllocationTests.Stream_Parser_Over_Segments` measures 0 B with `GC.GetAllocatedBytesForCurrentThread`).

## 1. Datagram header length (ADR 0007 loop)

`DatagramFraming.GetHeaderLength` runs for every datagram message on the send path (entry sizing, container
packing) and inside `WriteHeader`. Benchmark: `DatagramHeaderLengthBench`, 256 (channel, header) pairs per
invocation. `keyed32` = every message on one UnreliableSequenced keyed channel with 1–2 byte keys (the realistic
case: a flush packs runs of one channel); `mixed` = random channel among six shapes (plain, 16-bit, keyed 32-bit,
fragmenting, ReliableLatest+LZ4, two-byte id with everything) and keys of every varint length.

### V0 — simple and correct (archived: `DatagramHeaderLengthV0`)

Recomputes everything from the channel's public flags on every call: `Id ≤ 63 ? 1 : 2`, then one `if` per optional
field (`HasSequence` → 2/4, `Keyed` → `VarInt.GetLength(key)`, `Fragmentation` → 1/2, compression →
`VarInt.GetLength(raw)`).

### Hypothesis for V1 (archived: `DatagramHeaderLengthV1`)

Table driven and branch free: precompute per channel the constant part (id + sequence + FragCount) and a 0/−1 mask
per optional field; compute varint lengths branch-free as `1 << ((v > 63) + (v > 16383) + (v > 2^30−1))`. Expected:
no mispredictions on mixed shapes, and fewer instructions than V0's recomputation.

### Measurement (mean ns per 256 lengths; Ratio vs V0)

| Set | Toolchain | V0 branchy | V1 table-driven | Current: precomputed constant + branches |
|---|---|---:|---:|---:|
| keyed32 | Default | 889.7 (1.00) | 1 625.2 (1.84) | **679.2 (0.77)** |
| keyed32 | InProcess | 921.3 (1.00) | 1 377.6 (1.50) | **648.8 (0.71)** |
| mixed | Default | 1 160.4 (1.00) | 1 520.5 (1.33) | **924.2 (0.81)** |
| mixed | InProcess | 1 559.9 (1.00) | 1 386.9 (0.89) | **1 077.9 (0.69)** |

(First run, V0 vs V1 only: keyed32 854.5 vs 1 441.2 / 889.2 vs 1 309.3 ns; mixed 1 439.7 vs 1 201.8 /
1 494.2 vs 1 515.0 ns.)

### Decision

The hypothesis is refuted. The per-field branches are perfectly predicted on the realistic single-shape stream and
still predicted well on the mixed set (the branch pattern depends on the channel, and six channels fit the
predictor's history easily). V1 pays three compares plus setcc/adds per varint on every call; the if-chain
`VarInt.GetLength` exits after one compare for small keys. V0's real cost was recomputing the id length and the
sequence width, which the precomputed `FixedHeaderBytesWithoutKey` removes. **Shipped:** precomputed constant +
one branch per optional field ("Current"), 23–31 % faster than V0 and 1.5–2.4× faster than V1 on the realistic set.
Both V0 and V1 are archived. The same change took `WriteHeader` (256 headers) from 2.88 → 2.15 µs (keyed32) and
1.99 → 1.14 µs (unkeyed16).

## 2. Datagram write/parse (`DatagramFramingBench`)

256 headers per invocation, 24-byte payloads, `unkeyed16` = UnreliableSequenced 16-bit (3-byte header), `keyed32` =
32-bit sequence + 1–2 byte key (5–6 byte header). Mean µs per 256 (ns per header in parentheses), shipped code:

| Shape | Toolchain | WriteHeader | TryParse | WriteAndParse |
|---|---|---:|---:|---:|
| unkeyed16 | Default | 1.135 (4.4) | 2.465 (9.6) | 3.189 (12.5) |
| unkeyed16 | InProcess | 1.342 (5.2) | 2.145 (8.4) | 3.556 (13.9) |
| keyed32 | Default | 2.155 (8.4) | 3.045 (11.9) | 5.071 (19.8) |
| keyed32 | InProcess | 2.154 (8.4) | 3.078 (12.0) | 5.047 (19.7) |

`TryParse` includes the channel-table lookup, full validation (minimal varints, size limits) and the payload-size
checks; a keyed 32-bit header parses in ≈ 12 ns.

## 3. Packed container (`PackedContainerBench`)

32 keyed messages (32-bit sequence, 1–2 byte key, 16-byte payload), ≈ 750-byte container with a tick:

| Operation | Default | InProcess | per message |
|---|---:|---:|---:|
| `TryParse` (full validation) + iterate | 275.1 ns | 286.5 ns | ≈ 8.8 ns |
| … + `DatagramFraming.TryParse` of every inner message | 568.5 ns | 546.2 ns | ≈ 17.4 ns |
| Build in place (`TryReserve` + `WriteHeader` + 16-byte fill) | 898.8 ns | 839.1 ns | ≈ 27 ns |

Validation walks the length varints once before yielding (so a malformed container is dropped whole, never half
delivered); iteration then re-reads the already-validated varints without checks.

## 4. Stream frame parser throughput (`StreamFrameParserBench`)

≈ 1 MiB ReliableOrdered stream of 25 575 frames (1-byte length + 40-byte payload), fed as one span or as receive
segments; events are consumed exactly as the session layer will (`PayloadChunk` summed, `MessageEnd` counted).

| Segment size | Default | InProcess | Throughput (Default) | per message |
|---:|---:|---:|---:|---:|
| 1 350 B (≈ one QUIC packet; headers straddle segments) | 981.2 µs | 1 010.5 µs | ≈ 1.07 GB/s | ≈ 38 ns |
| 16 KiB | 916.1 µs | 1 257.8 µs | ≈ 1.14 GB/s | ≈ 36 ns |
| 1 MiB (one span) | 967.1 µs | 1 256.3 µs | ≈ 1.08 GB/s | ≈ 38 ns |

Segmentation does not matter: the straddling-header slow path (copy ≤ 32 bytes into the internal buffer and
re-parse) triggers on ≈ 3 % of frames at 1 350-byte segments and costs nothing measurable. Per message the parser
returns three events (`MessageStart`, `PayloadChunk`, `MessageEnd`); ≈ 12 ns per event including the payload slice.
No further optimisation is planned until the session-layer E2E benchmark shows the parser on the profile.

## 5. Re-run after the review fixes (2026-09-15)

Hot paths touched by the review fixes: `DatagramFraming.WriteHeader` gained a stream-only channel guard (one
predictable branch on a precomputed bool), and `StreamFrameParser.Read` now takes the `ChannelTable` as a parameter
instead of holding it in a field (the parser is reference-free). Same machine and command, filtered to the three
affected classes; the machine was shared with other builds, so ShortRun error bars are wider than in §§ 2–4.

| Benchmark | Case | Default | InProcess | Before (Default) |
|---|---|---:|---:|---:|
| `DatagramFramingBench.WriteHeader` (256) | unkeyed16 | 0.751 µs | 0.849 µs | 1.135 µs |
| | keyed32 | 1.303 µs | 1.286 µs | 2.155 µs |
| `DatagramFramingBench.TryParse` (256) | unkeyed16 | 1.927 µs | 1.468 µs | 2.465 µs |
| | keyed32 | 1.935 µs | 1.954 µs | 3.045 µs |
| `DatagramFramingBench.WriteAndParse` (256) | unkeyed16 | 2.944 µs | 2.368 µs | 3.189 µs |
| | keyed32 | 3.814 µs | 3.874 µs | 5.071 µs |
| `PackedContainerBench` (32 messages) | Iterate | 260.5 ns | 252.3 ns | 275.1 ns |
| | IterateAndParse | 429.1 ns | 430.4 ns | 568.5 ns |
| | Build | 502.0 ns | 583.0 ns | 898.8 ns |
| `StreamFrameParserBench` (≈ 1 MiB) | 1 350 B segments | 1 400.5 µs | 745.4 µs | 981.2 µs |
| | 16 KiB segments | 604.8 µs | 770.2 µs | 916.1 µs |
| | 1 MiB (one span) | 626.0 µs | 779.9 µs | 967.1 µs |

No regression: the guard is free (the datagram numbers are at or below the previous run) and passing the table as an
argument costs nothing measurable. The single slower cell (1 350 B segments, Default, 1.40 ms) is not reproduced by the
in-process run of the same case (0.75 ms) and is attributed to load from concurrent builds. Allocation is 0 B in the
`Default` toolchain; the in-process toolchain reports 7 B/op on the stream parser, the harness overhead noted in the
setup section.
