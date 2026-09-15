# Control protocol: codec, session tokens, auth-failure limiter

Method per ADR 0007: simple correct V0, then a hypothesis, then V1, then measure and keep the winner. The loser is
archived in `benchmarks/Tedd.Quicly.Archive/Control` (namespace `Tedd.Quicly.Archive.Control`) so the comparison
stays runnable.

## Measurement setup

* Hardware: AMD Ryzen 9 5950X (16C/32T), Windows 11 Pro 25H2.
* SDK 11.0.100-preview.7; runtime .NET 10.0.12 (X64 RyuJIT x86-64-v3). BenchmarkDotNet 0.15.8, `Job.ShortRun`
  (3 warm-up + 3 measured iterations, 1 launch), `MemoryDiagnoser`.
* Each benchmark appears twice. The `Default` row is the out-of-process ShortRun job that `Program.cs` adds for
  every class on .NET 10. The `InProcessEmit` row is the class's own `[Config(typeof(InProcessShortRunConfig))]`
  job.
* Command:
  `dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*Control*'`
  (classes `ControlCodecBench` and `SessionTokenBench`).
* Every benchmark allocates 0 B. The zero-allocation tests (`ControlZeroAllocationTests`) guard the same paths
  on both TFMs.
* Other agents were building and testing on the same machine during these runs, so ShortRun error bars are wide.
  Two runs are shown where a decision depends on them; differences under about 5 % are treated as noise.

## Codec (`ControlCodecBench`)

Benchmarks (mean ns per invocation):

* `PingPong_*`: encode a Ping, read the frame, parse it, encode the Pong, read it, parse it. This is one full RTT
  sample, in datagram and in stream form.
* `LatestAck32_Encode`: `LatestAckBatchWriter` with 32 entries plus `Finish`, into a 1 200-byte datagram.
* `LatestAck32_Decode`: `TryReadDatagram` plus `TryParse` (validates the whole batch), then enumerate all 32
  entries.
* `LatestAck32_RoundTrip`: encode followed by decode.

Entry data is game-like: channel ids 2–19 (1-byte varints), keys spread evenly over 1-, 2- and 4-byte varints,
random 32-bit versions. That makes a 219-byte batch.

### Baseline (V0 decode everywhere), run 1

| Method                | Default    | InProcessEmit |
|-----------------------|-----------:|--------------:|
| PingPong_Datagram     |   65.95    |   63.08       |
| PingPong_Stream       |   64.28    |   74.74       |
| LatestAck32_Encode    |  219.56    |  253.16       |
| LatestAck32_Decode    |  451.58    |  456.80       |
| LatestAck32_RoundTrip | 1 314.62 * | 1 267.83 *    |

\* The error bars (±938 / ±248 ns) put this outlier far outside encode + decode. Later runs measured 436–601 ns
for the same code path.

### Hypothesis for V1 (LatestAck decode)

The server sends one ack batch per peer every `AckDelay` (5 ms). With 1 000 peers that is 200 000 batches per
second, which at ~450 ns each is about 9 % of a core spent only parsing acks. V0 costs ~14 ns per entry: it makes
about six calls that are not inlined per entry. It validates through a general bounds-checked cursor
(`ControlReader.ReadChannel` / `ReadVarInt`, then `VarInt.TryReadMinimal`, then `TryReadCore`). It then decodes
again with one `VarInt.TryRead` per field. Every minimal channel id is a 1- or 2-byte varint, and so are most keys.
Decoding those two forms inline, and sending only 4- and 8-byte encodings to `VarInt`, should remove most of those
calls in both passes. The whole-batch-first semantics (a malformed batch is never partially applied) stay the same.

V1 (`LatestBatchFormat.ReadVarInt` / `ReadTrusted` in `src/Tedd.Quicly.Core/Control/LatestBatch.cs`):

* Validation: a single loop over `(span, ref position)`. The 1- and 2-byte forms are decoded inline, including
  the minimality check (a 2-byte value must be ≥ 64). A `NoInlining` slow path handles everything else and
  classifies truncated versus non-minimal input.
* Decode: the same fast path without checks (the batch is already validated), falling back to `VarInt.TryRead`
  for longer encodings.

V0 is archived as `Tedd.Quicly.Archive.Control.LatestAckDecodeV0`.

### Measurement

| Method                | Run 1 Default | Run 1 InProcessEmit | Run 2 Default | Run 2 InProcessEmit |
|-----------------------|--------------:|--------------------:|--------------:|--------------------:|
| PingPong_Datagram     |  60.73        |  52.55              |  55.91        |  55.49              |
| PingPong_Stream       |  63.73        |  62.84              |  61.05        |  78.86              |
| LatestAck32_Encode    | 240.35        | 175.32              | 241.46        | 234.02              |
| LatestAck32_Decode_V0 | 461.69        | 378.21              | 459.21        | 413.92              |
| **LatestAck32_Decode (V1)** | **230.25** | **271.35**     | **288.80**    | **336.17**          |
| LatestAck32_RoundTrip | 436.50        | 601.44              | 488.16        | 395.68              |

V1 is 1.2–2.0× faster than V0 in all four comparisons (V0 has StdDev 2–53 ns; V1 has 49–81 ns). That is well
outside ShortRun noise. Per entry it drops from ~14 ns to ~7–10 ns, including frame reading and the enumeration
loop.

**Decision:** keep V1. It is about 60 more lines in one internal class, fully covered by the existing batch tests,
the fuzz tests and the canonical re-encode oracle.

## Session tokens and the auth-failure limiter (`SessionTokenBench`)

Measured once, on the V0 codec; token code is independent of the ack decode change. Mean ns per call.

| Method                              | Default  | InProcessEmit | Notes |
|-------------------------------------|---------:|--------------:|-------|
| Token_Mint                          |   704.29 |   810.75      | 16 random bytes + HMAC-SHA256 over 37 bytes |
| Token_Inspect_Valid                 |   821.15 | 1 025.23      | HMAC + constant-time compare + replay lookup under the lock |
| Token_Validate_Replayed             |   806.06 |   977.39      | consuming path that hits the replay cache |
| Token_Validate_Tampered             |   792.34 |   888.08      | MAC mismatch (one HMAC, no lock) |
| Token_Inspect_PreviousKey           | 1 315.94 | 1 835.13      | two HMACs while the previous key is accepted |
| AuthFailures_IPv4_Check_And_Record  |    55.29 |    71.25      | keyed xxHash64 + one 8-way bucket scan, twice |
| AuthFailures_IPv6_Check_And_Record  |    66.30 |    68.00      | adds `IPAddress.TryWriteBytes` and the /64 mask |

Token cost is almost entirely `HMACSHA256.HashData`. That one-shot CNG call derives the ipad/opad key state on
every call, so a 37-byte message costs four SHA-256 compressions. A reusable keyed HMAC state would need two and
no per-call hash object, which could plausibly halve the cost. It was not pursued. Tokens are validated once per
admission, next to a full TLS handshake, so ~0.8 µs is noise at that point. A reusable state would also need
per-thread instances or a lock around the MAC. Revisit only if admission throughput is measured as a bottleneck.

## After the review fixes

Three hot-path changes came out of the review:

* Session tokens: minting and validation now lease the key set (one `Interlocked` increment, a re-check and one
  decrement around the HMAC). A rotation or `Dispose` therefore never wipes key bytes that an in-flight HMAC is
  still reading.
* Auth-failure limiter: `IsAllowed` admits untracked addresses without consulting a shared overflow bucket, and
  `RecordFailure` evicts the entry with the smallest arrival time when a bucket is full.
* `TryReadStream` rejects a `0b11`-prefixed Length on its first byte.

Same setup and command as above (both classes in one run), mean ns per call, every row 0 B allocated:

| Method                             | Default  | InProcessEmit |
|------------------------------------|---------:|--------------:|
| PingPong_Datagram                  |    56.18 |    49.33      |
| PingPong_Stream                    |    54.06 |    63.72      |
| LatestAck32_Encode                 |   150.18 |   165.57      |
| LatestAck32_Decode_V0              |   418.72 |   400.19      |
| LatestAck32_Decode (V1)            |   196.82 |   210.07      |
| LatestAck32_RoundTrip              |   395.98 |   362.35      |
| Token_Mint                         |   529.46 |   549.73      |
| Token_Inspect_Valid                |   609.26 |   664.13      |
| Token_Validate_Replayed            |   562.94 |   645.60      |
| Token_Validate_Tampered            |   562.87 |   643.74      |
| Token_Inspect_PreviousKey          | 1 266.69 | 1 542.81      |
| AuthFailures_IPv4_Check_And_Record |    38.24 |    41.87      |
| AuthFailures_IPv6_Check_And_Record |    47.32 |    52.18      |

The key lease costs nothing measurable: every token row is at or below the earlier measurement. The drop is
machine load, not the change; HMAC still dominates, and two uncontended interlocked operations are a few ns
against ~550 ns. The limiter got cheaper, because `IsAllowed` no longer tracks a free slot or reads the overflow
state. The codec rows are within the run-to-run spread of the earlier V1 measurement. No hypothesis loop was run
for these changes: they are correctness fixes with no measurable cost.
