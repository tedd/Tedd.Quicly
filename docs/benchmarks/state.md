# State: key tables, mailboxes, send-entry table

Method per [ADR 0007](../adr/0007-measurement-method.md): write the simple correct version (V0), benchmark it,
state a hypothesis, implement it (V1), measure, keep the winner, and archive the loser in
`benchmarks/Tedd.Quicly.Archive/State` so the comparison stays runnable. The structures are specified in
[docs/design/session-layer.md §3](../design/session-layer.md) and [ADR 0008](../adr/0008-hot-path-memory-and-threading-contract.md).

Benchmarks: `benchmarks/Tedd.Quicly.Benchmarks/State/*.cs`. Run with

```
dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*KeyHashingBench*' '*KeyTableBench*' '*MailboxesBench*' '*SendEntryTableBench*'
```

Every State benchmark class carries `[Config(typeof(InProcessShortRunConfig))]`: `Job.ShortRun` on the in-process
emit toolchain with `MemoryDiagnoser` (BenchmarkDotNet 0.15.8 cannot spawn `net11.0` children). `Program.cs` adds
its own global `ShortRun` job, which on .NET 10 is the normal out-of-process toolchain, so each case runs under both
jobs. The tables below show the in-process job, the repository convention. The out-of-process job was used as a
cross-check: see the note after each table.

Hardware / software (2026-09-15):

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.9445/25H2/2025Update/HudsonValley2)
AMD Ryzen 9 5950X 3.40GHz, 1 CPU, 32 logical and 16 physical cores
.NET SDK 11.0.100-preview.7.26381.103
  [Host] : .NET 10.0.12 (10.0.12, 10.0.1226.42308), X64 RyuJIT x86-64-v3
Job=ShortRun (InProcessEmitToolchain)  IterationCount=3  LaunchCount=1  WarmupCount=3
```

ShortRun = 1 launch, 3 warm-up, 3 measured iterations. The *Error* column (99.9 % CI half-width from N = 3) is wide,
so read *Mean* and *StdDev* and treat differences under ~20 % as noise. Other agents were running test suites on the
same machine during the run, which adds noise but does not change any decision below: each one rests on a gap far
larger than that. Every row allocates 0 bytes.

## 1. KeyTable hashing: identity (V0) vs seeded fmix64 (V1)

**V0** (`Archive.State.KeyTableV0`): open addressing, linear probing, native structure-of-arrays `keys[]`/`slots[]`,
load ≤ 0.5, backward-shift deletion, and the **identity hash**: the home bucket is the low bits of the key. It's the
obvious first version: a hash costs nothing, and the common case (small integer entity ids) then fills buckets
0 … N-1 in order, perfect for the cache.

**Hypothesis.** The identity hash is fast only while the low bits of the keys are well distributed. Real key sets
often aren't: entity ids with a stride (i × 1024), aligned handles or pointers, or ids that differ only in their high
bits all land on a few home buckets. With linear probing those buckets merge into long runs, and a lookup's cost
grows with the run length (O(n) in the worst case instead of O(1)). Worse, the keys of a keyed channel are chosen by
the *remote peer* (ADR 0009), so a hostile peer can make every lookup scan the whole table on purpose. MurmurHash3's
64-bit finalizer (`fmix64`: two multiplies, three xor-shifts, ~5 cycles of latency) makes every output bit depend on
every input bit. That spreads any structured key set evenly over the buckets, and XORing a random per-table seed in
first means a peer can't compute colliding keys. (`fmix64` alone is a public bijection with a known inverse, so
without a seed an attacker could generate a whole run of keys for one bucket.) Expected: V1 a few ns slower than V0 on
sequential keys (mixing cost, and random rather than sequential bucket access), about equal on random keys (where
the identity hash is already a good hash), and orders of magnitude faster on strided keys.

**V1** (`Core.State.KeyTable`): identical structure, home bucket = `fmix64(key ^ seed) & mask`, seed from
`RandomNumberGenerator` per table.

`KeyHashingBench`: N keys inserted into a table sized for N (load 0.5), then 65 536 lookups per invoke in random
order over the key set (hits) or over N absent keys of the same pattern (misses). Time is per lookup; *Ratio* is
relative to `V0_Identity_Hit` of the same (N, Pattern).

| Method           | N     | Pattern    | Mean        | Error      | StdDev    | Ratio | Allocated |
|----------------- |------ |----------- |------------:|-----------:|----------:|------:|----------:|
| V0_Identity_Hit  | 1024  | Sequential |   0.8139 ns |  0.0250 ns | 0.0014 ns |  1.00 |         - |
| V1_Fmix64_Hit    | 1024  | Sequential |   5.6706 ns |  0.1119 ns | 0.0061 ns |  6.97 |         - |
| V0_Identity_Miss | 1024  | Sequential |   1.1977 ns |  0.0459 ns | 0.0025 ns |  1.47 |         - |
| V1_Fmix64_Miss   | 1024  | Sequential |  12.7044 ns |  1.1065 ns | 0.0607 ns | 15.61 |         - |
| V0_Identity_Hit  | 1024  | Strided    | 243.2516 ns | 25.8850 ns | 1.4188 ns |  1.00 |         - |
| V1_Fmix64_Hit    | 1024  | Strided    |   5.6489 ns |  0.1733 ns | 0.0095 ns |  0.02 |         - |
| V0_Identity_Miss | 1024  | Strided    | 473.0252 ns | 37.3021 ns | 2.0447 ns |  1.94 |         - |
| V1_Fmix64_Miss   | 1024  | Strided    |  13.2594 ns |  0.3017 ns | 0.0165 ns |  0.05 |         - |
| V0_Identity_Hit  | 1024  | Random     |   4.3598 ns |  0.5937 ns | 0.0325 ns |  1.00 |         - |
| V1_Fmix64_Hit    | 1024  | Random     |   6.2388 ns |  0.1799 ns | 0.0099 ns |  1.43 |         - |
| V0_Identity_Miss | 1024  | Random     |  10.0263 ns |  0.4469 ns | 0.0245 ns |  2.30 |         - |
| V1_Fmix64_Miss   | 1024  | Random     |  13.0839 ns |  0.8989 ns | 0.0493 ns |  3.00 |         - |
| V0_Identity_Hit  | 65536 | Sequential |   1.6989 ns |  0.1894 ns | 0.0104 ns |  1.00 |         - |
| V1_Fmix64_Hit    | 65536 | Sequential |   8.5249 ns |  1.5802 ns | 0.0866 ns |  5.02 |         - |
| V0_Identity_Miss | 65536 | Sequential |   1.2113 ns |  0.0626 ns | 0.0034 ns |  0.71 |         - |
| V1_Fmix64_Miss   | 65536 | Sequential |  17.4082 ns |  9.2851 ns | 0.5089 ns | 10.25 |         - |
| V0_Identity_Hit  | 65536 | Strided    | 249.0085 ns |  7.3690 ns | 0.4039 ns |  1.00 |         - |
| V1_Fmix64_Hit    | 65536 | Strided    |   8.5919 ns |  0.6941 ns | 0.0380 ns |  0.03 |         - |
| V0_Identity_Miss | 65536 | Strided    | 483.3950 ns | 52.9613 ns | 2.9030 ns |  1.94 |         - |
| V1_Fmix64_Miss   | 65536 | Strided    |  17.0277 ns |  2.6517 ns | 0.1454 ns |  0.07 |         - |
| V0_Identity_Hit  | 65536 | Random     |   6.1826 ns |  0.7273 ns | 0.0399 ns |  1.00 |         - |
| V1_Fmix64_Hit    | 65536 | Random     |   8.4783 ns |  0.5920 ns | 0.0324 ns |  1.37 |         - |
| V0_Identity_Miss | 65536 | Random     |  13.3371 ns |  1.2391 ns | 0.0679 ns |  2.16 |         - |
| V1_Fmix64_Miss   | 65536 | Random     |  16.9770 ns |  2.5627 ns | 0.1405 ns |  2.75 |         - |

Out-of-process cross-check: same picture, every V1 row within 15 % of the table above. V0 strided is 190/197 ns per
hit and 372/385 ns per miss there, still about 30 times V1.

**Result.** The hypothesis held on all three counts.

* *Sequential keys* (V0's best case): V1 costs 4.9 ns (1k) and 6.8 ns (64k) more per hit, 0.8 → 5.7 ns and
  1.7 → 8.5 ns. That is the mixing latency, plus random instead of sequential bucket access. V0's misses are almost
  free here because an absent key N + i lands on an empty bucket right away.
* *Random keys*: V1 costs +1.9 / +2.3 ns per hit and +3.1 / +3.6 ns per miss, the bare latency of the mix.
* *Strided keys* (i × 1024): V0 collapses. 1 024 keys land on 2 home buckets and 65 536 keys on 128, each a run of
  512, so a hit costs 243 / 249 ns and a miss 473 / 483 ns. V1 stays at 5.6 / 8.6 ns per hit and 13 / 17 ns per
  miss: 29 to 43 times faster.
* V1 is **flat across patterns**: 5.6 to 6.2 ns per hit at 1k and 8.5 to 8.6 ns at 64k, whatever the keys look like.

**Decision: V1 (seeded fmix64).** A per-message lookup has to be fast in the worst case, not the best, and on the
receive side the key set is chosen by the peer. Paying 2 to 7 ns per lookup for a cost that doesn't depend on the
keys is cheap next to a 250 to 480 ns (and attacker-steerable) worst case. The seed XOR is one instruction and is
not measured separately. V0 is archived as `Archive.State.KeyTableV0`.

## 2. KeyTable vs Dictionary

`KeyTableBench`: N random 64-bit keys. `KeyTable_*` and `Dictionary_*` look up (or remove and re-add) the same keys
in the same random order; `DenseKeyTable_Hit` looks up keys 0 … N-1 in a `DenseKeyTable(N)`. 65 536 operations per
invoke, time per operation.

| Method               | N     | Mean       | Error      | StdDev    | Ratio | Allocated |
|--------------------- |------ |-----------:|-----------:|----------:|------:|----------:|
| KeyTable_Hit         | 1024  |  6.0935 ns |  0.2682 ns | 0.0147 ns |  1.00 |         - |
| Dictionary_Hit       | 1024  |  7.9722 ns |  0.1571 ns | 0.0086 ns |  1.31 |         - |
| DenseKeyTable_Hit    | 1024  |  0.6092 ns |  0.0648 ns | 0.0036 ns |  0.10 |         - |
| KeyTable_Miss        | 1024  | 12.8112 ns |  0.2018 ns | 0.0111 ns |  2.10 |         - |
| Dictionary_Miss      | 1024  | 11.4187 ns |  0.2508 ns | 0.0137 ns |  1.87 |         - |
| KeyTable_RemoveAdd   | 1024  | 35.3484 ns |  0.8922 ns | 0.0489 ns |  5.80 |         - |
| Dictionary_RemoveAdd | 1024  | 18.7116 ns |  0.3252 ns | 0.0178 ns |  3.07 |         - |
| KeyTable_Hit         | 65536 |  8.4726 ns |  0.8793 ns | 0.0482 ns |  1.00 |         - |
| Dictionary_Hit       | 65536 | 12.6385 ns |  1.0200 ns | 0.0559 ns |  1.49 |         - |
| DenseKeyTable_Hit    | 65536 |  0.6163 ns |  0.0306 ns | 0.0017 ns |  0.07 |         - |
| KeyTable_Miss        | 65536 | 16.7968 ns |  2.2708 ns | 0.1245 ns |  1.98 |         - |
| Dictionary_Miss      | 65536 | 15.8710 ns |  2.3776 ns | 0.1303 ns |  1.87 |         - |
| KeyTable_RemoveAdd   | 65536 | 44.0226 ns |  7.8082 ns | 0.4280 ns |  5.20 |         - |
| Dictionary_RemoveAdd | 65536 | 31.3567 ns |  5.9991 ns | 0.3288 ns |  3.70 |         - |

Out-of-process cross-check: every row within 15 % of the table above, except `Dictionary_Miss` (9.6 / 13.2 ns
there, about 17 % faster). So the two misses are within noise of each other.

* **Hits**, the per-message path: `KeyTable` is 1.31× faster than `Dictionary<ulong,int>` at 1k keys and 1.49× at
  64k. Once the arrays outgrow L2 the gap widens because `KeyTable`'s two loads (`slots[i]`, `keys[i]`) are
  independent and overlap, while `Dictionary` has two dependent loads (bucket, then entry).
* **Misses** are 6 to 12 % slower than `Dictionary`, within noise at 64k. At load 0.5 an unsuccessful linear probe
  inspects about 2.5 buckets, and the loop exit is data-dependent. A miss happens once per new key and on junk
  keys, not per accepted message.
* **Churn** (remove + re-add) is 1.9× (1k) and 1.4× (64k) slower than `Dictionary`. `Remove` is a hit lookup plus a
  backward shift that re-mixes every entry it moves past, and the re-add is a miss lookup plus an insert, whereas
  `Dictionary` unlinks and relinks one chain entry. Keys are added and retired once per key lifetime (first update,
  `KeyRetired`), never per message, so this is off the hot path and not worth a larger structure. If a workload
  with heavy key churn ever shows it, the lever is a lower maximum load (shorter runs to shift), not the hash.
* **`DenseKeyTable`** is a bounds check plus one bit test, 0.61 ns at any size: 10 to 14 times faster than the hashed
  table. Channels whose keys are small integers should declare `KeySpace.Dense`.
* Neither table allocates, and `KeyTable` is bounded (`MaxKeys`), native, reference-free and gives the dense,
  stable slot indices the per-key state arrays need. `Dictionary` offers none of these.

## 3. Mailboxes: scalar bitset scan (V0) vs vector line skip (V1)

`MailboxesBench`: a channel with 65 536 key slots, single-threaded, so this measures instructions and fences, not
cross-core traffic. `*_PostPopTake` is one receive→poll cycle for `DirtyKeys` distinct random keys: `Post` per key
(interlocked exchange + interlocked OR), then one `PopDirty` and a `Take` (interlocked exchange) per dirty key.
`Ring_EnqueueDequeue` pushes the same number of items through an `SpscRing<int>`, which is what a non-coalescing
channel pays. `*_PopDirty_Clean` is the bitset scan of an idle channel (1 024 words), the per-poll floor. Time per
cycle.

**V0** (`Archive.State.MailboxesV0`): `PopDirty` reads every word of the dirty bitset with a volatile load and a
branch.

**Hypothesis.** In the first run (V0 was the shipping version then), scanning a clean 65 536-slot channel cost
472 ns, 73 % of a whole 16-key cycle (646 ns). At 0.46 ns per word that is loop and branch overhead, not memory:
8 KiB stays in L1/L2. Testing a whole 64-byte line (8 words) with one vector OR-and-compare, and skipping it when
zero, should cut the idle floor several-fold and speed up any sparse poll. The protocol doesn't change: the vector
read is only a hint, and clearing is still the interlocked AND of the bits handed out. Expected: 3 to 6× on the
clean scan, and neutral for dense dirty sets (one extra test per dirty line).

**V1** (`Core.State.Mailboxes`): four `Vector128` loads OR'd per line (SSE2 / AdvSIMD, so one code path on x64 and
arm64), then the V0 word loop inside a non-clean line.

Second run, both versions in the same process:

| Method              | DirtyKeys | Mean         | Error       | StdDev     | Ratio | Allocated |
|-------------------- |---------- |-------------:|------------:|-----------:|------:|----------:|
| V1_PostPopTake      | 16        |    522.82 ns |    73.61 ns |   4.035 ns |  1.00 |         - |
| V0_PostPopTake      | 16        |    938.21 ns |   808.74 ns |  44.330 ns |  1.79 |         - |
| Ring_EnqueueDequeue | 16        |     50.50 ns |    34.02 ns |   1.865 ns |  0.10 |         - |
| V1_PopDirty_Clean   | 16        |    296.30 ns |    37.73 ns |   2.068 ns |  0.57 |         - |
| V0_PopDirty_Clean   | 16        |    881.08 ns |    57.03 ns |   3.126 ns |  1.69 |         - |
| V1_PostPopTake      | 1024      | 15,607.39 ns |   995.32 ns |  54.557 ns |  1.00 |         - |
| V0_PostPopTake      | 1024      | 14,500.71 ns |   584.78 ns |  32.054 ns |  0.93 |         - |
| Ring_EnqueueDequeue | 1024      |  2,996.96 ns | 4,174.42 ns | 228.814 ns |  0.19 |         - |
| V1_PopDirty_Clean   | 1024      |    213.09 ns |   105.64 ns |   5.790 ns |  0.01 |         - |
| V0_PopDirty_Clean   | 1024      |    859.67 ns |   432.27 ns |  23.694 ns |  0.06 |         - |

(`*_PopDirty_Clean` doesn't depend on `DirtyKeys`; its two rows are two measurements of the same thing.) The machine
was more heavily loaded by other agents during this run than during the first: V0's clean scan measured 860 to 880 ns
here against 472 ns before, and the ring 50 against 36 ns. So compare rows within this table, not across runs.
Out-of-process cross-check: V1 clean scan 168 / 293 ns against V0's 674 / 834 ns; 16-key cycle 2.29× in V1's
favour; 1 024-key cycle 0.96.

**Result.** The clean scan is 3 to 4× faster (213 to 296 ns against 860 to 880 ns), and a sparse 16-key cycle 1.8×
(2.3× out of process). A dense cycle (1 024 random keys out of 65 536, so most lines are dirty) is 7 % slower
in-process and 4 % out of process, inside the noise band as predicted. The speed-up lands at the low end of the
hypothesis: about 2 ns per line remains, for four loads, three ORs, the compare and the loop's bounds checks.

**Decision: V1.** V0 is archived as `Archive.State.MailboxesV0`. The idle floor is now about 0.2 to 0.3 µs per poll
for a 65 536-slot channel (it scales with slots, not with traffic). If that ever matters, the next lever is a
summary bitset with one bit per line, so the scan becomes O(dirty lines). That was not done: it would add a second
interlocked OR to every post on the transport thread's per-message path in order to save the game thread ~0.2 µs
per poll.

**Mailbox vs ring.** A mailbox update costs about 14 ns per key: three interlocked operations (exchange, OR,
exchange) plus the AND per word. An SPSC ring item costs 2 to 3 ns (plain stores and loads with release/acquire).
The mailbox pays ~5× per update for what a ring cannot do:

* bounded memory per key, whatever the update rate;
* latest-wins coalescing: a key updated ten times between polls costs the game thread one take, and the transport
  thread frees the nine superseded leases at once;
* no back-pressure and no drops.

Channels without `CoalesceOnReceive` use the ring.

## 4. SendEntryTable

`SendEntryTableBench`: the ADR 0008 slot protocol on one thread. Allocate, copy an 8-byte header into the scratch
area, set the payload segment and cold fields, publish (release store), complete through the context (the
generation-checked 64-bit CAS of `TryTransitionContext`), free. `Single` does one entry at a time, `Batch32` keeps 32
in flight, and `Container16` packs 15 published members into a container entry and fans its completion out. Time per
entry.

| Method      | Mean     | Error    | StdDev   | Ratio | Allocated |
|------------ |---------:|---------:|---------:|------:|----------:|
| Single      | 18.53 ns | 5.632 ns | 0.309 ns |  1.00 |         - |
| Batch32     | 21.27 ns | 1.408 ns | 0.077 ns |  1.15 |         - |
| Container16 | 22.35 ns | 2.925 ns | 0.160 ns |  1.21 |         - |

Out-of-process cross-check: 20.4 / 25.2 / 29.4 ns.

The whole per-message bookkeeping of the send path (slot, header copy, publish, the one interlocked
compare-exchange of the completion, free) costs about 20 ns and allocates nothing. With 32 entries in flight, or
with container fan-out, it grows by only 15 to 20 %: the working set is 32 cache lines of hot entries plus the cold
arrays. That is well below the cost of the transport call it feeds, so no V1 is warranted. The 64-bit state word
that makes completions ABA-safe is still one `lock cmpxchg`, just 8 bytes wide instead of 4. It was not benchmarked
against a 32-bit-CAS variant, because the 32-bit version is not correct (see the `SendEntryTable` remarks).
