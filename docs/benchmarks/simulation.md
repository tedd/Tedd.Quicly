# Simulation — `SimulatedTransport` harness cost

`Tedd.Quicly.Testing.Simulation` (design: [session-layer.md §5](../design/session-layer.md)) is test and benchmark
infrastructure, not a shipping hot path, so there is no V0 → V1 optimisation round here (ADR 0007 applies to
optimisations; nothing is archived). What matters is (a) that the harness adds **no GC allocation** in steady
state, so allocation tests of the session layer run over it can assert 0 B, and (b) that its per-event cost is
small next to the code under test. This page records the baseline.

## Benchmark

`benchmarks/Tedd.Quicly.Benchmarks/Simulation/SimulatedTransportBench.cs`, `[Config(typeof(InProcessShortRunConfig))]`.
Run with

```
dotnet run -c Release -f net10.0 --project benchmarks/Tedd.Quicly.Benchmarks -- --filter '*SimulatedTransportBench*'
```

| Benchmark | Workload | Reported per |
|---|---|---|
| `DatagramPingPong` | ideal link, 1 ms one-way delay: one 600-byte datagram A→B **and** one B→A, then `Advance(100 µs)`; 1 000 rounds per invocation | round (two datagrams: 2 × Sent, 2 × delivery, 2 × Acknowledged = 6 events + 2 sends) |
| `DatagramPingPongLossy` | same with 500 µs jitter, 5 % loss, 5 % reordering | round |
| `StreamSend64KiB` | one 64 KiB `SendStream` on an open stream (55 packets of 1 200 bytes), then `RunUntilIdle` (55 arrivals + copy into the receive buffer + 55 receive callbacks + 1 completion) | send |

Sinks are empty (they only count stream bytes), so the numbers are the harness alone.

Hardware / software (2026-09-15):

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.9445/25H2/2025Update/HudsonValley2)
AMD Ryzen 9 5950X 3.40GHz, 1 CPU, 32 logical and 16 physical cores
.NET SDK 11.0.100-preview.7.26381.103
  [Host] : .NET 10.0.12 (10.0.12, 10.0.1226.42308), X64 RyuJIT x86-64-v3
Job=ShortRun  IterationCount=3  LaunchCount=1  WarmupCount=3
```

On .NET 10 `Program.CreateConfig` adds the default out-of-process ShortRun job next to the class's in-process
one, hence two rows per method. ShortRun has N = 3, so the *Error* column is wide; read *Mean* and *StdDev*.

## Results (net10.0)

| Method                | Toolchain              | Mean      | Error      | StdDev    | Allocated |
|---------------------- |----------------------- |----------:|-----------:|----------:|----------:|
| DatagramPingPong      | Default                |  1.491 us |  2.0135 us | 0.1104 us |         - |
| DatagramPingPongLossy | Default                |  1.634 us |  0.6417 us | 0.0352 us |         - |
| StreamSend64KiB       | Default                | 11.886 us | 37.5745 us | 2.0596 us |         - |
| DatagramPingPong      | InProcessEmitToolchain |  1.460 us |  4.3963 us | 0.2410 us |         - |
| DatagramPingPongLossy | InProcessEmitToolchain |  1.366 us |  0.3593 us | 0.0197 us |         - |
| StreamSend64KiB       | InProcessEmitToolchain | 11.457 us |  5.5027 us | 0.3016 us |         - |

## Reading

* **Zero allocation** in all three workloads, confirmed independently by the unit tests
  `DatagramTests.SteadyState_DatagramSendAndDelivery_DoesNotAllocate` (loss, jitter and reordering on) and
  `SteadyState_UnderBandwidthLimit_DoesNotAllocate` (serialization queue on), both measured with
  `GC.GetAllocatedBytesForCurrentThread()`. What makes that hold: events are one flat struct in an array
  min-heap; datagram and stream-send records are struct arrays with free lists and generation tags; payload
  copies go into pinned power-of-two arrays from a per-network pool; stream slots are reused objects; the
  random generator is a struct (xoshiro256**); the lock is `System.Threading.Lock`.
* **About 0.25 µs per event** (1.46 µs / 6 events + 2 copies per datagram round; loss and reordering do not
  change it measurably), and **about 0.2 µs per 1 200-byte stream packet** including the copy into the
  receiver's reassembly buffer. A session-layer test that runs 10⁵ datagrams through the harness spends well
  under 0.1 s in it.
* Datagram and stream payloads are copied when a send is accepted (the contract allows it). The flip side: the
  simulator cannot catch a caller that releases or reuses a buffer before the matching completion — that
  lifetime rule (ADR 0008 invariant 1) must be covered by the session layer's own tests or by the MsQuic
  end-to-end tests.

Decision: keep as is. Revisit only if a session-layer benchmark shows the harness above ~10 % of its time.
