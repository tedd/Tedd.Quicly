using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Benchmarks.Threading;

/// <summary>
/// Cross-core hand-off of received messages, the way a peer moves them from the transport thread (producer) to the game
/// thread (consumer): <see cref="Batch"/> messages are published per pass, then the consumer drains the pass. Every
/// session benchmark runs both peers on one thread, so this is the only benchmark in which the rings, the leases and their
/// counters actually cross cores.
/// </summary>
/// <remarks>
/// <para>
/// Protocol per pass (lockstep, phases not overlapped so each side's busy time can be timed on its own): the consumer (the
/// benchmark thread) raises a padded <c>go</c> word; the producer thread publishes the batch and raises a padded
/// <c>done</c> word; the consumer then drains the batch. <see cref="Placement"/> <c>Single</c> runs both phases on the
/// benchmark thread (the single-core reference every session benchmark measures); <c>SameCcd</c> pins the producer to
/// another physical core of the same CCD (CPU 6 against the consumer's CPU 4), <c>CrossCcd</c> to the other CCD (CPU 20).
/// The process affinity must include those CPUs (driver: <c>PROF_AFFINITY=100050</c>).
/// </para>
/// <para>
/// The reported time per operation is wall time per message and includes the two <c>go</c>/<c>done</c> hand-offs per
/// pass (the latency part, amortised over the batch). <see cref="Cleanup"/> also prints each side's busy time per message
/// (producer phase, consumer phase) measured after a 2.5 s warm-up: their sum minus the <c>Single</c> sum is the per-message
/// cross-core cost of the kernel.
/// </para>
/// <para>
/// Kernels: <c>Ring64</c> — <see cref="SpscRing{T}"/> of 64-byte <see cref="ReceiveEntry"/>, producer
/// <see cref="SpscRing{T}.TryEnqueueReserving"/> as <c>PeerCore.TryEnqueueReceive</c> does. <c>Ring16</c> — the same with a
/// 16-byte element (completion-ring sized). <c>Mpsc64</c> — <see cref="MpscRing{T}"/> with one producer and 64-byte slots
/// (the ThreadSafeSend front's shape). <c>RingLease</c> — <c>Ring64</c> plus the receive lease as the peer handles it: the
/// producer rents a 64-byte block, adds it to a shared byte budget with <see cref="Interlocked.Add(ref long, long)"/>
/// (<c>PeerCore.TryRentReceive</c>) and writes the payload; the consumer reads the payload, subtracts the budget and returns
/// the block (<c>PeerCore.ReturnReceive</c>). <c>RingLeaseNoBudget</c> — without the budget counter.
/// <c>RingLeaseReturnRing</c> — prototype of a return ring: the consumer hands blocks back through an SPSC ring and the
/// producer reuses them from there before renting (no budget counter), so no lease operation is a lock-prefixed
/// instruction in steady state. <c>RingLeaseTransportReturn</c> — the general form of that idea: the consumer hands every
/// block back through the SPSC ring, and the producer (the thread that rented it) subtracts the budget and returns it to
/// the pool at the start of its next pass, so every atomic on the pool's and the budget's lines runs on one core.
/// </para>
/// </remarks>
[Config(typeof(ThreadingBenchmarkConfig))]
public unsafe class CrossCoreHandoffBench
{
    /// <summary>Messages per invocation.</summary>
    public const int Messages = 1000;

    private const int ConsumerCpu = 4;
    private const int SameCcdCpu = 6;
    private const int CrossCcdCpu = 20;

    private enum Kernel
    {
        Ring64,
        Ring16,
        Mpsc64,
        RingLease,
        RingLeaseNoBudget,
        RingLeaseReturnRing,
        RingLeaseTransportReturn,
    }

    private SpscRing<ReceiveEntry> _ring64 = null!;
    private SpscRing<Small16> _ring16 = null!;
    private MpscRing<Payload56> _mpsc = null!;
    private SpscRing<BufferLease> _returns = null!;
    private SlabAllocator _allocator = null!;
    private Thread? _producer;
    private Kernel _kernel;
    private long _seq;
    private PaddedLong _go;
    private PaddedLong _done;
    private PaddedLong _budget;
    private PaddedLong _producerTicks;
    private long _consumerTicks;
    private long _wallTicks;
    private long _measuredMessages;
    private long _sink;
    private long _setupTimestamp;
    private bool _measuring;
    private int _harnessThread;

    /// <summary>Messages published per pass.</summary>
    [Params(1, 10, 100)]
    public int Batch { get; set; } = 100;

    /// <summary><c>Single</c>, <c>SameCcd</c> or <c>CrossCcd</c>.</summary>
    [Params("Single", "SameCcd", "CrossCcd")]
    public string Placement { get; set; } = "Single";

    [GlobalSetup]
    public void Setup()
    {
        if (Messages % Batch != 0)
        {
            throw new InvalidOperationException("Batch must divide " + Messages);
        }

        _ring64 = new SpscRing<ReceiveEntry>(4096);
        _ring16 = new SpscRing<Small16>(8192);
        _mpsc = new MpscRing<Payload56>(1024);
        _returns = new SpscRing<BufferLease>(1024);
        _allocator = new SlabAllocator(new SlabAllocatorOptions
        {
            FreeListShards = 2,
            SizeClasses = [new(64, 4096), new(1536, 64)],
        });
        PinHarness();
        if (Placement != "Single")
        {
            int cpu = Placement switch
            {
                "SameCcd" => SameCcdCpu,
                "CrossCcd" => CrossCcdCpu,
                _ => throw new ArgumentException(Placement),
            };
            _producer = new Thread(() => ProducerMain(cpu)) { IsBackground = true, Priority = ThreadPriority.Highest };
            _producer.Start();
        }

        _setupTimestamp = Stopwatch.GetTimestamp();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (_producer is not null)
        {
            Volatile.Write(ref _go.Value, -1);
            _producer.Join();
        }

        if (_measuredMessages > 0)
        {
            double f = 1e9 / Stopwatch.Frequency / _measuredMessages;
            Console.WriteLine(string.Create(System.Globalization.CultureInfo.InvariantCulture,
                $"HANDOFF {_kernel} {Placement} batch={Batch}: producer {_producerTicks.Value * f:F1} | consumer {_consumerTicks * f:F1} | busy {(_producerTicks.Value + _consumerTicks) * f:F1} | wall {_wallTicks * f:F1} ns/msg ({_measuredMessages} msgs)"));
        }

        _ring64.Dispose();
        _ring16.Dispose();
        _mpsc.Dispose();
        _returns.Dispose();
        _allocator.Dispose();
    }

    [Benchmark(OperationsPerInvoke = Messages)]
    public void Ring64() => Run(Kernel.Ring64);

    [Benchmark(OperationsPerInvoke = Messages)]
    public void Ring16() => Run(Kernel.Ring16);

    [Benchmark(OperationsPerInvoke = Messages)]
    public void Mpsc64() => Run(Kernel.Mpsc64);

    [Benchmark(OperationsPerInvoke = Messages)]
    public void RingLease() => Run(Kernel.RingLease);

    [Benchmark(OperationsPerInvoke = Messages)]
    public void RingLeaseNoBudget() => Run(Kernel.RingLeaseNoBudget);

    [Benchmark(OperationsPerInvoke = Messages)]
    public void RingLeaseReturnRing() => Run(Kernel.RingLeaseReturnRing);

    [Benchmark(OperationsPerInvoke = Messages)]
    public void RingLeaseTransportReturn() => Run(Kernel.RingLeaseTransportReturn);

    private void Run(Kernel kernel)
    {
        if (Environment.CurrentManagedThreadId != _harnessThread)
        {
            PinHarness();
        }

        if (!_measuring && Stopwatch.GetElapsedTime(_setupTimestamp).TotalSeconds > 2.5)
        {
            // The producer is idle between passes (it has published its last `done`), so its counter can be reset here.
            _measuring = true;
            _producerTicks.Value = 0;
            _consumerTicks = 0;
            _wallTicks = 0;
            _measuredMessages = 0;
        }

        _kernel = kernel;
        int batch = Batch;
        int passes = Messages / batch;
        long consumer = 0;
        long start = Stopwatch.GetTimestamp();
        if (_producer is null)
        {
            long producer = 0;
            for (int p = 0; p < passes; p++)
            {
                long t0 = Stopwatch.GetTimestamp();
                Produce(kernel, batch);
                long t1 = Stopwatch.GetTimestamp();
                Consume(kernel, batch);
                long t2 = Stopwatch.GetTimestamp();
                producer += t1 - t0;
                consumer += t2 - t1;
            }

            _producerTicks.Value += producer;
        }
        else
        {
            for (int p = 0; p < passes; p++)
            {
                long seq = ++_seq;
                Volatile.Write(ref _go.Value, seq);
                while (Volatile.Read(ref _done.Value) != seq)
                {
                    Thread.SpinWait(1);
                }

                long t0 = Stopwatch.GetTimestamp();
                Consume(kernel, batch);
                consumer += Stopwatch.GetTimestamp() - t0;
            }
        }

        _consumerTicks += consumer;
        _wallTicks += Stopwatch.GetTimestamp() - start;
        _measuredMessages += Messages;
    }

    private void ProducerMain(int cpu)
    {
        Pin(cpu);
        long seen = 0;
        while (true)
        {
            long go;
            while ((go = Volatile.Read(ref _go.Value)) == seen)
            {
                Thread.SpinWait(1);
            }

            if (go < 0)
            {
                return;
            }

            seen = go;
            long t0 = Stopwatch.GetTimestamp();
            Produce(_kernel, Batch);
            _producerTicks.Value += Stopwatch.GetTimestamp() - t0;
            Volatile.Write(ref _done.Value, go);
        }
    }

    private void Produce(Kernel kernel, int batch)
    {
        switch (kernel)
        {
            case Kernel.Ring64:
                for (int i = 0; i < batch; i++)
                {
                    ReceiveEntry entry = default;
                    entry.Channel = 2;
                    entry.Sequence = (uint)i;
                    entry.Length = 64;
                    Check(_ring64.TryEnqueueReserving(in entry, 0));
                }

                break;
            case Kernel.Ring16:
                for (int i = 0; i < batch; i++)
                {
                    Small16 item = new() { A = i, B = batch };
                    Check(_ring16.TryEnqueueReserving(in item, 0));
                }

                break;
            case Kernel.Mpsc64:
                for (int i = 0; i < batch; i++)
                {
                    Payload56 item = default;
                    item.A = i;
                    item.G = batch;
                    Check(_mpsc.TryEnqueue(in item));
                }

                break;
            default:
                ProduceLeases(kernel, batch);
                break;
        }
    }

    private void ProduceLeases(Kernel kernel, int batch)
    {
        SlabAllocator allocator = _allocator;
        if (kernel == Kernel.RingLeaseTransportReturn)
        {
            // The leases the consumer handed back since the last pass go back to the pool (and the budget) from here, the
            // thread that rented them, so the shard head, the shard counters and the budget stay in this core's cache.
            while (_returns.TryDequeue(out BufferLease returned))
            {
                Interlocked.Add(ref _budget.Value, -returned.Length);
                allocator.Return(in returned);
            }
        }

        for (int i = 0; i < batch; i++)
        {
            BufferLease lease;
            if (kernel != Kernel.RingLeaseReturnRing || !_returns.TryDequeue(out lease))
            {
                Check(allocator.TryRent(64, out lease));
            }

            if (kernel is Kernel.RingLease or Kernel.RingLeaseTransportReturn)
            {
                Interlocked.Add(ref _budget.Value, lease.Length);
            }

            long* p = (long*)allocator.GetPointer(in lease);
            for (int k = 0; k < 8; k++)
            {
                p[k] = i + k;
            }

            ReceiveEntry entry = default;
            entry.Channel = 2;
            entry.Sequence = (uint)i;
            entry.Lease = lease;
            entry.Length = 64;
            Check(_ring64.TryEnqueueReserving(in entry, 0));
        }
    }

    private void Consume(Kernel kernel, int batch)
    {
        long sum = 0;
        switch (kernel)
        {
            case Kernel.Ring64:
                for (int i = 0; i < batch; i++)
                {
                    Check(_ring64.TryDequeue(out ReceiveEntry entry));
                    sum += entry.Sequence + entry.Length;
                }

                break;
            case Kernel.Ring16:
                for (int i = 0; i < batch; i++)
                {
                    Check(_ring16.TryDequeue(out Small16 item));
                    sum += item.A;
                }

                break;
            case Kernel.Mpsc64:
                for (int i = 0; i < batch; i++)
                {
                    Check(_mpsc.TryDequeue(out Payload56 item));
                    sum += item.A;
                }

                break;
            default:
                SlabAllocator allocator = _allocator;
                for (int i = 0; i < batch; i++)
                {
                    Check(_ring64.TryDequeue(out ReceiveEntry entry));
                    long* p = (long*)allocator.GetPointer(in entry.Lease);
                    for (int k = 0; k < 8; k++)
                    {
                        sum += p[k];
                    }

                    if (kernel == Kernel.RingLeaseTransportReturn)
                    {
                        if (!_returns.TryEnqueue(in entry.Lease))
                        {
                            Interlocked.Add(ref _budget.Value, -entry.Lease.Length);
                            allocator.Return(in entry.Lease);
                        }

                        continue;
                    }

                    if (kernel == Kernel.RingLease)
                    {
                        Interlocked.Add(ref _budget.Value, -entry.Lease.Length);
                    }

                    if (kernel != Kernel.RingLeaseReturnRing || !_returns.TryEnqueue(in entry.Lease))
                    {
                        allocator.Return(in entry.Lease);
                    }
                }

                break;
        }

        _sink += sum;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Check(bool ok)
    {
        if (!ok)
        {
            Fail();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void Fail() => throw new InvalidOperationException("ring or pool unexpectedly full/empty");

    private void PinHarness()
    {
        Pin(ConsumerCpu);
        _harnessThread = Environment.CurrentManagedThreadId;
    }

    private static void Pin(int cpu)
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        if (SetThreadAffinityMask(GetCurrentThread(), (nuint)1 << cpu) == 0)
        {
            throw new InvalidOperationException($"Cannot pin a thread to CPU {cpu}: the process affinity must include it (driver: PROF_AFFINITY=100050).");
        }
    }

    [DllImport("kernel32.dll")]
    private static extern nint GetCurrentThread();

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern nuint SetThreadAffinityMask(nint thread, nuint mask);

    private struct Small16
    {
        public long A;
        public long B;
    }

    private struct Payload56
    {
        public long A;
        public long B;
        public long C;
        public long D;
        public long E;
        public long F;
        public long G;
    }
}
