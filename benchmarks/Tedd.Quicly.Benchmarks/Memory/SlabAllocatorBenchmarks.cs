using System.Buffers;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.Memory;
using Tedd.Quicly.Core.Memory;

namespace Tedd.Quicly.Benchmarks.Memory;

/// <summary>
/// Rent/return of 1 536-byte blocks: the shipping sharded lock-free <see cref="SlabAllocator"/> (V2, with and
/// without lease validation) against the archived single-stack lock-free V1, the archived lock-based V0,
/// <see cref="ArrayPool{T}.Shared"/> and raw <see cref="NativeMemory"/>. Each thread rents a batch of 16
/// blocks, touches one byte in each and returns them, for <see cref="OpsPerThread"/> rent/return pairs. The
/// reported time is wall time divided by <see cref="OpsPerThread"/>, i.e. cost per operation per thread: with
/// perfect scaling the 8-thread number equals the 1-thread number, and any increase is contention.
/// The ShortRun job comes from <see cref="Program"/> (in-process on .NET 11, where BenchmarkDotNet 0.15.8
/// cannot validate the preview SDK).
/// </summary>
[MemoryDiagnoser]
public class SlabAllocatorBenchmarks
{
    private const int OpsPerThread = 1_000_000;
    private const int Batch = 16;
    private const int BlockSize = 1536;

    private static readonly (int, int)[] DefaultClasses =
        [(64, 16_384), (256, 4_096), (1_536, 2_048), (4_096, 768), (16_384, 192), (65_536, 32), (262_144, 12)];

    private SlabAllocator _slab = null!;
    private SlabAllocator _slabValidated = null!;
    private SlabAllocatorV1 _slabV1 = null!;
    private SlabAllocatorV0 _slabV0 = null!;

    [Params(1, 8)]
    public int Threads { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _slab = new SlabAllocator(new SlabAllocatorOptions { ValidateLeases = false });
        _slabValidated = new SlabAllocator(new SlabAllocatorOptions { ValidateLeases = true });
        _slabV1 = new SlabAllocatorV1(DefaultClasses, validateLeases: false);
        _slabV0 = new SlabAllocatorV0(DefaultClasses);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _slab.Dispose();
        _slabValidated.Dispose();
        _slabV1.Dispose();
        _slabV0.Dispose();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = OpsPerThread)]
    public void Slab_V2_Sharded() => Run(SlabV2Body, _slab);

    [Benchmark(OperationsPerInvoke = OpsPerThread)]
    public void Slab_V2_Sharded_Validated() => Run(SlabV2Body, _slabValidated);

    [Benchmark(OperationsPerInvoke = OpsPerThread)]
    public void Slab_V1_SingleStack() => Run(SlabV1Body, _slabV1);

    [Benchmark(OperationsPerInvoke = OpsPerThread)]
    public void Slab_V0_Lock() => Run(SlabV0Body, _slabV0);

    [Benchmark(OperationsPerInvoke = OpsPerThread)]
    public void ArrayPool_Shared() => Run(ArrayPoolBody, ArrayPool<byte>.Shared);

    [Benchmark(OperationsPerInvoke = OpsPerThread)]
    public void NativeMemory_AllocFree() => Run(NativeMemoryBody, null);

    private void Run(ParameterizedThreadStart body, object? state)
    {
        if (Threads == 1)
        {
            body(state);
            return;
        }

        var threads = new Thread[Threads];
        for (int t = 0; t < threads.Length; t++)
        {
            threads[t] = new Thread(body, 256 * 1024);
            threads[t].Start(state);
        }

        for (int t = 0; t < threads.Length; t++)
            threads[t].Join();
    }

    private static void SlabV2Body(object? state)
    {
        var slab = (SlabAllocator)state!;
        Span<BufferLease> held = stackalloc BufferLease[Batch];
        for (int i = 0; i < OpsPerThread / Batch; i++)
        {
            for (int b = 0; b < Batch; b++)
            {
                if (!slab.TryRent(BlockSize, out held[b]))
                    throw new InvalidOperationException("exhausted");
                slab.GetSpan(in held[b])[0] = (byte)b;
            }

            for (int b = 0; b < Batch; b++)
                slab.Return(in held[b]);
        }
    }

    private static void SlabV1Body(object? state)
    {
        var slab = (SlabAllocatorV1)state!;
        Span<BufferLeaseV1> held = stackalloc BufferLeaseV1[Batch];
        for (int i = 0; i < OpsPerThread / Batch; i++)
        {
            for (int b = 0; b < Batch; b++)
            {
                if (!slab.TryRent(BlockSize, out held[b]))
                    throw new InvalidOperationException("exhausted");
                slab.GetSpan(in held[b])[0] = (byte)b;
            }

            for (int b = 0; b < Batch; b++)
                slab.Return(in held[b]);
        }
    }

    private static void SlabV0Body(object? state)
    {
        var slab = (SlabAllocatorV0)state!;
        Span<BufferLeaseV0> held = stackalloc BufferLeaseV0[Batch];
        for (int i = 0; i < OpsPerThread / Batch; i++)
        {
            for (int b = 0; b < Batch; b++)
            {
                if (!slab.TryRent(BlockSize, out held[b]))
                    throw new InvalidOperationException("exhausted");
                slab.GetSpan(in held[b])[0] = (byte)b;
            }

            for (int b = 0; b < Batch; b++)
                slab.Return(in held[b]);
        }
    }

    private static void ArrayPoolBody(object? state)
    {
        var pool = (ArrayPool<byte>)state!;
        var held = new byte[Batch][];
        for (int i = 0; i < OpsPerThread / Batch; i++)
        {
            for (int b = 0; b < Batch; b++)
            {
                held[b] = pool.Rent(BlockSize);
                held[b][0] = (byte)b;
            }

            for (int b = 0; b < Batch; b++)
                pool.Return(held[b]);
        }
    }

    private static unsafe void NativeMemoryBody(object? state)
    {
        byte** held = stackalloc byte*[Batch];
        for (int i = 0; i < OpsPerThread / Batch; i++)
        {
            for (int b = 0; b < Batch; b++)
            {
                held[b] = (byte*)NativeMemory.Alloc(BlockSize);
                held[b][0] = (byte)b;
            }

            for (int b = 0; b < Batch; b++)
                NativeMemory.Free(held[b]);
        }
    }
}
