using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.Threading;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Benchmarks.Threading;

/// <summary>
/// Single-threaded round trips through a completion slot. <c>*_CompleteThenAwait</c> completes both stages
/// before awaiting (the wait returns synchronously and the slot auto-releases on the second completion).
/// <c>*_AwaitThenComplete</c> attaches a continuation first, so the completion runs the IValueTaskSource path
/// inline and the slot is released when the result is read. Compares the shipping lock-free table with the
/// archived V0 (monitor lock per slot, locked free list).
/// </summary>
[Config(typeof(ThreadingBenchmarkConfig))]
public class CompletionTableBenchmarks
{
    private const int Rounds = 1024;

    private CompletionTable _table = null!;
    private CompletionTableV0 _tableV0 = null!;
    private Action _continuation = null!;
    private int _signals;

    [GlobalSetup]
    public void Setup()
    {
        _table = new CompletionTable(16);
        _tableV0 = new CompletionTableV0(16);
        _continuation = () => _signals++;
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Rounds)]
    public int Table_V1_CompleteThenAwait()
    {
        CompletionTable table = _table;
        int delivered = 0;
        for (int i = 0; i < Rounds; i++)
        {
            table.TryAllocate(out SendToken token);
            table.Complete(token, CompletionStage.BufferReleased, DeliveryStatus.Pending);
            table.Complete(token, CompletionStage.RemoteAccepted, DeliveryStatus.Delivered);
            if (table.WaitAsync(token, CompletionStage.RemoteAccepted).Result == DeliveryStatus.Delivered)
                delivered++;
        }

        return delivered;
    }

    [Benchmark(OperationsPerInvoke = Rounds)]
    public int Table_V0_Archive_CompleteThenAwait()
    {
        CompletionTableV0 table = _tableV0;
        int delivered = 0;
        for (int i = 0; i < Rounds; i++)
        {
            table.TryAllocate(out SendToken token);
            table.Complete(token, CompletionStage.BufferReleased, DeliveryStatus.Pending);
            table.Complete(token, CompletionStage.RemoteAccepted, DeliveryStatus.Delivered);
            if (table.WaitAsync(token, CompletionStage.RemoteAccepted).Result == DeliveryStatus.Delivered)
                delivered++;
        }

        return delivered;
    }

    [Benchmark(OperationsPerInvoke = Rounds)]
    public int Table_V1_AwaitThenComplete()
    {
        CompletionTable table = _table;
        Action continuation = _continuation;
        int delivered = 0;
        for (int i = 0; i < Rounds; i++)
        {
            table.TryAllocate(out SendToken token);
            ValueTaskAwaiter<DeliveryStatus> awaiter = table.WaitAsync(token, CompletionStage.RemoteAccepted).GetAwaiter();
            awaiter.UnsafeOnCompleted(continuation);
            table.Complete(token, CompletionStage.BufferReleased, DeliveryStatus.Pending);
            table.Complete(token, CompletionStage.RemoteAccepted, DeliveryStatus.Delivered);
            if (awaiter.GetResult() == DeliveryStatus.Delivered)
                delivered++;
        }

        return delivered;
    }

    [Benchmark(OperationsPerInvoke = Rounds)]
    public int Table_V0_Archive_AwaitThenComplete()
    {
        CompletionTableV0 table = _tableV0;
        Action continuation = _continuation;
        int delivered = 0;
        for (int i = 0; i < Rounds; i++)
        {
            table.TryAllocate(out SendToken token);
            ValueTaskAwaiter<DeliveryStatus> awaiter = table.WaitAsync(token, CompletionStage.RemoteAccepted).GetAwaiter();
            awaiter.UnsafeOnCompleted(continuation);
            table.Complete(token, CompletionStage.BufferReleased, DeliveryStatus.Pending);
            table.Complete(token, CompletionStage.RemoteAccepted, DeliveryStatus.Delivered);
            if (awaiter.GetResult() == DeliveryStatus.Delivered)
                delivered++;
        }

        return delivered;
    }
}
