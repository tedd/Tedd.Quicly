using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Benchmarks.Threading;

/// <summary>
/// The shipping <see cref="CompletionTable"/>'s single-threaded round trips (as in <see cref="CompletionTableBenchmarks"/>,
/// without the archived V0) as <c>void</c> methods, so the sustained driver and the paired in-process host can run them.
/// <c>CompleteThenAwait</c> / <c>AwaitThenComplete</c> use a 16-slot table; <c>CompleteThenAwaitPeerSized</c> uses a
/// 4 096-slot table (a peer's default send-table size), so the FIFO free list walks every slot before reusing one.
/// </summary>
[Config(typeof(ThreadingBenchmarkConfig))]
public class CompletionTableRoundTripBench
{
    private const int Rounds = 1024;

    private CompletionTable _table = null!;
    private CompletionTable _peerSized = null!;
    private Action _continuation = null!;
    private int _signals;
    private int _delivered;

    [GlobalSetup]
    public void Setup()
    {
        _table = new CompletionTable(16);
        _peerSized = new CompletionTable(4096);
        _continuation = () => _signals++;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _table.Dispose();
        _peerSized.Dispose();
    }

    [Benchmark(OperationsPerInvoke = Rounds)]
    public void CompleteThenAwait() => _delivered += CompleteThenAwait(_table);

    [Benchmark(OperationsPerInvoke = Rounds)]
    public void CompleteThenAwaitPeerSized() => _delivered += CompleteThenAwait(_peerSized);

    [Benchmark(OperationsPerInvoke = Rounds)]
    public void AwaitThenComplete()
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
            {
                delivered++;
            }
        }

        _delivered += delivered;
    }

    private static int CompleteThenAwait(CompletionTable table)
    {
        int delivered = 0;
        for (int i = 0; i < Rounds; i++)
        {
            table.TryAllocate(out SendToken token);
            table.Complete(token, CompletionStage.BufferReleased, DeliveryStatus.Pending);
            table.Complete(token, CompletionStage.RemoteAccepted, DeliveryStatus.Delivered);
            if (table.WaitAsync(token, CompletionStage.RemoteAccepted).Result == DeliveryStatus.Delivered)
            {
                delivered++;
            }
        }

        return delivered;
    }
}
