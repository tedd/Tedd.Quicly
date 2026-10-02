using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Benchmarks.Threading;

/// <summary>
/// The blocking <see cref="CompletionTable.Wait"/> woken from another thread: the owner (the benchmark thread) allocates a
/// slot, hands its token to a completer thread and calls <see cref="CompletionTable.Wait"/> on
/// <see cref="CompletionStage.RemoteAccepted"/>; the completer completes that stage <see cref="DelayUs"/> microseconds after
/// it saw the token, and the owner releases the slot once the wait returned. The time per operation is one such round:
/// the delay, the hand-off and the wait's wake-up.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="DelayUs"/> picks the phase of the wait that sees the completion: 0 — the table's own short spin; 10 — just
/// past it, where an event that spins before it blocks still catches it; 100 — after the waiter has parked on the event.
/// </para>
/// <para>
/// The completer polls for the next token with non-yielding spins, so it only ever loses its core to the scheduler.
/// Run loaded by pinning the process together with busy-loop processes to the same few cores (docs/benchmarks/threading.md
/// section 7); there every spin that yields hands the core to a busy thread for a scheduler quantum.
/// </para>
/// </remarks>
[Config(typeof(ThreadingBenchmarkConfig))]
public class CompletionTableWaitBench
{
    private const int Rounds = 64;

    private CompletionTable _table = null!;
    private Thread _completer = null!;
    private long _delayTicks;
    private SendToken _token;
    private PaddedLong _request;
    private volatile bool _stop;
    private int _delivered;

    /// <summary>Microseconds between the completer seeing the token and completing the stage.</summary>
    [Params(0, 10, 20, 40, 100)]
    public int DelayUs { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _table = new CompletionTable(16);
        _delayTicks = DelayUs * Stopwatch.Frequency / 1_000_000;
        _stop = false;
        _completer = new Thread(CompleterLoop) { IsBackground = true, Name = "completer" };
        _completer.Start();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _stop = true;
        _completer.Join();
        _table.Dispose();
    }

    [Benchmark(OperationsPerInvoke = Rounds)]
    public void WaitWokenByOtherThread()
    {
        CompletionTable table = _table;
        int delivered = 0;
        for (int i = 0; i < Rounds; i++)
        {
            table.TryAllocate(out SendToken token);
            _token = token;
            Volatile.Write(ref _request.Value, _request.Value + 1);
            if (table.Wait(token, CompletionStage.RemoteAccepted, Timeout.InfiniteTimeSpan) == DeliveryStatus.Delivered)
            {
                delivered++;
            }

            table.Release(token);
        }

        _delivered += delivered;
    }

    private void CompleterLoop()
    {
        long seen = 0;
        while (true)
        {
            long request;
            while ((request = Volatile.Read(ref _request.Value)) == seen)
            {
                if (_stop)
                {
                    return;
                }

                Thread.SpinWait(8);
            }

            seen = request;
            SendToken token = _token;
            if (_delayTicks > 0)
            {
                long until = Stopwatch.GetTimestamp() + _delayTicks;
                while (Stopwatch.GetTimestamp() < until)
                {
                    Thread.SpinWait(8);
                }
            }

            _table.Complete(token, CompletionStage.RemoteAccepted, DeliveryStatus.Delivered);
        }
    }
}
