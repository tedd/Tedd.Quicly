using System.Diagnostics;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Tests.Threading;

/// <summary>The blocking <see cref="CompletionTable.Wait"/> on a CPU that a busy thread wants too.</summary>
public class CompletionTableBlockingWaitTests
{
    private const CompletionStage Remote = CompletionStage.RemoteAccepted;

    /// <remarks>
    /// <para>
    /// The waiting thread and a busy loop are pinned to the same CPU, and a third thread completes the stage 200 us after
    /// it saw the token, long after the table's short spin has ended. A wait that has parked is woken by the completion and
    /// takes the CPU back from the busy loop at once. A wait that is still spinning with <see cref="Thread.Yield"/> (a
    /// <see cref="ManualResetEventSlim"/> with its default spin count does 25 rounds of yields and spins before it blocks)
    /// has handed the CPU to the busy loop, and nothing can wake a thread that has not parked: it runs again when the busy
    /// loop's time slice ends. On a machine with no free core every thread is in that position, and a stress test that
    /// waits this way ran several times longer on four loaded cores (docs/benchmarks/threading.md section 7).
    /// </para>
    /// <para>
    /// The waiter and the busy loop both run at the highest thread priority. Once woken, the waiter (boosted above the
    /// busy loop by the wake-up) preempts it and every normal-priority thread: in a full test run other tests' threads,
    /// boosted by their own wake-ups, would otherwise hold its CPU for a quantum and delay even a parked waiter. A yielding
    /// waiter gives the CPU to a busy loop of equal priority for the whole of its quantum, which is a fixed amount of CPU
    /// time (a busy loop of lower priority would lose the CPU again at the next clock tick, and the clock may tick every
    /// millisecond when another application asked for that).
    /// </para>
    /// <para>
    /// The measured latency runs from just before the completion to the waiter's return, so a completer delayed by the
    /// machine's load does not count. The median of 21 rounds must stay under 1 ms: a parked wait takes microseconds, a
    /// yielding one a quantum (about 31 ms here). Windows only (thread affinity through kernel32), and the process must be
    /// allowed at least two CPUs.
    /// </para>
    /// </remarks>
    [Fact]
    public void A_Blocking_Wait_Parks_Instead_Of_Handing_Its_Cpu_To_A_Busy_Thread()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Threads are pinned through kernel32.");
        ulong allowed;
        using (Process self = Process.GetCurrentProcess())
        {
            allowed = (ulong)self.ProcessorAffinity;
        }

        ulong shared = 1UL << (63 - System.Numerics.BitOperations.LeadingZeroCount(allowed));
        ulong others = allowed & ~shared;
        Assert.SkipWhen(others == 0, "The process is allowed a single CPU.");

        const int rounds = 21;
        long delayTicks = Stopwatch.Frequency / 5_000;
        var table = new CompletionTable(4);
        var latencies = new double[rounds];
        var statuses = new DeliveryStatus[rounds];
        long request = 0;
        long completedAt = 0;
        int stop = 0;
        SendToken current = default;
        Exception? failure = null;

        var busy = new Thread(() =>
        {
            Pin(shared);
            while (Volatile.Read(ref stop) == 0)
            {
            }
        })
        { IsBackground = true, Name = "busy", Priority = ThreadPriority.Highest };

        var completer = new Thread(() =>
        {
            try
            {
                Pin(others);
                long seen = 0;
                while (true)
                {
                    long next;
                    while ((next = Volatile.Read(ref request)) == seen)
                    {
                        if (Volatile.Read(ref stop) != 0)
                            return;
                        Thread.SpinWait(8);
                    }

                    seen = next;
                    SendToken token = current;
                    long until = Stopwatch.GetTimestamp() + delayTicks;
                    while (Stopwatch.GetTimestamp() < until)
                        Thread.SpinWait(8);
                    Volatile.Write(ref completedAt, Stopwatch.GetTimestamp());
                    table.Complete(token, Remote, DeliveryStatus.Delivered);
                }
            }
            catch (Exception e)
            {
                failure = e;
            }
        })
        { IsBackground = true, Name = "completer" };

        var waiter = new Thread(() =>
        {
            try
            {
                Pin(shared);
                for (int i = 0; i < rounds; i++)
                {
                    Assert.True(table.TryAllocate(out SendToken token));
                    current = token;
                    Volatile.Write(ref request, i + 1);
                    statuses[i] = table.Wait(token, Remote, TimeSpan.FromSeconds(10));
                    long woke = Stopwatch.GetTimestamp();
                    latencies[i] = Stopwatch.GetElapsedTime(Volatile.Read(ref completedAt), woke).TotalMilliseconds;
                    table.Release(token);
                }
            }
            catch (Exception e)
            {
                failure = e;
            }
        })
        { IsBackground = true, Name = "waiter", Priority = ThreadPriority.Highest };

        busy.Start();
        completer.Start();
        waiter.Start();
        waiter.Join();
        Volatile.Write(ref stop, 1);
        completer.Join();
        busy.Join();

        Assert.True(failure is null, $"A thread failed: {failure}");
        Assert.All(statuses, s => Assert.Equal(DeliveryStatus.Delivered, s));
        double[] sorted = (double[])latencies.Clone();
        Array.Sort(sorted);
        double median = sorted[rounds / 2];
        string report = $"Median wake-up {median:F3} ms after the completion; every round (ms): {string.Join(", ", latencies.Select(l => l.ToString("F3", System.Globalization.CultureInfo.InvariantCulture)))}";
        TestContext.Current.TestOutputHelper?.WriteLine(report);
        Assert.True(median < 1, report);
    }

    private static void Pin(ulong mask)
    {
        if (SetThreadAffinityMask(GetCurrentThread(), (nuint)mask) == 0)
            throw new InvalidOperationException($"Cannot pin a thread to CPU mask 0x{mask:X}.");
    }

    [DllImport("kernel32.dll")]
    private static extern nint GetCurrentThread();

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern nuint SetThreadAffinityMask(nint thread, nuint mask);
}
