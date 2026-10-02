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
    /// The waiter and the busy loop both run at the highest thread priority. Once woken, the waiter (boosted above the busy
    /// loop by the wake-up) preempts it and every normal-priority thread. A yielding waiter gives the CPU to a busy loop of
    /// equal priority for the whole of its quantum, which is a fixed amount of CPU time (a busy loop of lower priority would
    /// lose the CPU again at the next clock tick, and the clock ticks every millisecond while some application asks for it).
    /// </para>
    /// <para>
    /// What is measured is the time from just before the completion to the waiter's return. Rounds alternate between the
    /// table and a reference: a <see cref="ManualResetEventSlim"/> with spin count 0, which parks at once, set by the same
    /// completer. In a full test run the whole process is paused now and then (garbage collections, other tests' threads),
    /// which delays both kinds of round alike; the median of the paired differences (table minus the reference round next
    /// to it) must stay under 5 ms. A parked table wait is within microseconds of the reference; a yielding one is a
    /// quantum (about 31 ms) behind it. Windows only (thread affinity through kernel32), and the process must be allowed
    /// at least two CPUs.
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

        const int pairs = 21;
        long delayTicks = Stopwatch.Frequency / 5_000;
        var table = new CompletionTable(4);
        using var reference = new ManualResetEventSlim(false, spinCount: 0);
        var tableLatencies = new double[pairs];
        var referenceLatencies = new double[pairs];
        var statuses = new DeliveryStatus[pairs];
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

        // Odd requests complete the table's current token, even ones set the reference event.
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
                    if ((next & 1) != 0)
                        table.Complete(token, Remote, DeliveryStatus.Delivered);
                    else
                        reference.Set();
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
                long next = 0;
                for (int i = 0; i < pairs; i++)
                {
                    Assert.True(table.TryAllocate(out SendToken token));
                    current = token;
                    Volatile.Write(ref request, ++next);
                    statuses[i] = table.Wait(token, Remote, TimeSpan.FromSeconds(10));
                    long woke = Stopwatch.GetTimestamp();
                    tableLatencies[i] = Stopwatch.GetElapsedTime(Volatile.Read(ref completedAt), woke).TotalMilliseconds;
                    table.Release(token);

                    reference.Reset();
                    Volatile.Write(ref request, ++next);
                    Assert.True(reference.Wait(TimeSpan.FromSeconds(10)));
                    woke = Stopwatch.GetTimestamp();
                    referenceLatencies[i] = Stopwatch.GetElapsedTime(Volatile.Read(ref completedAt), woke).TotalMilliseconds;
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
        double[] differences = new double[pairs];
        for (int i = 0; i < pairs; i++)
            differences[i] = tableLatencies[i] - referenceLatencies[i];
        Array.Sort(differences);
        double median = differences[pairs / 2];
        string report = string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"Median of (table - reference) {median:F3} ms; table (ms): {string.Join(", ", tableLatencies.Select(l => l.ToString("F3", System.Globalization.CultureInfo.InvariantCulture)))}; reference (ms): {string.Join(", ", referenceLatencies.Select(l => l.ToString("F3", System.Globalization.CultureInfo.InvariantCulture)))}");
        TestContext.Current.TestOutputHelper?.WriteLine(report);
        Assert.True(median < 5, report);
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
