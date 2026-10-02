using System.Diagnostics;
using System.Globalization;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Review (perf lens) of 3d60a10, the blocking <see cref="CompletionTable.Wait"/> whose event no longer spins: the idle cost
/// is not confined to "a completion within 10–20 µs that a send's stages practically never hit".
/// </summary>
public class ReviewCwaitperfIdleWakeTests
{
    private const CompletionStage Remote = CompletionStage.RemoteAccepted;

    /// <remarks>
    /// <para>
    /// The table's own spin (ten <see cref="SpinWait"/> rounds that never yield) ends about 3 µs into the wait. With the
    /// event's spin count at 0 the waiter then parks, and a completion that arrives between ~3 and ~14 µs pays a kernel
    /// wake-up: +6–8 µs per wait measured in a paired in-process A/B on CPUs 16–23 (+7.2–7.7 µs at 5, 8, 10 and 12 µs,
    /// 20 of 20 blocks each, three launches; A/A 0.00 µs). That is exactly where the library's own fast path lands: over
    /// MsQuic loopback a tracked datagram's <c>QuiclyPeer.Wait(BufferReleased)</c> completes 7–13 µs after Flush; its
    /// median went from 10.8–11.3 µs (old event) to 17.6–20.3 µs (spin count 0), paired block-median difference +6.7 to
    /// +8.2 µs, B above A in 30 of 30 blocks in each of three launches. A bounded spin that never yields (up to 20 µs,
    /// then park with spin count 0) measured the same as the old event idle and the same as spin count 0 under eight busy
    /// loops (no lost quantum), so the regression is not the price of the loaded fix.
    /// </para>
    /// <para>
    /// Measured: the completer completes the stage 6 µs after it saw the token; the time from just before the completion
    /// to the waiter's return, paired with a reference round in which the waiter spins (never yielding) on a flag the same
    /// completer sets after the same delay. A wait that is still spinning when the completion lands is within a fraction of
    /// a microsecond of the reference; one that has parked is a kernel wake-up (6–11 µs) behind it. The median of the paired
    /// differences must stay under 3 µs. Both threads run at the highest priority so other tests' threads rarely
    /// interrupt them; the median absorbs the rounds they do.
    /// </para>
    /// </remarks>
    [Fact]
    public void A_Blocking_Wait_Completed_Six_Microseconds_In_Is_Not_Delayed_By_A_Kernel_Wake_Up()
    {
        Assert.SkipWhen(Environment.ProcessorCount < 2, "The waiter and the completer need a CPU each.");

        const int pairs = 201;
        long delayTicks = 6 * Stopwatch.Frequency / 1_000_000;
        var table = new CompletionTable(4);
        var tableLatencies = new double[pairs];
        var referenceLatencies = new double[pairs];
        var statuses = new DeliveryStatus[pairs];
        long request = 0;
        long completedAt = 0;
        int flag = 0;
        int stop = 0;
        SendToken current = default;
        Exception? failure = null;

        // Odd requests complete the table's current token, even ones raise the reference flag.
        var completer = new Thread(() =>
        {
            try
            {
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
                        Volatile.Write(ref flag, 1);
                }
            }
            catch (Exception e)
            {
                failure = e;
            }
        })
        { IsBackground = true, Name = "completer", Priority = ThreadPriority.Highest };

        var waiter = new Thread(() =>
        {
            try
            {
                long next = 0;
                // Warm-up: the slots' events exist and the code is jitted before the measured rounds.
                for (int i = 0; i < 2 * pairs + 50; i++)
                {
                    Round(measure: i >= 50 + pairs ? i - 50 - pairs : -1);
                }

                void Round(int measure)
                {
                    Assert.True(table.TryAllocate(out SendToken token));
                    current = token;
                    Volatile.Write(ref request, ++next);
                    DeliveryStatus status = table.Wait(token, Remote, TimeSpan.FromSeconds(10));
                    long woke = Stopwatch.GetTimestamp();
                    double tableUs = Stopwatch.GetElapsedTime(Volatile.Read(ref completedAt), woke).TotalMicroseconds;
                    table.Release(token);

                    Volatile.Write(ref flag, 0);
                    Volatile.Write(ref request, ++next);
                    while (Volatile.Read(ref flag) == 0)
                        Thread.SpinWait(1);
                    woke = Stopwatch.GetTimestamp();
                    double referenceUs = Stopwatch.GetElapsedTime(Volatile.Read(ref completedAt), woke).TotalMicroseconds;
                    if (measure >= 0)
                    {
                        statuses[measure] = status;
                        tableLatencies[measure] = tableUs;
                        referenceLatencies[measure] = referenceUs;
                    }
                }
            }
            catch (Exception e)
            {
                failure = e;
            }
        })
        { IsBackground = true, Name = "waiter", Priority = ThreadPriority.Highest };

        completer.Start();
        waiter.Start();
        waiter.Join();
        Volatile.Write(ref stop, 1);
        completer.Join();

        Assert.True(failure is null, $"A thread failed: {failure}");
        Assert.All(statuses, s => Assert.Equal(DeliveryStatus.Delivered, s));
        double[] differences = new double[pairs];
        for (int i = 0; i < pairs; i++)
            differences[i] = tableLatencies[i] - referenceLatencies[i];
        Array.Sort(differences);
        double median = differences[pairs / 2];
        double[] sortedTable = [.. tableLatencies.Order()];
        double[] sortedReference = [.. referenceLatencies.Order()];
        string report = string.Create(CultureInfo.InvariantCulture,
            $"Median of (table - reference) {median:F2} us (p25 {differences[pairs / 4]:F2}, p75 {differences[3 * pairs / 4]:F2}); table wake after completion median {sortedTable[pairs / 2]:F2} us, reference {sortedReference[pairs / 2]:F2} us");
        TestContext.Current.TestOutputHelper?.WriteLine(report);
        Assert.True(median < 3, report);
    }
}
