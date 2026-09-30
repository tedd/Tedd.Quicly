using System.Diagnostics;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Tests.Threading;

/// <summary>
/// Review tests: races between an owner-thread <see cref="CompletionTable.Release"/> + re-allocation and a
/// late <see cref="CompletionTable.Complete"/> from a transport thread.
/// </summary>
public class CompletionTableReviewTests
{
    private const CompletionStage Buffer = CompletionStage.BufferReleased;

    private static long Pack(SendToken token) => ((long)token.Generation << 32) | (uint)token.Slot;

    private static SendToken Unpack(long packed) => new((int)(packed & 0xFFFF_FFFF), (uint)(packed >> 32));

    /// <summary>
    /// The generation check in <c>CompletionTable.Complete</c> is not atomic with the state transition inside
    /// <c>Slot.Complete</c>. If the owner releases the token and re-allocates the same slot between the two,
    /// the late completion lands on the <em>new</em> occupant: the fresh token starts out with a completed
    /// stage and the previous send's status. Any thread may call Complete while the owner calls Release, so
    /// this must never happen.
    /// </summary>
    /// <remarks>
    /// The window is a Complete that took its token before the owner's Release and has not returned when the slot has been
    /// re-allocated. A transport thread polls for the token in a hot loop, so that its Complete starts a nearly fixed time
    /// after the owner publishes, and the owner sweeps its Release across that Complete with a random delay: both threads
    /// of a pair have to run at once. There is therefore one pair per two logical processors (at most eight), sharing the
    /// 3.2 million iterations that found the race, and a thread that waits longer than a hand-off takes while both run
    /// blocks instead of spinning (<see cref="HandOff"/>), so that on a machine with no core to spare a hand-off costs a
    /// thread wake-up rather than a scheduler quantum (which made the run take many minutes). The run ends after its
    /// iterations or after <see cref="Budget"/>, and must have hit the window at least <see cref="RequiredOverlaps"/> times.
    /// </remarks>
    [Fact]
    public void Review_Late_Complete_Racing_Release_And_Reallocation_Must_Not_Touch_The_New_Occupant()
    {
        int pairs = Math.Clamp(Environment.ProcessorCount / 2, 1, 8);
        int iterationsPerPair = TotalIterations / pairs;
        long start = Stopwatch.GetTimestamp();
        long deadline = start + (long)(Budget.TotalSeconds * Stopwatch.Frequency);
        int corrupted = 0;
        long iterations = 0;
        long overlaps = 0;
        Exception? failure = null;
        var workers = new Thread[pairs];

        for (int p = 0; p < pairs; p++)
        {
            int seed = p;
            workers[p] = new Thread(() =>
            {
                var table = new CompletionTable(1);
                using var published = new HandOff();
                using var completed = new HandOff();
                var random = new Random(seed);

                var transport = new Thread(() =>
                {
                    try
                    {
                        long packed;
                        while ((packed = published.Take()) != Stop)
                        {
                            table.Complete(Unpack(packed), Buffer, DeliveryStatus.Delivered);
                            completed.Put(1);
                        }
                    }
                    catch (Exception e)
                    {
                        Interlocked.CompareExchange(ref failure, e, null);
                    }
                })
                { IsBackground = true, Name = "transport-" + seed };
                transport.Start();

                int localCorrupted = 0;
                int localOverlaps = 0;
                int i = 0;
                try
                {
                    for (; i < iterationsPerPair && ((i & 255) != 0 || Stopwatch.GetTimestamp() < deadline); i++)
                    {
                        Assert.True(table.TryAllocate(out SendToken old));
                        published.Put(Pack(old));

                        // Sweep the release across the transport's Complete call.
                        Thread.SpinWait(random.Next(0, 96));

                        // Owner gives up on the send and immediately reuses the slot for the next one. A Complete(old) that
                        // had taken its token before the Release and has not returned after the re-allocation spans both.
                        bool taken = !published.HasValue;
                        table.Release(old);
                        SendToken fresh;
                        while (!table.TryAllocate(out fresh))
                            Thread.SpinWait(1);
                        if (taken && !completed.HasValue)
                            localOverlaps++;

                        // Let the late Complete(old) finish, then look at the untouched fresh token.
                        completed.Take();
                        if (table.IsCompleted(fresh, Buffer) || table.GetStatus(fresh) != DeliveryStatus.Pending)
                            localCorrupted++;

                        table.Release(fresh);
                    }
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                }
                finally
                {
                    published.Put(Stop);
                    transport.Join();
                }

                Interlocked.Add(ref corrupted, localCorrupted);
                Interlocked.Add(ref iterations, i);
                Interlocked.Add(ref overlaps, localOverlaps);
            })
            { IsBackground = true, Name = "owner-" + p };
        }

        foreach (Thread t in workers)
            t.Start();
        foreach (Thread t in workers)
            t.Join();

        string run = $"{pairs} pairs, {iterations:N0} iterations in {Stopwatch.GetElapsedTime(start).TotalSeconds:F1} s, "
            + $"{overlaps:N0} Completes in flight across a Release and the re-allocation";
        TestContext.Current.TestOutputHelper?.WriteLine(run);
        Assert.True(failure is null, $"A thread of the run failed: {failure}");
        Assert.True(corrupted == 0, $"{corrupted} fresh tokens were touched by a late Complete ({run}).");
        Assert.True(overlaps >= RequiredOverlaps, $"The run did not hit the race window {RequiredOverlaps:N0} times ({run}).");
    }

    /// <summary>Iterations of the race test, shared by its pairs: the count that found the race (275 corruptions).</summary>
    private const int TotalIterations = 3_200_000;

    /// <summary>Wall-clock time after which the race test stops, whatever its iteration count.</summary>
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(20);

    /// <summary>
    /// How often the race test must have hit its window. Against the table before the fix (0c8be2b), one hit in six to
    /// twelve was a corruption, on sixteen idle cores as on four cores with two busy-looping processes per core; the fixed
    /// table gets about one hit per thousand iterations.
    /// </summary>
    private const int RequiredOverlaps = 500;

    /// <summary>Handed to a transport thread to end it (generation 0x80000000 is even, so no token packs to it).</summary>
    private const long Stop = long.MinValue;
}

/// <summary>
/// Follow-ups from the same review: exhaustion is only reported when the free list is genuinely empty, and
/// a completion that loses (or is ignored) never changes the status.
/// </summary>
public class CompletionTableReviewFollowUpTests
{
    private const CompletionStage Buffer = CompletionStage.BufferReleased;
    private const CompletionStage Remote = CompletionStage.RemoteAccepted;

    private static long Pack(SendToken token) => ((long)token.Generation << 32) | (uint)token.Slot;

    private static SendToken Unpack(long packed) => new((int)(packed & 0xFFFF_FFFF), (uint)(packed >> 32));

    /// <summary>
    /// A completing thread returns the slot to the free list in two steps (claim a place, publish the index).
    /// <see cref="CompletionTable.Available"/> counts the slot as soon as the place is claimed, so
    /// <see cref="CompletionTable.TryAllocate"/> must wait out the publish rather than report exhaustion.
    /// </summary>
    /// <remarks>
    /// The owner spins on TryAllocate while the transport thread releases the slot: that is the window. The transport
    /// thread takes each token through a hand-off that spins and then blocks, and an owner that has spun for far longer
    /// than a release takes while both threads run waits for the release instead (<see cref="HandOff"/>), so that on a
    /// machine with no core to spare a hand-off costs a thread wake-up rather than a scheduler quantum.
    /// </remarks>
    [Fact]
    public void TryAllocate_Succeeds_Whenever_Available_Is_Positive_While_A_Release_Is_Being_Published()
    {
        const int iterations = 200_000;
        const long stop = long.MinValue;
        long spinTicks = Stopwatch.Frequency / 20_000;
        var table = new CompletionTable(1);
        using var published = new HandOff();
        using var released = new HandOff();
        int spuriousExhaustion = 0;
        Exception? transportFailure = null;

        var transport = new Thread(() =>
        {
            try
            {
                long packed;
                while ((packed = published.Take()) != stop)
                {
                    SendToken token = Unpack(packed);
                    table.Complete(token, Buffer, DeliveryStatus.Pending);
                    table.Complete(token, Remote, DeliveryStatus.Delivered); // releases the slot from this thread
                    released.Put(1);
                }
            }
            catch (Exception e)
            {
                transportFailure = e;
            }
        })
        { IsBackground = true, Name = "transport" };
        transport.Start();

        try
        {
            for (int i = 0; i < iterations; i++)
            {
                SendToken token;
                long spinUntil = Stopwatch.GetTimestamp() + spinTicks;
                for (int attempt = 1; ; attempt++)
                {
                    // Once Available says the slot is back, the release has been claimed and TryAllocate must succeed.
                    bool expectSuccess = table.Available != 0;
                    if (table.TryAllocate(out token))
                        break;
                    if (expectSuccess)
                        spuriousExhaustion++;
                    if ((attempt & 15) != 0 || Stopwatch.GetTimestamp() < spinUntil)
                    {
                        Thread.SpinWait(1);
                        continue;
                    }

                    // The transport thread has no core: wait for its release (a hand-off it made earlier may still be
                    // there, in which case this returns at once and the owner spins once more).
                    released.Take();
                    spinUntil = Stopwatch.GetTimestamp() + spinTicks;
                }

                published.Put(Pack(token));
            }

            // Drain the last release so the table is whole again.
            while (table.Available != 1)
                released.Take();
        }
        finally
        {
            published.Put(stop);
            transport.Join();
        }

        Assert.True(transportFailure is null, $"The transport thread failed: {transportFailure}");
        Assert.Equal(0, spuriousExhaustion);
        Assert.True(table.TryAllocate(out _));
        Assert.Equal(0, table.Available);
    }

    [Fact]
    public void TryAllocate_Reports_Exhaustion_When_No_Release_Is_In_Flight()
    {
        var table = new CompletionTable(2);
        Assert.True(table.TryAllocate(out SendToken first));
        Assert.True(table.TryAllocate(out SendToken second));
        Assert.False(table.TryAllocate(out SendToken none));
        Assert.False(none.IsValid);

        table.Release(first);
        Assert.True(table.TryAllocate(out SendToken third));
        Assert.Equal(first.Slot, third.Slot);
        Assert.False(table.TryAllocate(out _));
        table.Release(second);
        table.Release(third);
        Assert.Equal(2, table.Available);
    }

    /// <summary>
    /// Two threads complete the same stage with different statuses; exactly one transition wins and the
    /// status is the winner's. The loser's status must never be observable, not even transiently.
    /// </summary>
    /// <remarks>
    /// Each thread waits a random 0 to 7 spin iterations before its Complete, so that either may come first and the two
    /// often meet (against the table before the fix, which wrote a losing call's status, this found 10 to 51 bad statuses
    /// per run; racing without the delays found 0 to 6, and none in four runs of nine), and each must have won at least
    /// 1 000 times. The rival thread takes each token through a hand-off that spins and then blocks, and hands back when its
    /// Complete has returned (<see cref="HandOff"/>): on a machine with no core to spare a hand-off costs a thread wake-up
    /// rather than a scheduler quantum, and the second look at the status comes after both calls.
    /// </remarks>
    [Fact]
    public void Losing_Concurrent_Complete_Never_Writes_Its_Status()
    {
        const int iterations = 20_000;
        const int requiredWins = 1_000;
        const long stop = long.MinValue;
        var table = new CompletionTable(1);
        using var published = new HandOff();
        using var rivalDone = new HandOff();
        var random = new Random(1);
        int mismatches = 0;
        int rivalWins = 0;
        Exception? rivalFailure = null;

        var rival = new Thread(() =>
        {
            var rivalRandom = new Random(2);
            try
            {
                long packed;
                while ((packed = published.Take()) != stop)
                {
                    Thread.SpinWait(rivalRandom.Next(0, 8));
                    table.Complete(Unpack(packed), Remote, DeliveryStatus.Failed);
                    rivalDone.Put(1);
                }
            }
            catch (Exception e)
            {
                rivalFailure = e;
            }
        })
        { IsBackground = true, Name = "rival" };
        rival.Start();

        try
        {
            for (int i = 0; i < iterations; i++)
            {
                Assert.True(table.TryAllocate(out SendToken token));
                published.Publish(Pack(token));
                Thread.SpinWait(random.Next(0, 8));
                table.Complete(token, Remote, DeliveryStatus.Delivered);

                // Whoever won, the status must agree with the completed stage's outcome for as long as the slot
                // is live, and the ignored call must not have changed it.
                DeliveryStatus first = table.GetStatus(token);
                published.WakeIfBlocked();
                rivalDone.Take();
                if (first is not (DeliveryStatus.Delivered or DeliveryStatus.Failed) || table.GetStatus(token) != first)
                    mismatches++;
                if (first == DeliveryStatus.Failed)
                    rivalWins++;

                table.Complete(token, Buffer, DeliveryStatus.Pending);
                Assert.Equal(first, table.GetStatus(token)); // remembered as the released status
            }
        }
        finally
        {
            published.Put(stop);
            rival.Join();
        }

        Assert.True(rivalFailure is null, $"The rival thread failed: {rivalFailure}");
        Assert.Equal(0, mismatches);
        Assert.Equal(1, table.Available);
        Assert.True(rivalWins >= requiredWins && iterations - rivalWins >= requiredWins,
            $"The two calls did not race: the rival won {rivalWins:N0} of {iterations:N0} times.");
    }

    [Fact]
    public void Stale_Token_Never_Observes_The_New_Occupant()
    {
        var table = new CompletionTable(1);
        Assert.True(table.TryAllocate(out SendToken old));
        table.Complete(old, Buffer, DeliveryStatus.Superseded);
        table.Release(old);

        Assert.True(table.TryAllocate(out SendToken fresh));
        ValueTask<DeliveryStatus> freshWait = table.WaitAsync(fresh, Remote);
        table.Complete(fresh, Buffer, DeliveryStatus.Pending);

        // The stale token sees its own released status everywhere and cannot complete or wait on the fresh slot.
        table.Complete(old, Remote, DeliveryStatus.Delivered);
        Assert.False(table.IsCompleted(fresh, Remote));
        Assert.False(freshWait.IsCompleted);
        Assert.Equal(DeliveryStatus.Pending, table.GetStatus(fresh));
        Assert.Equal(DeliveryStatus.Superseded, table.GetStatus(old));
        Assert.True(table.IsCompleted(old, Remote));
        Assert.Equal(DeliveryStatus.Superseded, table.WaitAsync(old, Remote).Result);
        Assert.Equal(DeliveryStatus.Superseded, table.Wait(old, Remote, TimeSpan.FromMilliseconds(50)));
        table.Release(old);
        Assert.False(table.IsCompleted(fresh, Remote));

        table.Complete(fresh, Remote, DeliveryStatus.Delivered);
        Assert.Equal(DeliveryStatus.Delivered, freshWait.Result);
        Assert.Equal(1, table.Available);
    }
}
