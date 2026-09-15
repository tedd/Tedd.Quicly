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
    [Fact]
    public void Review_Late_Complete_Racing_Release_And_Reallocation_Must_Not_Touch_The_New_Occupant()
    {
        const int pairs = 8;
        const int iterationsPerPair = 400_000;
        int corrupted = 0;
        var workers = new Thread[pairs];

        for (int p = 0; p < pairs; p++)
        {
            int seed = p;
            workers[p] = new Thread(() =>
            {
                var table = new CompletionTable(1);
                long published = 0;
                int ack = 0;
                bool stop = false;
                var random = new Random(seed);

                var transport = new Thread(() =>
                {
                    SpinWait spinner = default;
                    while (!Volatile.Read(ref stop))
                    {
                        long packed = Interlocked.Exchange(ref published, 0);
                        if (packed == 0)
                        {
                            spinner.SpinOnce(sleep1Threshold: -1);
                            continue;
                        }

                        spinner.Reset();
                        table.Complete(Unpack(packed), Buffer, DeliveryStatus.Delivered);
                        Volatile.Write(ref ack, 1);
                    }
                })
                { IsBackground = true, Name = "transport-" + seed };
                transport.Start();

                int localCorrupted = 0;
                for (int i = 0; i < iterationsPerPair; i++)
                {
                    Assert.True(table.TryAllocate(out SendToken old));
                    Volatile.Write(ref ack, 0);
                    Volatile.Write(ref published, Pack(old));

                    // Sweep the release across the transport's Complete call.
                    Thread.SpinWait(random.Next(0, 96));

                    // Owner gives up on the send and immediately reuses the slot for the next one.
                    table.Release(old);
                    SendToken fresh;
                    while (!table.TryAllocate(out fresh))
                        Thread.SpinWait(1);

                    // Let the late Complete(old) finish, then look at the untouched fresh token.
                    while (Volatile.Read(ref ack) == 0)
                        Thread.SpinWait(1);

                    if (table.IsCompleted(fresh, Buffer) || table.GetStatus(fresh) != DeliveryStatus.Pending)
                        localCorrupted++;

                    table.Release(fresh);
                }

                Volatile.Write(ref stop, true);
                transport.Join();
                Interlocked.Add(ref corrupted, localCorrupted);
            })
            { IsBackground = true, Name = "owner-" + p };
        }

        foreach (Thread t in workers)
            t.Start();
        foreach (Thread t in workers)
            t.Join();

        Assert.Equal(0, corrupted);
    }
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
    [Fact]
    public void TryAllocate_Succeeds_Whenever_Available_Is_Positive_While_A_Release_Is_Being_Published()
    {
        const int iterations = 200_000;
        var table = new CompletionTable(1);
        long published = 0;
        bool stop = false;
        int spuriousExhaustion = 0;

        var transport = new Thread(() =>
        {
            SpinWait spinner = default;
            while (!Volatile.Read(ref stop))
            {
                long packed = Interlocked.Exchange(ref published, 0);
                if (packed == 0)
                {
                    spinner.SpinOnce(sleep1Threshold: -1);
                    continue;
                }

                spinner.Reset();
                SendToken token = Unpack(packed);
                table.Complete(token, Buffer, DeliveryStatus.Pending);
                table.Complete(token, Remote, DeliveryStatus.Delivered); // releases the slot from this thread
            }
        })
        { IsBackground = true, Name = "transport" };
        transport.Start();

        for (int i = 0; i < iterations; i++)
        {
            SendToken token;
            while (true)
            {
                // Once Available says the slot is back, the release has been claimed and TryAllocate must succeed.
                bool expectSuccess = table.Available != 0;
                if (table.TryAllocate(out token))
                    break;
                if (expectSuccess)
                    spuriousExhaustion++;
                Thread.SpinWait(1);
            }

            Volatile.Write(ref published, Pack(token));
        }

        // Drain the last release so the table is whole again.
        while (table.Available != 1)
            Thread.SpinWait(1);
        Volatile.Write(ref stop, true);
        transport.Join();

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
    [Fact]
    public void Losing_Concurrent_Complete_Never_Writes_Its_Status()
    {
        const int iterations = 20_000;
        var table = new CompletionTable(1);
        long published = 0;
        bool stop = false;
        int mismatches = 0;

        var rival = new Thread(() =>
        {
            SpinWait spinner = default;
            while (!Volatile.Read(ref stop))
            {
                long packed = Interlocked.Exchange(ref published, 0);
                if (packed == 0)
                {
                    spinner.SpinOnce(sleep1Threshold: -1);
                    continue;
                }

                spinner.Reset();
                table.Complete(Unpack(packed), Remote, DeliveryStatus.Failed);
            }
        })
        { IsBackground = true, Name = "rival" };
        rival.Start();

        for (int i = 0; i < iterations; i++)
        {
            Assert.True(table.TryAllocate(out SendToken token));
            Volatile.Write(ref published, Pack(token));
            table.Complete(token, Remote, DeliveryStatus.Delivered);

            // Whoever won, the status must agree with the completed stage's outcome for as long as the slot
            // is live, and the ignored call must not have changed it.
            DeliveryStatus first = table.GetStatus(token);
            while (Volatile.Read(ref published) != 0)
                Thread.SpinWait(1);
            Thread.SpinWait(20);
            if (first is not (DeliveryStatus.Delivered or DeliveryStatus.Failed) || table.GetStatus(token) != first)
                mismatches++;

            table.Complete(token, Buffer, DeliveryStatus.Pending);
            Assert.Equal(first, table.GetStatus(token)); // remembered as the released status
        }

        Volatile.Write(ref stop, true);
        rival.Join();
        Assert.Equal(0, mismatches);
        Assert.Equal(1, table.Available);
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
