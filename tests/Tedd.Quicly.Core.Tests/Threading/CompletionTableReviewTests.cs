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
