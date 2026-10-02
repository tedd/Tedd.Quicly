using System.Diagnostics;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Review (contract lens, branch fix/completion-wait-spin): two blocking waits on the two stages of one slot share the slot's
/// single <see cref="ManualResetEventSlim"/>.
/// </summary>
/// <remarks>
/// <para>
/// When the first stage completes, <c>Set</c> pulses both parked waiters. If the second-stage waiter re-acquires the event's
/// lock first, it finds its own stage still pending, loops and calls <c>Reset</c> before the first-stage waiter has
/// re-checked <c>IsSet</c>; that waiter then goes back into <c>Monitor.Wait</c> and sleeps until its timeout (or until the
/// other stage completes), although its stage completed long before. The status it finally returns is right; only the
/// wake-up is lost.
/// </para>
/// <para>
/// Pre-existing: the same failure rate was measured on main (5ab528b, event spin count 35) and on 04cf395 (spin count 0),
/// roughly a quarter of the rounds on CPUs 16-23. The table's contract makes the blocking <c>Wait</c> owner-thread only, so
/// two concurrent blocking waits are outside it; but neither <c>CompletionTable.Wait</c> nor <c>QuiclyPeer.Wait</c> rejects
/// or documents the case, and <c>QuiclyPeer.Wait</c> tells PollOnly users to wait on a thread other than the poller.
/// </para>
/// </remarks>
public class ReviewCwaitcontractTwoWaiterTests
{
    [Fact]
    public void A_Completed_Stage_Wakes_Its_Blocking_Waiter_While_The_Other_Stage_Is_Also_Waited_On()
    {
        const int rounds = 60;
        var timeout = TimeSpan.FromMilliseconds(300);
        var table = new CompletionTable(2);
        for (int i = 0; i < rounds; i++)
        {
            Assert.True(table.TryAllocate(out SendToken token));
            using var started = new CountdownEvent(2);
            long bufferWoke = 0;
            var bufferWaiter = new Thread(() =>
            {
                started.Signal();
                _ = table.Wait(token, CompletionStage.BufferReleased, timeout);
                Volatile.Write(ref bufferWoke, Stopwatch.GetTimestamp());
            });
            var remoteWaiter = new Thread(() =>
            {
                started.Signal();
                _ = table.Wait(token, CompletionStage.RemoteAccepted, timeout * 3);
            });
            bufferWaiter.Start();
            remoteWaiter.Start();
            started.Wait();
            Thread.Sleep(2); // both are parked by now (the table's spin lasts microseconds)

            long completedAt = Stopwatch.GetTimestamp();
            table.Complete(token, CompletionStage.BufferReleased, DeliveryStatus.Pending);
            bufferWaiter.Join();
            double ms = Stopwatch.GetElapsedTime(completedAt, Volatile.Read(ref bufferWoke)).TotalMilliseconds;

            table.Complete(token, CompletionStage.RemoteAccepted, DeliveryStatus.Delivered);
            remoteWaiter.Join();

            Assert.True(ms < timeout.TotalMilliseconds / 2,
                $"Round {i}: the BufferReleased waiter returned {ms:F1} ms after its stage completed (timeout {timeout.TotalMilliseconds} ms): its wake-up was reset away by the RemoteAccepted waiter.");
        }
    }
}
