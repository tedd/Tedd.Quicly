using System.Diagnostics;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Threading;

// xUnit1031: every blocking .Result below is on a ValueTask the test has just proven complete (or that the
// table completes synchronously); the point is to exercise the synchronous completion path without an async
// state machine.
#pragma warning disable xUnit1031

namespace Tedd.Quicly.Core.Tests.Threading;

public class CompletionTableTests
{
    private const CompletionStage Buffer = CompletionStage.BufferReleased;
    private const CompletionStage Remote = CompletionStage.RemoteAccepted;

    /// <summary>
    /// Pre-allocated awaiter: registers one cached continuation on a pending ValueTask and waits until it has run, spinning
    /// first and then blocked (<see cref="HandOff"/>), so that waiting for a thread that has no core costs a wake-up rather
    /// than a scheduler quantum. Lets tests drive the IValueTaskSource path without an async state machine, and allocates
    /// nothing on the waiting thread.
    /// </summary>
    private sealed class HandOffAwaiter
    {
        private readonly HandOff _ran = new();
        private readonly Action _continuation;

        public HandOffAwaiter() => _continuation = () => _ran.Put(1);

        public DeliveryStatus Await(ValueTask<DeliveryStatus> task)
        {
            ValueTaskAwaiter<DeliveryStatus> awaiter = task.GetAwaiter();
            if (!awaiter.IsCompleted)
            {
                awaiter.UnsafeOnCompleted(_continuation);
                _ran.Take();
            }

            return awaiter.GetResult();
        }

        /// <summary>Attaches the continuation now; the caller triggers completion afterwards and then calls <see cref="Finish"/>.</summary>
        public ValueTaskAwaiter<DeliveryStatus> Register(ValueTask<DeliveryStatus> task)
        {
            ValueTaskAwaiter<DeliveryStatus> awaiter = task.GetAwaiter();
            awaiter.UnsafeOnCompleted(_continuation);
            return awaiter;
        }

        public DeliveryStatus Finish(ValueTaskAwaiter<DeliveryStatus> awaiter)
        {
            _ran.Take();
            return awaiter.GetResult();
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(CompletionTable.MaxCapacity + 1)]
    [InlineData(1 << 30)]
    public void Constructor_Rejects_Invalid_Capacity(int capacity)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new CompletionTable(capacity));
    }

    [Fact]
    public void Default_Token_Is_Invalid_And_Allocated_Tokens_Are_Valid()
    {
        Assert.False(default(SendToken).IsValid);
        Assert.False(new SendToken(-1, 5).IsValid);
        var table = new CompletionTable(2);
        Assert.True(table.TryAllocate(out SendToken token));
        Assert.True(token.IsValid);
        Assert.Equal(2, table.Capacity);
        Assert.Equal(1, table.Available);
    }

    [Fact]
    public void Exhaustion_Returns_False_And_Distinct_Slots()
    {
        var table = new CompletionTable(3);
        var seen = new HashSet<int>();
        for (int i = 0; i < 3; i++)
        {
            Assert.True(table.TryAllocate(out SendToken token));
            Assert.True(seen.Add(token.Slot));
        }

        Assert.False(table.TryAllocate(out SendToken none));
        Assert.False(none.IsValid);
        Assert.Equal(0, table.Available);
    }

    [Fact]
    public void Fresh_Slot_Is_Pending_On_Both_Stages()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);
        Assert.False(table.IsCompleted(token, Buffer));
        Assert.False(table.IsCompleted(token, Remote));
        Assert.Equal(DeliveryStatus.Pending, table.GetStatus(token));
    }

    [Fact]
    public void Complete_Before_Await_Returns_Synchronously()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);

        table.Complete(token, Buffer, DeliveryStatus.Pending);
        Assert.True(table.IsCompleted(token, Buffer));
        Assert.False(table.IsCompleted(token, Remote));
        Assert.Equal(DeliveryStatus.Pending, table.GetStatus(token));

        ValueTask<DeliveryStatus> bufferWait = table.WaitAsync(token, Buffer);
        Assert.True(bufferWait.IsCompletedSuccessfully);
        Assert.Equal(DeliveryStatus.Pending, bufferWait.Result);

        table.Complete(token, Remote, DeliveryStatus.Delivered);
        ValueTask<DeliveryStatus> remoteWait = table.WaitAsync(token, Remote);
        Assert.True(remoteWait.IsCompletedSuccessfully);
        Assert.Equal(DeliveryStatus.Delivered, remoteWait.Result);
    }

    [Fact]
    public void Await_Before_Complete_Is_Signalled_With_Status_At_Completion()
    {
        var table = new CompletionTable(1);
        var awaiter = new HandOffAwaiter();
        table.TryAllocate(out SendToken token);

        ValueTask<DeliveryStatus> remoteWait = table.WaitAsync(token, Remote);
        Assert.False(remoteWait.IsCompleted);

        var completer = new Thread(() =>
        {
            Thread.Sleep(10);
            table.Complete(token, Remote, DeliveryStatus.Delivered);
        });
        completer.Start();
        Assert.Equal(DeliveryStatus.Delivered, awaiter.Await(remoteWait));
        completer.Join();

        // The other stage is independent and still pending.
        Assert.False(table.IsCompleted(token, Buffer));
        ValueTask<DeliveryStatus> bufferWait = table.WaitAsync(token, Buffer);
        Assert.False(bufferWait.IsCompleted);
        table.Complete(token, Buffer, DeliveryStatus.Pending);
        Assert.Equal(DeliveryStatus.Delivered, awaiter.Await(bufferWait));

        // Both stages done and no waits outstanding: the slot was released.
        Assert.Equal(1, table.Available);
    }

    [Fact]
    public async Task Async_Method_Await_Works_For_Both_Stages()
    {
        var table = new CompletionTable(2);
        table.TryAllocate(out SendToken token);

        Task<DeliveryStatus> buffer = table.WaitAsync(token, Buffer).AsTask();
        Task<DeliveryStatus> remote = table.WaitAsync(token, Remote).AsTask();
        Assert.False(buffer.IsCompleted);
        Assert.False(remote.IsCompleted);

        _ = Task.Run(() =>
        {
            table.Complete(token, Buffer, DeliveryStatus.Pending);
            table.Complete(token, Remote, DeliveryStatus.Failed);
        });

        Assert.Equal(DeliveryStatus.Failed, await remote);
        DeliveryStatus bufferStatus = await buffer;
        Assert.True(bufferStatus is DeliveryStatus.Pending or DeliveryStatus.Failed);
        Assert.Equal(DeliveryStatus.Failed, table.GetStatus(token));
    }

    [Fact]
    public void Repeated_Complete_On_Same_Stage_Is_Ignored_And_Pending_Keeps_Status()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);

        table.Complete(token, Buffer, DeliveryStatus.Superseded);
        table.Complete(token, Buffer, DeliveryStatus.Failed);
        Assert.Equal(DeliveryStatus.Superseded, table.GetStatus(token));

        table.Complete(token, Remote, DeliveryStatus.Pending);
        Assert.Equal(DeliveryStatus.Superseded, table.GetStatus(token));
    }

    [Fact]
    public void Stale_Token_Semantics_After_Auto_Release()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);
        table.Complete(token, Buffer, DeliveryStatus.Pending);
        table.Complete(token, Remote, DeliveryStatus.Expired);
        Assert.Equal(1, table.Available);

        // Ignored, reported as complete, and the final status is remembered.
        table.Complete(token, Buffer, DeliveryStatus.Failed);
        table.Complete(token, Remote, DeliveryStatus.Failed);
        Assert.True(table.IsCompleted(token, Buffer));
        Assert.True(table.IsCompleted(token, Remote));
        Assert.Equal(DeliveryStatus.Expired, table.GetStatus(token));

        ValueTask<DeliveryStatus> wait = table.WaitAsync(token, Remote);
        Assert.True(wait.IsCompletedSuccessfully);
        Assert.Equal(DeliveryStatus.Expired, wait.Result);
        Assert.Equal(DeliveryStatus.Expired, table.Wait(token, Buffer, TimeSpan.Zero));

        table.Release(token);
        Assert.Equal(1, table.Available);

        // The slot is reusable and the new occupant does not inherit anything.
        Assert.True(table.TryAllocate(out SendToken reused));
        Assert.Equal(token.Slot, reused.Slot);
        Assert.NotEqual(token.Generation, reused.Generation);
        Assert.Equal(DeliveryStatus.Pending, table.GetStatus(reused));
        Assert.False(table.IsCompleted(reused, Buffer));
        Assert.False(table.IsCompleted(reused, Remote));

        // The stale token still reports its own status, not the new occupant's.
        table.Complete(reused, Remote, DeliveryStatus.Delivered);
        Assert.Equal(DeliveryStatus.Expired, table.GetStatus(token));
        Assert.Equal(DeliveryStatus.Delivered, table.GetStatus(reused));
    }

    [Fact]
    public void Slot_Reuse_Bumps_Generation_Each_Time()
    {
        var table = new CompletionTable(1);
        uint previous = 0;
        for (int i = 0; i < 100; i++)
        {
            Assert.True(table.TryAllocate(out SendToken token));
            Assert.Equal(0, token.Slot);
            Assert.True(token.Generation > previous);
            Assert.Equal(1u, token.Generation & 1); // odd generations never wrap to the invalid value 0
            previous = token.Generation;
            table.Complete(token, Buffer, DeliveryStatus.Delivered);
            table.Complete(token, Remote, DeliveryStatus.Delivered);
        }
    }

    [Fact]
    public void Cancelling_A_Wait_Leaves_The_Slot_Alive()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);
        using var cts = new CancellationTokenSource();

        ValueTask<DeliveryStatus> wait = table.WaitAsync(token, Remote, cts.Token);
        Assert.False(wait.IsCompleted);

        cts.Cancel();
        Assert.True(wait.IsCanceled);
        var exception = Assert.Throws<OperationCanceledException>(() => wait.GetAwaiter().GetResult());
        Assert.Equal(cts.Token, exception.CancellationToken);

        // The send itself is untouched.
        Assert.False(table.IsCompleted(token, Remote));
        Assert.Equal(DeliveryStatus.Pending, table.GetStatus(token));
        Assert.Equal(0, table.Available);

        // A new wait can be started and completes normally.
        ValueTask<DeliveryStatus> second = table.WaitAsync(token, Remote);
        Assert.False(second.IsCompleted);
        table.Complete(token, Remote, DeliveryStatus.Delivered);
        Assert.Equal(DeliveryStatus.Delivered, new HandOffAwaiter().Await(second));

        // Same for the other stage, repeatedly, so both cores go through cancel and reuse.
        for (int i = 0; i < 3; i++)
        {
            using var bufferCts = new CancellationTokenSource();
            ValueTask<DeliveryStatus> bufferWait = table.WaitAsync(token, Buffer, bufferCts.Token);
            bufferCts.Cancel();
            Assert.Throws<OperationCanceledException>(() => bufferWait.GetAwaiter().GetResult());
            Assert.False(table.IsCompleted(token, Buffer));
        }

        ValueTask<DeliveryStatus> bufferSecond = table.WaitAsync(token, Buffer);
        Assert.False(bufferSecond.IsCompleted);
        table.Complete(token, Buffer, DeliveryStatus.Pending);
        Assert.Equal(DeliveryStatus.Delivered, new HandOffAwaiter().Await(bufferSecond));
        Assert.Equal(1, table.Available);
    }

    [Fact]
    public void Already_Cancelled_Token_Cancels_The_Wait_Immediately()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);
        ValueTask<DeliveryStatus> wait = table.WaitAsync(token, Buffer, new CancellationToken(canceled: true));
        Assert.True(wait.IsCanceled);
        Assert.Throws<OperationCanceledException>(() => wait.GetAwaiter().GetResult());
        Assert.False(table.IsCompleted(token, Buffer));
    }

    [Fact]
    public void Cancelling_After_Completion_Has_No_Effect()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);
        using var cts = new CancellationTokenSource();

        ValueTask<DeliveryStatus> wait = table.WaitAsync(token, Buffer, cts.Token);
        table.Complete(token, Buffer, DeliveryStatus.Delivered);
        cts.Cancel();
        Assert.True(wait.IsCompletedSuccessfully);
        Assert.Equal(DeliveryStatus.Delivered, wait.Result);
    }

    [Fact]
    public void Second_Outstanding_Wait_On_Same_Stage_Is_Rejected()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);
        ValueTask<DeliveryStatus> first = table.WaitAsync(token, Buffer);
        Assert.Throws<InvalidOperationException>(() => table.WaitAsync(token, Buffer));

        // Waiting on the other stage is fine.
        ValueTask<DeliveryStatus> other = table.WaitAsync(token, Remote);
        Assert.False(other.IsCompleted);

        table.Complete(token, Buffer, DeliveryStatus.Delivered);
        table.Complete(token, Remote, DeliveryStatus.Delivered);
        Assert.Equal(DeliveryStatus.Delivered, first.Result);
        Assert.Equal(DeliveryStatus.Delivered, other.Result);
        Assert.Equal(1, table.Available);
    }

    [Fact]
    public void Slot_Is_Not_Released_While_A_Wait_Is_Outstanding()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);
        ValueTask<DeliveryStatus> wait = table.WaitAsync(token, Remote);

        table.Complete(token, Buffer, DeliveryStatus.Pending);
        table.Complete(token, Remote, DeliveryStatus.Delivered);

        // Both stages are complete but the ValueTask has not been consumed yet.
        Assert.Equal(0, table.Available);
        Assert.False(table.TryAllocate(out _));
        Assert.True(wait.IsCompletedSuccessfully);
        Assert.Equal(DeliveryStatus.Delivered, wait.Result);
        Assert.Equal(1, table.Available);
    }

    [Fact]
    public void Consumed_ValueTask_Cannot_Be_Reused_After_The_Slot_Is_Recycled()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);
        ValueTask<DeliveryStatus> bufferWait = table.WaitAsync(token, Buffer);
        ValueTask<DeliveryStatus> remoteWait = table.WaitAsync(token, Remote);
        table.Complete(token, Buffer, DeliveryStatus.Delivered);
        table.Complete(token, Remote, DeliveryStatus.Delivered);
        Assert.Equal(DeliveryStatus.Delivered, bufferWait.Result);
        Assert.Equal(DeliveryStatus.Delivered, remoteWait.Result);
        Assert.Equal(1, table.Available);

        Assert.Throws<InvalidOperationException>(() => bufferWait.IsCompleted);
        Assert.Throws<InvalidOperationException>(() => remoteWait.IsCompleted);
        Assert.Throws<InvalidOperationException>(() => bufferWait.GetAwaiter().GetResult());
        Assert.Throws<InvalidOperationException>(() => remoteWait.GetAwaiter().OnCompleted(static () => { }));
    }

    [Fact]
    public void Synchronous_Wait_Returns_When_Completed_From_Another_Thread()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);

        var completer = new Thread(() =>
        {
            Thread.Sleep(30);
            table.Complete(token, Buffer, DeliveryStatus.Delivered);
        });
        completer.Start();
        Assert.Equal(DeliveryStatus.Delivered, table.Wait(token, Buffer, TimeSpan.FromSeconds(10)));
        completer.Join();

        // Already complete: returns without spinning.
        Assert.Equal(DeliveryStatus.Delivered, table.Wait(token, Buffer, TimeSpan.Zero));

        // The parked event is reused for the next wait on the same slot, including an infinite one.
        completer = new Thread(() =>
        {
            Thread.Sleep(30);
            table.Complete(token, Remote, DeliveryStatus.Delivered);
        });
        completer.Start();
        Assert.Equal(DeliveryStatus.Delivered, table.Wait(token, Remote, Timeout.InfiniteTimeSpan));
        completer.Join();
        Assert.Equal(1, table.Available);
    }

    [Fact]
    public void Synchronous_Wait_Times_Out_And_Returns_Pending()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);

        Assert.Equal(DeliveryStatus.Pending, table.Wait(token, Remote, TimeSpan.Zero));
        Assert.Equal(DeliveryStatus.Pending, table.Wait(token, Remote, TimeSpan.FromMilliseconds(30)));
        Assert.False(table.IsCompleted(token, Remote));

        // A completion of the *other* stage wakes the waiter, which must go back to sleep until timeout.
        var completer = new Thread(() =>
        {
            Thread.Sleep(20);
            table.Complete(token, Buffer, DeliveryStatus.Pending);
        });
        completer.Start();
        Assert.Equal(DeliveryStatus.Pending, table.Wait(token, Remote, TimeSpan.FromMilliseconds(120)));
        completer.Join();
        Assert.True(table.IsCompleted(token, Buffer));
        Assert.False(table.IsCompleted(token, Remote));
    }

    [Fact]
    public void Synchronous_Wait_Rejects_Invalid_Timeouts()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);
        Assert.Throws<ArgumentOutOfRangeException>(() => table.Wait(token, Buffer, TimeSpan.FromMilliseconds(-2)));
        Assert.Throws<ArgumentOutOfRangeException>(() => table.Wait(token, Buffer, TimeSpan.FromDays(365)));
    }

    [Fact]
    public void Release_Completes_Pending_Stages_With_Canceled()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);
        ValueTask<DeliveryStatus> wait = table.WaitAsync(token, Remote);

        table.Release(token);
        Assert.True(wait.IsCompletedSuccessfully);
        Assert.Equal(DeliveryStatus.Canceled, wait.Result);
        Assert.Equal(1, table.Available);
        Assert.Equal(DeliveryStatus.Canceled, table.GetStatus(token));

        // Late completions from the transport are ignored.
        table.Complete(token, Remote, DeliveryStatus.Delivered);
        Assert.Equal(DeliveryStatus.Canceled, table.GetStatus(token));
    }

    [Fact]
    public void Release_Keeps_A_Known_Status()
    {
        var table = new CompletionTable(1);
        table.TryAllocate(out SendToken token);
        table.Complete(token, Buffer, DeliveryStatus.Disconnected);
        table.Release(token);
        Assert.Equal(1, table.Available);
        Assert.Equal(DeliveryStatus.Disconnected, table.GetStatus(token));
        Assert.True(table.IsCompleted(token, Remote));
    }

    [Fact]
    public void CompleteAll_Completes_Every_Live_Occupant_And_Leaves_Free_Slots_Alone()
    {
        var table = new CompletionTable(3);
        table.TryAllocate(out SendToken waited);
        table.TryAllocate(out SendToken quiet);
        ValueTask<DeliveryStatus> wait = table.WaitAsync(waited, Remote);
        table.Complete(quiet, Buffer, DeliveryStatus.Pending);

        table.CompleteAll(DeliveryStatus.Disconnected);

        // The waited slot stays allocated until its wait is consumed; the other one is released at once.
        Assert.Equal(2, table.Available);
        Assert.True(wait.IsCompletedSuccessfully);
        Assert.Equal(DeliveryStatus.Disconnected, wait.Result);
        Assert.Equal(3, table.Available);
        Assert.Equal(DeliveryStatus.Disconnected, table.GetStatus(waited));
        Assert.Equal(DeliveryStatus.Disconnected, table.GetStatus(quiet));

        // Nothing is live any more, so a second teardown pass finds nothing to complete.
        table.CompleteAll(DeliveryStatus.Canceled);
        Assert.Equal(DeliveryStatus.Disconnected, table.GetStatus(waited));
        Assert.Equal(3, table.Available);
    }

    [Fact]
    public void A_Wait_Released_On_Teardown_Can_Be_Consumed_After_The_Table_Is_Disposed()
    {
        // A peer disposed without being closed completes every tracked send's wait (CompleteAll) and disposes the table once
        // its transport has reported the close, and the application may consume such a wait only afterwards. Consuming the
        // last one releases the slot, and that release writes into the free list, which must therefore still exist. 65 536
        // slots make the free list a 1 MiB block: a native one freed by Dispose (the table before 68035f8) goes back to the
        // OS, so the write faults instead of silently corrupting the heap.
        var table = new CompletionTable(1 << 16);
        Assert.True(table.TryAllocate(out SendToken token));
        ValueTask<DeliveryStatus> buffer = table.WaitAsync(token, Buffer);
        ValueTask<DeliveryStatus> remote = table.WaitAsync(token, Remote);
        table.CompleteAll(DeliveryStatus.Disconnected);

        table.Dispose();
        table.Dispose();

        Assert.Equal(DeliveryStatus.Disconnected, buffer.Result);
        Assert.Equal(DeliveryStatus.Disconnected, remote.Result);
        Assert.Equal(DeliveryStatus.Disconnected, table.GetStatus(token));

        // The late release went back to the (GC-managed) free list like any other.
        Assert.Equal(table.Capacity, table.Available);
    }

    [Fact]
    public void Late_Releases_Racing_Dispose_Never_Fault()
    {
        // Waits released on teardown and consumed on other threads while the owner disposes the table (a thread-pool
        // continuation running during teardown). Each round uses a fresh table of 32 768 slots, whose free list as a native
        // block (512 KiB) would go back to the OS when Dispose freed it, so a release that raced Dispose would fault.
        for (int round = 0; round < 10; round++)
        {
            var table = new CompletionTable(1 << 15);
            var waits = new ValueTask<DeliveryStatus>[64];
            for (int i = 0; i < waits.Length; i++)
            {
                Assert.True(table.TryAllocate(out SendToken token));
                waits[i] = table.WaitAsync(token, Remote);
                table.Complete(token, Buffer, DeliveryStatus.Pending);
            }

            table.CompleteAll(DeliveryStatus.Disconnected);
            using var start = new Barrier(3);
            var consumers = new Thread[2];
            int wrong = 0;
            for (int c = 0; c < consumers.Length; c++)
            {
                int first = c;
                consumers[c] = new Thread(() =>
                {
                    start.SignalAndWait();
                    for (int i = first; i < waits.Length; i += 2)
                    {
                        if (waits[i].Result != DeliveryStatus.Disconnected)
                        {
                            Interlocked.Increment(ref wrong);
                        }
                    }
                });
                consumers[c].Start();
            }

            start.SignalAndWait();
            table.Dispose();
            foreach (Thread consumer in consumers)
            {
                consumer.Join();
            }

            Assert.Equal(0, wrong);
            Assert.Equal(table.Capacity, table.Available);
        }
    }

    [Fact]
    public void Tokens_Outside_The_Table_Are_Rejected()
    {
        var table = new CompletionTable(2);
        var bad = new SendToken(2, 1);
        var negative = new SendToken(-1, 1);
        Assert.Throws<ArgumentOutOfRangeException>(() => table.Complete(bad, Buffer, DeliveryStatus.Delivered));
        Assert.Throws<ArgumentOutOfRangeException>(() => table.IsCompleted(negative, Buffer));
        Assert.Throws<ArgumentOutOfRangeException>(() => table.GetStatus(bad));
        Assert.Throws<ArgumentOutOfRangeException>(() => table.WaitAsync(bad, Buffer));
        Assert.Throws<ArgumentOutOfRangeException>(() => table.Wait(bad, Buffer, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => table.Release(negative));
    }

    [Fact]
    public async Task RunContinuationsAsynchronously_Moves_Continuation_Off_The_Completing_Thread()
    {
        var table = new CompletionTable(1, runContinuationsAsynchronously: true);
        table.TryAllocate(out SendToken token);

        int completingThread = 0;
        int continuationThread = 0;
        var done = new TaskCompletionSource<DeliveryStatus>(TaskCreationOptions.RunContinuationsAsynchronously);

        ValueTask<DeliveryStatus> wait = table.WaitAsync(token, Buffer);
        ValueTaskAwaiter<DeliveryStatus> awaiter = wait.GetAwaiter();
        awaiter.UnsafeOnCompleted(() =>
        {
            continuationThread = Environment.CurrentManagedThreadId;
            done.SetResult(awaiter.GetResult());
        });

        var completer = new Thread(() =>
        {
            completingThread = Environment.CurrentManagedThreadId;
            table.Complete(token, Buffer, DeliveryStatus.Delivered);
        });
        completer.Start();

        Assert.Equal(DeliveryStatus.Delivered, await done.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        completer.Join();
        Assert.NotEqual(completingThread, continuationThread);
    }

    /// <remarks>
    /// Every mode waits for the send's remote stage before the next one starts, so one token at a time is handed to the
    /// transport thread (<see cref="HandOff"/>: it spins for the next one and then blocks, so that on a machine with no
    /// core to spare a hand-off costs a wake-up rather than a scheduler quantum).
    /// </remarks>
    [Fact]
    public void Stress_Transport_Thread_Completes_While_Owner_Awaits()
    {
        const int iterations = 20_000;
        const long stop = -1;
        var table = new CompletionTable(64);
        var awaiter = new HandOffAwaiter();
        using var handoff = new HandOff();
        var random = new Random(12345);
        int failures = 0;
        Exception? transportFailure = null;

        var transport = new Thread(() =>
        {
            var localRandom = new Random(777);
            try
            {
                long packed;
                while ((packed = handoff.Take()) != stop)
                {
                    var token = new SendToken((int)(packed & 0xFFFF_FFFF), (uint)(packed >> 32));
                    if ((localRandom.Next() & 1) == 0)
                        Thread.SpinWait(localRandom.Next(0, 200));
                    table.Complete(token, Buffer, DeliveryStatus.Pending);
                    if ((localRandom.Next() & 1) == 0)
                        Thread.SpinWait(localRandom.Next(0, 200));
                    table.Complete(token, Remote, DeliveryStatus.Delivered);
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
                Assert.True(table.TryAllocate(out SendToken token));
                long packed = ((long)token.Generation << 32) | (uint)token.Slot;
                int mode = random.Next(4);

                ValueTask<DeliveryStatus> remoteWait = default;
                ValueTask<DeliveryStatus> bufferWait = default;
                if (mode is 0 or 1)
                    remoteWait = table.WaitAsync(token, Remote);
                if (mode == 1)
                    bufferWait = table.WaitAsync(token, Buffer);

                handoff.Put(packed);

                switch (mode)
                {
                    case 0:
                        if (awaiter.Await(remoteWait) != DeliveryStatus.Delivered)
                            failures++;
                        break;
                    case 1:
                        if (awaiter.Await(bufferWait) is not (DeliveryStatus.Pending or DeliveryStatus.Delivered))
                            failures++;
                        if (awaiter.Await(remoteWait) != DeliveryStatus.Delivered)
                            failures++;
                        break;
                    case 2:
                        if (table.Wait(token, Remote, TimeSpan.FromSeconds(10)) != DeliveryStatus.Delivered)
                            failures++;
                        break;
                    default:
                        // Await after completion (probably): either path must yield Delivered.
                        Thread.SpinWait(random.Next(0, 300));
                        if (awaiter.Await(table.WaitAsync(token, Remote)) != DeliveryStatus.Delivered)
                            failures++;
                        break;
                }
            }
        }
        finally
        {
            handoff.Put(stop);
            transport.Join();
        }

        Assert.True(transportFailure is null, $"The transport thread failed: {transportFailure}");
        Assert.Equal(0, failures);
        Assert.Equal(64, table.Available);
    }

    /// <summary>
    /// A cancellation and a completion of the same wait race each other. Whichever wins, the wait ends exactly once
    /// (delivered or canceled), the completion itself lands, and the slot is free again once its other stage completes.
    /// </summary>
    /// <remarks>
    /// The completing and the cancelling call come from two threads that live for the whole run and are released together
    /// for every round, once the wait's continuation is attached; each waits a random moment first, so that the two calls
    /// meet in either order and at every point of each other. A thread that waits longer than a release takes while both
    /// threads run blocks instead of spinning (<see cref="HandOff"/>). (Two new threads per round rarely raced: the completer,
    /// started first, won all but a handful of 5 000 rounds, and on a machine with no core to spare the awaiting thread's
    /// spin lost a scheduler quantum in every round, so the run took many minutes.) The run ends after its rounds or after
    /// 20 s, and must have seen both outcomes and at least 500 rounds in which the two calls ran at the same time.
    /// </remarks>
    [Fact]
    public void Stress_Cancel_Races_Completion()
    {
        const int rounds = 20_000;
        const int requiredOverlaps = 500;
        const long stop = -1;
        long start = Stopwatch.GetTimestamp();
        long deadline = start + (20 * Stopwatch.Frequency);
        var table = new CompletionTable(4);
        using var completeGo = new HandOff();
        using var cancelGo = new HandOff();
        using var completeDone = new HandOff();
        using var cancelDone = new HandOff();
        SendToken token = default;
        CancellationTokenSource? cts = null;
        long clock = 0;
        long completeStart = 0;
        long completeEnd = 0;
        long cancelStart = 0;
        long cancelEnd = 0;
        int continuations = 0;
        Exception? failure = null;
        Action continuation = () => Interlocked.Increment(ref continuations);

        var completer = new Thread(() => Serve(completeGo, completeDone, 1, () =>
        {
            completeStart = Interlocked.Increment(ref clock);
            table.Complete(token, Remote, DeliveryStatus.Delivered);
            completeEnd = Interlocked.Increment(ref clock);
        }))
        { IsBackground = true, Name = "completer" };
        var canceller = new Thread(() => Serve(cancelGo, cancelDone, 2, () =>
        {
            cancelStart = Interlocked.Increment(ref clock);
            cts!.Cancel();
            cancelEnd = Interlocked.Increment(ref clock);
        }))
        { IsBackground = true, Name = "canceller" };

        int completedWaits = 0;
        int canceledWaits = 0;
        int overlaps = 0;
        int round = 0;
        completer.Start();
        canceller.Start();
        try
        {
            for (; round < rounds && ((round & 255) != 0 || Stopwatch.GetTimestamp() < deadline); round++)
            {
                Assert.True(table.TryAllocate(out token));
                cts = new CancellationTokenSource();
                // Without a captured context the continuation runs inline on the thread that ends the wait. Nothing awaits
                // here, so no parallelization limit is bypassed.
#pragma warning disable xUnit1030
                ConfiguredValueTaskAwaitable<DeliveryStatus>.ConfiguredValueTaskAwaiter awaiter =
                    table.WaitAsync(token, Remote, cts.Token).ConfigureAwait(false).GetAwaiter();
#pragma warning restore xUnit1030
                continuations = 0;
                awaiter.UnsafeOnCompleted(continuation);

                completeGo.Put(1);
                cancelGo.Put(1);
                completeDone.Take();
                cancelDone.Take();
                if (failure is not null)
                    Assert.Fail($"Round {round}: {failure}");

                // The two calls ran at the same time when each started before the other ended.
                if (completeStart < cancelEnd && cancelStart < completeEnd)
                    overlaps++;

                // The wait ended exactly once, on whichever thread ended it.
                Assert.Equal(1, Volatile.Read(ref continuations));
                try
                {
                    Assert.Equal(DeliveryStatus.Delivered, awaiter.GetResult());
                    completedWaits++;
                }
                catch (OperationCanceledException)
                {
                    canceledWaits++;
                }

                // Whatever happened to the wait, the completion itself always landed.
                Assert.Equal(DeliveryStatus.Delivered, table.GetStatus(token));
                Assert.True(table.IsCompleted(token, Remote));
                table.Complete(token, Buffer, DeliveryStatus.Pending);
                Assert.Equal(4, table.Available);
                cts.Dispose();
            }
        }
        finally
        {
            completeGo.Put(stop);
            cancelGo.Put(stop);
            completer.Join(HandOff.Timeout);
            canceller.Join(HandOff.Timeout);
        }

        string run = $"{round:N0} rounds in {Stopwatch.GetElapsedTime(start).TotalSeconds:F1} s: {completedWaits:N0} delivered, "
            + $"{canceledWaits:N0} canceled, the two calls at the same time in {overlaps:N0}";
        TestContext.Current.TestOutputHelper?.WriteLine(run);
        Assert.Equal(round, canceledWaits + completedWaits);
        Assert.True(overlaps >= requiredOverlaps && completedWaits > 0 && canceledWaits > 0, $"The run did not exercise the race ({run}).");

        // One call per round, a random few hundred nanoseconds after the round was released.
        void Serve(HandOff go, HandOff done, int seed, Action call)
        {
            var random = new Random(seed);
            try
            {
                while (go.Take() != stop)
                {
                    Thread.SpinWait(random.Next(0, 64));
                    try
                    {
                        call();
                    }
                    catch (Exception e)
                    {
                        Interlocked.CompareExchange(ref failure, e, null);
                    }

                    done.Put(1);
                }
            }
            catch (TimeoutException e)
            {
                Interlocked.CompareExchange(ref failure, e, null);
            }
        }
    }

    [Fact]
    public void Allocate_Complete_Await_Completed_Loop_Does_Not_Allocate()
    {
        var table = new CompletionTable(8);
        RunLoop(table, 10_000);

        WindowedAllocation.AssertNone(() => RunLoop(table, 20_000));

        static void RunLoop(CompletionTable table, int iterations)
        {
            for (int i = 0; i < iterations; i++)
            {
                table.TryAllocate(out SendToken token);
                table.Complete(token, Buffer, DeliveryStatus.Pending);
                ValueTask<DeliveryStatus> bufferWait = table.WaitAsync(token, Buffer);
                _ = bufferWait.Result;
                _ = table.IsCompleted(token, Remote);
                table.Complete(token, Remote, DeliveryStatus.Delivered);
                ValueTask<DeliveryStatus> remoteWait = table.WaitAsync(token, Remote);
                _ = remoteWait.Result;
                _ = table.GetStatus(token);
                _ = table.Wait(token, Remote, TimeSpan.Zero);
            }
        }
    }

    [Fact]
    public void Await_Registered_Before_Completion_Does_Not_Allocate()
    {
        // The continuation is attached before the transport thread completes the stage, so the completing
        // thread runs it inline. (If a continuation is attached *after* completion, the runtime's
        // ManualResetValueTaskSourceCore queues it to the thread pool, which allocates a work item; that is
        // runtime behaviour, not the table's, and does not occur when completions are polled on the owner thread.)
        SynchronizationContext? previousContext = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(null);
        try
        {
            const long stop = -1;
            var table = new CompletionTable(8);
            var awaiter = new HandOffAwaiter();
            var remoteAwaiter = new HandOffAwaiter();
            using var published = new HandOff();

            // The token goes to the transport thread through a hand-off that spins and then blocks, as do the waits for
            // the two continuations: on a machine with no core to spare a hand-off costs a wake-up, not a scheduler
            // quantum. Neither side allocates.
            Exception? transportFailure = null;
            var transport = new Thread(() =>
            {
                try
                {
                    long packed;
                    while ((packed = published.Take()) != stop)
                    {
                        var token = new SendToken((int)(packed & 0xFFFF_FFFF), (uint)(packed >> 32));
                        table.Complete(token, Buffer, DeliveryStatus.Pending);
                        table.Complete(token, Remote, DeliveryStatus.Delivered);
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
                // Warm up until tiered compilation has settled (the loop is cross-thread and slow enough that a
                // fixed iteration count could finish before tier-1 code is installed).
                long warmupStart = Environment.TickCount64;
                while (Environment.TickCount64 - warmupStart < 500)
                    RunLoop(table, awaiter, remoteAwaiter, published, 1_000);

                WindowedAllocation.AssertNone(() => RunLoop(table, awaiter, remoteAwaiter, published, 4_000));
            }
            finally
            {
                published.Put(stop);
                transport.Join();
            }

            Assert.True(transportFailure is null, $"The transport thread failed: {transportFailure}");
            Assert.Equal(8, table.Available);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previousContext);
        }

        static void RunLoop(CompletionTable table, HandOffAwaiter bufferAwaiter, HandOffAwaiter remoteAwaiter, HandOff published, int iterations)
        {
            for (int i = 0; i < iterations; i++)
            {
                table.TryAllocate(out SendToken token);
                ValueTaskAwaiter<DeliveryStatus> bufferWait = bufferAwaiter.Register(table.WaitAsync(token, Buffer));
                ValueTaskAwaiter<DeliveryStatus> remoteWait = remoteAwaiter.Register(table.WaitAsync(token, Remote));
                published.Put(((long)token.Generation << 32) | (uint)token.Slot);
                bufferAwaiter.Finish(bufferWait);
                remoteAwaiter.Finish(remoteWait);
            }
        }
    }
}
