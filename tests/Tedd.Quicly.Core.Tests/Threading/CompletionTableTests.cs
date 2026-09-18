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
    /// Pre-allocated awaiter: registers one cached continuation on a pending ValueTask and spins until it runs.
    /// Lets tests drive the IValueTaskSource path without an async state machine.
    /// </summary>
    private sealed class SpinAwaiter
    {
        private readonly Action _continuation;
        private volatile bool _signaled;

        public SpinAwaiter() => _continuation = () => _signaled = true;

        public DeliveryStatus Await(ValueTask<DeliveryStatus> task)
        {
            ValueTaskAwaiter<DeliveryStatus> awaiter = task.GetAwaiter();
            if (!awaiter.IsCompleted)
            {
                _signaled = false;
                awaiter.UnsafeOnCompleted(_continuation);
                SpinWait spinner = default;
                while (!_signaled)
                    spinner.SpinOnce(sleep1Threshold: -1);
            }

            return awaiter.GetResult();
        }

        /// <summary>Attaches the continuation now; the caller triggers completion afterwards and then calls <see cref="Finish"/>.</summary>
        public ValueTaskAwaiter<DeliveryStatus> Register(ValueTask<DeliveryStatus> task)
        {
            ValueTaskAwaiter<DeliveryStatus> awaiter = task.GetAwaiter();
            _signaled = false;
            awaiter.UnsafeOnCompleted(_continuation);
            return awaiter;
        }

        public DeliveryStatus Finish(ValueTaskAwaiter<DeliveryStatus> awaiter)
        {
            SpinWait spinner = default;
            while (!_signaled)
                spinner.SpinOnce(sleep1Threshold: -1);
            return awaiter.GetResult();
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData((1 << 30) + 1)]
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
        var awaiter = new SpinAwaiter();
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
        Assert.Equal(DeliveryStatus.Delivered, new SpinAwaiter().Await(second));

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
        Assert.Equal(DeliveryStatus.Delivered, new SpinAwaiter().Await(bufferSecond));
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
        // last one releases the slot, and that release must not write into the freed free list. 65 536 slots make the ring a
        // 1 MiB block, which the allocator hands back to the OS when it is freed, so a write into it faults instead of
        // silently corrupting the heap.
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

    [Fact]
    public void Stress_Transport_Thread_Completes_While_Owner_Awaits()
    {
        const int iterations = 20_000;
        var table = new CompletionTable(64);
        var awaiter = new SpinAwaiter();
        var handoff = new MpscRing<long>(256);
        var random = new Random(12345);
        int failures = 0;

        var transport = new Thread(() =>
        {
            var localRandom = new Random(777);
            long packed;
            SpinWait spinner = default;
            while (true)
            {
                if (!handoff.TryDequeue(out packed))
                {
                    spinner.SpinOnce(sleep1Threshold: -1);
                    continue;
                }

                spinner.Reset();
                if (packed == -1)
                    return;

                var token = new SendToken((int)(packed & 0xFFFF_FFFF), (uint)(packed >> 32));
                if ((localRandom.Next() & 1) == 0)
                    Thread.SpinWait(localRandom.Next(0, 200));
                table.Complete(token, Buffer, DeliveryStatus.Pending);
                if ((localRandom.Next() & 1) == 0)
                    Thread.SpinWait(localRandom.Next(0, 200));
                table.Complete(token, Remote, DeliveryStatus.Delivered);
            }
        })
        { IsBackground = true, Name = "transport" };
        transport.Start();

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

            while (!handoff.TryEnqueue(packed))
                Thread.Yield();

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

        while (!handoff.TryEnqueue(-1))
            Thread.Yield();
        transport.Join();

        Assert.Equal(0, failures);
        Assert.Equal(64, table.Available);
    }

    [Fact]
    public void Stress_Cancel_Races_Completion()
    {
        const int iterations = 5_000;
        var table = new CompletionTable(4);
        var awaiter = new SpinAwaiter();
        int canceledWaits = 0;
        int completedWaits = 0;

        for (int i = 0; i < iterations; i++)
        {
            Assert.True(table.TryAllocate(out SendToken token));
            using var cts = new CancellationTokenSource();
            ValueTask<DeliveryStatus> wait = table.WaitAsync(token, Remote, cts.Token);

            var completer = new Thread(() => table.Complete(token, Remote, DeliveryStatus.Delivered));
            var canceller = new Thread(cts.Cancel);
            completer.Start();
            canceller.Start();

            try
            {
                Assert.Equal(DeliveryStatus.Delivered, awaiter.Await(wait));
                completedWaits++;
            }
            catch (OperationCanceledException)
            {
                canceledWaits++;
            }

            completer.Join();
            canceller.Join();

            // Whatever happened to the wait, the completion itself always landed.
            Assert.Equal(DeliveryStatus.Delivered, table.GetStatus(token));
            Assert.True(table.IsCompleted(token, Remote));
            table.Complete(token, Buffer, DeliveryStatus.Pending);
            Assert.Equal(4, table.Available);
        }

        Assert.Equal(iterations, canceledWaits + completedWaits);
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
            var table = new CompletionTable(8);
            var awaiter = new SpinAwaiter();
            var remoteAwaiter = new SpinAwaiter();
            long pendingToken = 0;
            bool stop = false;

            var transport = new Thread(() =>
            {
                SpinWait spinner = default;
                while (!Volatile.Read(ref stop))
                {
                    long packed = Interlocked.Exchange(ref pendingToken, 0);
                    if (packed == 0)
                    {
                        spinner.SpinOnce(sleep1Threshold: -1);
                        continue;
                    }

                    spinner.Reset();
                    var token = new SendToken((int)(packed & 0xFFFF_FFFF), (uint)(packed >> 32));
                    table.Complete(token, Buffer, DeliveryStatus.Pending);
                    table.Complete(token, Remote, DeliveryStatus.Delivered);
                }
            })
            { IsBackground = true, Name = "transport" };
            transport.Start();

            // Warm up until tiered compilation has settled (the loop is cross-thread and slow enough that a
            // fixed iteration count could finish before tier-1 code is installed).
            long warmupStart = Environment.TickCount64;
            while (Environment.TickCount64 - warmupStart < 500)
                RunLoop(table, awaiter, remoteAwaiter, ref pendingToken, 1_000);

            try
            {
                WindowedAllocation.AssertNone(() => RunLoop(table, awaiter, remoteAwaiter, ref pendingToken, 4_000));
            }
            finally
            {
                Volatile.Write(ref stop, true);
                transport.Join();
            }

            Assert.Equal(8, table.Available);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previousContext);
        }

        static void RunLoop(CompletionTable table, SpinAwaiter bufferAwaiter, SpinAwaiter remoteAwaiter, ref long pendingToken, int iterations)
        {
            for (int i = 0; i < iterations; i++)
            {
                table.TryAllocate(out SendToken token);
                ValueTaskAwaiter<DeliveryStatus> bufferWait = bufferAwaiter.Register(table.WaitAsync(token, Buffer));
                ValueTaskAwaiter<DeliveryStatus> remoteWait = remoteAwaiter.Register(table.WaitAsync(token, Remote));
                Volatile.Write(ref pendingToken, ((long)token.Generation << 32) | (uint)token.Slot);
                bufferAwaiter.Finish(bufferWait);
                remoteAwaiter.Finish(remoteWait);
            }
        }
    }
}
