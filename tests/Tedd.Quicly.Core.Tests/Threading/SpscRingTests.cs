using System.Diagnostics;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Tests.Threading;

public class SpscRingTests
{
    [Theory]
    [InlineData(1, 2)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(4, 4)]
    [InlineData(1000, 1024)]
    public void Capacity_Rounds_Up_To_Power_Of_Two(int requested, int expected)
    {
        var ring = new SpscRing<int>(requested);
        Assert.Equal(expected, ring.Capacity);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData((1 << 30) + 1)]
    public void Constructor_Rejects_Invalid_Capacity(int requested)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpscRing<int>(requested));
    }

    [Fact]
    public void Empty_Ring_Reports_Empty_And_Dequeue_Fails()
    {
        var ring = new SpscRing<long>(8);
        Assert.True(ring.IsEmpty);
        Assert.Equal(0, ring.Count);
        Assert.False(ring.TryDequeue(out long item));
        Assert.Equal(0, item);
    }

    [Fact]
    public void Full_Ring_Rejects_Enqueue_And_Keeps_Contents()
    {
        var ring = new SpscRing<int>(4);
        for (int i = 0; i < ring.Capacity; i++)
            Assert.True(ring.TryEnqueue(i));

        Assert.False(ring.TryEnqueue(99));
        Assert.Equal(ring.Capacity, ring.Count);
        Assert.False(ring.IsEmpty);

        for (int i = 0; i < ring.Capacity; i++)
        {
            Assert.True(ring.TryDequeue(out int item));
            Assert.Equal(i, item);
        }

        Assert.True(ring.IsEmpty);
    }

    [Fact]
    public void Preserves_Fifo_Order_Across_Many_Laps()
    {
        var ring = new SpscRing<int>(8);
        int next = 0;
        int expected = 0;
        for (int lap = 0; lap < 1000; lap++)
        {
            int burst = 1 + (lap % ring.Capacity);
            for (int i = 0; i < burst; i++)
                Assert.True(ring.TryEnqueue(next++));
            for (int i = 0; i < burst; i++)
            {
                Assert.True(ring.TryDequeue(out int item));
                Assert.Equal(expected++, item);
            }
        }

        Assert.True(ring.IsEmpty);
    }

    [Fact]
    public void Count_Tracks_Enqueue_And_Dequeue()
    {
        var ring = new SpscRing<byte>(16);
        for (int i = 0; i < 10; i++)
            ring.TryEnqueue((byte)i);
        Assert.Equal(10, ring.Count);
        for (int i = 0; i < 4; i++)
            ring.TryDequeue(out _);
        Assert.Equal(6, ring.Count);
    }

    /// <remarks>
    /// <para>The producer waits for room and the consumer for an item through a <see cref="Doorbell"/>: each spins for a while
    /// and then blocks until the other rings, so that on a machine with no core to spare a wait costs a thread wake-up rather
    /// than the scheduler quantum a yielding spin loses at every yield (which made the run take minutes).</para>
    /// <para>Each side rings once per <c>RingBatch</c> items, before it waits itself, and at the end, not per item: a ring is
    /// a full fence, and one per item slowed the consumer so much that it never caught up with the producer. A waiter is never
    /// left asleep: it blocks only once it has announced that it does and looked again, and the other side rings within a
    /// batch (a full ring holds many) or before it waits.</para>
    /// <para>At full speed the producer keeps the ring full, so the consumer seldom looks at an empty ring while an item is
    /// published; one phase of <c>PhaseItems</c> in eight is therefore paced, the producer pausing a random 0-31 spins before
    /// each item. The run must have handed over <c>RequiredHandOvers</c> items at each end of the ring while the waiting
    /// thread spun, that is while both threads ran: once the producer has sent its <c>items</c>, it goes on until the run
    /// has, or until <c>BudgetSeconds</c> have passed since the start.</para>
    /// </remarks>
    [Fact]
    public void Stress_One_Producer_One_Consumer_Five_Million_Items_Checksum()
    {
        const long items = 5_000_000;
        const int RingBatch = 64;
        const int PhaseItems = 8_192;
        const long RequiredHandOvers = 20_000;
        const int BudgetSeconds = 60;
        long start = Stopwatch.GetTimestamp();
        long deadline = start + (BudgetSeconds * Stopwatch.Frequency);
        var ring = new SpscRing<long>(1024);
        using var room = new Doorbell();
        using var arrival = new Doorbell();
        Exception? failure = null;
        int stop = 0;
        int producerDone = 0;
        long produced = 0;

        var producer = new Thread(() =>
        {
            long i = 1;
            try
            {
                var random = new Random(1);
                int unrung = 0;
                for (; i <= items || Volatile.Read(ref stop) == 0; i++)
                {
                    // One phase in eight is paced: the consumer catches up and looks at the empty ring as items arrive.
                    if (((i / PhaseItems) & 7) == 7)
                        Thread.SpinWait(random.Next(0, 32));

                    if (!ring.TryEnqueue(i))
                    {
                        arrival.Ring();
                        unrung = 0;
                        do
                        {
                            room.Wait();
                        }
                        while (!ring.TryEnqueue(i));
                        room.Satisfied();
                    }

                    if (++unrung == RingBatch)
                    {
                        arrival.Ring();
                        unrung = 0;
                    }
                }
            }
            catch (Exception e)
            {
                failure = e;
            }
            finally
            {
                // Its last item is in the ring before it says it is done, and the consumer is rung after.
                produced = i - 1;
                Volatile.Write(ref producerDone, 1);
                arrival.Ring();
            }
        })
        { IsBackground = true, Name = "spsc-producer" };
        producer.Start();

        long sum = 0;
        long received = 0;
        long expected = 1;
        long nextCheck = items;
        int unrungRoom = 0;
        try
        {
            while (true)
            {
                if (!ring.TryDequeue(out long item))
                {
                    // The producer is done, so the ring holds all it ever will: done once it is empty.
                    if (Volatile.Read(ref producerDone) != 0 && ring.IsEmpty)
                        break;

                    if (unrungRoom != 0)
                    {
                        room.Ring();
                        unrungRoom = 0;
                    }

                    arrival.Wait();
                    continue;
                }

                arrival.Satisfied();
                if (++unrungRoom == RingBatch)
                {
                    room.Ring();
                    unrungRoom = 0;
                }

                Assert.Equal(expected++, item);
                sum += item;
                received++;

                // Past the producer's items, stop it once both ends have their hand-overs (the producer's count is read
                // while it runs, which can only make it look smaller) or the budget is spent.
                if (stop == 0 && received >= nextCheck)
                {
                    nextCheck = received + 4_096;
                    if ((room.SpunWaits >= RequiredHandOvers && arrival.SpunWaits >= RequiredHandOvers)
                        || Stopwatch.GetTimestamp() >= deadline)
                    {
                        Volatile.Write(ref stop, 1);
                    }
                }
            }

            Assert.True(producer.Join(HandOff.Timeout));
        }
        finally
        {
            Volatile.Write(ref stop, 1);
            TestContext.Current.TestOutputHelper?.WriteLine(
                $"{received:N0} items in {Stopwatch.GetElapsedTime(start).TotalSeconds:F1} s: the producer found the ring full {room.Waits:N0} times "
                + $"({room.SpunWaits:N0} ended while it spun), the consumer found it empty {arrival.Waits:N0} times ({arrival.SpunWaits:N0}).");
        }

        Assert.Null(failure);
        Assert.True(produced >= items);
        Assert.Equal(produced, received);
        Assert.Equal(produced * (produced + 1) / 2, sum);
        Assert.True(ring.IsEmpty);

        // The two windows a ring's indices can go wrong in, each run while the other thread was running: room made while the
        // producer looked at a full ring, and an item published while the consumer looked at an empty one.
        Assert.True(
            room.SpunWaits >= RequiredHandOvers && arrival.SpunWaits >= RequiredHandOvers,
            $"The run did not exercise both ends of the ring within {BudgetSeconds} s: {room.SpunWaits:N0} and {arrival.SpunWaits:N0} hand-overs of {RequiredHandOvers:N0} required.");
    }

    [Fact]
    public void Steady_State_Does_Not_Allocate()
    {
        var ring = new SpscRing<long>(64);
        RunLoop(ring, 10_000);

        WindowedAllocation.AssertNone(() => RunLoop(ring, 20_000));

        static void RunLoop(SpscRing<long> ring, int iterations)
        {
            for (int i = 0; i < iterations; i++)
            {
                ring.TryEnqueue(i);
                ring.TryEnqueue(i + 1);
                ring.TryDequeue(out _);
                ring.TryDequeue(out _);
                _ = ring.Count;
                _ = ring.IsEmpty;
            }
        }
    }
}
