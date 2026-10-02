using System.Diagnostics;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Tests.Threading;

public class MpscRingTests
{
    /// <summary>
    /// How long a producer of the stress test spins for room before it blocks: briefly, because eight producers outnumber
    /// the cores of a small machine, and a spinning producer holds a core the consumer needs to make the room.
    /// </summary>
    private static readonly TimeSpan ProducerSpin = TimeSpan.FromMicroseconds(5);

    [Theory]
    [InlineData(1, 2)]
    [InlineData(2, 2)]
    [InlineData(5, 8)]
    [InlineData(1024, 1024)]
    [InlineData(1025, 2048)]
    public void Capacity_Rounds_Up_To_Power_Of_Two(int requested, int expected)
    {
        var ring = new MpscRing<int>(requested);
        Assert.Equal(expected, ring.Capacity);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    [InlineData((1 << 30) + 1)]
    public void Constructor_Rejects_Invalid_Capacity(int requested)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new MpscRing<int>(requested));
    }

    [Fact]
    public void Empty_Ring_Reports_Empty_And_Dequeue_Fails()
    {
        var ring = new MpscRing<int>(4);
        Assert.True(ring.IsEmpty);
        Assert.Equal(0, ring.Count);
        Assert.False(ring.TryDequeue(out int item));
        Assert.Equal(0, item);
        Assert.Equal(0, ring.TryDequeueBatch(new int[4]));
    }

    [Fact]
    public void Full_Ring_Rejects_Enqueue_And_Keeps_Contents()
    {
        var ring = new MpscRing<int>(4);
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
        Assert.True(ring.TryEnqueue(5));
    }

    [Fact]
    public void Preserves_Fifo_Order_Across_Many_Laps()
    {
        var ring = new MpscRing<int>(8);
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
    }

    [Fact]
    public void Batch_Dequeue_Returns_Up_To_Destination_Length_In_Order()
    {
        var ring = new MpscRing<int>(16);
        for (int i = 0; i < 10; i++)
            ring.TryEnqueue(i);

        int[] destination = new int[4];
        Assert.Equal(4, ring.TryDequeueBatch(destination));
        Assert.Equal(new[] { 0, 1, 2, 3 }, destination);
        Assert.Equal(6, ring.Count);

        Assert.Equal(0, ring.TryDequeueBatch(Span<int>.Empty));

        int[] large = new int[16];
        Assert.Equal(6, ring.TryDequeueBatch(large));
        Assert.Equal(new[] { 4, 5, 6, 7, 8, 9 }, large.AsSpan(0, 6).ToArray());
        Assert.True(ring.IsEmpty);

        // Slots freed by the batch are reusable.
        for (int i = 0; i < ring.Capacity; i++)
            Assert.True(ring.TryEnqueue(i));
        Assert.False(ring.TryEnqueue(-1));
    }

    /// <remarks>
    /// <para>A producer waits for room and the consumer for an item through a <see cref="Doorbell"/>: each spins for a while
    /// and then blocks until it is rung, so that on a machine with no core to spare a wait costs a thread wake-up rather than
    /// the scheduler quantum a yielding spin loses at every yield (which made the run take minutes). A producer spins only
    /// <see cref="ProducerSpin"/>; producers ring the consumer per <c>ArrivalBatch</c> items, before they wait and at the end,
    /// and the consumer rings them per <c>RoomBatch</c> freed slots and before it waits (see the loop).</para>
    /// <para>At full speed the producers keep the ring full, so the consumer seldom looks at an empty ring while an item is
    /// published; one phase of <c>PhaseItems</c> in eight is therefore paced, every producer pausing a random 0-63 spins before
    /// each item. The run must have handed over <c>RequiredHandOvers</c> items at each end of the ring while the waiting
    /// thread spun, that is while both sides ran.</para>
    /// </remarks>
    [Fact]
    public void Stress_Eight_Producers_One_Consumer_Per_Producer_Sequence()
    {
        const int producers = 8;
        const int itemsPerProducer = 500_000;
        const int RoomBatch = 256;
        const int ArrivalBatch = 64;
        const int PhaseItems = 8_192;
        const long RequiredHandOvers = 20_000;
        long start = Stopwatch.GetTimestamp();
        var ring = new MpscRing<long>(1024);
        var room = new Doorbell[producers];
        using var arrival = new Doorbell();
        Exception? failure = null;
        int paced = 0;

        var threads = new Thread[producers];
        for (int p = 0; p < producers; p++)
        {
            long producerId = p;
            Doorbell bell = room[p] = new Doorbell(ProducerSpin);
            threads[p] = new Thread(() =>
            {
                try
                {
                    var random = new Random((int)producerId + 1);
                    int unrung = 0;
                    for (long seq = 0; seq < itemsPerProducer; seq++)
                    {
                        // In a paced phase the consumer catches up and looks at the empty ring as items arrive.
                        if (Volatile.Read(ref paced) != 0)
                            Thread.SpinWait(random.Next(0, 64));

                        long item = (producerId << 32) | seq;
                        if (!ring.TryEnqueue(item))
                        {
                            arrival.Ring();
                            unrung = 0;
                            do
                            {
                                bell.Wait();
                            }
                            while (!ring.TryEnqueue(item));
                            bell.Satisfied();
                        }

                        if (++unrung == ArrivalBatch)
                        {
                            arrival.Ring();
                            unrung = 0;
                        }
                    }

                    arrival.Ring();
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                }
            })
            { IsBackground = true, Name = "mpsc-producer-" + p };
        }

        foreach (Thread t in threads)
            t.Start();

        long[] expected = new long[producers];
        long received = 0;
        long[] batch = new long[32];
        bool useBatch = false;
        int freedSinceRing = 0;
        try
        {
            while (received < (long)producers * itemsPerProducer)
            {
                int count;
                if (useBatch)
                {
                    count = ring.TryDequeueBatch(batch);
                }
                else
                {
                    count = ring.TryDequeue(out batch[0]) ? 1 : 0;
                }

                useBatch = !useBatch;
                if (count == 0)
                {
                    // Producers that blocked on a full ring get the room freed since the last ring before the consumer waits.
                    if (freedSinceRing != 0)
                    {
                        Doorbell.RingAll(room);
                        freedSinceRing = 0;
                    }

                    arrival.Wait();
                    continue;
                }

                arrival.Satisfied();

                // Eight producers outrun one consumer, so the ring is full most of the time: ringing them per freed slot
                // would wake every blocked producer to fight over one slot. They are rung per RoomBatch freed slots
                // instead, and before the consumer waits; a producer that blocked saw the ring full, so the consumer
                // has at least a ring's worth of items, and with them several rings, still to come.
                freedSinceRing += count;
                if (freedSinceRing >= RoomBatch)
                {
                    Doorbell.RingAll(room);
                    freedSinceRing = 0;
                }

                for (int i = 0; i < count; i++)
                {
                    long item = batch[i];
                    int producer = (int)(item >> 32);
                    long seq = item & 0xFFFF_FFFF;
                    Assert.Equal(expected[producer], seq);
                    expected[producer]++;
                }

                received += count;

                // One phase in eight is paced, for every producer at once.
                int phase = ((received / PhaseItems) & 7) == 7 ? 1 : 0;
                if (phase != paced)
                    Volatile.Write(ref paced, phase);
            }

            foreach (Thread t in threads)
                Assert.True(t.Join(HandOff.Timeout));
        }
        finally
        {
            TestContext.Current.TestOutputHelper?.WriteLine(
                $"{received:N0} items in {Stopwatch.GetElapsedTime(start).TotalSeconds:F1} s: producers found the ring full {room.Sum(b => b.Waits):N0} "
                + $"times ({room.Sum(b => b.SpunWaits):N0} ended while they spun), the consumer found it empty {arrival.Waits:N0} times "
                + $"({arrival.SpunWaits:N0}).");
            foreach (Doorbell bell in room)
                bell.Dispose();
        }

        Assert.Null(failure);
        Assert.True(ring.IsEmpty);
        foreach (long e in expected)
            Assert.Equal(itemsPerProducer, e);

        // The windows a ring's slots can go wrong in, each run while the other side was running: room made while producers
        // looked at a full ring (and raced each other for it), and an item published while the consumer looked at an empty one.
        long roomHandOvers = room.Sum(b => b.SpunWaits);
        Assert.True(
            roomHandOvers >= RequiredHandOvers && arrival.SpunWaits >= RequiredHandOvers,
            $"The run did not exercise both ends of the ring: {roomHandOvers:N0} and {arrival.SpunWaits:N0} hand-overs of {RequiredHandOvers:N0} required.");
    }

    [Fact]
    public void Steady_State_Does_Not_Allocate()
    {
        var ring = new MpscRing<long>(64);
        long[] batch = new long[8];
        RunLoop(ring, batch, 10_000);

        WindowedAllocation.AssertNone(() => RunLoop(ring, batch, 20_000));

        static void RunLoop(MpscRing<long> ring, long[] batch, int iterations)
        {
            for (int i = 0; i < iterations; i++)
            {
                ring.TryEnqueue(i);
                ring.TryEnqueue(i + 1);
                ring.TryEnqueue(i + 2);
                ring.TryDequeue(out _);
                ring.TryDequeueBatch(batch);
                _ = ring.Count;
                _ = ring.IsEmpty;
            }
        }
    }

    [Fact]
    public void Pinned_Heap_Ring_Is_Aligned_Keeps_Fifo_And_Survives_Dispose()
    {
        // The completion table's free list: slots on the pinned object heap, 64-byte aligned like a native ring, and still
        // usable after Dispose (which frees nothing for this storage), so a late enqueue is harmless.
        var ring = new MpscRing<int>(100, pinnedObjectHeap: true);
        Assert.Equal(128, ring.Capacity);
        Assert.Equal(0, ring.Address % CacheLine.Size);
        for (int lap = 0; lap < 3; lap++)
        {
            for (int i = 0; i < ring.Capacity; i++)
                Assert.True(ring.TryEnqueue(lap * 1000 + i));
            Assert.False(ring.TryEnqueue(-1));
            for (int i = 0; i < ring.Capacity; i++)
            {
                Assert.True(ring.TryDequeue(out int item));
                Assert.Equal(lap * 1000 + i, item);
            }
        }

        ring.Dispose();
        ring.Dispose();
        Assert.True(ring.TryEnqueue(7));
        Assert.True(ring.TryDequeue(out int late));
        Assert.Equal(7, late);
        Assert.True(ring.IsEmpty);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(128)]
    [InlineData(1 << 20)]
    [InlineData(1 << 26)] // 1 GiB of 16-byte slots: the largest power of two whose block is an array
    public void Pinned_Heap_Block_Holds_The_Slots_Plus_The_Alignment_Slack(int capacity)
    {
        Assert.Equal((long)capacity * MpscRing<int>.SlotSize + CacheLine.Size - 1, MpscRing<int>.PinnedBlockLength(capacity));
    }

    [Fact]
    public void Pinned_Heap_Block_Length_Is_Accepted_Up_To_The_Largest_Array()
    {
        // The exact boundary, for two slot sizes (int: 16 bytes, Guid: 24 bytes).
        int maxInt = (int)((Array.MaxLength - (CacheLine.Size - 1L)) / MpscRing<int>.SlotSize);
        Assert.Equal((long)maxInt * 16 + 63, MpscRing<int>.PinnedBlockLength(maxInt));
        Assert.True(MpscRing<int>.PinnedBlockLength(maxInt) > Array.MaxLength - 16);
        Assert.Throws<ArgumentOutOfRangeException>(() => MpscRing<int>.PinnedBlockLength(maxInt + 1));

        int maxGuid = (int)((Array.MaxLength - (CacheLine.Size - 1L)) / MpscRing<Guid>.SlotSize);
        Assert.True(MpscRing<Guid>.PinnedBlockLength(maxGuid) <= Array.MaxLength);
        Assert.Throws<ArgumentOutOfRangeException>(() => MpscRing<Guid>.PinnedBlockLength(maxGuid + 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => MpscRing<int>.PinnedBlockLength(-1));
    }

    [Theory]
    [InlineData(1 << 27)] // 2 GiB of 16-byte slots: past the largest array
    [InlineData(1 << 28)] // 4 GiB: computed in 32 bits this wrapped to a 63-byte block
    [InlineData(1 << 30)] // the largest capacity a ring accepts (CompletionTable documents it): 16 GiB, also wrapped to 63 bytes
    public void Pinned_Heap_Ring_Longer_Than_The_Largest_Array_Is_Rejected_Instead_Of_Wrapping(int capacity)
    {
        Assert.Equal(16, MpscRing<int>.SlotSize);
        Assert.Throws<ArgumentOutOfRangeException>(() => MpscRing<int>.PinnedBlockLength(capacity));

        // The constructor sizes the block before it allocates anything, so this allocates nothing.
        Assert.Throws<ArgumentOutOfRangeException>(() => new MpscRing<int>(capacity, pinnedObjectHeap: true));
    }
}
