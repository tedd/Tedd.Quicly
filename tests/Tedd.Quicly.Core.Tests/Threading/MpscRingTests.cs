using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Tests.Threading;

public class MpscRingTests
{
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

    [Fact]
    public void Stress_Eight_Producers_One_Consumer_Per_Producer_Sequence()
    {
        const int producers = 8;
        const int itemsPerProducer = 500_000;
        var ring = new MpscRing<long>(1024);

        var threads = new Thread[producers];
        for (int p = 0; p < producers; p++)
        {
            long producerId = p;
            threads[p] = new Thread(() =>
            {
                SpinWait spinner = default;
                for (long seq = 0; seq < itemsPerProducer; seq++)
                {
                    long item = (producerId << 32) | seq;
                    while (!ring.TryEnqueue(item))
                        spinner.SpinOnce(sleep1Threshold: -1);
                    spinner.Reset();
                }
            })
            { IsBackground = true, Name = "mpsc-producer-" + p };
        }

        foreach (Thread t in threads)
            t.Start();

        long[] expected = new long[producers];
        long received = 0;
        long[] batch = new long[32];
        SpinWait consumerSpinner = default;
        bool useBatch = false;
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
                consumerSpinner.SpinOnce(sleep1Threshold: -1);
                continue;
            }

            consumerSpinner.Reset();
            for (int i = 0; i < count; i++)
            {
                long item = batch[i];
                int producer = (int)(item >> 32);
                long seq = item & 0xFFFF_FFFF;
                Assert.Equal(expected[producer], seq);
                expected[producer]++;
            }

            received += count;
        }

        foreach (Thread t in threads)
            t.Join();

        Assert.True(ring.IsEmpty);
        foreach (long e in expected)
            Assert.Equal(itemsPerProducer, e);
    }

    [Fact]
    public void Steady_State_Does_Not_Allocate()
    {
        var ring = new MpscRing<long>(64);
        long[] batch = new long[8];
        RunLoop(ring, batch, 10_000);

        long before = GC.GetAllocatedBytesForCurrentThread();
        RunLoop(ring, batch, 100_000);
        long after = GC.GetAllocatedBytesForCurrentThread();

        Assert.Equal(0, after - before);

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
}
