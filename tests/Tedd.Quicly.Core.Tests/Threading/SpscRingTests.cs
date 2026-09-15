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

    [Fact]
    public void Stress_One_Producer_One_Consumer_Five_Million_Items_Checksum()
    {
        const long items = 5_000_000;
        var ring = new SpscRing<long>(1024);

        var producer = new Thread(() =>
        {
            SpinWait spinner = default;
            for (long i = 1; i <= items; i++)
            {
                while (!ring.TryEnqueue(i))
                    spinner.SpinOnce(sleep1Threshold: -1);
                spinner.Reset();
            }
        })
        { IsBackground = true, Name = "spsc-producer" };
        producer.Start();

        long sum = 0;
        long received = 0;
        long expected = 1;
        SpinWait consumerSpinner = default;
        while (received < items)
        {
            if (ring.TryDequeue(out long item))
            {
                Assert.Equal(expected++, item);
                sum += item;
                received++;
                consumerSpinner.Reset();
            }
            else
            {
                consumerSpinner.SpinOnce(sleep1Threshold: -1);
            }
        }

        producer.Join();
        Assert.Equal(items * (items + 1) / 2, sum);
        Assert.True(ring.IsEmpty);
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
