using System.Diagnostics;
using Tedd.Quicly.Core.Time;

namespace Tedd.Quicly.Core.Tests.Time;

public class ClockTests
{
    [Fact]
    public void MonotonicClock_Is_Non_Decreasing_And_Advances()
    {
        IClock clock = MonotonicClock.Instance;
        Assert.Same(MonotonicClock.Instance, clock);

        long previous = clock.NowMicros;
        Assert.True(previous >= 0);
        for (int i = 0; i < 100_000; i++)
        {
            long now = clock.NowMicros;
            Assert.True(now >= previous);
            previous = now;
        }

        long start = clock.NowMicros;
        Thread.Sleep(20);
        long elapsed = clock.NowMicros - start;
        Assert.InRange(elapsed, 15_000, 5_000_000);
    }

    [Fact]
    public void MonotonicClock_Does_Not_Allocate()
    {
        IClock clock = MonotonicClock.Instance;
        long sink = 0;
        for (int i = 0; i < 1000; i++)
            sink += clock.NowMicros;

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100_000; i++)
            sink += clock.NowMicros;
        long after = GC.GetAllocatedBytesForCurrentThread();

        Assert.Equal(0, after - before);
        Assert.True(sink > 0);
    }

    [Fact]
    public void MonotonicClock_Tick_Conversion_Is_Exact_Without_Overflow()
    {
        long frequency = Stopwatch.Frequency;
        Assert.Equal(0, MonotonicClock.ToMicros(0));
        Assert.Equal(1_000_000, MonotonicClock.ToMicros(frequency));
        Assert.Equal(500_000, MonotonicClock.ToMicros(frequency / 2));

        // Ten years of ticks at any plausible frequency would overflow a naive ticks * 1e6 product.
        long tenYears = frequency * 60 * 60 * 24 * 365 * 10;
        Assert.Equal(60L * 60 * 24 * 365 * 10 * 1_000_000, MonotonicClock.ToMicros(tenYears));
    }

    [Fact]
    public void VirtualClock_Starts_At_Given_Value_And_Advances()
    {
        var clock = new VirtualClock();
        Assert.Equal(0, clock.NowMicros);

        clock.Advance(TimeSpan.FromMilliseconds(1.5));
        Assert.Equal(1_500, clock.NowMicros);

        clock.AdvanceMicros(10);
        Assert.Equal(1_510, clock.NowMicros);

        clock.Advance(TimeSpan.Zero);
        Assert.Equal(1_510, clock.NowMicros);

        var started = new VirtualClock(42);
        Assert.Equal(42, started.NowMicros);
    }

    [Fact]
    public void VirtualClock_Set_Moves_To_Absolute_Value_In_Either_Direction()
    {
        var clock = new VirtualClock(1_000);
        clock.Set(5_000);
        Assert.Equal(5_000, clock.NowMicros);
        clock.Set(10);
        Assert.Equal(10, clock.NowMicros);
    }

    [Fact]
    public void VirtualClock_Rejects_Negative_Values()
    {
        var clock = new VirtualClock();
        Assert.Throws<ArgumentOutOfRangeException>(() => new VirtualClock(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => clock.Advance(TimeSpan.FromTicks(-10)));
        Assert.Throws<ArgumentOutOfRangeException>(() => clock.AdvanceMicros(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => clock.Set(-1));
        Assert.Equal(0, clock.NowMicros);
    }

    [Fact]
    public void VirtualClock_Is_Usable_Through_The_Interface()
    {
        IClock clock = new VirtualClock(7);
        Assert.Equal(7, clock.NowMicros);
    }
}
