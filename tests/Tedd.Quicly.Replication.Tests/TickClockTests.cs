namespace Tedd.Quicly.Replication.Tests;

public class FixedTickClockTests
{
    [Fact]
    public void Constructor_Validates()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new FixedTickClock(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new FixedTickClock(1_000_001));
        Assert.Throws<ArgumentOutOfRangeException>(() => new FixedTickClock(60, 0));
        FixedTickClock clock = new(60, 5);
        Assert.Equal(60, clock.TickRateHz);
        Assert.Equal(5, clock.MaxCatchUpTicks);
        Assert.Equal(1_000_000.0 / 60, clock.TickDurationMicros, 9);
        Assert.Equal(0, clock.Tick);
        Assert.Equal(0f, clock.InterpolationAlpha);
    }

    [Fact]
    public void Accumulates_Exactly_Without_Drift()
    {
        FixedTickClock clock = new(60, 100);
        int ticks = 0;
        for (int i = 0; i < 60; i++)
        {
            ticks += clock.Advance(16_666);
        }

        Assert.Equal(59, ticks);
        ticks += clock.Advance(40);
        Assert.Equal(60, ticks);
        Assert.Equal(60, clock.Tick);
        Assert.Equal(0f, clock.InterpolationAlpha);

        Random random = new(40);
        FixedTickClock jittery = new(60, 100);
        long remaining = 10_000_000;
        int total = 0;
        while (remaining > 0)
        {
            long frame = Math.Min(remaining, random.Next(1, 50_000));
            remaining -= frame;
            total += jittery.Advance(frame);
            Assert.InRange(jittery.InterpolationAlpha, 0f, 1f);
        }

        Assert.Equal(600, total);
        Assert.Equal(0, jittery.DroppedTicks);
    }

    [Fact]
    public void Catch_Up_Is_Capped_And_The_Fraction_Kept()
    {
        FixedTickClock clock = new(60, 4);
        Assert.Equal(4, clock.Advance(1_000_000));
        Assert.Equal(56, clock.DroppedTicks);
        Assert.Equal(4, clock.Tick);
        Assert.Equal(0f, clock.InterpolationAlpha);
        Assert.Equal(0, clock.Advance(8_333));
        Assert.Equal(0.49998f, clock.InterpolationAlpha, 4);
        Assert.Equal(0, clock.Advance(-5_000));
        Assert.Equal(4, clock.Advance(long.MaxValue));
        Assert.True(clock.DroppedTicks > 56);
    }

    [Fact]
    public void AdvanceTo_Uses_The_First_Call_As_Reference()
    {
        FixedTickClock clock = new(60);
        Assert.Equal(0, clock.AdvanceTo(5_000_000));
        Assert.Equal(2, clock.AdvanceTo(5_033_334));
        Assert.Equal(0, clock.AdvanceTo(5_040_000));
        Assert.Equal(2, clock.Tick);
        clock.Reset();
        Assert.Equal(0, clock.Tick);
        Assert.Equal(0, clock.DroppedTicks);
        Assert.Equal(0f, clock.InterpolationAlpha);
        Assert.Equal(0, clock.AdvanceTo(100));
        Assert.Equal(1, clock.AdvanceTo(100 + 16_667));
    }
}

public class ServerTickEstimatorTests
{
    [Fact]
    public void Constructor_Validates()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServerTickEstimator(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServerTickEstimator(60, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServerTickEstimator(60, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServerTickEstimator(60, double.NaN));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServerTickEstimator(60, 0.05, -1));
        ServerTickEstimator estimator = new(30, 0.1, 1000);
        Assert.Equal(30, estimator.TickRateHz);
        Assert.Equal(0.1, estimator.MaxSlewRate);
        Assert.Equal(1000, estimator.StepThresholdMicros);
    }

    [Fact]
    public void Needs_An_Observation_Before_Locking()
    {
        ServerTickEstimator estimator = new(60);
        estimator.SetClockOffset(1_000_000);
        Assert.Equal(0, estimator.Update(5_000_000));
        Assert.False(estimator.IsLocked);
        Assert.Equal(0, estimator.EstimatedTick);
        Assert.Equal(0, estimator.CurrentTick);
    }

    [Fact]
    public void Locks_Onto_Offset_And_Server_Tick_Timeline()
    {
        ServerTickEstimator estimator = new(60);
        estimator.SetClockOffset(1_000_000);
        estimator.ObserveServerTick(600, 11_000_000);   // tick 600 began at server time 11 s: tick 0 at server time 1 s
        Assert.Equal(0, estimator.TargetCorrectionMicros, 6);
        Assert.Equal(600, estimator.Update(10_000_000), 6);
        Assert.True(estimator.IsLocked);
        Assert.Equal(603.0, estimator.Update(10_050_000), 6);
        Assert.Equal(603, estimator.CurrentTick);
        Assert.Equal(0, estimator.StepCount);
    }

    [Fact]
    public void Small_Corrections_Are_Slewed_Monotonically()
    {
        ServerTickEstimator estimator = new(60, 0.05, 250_000);
        estimator.ObserveServerTick(0, 0);
        estimator.Update(1_000_000);
        Assert.Equal(0, estimator.CorrectionMicros);

        estimator.SetClockOffset(10_000);                // +10 ms
        estimator.Update(1_100_000);                     // 100 ms elapsed: at most 5 ms applied
        Assert.Equal(5_000, estimator.CorrectionMicros, 6);
        estimator.Update(1_200_000);
        Assert.Equal(10_000, estimator.CorrectionMicros, 6);
        estimator.Update(1_300_000);
        Assert.Equal(10_000, estimator.CorrectionMicros, 6);

        estimator.SetClockOffset(-190_000);              // -200 ms, below the step threshold
        double previous = estimator.EstimatedTick;
        for (long local = 1_310_000; local < 6_000_000; local += 10_000)
        {
            double tick = estimator.Update(local);
            Assert.True(tick > previous);
            previous = tick;
        }

        Assert.Equal(-190_000, estimator.CorrectionMicros, 6);
        Assert.Equal(0, estimator.StepCount);

        // Time going backwards applies no slew.
        estimator.SetClockOffset(0);
        estimator.Update(5_000_000);
        Assert.Equal(-190_000, estimator.CorrectionMicros, 6);
    }

    [Fact]
    public void Large_Errors_Step_And_Resync_Jumps()
    {
        ServerTickEstimator estimator = new(60);
        estimator.ObserveServerTick(0, 0);
        estimator.Update(0);
        estimator.SetClockOffset(1_000_000);
        estimator.Update(10_000);
        Assert.Equal(1_000_000, estimator.CorrectionMicros, 6);
        Assert.Equal(1, estimator.StepCount);

        estimator.ObserveServerTick(100, 1_000_000);      // server fell 1/0.6 s behind its schedule: small change
        estimator.SetClockOffset(1_010_000);
        estimator.Resync();
        estimator.Update(20_000);
        Assert.Equal(estimator.TargetCorrectionMicros, estimator.CorrectionMicros, 6);
        Assert.Equal(1, estimator.StepCount);
    }
}
