namespace Tedd.Quicly.Replication.Tests;

public class InterpolationBufferTests
{
    [Fact]
    public void Constructor_And_Properties_Validate()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new InterpolationBuffer<float>(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InterpolationBuffer<float>(4, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InterpolationBuffer<float>(4, 0, -1));
        InterpolationBuffer<float> buffer = new();
        Assert.Equal(InterpolationBuffer<float>.DefaultCapacity, buffer.Capacity);
        Assert.Equal(InterpolationBuffer<float>.DefaultDelayMicros, buffer.DelayMicros);
        Assert.Equal(InterpolationBuffer<float>.DefaultMaxExtrapolationMicros, buffer.MaxExtrapolationMicros);
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.DelayMicros = -1);
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.MaxExtrapolationMicros = -1);
        Assert.False(buffer.TryGetNewest(out long timestamp, out float value));
        Assert.Equal(0, timestamp);
        Assert.Equal(0f, value);
        Assert.Equal(InterpolationStatus.Empty, buffer.Sample(0, out float a, out float b, out float t));
        Assert.Equal((0f, 0f, 0f), (a, b, t));
    }

    [Fact]
    public void Single_Sample_Is_Held()
    {
        InterpolationBuffer<float> buffer = new(4, 0, 50_000);
        Assert.True(buffer.Add(1000, 5f));
        Assert.Equal(InterpolationStatus.Clamped, buffer.Sample(500, out float a, out float b, out float t));
        Assert.Equal((5f, 5f, 0f), (a, b, t));
        Assert.Equal(InterpolationStatus.Interpolated, buffer.Sample(1000, out a, out b, out t));
        Assert.Equal((5f, 5f, 0f), (a, b, t));
        Assert.Equal(InterpolationStatus.Clamped, buffer.Sample(2000, out a, out b, out t));
        Assert.Equal((5f, 5f, 0f), (a, b, t));
    }

    [Fact]
    public void Interpolates_Clamps_And_Caps_Extrapolation()
    {
        InterpolationBuffer<float> buffer = new(8, 0, 50_000);
        buffer.Add(0, 0f);
        buffer.Add(100_000, 10f);
        Assert.Equal(InterpolationStatus.Interpolated, buffer.Sample(50_000, out float a, out float b, out float t));
        Assert.Equal((0f, 10f, 0.5f), (a, b, t));
        Assert.Equal(InterpolationStatus.Interpolated, buffer.Sample(0, out a, out b, out t));
        Assert.Equal((0f, 10f, 0f), (a, b, t));
        Assert.Equal(InterpolationStatus.Interpolated, buffer.Sample(100_000, out a, out b, out t));
        Assert.Equal((0f, 10f, 1f), (a, b, t));
        Assert.Equal(InterpolationStatus.Clamped, buffer.Sample(-1, out a, out b, out t));
        Assert.Equal((0f, 0f, 0f), (a, b, t));
        Assert.Equal(InterpolationStatus.Extrapolated, buffer.Sample(120_000, out a, out b, out t));
        Assert.Equal((0f, 10f), (a, b));
        Assert.Equal(1.2f, t, 5);
        Assert.Equal(InterpolationStatus.Extrapolated, buffer.Sample(150_000, out _, out _, out t));
        Assert.Equal(1.5f, t, 5);
        Assert.Equal(InterpolationStatus.ExtrapolationLimited, buffer.Sample(1_000_000, out _, out _, out t));
        Assert.Equal(1.5f, t, 5);

        buffer.MaxExtrapolationMicros = 0;
        Assert.Equal(InterpolationStatus.ExtrapolationLimited, buffer.Sample(100_001, out _, out _, out t));
        Assert.Equal(1f, t);

        buffer.DelayMicros = 10_000;
        Assert.Equal(InterpolationStatus.Interpolated, buffer.Sample(60_000, out _, out _, out t));
        Assert.Equal(0.5f, t, 5);
    }

    [Fact]
    public void Out_Of_Order_And_Duplicate_Samples()
    {
        InterpolationBuffer<float> buffer = new(8, 0, 0);
        buffer.Add(0, 0f);
        buffer.Add(200, 20f);
        Assert.True(buffer.Add(100, 10f));
        Assert.Equal(InterpolationStatus.Interpolated, buffer.Sample(150, out float a, out float b, out float t));
        Assert.Equal((10f, 20f, 0.5f), (a, b, t));
        Assert.True(buffer.Add(100, 11f));
        Assert.Equal(3, buffer.Count);
        Assert.Equal(InterpolationStatus.Interpolated, buffer.Sample(100, out a, out b, out t));
        Assert.Equal((11f, 20f, 0f), (a, b, t));
        Assert.True(buffer.Add(-50, -5f));
        Assert.Equal(InterpolationStatus.Interpolated, buffer.Sample(-25, out a, out b, out t));
        Assert.Equal((-5f, 0f, 0.5f), (a, b, t));
        Assert.True(buffer.Add(200, 21f));
        Assert.True(buffer.TryGetNewest(out long newest, out float value));
        Assert.Equal((200L, 21f), (newest, value));
    }

    [Fact]
    public void A_Full_Ring_Drops_The_Oldest()
    {
        InterpolationBuffer<float> buffer = new(4, 0, 0);
        foreach (int time in new[] { 10, 20, 30, 40 })
        {
            Assert.True(buffer.Add(time, time));
        }

        Assert.True(buffer.Add(50, 50));
        Assert.Equal(4, buffer.Count);
        Assert.Equal(InterpolationStatus.Clamped, buffer.Sample(15, out float a, out _, out _));
        Assert.Equal(20f, a);
        Assert.False(buffer.Add(15, 15));
        Assert.True(buffer.Add(25, 25));
        Assert.Equal(InterpolationStatus.Clamped, buffer.Sample(24, out a, out _, out _));
        Assert.Equal(25f, a);
        Assert.True(buffer.Add(35, 35));
        Assert.Equal(InterpolationStatus.Interpolated, buffer.Sample(30, out a, out float b, out _));
        Assert.Equal((30f, 35f), (a, b));
        buffer.Clear();
        Assert.Equal(0, buffer.Count);
    }

    [Fact]
    public void Random_Inserts_Match_A_Sorted_Model()
    {
        Random random = new(30);
        InterpolationBuffer<float> buffer = new(16, 0, 0);
        SortedDictionary<long, float> model = [];
        for (int iteration = 0; iteration < 5000; iteration++)
        {
            long time = random.Next(0, 400) + (iteration / 4);
            float value = random.Next(1000);
            bool expectedAccepted = true;
            if (model.ContainsKey(time))
            {
                model[time] = value;
            }
            else if (model.Count == 16)
            {
                long oldest = model.Keys.First();
                if (time < oldest)
                {
                    expectedAccepted = false;
                }
                else
                {
                    model.Remove(oldest);
                    model[time] = value;
                }
            }
            else
            {
                model[time] = value;
            }

            Assert.Equal(expectedAccepted, buffer.Add(time, value));
            Assert.Equal(model.Count, buffer.Count);

            long[] times = [.. model.Keys];
            float[] values = [.. model.Values];
            for (int i = 0; i < times.Length; i++)
            {
                Assert.Equal(InterpolationStatus.Interpolated, buffer.Sample(times[i], out float a, out float b, out float t));
                if (i < times.Length - 1)
                {
                    Assert.Equal((values[i], values[i + 1], 0f), (a, b, t));
                }
                else if (times.Length > 1)
                {
                    Assert.Equal((values[i - 1], values[i], 1f), (a, b, t));
                }
            }
        }
    }
}
