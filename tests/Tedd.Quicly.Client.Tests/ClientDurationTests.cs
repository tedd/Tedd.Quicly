namespace Tedd.Quicly.Client.Tests;

/// <summary>
/// The client converts its auto-flush interval to whole microseconds, where 0 means "off". A positive duration shorter than
/// one microsecond used to truncate to 0 and silently switch auto-flush off; it must round up to 1 instead, the same rule
/// as <c>PeerOptions.ToMicros</c> and <c>QuiclyServer.ToMicros</c>.
/// </summary>
public class ClientDurationTests
{
    [Theory]
    [InlineData(1L, 1L)]           // 100 ns: positive, so it rounds up rather than meaning "off"
    [InlineData(5L, 1L)]           // 500 ns
    [InlineData(9L, 1L)]           // 900 ns
    [InlineData(10L, 1L)]          // exactly 1 µs
    [InlineData(15L, 1L)]          // 1.5 µs truncates to whole microseconds above 1
    [InlineData(100_000L, 10_000L)] // 10 ms
    public void A_Positive_Duration_Converts_To_At_Least_One_Microsecond(long ticks, long expected)
    {
        Assert.Equal(expected, QuiclyClient.ToMicros(TimeSpan.FromTicks(ticks)));
    }

    [Fact]
    public void Zero_Stays_Zero_So_Auto_Flush_Can_Still_Be_Switched_Off()
    {
        Assert.Equal(0L, QuiclyClient.ToMicros(TimeSpan.Zero));
    }

    [Fact]
    public void A_Negative_Duration_Never_Becomes_A_Positive_Interval()
    {
        Assert.True(QuiclyClient.ToMicros(TimeSpan.FromTicks(-5)) <= 0);
        Assert.True(QuiclyClient.ToMicros(TimeSpan.FromMilliseconds(-10)) <= 0);
    }
}
