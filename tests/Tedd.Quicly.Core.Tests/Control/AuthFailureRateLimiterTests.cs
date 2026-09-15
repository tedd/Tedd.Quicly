using System.Net;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Time;

namespace Tedd.Quicly.Core.Tests.Control;

public class AuthFailureRateLimiterTests
{
    private static readonly IPAddress A = IPAddress.Parse("10.0.0.1");
    private static readonly IPAddress B = IPAddress.Parse("10.0.0.2");

    private static void Fail(AuthFailureRateLimiter limiter, IPAddress address, int times)
    {
        for (int i = 0; i < times; i++)
        {
            limiter.RecordFailure(address);
        }
    }

    [Fact]
    public void Burst_Then_Blocked_Then_One_Attempt_Per_Interval()
    {
        VirtualClock clock = new(10_000);
        AuthFailureRateLimiter limiter = new(clock, capacity: 64, burst: 3, refillIntervalMicros: 1000);
        Assert.True(limiter.IsAllowed(A));
        Fail(limiter, A, 2);
        Assert.True(limiter.IsAllowed(A));
        Fail(limiter, A, 1);
        Assert.False(limiter.IsAllowed(A));

        clock.AdvanceMicros(999);
        Assert.False(limiter.IsAllowed(A));
        clock.AdvanceMicros(1);
        Assert.True(limiter.IsAllowed(A));
        Fail(limiter, A, 1);
        Assert.False(limiter.IsAllowed(A));

        // After burst × interval with no failures the address is back to a full bucket.
        clock.AdvanceMicros(3000);
        Fail(limiter, A, 2);
        Assert.True(limiter.IsAllowed(A));
    }

    [Fact]
    public void Hammering_Does_Not_Extend_The_Block_Beyond_One_Interval()
    {
        VirtualClock clock = new(0);
        AuthFailureRateLimiter limiter = new(clock, burst: 3, refillIntervalMicros: 1000);
        Fail(limiter, A, 1000);
        Assert.False(limiter.IsAllowed(A));
        clock.AdvanceMicros(999);
        Assert.False(limiter.IsAllowed(A));
        clock.AdvanceMicros(1);
        Assert.True(limiter.IsAllowed(A));
    }

    [Fact]
    public void Burst_Of_One()
    {
        VirtualClock clock = new(0);
        AuthFailureRateLimiter limiter = new(clock, burst: 1, refillIntervalMicros: 10);
        Fail(limiter, A, 1);
        Assert.False(limiter.IsAllowed(A));
        clock.AdvanceMicros(10);
        Assert.True(limiter.IsAllowed(A));
    }

    [Fact]
    public void Addresses_Are_Independent_And_Ipv4_Mapped_Is_The_Same_Address()
    {
        VirtualClock clock = new(0);
        AuthFailureRateLimiter limiter = new(clock, burst: 2, refillIntervalMicros: 1000);
        Fail(limiter, A, 2);
        Assert.False(limiter.IsAllowed(A));
        Assert.True(limiter.IsAllowed(B));
        Assert.False(limiter.IsAllowed(IPAddress.Parse("::ffff:10.0.0.1")));
        Assert.False(limiter.IsAllowed([10, 0, 0, 1]));
        Assert.False(limiter.IsAllowed([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 10, 0, 0, 1]));
        Assert.True(limiter.IsAllowed([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 10, 0, 0, 2]));
        Assert.True(limiter.IsAllowed(IPAddress.Parse("::10.0.0.1"))); // IPv4-compatible (deprecated) is a different key
    }

    [Theory]
    [InlineData(64, "2001:db8::1", "2001:db8::ffff:2", true)]
    [InlineData(64, "2001:db8::1", "2001:db8:0:1::1", false)]
    [InlineData(128, "2001:db8::1", "2001:db8::2", false)]
    [InlineData(128, "2001:db8::1", "2001:db8::1", true)]
    [InlineData(96, "2001:db8::1:0:1", "2001:db8::1:0:2", true)]
    [InlineData(96, "2001:db8::1:0:1", "2001:db8::2:0:1", false)]
    [InlineData(32, "2001:db8:1::", "2001:db8:2::", true)]
    [InlineData(32, "2001:db8::", "2001:db9::", false)]
    public void Ipv6_Is_Aggregated_By_Prefix(int prefix, string blocked, string probe, bool sharesBucket)
    {
        VirtualClock clock = new(0);
        AuthFailureRateLimiter limiter = new(clock, burst: 1, refillIntervalMicros: 1000, ipv6PrefixLength: prefix);
        limiter.RecordFailure(IPAddress.Parse(blocked));
        Assert.False(limiter.IsAllowed(IPAddress.Parse(blocked)));
        Assert.Equal(!sharesBucket, limiter.IsAllowed(IPAddress.Parse(probe)));
    }

    [Fact]
    public void Full_Bucket_Falls_Back_To_The_Shared_Overflow_Bucket()
    {
        VirtualClock clock = new(0);
        AuthFailureRateLimiter limiter = new(clock, capacity: 8, burst: 3, refillIntervalMicros: 1000); // one 8-way bucket
        Assert.Equal(8, limiter.Capacity);
        for (int i = 1; i <= 8; i++)
        {
            limiter.RecordFailure([10, 0, 0, (byte)i]);
        }

        Assert.Equal(8, limiter.ActiveCount);
        Assert.Equal(0, limiter.OverflowFailures);
        Assert.True(limiter.IsAllowed([10, 0, 1, 1])); // untracked, overflow bucket still full

        for (int i = 1; i <= 3; i++)
        {
            limiter.RecordFailure([10, 0, 1, (byte)i]);
        }

        Assert.Equal(3, limiter.OverflowFailures);
        Assert.False(limiter.IsAllowed([10, 0, 2, 1])); // every untracked address now shares the exhausted overflow bucket
        Assert.True(limiter.IsAllowed([10, 0, 0, 1]));  // tracked addresses keep their own state

        clock.AdvanceMicros(1000); // tracked entries (one failure each) are idle again
        Assert.Equal(0, limiter.ActiveCount);
        Assert.True(limiter.IsAllowed([10, 0, 2, 1]));
        limiter.RecordFailure([10, 0, 3, 1]);
        Assert.Equal(3, limiter.OverflowFailures);
        Assert.Equal(1, limiter.ActiveCount);
    }

    [Fact]
    public void Many_Addresses_Stay_Within_The_Table()
    {
        VirtualClock clock = new(0);
        AuthFailureRateLimiter limiter = new(clock, capacity: 1024);
        for (int i = 0; i < 3000; i++)
        {
            limiter.RecordFailure([192, 168, (byte)(i >> 8), (byte)i]);
        }

        Assert.Equal(1024, limiter.ActiveCount);
        Assert.Equal(3000 - 1024, limiter.OverflowFailures);
    }

    [Fact]
    public void Concurrent_Failures_Are_All_Counted()
    {
        VirtualClock clock = new(0);
        AuthFailureRateLimiter limiter = new(clock, burst: 1000, refillIntervalMicros: 1);
        Parallel.For(0, 999, _ => limiter.RecordFailure(A));
        Assert.True(limiter.IsAllowed(A));
        limiter.RecordFailure(A);
        Assert.False(limiter.IsAllowed(A));
    }

    [Fact]
    public void Argument_Validation()
    {
        VirtualClock clock = new(0);
        Assert.Throws<ArgumentNullException>(() => new AuthFailureRateLimiter(null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => new AuthFailureRateLimiter(clock, capacity: 7));
        Assert.Throws<ArgumentOutOfRangeException>(() => new AuthFailureRateLimiter(clock, capacity: (1 << 22) + 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new AuthFailureRateLimiter(clock, burst: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new AuthFailureRateLimiter(clock, burst: 1_000_001));
        Assert.Throws<ArgumentOutOfRangeException>(() => new AuthFailureRateLimiter(clock, refillIntervalMicros: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new AuthFailureRateLimiter(clock, refillIntervalMicros: 86_400_000_001L));
        Assert.Throws<ArgumentOutOfRangeException>(() => new AuthFailureRateLimiter(clock, ipv6PrefixLength: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new AuthFailureRateLimiter(clock, ipv6PrefixLength: 129));
        Assert.Equal(16, new AuthFailureRateLimiter(clock, capacity: 10).Capacity);

        AuthFailureRateLimiter limiter = new(clock);
        Assert.Throws<ArgumentException>(() => limiter.IsAllowed(new byte[5]));
        Assert.Throws<ArgumentException>(() => limiter.RecordFailure(ReadOnlySpan<byte>.Empty));
        Assert.Throws<ArgumentNullException>(() => limiter.IsAllowed((IPAddress)null!));
        Assert.Throws<ArgumentNullException>(() => limiter.RecordFailure((IPAddress)null!));
    }
}
