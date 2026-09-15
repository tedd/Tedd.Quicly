using System.Net;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme.Tests;

public class AcmeRetryOptionsTests
{
    [Fact]
    public void Defaults()
    {
        AcmeRetryOptions o = new();
        Assert.Equal(5, o.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(5), o.InitialDelay);
        Assert.Equal(2.0, o.Multiplier);
        Assert.Equal(TimeSpan.FromMinutes(10), o.MaxDelay);
        Assert.Equal(0.2, o.Jitter);
        Assert.Equal(TimeSpan.FromHours(1), o.MaxRetryAfter);
        Assert.Same(Random.Shared, o.Random);
        o.Validate();
    }

    [Fact]
    public void Validate_RejectsBadValues()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new AcmeRetryOptions { MaxAttempts = 0 }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new AcmeRetryOptions { InitialDelay = TimeSpan.FromSeconds(-1) }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new AcmeRetryOptions { Multiplier = 0.5 }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new AcmeRetryOptions { MaxDelay = TimeSpan.FromSeconds(1) }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new AcmeRetryOptions { Jitter = -0.1 }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new AcmeRetryOptions { Jitter = 1.5 }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new AcmeRetryOptions { MaxRetryAfter = TimeSpan.FromSeconds(-1) }.Validate());
        Assert.Throws<ArgumentNullException>(() => new AcmeRetryOptions { Random = null! }.Validate());
    }

    [Fact]
    public void GetDelay_GrowsExponentially_AndIsCapped()
    {
        AcmeRetryOptions o = new() { InitialDelay = TimeSpan.FromSeconds(1), Multiplier = 3, MaxDelay = TimeSpan.FromSeconds(20), Jitter = 0 };
        Assert.Equal(TimeSpan.FromSeconds(1), o.GetDelay(1, null));
        Assert.Equal(TimeSpan.FromSeconds(3), o.GetDelay(2, null));
        Assert.Equal(TimeSpan.FromSeconds(9), o.GetDelay(3, null));
        Assert.Equal(TimeSpan.FromSeconds(20), o.GetDelay(4, null)); // 27 capped
        Assert.Equal(TimeSpan.FromSeconds(20), o.GetDelay(50, null)); // no overflow
        Assert.Throws<ArgumentOutOfRangeException>(() => o.GetDelay(0, null));
    }

    [Fact]
    public void GetDelay_HonoursRetryAfter_AsLowerBound_ClampedToMax()
    {
        AcmeRetryOptions o = new() { InitialDelay = TimeSpan.FromSeconds(1), MaxDelay = TimeSpan.FromSeconds(4), Jitter = 0, MaxRetryAfter = TimeSpan.FromMinutes(5) };
        Assert.Equal(TimeSpan.FromSeconds(30), o.GetDelay(1, TimeSpan.FromSeconds(30))); // Retry-After wins over 1 s
        Assert.Equal(TimeSpan.FromSeconds(2), o.GetDelay(2, TimeSpan.FromSeconds(1)));   // back-off wins over 1 s
        Assert.Equal(TimeSpan.FromSeconds(1), o.GetDelay(1, TimeSpan.Zero));               // zero / negative are ignored
        Assert.Equal(TimeSpan.FromSeconds(1), o.GetDelay(1, TimeSpan.FromSeconds(-5)));
        Assert.Equal(TimeSpan.FromMinutes(5), o.GetDelay(1, TimeSpan.FromHours(3)));       // clamped, but above MaxDelay
    }

    [Fact]
    public void GetDelay_Jitter_StaysWithinBounds_AndVaries()
    {
        AcmeRetryOptions o = new() { InitialDelay = TimeSpan.FromSeconds(10), Jitter = 0.25, Random = new Random(1234) };
        HashSet<long> seen = [];
        for (int i = 0; i < 200; i++)
        {
            TimeSpan d = o.GetDelay(1, null);
            Assert.InRange(d, TimeSpan.FromSeconds(7.5), TimeSpan.FromSeconds(12.5));
            seen.Add(d.Ticks);
        }

        Assert.True(seen.Count > 100, "jitter should produce varying delays");

        // Jitter is applied to the back-off, and Retry-After still acts as the floor.
        Assert.Equal(TimeSpan.FromMinutes(1), o.GetDelay(1, TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void IsTransient_Classification()
    {
        Assert.True(AcmeRetryOptions.IsTransient(new AcmeException(new AcmeProblem { Type = AcmeErrorTypes.RateLimited })));
        Assert.True(AcmeRetryOptions.IsTransient(new AcmeException(new AcmeProblem { Type = AcmeErrorTypes.BadNonce })));
        Assert.True(AcmeRetryOptions.IsTransient(new AcmeException(new AcmeProblem { Type = AcmeErrorTypes.ServerInternal })));
        Assert.True(AcmeRetryOptions.IsTransient(new AcmeException(new AcmeProblem { Type = AcmeErrorTypes.Unknown }, HttpStatusCode.BadGateway)));
        Assert.True(AcmeRetryOptions.IsTransient(new AcmeException(new AcmeProblem { Type = AcmeErrorTypes.Unknown }, HttpStatusCode.TooManyRequests)));
        Assert.False(AcmeRetryOptions.IsTransient(new AcmeException(new AcmeProblem { Type = AcmeErrorTypes.Malformed }, HttpStatusCode.BadRequest)));
        Assert.False(AcmeRetryOptions.IsTransient(new AcmeException(new AcmeProblem { Type = AcmeErrorTypes.ValidationFailed })));
        Assert.False(AcmeRetryOptions.IsTransient(new AcmeException(new AcmeProblem { Type = AcmeErrorTypes.Unknown }, HttpStatusCode.NotFound)));
        Assert.True(AcmeRetryOptions.IsTransient(new HttpRequestException("refused")));
        Assert.True(AcmeRetryOptions.IsTransient(new IOException("reset")));
        Assert.True(AcmeRetryOptions.IsTransient(new TaskCanceledException("timeout", new TimeoutException())));
        Assert.False(AcmeRetryOptions.IsTransient(new TaskCanceledException("cancelled")));
        Assert.False(AcmeRetryOptions.IsTransient(new InvalidOperationException()));
        Assert.Throws<ArgumentNullException>(() => AcmeRetryOptions.IsTransient(null!));
    }
}
