using System.Net;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme;

/// <summary>
/// Exponential back-off with jitter for retrying transient ACME failures (rate limits, 5xx, connection errors), honouring
/// the server's <c>Retry-After</c> (ADR 0009).
/// </summary>
public sealed class AcmeRetryOptions
{
    /// <summary>Total attempts (1 = no retry). Default 5.</summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>Delay before the first retry. Default 5 s.</summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Growth factor per retry. Default 2.</summary>
    public double Multiplier { get; set; } = 2.0;

    /// <summary>Cap for the computed back-off (before jitter; <c>Retry-After</c> may exceed it). Default 10 min.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Jitter as a fraction of the delay: the delay is multiplied by a uniform random factor in <c>[1 - Jitter, 1 + Jitter]</c>. Default 0.2; 0 disables jitter.</summary>
    public double Jitter { get; set; } = 0.2;

    /// <summary>Longest <c>Retry-After</c> that is honoured as-is; larger values are clamped. Default 1 h.</summary>
    public TimeSpan MaxRetryAfter { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Random source for jitter (replace in tests for determinism).</summary>
    public Random Random { get; set; } = Random.Shared;

    /// <summary>Validates the option values.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A value is out of range.</exception>
    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxAttempts, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(InitialDelay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(Multiplier, 1.0);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxDelay, InitialDelay);
        ArgumentOutOfRangeException.ThrowIfNegative(Jitter);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(Jitter, 1.0);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxRetryAfter, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(Random);
    }

    /// <summary>
    /// Computes the wait before retry number <paramref name="retry"/> (1-based): <c>min(InitialDelay * Multiplier^(retry-1), MaxDelay)</c>
    /// scaled by the jitter factor; a server <c>Retry-After</c> (clamped to <see cref="MaxRetryAfter"/>) is a lower bound.
    /// </summary>
    public TimeSpan GetDelay(int retry, TimeSpan? retryAfter)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(retry, 1);
        double ticks = InitialDelay.Ticks * Math.Pow(Multiplier, retry - 1);
        if (ticks > MaxDelay.Ticks)
        {
            ticks = MaxDelay.Ticks;
        }

        if (Jitter > 0)
        {
            double factor = 1.0 - Jitter + (Random.NextDouble() * 2.0 * Jitter);
            ticks *= factor;
        }

        TimeSpan delay = TimeSpan.FromTicks((long)ticks);
        if (retryAfter is TimeSpan ra && ra > TimeSpan.Zero)
        {
            TimeSpan clamped = ra > MaxRetryAfter ? MaxRetryAfter : ra;
            if (clamped > delay)
            {
                delay = clamped;
            }
        }

        return delay;
    }

    /// <summary>
    /// True when <paramref name="exception"/> is worth retrying: <c>rateLimited</c>, <c>badNonce</c>, <c>serverInternal</c>,
    /// any 5xx / 429 response, or a transport failure (<see cref="HttpRequestException"/>, <see cref="IOException"/>, HTTP timeout).
    /// </summary>
    public static bool IsTransient(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        switch (exception)
        {
            case AcmeException acme:
                if (acme.IsType(AcmeErrorTypes.RateLimited) || acme.IsType(AcmeErrorTypes.BadNonce) || acme.IsType(AcmeErrorTypes.ServerInternal))
                {
                    return true;
                }

                return acme.StatusCode is HttpStatusCode status && ((int)status >= 500 || status == HttpStatusCode.TooManyRequests);
            case HttpRequestException:
            case IOException:
                return true;
            case TaskCanceledException tce:
                // HttpClient signals its own timeout as TaskCanceledException with a TimeoutException inside.
                return tce.InnerException is TimeoutException;
            default:
                return false;
        }
    }
}
