namespace Tedd.Quicly.Acme;

/// <summary>Delay helpers shared by the client, the manager and the scheduler.</summary>
internal static class AcmeTimers
{
    /// <summary>
    /// Longest single timer wait. <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/> throws above
    /// <c>uint.MaxValue - 1</c> milliseconds (about 49.7 days); longer waits are clamped here, and every caller
    /// re-evaluates its condition after waking, so clamping only splits a long wait into chunks.
    /// </summary>
    public static readonly TimeSpan MaxDelay = TimeSpan.FromDays(30);

    /// <summary>
    /// Clamps <paramref name="delay"/> to <c>[0, <see cref="MaxDelay"/>]</c> and rounds a positive delay <b>up</b> to a
    /// whole millisecond. Timers have millisecond resolution and truncate, so a wait for "the time left until X" would
    /// otherwise wake a fraction of a millisecond before X; the sub-millisecond remainder then rounds to a zero delay
    /// that completes without waiting, and a loop re-checking <c>now &lt; X</c> spins (forever on a simulated clock).
    /// </summary>
    public static TimeSpan Clamp(TimeSpan delay)
    {
        if (delay <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        if (delay >= MaxDelay)
        {
            return MaxDelay;
        }

        long milliseconds = (delay.Ticks + TimeSpan.TicksPerMillisecond - 1) / TimeSpan.TicksPerMillisecond;
        return TimeSpan.FromTicks(milliseconds * TimeSpan.TicksPerMillisecond);
    }

    /// <summary><see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/> with <paramref name="delay"/> clamped by <see cref="Clamp"/>.</summary>
    public static Task Delay(TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        return Task.Delay(Clamp(delay), timeProvider, cancellationToken);
    }
}
