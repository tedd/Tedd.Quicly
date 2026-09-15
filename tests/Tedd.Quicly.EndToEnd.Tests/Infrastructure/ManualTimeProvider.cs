namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>
/// A clock whose timers fire only when the test advances it, synchronously inside <see cref="Advance"/>. The certificate
/// binder's grace period runs on it, so "not disposed before the grace period ends, disposed once it has" is asserted
/// without racing the machine's timing.
/// </summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    private readonly Lock _lock = new();
    private readonly List<ManualTimer> _timers = [];
    private DateTimeOffset _now = new(2026, 9, 15, 12, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow()
    {
        lock (_lock)
        {
            return _now;
        }
    }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ManualTimer timer = new(this, callback, state);
        timer.Change(dueTime, period);
        return timer;
    }

    /// <summary>Moves the clock forward and runs every timer that became due, on the calling thread.</summary>
    public void Advance(TimeSpan by)
    {
        List<ManualTimer> due = [];
        lock (_lock)
        {
            _now += by;
            foreach (ManualTimer timer in _timers)
            {
                if (timer.DueAt is { } at && at <= _now)
                {
                    due.Add(timer);
                    timer.DueAt = timer.Period > TimeSpan.Zero ? at + timer.Period : null;
                }
            }
        }

        foreach (ManualTimer timer in due)
        {
            timer.Fire();
        }
    }

    private void Schedule(ManualTimer timer, TimeSpan dueTime, TimeSpan period)
    {
        lock (_lock)
        {
            timer.Period = period == Timeout.InfiniteTimeSpan ? TimeSpan.Zero : period;
            timer.DueAt = dueTime == Timeout.InfiniteTimeSpan ? null : _now + dueTime;
            if (!_timers.Contains(timer))
            {
                _timers.Add(timer);
            }
        }
    }

    private void Remove(ManualTimer timer)
    {
        lock (_lock)
        {
            timer.DueAt = null;
            _timers.Remove(timer);
        }
    }

    private sealed class ManualTimer(ManualTimeProvider owner, TimerCallback callback, object? state) : ITimer
    {
        public DateTimeOffset? DueAt { get; set; }

        public TimeSpan Period { get; set; }

        public void Fire() => callback(state);

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            owner.Schedule(this, dueTime, period);
            return true;
        }

        public void Dispose() => owner.Remove(this);

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
