namespace Tedd.Quicly.Acme.Tests.Fake;

/// <summary>
/// A <see cref="TimeProvider"/> whose timers fire immediately and advance a simulated clock by the requested due time,
/// so polling / renewal loops run instantly while every requested delay is recorded.
/// </summary>
public sealed class FastTimeProvider : TimeProvider
{
    private readonly object _lock = new();
    private DateTimeOffset _now;

    public FastTimeProvider(DateTimeOffset? start = null)
    {
        _now = start ?? new DateTimeOffset(2026, 9, 15, 12, 0, 0, TimeSpan.Zero);
    }

    public List<TimeSpan> Delays { get; } = [];

    public DateTimeOffset Now
    {
        get { lock (_lock) { return _now; } }
        set { lock (_lock) { _now = value; } }
    }

    public override DateTimeOffset GetUtcNow() => Now;

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        lock (_lock)
        {
            Delays.Add(dueTime);
            if (dueTime > TimeSpan.Zero)
            {
                _now += dueTime;
            }
        }

        callback(state);
        return new NoopTimer();
    }

    private sealed class NoopTimer : ITimer
    {
        public bool Change(TimeSpan dueTime, TimeSpan period) => true;

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
