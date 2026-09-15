namespace Tedd.Quicly.Core.Time;

/// <summary>Source of monotonic time in microseconds. Never goes backwards; unrelated to wall-clock time.</summary>
public interface IClock
{
    /// <summary>Current time in microseconds since an arbitrary fixed origin (for <see cref="MonotonicClock"/>, process start).</summary>
    long NowMicros { get; }
}
