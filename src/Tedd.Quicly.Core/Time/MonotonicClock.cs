using System.Diagnostics;

namespace Tedd.Quicly.Core.Time;

/// <summary>
/// <see cref="IClock"/> backed by <see cref="Stopwatch"/>: microseconds since the clock was first used, which
/// happens during process start-up in practice. Thread-safe, allocation-free, never goes backwards.
/// </summary>
public sealed class MonotonicClock : IClock
{
    private static readonly long s_frequency = Stopwatch.Frequency;
    private static readonly long s_origin = Stopwatch.GetTimestamp();

    /// <summary>The process-wide instance.</summary>
    public static MonotonicClock Instance { get; } = new();

    private MonotonicClock()
    {
    }

    /// <inheritdoc/>
    public long NowMicros => ToMicros(Stopwatch.GetTimestamp() - s_origin);

    /// <summary>Converts a <see cref="Stopwatch"/> tick delta to microseconds without overflow or floating point.</summary>
    /// <param name="ticks">Number of <see cref="Stopwatch"/> ticks (non-negative).</param>
    internal static long ToMicros(long ticks)
    {
        long frequency = s_frequency;
        long seconds = ticks / frequency;
        long remainder = ticks - seconds * frequency;
        return seconds * 1_000_000 + remainder * 1_000_000 / frequency;
    }
}
