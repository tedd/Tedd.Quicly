namespace Tedd.Quicly.Core.Time;

/// <summary>
/// <see cref="IClock"/> that only moves when told to. Used by deterministic tests and the simulated transport.
/// Reads are thread-safe; writes are expected from one thread at a time.
/// </summary>
public sealed class VirtualClock : IClock
{
    private long _nowMicros;

    /// <summary>Creates a clock that starts at <paramref name="startMicros"/> (default 0).</summary>
    /// <param name="startMicros">Initial value of <see cref="NowMicros"/>; must be non-negative.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="startMicros"/> is negative.</exception>
    public VirtualClock(long startMicros = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(startMicros);
        _nowMicros = startMicros;
    }

    /// <inheritdoc/>
    public long NowMicros => Volatile.Read(ref _nowMicros);

    /// <summary>Moves the clock forward by <paramref name="delta"/>.</summary>
    /// <param name="delta">Amount to add; must be non-negative.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="delta"/> is negative.</exception>
    public void Advance(TimeSpan delta) => AdvanceMicros(delta.Ticks / (TimeSpan.TicksPerMillisecond / 1000));

    /// <summary>Moves the clock forward by <paramref name="micros"/> microseconds.</summary>
    /// <param name="micros">Amount to add; must be non-negative.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="micros"/> is negative.</exception>
    public void AdvanceMicros(long micros)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(micros);
        Volatile.Write(ref _nowMicros, _nowMicros + micros);
    }

    /// <summary>
    /// Sets the clock to an absolute value. Unlike <see cref="Advance"/> this may move the clock backwards; only
    /// tests that reset state between scenarios should rely on that.
    /// </summary>
    /// <param name="micros">New value of <see cref="NowMicros"/>; must be non-negative.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="micros"/> is negative.</exception>
    public void Set(long micros)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(micros);
        Volatile.Write(ref _nowMicros, micros);
    }
}
