namespace Tedd.Quicly.Replication;

/// <summary>
/// Fixed-step simulation clock: accumulates elapsed real time and tells the game how many fixed ticks to simulate,
/// with a catch-up cap (no "spiral of death") and the interpolation alpha for rendering between ticks.
/// </summary>
/// <remarks>
/// <para>
/// Time is accumulated exactly in integer units of microseconds × Hz, so a 60 Hz clock produces exactly 60 ticks per
/// 1 000 000 µs with no drift, whatever the frame lengths. When more than <see cref="MaxCatchUpTicks"/> ticks are due
/// in one <see cref="Advance"/>, only that many are returned and the rest are dropped (counted in
/// <see cref="DroppedTicks"/>); the fractional remainder is kept.
/// </para>
/// <para>Usage: <c>int n = clock.AdvanceTo(now); for (…n…) Simulate(); Render(alpha: clock.InterpolationAlpha);</c>. Not thread-safe.</para>
/// </remarks>
public sealed class FixedTickClock
{
    private const long MicrosPerSecond = 1_000_000;

    // Clamp for a single elapsed value so elapsed × Hz cannot overflow (11.5 days at 1 MHz still fits easily).
    private const long MaxElapsedMicros = 1_000_000_000_000;

    private readonly long _hz;
    private readonly int _maxCatchUp;
    private long _accumulator;
    private long _last;
    private bool _hasLast;

    /// <summary>Creates a clock.</summary>
    /// <param name="tickRateHz">Ticks per second, 1..1 000 000.</param>
    /// <param name="maxCatchUpTicks">Most ticks one <see cref="Advance"/> may return (at least 1).</param>
    /// <exception cref="ArgumentOutOfRangeException">An argument is out of range.</exception>
    public FixedTickClock(int tickRateHz, int maxCatchUpTicks = 8)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(tickRateHz, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(tickRateHz, 1_000_000);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCatchUpTicks, 1);
        _hz = tickRateHz;
        _maxCatchUp = maxCatchUpTicks;
    }

    /// <summary>Ticks per second.</summary>
    public int TickRateHz => (int)_hz;

    /// <summary>Most ticks one <see cref="Advance"/> returns.</summary>
    public int MaxCatchUpTicks => _maxCatchUp;

    /// <summary>Length of one tick in microseconds.</summary>
    public double TickDurationMicros => (double)MicrosPerSecond / _hz;

    /// <summary>Total ticks returned so far: after <see cref="Advance"/> returned <c>n</c>, the ticks to simulate are <c>Tick − n + 1 … Tick</c> (1-based).</summary>
    public long Tick { get; private set; }

    /// <summary>Ticks that were due but dropped by the catch-up cap.</summary>
    public long DroppedTicks { get; private set; }

    /// <summary>Fraction of the next tick already elapsed, in [0, 1): the blend factor between the last two simulated states.</summary>
    public float InterpolationAlpha => (float)((double)_accumulator / MicrosPerSecond);

    /// <summary>Adds elapsed time and returns the number of ticks to simulate now.</summary>
    /// <param name="elapsedMicros">Elapsed real time; negative values count as 0.</param>
    /// <returns>Ticks to simulate, 0..<see cref="MaxCatchUpTicks"/>.</returns>
    public int Advance(long elapsedMicros)
    {
        long elapsed = Math.Clamp(elapsedMicros, 0, MaxElapsedMicros);
        _accumulator += elapsed * _hz;
        long ticks = _accumulator / MicrosPerSecond;
        _accumulator -= ticks * MicrosPerSecond;
        if (ticks > _maxCatchUp)
        {
            DroppedTicks += ticks - _maxCatchUp;
            ticks = _maxCatchUp;
        }

        Tick += ticks;
        return (int)ticks;
    }

    /// <summary>Advances to the absolute monotonic time <paramref name="nowMicros"/>; the first call only sets the reference and returns 0.</summary>
    /// <param name="nowMicros">Current time (e.g. <c>IClock.NowMicros</c>).</param>
    /// <returns>Ticks to simulate.</returns>
    public int AdvanceTo(long nowMicros)
    {
        if (!_hasLast)
        {
            _last = nowMicros;
            _hasLast = true;
            return 0;
        }

        long elapsed = nowMicros - _last;
        _last = nowMicros;
        return Advance(elapsed);
    }

    /// <summary>Resets tick count, accumulator, dropped-tick counter and the <see cref="AdvanceTo"/> reference.</summary>
    public void Reset()
    {
        _accumulator = 0;
        _hasLast = false;
        _last = 0;
        Tick = 0;
        DroppedTicks = 0;
    }
}
