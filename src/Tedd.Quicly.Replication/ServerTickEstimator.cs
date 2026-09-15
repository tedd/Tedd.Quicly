namespace Tedd.Quicly.Replication;

/// <summary>
/// Client-side estimate of the server's current (fractional) tick, from QUICLY's advisory clock offset and the
/// server's tick rate, corrected by slewing rather than stepping.
/// </summary>
/// <remarks>
/// <para>
/// Model: <c>serverMicros = localMicros + offset</c> (<see cref="SetClockOffset"/>, e.g.
/// <c>peer.EstimatedRemoteMicros() − clock.NowMicros</c>, PROTOCOL.md §4.6) and the server's tick <c>k</c> begins at
/// <c>serverMicros = origin + k · 10^6 / rate</c> (<see cref="ObserveServerTick"/>, from a snapshot that carries its
/// tick and the server's clock at that tick, in the same clock domain the offset maps to). The estimate is
/// <c>(localMicros + correction) · rate / 10^6</c> where <c>correction</c> tracks <c>offset − origin</c>.
/// </para>
/// <para>
/// <b>Slewing:</b> the first <see cref="Update"/> after the first observation locks onto the target directly. Later
/// changes of the target are applied gradually: at most <see cref="MaxSlewRate"/> × elapsed local time per update
/// (default 5 %, i.e. the estimated server clock runs between 0.95× and 1.05× real time while converging), so
/// while slewing, with non-decreasing local time, the estimate never jumps and never runs backwards. An error
/// larger than <see cref="StepThresholdMicros"/> (a resume, a server hitch) is corrected in one step instead
/// (counted in <see cref="StepCount"/>); a step can move the estimate backwards, and so can an <see cref="Update"/>
/// whose local time is earlier than the previous one.
/// </para>
/// <para>The offset is peer-supplied and advisory; the result is for scheduling and interpolation only. Not thread-safe.</para>
/// </remarks>
public sealed class ServerTickEstimator
{
    private const double MicrosPerSecond = 1_000_000.0;

    private readonly double _hz;
    private long _offsetTarget;
    private double _originTarget;
    private bool _hasObservation;
    private bool _locked;
    private double _correction;
    private long _lastLocal;

    /// <summary>Creates an estimator.</summary>
    /// <param name="serverTickRateHz">The server's ticks per second (at least 1).</param>
    /// <param name="maxSlewRate">Largest correction per microsecond of elapsed local time, in (0, 1).</param>
    /// <param name="stepThresholdMicros">Errors above this are stepped, not slewed (non-negative).</param>
    /// <exception cref="ArgumentOutOfRangeException">An argument is out of range.</exception>
    public ServerTickEstimator(int serverTickRateHz, double maxSlewRate = 0.05, long stepThresholdMicros = 250_000)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(serverTickRateHz, 1);
        if (!(maxSlewRate > 0 && maxSlewRate < 1))
        {
            throw new ArgumentOutOfRangeException(nameof(maxSlewRate), maxSlewRate, "Slew rate must be in (0, 1).");
        }

        ArgumentOutOfRangeException.ThrowIfNegative(stepThresholdMicros);
        _hz = serverTickRateHz;
        TickRateHz = serverTickRateHz;
        MaxSlewRate = maxSlewRate;
        StepThresholdMicros = stepThresholdMicros;
    }

    /// <summary>The server's ticks per second.</summary>
    public int TickRateHz { get; }

    /// <summary>Largest correction per unit of elapsed local time.</summary>
    public double MaxSlewRate { get; }

    /// <summary>Errors above this many microseconds are stepped.</summary>
    public long StepThresholdMicros { get; }

    /// <summary><see langword="true"/> once a server tick has been observed and <see cref="Update"/> has run.</summary>
    public bool IsLocked => _locked;

    /// <summary>The estimate computed by the last <see cref="Update"/> (0 before the lock).</summary>
    public double EstimatedTick { get; private set; }

    /// <summary><see cref="EstimatedTick"/> rounded down.</summary>
    public long CurrentTick => (long)Math.Floor(EstimatedTick);

    /// <summary>The applied (slewed) correction in microseconds: <c>estimate = (local + correction) · rate / 10^6</c>.</summary>
    public double CorrectionMicros => _correction;

    /// <summary>The correction the estimator is converging to.</summary>
    public double TargetCorrectionMicros => _offsetTarget - _originTarget;

    /// <summary>Number of corrections applied as steps (initial lock excluded).</summary>
    public long StepCount { get; private set; }

    /// <summary>Sets the clock offset: <c>serverMicros = localMicros + offsetMicros</c>.</summary>
    /// <param name="offsetMicros">The offset (QUICLY's advisory estimate).</param>
    public void SetClockOffset(long offsetMicros) => _offsetTarget = offsetMicros;

    /// <summary>Records that server tick <paramref name="serverTick"/> began at server time <paramref name="serverMicros"/>.</summary>
    /// <param name="serverTick">The tick.</param>
    /// <param name="serverMicros">The server's clock at that tick.</param>
    public void ObserveServerTick(long serverTick, long serverMicros)
    {
        _originTarget = serverMicros - serverTick * MicrosPerSecond / _hz;
        _hasObservation = true;
    }

    /// <summary>Makes the next <see cref="Update"/> jump straight to the target (for example after a reconnect).</summary>
    public void Resync() => _locked = false;

    /// <summary>Advances the slewing to <paramref name="localNowMicros"/> and returns the estimated server tick.</summary>
    /// <param name="localNowMicros">Local monotonic time.</param>
    /// <returns>The fractional server tick, or 0 before the first observation.</returns>
    public double Update(long localNowMicros)
    {
        if (!_hasObservation)
        {
            _lastLocal = localNowMicros;
            return 0;
        }

        double target = TargetCorrectionMicros;
        if (!_locked)
        {
            _correction = target;
            _locked = true;
        }
        else
        {
            double error = target - _correction;
            if (Math.Abs(error) > StepThresholdMicros)
            {
                _correction = target;
                StepCount++;
            }
            else
            {
                double maxStep = Math.Max(0, localNowMicros - _lastLocal) * MaxSlewRate;
                _correction += Math.Clamp(error, -maxStep, maxStep);
            }
        }

        _lastLocal = localNowMicros;
        EstimatedTick = (localNowMicros + _correction) * _hz / MicrosPerSecond;
        return EstimatedTick;
    }
}
