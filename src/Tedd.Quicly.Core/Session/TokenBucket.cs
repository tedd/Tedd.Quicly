namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Integer token bucket driven by caller-supplied clock micros (no timers, no floating point). Used for the control
/// message rate (PROTOCOL.md §7), the Pong rate (§2.3) and the decode budget (§7). One owning thread.
/// </summary>
internal struct TokenBucket
{
    private const long Scale = 1_000_000; // one token = 1e6 units; refill = rate units per elapsed microsecond
    private const long MaxElapsedMicros = 3_600_000_000L;

    private long _units;
    private long _capacity;
    private long _rate;
    private long _last;

    /// <summary>Starts full.</summary>
    /// <param name="ratePerSecond">Tokens added per second.</param>
    /// <param name="burst">Bucket size in tokens.</param>
    /// <param name="nowMicros">Current clock micros.</param>
    public void Initialize(long ratePerSecond, long burst, long nowMicros)
    {
        _rate = ratePerSecond;
        _capacity = burst * Scale;
        _units = _capacity;
        _last = nowMicros;
    }

    /// <summary>Takes <paramref name="count"/> tokens if available.</summary>
    /// <param name="nowMicros">Current clock micros.</param>
    /// <param name="count">Tokens to take.</param>
    /// <returns><see langword="false"/> (nothing taken) when the bucket holds fewer.</returns>
    public bool TryTake(long nowMicros, long count = 1)
    {
        long elapsed = nowMicros - _last;
        if (elapsed > 0)
        {
            _last = nowMicros;
            long added = Math.Min(elapsed, MaxElapsedMicros) * _rate;
            _units = _capacity - _units <= added ? _capacity : _units + added;
        }

        long needed = count * Scale;
        if (_units < needed)
        {
            return false;
        }

        _units -= needed;
        return true;
    }
}
