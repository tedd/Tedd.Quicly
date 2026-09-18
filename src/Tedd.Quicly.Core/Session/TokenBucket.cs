namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Integer token bucket driven by caller-supplied clock micros (no timers, no floating point). Used for the control
/// message rate (PROTOCOL.md §7), the Pong rate (§2.3), the decode budget (§7) and the send cap
/// (<see cref="PeerOptions.MaxSendBytesPerSecond"/>, §4.5, one token per byte). One owning thread.
/// </summary>
/// <remarks>
/// <see cref="TryTake"/> never overdraws. The send cap uses <see cref="Available"/> and <see cref="Consume"/> instead: a
/// scheduler pass sends while the balance is positive, its last datagram may overdraw, and later refills repay the debt,
/// so the long-run rate is the configured one and no datagram larger than the balance is held back forever.
/// </remarks>
internal struct TokenBucket
{
    private const long Scale = 1_000_000; // one token = 1e6 units; refill = rate units per elapsed microsecond
    private const long MaxElapsedMicros = 3_600_000_000L;

    /// <summary>
    /// The largest rate the bucket's integer refill (elapsed micros × rate, the gap capped at an hour) represents without
    /// overflowing: about 2.5 GB/s. A caller deriving a rate from an estimate clamps it here.
    /// </summary>
    public const long MaxRatePerSecond = long.MaxValue / MaxElapsedMicros;

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

    /// <summary>
    /// Changes the rate and burst of a running bucket without refilling it: the level earned so far is carried over (a
    /// <see cref="Consume"/> debt included) and clamped to the new burst. A rate derived from a moving estimate — the
    /// ReliableLatest retry budget follows the congestion window — would otherwise hand out a full bucket again on every
    /// change, so the cap would never bind.
    /// </summary>
    /// <param name="ratePerSecond">Tokens added per second.</param>
    /// <param name="burst">Bucket size in tokens.</param>
    /// <param name="nowMicros">Current clock micros.</param>
    public void SetRate(long ratePerSecond, long burst, long nowMicros)
    {
        if (_rate == 0 && _capacity == 0)
        {
            Initialize(ratePerSecond, burst, nowMicros);
            return;
        }

        Refill(nowMicros);
        _rate = ratePerSecond;
        _capacity = burst * Scale;
        _last = nowMicros;
        if (_units > _capacity)
        {
            _units = _capacity;
        }
    }

    /// <summary>Takes <paramref name="count"/> tokens if available.</summary>
    /// <param name="nowMicros">Current clock micros.</param>
    /// <param name="count">Tokens to take.</param>
    /// <returns><see langword="false"/> (nothing taken) when the bucket holds fewer.</returns>
    public bool TryTake(long nowMicros, long count = 1)
    {
        Refill(nowMicros);
        long needed = count * Scale;
        if (_units < needed)
        {
            return false;
        }

        _units -= needed;
        return true;
    }

    /// <summary>Refills and returns the whole tokens available now; negative while a <see cref="Consume"/> debt is outstanding.</summary>
    /// <param name="nowMicros">Current clock micros.</param>
    /// <returns>The balance in whole tokens, rounded down.</returns>
    public long Available(long nowMicros)
    {
        Refill(nowMicros);
        return _units >= 0 ? _units / Scale : -((Scale - 1 - _units) / Scale);
    }

    /// <summary>Takes <paramref name="count"/> tokens unconditionally; the balance may go negative (a debt that later refills repay).</summary>
    /// <param name="count">Tokens to take.</param>
    public void Consume(long count) => _units -= count * Scale;

    /// <summary>Micros after the last refill until at least <paramref name="count"/> tokens are available.</summary>
    /// <param name="count">Tokens wanted.</param>
    /// <returns>0 when they are available now; <see cref="long.MaxValue"/> when the rate is 0.</returns>
    public readonly long MicrosUntil(long count)
    {
        long missing = (count * Scale) - _units;
        if (missing <= 0)
        {
            return 0;
        }

        return _rate <= 0 ? long.MaxValue : (missing + _rate - 1) / _rate;
    }

    private void Refill(long nowMicros)
    {
        long elapsed = nowMicros - _last;
        if (elapsed > 0)
        {
            _last = nowMicros;
            long added = Math.Min(elapsed, MaxElapsedMicros) * _rate;
            _units = _capacity - _units <= added ? _capacity : _units + added;
        }
    }
}
