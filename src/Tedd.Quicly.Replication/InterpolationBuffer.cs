namespace Tedd.Quicly.Replication;

/// <summary>Outcome of <see cref="InterpolationBuffer{T}.Sample"/>.</summary>
public enum InterpolationStatus
{
    /// <summary>No samples; <c>a</c>, <c>b</c> are <see langword="default"/> and <c>t</c> is 0.</summary>
    Empty = 0,

    /// <summary>The target time lies between two samples (inclusive): blend <c>a</c> → <c>b</c> by <c>t</c> in [0, 1].</summary>
    Interpolated = 1,

    /// <summary>
    /// The target lies before the oldest sample, or after the only sample: <c>a</c> = <c>b</c> = that sample, <c>t</c> = 0.
    /// </summary>
    Clamped = 2,

    /// <summary>The target lies after the newest sample, within the extrapolation cap: <c>t</c> &gt; 1 continues the last segment.</summary>
    Extrapolated = 3,

    /// <summary>
    /// The target lies further after the newest sample than the extrapolation cap (samples stopped arriving): <c>t</c> is
    /// held at the cap.
    /// </summary>
    ExtrapolationLimited = 4,
}

/// <summary>
/// Ring of timestamped samples of a remote entity's state, sampled at <c>render time − delay</c> for smooth
/// entity interpolation.
/// </summary>
/// <typeparam name="T">The sampled state (position, rotation, …); blending is the caller's job.</typeparam>
/// <remarks>
/// <para>
/// Samples are kept sorted by timestamp: out-of-order inserts are placed correctly, a duplicate timestamp replaces the
/// stored value, and when the ring is full the oldest sample is dropped (a sample older than everything in a full
/// ring is rejected). <see cref="Sample"/> never extrapolates further than <see cref="MaxExtrapolationMicros"/> past
/// the newest sample, so a gap in arrivals freezes the entity instead of letting it fly off.
/// </para>
/// <para>Timestamps are in microseconds on any monotonic time base (e.g. the server tick × tick duration). Allocation-free; not thread-safe.</para>
/// </remarks>
public sealed class InterpolationBuffer<T>
    where T : unmanaged
{
    /// <summary>Default number of samples kept.</summary>
    public const int DefaultCapacity = 32;

    /// <summary>Default interpolation delay (100 ms).</summary>
    public const long DefaultDelayMicros = 100_000;

    /// <summary>Default extrapolation cap (50 ms).</summary>
    public const long DefaultMaxExtrapolationMicros = 50_000;

    private readonly long[] _times;
    private readonly T[] _values;
    private int _oldest;
    private int _count;
    private long _delay;
    private long _maxExtrapolation;

    /// <summary>Creates a buffer.</summary>
    /// <param name="capacity">Samples kept (at least 1).</param>
    /// <param name="delayMicros">Interpolation delay subtracted from the render time (non-negative).</param>
    /// <param name="maxExtrapolationMicros">How far past the newest sample <see cref="Sample"/> may extrapolate (non-negative; 0 = never).</param>
    /// <exception cref="ArgumentOutOfRangeException">An argument is out of range.</exception>
    public InterpolationBuffer(int capacity = DefaultCapacity, long delayMicros = DefaultDelayMicros, long maxExtrapolationMicros = DefaultMaxExtrapolationMicros)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        _times = new long[capacity];
        _values = new T[capacity];
        DelayMicros = delayMicros;
        MaxExtrapolationMicros = maxExtrapolationMicros;
    }

    /// <summary>Samples kept at most.</summary>
    public int Capacity => _times.Length;

    /// <summary>Samples currently stored.</summary>
    public int Count => _count;

    /// <summary>Interpolation delay in microseconds.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
    public long DelayMicros
    {
        get => _delay;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _delay = value;
        }
    }

    /// <summary>Extrapolation cap in microseconds.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
    public long MaxExtrapolationMicros
    {
        get => _maxExtrapolation;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _maxExtrapolation = value;
        }
    }

    /// <summary>Inserts a sample in timestamp order.</summary>
    /// <param name="timestampMicros">The sample's timestamp.</param>
    /// <param name="value">The sample.</param>
    /// <returns><see langword="false"/> when the ring is full and the sample is older than every stored one.</returns>
    public bool Add(long timestampMicros, in T value)
    {
        if (_count == 0 || timestampMicros > _times[Slot(_count - 1)])
        {
            if (_count == _times.Length)
            {
                _oldest = Slot(1);
                _count--;
            }

            int slot = Slot(_count);
            _times[slot] = timestampMicros;
            _values[slot] = value;
            _count++;
            return true;
        }

        // Out of order or duplicate: find the last sample not newer than the timestamp.
        int i = _count - 1;
        while (i >= 0 && _times[Slot(i)] > timestampMicros)
        {
            i--;
        }

        if (i >= 0 && _times[Slot(i)] == timestampMicros)
        {
            _values[Slot(i)] = value;
            return true;
        }

        if (_count == _times.Length)
        {
            if (i < 0)
            {
                return false;
            }

            _oldest = Slot(1);
            _count--;
            i--;
        }

        for (int k = _count; k > i + 1; k--)
        {
            int to = Slot(k);
            int from = Slot(k - 1);
            _times[to] = _times[from];
            _values[to] = _values[from];
        }

        int insert = Slot(i + 1);
        _times[insert] = timestampMicros;
        _values[insert] = value;
        _count++;
        return true;
    }

    /// <summary>Returns the pair of samples around <c><paramref name="renderTimeMicros"/> − <see cref="DelayMicros"/></c> and the blend factor.</summary>
    /// <param name="renderTimeMicros">The current render time.</param>
    /// <param name="a">The earlier sample.</param>
    /// <param name="b">The later sample.</param>
    /// <param name="t">Blend factor: <c>value = a + (b − a) · t</c>; in [0, 1] when interpolating, above 1 when extrapolating.</param>
    /// <returns>What kind of result was produced (see <see cref="InterpolationStatus"/>).</returns>
    public InterpolationStatus Sample(long renderTimeMicros, out T a, out T b, out float t)
    {
        if (_count == 0)
        {
            a = default;
            b = default;
            t = 0f;
            return InterpolationStatus.Empty;
        }

        long target = renderTimeMicros - _delay;
        int newest = _count - 1;
        long newestTime = _times[Slot(newest)];
        long oldestTime = _times[_oldest];
        if (target < oldestTime || (target > newestTime && _count == 1))
        {
            a = _values[target < oldestTime ? _oldest : Slot(newest)];
            b = a;
            t = 0f;
            return InterpolationStatus.Clamped;
        }

        if (target > newestTime)
        {
            int previous = Slot(newest - 1);
            long previousTime = _times[previous];
            long ahead = target - newestTime;
            InterpolationStatus status = InterpolationStatus.Extrapolated;
            if (ahead > _maxExtrapolation)
            {
                ahead = _maxExtrapolation;
                status = InterpolationStatus.ExtrapolationLimited;
            }

            a = _values[previous];
            b = _values[Slot(newest)];
            t = (float)((double)(newestTime - previousTime + ahead) / (newestTime - previousTime));
            return status;
        }

        // oldestTime <= target <= newestTime.
        int i = newest;
        while (_times[Slot(i)] > target)
        {
            i--;
        }

        if (i == newest)
        {
            // Exactly on the newest sample: the end of the last segment (continuous with extrapolation).
            a = _values[Slot(_count == 1 ? newest : newest - 1)];
            b = _values[Slot(newest)];
            t = _count == 1 ? 0f : 1f;
            return InterpolationStatus.Interpolated;
        }

        int lower = Slot(i);
        int upper = Slot(i + 1);
        a = _values[lower];
        b = _values[upper];
        t = (float)((double)(target - _times[lower]) / (_times[upper] - _times[lower]));
        return InterpolationStatus.Interpolated;
    }

    /// <summary>Returns the newest sample.</summary>
    /// <param name="timestampMicros">Its timestamp, or 0 when empty.</param>
    /// <param name="value">Its value, or <see langword="default"/>.</param>
    /// <returns><see langword="false"/> when empty.</returns>
    public bool TryGetNewest(out long timestampMicros, out T value)
    {
        if (_count == 0)
        {
            timestampMicros = 0;
            value = default;
            return false;
        }

        int slot = Slot(_count - 1);
        timestampMicros = _times[slot];
        value = _values[slot];
        return true;
    }

    /// <summary>Removes every sample.</summary>
    public void Clear()
    {
        _oldest = 0;
        _count = 0;
    }

    private int Slot(int logical)
    {
        int slot = _oldest + logical;
        return slot >= _times.Length ? slot - _times.Length : slot;
    }
}
