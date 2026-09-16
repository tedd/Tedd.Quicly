using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.Session;

/// <summary>One Ping/Pong exchange, all four timestamps in 32-bit connection-relative micros (PROTOCOL.md §2.3).</summary>
internal struct PongSample
{
    /// <summary>t1: our Ping send time, echoed.</summary>
    public uint Echo;

    /// <summary>t2: the responder's receive time.</summary>
    public uint RemoteReceive;

    /// <summary>t3: the responder's send time.</summary>
    public uint RemoteSend;

    /// <summary>t4: our Pong receive time (stamped on the transport thread at arrival).</summary>
    public uint LocalReceive;
}

/// <summary>
/// Application RTT and clock-offset filter (PROTOCOL.md §4.6), game thread. RTT is smoothed as in RFC 6298; the
/// published offset is the sample with the smallest RTT among the last eight, stepped until the fast lock ends and
/// slewed afterwards (moved toward the target by at most 1/16 of the elapsed time), so the remote clock estimate never
/// jumps once locked. One-way jitter follows RFC 3550 from consecutive responder receive times.
/// </summary>
internal struct PingClock
{
    /// <summary>Samples in the offset window.</summary>
    public const int Window = 8;

    /// <summary>Slew limit: the published offset moves at most elapsed / 16.</summary>
    public const int SlewDivisor = 16;

    public long SmoothedRtt;
    public long RttVariance;
    public long MinRtt;
    public long MaxRtt;
    public long LatestRtt;
    public long Samples;
    public long Jitter;
    public long PublishedOffset;
    public long TargetOffset;
    public bool HasOffset;
    public bool Locked;

    private Window8 _rtt;
    private Window8 _offset;
    private int _filled;
    private int _next;
    private uint _lastEcho;
    private uint _lastRemoteReceive;
    private bool _hasLast;
    private long _lastAdvance;

    /// <summary>Folds a Pong into the filters.</summary>
    /// <param name="sample">The exchange.</param>
    public void AddSample(in PongSample sample)
    {
        // Wrap-safe differences: every timestamp is a truncated 32-bit clock, and the four are close together.
        long rtt = (long)unchecked((int)(sample.LocalReceive - sample.Echo)) - unchecked((int)(sample.RemoteSend - sample.RemoteReceive));
        if (rtt < 0)
        {
            rtt = 0;
        }

        long offset = ((long)unchecked((int)(sample.RemoteReceive - sample.Echo)) + unchecked((int)(sample.RemoteSend - sample.LocalReceive))) / 2;

        LatestRtt = rtt;
        if (Samples == 0)
        {
            SmoothedRtt = rtt;
            RttVariance = rtt / 2;
            MinRtt = rtt;
            MaxRtt = rtt;
        }
        else
        {
            RttVariance = (3 * RttVariance + Math.Abs(SmoothedRtt - rtt)) / 4;
            SmoothedRtt = (7 * SmoothedRtt + rtt) / 8;
            MinRtt = Math.Min(MinRtt, rtt);
            MaxRtt = Math.Max(MaxRtt, rtt);
        }

        Samples++;

        if (_hasLast)
        {
            long d = Math.Abs((long)unchecked((int)(sample.RemoteReceive - _lastRemoteReceive)) - unchecked((int)(sample.Echo - _lastEcho)));
            Jitter += (d - Jitter) / 16;
        }

        _hasLast = true;
        _lastEcho = sample.Echo;
        _lastRemoteReceive = sample.RemoteReceive;

        _rtt[_next] = rtt;
        _offset[_next] = offset;
        _next = (_next + 1) & (Window - 1);
        if (_filled < Window)
        {
            _filled++;
        }

        // The sample with the smallest RTT wins; on a tie the newest one (its offset is the most current, which matters
        // when the clocks drift), so walk the window from the oldest sample to the newest and accept equal values.
        int oldest = (_next - _filled + Window) & (Window - 1);
        int best = oldest;
        for (int i = 1; i < _filled; i++)
        {
            int index = (oldest + i) & (Window - 1);
            if (_rtt[index] <= _rtt[best])
            {
                best = index;
            }
        }

        TargetOffset = _offset[best];
        if (!Locked || !HasOffset)
        {
            PublishedOffset = TargetOffset;
            HasOffset = true;
        }
    }

    /// <summary>Advances slewing to <paramref name="nowMicros"/>.</summary>
    /// <param name="nowMicros">Current clock micros.</param>
    /// <param name="lockReached">The fast lock period is over (from then on the offset is slewed, never stepped).</param>
    public void Advance(long nowMicros, bool lockReached)
    {
        if (!Locked)
        {
            if (!lockReached || !HasOffset)
            {
                return;
            }

            Locked = true;
            _lastAdvance = nowMicros;
            return;
        }

        long elapsed = nowMicros - _lastAdvance;
        if (elapsed <= 0)
        {
            return;
        }

        _lastAdvance = nowMicros;
        long diff = TargetOffset - PublishedOffset;
        if (diff == 0)
        {
            return;
        }

        long step = Math.Max(1, elapsed / SlewDivisor);
        PublishedOffset = Math.Abs(diff) <= step ? TargetOffset : PublishedOffset + (diff > 0 ? step : -step);
    }

    [InlineArray(Window)]
    private struct Window8
    {
        private long _element0;
    }
}
