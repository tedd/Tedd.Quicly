using System.Diagnostics;

namespace Tedd.Quicly.Core.Tests.Threading;

/// <summary>
/// A value handed from one thread to another (one giver and one taker at a time; zero means that nothing is waiting). The
/// taker spins for a while, which is where a hand-off lands when both threads have a core, and then blocks; the giver wakes
/// it only when it has blocked. On a machine with no core to spare, a hand-off to a thread that has none therefore costs a
/// thread wake-up (and a woken thread is boosted past the busy threads of its own priority), not the scheduler quantum that
/// a spinning or yielding taker loses to them.
/// </summary>
internal sealed class HandOff : IDisposable
{
    /// <summary>How long a taker blocks before it gives up: only a giver that died or hangs takes this long.</summary>
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    /// <summary>How long a taker spins before it blocks: far longer than a hand-off between two threads that both run.</summary>
    private static readonly long SpinTicks = Stopwatch.Frequency / 20_000;

    private readonly AutoResetEvent _wake = new(false);
    private long _value;
    private int _blocked;

    /// <summary>True while a value is waiting to be taken.</summary>
    public bool HasValue => Volatile.Read(ref _value) != 0;

    /// <summary>Hands <paramref name="value"/> (not zero) over, and wakes the taker if it has blocked.</summary>
    public void Put(long value)
    {
        Publish(value);
        WakeIfBlocked();
    }

    /// <summary>
    /// Hands <paramref name="value"/> (not zero) over with a plain store, which a spinning taker sees without the giver
    /// waiting for it: for a giver that races the taker right after the hand-off. <see cref="WakeIfBlocked"/> must follow.
    /// </summary>
    public void Publish(long value) => Volatile.Write(ref _value, value);

    /// <summary>Wakes the taker if it has blocked; after <see cref="Publish"/>.</summary>
    public void WakeIfBlocked()
    {
        // The fence orders the value before the look at the flag; the taker sets the flag before its last look at the
        // value, so one of the two sees the other and the wake-up cannot be lost.
        Interlocked.MemoryBarrier();
        if (Volatile.Read(ref _blocked) != 0)
        {
            _wake.Set();
        }
    }

    /// <summary>Takes the value, spinning for it for a while and then blocked.</summary>
    /// <exception cref="TimeoutException">Nothing was handed over within <see cref="Timeout"/>.</exception>
    public long Take()
    {
        long spinUntil = Stopwatch.GetTimestamp() + SpinTicks;
        while (true)
        {
            long value = Interlocked.Exchange(ref _value, 0);
            if (value != 0)
            {
                return value;
            }

            if (Stopwatch.GetTimestamp() < spinUntil)
            {
                Thread.SpinWait(1);
                continue;
            }

            Interlocked.Exchange(ref _blocked, 1);
            value = Interlocked.Exchange(ref _value, 0);
            bool woken = value != 0 || _wake.WaitOne(Timeout);
            Volatile.Write(ref _blocked, 0);
            if (value != 0)
            {
                return value;
            }

            if (!woken)
            {
                throw new TimeoutException($"Nothing was handed over within {Timeout.TotalSeconds:F0} s.");
            }

            // Woken (or by a wake-up left over from a hand-off that the last look above took): look again, spinning first.
            spinUntil = Stopwatch.GetTimestamp() + SpinTicks;
        }
    }

    public void Dispose() => _wake.Dispose();
}
