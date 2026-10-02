using System.Diagnostics;

namespace Tedd.Quicly.Core.Tests.Threading;

/// <summary>
/// A wake-up for one thread that waits for a condition other threads make true (a ring that has an item for it, or room
/// for one). The waiter spins for a while, which is where the condition comes true when both threads have a core, and then
/// blocks; <see cref="Ring"/> wakes it only when it has blocked. Like <see cref="HandOff"/>, a wait for a thread that has no
/// core therefore costs a thread wake-up (and the woken thread is boosted past the busy threads of its own priority), not
/// the scheduler quantum that a yielding spin (<see cref="SpinWait.SpinOnce(int)"/> past its tenth round) loses to them at
/// every yield.
/// </summary>
/// <remarks>
/// The waiter calls <see cref="Wait"/> each time it finds the condition false, and <see cref="Satisfied"/> once it finds it
/// true. When the spinning is over, <see cref="Wait"/> announces that the waiter blocks and returns without blocking, so the
/// waiter looks at the condition once more; the next <see cref="Wait"/> blocks. A ringer stores, fences and then looks at
/// the announcement, and the waiter announces, fences and then looks at the condition, so one of the two sees the other
/// and a wake-up cannot be lost.
/// </remarks>
internal sealed class Doorbell : IDisposable
{
    /// <summary>How long a waiter spins before it blocks by default: far longer than a hand-off between two threads that both run.</summary>
    public static readonly TimeSpan DefaultSpin = TimeSpan.FromMicroseconds(50);

    private readonly AutoResetEvent _wake = new(false);
    private readonly long _spinTicks;
    private int _blocked;

    // The waiter's own.
    private bool _waiting;
    private bool _announced;
    private bool _blockedThisWait;
    private long _spinUntil;

    /// <summary>A doorbell whose waiter spins for <see cref="DefaultSpin"/> before it blocks.</summary>
    public Doorbell()
        : this(DefaultSpin)
    {
    }

    /// <summary>A doorbell whose waiter spins for <paramref name="spin"/> before it blocks (zero: it blocks at once).</summary>
    public Doorbell(TimeSpan spin) => _spinTicks = (long)(spin.TotalSeconds * Stopwatch.Frequency);

    /// <summary>Waits that found the condition false at least once (the waiter's count; read it once the waiter is done).</summary>
    public long Waits { get; private set; }

    /// <summary>Waits that blocked at least once; the others ended while the waiter spun, with the other thread running.</summary>
    public long BlockedWaits { get; private set; }

    /// <summary>Waits that ended while the waiter spun: the condition came true while both threads were running.</summary>
    public long SpunWaits => Waits - BlockedWaits;

    /// <summary>Wakes the waiter if it has blocked; after the store that makes its condition true.</summary>
    public void Ring()
    {
        // The fence orders the condition's store before the look at the announcement; see the remarks.
        Interlocked.MemoryBarrier();
        if (Volatile.Read(ref _blocked) != 0)
        {
            _wake.Set();
        }
    }

    /// <summary>Wakes each of <paramref name="bells"/> whose waiter has blocked, behind one fence.</summary>
    public static void RingAll(Doorbell[] bells)
    {
        Interlocked.MemoryBarrier();
        foreach (Doorbell bell in bells)
        {
            if (Volatile.Read(ref bell._blocked) != 0)
            {
                bell._wake.Set();
            }
        }
    }

    /// <summary>
    /// One step of a wait, for the waiter that has just found its condition false: spins once while the wait is young, then
    /// announces that it blocks (the waiter looks at the condition once more), then blocks until it is rung.
    /// </summary>
    /// <exception cref="TimeoutException">Nobody rang within <see cref="HandOff.Timeout"/>.</exception>
    public void Wait()
    {
        if (!_waiting)
        {
            _waiting = true;
            _spinUntil = Stopwatch.GetTimestamp() + _spinTicks;
        }

        if (_announced)
        {
            bool woken = _wake.WaitOne(HandOff.Timeout);
            Volatile.Write(ref _blocked, 0);
            _announced = false;
            _blockedThisWait = true;
            if (!woken)
            {
                throw new TimeoutException($"Nobody rang within {HandOff.Timeout.TotalSeconds:F0} s.");
            }

            // Woken (or by a ring left over from a wait that the last look ended): look again, spinning first.
            _spinUntil = Stopwatch.GetTimestamp() + _spinTicks;
            return;
        }

        if (Stopwatch.GetTimestamp() < _spinUntil)
        {
            Thread.SpinWait(1);
            return;
        }

        Interlocked.Exchange(ref _blocked, 1);
        _announced = true;
    }

    /// <summary>Ends the wait, for the waiter that has found its condition true; nothing when it did not have to wait.</summary>
    public void Satisfied()
    {
        if (!_waiting)
        {
            return;
        }

        if (_announced)
        {
            Volatile.Write(ref _blocked, 0);
            _announced = false;
        }

        Waits++;
        if (_blockedThisWait)
        {
            BlockedWaits++;
        }

        _waiting = false;
        _blockedThisWait = false;
    }

    public void Dispose() => _wake.Dispose();
}
