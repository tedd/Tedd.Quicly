using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Testing.Simulation;

/// <summary>
/// A deterministic in-memory network. Links created from it carry datagrams and streams between
/// <see cref="SimulatedTransport"/> pairs with configurable delay, jitter, loss, reordering, bandwidth, MTU changes and
/// disconnects, all driven by a <see cref="VirtualClock"/> and one seeded random generator: the same seed and the
/// same sequence of calls produce the same sequence of callbacks.
/// </summary>
/// <remarks>
/// <para>
/// Nothing happens on its own. Every <see cref="ITransportSink"/> callback (and every listener callback) is raised from
/// inside <see cref="Advance"/>, <see cref="AdvanceTo"/> or <see cref="RunUntilIdle"/>, on the calling thread, which
/// plays the role of the transport thread. Unlike MsQuic, a callback never runs inline inside an API call: even a
/// completion that is due "now" is delivered by the next <c>Advance</c> (for example <c>Advance(0)</c>).
/// </para>
/// <para>
/// Events are ordered by due time and, among equal due times, by the order in which they were scheduled. Callbacks may
/// call transport APIs; events those calls schedule at or before the advance target are delivered in the same
/// <c>Advance</c>. The network, its transports and listeners share one lock, so API calls from other threads are safe
/// but serialised with event delivery.
/// </para>
/// </remarks>
public sealed class SimulatedNetwork : IDisposable
{
    internal readonly Lock Gate = new();
    internal readonly SimBufferPool Pool = new();
    private readonly VirtualClock _clock;
    private readonly EventQueue _queue = new();
    private readonly Dictionary<EndPoint, SimulatedListener> _listeners = new();
    private DeterministicRandom _random;
    private int _nextHost;
    private bool _disposed;

    /// <summary>Creates a network driven by <paramref name="clock"/> whose random draws follow <paramref name="seed"/>.</summary>
    /// <param name="clock">The clock the network advances. Share it with the code under test.</param>
    /// <param name="seed">Seed of the random generator (loss, jitter, reordering).</param>
    public SimulatedNetwork(VirtualClock clock, int seed)
    {
        ArgumentNullException.ThrowIfNull(clock);
        _clock = clock;
        Seed = seed;
        _random = new DeterministicRandom(seed);
    }

    /// <summary>The clock this network advances.</summary>
    public VirtualClock Clock => _clock;

    /// <summary>The seed given at construction.</summary>
    public int Seed { get; }

    /// <summary>Current virtual time in microseconds.</summary>
    public long NowMicros => _clock.NowMicros;

    /// <summary>Number of scheduled events not yet delivered.</summary>
    public int PendingEvents
    {
        get
        {
            lock (Gate)
                return _queue.Count;
        }
    }

    internal ref DeterministicRandom Random => ref _random;

    internal bool IsDisposed => _disposed;

    /// <summary>Internal consistency checks that failed (always 0 unless the simulator has a bug); tests assert it.</summary>
    internal int InvariantViolations;

    /// <summary>Counts a failed internal consistency check (a real check in every build, unlike <c>Debug.Assert</c>).</summary>
    internal bool Invariant(bool condition)
    {
        if (!condition)
            InvariantViolations++;
        return condition;
    }

    /// <summary>
    /// Creates a connected pair over a new link. Both ends start in <see cref="TransportState.Connecting"/>; the next
    /// <c>Advance</c> raises <see cref="ITransportSink.OnDatagramCapabilityChanged"/> on A then B, and after
    /// <see cref="LinkOptions.ConnectDelayMicros"/> (default one round trip) <see cref="ITransportSink.OnConnected"/> on A then B.
    /// Both capability reports come before either connect, also when the connect delay is zero.
    /// A is the client (client-initiated QUIC stream ids).
    /// </summary>
    /// <param name="sinkA">Receives the callbacks of end A.</param>
    /// <param name="sinkB">Receives the callbacks of end B.</param>
    /// <param name="options">Link conditions; <c>null</c> for an ideal link (no delay, no loss, unlimited bandwidth).</param>
    /// <returns>The two ends.</returns>
    public (SimulatedTransport A, SimulatedTransport B) CreatePair(ITransportSink sinkA, ITransportSink sinkB, LinkOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(sinkA);
        ArgumentNullException.ThrowIfNull(sinkB);
        LinkOptions copy = (options ?? new LinkOptions()).Clone();
        copy.Validate();
        lock (Gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            SimulatedLink link = new(this, copy, SimulatedConnector.DefaultAlpn);
            SimulatedTransport a = new(this, link, isClient: true, sinkA, AllocateEndPoint());
            SimulatedTransport b = new(this, link, isClient: false, sinkB, AllocateEndPoint());
            Attach(link, a, b);
            long now = NowMicros;
            long connectAt = now + (copy.ConnectDelayMicros ?? 2 * copy.DelayMicros);
            // Both capability reports come before both connects, even when the connect delay is zero.
            a.ScheduleCapability(now);
            b.ScheduleCapability(now);
            a.ScheduleConnected(connectAt);
            b.ScheduleConnected(connectAt);
            link.ScheduleTimeline();
            return (a, b);
        }
    }

    /// <summary>Moves time forward by <paramref name="micros"/>, delivering every event due up to the new time.</summary>
    /// <param name="micros">Non-negative amount of virtual time to advance.</param>
    /// <returns>Number of events delivered.</returns>
    public int Advance(long micros)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(micros);
        lock (Gate)
            return AdvanceTo(NowMicros + micros);
    }

    /// <summary>Moves time forward to <paramref name="targetMicros"/>, delivering every event due up to it in order.</summary>
    /// <param name="targetMicros">Absolute virtual time; must not be earlier than <see cref="NowMicros"/>.</param>
    /// <returns>Number of events delivered.</returns>
    public int AdvanceTo(long targetMicros)
    {
        lock (Gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ArgumentOutOfRangeException.ThrowIfLessThan(targetMicros, NowMicros);
            int delivered = 0;
            while (_queue.Count > 0 && _queue.PeekDue <= targetMicros)
            {
                DispatchNext();
                delivered++;
            }
            if (targetMicros > NowMicros)
                _clock.Set(targetMicros);
            return delivered;
        }
    }

    /// <summary>
    /// Delivers events until none is left or the next one is due more than <paramref name="maxMicros"/> from now.
    /// When the queue drains the clock stays at the last delivered event; otherwise it moves to the limit.
    /// </summary>
    /// <param name="maxMicros">Non-negative bound on how far virtual time may move.</param>
    /// <returns><c>true</c> when no event is pending any more.</returns>
    public bool RunUntilIdle(long maxMicros)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxMicros);
        lock (Gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            long deadline = NowMicros + maxMicros;
            while (_queue.Count > 0 && _queue.PeekDue <= deadline)
                DispatchNext();
            if (_queue.Count == 0)
                return true;
            _clock.Set(deadline);
            return false;
        }
    }

    /// <summary>Discards every pending event and unregisters all listeners. Transports stop delivering callbacks.</summary>
    public void Dispose()
    {
        lock (Gate)
        {
            _disposed = true;
            _queue.Clear();
            _listeners.Clear();
        }
    }

    private void DispatchNext()
    {
        SimEvent e = _queue.Pop();
        if (e.Due > NowMicros)
            _clock.Set(e.Due);
        if (e.Target is not null)
            e.Target.Dispatch(ref e);
        else if (e.Obj is SimulatedLink link)
            link.Dispatch(ref e);
        else
            ((SimulatedConnector.Attempt)e.Obj!).Run(this);
    }

    internal void Schedule(ref SimEvent e)
    {
        if (!_disposed)
            _queue.Push(ref e);
    }

    internal static void Attach(SimulatedLink link, SimulatedTransport a, SimulatedTransport b)
    {
        link.A = a;
        link.B = b;
        a.Peer = b;
        b.Peer = a;
        a.RemoteEndPoint = b.LocalEndPoint;
        b.RemoteEndPoint = a.LocalEndPoint;
    }

    internal IPEndPoint AllocateEndPoint()
    {
        int host = ++_nextHost;
        return new IPEndPoint(new IPAddress(new byte[] { 10, (byte)(host >> 16), (byte)(host >> 8), (byte)host }), 49152 + (host % 16384));
    }

    internal void RegisterListener(SimulatedListener listener)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_listeners.TryAdd(listener.LocalEndPoint, listener))
            throw new InvalidOperationException($"A listener is already started on {listener.LocalEndPoint}.");
    }

    internal void UnregisterListener(SimulatedListener listener)
    {
        if (_listeners.TryGetValue(listener.LocalEndPoint, out SimulatedListener? existing) && ReferenceEquals(existing, listener))
            _listeners.Remove(listener.LocalEndPoint);
    }

    internal SimulatedListener? FindListener(EndPoint endpoint) => _listeners.GetValueOrDefault(endpoint);
}
