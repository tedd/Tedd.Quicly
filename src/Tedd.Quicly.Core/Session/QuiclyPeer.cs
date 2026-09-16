using System.Net;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// One QUICLY session over one transport connection (ARCHITECTURE.md §6): channels, delivery modes, the control protocol
/// (handshake, ping/clock sync, close) and the hand-off between the transport thread and the game thread.
/// </summary>
/// <remarks>
/// <para><b>Threads.</b> The <em>game thread</em> is whichever thread calls <c>Send*</c>, <see cref="Flush"/>,
/// <see cref="Poll"/>, <see cref="Drain"/>, <see cref="Close(Session.CloseReason)"/> and the other members (one thread at a
/// time; the peer is not thread-safe). Transport callbacks arrive on the transport's threads through
/// <see cref="TransportSink"/> and never touch game-thread state (ADR 0008). Handlers, <see cref="StateChanged"/> and
/// <see cref="IPeerAdmission.Admit"/> run on the game thread inside <see cref="Poll"/>; time-driven work (pings,
/// heartbeat, admission timeout, close linger, engine retries) runs inside <see cref="Poll"/> and <see cref="Flush"/>
/// with the clock read once per call.</para>
/// <para><b>Lifecycle.</b> <see cref="PeerState.Connecting"/> → <see cref="PeerState.Handshaking"/> (transport connected;
/// the client sends Hello) → <see cref="PeerState.Connected"/> (HelloAck accepted) → <see cref="PeerState.Closing"/> →
/// <see cref="PeerState.Closed"/>. Every change is reported by <see cref="StateChanged"/> from <see cref="Poll"/>; after
/// the Closed event no handler or event runs. <see cref="Dispose"/> closes the transport if needed; native memory is freed
/// once the transport reported its close and no <see cref="Poll"/> or <see cref="Flush"/> call is running.</para>
/// </remarks>
public sealed unsafe partial class QuiclyPeer : IDisposable
{
    internal const int SignalConnected = 1 << 0;
    internal const int SignalHello = 1 << 1;
    internal const int SignalHelloAck = 1 << 2;
    internal const int SignalPeerClose = 1 << 3;
    internal const int SignalTransportClosed = 1 << 4;
    internal const int SignalCloseRequest = 1 << 5;
    internal const int SignalTableRequest = 1 << 6;
    internal const int SignalTableInfo = 1 << 7;

    // Lifetime word: native memory is freed exactly once, when the transport can no longer call back (ClosedSeen), the
    // application disposed the peer (DisposeRequested) and no game-thread Poll/Flush is running (InCall clear). Whoever
    // completes that condition frees; the word is only changed with Interlocked operations.
    private const int LifetimeClosedSeen = 1;
    private const int LifetimeDisposeRequested = 2;
    private const int LifetimeInCall = 4;

    private readonly PeerCore _core;
    private readonly PeerRole _role;
    private readonly IPeerAdmission? _admission;
    private readonly byte[] _authToken;
    private readonly byte[] _resumeToken;
    private readonly Sink _sink;
    private readonly IClock _clock;
    private readonly long _admissionTimeoutMicros;
    private readonly long _heartbeatMicros;
    private readonly long _streamIdleMicros;
    private readonly long _pingIntervalMicros;
    private readonly long _fastPingIntervalMicros;
    private readonly long _fastLockMicros;
    private readonly long _closeLingerMicros;
    private readonly long _sessionGraceMicros;
    private readonly int _maxMessageSizeOption;
    private readonly ushort _maxReceiveDatagram;
    private readonly bool _requestTable;
    private readonly uint _lastEpoch;
    private readonly bool _failFast;
    private readonly bool _needsDatagrams;
    private ITransport? _transport;
    private IPEndPoint? _remoteEndPoint;
    private int _signals;
    private int _lifetime;
    private int _callDepth;
    private int _freed;
    private bool _disposed;
    private Exception? _lastFault;

    private QuiclyPeer(PeerRole role, ChannelTable table, PeerOptions options, IPeerAdmission? admission, ReadOnlySpan<byte> authToken)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        if (authToken.Length > ControlCodec.MaxTokenLength)
        {
            throw new ArgumentException($"An auth token is at most {ControlCodec.MaxTokenLength} bytes.", nameof(authToken));
        }

        _role = role;
        _admission = admission;
        _authToken = authToken.ToArray();
        _resumeToken = options.SessionToken.ToArray();
        _clock = options.Clock;
        _admissionTimeoutMicros = PeerOptions.ToMicros(options.AdmissionTimeout);
        _heartbeatMicros = PeerOptions.ToMicros(options.HeartbeatTimeout);
        _streamIdleMicros = PeerOptions.ToMicros(options.StreamIdleTimeout);
        _pingIntervalMicros = PeerOptions.ToMicros(options.PingInterval);
        _fastPingIntervalMicros = PeerOptions.ToMicros(options.FastPingInterval);
        _fastLockMicros = PeerOptions.ToMicros(options.FastLockDuration);
        _closeLingerMicros = PeerOptions.ToMicros(options.CloseLinger);
        _sessionGraceMicros = PeerOptions.ToMicros(options.SessionGrace);
        _maxMessageSizeOption = options.MaxMessageSize;
        _maxReceiveDatagram = options.MaxReceiveDatagram;
        _requestTable = options.RequestChannelTable;
        _lastEpoch = options.LastEpoch;
        _failFast = options.FailFastOnCallbackException;
        foreach (ChannelDefinition channel in table.All)
        {
            _needsDatagrams |= channel.IsDatagramMode;
        }

        _sink = new Sink(this);
        _core = new PeerCore(this, role, table, options);
        _handlers = new MessageHandler?[_core.ChannelCount];
        // Built here, never inside Poll: the drain queues are native memory sized once (ARCHITECTURE.md §3).
        _queues = new ReceiveQueues(ReceiveQueues.NodesFor(options.ReceiveRingCapacity), _core.ChannelCount);
        InitializeSendSide(options);
        long now = _clock.NowMicros;
        _controlBucket.Initialize(options.ControlMessagesPerSecond, options.ControlMessagesPerSecond, now);
        _pongBucket.Initialize(options.PongsPerSecond, options.PongBurst, now);
        _decodeBucket.Initialize(options.DecodedBytesPerSecond, options.DecodedBytesPerSecond, now);
        InitializeScheduler(options.MaxSendBytesPerSecond, now);
        _admissionDeadline = now + _admissionTimeoutMicros;
        _lastReceiveMicros = now;
        _timerDeadline = _admissionDeadline;
        _nextDeadlineMicros = _admissionDeadline;
        try
        {
            _core.InitializeEngines(options.EngineFactory);
        }
        catch
        {
            _core.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Connects to a server (client role). The peer passes itself (<see cref="TransportSink"/>) to
    /// <paramref name="connector"/>; when the transport connects, the next <see cref="Poll"/> opens the control stream and
    /// sends Hello. Callbacks may arrive before this method returns.
    /// </summary>
    /// <param name="connector">Creates the transport.</param>
    /// <param name="endpoint">The server.</param>
    /// <param name="serverName">TLS server name, or <see langword="null"/>.</param>
    /// <param name="table">The channel table (must hash equal to the server's).</param>
    /// <param name="options">Options (read once).</param>
    /// <param name="authToken">Application authentication token sent in Hello (at most 4 096 bytes; copied).</param>
    /// <returns>The peer, in <see cref="PeerState.Connecting"/>.</returns>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    /// <exception cref="ArgumentException">An option is out of range or the token is too long.</exception>
    public static QuiclyPeer Connect(ITransportConnector connector, EndPoint endpoint, string? serverName, ChannelTable table, PeerOptions options, ReadOnlySpan<byte> authToken = default)
    {
        ArgumentNullException.ThrowIfNull(connector);
        ArgumentNullException.ThrowIfNull(endpoint);
        QuiclyPeer peer = new(PeerRole.Client, table, options, null, authToken);
        peer._remoteEndPoint = endpoint as IPEndPoint;
        ITransport transport;
        try
        {
            transport = connector.Connect(endpoint, serverName, peer._sink);
        }
        catch
        {
            peer.FreeResources();
            throw;
        }

        peer.AttachTransport(transport);
        return peer;
    }

    /// <summary>
    /// Creates the server peer of an accepted connection. Call it from the listener's <see cref="AcceptCallback"/> and
    /// return <see cref="TransportSink"/> from there. The client's Hello is answered from <see cref="Poll"/> after
    /// <paramref name="admission"/> decided.
    /// </summary>
    /// <param name="transport">The accepted transport (owned by the peer from now on).</param>
    /// <param name="info">What the listener knows about the connection.</param>
    /// <param name="table">The channel table.</param>
    /// <param name="options">Options (read once).</param>
    /// <param name="admission">Decides who is admitted (runs on the game thread inside <see cref="Poll"/>).</param>
    /// <returns>The peer, in <see cref="PeerState.Connecting"/>.</returns>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    /// <exception cref="ArgumentException">An option is out of range.</exception>
    public static QuiclyPeer CreateServerPeer(ITransport transport, in NewConnectionInfo info, ChannelTable table, PeerOptions options, IPeerAdmission admission)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(admission);
        QuiclyPeer peer = new(PeerRole.Server, table, options, admission, default);
        peer._remoteEndPoint = info.RemoteEndPoint;
        peer.AttachTransport(transport);
        return peer;
    }

    private void AttachTransport(ITransport transport)
    {
        _transport = transport;
        _core.Transport = transport;
    }

    /// <summary>The callbacks of this peer's transport. Hand it to the transport (a server's accept callback returns it).</summary>
    public ITransportSink TransportSink => _sink;

    /// <summary>Dense slot of the peer in its owning host (used as <see cref="ReceiveHeader.PeerIndex"/>); set by the host.</summary>
    public int Index { get; set; }

    /// <summary>Application data.</summary>
    public ulong Tag { get; set; }

    /// <summary>Client or server.</summary>
    public PeerRole Role => _role;

    /// <summary>The lifecycle state as last observed by the game thread.</summary>
    public PeerState State => _state;

    /// <summary>The session epoch: 0 before admission, then ≥ 1 (PROTOCOL.md §4.1).</summary>
    public uint Epoch => _core.Epoch;

    /// <summary>The session id from the HelloAck (0 before admission).</summary>
    public ulong SessionId => _sessionId;

    /// <summary>The session token of the HelloAck (client: present it in <see cref="PeerOptions.SessionToken"/> to resume); empty when none.</summary>
    public ReadOnlyMemory<byte> SessionToken => _sessionToken;

    /// <summary>
    /// The HelloAck status of the handshake: <see cref="HelloStatus.Accepted"/> until a refusal is known (client: the status
    /// the server sent; server: the status it answered).
    /// </summary>
    public HelloStatus HandshakeStatus => _handshakeStatus;

    /// <summary>Why the session ended (<see cref="CloseSource.None"/> while it is open).</summary>
    public CloseReason CloseReason => _closeReason;

    /// <summary>The peer's address (updated on migration).</summary>
    public IPEndPoint? RemoteEndPoint => Volatile.Read(ref _remoteEndPoint);

    /// <summary>What the transport can do right now (default once the peer is disposed).</summary>
    public TransportCapabilities Capabilities
    {
        get
        {
            ITransport? transport = _transport;
            return transport is null || _disposed ? default : transport.Capabilities;
        }
    }

    /// <summary>The channel table.</summary>
    public ChannelTable Channels => _core.Table;

    /// <summary>The server's channel table with names, when the client requested it (<see cref="PeerOptions.RequestChannelTable"/>).</summary>
    public ChannelTableDescription? RemoteChannelTable => _remoteTable;

    /// <summary>The first exception a transport callback threw (a library bug), or <see langword="null"/>.</summary>
    public Exception? LastCallbackFault => Volatile.Read(ref _lastFault);

    internal PeerCore Core => _core;

    internal bool IsFreed => Volatile.Read(ref _freed) != 0;

    /// <summary>Sets a transport-to-game-thread signal bit (any thread).</summary>
    /// <param name="bit">The signal.</param>
    internal void Signal(int bit) => Interlocked.Or(ref _signals, bit);

    /// <summary>Copies the peer's statistics into <paramref name="statistics"/>. Allocation-free; game thread. Zero once disposed.</summary>
    /// <param name="statistics">Receives the snapshot.</param>
    public void GetStatistics(out PeerStatistics statistics)
    {
        statistics = default;
        if (_disposed)
        {
            return;
        }

        ITransport? transport = _transport;
        if (transport is not null && !_core.IsTransportClosed)
        {
            transport.GetStatistics(out statistics.Transport);
        }

        statistics.SmoothedRttMicros = _ping.SmoothedRtt;
        statistics.MinRttMicros = _ping.MinRtt;
        statistics.MaxRttMicros = _ping.MaxRtt;
        statistics.RttVarianceMicros = _ping.RttVariance;
        statistics.LatestRttMicros = _ping.LatestRtt;
        statistics.ClockOffsetMicros = _ping.PublishedOffset;
        statistics.JitterMicros = _ping.Jitter;
        statistics.RttSamples = _ping.Samples;
        statistics.MaxDatagramPayload = _core.MaxDatagramPayload;
        statistics.ReceiveRingCapacity = _core.ReceiveRing.Capacity;
        PeerCounters c = _core.Counters;
        statistics.ReceiveRingHighWater = Volatile.Read(ref c.ReceiveRingHighWater);
        statistics.SendEntriesInUse = _core.Entries.Count;
        statistics.SendTableCapacity = _core.Entries.Capacity;
        statistics.DatagramsReceived = Volatile.Read(ref c.DatagramsReceived);
        statistics.DatagramBytesReceived = Volatile.Read(ref c.DatagramBytesReceived);
        statistics.StreamBytesReceived = Volatile.Read(ref c.StreamBytesReceived);
        statistics.ControlMessagesReceived = Volatile.Read(ref c.ControlMessagesReceived);
        statistics.MalformedDatagrams = Volatile.Read(ref c.MalformedDatagrams);
        statistics.DroppedBeforeAdmission = Volatile.Read(ref c.DroppedBeforeAdmission);
        statistics.StreamsReset = Volatile.Read(ref c.StreamsReset);
        statistics.PingsSent = c.PingsSent;
        statistics.PongsReceived = Volatile.Read(ref c.PongsReceived);
        statistics.PongsSent = Volatile.Read(ref c.PongsSent) + c.StreamPongsSent;
        statistics.PingsIgnored = Volatile.Read(ref c.PingsIgnored);
        statistics.UnmatchedPongs = c.UnmatchedPongs;
        statistics.StaleCompletions = Volatile.Read(ref c.StaleCompletions);
        statistics.ReceiveRingDrops = Volatile.Read(ref c.ReceiveRingDrops);
        statistics.OutOfReceiveBuffers = Volatile.Read(ref c.OutOfReceiveBuffers);
        statistics.CallbackFaults = Volatile.Read(ref c.CallbackFaults);
        statistics.DecodeFailures = c.DecodeFailures;
        statistics.ControlSendFailures = c.ControlSendFailures + Volatile.Read(ref c.PongSendFailures);
        statistics.DatagramsSent = c.DatagramsSent;
        statistics.DatagramBytesSent = c.DatagramBytesSent;
        statistics.ContainersSent = c.ContainersSent;
        statistics.MessagesPacked = c.MessagesPacked;
        statistics.StreamSends = c.StreamSends;
        statistics.StreamBytesSent = c.StreamBytesSent;
        statistics.StreamReceivePends = Volatile.Read(ref c.StreamReceivePends);
        statistics.StreamIdleTimeouts = c.StreamIdleTimeouts;
        statistics.ThreadSafeSends = Volatile.Read(ref c.ThreadSafeSends);
        statistics.ThreadSafeSendDrops = Volatile.Read(ref c.ThreadSafeSendDrops);
        statistics.SendBytesOutstanding = _core.SendBytesOutstanding;
        statistics.ReceiveBytesOutstanding = _core.ReceiveBytesOutstanding;
    }

    /// <summary>Copies the counters of one channel into <paramref name="statistics"/>. Allocation-free; game thread.</summary>
    /// <param name="channel">The channel id.</param>
    /// <param name="statistics">Receives the snapshot.</param>
    /// <returns><see langword="false"/> when the table has no such channel or the peer is disposed.</returns>
    public bool GetChannelStatistics(ushort channel, out ChannelStatistics statistics)
    {
        statistics = default;
        int index = _core.ChannelIndexOf(channel);
        if (index < 0 || _disposed)
        {
            return false;
        }

        ref ChannelSendCounters send = ref _core.SendCounters(index);
        ref ChannelRecvCounters recv = ref _core.RecvCounters(index);
        statistics.Channel = channel;
        statistics.Sent = send.Sent;
        statistics.BytesSent = send.Bytes;
        statistics.SendSuperseded = send.Superseded;
        statistics.Expired = send.Expired;
        statistics.QueueFull = send.QueueFull;
        statistics.TooLarge = send.TooLarge;
        statistics.Retries = send.Retries;
        statistics.SendKeyTableFull = send.KeyTableFull;
        statistics.Received = Volatile.Read(ref recv.Received);
        statistics.BytesReceived = Volatile.Read(ref recv.Bytes);
        statistics.Dropped = Volatile.Read(ref recv.Dropped);
        statistics.ReceiveSuperseded = Volatile.Read(ref recv.Superseded);
        statistics.RingDrops = Volatile.Read(ref recv.RingDrops);
        statistics.ReceiveKeyTableFull = Volatile.Read(ref recv.KeyTableFull);
        statistics.ReceiveTooLarge = Volatile.Read(ref recv.TooLarge);
        statistics.OutOfBuffers = Volatile.Read(ref recv.OutOfBuffers);
        _core.GetEngine(index).AddStatistics(index, ref statistics);
        return true;
    }

    /// <summary>
    /// The remote peer's clock now, as microseconds since the remote's connection start (the time base of its Ping/Pong
    /// timestamps, PROTOCOL.md §4.6): local connection-relative time plus the published offset. Advisory (peer supplied);
    /// equals the local connection-relative time before the first Pong. Game thread.
    /// </summary>
    /// <returns>The estimate in microseconds.</returns>
    public long EstimatedRemoteMicros() => _clock.NowMicros - _core.ConnectionStartMicros + _ping.PublishedOffset;

    /// <summary>
    /// Time until the next time-driven work (ping, timeout, retry, linger) as of the last <see cref="Poll"/> or
    /// <see cref="Flush"/>; <see cref="Timeout.InfiniteTimeSpan"/> when nothing is scheduled. A host without other work may
    /// sleep this long before polling again (new network input is not covered: poll on transport activity too).
    /// </summary>
    public TimeSpan NextDeadline
    {
        get
        {
            long deadline = _nextDeadlineMicros;
            if (deadline == long.MaxValue)
            {
                return Timeout.InfiniteTimeSpan;
            }

            long remaining = deadline - _clock.NowMicros;
            return remaining <= 0 ? TimeSpan.Zero : TimeSpan.FromTicks(remaining * (TimeSpan.TicksPerMillisecond / 1000));
        }
    }

    /// <summary>The next deadline in clock micros (<see cref="PeerOptions.Clock"/>), or <see cref="long.MaxValue"/>.</summary>
    public long NextDeadlineMicros => _nextDeadlineMicros;

    /// <summary>
    /// Closes the transport if it is still open (error code 0, no linger) and releases the peer. Native memory is freed as
    /// soon as the transport has reported its close and no <see cref="Poll"/>/<see cref="Flush"/> is running (so disposing
    /// from a handler is safe). Release retained leases first; afterwards <see cref="Release(in ReceiveLease)"/> is a no-op.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        FailWaitersOnDispose();
        ITransport? transport = _transport;
        if (transport is not null)
        {
            if (!_transportCloseCalled && !_core.IsTransportClosed)
            {
                _transportCloseCalled = true;
                _core.MarkTransportClosing();
                transport.Close((ulong)QuiclyErrorCode.NoError, default);
            }

            transport.Dispose();
        }

        // Without a transport no close callback will come, so the close counts as seen.
        int requested = transport is null ? LifetimeDisposeRequested | LifetimeClosedSeen : LifetimeDisposeRequested;
        int previous = Interlocked.Or(ref _lifetime, requested);
        if (((previous | requested) & (LifetimeClosedSeen | LifetimeInCall)) == LifetimeClosedSeen)
        {
            FreeResources();
        }
    }

    /// <summary>The transport reported its close (transport thread; last callback).</summary>
    private void OnClosedSeen()
    {
        int previous = Interlocked.Or(ref _lifetime, LifetimeClosedSeen);
        if ((previous & (LifetimeDisposeRequested | LifetimeInCall)) == LifetimeDisposeRequested)
        {
            FreeResources();
        }
    }

    /// <summary>A game-thread Poll/Flush begins (nested calls from handlers only count once).</summary>
    private void EnterCall()
    {
        if (_callDepth++ == 0)
        {
            Interlocked.Or(ref _lifetime, LifetimeInCall);
        }
    }

    /// <summary>A game-thread Poll/Flush ends; frees native memory if the peer was disposed and the transport closed meanwhile.</summary>
    private void ExitCall()
    {
        if (--_callDepth != 0)
        {
            return;
        }

        int previous = Interlocked.And(ref _lifetime, ~LifetimeInCall);
        if ((previous & (LifetimeDisposeRequested | LifetimeClosedSeen)) == (LifetimeDisposeRequested | LifetimeClosedSeen))
        {
            FreeResources();
        }
    }

    private void FreeResources()
    {
        if (Interlocked.Exchange(ref _freed, 1) != 0)
        {
            return;
        }

        _queues.ReleaseAll(_core);
        if (_hasHeld)
        {
            _hasHeld = false;
            _core.ReturnReceive(in _held.Lease);
        }

        ReleaseForeignSends();
        _core.Dispose();
        _controlPool.Dispose();
        _queues.Dispose();
        _pongs.Dispose();
        _streamPings.Dispose();
        _front?.Dispose();
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private readonly record struct StateTransition(PeerState From, PeerState To);
}
