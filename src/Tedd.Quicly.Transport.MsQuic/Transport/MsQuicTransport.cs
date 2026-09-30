using System.Net;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// <see cref="ITransport"/> over one <see cref="MsQuicConnection"/> (raw QUIC, ALPN <c>quicly/1</c>). Created by
/// <see cref="MsQuicTransportConnector"/> (client) or <see cref="MsQuicTransportListener"/> (server); translates the
/// wrapper's connection and stream events into <see cref="ITransportSink"/> callbacks as the contract documents.
/// </summary>
/// <remarks>
/// <para><b>Callbacks.</b> Every sink call runs on an MsQuic worker thread, serialised per connection, and may run inline
/// inside an API call made from another callback of the same connection (MsQuic executes such calls inline).
/// <see cref="ITransportSink.OnDatagramCapabilityChanged"/> may precede <see cref="ITransportSink.OnConnected"/>;
/// <see cref="ITransportSink.OnStreamsAvailable"/> is only raised once connected. <see cref="ITransportSink.OnClosed"/> is
/// raised exactly once, after MsQuic's SHUTDOWN_COMPLETE and after every stream of the connection reported
/// <see cref="ITransportSink.OnStreamShutdownComplete"/> (streams that were never started are reported by the transport
/// itself, MsQuic does not track them); nothing follows it. An exception thrown by the sink, or by the transport while it
/// translates an MsQuic event, is caught, recorded in <see cref="LastSinkException"/> and shuts the connection down with
/// <see cref="InternalErrorCode"/> (ADR 0008 §8).</para>
/// <para><b>Sends.</b> <see cref="SendDatagram"/> and <see cref="SendStream"/> hand the caller's
/// <see cref="TransportSegment"/> array to MsQuic as <c>QUIC_BUFFER*</c> without copying or translating it, and pass the
/// caller's context through as MsQuic's client context. Status mapping (<see cref="MapStatus"/>): accepted (SUCCESS or
/// PENDING) is <see cref="TransportStatus.Success"/>; <c>INVALID_STATE</c> is <see cref="TransportStatus.InvalidState"/>;
/// <c>INVALID_PARAMETER</c> on a datagram send is <see cref="TransportStatus.TooLarge"/> (the path MTU shrank);
/// <c>OUT_OF_MEMORY</c> is <see cref="TransportStatus.OutOfMemory"/>; <c>STREAM_LIMIT_REACHED</c> is
/// <see cref="TransportStatus.StreamLimitReached"/>; <c>NOT_SUPPORTED</c> is <see cref="TransportStatus.NotSupported"/>;
/// anything else is <see cref="TransportStatus.Failed"/>.</para>
/// <para><b>Streams.</b> Streams live in a generation-tagged table of <see cref="MsQuicTransportOptions.MaxStreams"/>
/// slots (grown on demand, slots reused), or more when the streams the peer was granted need more room: a peer stream is
/// never refused for want of a slot, and a quarter of the table is this end's own
/// (<see cref="MsQuicTransportOptions.StreamTableFor"/>). A local stream is started by <see cref="StartStream"/> or by the first send
/// carrying <see cref="TransportSendFlags.Start"/>, with <c>FAIL_BLOCKED | SHUTDOWN_ON_FAIL</c>. MsQuic queues the start,
/// so a start the peer's stream limit refuses is reported asynchronously: <see cref="ITransportSink.OnStreamStarted"/> with
/// <see cref="TransportStatus.StreamLimitReached"/>, a canceled completion for every send accepted with the start, then
/// <see cref="ITransportSink.OnStreamShutdownComplete"/>. A refused stream never starts (<see cref="StartStream"/> returns
/// <see cref="TransportStatus.InvalidState"/>): release it with <see cref="CloseStream"/> and open a new stream to retry after
/// <see cref="ITransportSink.OnStreamsAvailable"/>.</para>
/// <para><b>Receive.</b> <see cref="ITransportSink.OnStreamReceived"/> sees MsQuic's receive buffers reinterpreted in place.
/// Consuming everything completes the receive. <see cref="ReceiveResult.PendingAfter"/> is answered to MsQuic as a partial
/// consumption of the bytes the sink took, which MsQuic completes itself when the callback returns and after which it
/// pauses the stream (measured on msquic 2.5.10, see <c>docs/benchmarks/msquic-transport.md</c>);
/// <see cref="ResumeStreamReceive"/> later calls <c>StreamReceiveSetEnabled(TRUE)</c>, which MsQuic always queues to the
/// connection's worker, and the bytes the resume credits are skipped at the head of the next indication. No receive is
/// left pending for another thread to complete: MsQuic drops a completion of zero bytes that reaches it before the
/// receive callback has returned, and the stream would never be indicated again (the same table, R4). Two holds cannot be
/// partial: of an indication the sink consumed whole one byte is kept back and skipped later, and an indication that
/// carries only the FIN stays pending and is indicated again by the re-enable alone. A FIN the sink saw on the indication
/// it held is not shown to it a second time: once the bytes before it are the sink's, the peer's send shutdown follows
/// (as on the simulator). A resume that arrives while the receive callback is still running is
/// applied when it returns. A partial <see cref="ReceiveResult.Consumed"/> of at least one byte re-enables receives
/// inline, so MsQuic indicates the remainder again right away, together with anything that arrived since; consuming
/// nothing of a non-empty indication without <c>Pending</c> counts as <c>PendingAfter(0)</c> (see <see cref="ReceiveResult"/>).</para>
/// <para><b>Closing streams.</b> Every native <c>StreamClose</c> runs on a thread-pool work item (this object, no
/// allocation) that drains a lock-free deferred-close list, never on the calling thread and never on a callback thread:
/// MsQuic executes <c>StreamClose</c> of a never-started stream on the connection's worker and waits for it. Every close
/// queued while the work item is pending is handled by that one work item. So <see cref="CloseStream"/> and
/// <see cref="AbortStream"/> return at once, and a released slot returns to the table once the work item has run
/// (<see cref="OpenStreamCount"/> counts it until then, and a full table can answer <see cref="TransportStatus.OutOfMemory"/>
/// for that moment). Before shutdown completes, <see cref="CloseStream"/> aborts both directions with code 0; pending send
/// completions are still reported (canceled), the shutdown callback is not.</para>
/// <para><b>Calls that wait for the MsQuic worker.</b> <see cref="SetStreamPriority"/> on a started stream,
/// <see cref="UpdatePeerStreamLimits"/>, <see cref="Close"/> with a reason and <see cref="GetStatistics"/> are MsQuic
/// parameter calls: made off the connection's worker thread, MsQuic queues them to that worker and waits for it (about
/// 15 µs when it is idle, as long as a running callback takes otherwise). Never call them from a callback of another
/// connection: when both connections share a worker that is a deadlock. From a callback of this connection they run
/// inline. Every other member returns without waiting for the worker.</para>
/// <para><b>Lifetime.</b> <see cref="Dispose"/> may be called from any thread and never waits: it shuts the connection
/// down if needed, and the connection and stream handles are closed on a thread-pool thread once MsQuic reported
/// SHUTDOWN_COMPLETE (<see cref="WaitForHandlesClosed"/>). Do not call other members concurrently with or after
/// <see cref="Dispose"/>.</para>
/// <para><b>Platform.</b> 64-bit processes only: <see cref="TransportSegment"/> is handed to MsQuic as <c>QUIC_BUFFER</c>,
/// which has the same layout only there (<see cref="SegmentLayoutMatchesQuicBuffer"/>); the connector, the listener and
/// the transport throw <see cref="PlatformNotSupportedException"/> in a 32-bit process.</para>
/// </remarks>
public sealed unsafe partial class MsQuicTransport : ITransport, IMsQuicConnectionEvents, IMsQuicStreamEvents, IThreadPoolWorkItem
{
    /// <summary>Application error code used when a sink callback threw (PROTOCOL section 6 <c>InternalError</c>).</summary>
    public const ulong InternalErrorCode = 0x07;

    /// <summary>Longest close reason accepted by <see cref="Close"/>, in bytes.</summary>
    public const int MaxReasonBytes = 512;

    private const int StateConnecting = (int)TransportState.Connecting;
    private const int StateConnected = (int)TransportState.Connected;
    private const int StateClosing = (int)TransportState.Closing;
    private const int StateClosed = (int)TransportState.Closed;

    /// <summary>Bit of <see cref="_datagramState"/> set while datagram sends are enabled; the low 16 bits hold MaxSendLength.</summary>
    private const int DatagramEnabledBit = 1 << 16;

    private static readonly bool s_segmentLayoutMatches = ComputeSegmentLayoutMatch();

    private readonly MsQuicConnection _connection;
    private readonly TransportDiagnosticCallback? _diagnostic;
    private readonly ServerCertificatePolicy? _certificatePolicy;
    private readonly string? _serverName;
    private readonly QUIC_STREAM_SCHEDULING_SCHEME _schedulingScheme;
    private readonly bool _cancelOnBlockedSupported;
    private readonly QUIC_SEND_FLAGS _supportedSendFlags;
    private readonly ManualResetEventSlim _handlesClosed = new(false);
    private ITransportSink? _sink;
    private int _state;
    private int _closedDelivered;
    private int _closeRecorded;
    private TransportCloseReason _closeReason = TransportCloseReason.Transport;
    private ulong _closeErrorCode;
    private int _closeStatus;

    /// <summary>
    /// DATAGRAM_STATE_CHANGED as one value (<see cref="DatagramEnabledBit"/> | MaxSendLength), so a reader never pairs the
    /// enabled flag of one report with the maximum payload of another.
    /// </summary>
    private volatile int _datagramState;
    private int _sinkFailed;
    private Exception? _lastSinkException;
    private long _sinkExceptions;
    private long _refusedPeerStreams;
    private long _lateEvents;
    private int _disposed;

    /// <summary>
    /// 1 once SHUTDOWN_COMPLETE has been handled. Written with <see cref="Interlocked.Exchange(ref int, int)"/> before
    /// <see cref="_disposed"/> is read (and <see cref="Dispose"/> exchanges <see cref="_disposed"/> before reading this), so
    /// at least one side always sees the other: a plain volatile store could be reordered after the load on x86 and both
    /// sides would miss the release.
    /// </summary>
    private int _shutdownComplete;
    private int _handleClosed;
    private int _workScheduled;
    private IPEndPoint? _remoteEndPoint;
    private TransportConnectedInfo _connectedInfo;
    private MsQuicTransportListener.ConfigurationEntry? _configurationLease;

    /// <summary>Unidirectional streams the connection's settings grant the peer before <see cref="UpdatePeerStreamLimits"/> (<see cref="TransportCapabilities.PeerUnidirectionalStreams"/>).</summary>
    private readonly int _initialPeerUnidiStreams;
    private int _streamTableRaised;

    internal MsQuicTransport(MsQuicConnection connection, ITransportSink? sink, MsQuicTransportOptions options, ServerCertificatePolicy? certificatePolicy, string? serverName, int initialPeerUnidiStreams = 0, int initialPeerBidiStreams = 0)
    {
        _initialPeerUnidiStreams = initialPeerUnidiStreams;
        ThrowIfSegmentLayoutUnsupported();
        _connection = connection;
        _sink = sink;
        _diagnostic = options.Diagnostic;
        _certificatePolicy = certificatePolicy;
        _serverName = serverName;
        _schedulingScheme = options.StreamSchedulingScheme;
        _supportedSendFlags = connection.Api.SupportedSendFlags;
        _cancelOnBlockedSupported = (_supportedSendFlags & QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED) != 0;
        InitializeStreams(options.MaxStreams);
        MakeRoomForPeerStreams(initialPeerBidiStreams, initialPeerUnidiStreams);
        connection.Events = this;
    }

    /// <summary>
    /// True when <see cref="TransportSegment"/> has the size and field offsets of MsQuic's <c>QUIC_BUFFER</c>, so segment
    /// arrays can be handed to MsQuic unchanged. Holds in 64-bit processes only (<c>QUIC_BUFFER</c> is 8 bytes in a 32-bit one).
    /// </summary>
    public static bool SegmentLayoutMatchesQuicBuffer => s_segmentLayoutMatches;

    /// <summary>The underlying wrapper connection (for diagnostics and parameters the contract does not cover). Never close it directly.</summary>
    public MsQuicConnection Connection => _connection;

    /// <summary>True for a transport accepted by a listener.</summary>
    public bool IsServer => _connection.IsServer;

    /// <summary>
    /// True when the loaded MsQuic accepts <c>QUIC_SEND_FLAG_CANCEL_ON_BLOCKED</c> (2.4+), also reported as
    /// <see cref="TransportCapabilities.CancelOnBlocked"/>. When false, <see cref="TransportSendFlags.CancelOnBlocked"/> is ignored.
    /// </summary>
    public bool CancelOnBlockedSupported => _cancelOnBlockedSupported;

    /// <summary>
    /// The last exception a sink callback (or the transport's handling of an MsQuic event) threw; the connection was then
    /// shut down with <see cref="InternalErrorCode"/>.
    /// </summary>
    public Exception? LastSinkException => Volatile.Read(ref _lastSinkException);

    /// <summary>Number of exceptions thrown by sink callbacks (and by the transport's handling of MsQuic events).</summary>
    public long SinkExceptionCount => Interlocked.Read(ref _sinkExceptions);

    /// <summary>
    /// Peer streams refused because the stream table was full. Stays 0: the table is sized for every stream the peer was
    /// granted (<see cref="MsQuicTransportOptions.StreamTableFor"/>), and MsQuic admits no other.
    /// </summary>
    public long RefusedPeerStreamCount => Interlocked.Read(ref _refusedPeerStreams);

    /// <summary>MsQuic events that arrived after <see cref="ITransportSink.OnClosed"/> and were dropped (expected to stay 0).</summary>
    public long LateEventCount => Interlocked.Read(ref _lateEvents);

    /// <summary>True once the native connection and stream handles are closed (after <see cref="Dispose"/> and SHUTDOWN_COMPLETE).</summary>
    public bool HandlesClosed => Volatile.Read(ref _handleClosed) != 0;

    /// <summary>Waits until the native handles are closed (see <see cref="HandlesClosed"/>). Never call it from an MsQuic callback.</summary>
    public bool WaitForHandlesClosed(TimeSpan timeout) => _handlesClosed.Wait(timeout);

    /// <summary>The peer's address as known at connect (or after the last address change); null before the handshake completes.</summary>
    public IPEndPoint? RemoteEndPoint => Volatile.Read(ref _remoteEndPoint);

    /// <summary>The server name the client connected to (SNI), or the one the client sent to a listener; null when none.</summary>
    public string? ServerName => _serverName;

    /// <summary>Called once the handles are closed, on a thread-pool thread (or on the thread that released an unstarted connection).</summary>
    internal Action<MsQuicTransport>? HandlesClosedCallback { get; set; }

    internal MsQuicTransportListener.ConfigurationEntry? ConfigurationLease
    {
        get => Volatile.Read(ref _configurationLease);
        set => Volatile.Write(ref _configurationLease, value);
    }

    internal void SetSink(ITransportSink sink) => Volatile.Write(ref _sink, sink);

    /// <inheritdoc/>
    public TransportCapabilities Capabilities
    {
        get
        {
            int datagrams = _datagramState;
            bool enabled = (datagrams & DatagramEnabledBit) != 0;
            return new TransportCapabilities
            {
                Datagrams = enabled,
                DatagramSendState = enabled,
                MaxDatagramPayload = enabled ? datagrams & 0xFFFF : 0,
                StreamPriority = true,
                AppOwnedReceiveBuffers = false,
                IdealSendBufferSize = true,
                CancelOnBlocked = _cancelOnBlockedSupported,

                // What the connection's own settings let the peer open from the first packet on. It is in the transport
                // parameters, so nothing the session asks for later takes it back.
                PeerUnidirectionalStreams = _initialPeerUnidiStreams,
            };
        }
    }

    /// <inheritdoc/>
    public TransportState State => (TransportState)Volatile.Read(ref _state);

    /// <inheritdoc/>
    /// <remarks>
    /// Sets <c>QUIC_PARAM_CONN_CLOSE_REASON_PHRASE</c> (a NUL-terminated copy; MsQuic rejects the phrase without the
    /// terminator) when a reason is given, then <c>ConnectionShutdown(errorCode)</c>. The peer reports
    /// <see cref="TransportCloseReason.Peer"/> with <paramref name="errorCode"/>; this end reports
    /// <see cref="TransportCloseReason.Local"/> with <paramref name="errorCode"/>. Ignored once closing. MsQuic does not
    /// surface the phrase it receives, so the peer never sees it through the contract. With a reason the call waits for the
    /// MsQuic worker (a parameter call; see the class remarks), without one it only queues the shutdown.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="reason"/> is longer than <see cref="MaxReasonBytes"/>.</exception>
    public void Close(ulong errorCode, ReadOnlySpan<byte> reason)
    {
        if (reason.Length > MaxReasonBytes) throw new ArgumentOutOfRangeException(nameof(reason), reason.Length, $"The close reason is limited to {MaxReasonBytes} bytes.");
        if (!TryMoveToClosing() || _connection.IsClosed) return;
        RecordClose(TransportCloseReason.Local, errorCode, 0);
        if (!reason.IsEmpty)
        {
            byte* phrase = stackalloc byte[MaxReasonBytes + 1];
            reason.CopyTo(new Span<byte>(phrase, MaxReasonBytes));
            phrase[reason.Length] = 0;
            _connection.SetParam(MsQuicParam.QUIC_PARAM_CONN_CLOSE_REASON_PHRASE, (uint)reason.Length + 1, phrase);
        }
        _connection.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, errorCode);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Reads <c>QUIC_PARAM_CONN_STATISTICS_V2</c> into a stack buffer (no allocation). Waits for the MsQuic worker when
    /// called off it (a parameter call, about 16 µs; see the class remarks). <see cref="TransportStatistics.BytesInFlight"/>
    /// is always 0: MsQuic's stable statistics do not report it. <see cref="TransportStatistics.RttVarianceMicros"/> needs
    /// MsQuic 2.5. All fields are 0 once the transport has been disposed.
    /// </remarks>
    public void GetStatistics(out TransportStatistics statistics)
    {
        statistics = default;
        if (Volatile.Read(ref _disposed) != 0 || _connection.IsClosed) return;
        if (MsQuicStatus.Failed(_connection.GetStatisticsV2(out QUIC_STATISTICS_V2 s, out uint written))) return;
        statistics.RttMicros = s.Rtt;
        statistics.MinRttMicros = s.MinRtt;
        statistics.MaxRttMicros = s.MaxRtt;
        statistics.RttVarianceMicros = written >= QUIC_STATISTICS_V2.SIZE_4 ? s.RttVariance : 0;
        statistics.CongestionWindowBytes = s.SendCongestionWindow;
        statistics.PathMtu = s.SendPathMtu;
        statistics.SendTotalBytes = s.SendTotalBytes;
        statistics.RecvTotalBytes = s.RecvTotalBytes;
        statistics.SendTotalPackets = s.SendTotalPackets;
        statistics.RecvTotalPackets = s.RecvTotalPackets;
        statistics.SendSuspectedLostPackets = s.SendSuspectedLostPackets;
        statistics.SendSpuriousLostPackets = s.SendSpuriousLostPackets;
        statistics.CongestionEvents = s.SendCongestionCount;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// <c>PeerBidiStreamCount</c> / <c>PeerUnidiStreamCount</c> through <c>QUIC_PARAM_CONN_SETTINGS</c>
    /// (<see cref="MsQuicConnection.UpdatePeerStreamLimits"/>), a parameter call that waits for the MsQuic worker when made
    /// off it (see the class remarks). The peer sees <see cref="ITransportSink.OnStreamsAvailable"/> once the MAX_STREAMS
    /// frame arrives. QUIC never takes granted credit back: a lower value only limits the credit MsQuic grants later, as the
    /// peer's streams close. Ignored once closing.
    /// </remarks>
    public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional)
    {
        int state = Volatile.Read(ref _state);
        if (state is StateClosing or StateClosed || _connection.IsClosed) return;
        // Before the grant: the table has the room when the first peer stream beyond the old limit arrives.
        MakeRoomForPeerStreams(bidirectional, unidirectional);
        _connection.UpdatePeerStreamLimits(bidirectional, unidirectional);
    }

    /// <summary>
    /// Shuts the connection down (error code 0) if it is still open and has the native handles closed on a thread-pool
    /// thread once MsQuic has reported SHUTDOWN_COMPLETE (<see cref="WaitForHandlesClosed"/>). Safe from any thread,
    /// including MsQuic callbacks; never waits for MsQuic.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        // The exchange is a full fence: SHUTDOWN_COMPLETE either reads _disposed == 1 (and schedules the release) or has
        // already published _shutdownComplete, which this read then sees.
        if (Volatile.Read(ref _shutdownComplete) == 0) Close(0, default);
        else ScheduleCleanup();
    }

    /// <summary>
    /// Releases a transport whose connection never started (ConnectionStart failed synchronously, so MsQuic raises no
    /// events): the handles are closed without waiting for SHUTDOWN_COMPLETE, on this thread unless it is a callback thread.
    /// </summary>
    internal void ReleaseUnstarted()
    {
        Volatile.Write(ref _state, StateClosed);
        Volatile.Write(ref _closedDelivered, 1);
        Volatile.Write(ref _disposed, 1);
        Interlocked.Exchange(ref _shutdownComplete, 1);
        if (MsQuicCallbackScope.IsInsideCallback) ScheduleCleanup();
        else ReleaseHandlesNow();
    }

    /// <summary>
    /// The listener refused the connection after creating this transport (the accept callback returned null or threw):
    /// MsQuic keeps the native connection and never raises an event for it, so the transport is
    /// <see cref="TransportState.Closed"/> at once, delivers no callback and has no handle of its own to close.
    /// </summary>
    internal void MarkRefused()
    {
        Volatile.Write(ref _state, StateClosed);
        Volatile.Write(ref _closedDelivered, 1);
        Volatile.Write(ref _disposed, 1);
        lock (_tableLock) _tableClosed = true;
        Interlocked.Exchange(ref _shutdownComplete, 1);
        Interlocked.Exchange(ref _handleClosed, 1);
        _handlesClosed.Set();
    }

    /// <summary>
    /// The listener accepted the connection but MsQuic refused its configuration: MsQuic drops the connection without any
    /// event, so the sink gets <see cref="ITransportSink.OnClosed"/> (<see cref="TransportCloseReason.Transport"/>,
    /// <paramref name="status"/>) here, on the worker thread, and the transport is released.
    /// </summary>
    internal void FailBeforeStart(int status)
    {
        ReleaseConfigurationLease();
        Volatile.Write(ref _state, StateClosed);
        lock (_tableLock) _tableClosed = true;
        ITransportSink? sink = Interlocked.Exchange(ref _closedDelivered, 1) == 0 ? Volatile.Read(ref _sink) : null;
        if (sink is not null)
        {
            try
            {
                sink.OnClosed(TransportCloseReason.Transport, 0, status);
            }
            catch (Exception ex)
            {
                RecordCallbackFailure(ex, "ITransportSink.OnClosed threw.");
            }
        }
        Volatile.Write(ref _disposed, 1);
        Interlocked.Exchange(ref _shutdownComplete, 1);
        ScheduleCleanup();
    }

    internal static void ThrowIfSegmentLayoutUnsupported()
    {
        if (!s_segmentLayoutMatches)
        {
            throw new PlatformNotSupportedException("The MsQuic transport hands TransportSegment arrays to MsQuic as QUIC_BUFFER arrays, whose layout matches only in a 64-bit process.");
        }
    }

    private static bool ComputeSegmentLayoutMatch()
    {
        TransportSegment segment = default;
        QUIC_BUFFER buffer = default;
        long segmentLength = (byte*)&segment.Length - (byte*)&segment;
        long segmentBuffer = (byte*)&segment.Buffer - (byte*)&segment;
        long bufferLength = (byte*)&buffer.Length - (byte*)&buffer;
        long bufferBuffer = (byte*)&buffer.Buffer - (byte*)&buffer;
        return sizeof(TransportSegment) == sizeof(QUIC_BUFFER) && segmentLength == bufferLength && segmentBuffer == bufferBuffer;
    }

    // ------------------------------------------------------------------ close bookkeeping

    private bool TryMoveToClosing()
    {
        int state = Volatile.Read(ref _state);
        while (state is StateConnecting or StateConnected)
        {
            int seen = Interlocked.CompareExchange(ref _state, StateClosing, state);
            if (seen == state) return true;
            state = seen;
        }
        return false;
    }

    private void RecordClose(TransportCloseReason reason, ulong errorCode, int status)
    {
        if (Interlocked.Exchange(ref _closeRecorded, 1) != 0) return;
        _closeReason = reason;
        _closeErrorCode = errorCode;
        _closeStatus = status;
    }

    /// <summary>
    /// An exception escaped a sink callback or the transport's handling of an MsQuic event: records it and shuts the
    /// connection down once with <see cref="InternalErrorCode"/>. Handlers catch everything, so no exception reaches the
    /// wrapper (which would poison the connection with its own error code instead).
    /// </summary>
    private void OnHandlerException(Exception exception)
    {
        RecordCallbackFailure(exception, "An ITransportSink callback (or the transport's handling of an MsQuic event) threw; the connection is shut down with InternalError.");
        if (Interlocked.Exchange(ref _sinkFailed, 1) != 0 || Volatile.Read(ref _closedDelivered) != 0) return;
        TryMoveToClosing();
        RecordClose(TransportCloseReason.Local, InternalErrorCode, 0);
        if (!_connection.IsClosed) _connection.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, InternalErrorCode);
    }

    /// <summary>Records an exception thrown where the connection is already going away (no shutdown needed).</summary>
    private void RecordCallbackFailure(Exception exception, string message)
    {
        Volatile.Write(ref _lastSinkException, exception);
        Interlocked.Increment(ref _sinkExceptions);
        MsQuicCallbackScope.OnEscapedException(exception);
        Diagnose(TransportDiagnosticLevel.Error, message, exception);
    }

    private void Diagnose(TransportDiagnosticLevel level, string message, Exception? exception)
    {
        try
        {
            _diagnostic?.Invoke(level, message, exception);
        }
        catch
        {
            // A diagnostics sink must not break the transport.
        }
    }

    /// <summary>The sink when callbacks may still be delivered, else null (and the event is counted as late).</summary>
    private ITransportSink? LiveSink
    {
        get
        {
            if (Volatile.Read(ref _closedDelivered) == 0) return Volatile.Read(ref _sink);
            Interlocked.Increment(ref _lateEvents);
            return null;
        }
    }

    // ------------------------------------------------------------------ handle release / cleanup work item

    /// <summary>Publishes SHUTDOWN_COMPLETE (full fence, then reads <see cref="_disposed"/>; see <see cref="_shutdownComplete"/>).</summary>
    private void MarkShutdownComplete()
    {
        Interlocked.Exchange(ref _shutdownComplete, 1);
        if (Volatile.Read(ref _disposed) != 0 || HasDeferredCloses) ScheduleCleanup();
    }

    /// <summary>Closes every stream handle and the connection handle. Never on an MsQuic callback thread.</summary>
    private void ReleaseHandlesNow()
    {
        if (Interlocked.Exchange(ref _handleClosed, 1) != 0) return;
        lock (_cleanupLock)
        {
            DrainDeferredClosesLocked();
            CloseAllStreamHandlesLocked();
            DrainDeferredClosesLocked();
        }
        ReleaseConfigurationLease();
        _connection.Close();
        _handlesClosed.Set();
        try
        {
            HandlesClosedCallback?.Invoke(this);
        }
        catch (Exception ex)
        {
            Diagnose(TransportDiagnosticLevel.Error, "The handles-closed callback threw.", ex);
        }
    }

    /// <summary>Queues <see cref="IThreadPoolWorkItem.Execute"/> unless it is already queued (no allocation: the transport is the work item).</summary>
    private void ScheduleCleanup()
    {
        if (Interlocked.Exchange(ref _workScheduled, 1) == 0) ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
    }

    void IThreadPoolWorkItem.Execute()
    {
        // A full fence before the reads below: a close queued, or a shutdown completing, after this point schedules the
        // work item again, and anything published before it is seen here.
        Interlocked.Exchange(ref _workScheduled, 0);
        try
        {
            if (Volatile.Read(ref _handleClosed) != 0) return;
            if (Volatile.Read(ref _disposed) != 0 && Volatile.Read(ref _shutdownComplete) != 0) ReleaseHandlesNow();
            else DrainDeferredCloses();
        }
        catch (Exception ex)
        {
            // An exception escaping a thread-pool work item would terminate the process.
            Diagnose(TransportDiagnosticLevel.Error, "Deferred handle cleanup failed.", ex);
        }
    }

    // ------------------------------------------------------------------ connection events (MsQuic worker thread)

    void IMsQuicConnectionEvents.Connected(MsQuicConnection connection, ReadOnlySpan<byte> negotiatedAlpn, bool sessionResumed)
    {
        try
        {
            ReleaseConfigurationLease();
            if (Interlocked.CompareExchange(ref _state, StateConnected, StateConnecting) != StateConnecting) return;
            QUIC_STREAM_SCHEDULING_SCHEME scheme = _schedulingScheme;
            _connection.SetParam(MsQuicParam.QUIC_PARAM_CONN_STREAM_SCHEDULING_SCHEME, in scheme);
            TransportConnectedInfo info = default;
            info.RemoteEndPoint = _connection.RemoteEndPoint;
            info.LocalEndPoint = _connection.LocalEndPoint;
            int length = Math.Min(negotiatedAlpn.Length, 255);
            negotiatedAlpn[..length].CopyTo(info.Alpn);
            info.AlpnLength = (byte)length;
            info.SessionResumed = sessionResumed;
            info.Capabilities = Capabilities;
            _connectedInfo = info;
            Volatile.Write(ref _remoteEndPoint, info.RemoteEndPoint);
            LiveSink?.OnConnected(in info);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }

    void IMsQuicConnectionEvents.ShutdownInitiatedByTransport(MsQuicConnection connection, int status, ulong errorCode)
    {
        TryMoveToClosing();
        RecordClose(TransportCloseReason.Transport, errorCode, status);
    }

    void IMsQuicConnectionEvents.ShutdownInitiatedByPeer(MsQuicConnection connection, ulong errorCode)
    {
        TryMoveToClosing();
        RecordClose(TransportCloseReason.Peer, errorCode, 0);
    }

    void IMsQuicConnectionEvents.ShutdownComplete(MsQuicConnection connection, bool handshakeCompleted, bool peerAcknowledgedShutdown, bool appCloseInProgress)
    {
        ITransportSink? sink = null;
        try
        {
            TryMoveToClosing();
            RecordClose(TransportCloseReason.Transport, 0, 0);
            ReleaseConfigurationLease();
            sink = Volatile.Read(ref _closedDelivered) == 0 ? Volatile.Read(ref _sink) : null;
            ShutdownRemainingStreams(sink);
        }
        catch (Exception ex)
        {
            RecordCallbackFailure(ex, "Reporting the remaining streams at connection shutdown failed.");
        }
        finally
        {
            // Whatever happened above: the transport is closed, OnClosed is delivered once and the handles get released.
            Volatile.Write(ref _state, StateClosed);
            if (Interlocked.Exchange(ref _closedDelivered, 1) == 0 && sink is not null) DeliverClosed(sink);
            MarkShutdownComplete();
        }
    }

    private void DeliverClosed(ITransportSink sink)
    {
        try
        {
            sink.OnClosed(_closeReason, _closeErrorCode, _closeStatus);
        }
        catch (Exception ex)
        {
            RecordCallbackFailure(ex, "ITransportSink.OnClosed threw.");
        }
    }

    void IMsQuicConnectionEvents.StreamsAvailable(MsQuicConnection connection, ushort bidirectionalCount, ushort unidirectionalCount)
    {
        // MsQuic reports the initial allowance before CONNECTED; the contract raises it only for later changes.
        if (Volatile.Read(ref _state) != StateConnected) return;
        try
        {
            LiveSink?.OnStreamsAvailable(bidirectionalCount, unidirectionalCount);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }

    void IMsQuicConnectionEvents.PeerAddressChanged(MsQuicConnection connection, in QUIC_ADDR address)
    {
        try
        {
            TransportConnectedInfo info = _connectedInfo;
            info.RemoteEndPoint = address.ToIPEndPoint();
            info.LocalEndPoint = _connection.LocalEndPoint;
            info.Capabilities = Capabilities;
            _connectedInfo = info;
            Volatile.Write(ref _remoteEndPoint, info.RemoteEndPoint);
            LiveSink?.OnPeerAddressChanged(in info);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }

    MsQuicCertificateDecision IMsQuicConnectionEvents.PeerCertificateReceived(MsQuicConnection connection, in MsQuicPeerCertificateInfo info)
        => _certificatePolicy?.Decide(in info, _serverName) ?? MsQuicCertificateDecision.Reject;

    private void ReleaseConfigurationLease() => Interlocked.Exchange(ref _configurationLease, null)?.Release();
}
