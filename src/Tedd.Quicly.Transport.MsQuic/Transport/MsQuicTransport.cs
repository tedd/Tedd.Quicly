using System.Net;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// <see cref="ITransport"/> over one <see cref="MsQuicConnection"/> (raw QUIC, ALPN <c>quicly/1</c>). Created by
/// <see cref="MsQuicTransportConnector"/> (client) or <see cref="MsQuicTransportListener"/> (server); translates the
/// wrapper's connection and stream events into <see cref="ITransportSink"/> callbacks exactly as the contract documents.
/// </summary>
/// <remarks>
/// <para><b>Callbacks.</b> Every sink call runs on an MsQuic worker thread, serialised per connection, and may run inline
/// inside an API call made from another callback of the same connection (MsQuic executes such calls inline).
/// <see cref="ITransportSink.OnDatagramCapabilityChanged"/> may precede <see cref="ITransportSink.OnConnected"/>.
/// <see cref="ITransportSink.OnClosed"/> is raised exactly once, after MsQuic's SHUTDOWN_COMPLETE, and after every stream
/// of the connection reported <see cref="ITransportSink.OnStreamShutdownComplete"/>; nothing follows it. An exception
/// thrown by the sink is caught, recorded in <see cref="LastSinkException"/> and shuts the connection down with
/// <see cref="InternalErrorCode"/> (ADR 0008 section 8).</para>
/// <para><b>Sends.</b> <see cref="SendDatagram"/> and <see cref="SendStream"/> hand the caller's
/// <see cref="TransportSegment"/> array to MsQuic as <c>QUIC_BUFFER*</c> without copying or translating it, and pass the
/// caller's context through as MsQuic's client context. Status mapping: accepted (SUCCESS or PENDING) is
/// <see cref="TransportStatus.Success"/>; <c>INVALID_STATE</c> is <see cref="TransportStatus.InvalidState"/>;
/// <c>INVALID_PARAMETER</c> on a send is <see cref="TransportStatus.TooLarge"/>; <c>OUT_OF_MEMORY</c> is
/// <see cref="TransportStatus.OutOfMemory"/>; <c>STREAM_LIMIT_REACHED</c> is <see cref="TransportStatus.StreamLimitReached"/>;
/// <c>NOT_SUPPORTED</c> is <see cref="TransportStatus.NotSupported"/>; anything else is <see cref="TransportStatus.Failed"/>.</para>
/// <para><b>Streams.</b> Streams live in a fixed-capacity, generation-tagged table (<see cref="MsQuicTransportOptions.MaxStreams"/>).
/// A local stream is started by <see cref="StartStream"/> or by the first send carrying <see cref="TransportSendFlags.Start"/>
/// with <c>FAIL_BLOCKED | SHUTDOWN_ON_FAIL</c>: when the peer's stream limit is exhausted the start fails with
/// <see cref="TransportStatus.StreamLimitReached"/>, synchronously when MsQuic ran the start inline (then no
/// <see cref="ITransportSink.OnStreamStarted"/> follows) or through <see cref="ITransportSink.OnStreamStarted"/> otherwise
/// (a send accepted with the start then completes canceled). Either way MsQuic shuts the stream down, so
/// <see cref="ITransportSink.OnStreamShutdownComplete"/> follows; open a new stream after
/// <see cref="ITransportSink.OnStreamsAvailable"/>.</para>
/// <para><b>Receive.</b> <see cref="ITransportSink.OnStreamReceived"/> sees MsQuic's receive buffers reinterpreted in place.
/// Consuming everything completes the receive; <see cref="ReceiveResult.PendingAfter"/> returns <c>QUIC_STATUS_PENDING</c>
/// and <see cref="ResumeStreamReceive"/> later calls <c>StreamReceiveComplete(consumed)</c>, plus
/// <c>StreamReceiveSetEnabled(TRUE)</c> when bytes remain (MsQuic pauses a stream after a partial completion; measured,
/// see <c>docs/benchmarks/msquic-transport.md</c>). A partial <see cref="ReceiveResult.Consumed"/> of at least one byte
/// re-enables receives inline, so MsQuic indicates the remainder again right away (possibly without new data);
/// consuming nothing of a non-empty indication without <c>Pending</c> stalls the stream until
/// <see cref="ResumeStreamReceive"/>.</para>
/// <para><b>Closing streams.</b> <see cref="CloseStream"/> may be called from inside a callback (the contract calls it from
/// <see cref="ITransportSink.OnStreamShutdownComplete"/>); the native <c>StreamClose</c> is then deferred to a thread-pool
/// work item, never run on the callback thread. Before shutdown completes, <see cref="CloseStream"/> aborts both
/// directions with code 0; pending send completions are still reported (canceled), the shutdown callback is not.</para>
/// <para><b>Lifetime.</b> <see cref="Dispose"/> may be called from any thread. It shuts the connection down if needed; the
/// connection and stream handles are closed once MsQuic reported SHUTDOWN_COMPLETE, on a thread-pool thread when
/// that happens inside a callback. Do not call other members concurrently with or after <see cref="Dispose"/>.</para>
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

    private readonly MsQuicConnection _connection;
    private readonly TransportDiagnosticCallback? _diagnostic;
    private readonly ServerCertificatePolicy? _certificatePolicy;
    private readonly string? _serverName;
    private readonly QUIC_STREAM_SCHEDULING_SCHEME _schedulingScheme;
    private readonly bool _cancelOnBlockedSupported;
    private readonly ManualResetEventSlim _handlesClosed = new(false);
    private ITransportSink? _sink;
    private int _state;
    private int _closedDelivered;
    private int _closeRecorded;
    private TransportCloseReason _closeReason = TransportCloseReason.Transport;
    private ulong _closeErrorCode;
    private int _closeStatus;
    private volatile bool _datagramsEnabled;
    private volatile int _maxDatagramPayload;
    private int _sinkFailed;
    private Exception? _lastSinkException;
    private long _sinkExceptions;
    private long _refusedPeerStreams;
    private int _disposed;
    private volatile bool _shutdownComplete;
    private int _handleClosed;
    private int _workScheduled;
    private QUIC_STATISTICS_V2* _statistics;
    private TransportConnectedInfo _connectedInfo;
    private MsQuicTransportListener.ConfigurationEntry? _configurationLease;

    internal MsQuicTransport(MsQuicConnection connection, ITransportSink? sink, MsQuicTransportOptions options, ServerCertificatePolicy? certificatePolicy, string? serverName)
    {
        _connection = connection;
        _sink = sink;
        _diagnostic = options.Diagnostic;
        _certificatePolicy = certificatePolicy;
        _serverName = serverName;
        _schedulingScheme = options.StreamSchedulingScheme;
        _cancelOnBlockedSupported = (connection.Api.SupportedSendFlags & QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED) != 0;
        _statistics = (QUIC_STATISTICS_V2*)NativeMemory.AllocZeroed((nuint)sizeof(QUIC_STATISTICS_V2));
        InitializeStreams(options.MaxStreams);
        connection.Events = this;
    }

    /// <summary>The underlying wrapper connection (for diagnostics and parameters the contract does not cover).</summary>
    public MsQuicConnection Connection => _connection;

    /// <summary>True for a transport accepted by a listener.</summary>
    public bool IsServer => _connection.IsServer;

    /// <summary>
    /// True when the loaded MsQuic accepts <c>QUIC_SEND_FLAG_CANCEL_ON_BLOCKED</c> (2.4+). When false,
    /// <see cref="TransportSendFlags.CancelOnBlocked"/> is ignored (<see cref="TransportCapabilities"/> has no field for it).
    /// </summary>
    public bool CancelOnBlockedSupported => _cancelOnBlockedSupported;

    /// <summary>The last exception a sink callback threw (then the connection was shut down with <see cref="InternalErrorCode"/>).</summary>
    public Exception? LastSinkException => Volatile.Read(ref _lastSinkException);

    /// <summary>Number of exceptions thrown by sink callbacks.</summary>
    public long SinkExceptionCount => Interlocked.Read(ref _sinkExceptions);

    /// <summary>Peer streams refused because the stream table was full.</summary>
    public long RefusedPeerStreamCount => Interlocked.Read(ref _refusedPeerStreams);

    /// <summary>True once the native connection and stream handles are closed (after <see cref="Dispose"/> and SHUTDOWN_COMPLETE).</summary>
    public bool HandlesClosed => Volatile.Read(ref _handleClosed) != 0;

    /// <summary>Waits until the native handles are closed (see <see cref="HandlesClosed"/>). Never call it from an MsQuic callback.</summary>
    public bool WaitForHandlesClosed(TimeSpan timeout) => _handlesClosed.Wait(timeout);

    /// <summary>Called on a thread-pool thread (or the disposing thread) once the handles are closed.</summary>
    internal Action<MsQuicTransport>? HandlesClosedCallback { get; set; }

    internal MsQuicTransportListener.ConfigurationEntry? ConfigurationLease
    {
        get => Volatile.Read(ref _configurationLease);
        set => Volatile.Write(ref _configurationLease, value);
    }

    internal void SetSink(ITransportSink sink) => Volatile.Write(ref _sink, sink);

    /// <inheritdoc/>
    public TransportCapabilities Capabilities => new()
    {
        Datagrams = _datagramsEnabled,
        DatagramSendState = _datagramsEnabled,
        MaxDatagramPayload = _datagramsEnabled ? _maxDatagramPayload : 0,
        StreamPriority = true,
        AppOwnedReceiveBuffers = false,
        IdealSendBufferSize = true,
    };

    /// <inheritdoc/>
    public TransportState State => (TransportState)Volatile.Read(ref _state);

    /// <inheritdoc/>
    /// <remarks>
    /// Sets <c>QUIC_PARAM_CONN_CLOSE_REASON_PHRASE</c> (NUL-terminated copy) when a reason is given, then
    /// <c>ConnectionShutdown(errorCode)</c>. The peer reports <see cref="TransportCloseReason.Peer"/> with
    /// <paramref name="errorCode"/>; this end reports <see cref="TransportCloseReason.Local"/>. Ignored once closing.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="reason"/> is longer than <see cref="MaxReasonBytes"/>.</exception>
    public void Close(ulong errorCode, ReadOnlySpan<byte> reason)
    {
        if (reason.Length > MaxReasonBytes) throw new ArgumentOutOfRangeException(nameof(reason), reason.Length, $"The close reason is limited to {MaxReasonBytes} bytes.");
        int state = Volatile.Read(ref _state);
        while (state is StateConnecting or StateConnected)
        {
            int seen = Interlocked.CompareExchange(ref _state, StateClosing, state);
            if (seen == state) break;
            state = seen;
        }
        if (state is StateClosing or StateClosed || _connection.IsClosed) return;
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
    /// Reads <c>QUIC_PARAM_CONN_STATISTICS_V2</c> into a native buffer owned by the transport (no allocation).
    /// <see cref="TransportStatistics.BytesInFlight"/> is always 0: MsQuic's stable statistics do not report it.
    /// All fields are 0 once the handles are closed.
    /// </remarks>
    public void GetStatistics(out TransportStatistics statistics)
    {
        statistics = default;
        QUIC_STATISTICS_V2* s = _statistics;
        if (s == null || _connection.IsClosed) return;
        uint length = (uint)sizeof(QUIC_STATISTICS_V2);
        if (MsQuicStatus.Failed(_connection.GetParam(MsQuicParam.QUIC_PARAM_CONN_STATISTICS_V2, &length, s))) return;
        statistics.RttMicros = s->Rtt;
        statistics.MinRttMicros = s->MinRtt;
        statistics.MaxRttMicros = s->MaxRtt;
        statistics.RttVarianceMicros = length >= QUIC_STATISTICS_V2.SIZE_4 ? s->RttVariance : 0;
        statistics.CongestionWindowBytes = s->SendCongestionWindow;
        statistics.PathMtu = s->SendPathMtu;
        statistics.SendTotalBytes = s->SendTotalBytes;
        statistics.RecvTotalBytes = s->RecvTotalBytes;
        statistics.SendTotalPackets = s->SendTotalPackets;
        statistics.RecvTotalPackets = s->RecvTotalPackets;
        statistics.SendSuspectedLostPackets = s->SendSuspectedLostPackets;
        statistics.SendSpuriousLostPackets = s->SendSpuriousLostPackets;
        statistics.CongestionEvents = s->SendCongestionCount;
    }

    /// <summary>
    /// Shuts the connection down (error code 0) if it is still open and releases the native handles once MsQuic has
    /// reported SHUTDOWN_COMPLETE. Safe from any thread, including MsQuic callbacks (the handle close is then deferred).
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        if (!_shutdownComplete) Close(0, default);
        TryReleaseHandles();
    }

    // ------------------------------------------------------------------ close bookkeeping

    private void RecordClose(TransportCloseReason reason, ulong errorCode, int status)
    {
        if (Interlocked.Exchange(ref _closeRecorded, 1) != 0) return;
        _closeReason = reason;
        _closeErrorCode = errorCode;
        _closeStatus = status;
    }

    private void MoveToClosing()
    {
        int state = Volatile.Read(ref _state);
        while (state is StateConnecting or StateConnected)
        {
            int seen = Interlocked.CompareExchange(ref _state, StateClosing, state);
            if (seen == state) return;
            state = seen;
        }
    }

    /// <summary>Records a sink exception and shuts the connection down once with <see cref="InternalErrorCode"/>.</summary>
    private void OnSinkException(Exception exception)
    {
        Volatile.Write(ref _lastSinkException, exception);
        Interlocked.Increment(ref _sinkExceptions);
        MsQuicCallbackScope.OnEscapedException(exception);
        Diagnose(TransportDiagnosticLevel.Error, "An ITransportSink callback threw; the connection is shut down with InternalError.", exception);
        if (Interlocked.Exchange(ref _sinkFailed, 1) != 0 || Volatile.Read(ref _closedDelivered) != 0) return;
        MoveToClosing();
        RecordClose(TransportCloseReason.Local, InternalErrorCode, 0);
        if (!_connection.IsClosed) _connection.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, InternalErrorCode);
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

    /// <summary>The sink when callbacks may still be delivered, else null.</summary>
    private ITransportSink? LiveSink => Volatile.Read(ref _closedDelivered) == 0 ? Volatile.Read(ref _sink) : null;

    // ------------------------------------------------------------------ handle release / cleanup work item

    private void TryReleaseHandles()
    {
        if (Volatile.Read(ref _disposed) == 0 || !_shutdownComplete || Volatile.Read(ref _handleClosed) != 0) return;
        if (MsQuicCallbackScope.IsInsideCallback)
        {
            ScheduleCleanup();
            return;
        }
        ReleaseHandlesNow();
    }

    private void ReleaseHandlesNow()
    {
        if (Interlocked.Exchange(ref _handleClosed, 1) != 0) return;
        DrainDeferredCloses();
        CloseAllStreamHandles();
        _connection.Close();
        QUIC_STATISTICS_V2* s = _statistics;
        _statistics = null;
        NativeMemory.Free(s);
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

    /// <summary>Queues <see cref="IThreadPoolWorkItem.Execute"/> once (no allocation: the transport is the work item).</summary>
    private void ScheduleCleanup()
    {
        if (Interlocked.Exchange(ref _workScheduled, 1) == 0) ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
    }

    void IThreadPoolWorkItem.Execute()
    {
        Volatile.Write(ref _workScheduled, 0);
        DrainDeferredCloses();
        if (Volatile.Read(ref _disposed) != 0 && _shutdownComplete) ReleaseHandlesNow();
    }

    // ------------------------------------------------------------------ connection events (MsQuic worker thread)

    void IMsQuicConnectionEvents.Connected(MsQuicConnection connection, ReadOnlySpan<byte> negotiatedAlpn, bool sessionResumed)
    {
        if (Interlocked.CompareExchange(ref _state, StateConnected, StateConnecting) != StateConnecting) return;
        if (IsServer) ApplySchedulingScheme();
        ReleaseConfigurationLease();
        TransportConnectedInfo info = default;
        info.RemoteEndPoint = _connection.RemoteEndPoint;
        info.LocalEndPoint = _connection.LocalEndPoint;
        int length = Math.Min(negotiatedAlpn.Length, 255);
        negotiatedAlpn[..length].CopyTo(info.Alpn);
        info.AlpnLength = (byte)length;
        info.SessionResumed = sessionResumed;
        info.Capabilities = Capabilities;
        _connectedInfo = info;
        ITransportSink? sink = LiveSink;
        if (sink is null) return;
        try
        {
            sink.OnConnected(in info);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
    }

    internal void ApplySchedulingScheme()
    {
        QUIC_STREAM_SCHEDULING_SCHEME scheme = _schedulingScheme;
        _connection.SetParam(MsQuicParam.QUIC_PARAM_CONN_STREAM_SCHEDULING_SCHEME, in scheme);
    }

    void IMsQuicConnectionEvents.ShutdownInitiatedByTransport(MsQuicConnection connection, int status, ulong errorCode)
    {
        MoveToClosing();
        RecordClose(TransportCloseReason.Transport, errorCode, status);
    }

    void IMsQuicConnectionEvents.ShutdownInitiatedByPeer(MsQuicConnection connection, ulong errorCode)
    {
        MoveToClosing();
        RecordClose(TransportCloseReason.Peer, errorCode, 0);
    }

    void IMsQuicConnectionEvents.ShutdownComplete(MsQuicConnection connection, bool handshakeCompleted, bool peerAcknowledgedShutdown, bool appCloseInProgress)
    {
        MoveToClosing();
        RecordClose(TransportCloseReason.Transport, 0, 0);
        ReleaseConfigurationLease();
        ShutdownRemainingStreams();
        Volatile.Write(ref _state, StateClosed);
        ITransportSink? sink = LiveSink;
        Volatile.Write(ref _closedDelivered, 1);
        if (sink is not null)
        {
            try
            {
                sink.OnClosed(_closeReason, _closeErrorCode, _closeStatus);
            }
            catch (Exception ex)
            {
                Volatile.Write(ref _lastSinkException, ex);
                Interlocked.Increment(ref _sinkExceptions);
                Diagnose(TransportDiagnosticLevel.Error, "ITransportSink.OnClosed threw.", ex);
            }
        }
        _shutdownComplete = true;
        if (Volatile.Read(ref _disposed) != 0) ScheduleCleanup();
        else if (HasDeferredCloses) ScheduleCleanup();
    }

    void IMsQuicConnectionEvents.DatagramStateChanged(MsQuicConnection connection, bool sendEnabled, ushort maxSendLength)
    {
        _maxDatagramPayload = maxSendLength;
        _datagramsEnabled = sendEnabled;
        ITransportSink? sink = LiveSink;
        if (sink is null) return;
        try
        {
            sink.OnDatagramCapabilityChanged(sendEnabled, sendEnabled ? maxSendLength : 0);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
    }

    void IMsQuicConnectionEvents.StreamsAvailable(MsQuicConnection connection, ushort bidirectionalCount, ushort unidirectionalCount)
    {
        ITransportSink? sink = LiveSink;
        if (sink is null) return;
        try
        {
            sink.OnStreamsAvailable(bidirectionalCount, unidirectionalCount);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
    }

    void IMsQuicConnectionEvents.PeerAddressChanged(MsQuicConnection connection, in QUIC_ADDR address)
    {
        TransportConnectedInfo info = _connectedInfo;
        info.RemoteEndPoint = address.ToIPEndPoint();
        info.LocalEndPoint = _connection.LocalEndPoint;
        info.Capabilities = Capabilities;
        _connectedInfo = info;
        ITransportSink? sink = LiveSink;
        if (sink is null) return;
        try
        {
            sink.OnPeerAddressChanged(in info);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
    }

    MsQuicCertificateDecision IMsQuicConnectionEvents.PeerCertificateReceived(MsQuicConnection connection, in MsQuicPeerCertificateInfo info)
        => _certificatePolicy?.Decide(in info, _serverName) ?? MsQuicCertificateDecision.Reject;

    private void ReleaseConfigurationLease() => Interlocked.Exchange(ref _configurationLease, null)?.Release();

    /// <summary>The server end point as seen at connect (allocates; for diagnostics).</summary>
    public IPEndPoint? RemoteEndPoint => _connectedInfo.RemoteEndPoint;
}
