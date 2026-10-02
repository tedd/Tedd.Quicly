using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Http3;
using Tedd.Quicly.Http3.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.WebTransport;

/// <summary>
/// <see cref="ITransport"/> carried inside one WebTransport session over HTTP/3 (ALPN <c>h3</c>, PROTOCOL.md §5). It
/// wraps an inner raw-QUIC <see cref="ITransport"/>, is its <see cref="ITransportSink"/>, and presents the same
/// <see cref="ITransport"/> contract to Core: the WebTransport prefixes are added on the way out and stripped on the way
/// in, so the two carriers are byte-identical at the QUICLY layer.
/// </summary>
/// <remarks>
/// <para><b>Session establishment.</b> Once the inner connection is up, both ends open the HTTP/3 control stream and send
/// SETTINGS, plus the two QPACK streams (the dynamic table is disabled, so they stay empty). The client opens a
/// bidirectional stream carrying an Extended CONNECT request (<c>:protocol webtransport</c>); the server validates it
/// against <see cref="WebTransportOptions.Path"/> and <see cref="WebTransportOptions.OriginPolicy"/> and answers
/// <c>:status 200</c>. That stream's QUIC id is the session id and it stays open for the session.
/// <see cref="ITransportSink.OnConnected"/> is raised only once the session exists, so Core never sees the HTTP/3
/// handshake.</para>
/// <para><b>Data.</b> Datagrams carry the RFC 9297 quarter-stream-id prefix, which the carrier prepends as one extra
/// gather segment (the caller's buffers are never copied) and which shrinks
/// <see cref="TransportCapabilities.MaxDatagramPayload"/>. Streams carry the draft-ietf-webtrans-http3 §4.2 preamble,
/// which the carrier sends as its own first send on the stream and strips from the first receive; the
/// <c>absoluteOffset</c> Core sees counts from after the preamble.</para>
/// <para><b>Identity.</b> The carrier mirrors the inner transport's stream slots, so a <see cref="TransportStreamId"/>
/// means the same thing on both sides of the wrapper and no lookup table is needed. The mirror follows the slots the
/// inner transport uses: it starts small and grows on demand, so it needs no size of its own.</para>
/// <para><b>Threading.</b> As for any transport: <see cref="ITransport"/> members run on the caller's thread, sink
/// callbacks on the inner transport's threads, serialised per connection.</para>
/// </remarks>
public sealed unsafe partial class WebTransportTransport : ITransport, ITransportSink
{
    private const int StateConnecting = 0;
    private const int StateEstablished = 1;
    private const int StateClosing = 2;
    private const int StateClosed = 3;

    /// <summary>Largest preamble the carrier writes per stream: two varints, each at most 8 bytes.</summary>
    private const int PreambleCapacity = 16;

    /// <summary>Largest datagram prefix: one varint of the quarter stream id.</summary>
    private const int DatagramPrefixCapacity = 8;

    /// <summary>The reason phrase limit every carrier shares (<see cref="ITransport.Close"/>).</summary>
    private const int MaxCloseReasonLength = 512;

    private const int ControlArenaSize = 1024;
    private const int ControlSegmentCount = 8;
    private const int SegControl = 0;
    private const int SegQpackEncoder = 1;
    private const int SegQpackDecoder = 2;
    private const int SegConnect = 3;
    private const int SegCapsule = 4;

    private readonly WebTransportOptions _options;
    private readonly bool _isClient;
    private readonly Lock _gate = new();

    private ITransport? _inner;
    private ITransportSink? _sink;

    private int _state = StateConnecting;
    private int _disposed;

    /// <summary>The CONNECT stream's QUIC id, valid once <see cref="_sessionIdKnown"/> is set.</summary>
    private ulong _sessionId;
    private volatile bool _sessionIdKnown;
    private TransportStreamId _connectStream;

    /// <summary>Length of the datagram prefix for <see cref="_sessionId"/>, cached once the session id is known.</summary>
    private int _datagramPrefixLength;

    private Http3Settings _peerSettings;
    private volatile bool _peerSettingsReceived;
    private volatile bool _connectSettled;
    private volatile bool _connectedRaised;

    private bool _innerConnected;
    private bool _controlStreamsOpened;
    private TransportConnectedInfo _connectedInfo;

    private bool _datagramsEnabled;
    private int _innerMaxDatagramPayload;
    private bool _datagramCapabilityRaised;

    private int _extraRequestStreams;

    /// <summary>The close code the peer sent in a CLOSE_WEBTRANSPORT_SESSION capsule, or -1.</summary>
    private long _peerCapsuleCode = -1;

    /// <summary>
    /// The peer ended the session — a CLOSE_WEBTRANSPORT_SESSION capsule, a finished CONNECT stream or GOAWAY. All
    /// three make this end close the inner connection, so the close arrives as <see cref="TransportCloseReason.Local"/>
    /// and only this says otherwise.
    /// </summary>
    private bool _peerEndedSession;

    /// <summary>The inner connection reported its close, so no callback and no send of its can touch this any more.</summary>
    private int _innerClosedSeen;

    /// <summary>The native arenas have been freed, exactly once.</summary>
    private int _nativeFreed;

    /// <summary>Whether the carrier's native arenas have been released (tests: the deferred free must not be lost).</summary>
    internal bool NativeMemoryReleased => Volatile.Read(ref _nativeFreed) != 0;

    private readonly NativeArray<byte> _controlBytes;
    private readonly NativeArray<TransportSegment> _controlSegments;
    private int _controlUsed;

    /// <summary>Creates a carrier. The inner transport is attached with <see cref="AttachInner"/> before any callback runs.</summary>
    /// <param name="options">Carrier options; used as given (the connector and listener pass a validated copy).</param>
    /// <param name="isClient">True for the connecting end (it sends the Extended CONNECT request).</param>
    /// <param name="sink">Core's sink, or null when the listener sets it with <see cref="SetSink"/> after its accept callback.</param>
    public WebTransportTransport(WebTransportOptions options, bool isClient, ITransportSink? sink = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _isClient = isClient;
        _sink = sink;
        _controlBytes = new NativeArray<byte>(ControlArenaSize);
        _controlSegments = new NativeArray<TransportSegment>(ControlSegmentCount);
        InitialiseStreams(options);
        InitialiseDatagrams(options);
    }

    /// <summary>The carrier's own lock. The connector holds it while it creates and attaches the inner transport.</summary>
    internal Lock Gate => _gate;

    /// <summary>The inner raw-QUIC transport, or null before it is attached.</summary>
    public ITransport? Inner => _inner;

    /// <summary>The session id (the CONNECT stream's QUIC id), or <see cref="ulong.MaxValue"/> before it is known.</summary>
    public ulong SessionId => _sessionIdKnown ? _sessionId : ulong.MaxValue;

    /// <summary>The peer's HTTP/3 SETTINGS, valid once they arrived.</summary>
    public Http3Settings PeerSettings => _peerSettings;

    /// <summary>True once the WebTransport session is established (Core has seen <see cref="ITransportSink.OnConnected"/>).</summary>
    public bool IsSessionEstablished => Volatile.Read(ref _state) == StateEstablished;

    /// <summary>Attaches the inner transport. Called once, before any of its callbacks may run.</summary>
    /// <param name="inner">The raw-QUIC transport this carrier runs over.</param>
    /// <exception cref="InvalidOperationException">An inner transport is already attached.</exception>
    public void AttachInner(ITransport inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        lock (_gate)
        {
            if (_inner is not null) throw new InvalidOperationException("The inner transport is already attached.");
            _inner = inner;
        }
    }

    /// <summary>Sets Core's sink. Called once, before the inner transport's callbacks may run.</summary>
    /// <param name="sink">The sink that receives the carrier's callbacks.</param>
    /// <exception cref="InvalidOperationException">A sink is already set.</exception>
    public void SetSink(ITransportSink sink)
    {
        ArgumentNullException.ThrowIfNull(sink);
        lock (_gate)
        {
            if (_sink is not null) throw new InvalidOperationException("The sink is already set.");
            _sink = sink;
        }
    }

    /// <inheritdoc/>
    public TransportCapabilities Capabilities
    {
        get
        {
            ITransport? inner = _inner;
            if (inner is null) return default;
            TransportCapabilities caps = inner.Capabilities;
            caps.MaxDatagramPayload = ReducedDatagramPayload(caps.MaxDatagramPayload);
            caps.PeerUnidirectionalStreams = SessionPeerUnidiStreams(caps.PeerUnidirectionalStreams);
            return caps;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// <see cref="TransportState.Connecting"/> covers both the QUIC handshake and the HTTP/3 session establishment:
    /// the carrier is <see cref="TransportState.Connected"/> only once the session exists.
    /// </remarks>
    public TransportState State => Volatile.Read(ref _state) switch
    {
        StateConnecting => TransportState.Connecting,
        StateEstablished => TransportState.Connected,
        StateClosing => TransportState.Closing,
        _ => TransportState.Closed,
    };

    /// <inheritdoc/>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="reason"/> is longer than 512 bytes.</exception>
    public void Close(ulong errorCode, ReadOnlySpan<byte> reason)
    {
        // Checked before anything moves, so a refused close leaves the carrier exactly as it was.
        ArgumentOutOfRangeException.ThrowIfGreaterThan(reason.Length, MaxCloseReasonLength, nameof(reason));

        ITransport? inner = _inner;
        if (inner is null) return;
        bool established;
        lock (_gate)
        {
            established = _state == StateEstablished;
            if (_state is StateClosing or StateClosed) return;
            _state = StateClosing;
        }

        // Both signals carry the application code: the capsule is the specified one and survives a graceful close, the
        // mapped HTTP/3 code survives a capsule that never made it out. Whichever the peer sees first wins.
        if (established) TrySendCloseCapsule(errorCode, reason);
        inner.Close(WebTransportErrorCode.ToHttp3(ToApplicationCode(errorCode)), reason);
    }

    /// <inheritdoc/>
    public void GetStatistics(out TransportStatistics statistics)
    {
        ITransport? inner = _inner;
        if (inner is null)
        {
            statistics = default;
            return;
        }

        inner.GetStatistics(out statistics);
    }

    /// <summary>Closes the session and releases the carrier's buffers. The inner transport is disposed too.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        ITransport? inner = _inner;
        inner?.Dispose();
        lock (_gate)
        {
            _state = StateClosed;
        }

        // Without an inner connection nothing can be holding the buffers and no close will ever be reported.
        if (inner is null) Volatile.Write(ref _innerClosedSeen, 1);
        TryFreeNative();
    }

    /// <summary>
    /// Frees the carrier's native memory once nothing can be using it: this end has been disposed and the inner
    /// connection has reported its close. Whichever happens last does the freeing, exactly once.
    /// </summary>
    /// <remarks>
    /// It cannot be done in <see cref="Dispose"/>. The inner transport's own Dispose only asks the connection to close
    /// and returns; the handles go later, when MsQuic raises SHUTDOWN_COMPLETE. Until then MsQuic may still be sending
    /// from these buffers — the preamble bytes, the control arena, the datagram arena are all handed to it by pointer —
    /// and may still raise callbacks into this carrier. <see cref="ITransportSink.OnClosed"/> is the point where that
    /// stops: "no further callbacks follow", and before it "every accepted send has completed".
    /// </remarks>
    private void TryFreeNative()
    {
        if (Volatile.Read(ref _disposed) == 0 || Volatile.Read(ref _innerClosedSeen) == 0) return;
        if (Interlocked.Exchange(ref _nativeFreed, 1) != 0) return;

        DisposeStreams();
        DisposeDatagrams();
        _controlSegments.Dispose();
        _controlBytes.Dispose();
    }

    // ---------------------------------------------------------------- inner connection callbacks

    /// <inheritdoc/>
    void ITransportSink.OnConnected(in TransportConnectedInfo info)
    {
        lock (_gate)
        {
            if (_innerConnected || _state is StateClosing or StateClosed) return;
            _innerConnected = true;
            _connectedInfo = info;
        }

        OpenHttp3ControlStreams();
    }

    /// <inheritdoc/>
    void ITransportSink.OnDatagramCapabilityChanged(bool enabled, int maxPayload)
    {
        bool raise;
        lock (_gate)
        {
            _datagramsEnabled = enabled;
            _innerMaxDatagramPayload = maxPayload;
            // Before the session id is known the prefix length is not, so the reduced payload would be a guess.
            raise = _sessionIdKnown;
            if (raise) _datagramCapabilityRaised = true;
        }

        if (raise) _sink?.OnDatagramCapabilityChanged(enabled, ReducedDatagramPayload(maxPayload));
    }

    /// <inheritdoc/>
    void ITransportSink.OnPeerAddressChanged(in TransportConnectedInfo info)
    {
        if (!_connectedRaised) return;
        TransportConnectedInfo adjusted = info;
        adjusted.Capabilities.MaxDatagramPayload = ReducedDatagramPayload(info.Capabilities.MaxDatagramPayload);
        _sink?.OnPeerAddressChanged(in adjusted);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The counts are credit the inner transport has left, and the carrier's own HTTP/3 streams are open streams of that
    /// transport, so they are already accounted for: the numbers pass through unchanged.
    /// </remarks>
    void ITransportSink.OnStreamsAvailable(ushort bidirectional, ushort unidirectional)
    {
        if (!_connectedRaised) return;
        _sink?.OnStreamsAvailable(bidirectional, unidirectional);
    }

    /// <inheritdoc/>
    void ITransportSink.OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
    {
        try
        {
            ReportClosed(reason, errorCode, transportStatus);
        }
        finally
        {
            // Outside ReportClosed's own early returns, and outside its state guard: a Dispose that ran first already
            // set the state to Closed, and this is still the moment the inner connection stops touching the arenas.
            Volatile.Write(ref _innerClosedSeen, 1);
            TryFreeNative();
        }
    }

    private void ReportClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
    {
        ITransportSink? sink;
        long capsuleCode;
        bool peerEnded;
        lock (_gate)
        {
            if (_state == StateClosed) return;
            _state = StateClosed;
            sink = _sink;
            capsuleCode = _peerCapsuleCode;
            peerEnded = _peerEndedSession;
        }

        FailPendingDatagrams();
        if (sink is null) return;

        // A session the peer closed shows up either as the capsule it sent on the CONNECT stream or as the mapped
        // HTTP/3 connection error; both carry the QUICLY application code.
        if (reason == TransportCloseReason.Peer)
        {
            if (capsuleCode >= 0) sink.OnClosed(TransportCloseReason.Peer, (ulong)capsuleCode, transportStatus);
            else if (WebTransportErrorCode.TryFromHttp3(errorCode, out uint application)) sink.OnClosed(TransportCloseReason.Peer, application, transportStatus);
            else sink.OnClosed(TransportCloseReason.Transport, errorCode, transportStatus);
            return;
        }

        if (reason == TransportCloseReason.Local && WebTransportErrorCode.TryFromHttp3(errorCode, out uint local) && !peerEnded)
        {
            sink.OnClosed(TransportCloseReason.Local, local, transportStatus);
            return;
        }

        // A capsule that arrived before the connection went down still tells us the peer's intent.
        if (capsuleCode >= 0)
        {
            sink.OnClosed(TransportCloseReason.Peer, (ulong)capsuleCode, transportStatus);
            return;
        }

        // The peer finished the CONNECT stream or sent GOAWAY: it ended the session without naming a code. This end
        // closed the inner connection in response, with H3_NO_ERROR, so the reason says Local and the code is an HTTP/3
        // one -- neither of which is the application's business. It ended cleanly, at the peer's request.
        if (peerEnded)
        {
            sink.OnClosed(TransportCloseReason.Peer, 0, transportStatus);
            return;
        }

        sink.OnClosed(reason, errorCode, transportStatus);
    }

    // ---------------------------------------------------------------- helpers

    /// <summary>The datagram payload Core may use: the inner transport's minus the HTTP datagram prefix.</summary>
    private int ReducedDatagramPayload(int innerPayload)
    {
        if (innerPayload <= 0) return 0;
        int prefix = _sessionIdKnown ? _datagramPrefixLength : DatagramPrefixCapacity;
        int reduced = innerPayload - prefix;
        return reduced > 0 ? reduced : 0;
    }

    /// <summary>QUICLY application error codes are small; anything larger is reported as an internal error.</summary>
    private static uint ToApplicationCode(ulong errorCode) => errorCode <= uint.MaxValue ? (uint)errorCode : (uint)QuiclyInternalError;

    private const ulong QuiclyInternalError = 0x07;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ITransport? ConnectedInner() => Volatile.Read(ref _state) == StateEstablished ? _inner : null;

    private void Diagnostic(TransportDiagnosticLevel level, string message, Exception? exception = null)
    {
        try
        {
            _options.Diagnostic?.Invoke(level, message, exception);
        }
        catch
        {
            // A diagnostics sink must not break the connection.
        }
    }
}
