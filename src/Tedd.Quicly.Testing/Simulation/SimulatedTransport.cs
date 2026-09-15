using System.Net;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Testing.Simulation;

/// <summary>
/// One end of a simulated link: an <see cref="ITransport"/> that carries datagrams and streams through a
/// <see cref="SimulatedNetwork"/>. Create pairs with <see cref="SimulatedNetwork.CreatePair"/> or through
/// <see cref="SimulatedConnector"/> / <see cref="SimulatedListener"/>.
/// </summary>
/// <remarks>
/// <para>
/// Every sink callback is raised from inside <see cref="SimulatedNetwork.Advance"/> (never inline in an API call).
/// Payloads are copied when a send is accepted, so callers may release their buffers as soon as the call returns,
/// although the contract only promises that after the completion.
/// </para>
/// <para>
/// Datagrams: accepted sends report <see cref="DatagramSendState.Sent"/> at the next advance step (or when the
/// serializer starts on them, under a bandwidth limit), then exactly one final state: <see cref="DatagramSendState.Acknowledged"/>
/// one one-way delay after delivery, <see cref="DatagramSendState.LostDiscarded"/> one round trip after a loss, or
/// <see cref="DatagramSendState.Canceled"/> on close or when an MTU drop makes a queued datagram too large.
/// </para>
/// <para>
/// Streams: data is cut into packets of the current maximum datagram payload, reassembled in order at the receiver
/// and delivered as one segment per packet; a send completes one one-way delay after all of its bytes (and every
/// byte before them) reached the peer's transport. Flow control is not modelled; receive back-pressure is
/// <see cref="ReceiveResult.PendingAfter"/>. Consuming at least one byte but fewer than delivered, without <c>Pending</c>,
/// keeps the remainder and indicates it again at the next advance step, together with any data that arrived meanwhile;
/// consuming nothing of a non-empty indication counts as <c>PendingAfter(0)</c> (both as the <see cref="ReceiveResult"/>
/// contract states, and as MsQuicTransport behaves). Aborting a local stream that was never started releases it like
/// <see cref="ITransport.CloseStream"/>.
/// <see cref="ITransportSink.OnStreamAborted"/> reports the direction the peer aborted:
/// <see cref="StreamAbortDirection.Send"/> (reset: our receive side is dead) or <see cref="StreamAbortDirection.Receive"/>
/// (stop-sending: our pending sends are canceled). Aborts are causal: the peer keeps delivering data it already holds
/// until the reset arrives one one-way delay later. Peer stream limits count concurrently open streams, like MsQuic: a
/// stream's credit returns to its opener when the peer's side has shut down. A start beyond the limit is refused as MsQuic
/// refuses it (asynchronously): the call returns Success, then OnStreamStarted(StreamLimitReached), a canceled completion
/// for a send made with the start and OnStreamShutdownComplete follow; the refused stream never starts. A lower
/// <see cref="ITransport.UpdatePeerStreamLimits"/> is ignored once the old limit can have reached the peer.
/// </para>
/// <para>
/// Fault injection: <see cref="DropNextDatagrams"/> and <see cref="LoseNextStreamPackets"/> hit specific packets without
/// seed hunting. Zero-allocation measurements need a warm-up that reaches the run's peak of concurrent events, streams
/// and sends first (the tables grow to it and then stay), which can take many thousands of operations under jitter.
/// </para>
/// <para>
/// <see cref="ITransport.CloseStream"/> before <see cref="ITransportSink.OnStreamShutdownComplete"/> aborts both
/// directions with error code 0; pending send completions are still reported (canceled), the shutdown callback is not.
/// </para>
/// </remarks>
public sealed unsafe partial class SimulatedTransport : ITransport
{
    /// <summary><c>transportStatus</c> of <see cref="ITransportSink.OnClosed"/> when the link was cut (<see cref="LinkOptions.DisconnectAtMicros"/>).</summary>
    public const int StatusDisconnected = 1;

    /// <summary><c>transportStatus</c> of <see cref="ITransportSink.OnClosed"/> when the listener rejected the connection.</summary>
    public const int StatusConnectionRefused = 2;

    /// <summary><c>transportStatus</c> of <see cref="ITransportSink.OnClosed"/> when no listener was started at the endpoint.</summary>
    public const int StatusUnreachable = 3;

    /// <summary><c>transportStatus</c> of <see cref="ITransportSink.OnClosed"/> when the handshake failed after the accept (<see cref="LinkOptions.FailHandshake"/>).</summary>
    public const int StatusHandshakeFailed = 4;

    /// <summary>Bytes added to the maximum datagram payload to report <see cref="TransportStatistics.PathMtu"/>.</summary>
    public const int PathOverheadBytes = 52;

    /// <summary>Longest close reason accepted by <see cref="Close"/>, in bytes.</summary>
    public const int MaxReasonBytes = 512;

    private readonly SimulatedNetwork _network;
    internal readonly SimulatedLink Link;
    internal SimulatedTransport? Peer;
    internal ITransportSink? Sink;
    private TransportState _state;
    private bool _closedDelivered;
    private bool _disposed;
    private byte[]? _peerCloseReason;
    private ulong _sendBytes;
    private ulong _recvBytes;
    private ulong _sendPackets;
    private ulong _recvPackets;
    private ulong _suspectedLost;
    private long _bytesInFlight;
    private int _forcedDatagramLosses;
    private int _forcedStreamLosses;
    internal SimulatedLinkStatistics LinkStats;

    internal SimulatedTransport(SimulatedNetwork network, SimulatedLink link, bool isClient, ITransportSink? sink, IPEndPoint localEndPoint)
    {
        _network = network;
        Link = link;
        IsClient = isClient;
        Sink = sink;
        LocalEndPoint = localEndPoint;
        AllowPeerBidi = link.Options.PeerBidiStreams;
        AllowPeerUni = link.Options.PeerUnidiStreams;
    }

    /// <summary>True for the end that initiated the connection (client-initiated QUIC stream ids).</summary>
    public bool IsClient { get; }

    /// <summary>Synthetic local address of this end.</summary>
    public IPEndPoint LocalEndPoint { get; }

    /// <summary>Address of the peer (the endpoint connected to, for a client); <c>null</c> when unknown.</summary>
    public EndPoint? RemoteEndPoint { get; internal set; }

    /// <summary>The network this transport belongs to.</summary>
    public SimulatedNetwork Network => _network;

    /// <summary>The reason phrase the peer passed to <see cref="Close"/>, once its close has arrived; empty otherwise.</summary>
    public ReadOnlyMemory<byte> PeerCloseReason
    {
        get
        {
            lock (_network.Gate)
                return _peerCloseReason;
        }
    }

    /// <inheritdoc/>
    public TransportCapabilities Capabilities
    {
        get
        {
            lock (_network.Gate)
                return BuildCapabilities();
        }
    }

    /// <inheritdoc/>
    public TransportState State
    {
        get
        {
            lock (_network.Gate)
                return _state;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// RTT is 2 × <see cref="LinkOptions.DelayMicros"/> (maximum adds twice the jitter, variance is the jitter); the
    /// congestion window is the bandwidth-delay product (at least two packets) under a bandwidth limit, else 16 MiB.
    /// </remarks>
    public void GetStatistics(out TransportStatistics statistics)
    {
        lock (_network.Gate)
        {
            LinkOptions o = Link.Options;
            statistics = default;
            statistics.RttMicros = ClampU32(2 * o.DelayMicros);
            statistics.MinRttMicros = statistics.RttMicros;
            statistics.MaxRttMicros = ClampU32(2 * (o.DelayMicros + o.JitterMicros));
            statistics.RttVarianceMicros = ClampU32(o.JitterMicros);
            statistics.CongestionWindowBytes = o.BandwidthBitsPerSecond == 0
                ? 16u << 20
                : ClampU32(Math.Max(2L * Link.MaxPayload, (long)(o.BandwidthBitsPerSecond / 8.0 * Math.Max(2 * o.DelayMicros, 1000) / 1_000_000)));
            statistics.BytesInFlight = (ulong)_bytesInFlight;
            statistics.PathMtu = (ushort)Math.Min(ushort.MaxValue, Link.MaxPayload + PathOverheadBytes);
            statistics.SendTotalBytes = _sendBytes;
            statistics.RecvTotalBytes = _recvBytes;
            statistics.SendTotalPackets = _sendPackets;
            statistics.RecvTotalPackets = _recvPackets;
            statistics.SendSuspectedLostPackets = _suspectedLost;
        }
    }

    /// <summary>Copies the counters of the direction from this end to its peer.</summary>
    public void GetLinkStatistics(out SimulatedLinkStatistics statistics)
    {
        lock (_network.Gate)
            statistics = LinkStats;
    }

    /// <summary>
    /// Targeted loss: the next <paramref name="count"/> datagrams that leave this end (in departure order, that is when
    /// the serializer finishes them under a bandwidth limit) are lost in flight and report
    /// <see cref="DatagramSendState.LostDiscarded"/>, whatever <see cref="LinkOptions.LossPercent"/> says. Datagrams
    /// dropped by a full queue or canceled do not count. Calls accumulate.
    /// </summary>
    /// <param name="count">Number of datagrams to drop; non-negative.</param>
    public void DropNextDatagrams(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        lock (_network.Gate)
            _forcedDatagramLosses = checked(_forcedDatagramLosses + count);
    }

    /// <summary>
    /// Targeted stream loss: each of the next <paramref name="count"/> stream packets that leave this end is lost once
    /// (on top of any random <see cref="LinkOptions.StreamLossPercent"/> losses), so it arrives one
    /// <see cref="LinkOptions.RetransmitDelayMicros"/> late and later packets overtake it. Stream data is never lost.
    /// Calls accumulate.
    /// </summary>
    /// <param name="count">Number of stream packets to delay by a retransmission; non-negative.</param>
    public void LoseNextStreamPackets(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        lock (_network.Gate)
            _forcedStreamLosses = checked(_forcedStreamLosses + count);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The next advance step completes every in-flight send of this end as canceled, raises
    /// <see cref="ITransportSink.OnStreamShutdownComplete"/> for every stream not yet shut down and then
    /// <see cref="ITransportSink.OnClosed"/> with <see cref="TransportCloseReason.Local"/>. The peer does the same one
    /// one-way delay later with <see cref="TransportCloseReason.Peer"/> and <paramref name="errorCode"/>. Further calls are ignored.
    /// As in QUIC, stream data and datagrams that already left this end still reach the peer if they arrive before the
    /// close does (they are reported canceled here all the same); data still queued behind a bandwidth limit is lost.
    /// A link cut (<see cref="LinkOptions.DisconnectAtMicros"/>) loses everything in flight.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="reason"/> is longer than <see cref="MaxReasonBytes"/>.</exception>
    public void Close(ulong errorCode, ReadOnlySpan<byte> reason)
    {
        if (reason.Length > MaxReasonBytes)
            throw new ArgumentOutOfRangeException(nameof(reason), reason.Length, $"The close reason is limited to {MaxReasonBytes} bytes.");
        lock (_network.Gate)
        {
            if (_state is TransportState.Closing or TransportState.Closed)
                return;
            _state = TransportState.Closing;
            long now = _network.NowMicros;
            Post(SimEventKind.CloseLocal, now, this, u0: errorCode);
            SimulatedTransport? peer = Peer;
            if (peer?.Sink is not null)
                Post(SimEventKind.ClosePeer, now + Link.Options.DelayMicros, peer, u0: errorCode, obj: reason.ToArray());
        }
    }

    /// <summary>Closes the connection with error code 0 if it is still open (callbacks still follow from <c>Advance</c>).</summary>
    public void Dispose()
    {
        lock (_network.Gate)
        {
            if (_disposed)
                return;
            _disposed = true;
        }
        Close(0, default);
    }

    internal void ScheduleConnect(long capabilityAt, long connectAt)
    {
        ScheduleCapability(capabilityAt);
        ScheduleConnected(connectAt);
    }

    internal void ScheduleCapability(long at) => Post(SimEventKind.CapabilityChanged, at, this);

    internal void ScheduleConnected(long at) => Post(SimEventKind.Connected, at, this);

    internal void Dispatch(ref SimEvent e)
    {
        if (_closedDelivered || Sink is null)
            return;
        switch (e.Kind)
        {
            case SimEventKind.CapabilityChanged:
                if (_state is TransportState.Connecting or TransportState.Connected)
                    Sink.OnDatagramCapabilityChanged(Link.Options.DatagramsEnabled, Link.MaxPayload);
                break;
            case SimEventKind.Connected:
                OnConnectedEvent();
                break;
            case SimEventKind.ConnectFailed:
                if (_state == TransportState.Connecting)
                {
                    _state = TransportState.Closing;
                    FinishClose(TransportCloseReason.Transport, 0, e.I0);
                }
                break;
            case SimEventKind.CloseLocal:
                FinishClose(TransportCloseReason.Local, e.U0, 0);
                break;
            case SimEventKind.ClosePeer:
                if (_state is TransportState.Connecting or TransportState.Connected)
                {
                    _state = TransportState.Closing;
                    _peerCloseReason = (byte[]?)e.Obj;
                    FinishClose(TransportCloseReason.Peer, e.U0, 0);
                }
                break;
            case SimEventKind.DatagramSent:
                OnDatagramSentEvent(ref e);
                break;
            case SimEventKind.DatagramArrive:
                OnDatagramArriveEvent(ref e);
                break;
            case SimEventKind.DatagramFinal:
                OnDatagramFinalEvent(ref e);
                break;
            case SimEventKind.TxDone:
                OnTxDoneEvent();
                break;
            default:
                DispatchStreamEvent(ref e);
                break;
        }
    }

    private void OnConnectedEvent()
    {
        if (_state != TransportState.Connecting)
            return;
        _state = TransportState.Connected;
        if (Peer is not null)
        {
            _peerAllowsBidi = Peer.AllowPeerBidi;
            _peerAllowsUni = Peer.AllowPeerUni;
        }
        TransportConnectedInfo info = default;
        info.RemoteEndPoint = RemoteEndPoint as IPEndPoint;
        info.LocalEndPoint = LocalEndPoint;
        Span<byte> alpn = info.Alpn;
        int alpnLength = Math.Min(Link.Alpn.Length, alpn.Length);
        Link.Alpn.AsSpan(0, alpnLength).CopyTo(alpn);
        info.AlpnLength = (byte)alpnLength;
        info.Capabilities = BuildCapabilities();
        Sink!.OnConnected(in info);
    }

    internal void OnDisconnected()
    {
        if (_closedDelivered || Sink is null || _state is TransportState.Closing or TransportState.Closed)
            return;
        _state = TransportState.Closing;
        FinishClose(TransportCloseReason.Transport, 0, StatusDisconnected);
    }

    internal bool IsConnectingState => _state == TransportState.Connecting;

    internal void ScheduleConnectFailed(long due, int transportStatus) => Post(SimEventKind.ConnectFailed, due, this, i0: transportStatus);

    internal void MarkRejected()
    {
        _state = TransportState.Closed;
        _closedDelivered = true;
    }

    private void FinishClose(TransportCloseReason reason, ulong errorCode, int transportStatus)
    {
        ITransportSink sink = Sink!;
        _scratch.Clear();
        _tx.Clear(_scratch);
        _scratch.Clear();
        _txBusy = false;
        // Packets already on the wire still reach a peer that has not closed (as in QUIC, where packets sent before
        // CONNECTION_CLOSE are processed); only what has not left this end is lost.
        bool keepForPeer = reason != TransportCloseReason.Transport
            && Peer is { _closedDelivered: false, _state: TransportState.Connecting or TransportState.Connected };
        CancelAllStreamSends(keepForPeer);
        CancelAllDatagrams(keepForPeer);
        ShutdownAllStreams();
        _state = TransportState.Closed;
        _closedDelivered = true;
        Peer?.ReleaseOrphans(); // this end receives nothing any more
        sink.OnClosed(reason, errorCode, transportStatus);
    }

    private TransportCapabilities BuildCapabilities()
    {
        LinkOptions o = Link.Options;
        return new TransportCapabilities
        {
            Datagrams = o.DatagramsEnabled,
            DatagramSendState = o.DatagramsEnabled && o.DatagramSendStateReporting,
            MaxDatagramPayload = Link.MaxPayload,
            StreamPriority = true,
            CancelOnBlocked = true,
        };
    }

    private void Post(SimEventKind kind, long due, SimulatedTransport target, int i0 = 0, uint g0 = 0, int i1 = 0, uint g1 = 0,
        long l0 = 0, long l1 = 0, ulong u0 = 0, byte b0 = 0, object? obj = null)
    {
        SimEvent e = new()
        {
            Kind = kind, Due = due, Target = target, I0 = i0, G0 = g0, I1 = i1, G1 = g1, L0 = l0, L1 = l1, U0 = u0, B0 = b0, Obj = obj,
        };
        _network.Schedule(ref e);
    }

    private static long SumSegments(TransportSegment* segments, int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count > 0 && segments == null)
            throw new ArgumentNullException(nameof(segments));
        long total = 0;
        for (int i = 0; i < count; i++)
            total += segments[i].Length;
        return total;
    }

    private static void CopySegments(TransportSegment* segments, int count, byte[] destination)
    {
        int offset = 0;
        for (int i = 0; i < count; i++)
        {
            int length = (int)segments[i].Length;
            if (length == 0)
                continue;
            new ReadOnlySpan<byte>(segments[i].Buffer, length).CopyTo(destination.AsSpan(offset));
            offset += length;
        }
    }

    private static uint ClampU32(long value) => (uint)Math.Clamp(value, 0, uint.MaxValue);
}
