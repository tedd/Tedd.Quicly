using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Http3;
using Tedd.Quicly.Http3.WebTransport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;

/// <summary>
/// Nothing of a session reaches the application before the session exists. <see cref="ITransportSink"/> promises
/// <see cref="ITransportSink.OnConnected"/> first, and a carrier that hands over peer streams or datagrams while it is
/// still <see cref="TransportState.Connecting"/> breaks that promise: the session's id is known as soon as the CONNECT
/// request is seen, which is well before the peer's SETTINGS have arrived and the session has been established.
/// </summary>
public sealed unsafe class WebTransportEstablishmentOrderTests : IDisposable
{
    private readonly VirtualClock _clock = new();
    private readonly SimulatedNetwork _network;
    private readonly SimulatedListener _listener;
    private readonly RawHttp3Peer _peer = new();
    private readonly RecordingSink _clientSink;
    private WebTransportConnector? _connector;
    private ITransport? _client;

    public WebTransportEstablishmentOrderTests()
    {
        _network = new SimulatedNetwork(_clock, seed: 1);
        _clientSink = new RecordingSink(_clock);
        _listener = new SimulatedListener(_network, new IPEndPoint(IPAddress.Loopback, 0));
    }

    private void Start()
    {
        _listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            _peer.Transport = transport;
            transport.UpdatePeerStreamLimits(8, 16);
            return _peer;
        });

        _connector = new WebTransportConnector(new SimulatedConnector(_network, new LinkOptions { DelayMicros = 2_000 }), null);
        _client = _connector.Connect(_listener.LocalEndPoint, "localhost", _clientSink);
        _client.UpdatePeerStreamLimits(8, 16);
    }

    private bool Pump(Func<bool> condition, int millis = 5_000)
    {
        long deadline = _network.NowMicros + (millis * 1000L);
        while (!condition())
        {
            if (_network.NowMicros >= deadline) return false;
            _network.Advance(250);
        }

        return true;
    }

    /// <summary>A unidirectional WebTransport stream naming <paramref name="sessionId"/>, with a byte of payload.</summary>
    private static byte[] DataStream(ulong sessionId, bool bidirectional)
    {
        byte[] bytes = new byte[WebTransportFraming.GetPreambleLength(sessionId, bidirectional) + 1];
        int written = bidirectional
            ? WebTransportFraming.WriteBidirectionalPreamble(bytes, sessionId)
            : WebTransportFraming.WriteUnidirectionalPreamble(bytes, sessionId);
        Assert.True(written > 0, "the preamble did not fit");
        bytes[written] = 0x5A;
        return bytes;
    }

    /// <summary>
    /// The peer answers CONNECT but never sends SETTINGS, so the session never establishes — and then opens
    /// WebTransport data streams for it. They must not be handed to the application.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_peer_stream_before_the_session_is_established_is_not_handed_over(bool bidirectional)
    {
        _peer.SendSettings = false;
        Start();
        Assert.True(Pump(() => _peer.ConnectAnswered && _peer.SessionId != ulong.MaxValue), "the Extended CONNECT answer");
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.Connected));

        byte[] bytes = DataStream(_peer.SessionId, bidirectional);
        if (bidirectional)
        {
            _peer.OpenBidirectional(bytes);
        }
        else
        {
            _peer.OpenUnidirectional(bytes);
        }

        Pump(static () => false, 500);

        Assert.Equal(TransportState.Connecting, _client!.State);
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.Connected));
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.PeerStreamStarted));
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.StreamReceived));
    }

    /// <summary>
    /// The other half of the same rule: a stream the peer opened optimistically is held, not lost. Once the session
    /// establishes it is handed over — after OnConnected, with its payload intact — because a browser may open a stream
    /// as soon as it has sent CONNECT and dropping it would be a real bug of its own.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_stream_opened_before_the_session_established_is_delivered_after_it(bool bidirectional)
    {
        _peer.SendSettings = false;
        Start();
        Assert.True(Pump(() => _peer.ConnectAnswered && _peer.SessionId != ulong.MaxValue), "the Extended CONNECT answer");

        byte[] bytes = DataStream(_peer.SessionId, bidirectional);
        if (bidirectional)
        {
            _peer.OpenBidirectional(bytes);
        }
        else
        {
            _peer.OpenUnidirectional(bytes);
        }

        Pump(static () => false, 200);
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.PeerStreamStarted));

        // The peer's SETTINGS arrive late; the session establishes and the held stream becomes Core's.
        _peer.SendSettingsNow();
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.Connected) == 1), "the client's OnConnected");
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.StreamReceived) > 0), "the held stream's bytes");

        // OnConnected comes first, then the stream: the contract the carrier broke.
        IReadOnlyList<RecordedEvent> events = _clientSink.Events;
        int connected = -1;
        int started = -1;
        for (int i = 0; i < events.Count; i++)
        {
            if (connected < 0 && events[i].Kind == RecordedEventKind.Connected) connected = i;
            if (started < 0 && events[i].Kind == RecordedEventKind.PeerStreamStarted) started = i;
        }

        Assert.True(connected >= 0, "OnConnected was never raised");
        Assert.True(started > connected, $"the peer stream was raised at {started}, before OnConnected at {connected}");

        // The preamble is stripped and the payload survives the hold.
        byte[] delivered = [];
        foreach (RecordedEvent e in events)
        {
            if (e.Kind == RecordedEventKind.StreamReceived) delivered = [.. delivered, .. e.Data];
        }

        Assert.Equal<byte>([0x5A], delivered);
    }

    /// <summary>The same for datagrams: a session that does not exist yet has no application to deliver to.</summary>
    [Fact]
    public void A_datagram_before_the_session_is_established_is_dropped()
    {
        _peer.SendSettings = false;
        Start();
        Assert.True(Pump(() => _peer.ConnectAnswered && _peer.SessionId != ulong.MaxValue), "the Extended CONNECT answer");

        using var buffer = new NativeBuffer(32);
        var span = new Span<byte>(buffer.Pointer, 32);
        int written = HttpDatagram.Write(span, _peer.SessionId, [0x11, 0x22]);
        Assert.True(written > 0, "the datagram did not fit");
        using var segments = new NativeSegments(1);
        segments.Set(0, buffer.Segment(0, written));
        Assert.Equal(TransportStatus.Success, _peer.Transport!.SendDatagram(segments.At(0), 1, 9, TransportSendFlags.None));

        Pump(static () => false, 500);

        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.Connected));
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.DatagramReceived));
    }

    /// <summary>
    /// The carrier's native arenas are freed once the inner connection has reported its close, and not before: the
    /// inner transport's Dispose only asks the connection to close and returns, so MsQuic may still be sending from the
    /// preamble bytes, the control arena and the datagram arena. Deferring the free must not lose it.
    /// </summary>
    [Fact]
    public void Native_memory_is_released_after_the_inner_connection_closes()
    {
        Start();
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.Connected) == 1), "the client's OnConnected");

        var carrier = (WebTransportTransport)_client!;
        Assert.False(carrier.NativeMemoryReleased, "the arenas were freed while the session was live");

        carrier.Dispose();

        // The inner transport's Dispose only asks the connection to close, so the free cannot have happened yet.
        Assert.False(carrier.NativeMemoryReleased, "the arenas were freed before the inner connection reported its close");

        Assert.True(
            Pump(() => carrier.NativeMemoryReleased),
            "the carrier never released its native memory after the inner connection closed");
    }

    public void Dispose()
    {
        _client?.Dispose();
        _connector?.Dispose();
        _listener.Dispose();
        _peer.Dispose();
    }
}
