using Tedd.Quicly.Testing.Conformance;
using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Http3;
using Tedd.Quicly.Http3.Qpack;
using Tedd.Quicly.Http3.WebTransport;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;

/// <summary>
/// What the carrier's client puts on the wire, seen by a peer that speaks plain HTTP/3 and knows nothing about the
/// carrier (<see cref="RawHttp3Peer"/>). These are the checks that say the session would work against a browser.
/// </summary>
public sealed unsafe class WebTransportWireTests : IDisposable
{
    private readonly VirtualClock _clock = new();
    private readonly SimulatedNetwork _network;
    private readonly SimulatedListener _listener;
    private readonly RawHttp3Peer _peer = new();
    private readonly RecordingSink _clientSink;
    private WebTransportConnector? _connector;
    private ITransport? _client;

    public WebTransportWireTests()
    {
        _network = new SimulatedNetwork(_clock, seed: 1);
        _clientSink = new RecordingSink(_clock);
        _listener = new SimulatedListener(_network, new IPEndPoint(IPAddress.Loopback, 0));
    }

    private ITransport Start(WebTransportOptions? options = null)
    {
        _listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            _peer.Transport = transport;
            // Room for the client's three HTTP/3 streams plus its WebTransport data streams.
            transport.UpdatePeerStreamLimits(8, 16);
            return _peer;
        });

        _connector = new WebTransportConnector(new SimulatedConnector(_network, new LinkOptions { DelayMicros = 2_000 }), options);
        _client = _connector.Connect(_listener.LocalEndPoint, "localhost", _clientSink);
        _client.UpdatePeerStreamLimits(8, 16);
        return _client;
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

    [Fact]
    public void Client_opens_the_control_and_qpack_streams_and_sends_settings()
    {
        Start();
        Assert.True(Pump(() => _peer.UnidirectionalStreamTypes.Count >= 3), "the client's three HTTP/3 streams");

        Assert.Contains((ulong)Http3StreamType.Control, _peer.UnidirectionalStreamTypes);
        Assert.Contains((ulong)Http3StreamType.QpackEncoder, _peer.UnidirectionalStreamTypes);
        Assert.Contains((ulong)Http3StreamType.QpackDecoder, _peer.UnidirectionalStreamTypes);

        byte[] control = _peer.ControlStreamBytes;
        Assert.Equal((byte)Http3StreamType.Control, control[0]);
        Assert.True(Http3FrameReader.TryReadFrame(control.AsSpan(1), out ulong type, out ReadOnlySpan<byte> payload, out _));
        Assert.Equal((ulong)Http3FrameType.Settings, type);
        Assert.Equal(Http3SettingsDecodeStatus.Ok, Http3Settings.Decode(payload, out Http3Settings settings));
        Assert.Equal(1ul, settings.EnableWebTransport);
        Assert.Equal(1ul, settings.EnableConnectProtocol);
        Assert.Equal(1ul, settings.H3Datagram);
        Assert.Equal(0ul, settings.QpackMaxTableCapacity);
    }

    [Fact]
    public void Client_sends_a_valid_extended_connect_request()
    {
        Start(new WebTransportOptions { Path = "/play", Origin = "https://game.example" });
        Assert.True(Pump(() => _peer.ConnectRequestFields.Length > 0), "the Extended CONNECT request");

        var headers = new Http3HeaderCollection(4096, 32);
        Assert.Equal(QpackDecodeStatus.Ok, QpackDecoder.Decode(_peer.ConnectRequestFields, headers));
        Assert.Equal(WebTransportRequestStatus.Ok, WebTransportRequest.Validate(headers, out WebTransportConnectRequest request));
        Assert.True(request.Path.SequenceEqual("/play"u8));
        Assert.True(request.Origin.SequenceEqual("https://game.example"u8));
        Assert.True(request.Authority.SequenceEqual("localhost"u8));
    }

    [Fact]
    public void A_plain_http3_peer_can_establish_the_session()
    {
        Start();
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.Connected) == 1), "the client's OnConnected");

        // The session id is the CONNECT stream's QUIC id, which both ends have to agree on.
        var carrier = (WebTransportTransport)_client!;
        Assert.Equal(_peer.SessionId, carrier.SessionId);
        Assert.True(carrier.IsSessionEstablished);
        Assert.Equal(TransportState.Connected, carrier.State);
    }

    [Fact]
    public void A_peer_that_never_sends_settings_never_establishes_the_session()
    {
        _peer.SendSettings = false;
        Start();
        Assert.True(Pump(() => _peer.ConnectAnswered), "the Extended CONNECT answer");
        Pump(static () => false, 500);

        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.Connected));
        Assert.Equal(TransportState.Connecting, _client!.State);
    }

    [Fact]
    public void A_peer_whose_settings_lack_webtransport_is_refused()
    {
        _peer.Settings = new Http3Settings { EnableConnectProtocol = 1, H3Datagram = 1, QpackMaxTableCapacity = 0, QpackBlockedStreams = 0 };
        Start();
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.Connected));
    }

    [Fact]
    public void A_refused_connect_closes_the_client_without_connecting()
    {
        _peer.ResponseStatus = 404;
        Start();
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.Connected));
    }

    [Fact]
    public void Datagrams_carry_the_rfc9297_quarter_stream_id_prefix()
    {
        Start();
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.Connected) == 1), "the client's OnConnected");

        using var payload = new NativeBuffer(16);
        for (int i = 0; i < 16; i++) payload.Pointer[i] = (byte)(0xA0 + i);
        using var segments = new NativeSegments(1);
        segments.Set(0, payload.Segment(0, 16));
        Assert.Equal(TransportStatus.Success, _client!.SendDatagram(segments.At(0), 1, 77, TransportSendFlags.None));
        Assert.True(Pump(() => _peer.DatagramPayloads.Count == 1), "the datagram at the peer");

        byte[] wire = _peer.DatagramPayloads[0];
        Assert.True(HttpDatagram.TryRead(wire, out ulong streamId, out ReadOnlySpan<byte> inner));
        Assert.Equal(((WebTransportTransport)_client).SessionId, streamId);
        Assert.Equal(16, inner.Length);
        for (int i = 0; i < 16; i++) Assert.Equal((byte)(0xA0 + i), inner[i]);
    }

    [Fact]
    public void The_datagram_payload_core_sees_is_the_link_payload_minus_the_prefix()
    {
        Start();
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.Connected) == 1), "the client's OnConnected");

        var carrier = (WebTransportTransport)_client!;
        int prefix = HttpDatagram.GetPrefixLength(carrier.SessionId);
        Assert.True(prefix > 0);
        Assert.Equal(carrier.Inner!.Capabilities.MaxDatagramPayload - prefix, carrier.Capabilities.MaxDatagramPayload);
    }

    [Fact]
    public void Unidirectional_data_streams_carry_the_webtransport_preamble()
    {
        Start();
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.Connected) == 1), "the client's OnConnected");

        using var payload = new NativeBuffer(8);
        for (int i = 0; i < 8; i++) payload.Pointer[i] = (byte)(0x10 + i);
        using var segments = new NativeSegments(1);
        segments.Set(0, payload.Segment(0, 8));
        Assert.Equal(TransportStatus.Success, _client!.OpenStream(StreamKind.Unidirectional, 5, 32767, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, _client.SendStream(id, segments.At(0), 1, 6, TransportSendFlags.Start | TransportSendFlags.Fin));
        Assert.True(Pump(() => _peer.DataStreamBytes.Count == 1 && _peer.DataStreamBytes[0].Length >= 8), "the data stream at the peer");

        byte[] wire = _peer.DataStreamBytes[0];
        Assert.Equal(WebTransportPreambleStatus.Ok, WebTransportFraming.TryReadUnidirectionalPreamble(wire, out ulong sessionId, out int consumed));
        Assert.Equal(((WebTransportTransport)_client).SessionId, sessionId);
        Assert.Equal(8, wire.Length - consumed);
        for (int i = 0; i < 8; i++) Assert.Equal((byte)(0x10 + i), wire[consumed + i]);
    }

    [Fact]
    public void A_datagram_for_another_session_is_dropped_instead_of_delivered()
    {
        Start();
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.Connected) == 1), "the client's OnConnected");

        var carrier = (WebTransportTransport)_client!;
        long droppedBefore = carrier.DatagramsDroppedForOtherSession;

        // A datagram naming a session id that is not ours: the peer's own CONNECT stream would be id 4 away.
        using var buffer = new NativeBuffer(32);
        var span = new Span<byte>(buffer.Pointer, 32);
        int written = HttpDatagram.Write(span, carrier.SessionId + 4, "hello"u8);
        using var segments = new NativeSegments(1);
        segments.Set(0, buffer.Segment(0, written));
        Assert.Equal(TransportStatus.Success, _peer.Transport!.SendDatagram(segments.At(0), 1, 9, TransportSendFlags.None));
        Assert.True(Pump(() => carrier.DatagramsDroppedForOtherSession > droppedBefore), "the dropped datagram");
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.DatagramReceived));
    }

    public void Dispose()
    {
        _client?.Dispose();
        if (!_network.IsDisposed) _network.RunUntilIdle(10_000_000);
        _connector?.Dispose();
        _listener.Dispose();
        _network.Dispose();
        _peer.Dispose();
    }
}
