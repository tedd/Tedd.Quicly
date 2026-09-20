using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;

/// <summary>
/// Session establishment between two carriers: the path and origin checks the server makes, the session id both ends
/// agree on, and the refusals. The <see cref="ITransport"/> contract itself is covered by
/// <see cref="WebTransportConformanceTests"/>.
/// </summary>
public sealed unsafe class WebTransportSessionTests : IDisposable
{
    private readonly VirtualClock _clock = new();
    private readonly SimulatedNetwork _network;
    private readonly List<IDisposable> _owned = [];
    private RecordingSink _clientSink = null!;
    private RecordingSink _serverSink = null!;
    private ITransport? _client;
    private ITransport? _server;

    public WebTransportSessionTests() => _network = new SimulatedNetwork(_clock, seed: 1);

    /// <summary>Starts a client and a server carrier, each with its own options.</summary>
    private void Start(WebTransportOptions? client = null, WebTransportOptions? server = null)
    {
        _clientSink = new RecordingSink(_clock);
        _serverSink = new RecordingSink(_clock);

        var simulated = new SimulatedListener(_network, new IPEndPoint(IPAddress.Loopback, 0));
        _owned.Add(simulated);
        var listener = new WebTransportListener(simulated, server);
        _owned.Add(listener);
        listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            _server = transport;
            return _serverSink;
        });

        var connector = new WebTransportConnector(new SimulatedConnector(_network, new LinkOptions { DelayMicros = 2_000 }), client);
        _owned.Add(connector);
        _client = connector.Connect(simulated.LocalEndPoint, "localhost", _clientSink);
        _client.UpdatePeerStreamLimits(8, 16);
        Pump(() => _server is not null);
        _server?.UpdatePeerStreamLimits(8, 16);
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

    private bool WaitConnected() => Pump(() => _clientSink.CountOf(RecordedEventKind.Connected) == 1 && _serverSink.CountOf(RecordedEventKind.Connected) == 1);

    [Fact]
    public void Two_carriers_establish_a_session_and_agree_on_its_id()
    {
        Start();
        Assert.True(WaitConnected(), "both ends connected");

        var client = (WebTransportTransport)_client!;
        var server = (WebTransportTransport)_server!;
        Assert.Equal(client.SessionId, server.SessionId);
        Assert.NotEqual(ulong.MaxValue, client.SessionId);
        Assert.True(client.IsSessionEstablished);
        Assert.True(server.IsSessionEstablished);
    }

    [Fact]
    public void The_alpn_reported_to_core_is_the_inner_connections()
    {
        Start();
        Assert.True(WaitConnected(), "both ends connected");
        RecordedEvent connected = _clientSink.OfKind(RecordedEventKind.Connected)[0];
        Assert.True(connected.Capabilities.Datagrams);
        Assert.True(connected.Capabilities.MaxDatagramPayload > 0);
    }

    [Fact]
    public void A_request_for_another_path_is_refused()
    {
        Start(new WebTransportOptions { Path = "/somewhere-else" }, new WebTransportOptions { Path = "/quicly" });
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.Connected));
        Assert.Equal(0, _serverSink.CountOf(RecordedEventKind.Connected));
    }

    [Fact]
    public void A_matching_path_other_than_the_default_is_accepted()
    {
        Start(new WebTransportOptions { Path = "/arena" }, new WebTransportOptions { Path = "/arena" });
        Assert.True(WaitConnected(), "both ends connected on the configured path");
    }

    [Fact]
    public void Origin_policy_require_refuses_a_request_without_an_origin()
    {
        var server = new WebTransportOptions { OriginPolicy = WebTransportOriginPolicy.Require };
        server.AllowedOrigins.Add("https://game.example");
        Start(new WebTransportOptions(), server);
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.Connected));
    }

    [Fact]
    public void Origin_policy_require_accepts_an_allowed_origin()
    {
        var server = new WebTransportOptions { OriginPolicy = WebTransportOriginPolicy.Require };
        server.AllowedOrigins.Add("https://game.example");
        Start(new WebTransportOptions { Origin = "https://game.example" }, server);
        Assert.True(WaitConnected(), "both ends connected");
    }

    [Fact]
    public void Origin_policy_require_refuses_an_origin_that_is_not_on_the_list()
    {
        var server = new WebTransportOptions { OriginPolicy = WebTransportOriginPolicy.Require };
        server.AllowedOrigins.Add("https://game.example");
        Start(new WebTransportOptions { Origin = "https://evil.example" }, server);
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.Connected));
    }

    [Fact]
    public void Origin_policy_check_if_present_lets_a_native_client_through()
    {
        var server = new WebTransportOptions { OriginPolicy = WebTransportOriginPolicy.CheckIfPresent };
        server.AllowedOrigins.Add("https://game.example");
        Start(new WebTransportOptions(), server);
        Assert.True(WaitConnected(), "a client without an origin connected");
    }

    [Fact]
    public void Origin_policy_check_if_present_still_refuses_a_bad_origin()
    {
        var server = new WebTransportOptions { OriginPolicy = WebTransportOriginPolicy.CheckIfPresent };
        server.AllowedOrigins.Add("https://game.example");
        Start(new WebTransportOptions { Origin = "https://evil.example" }, server);
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.Connected));
    }

    [Fact]
    public void A_datagram_gathering_more_segments_than_configured_is_too_large()
    {
        Start(new WebTransportOptions { MaxDatagramSegments = 2 }, new WebTransportOptions());
        Assert.True(WaitConnected(), "both ends connected");

        using var buffer = new NativeBuffer(64);
        using var segments = new NativeSegments(3);
        for (int i = 0; i < 3; i++) segments.Set(i, buffer.Segment(i * 8, 8));
        Assert.Equal(TransportStatus.TooLarge, _client!.SendDatagram(segments.At(0), 3, 1, TransportSendFlags.None));
        Assert.Equal(TransportStatus.Success, _client.SendDatagram(segments.At(0), 2, 2, TransportSendFlags.None));
    }

    [Fact]
    public void A_stream_send_gathering_more_segments_than_configured_is_too_large()
    {
        Start(new WebTransportOptions { MaxStreamSegments = 2 }, new WebTransportOptions());
        Assert.True(WaitConnected(), "both ends connected");

        using var buffer = new NativeBuffer(64);
        using var segments = new NativeSegments(3);
        for (int i = 0; i < 3; i++) segments.Set(i, buffer.Segment(i * 8, 8));
        Assert.Equal(TransportStatus.Success, _client!.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId id));
        Assert.Equal(TransportStatus.TooLarge, _client.SendStream(id, segments.At(0), 3, 2, TransportSendFlags.Start));
    }

    [Fact]
    public void Sends_before_the_session_exists_are_refused()
    {
        var clientSink = new RecordingSink(_clock);
        var simulated = new SimulatedListener(_network, new IPEndPoint(IPAddress.Loopback, 0));
        _owned.Add(simulated);
        simulated.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, static (ITransport _, in NewConnectionInfo _) => null);
        var connector = new WebTransportConnector(new SimulatedConnector(_network, new LinkOptions { DelayMicros = 2_000 }));
        _owned.Add(connector);
        ITransport client = connector.Connect(simulated.LocalEndPoint, "localhost", clientSink);
        _client = client;

        Assert.Equal(TransportState.Connecting, client.State);
        using var buffer = new NativeBuffer(16);
        using var segments = new NativeSegments(1);
        segments.Set(0, buffer.Segment(0, 16));
        Assert.Equal(TransportStatus.InvalidState, client.SendDatagram(segments.At(0), 1, 1, TransportSendFlags.None));
        Assert.Equal(TransportStatus.InvalidState, client.OpenStream(StreamKind.Unidirectional, 1, 32767, out _));
    }

    [Fact]
    public void A_listener_that_refuses_the_connection_closes_the_client()
    {
        var clientSink = new RecordingSink(_clock);
        var simulated = new SimulatedListener(_network, new IPEndPoint(IPAddress.Loopback, 0));
        _owned.Add(simulated);
        var listener = new WebTransportListener(simulated);
        _owned.Add(listener);
        listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, static (ITransport _, in NewConnectionInfo _) => null);

        var connector = new WebTransportConnector(new SimulatedConnector(_network, new LinkOptions { DelayMicros = 2_000 }));
        _owned.Add(connector);
        _client = connector.Connect(simulated.LocalEndPoint, "localhost", clientSink);

        Assert.True(Pump(() => clientSink.IsClosed), "the client's OnClosed");
        Assert.Equal(0, clientSink.CountOf(RecordedEventKind.Connected));
        Assert.Equal(1, listener.SessionsRefusedCount);
    }

    [Fact]
    public void A_close_reason_beyond_512_bytes_is_refused_without_changing_the_state()
    {
        Start();
        Assert.True(WaitConnected(), "both ends connected");
        Assert.Throws<ArgumentOutOfRangeException>(() => _client!.Close(9, new byte[513]));
        Assert.Equal(TransportState.Connected, _client!.State);
    }

    [Fact]
    public void The_datagram_send_path_does_not_allocate()
    {
        Start();
        Assert.True(WaitConnected(), "both ends connected");

        using var buffer = new NativeBuffer(64);
        using var segments = new NativeSegments(2);
        segments.Set(0, buffer.Segment(0, 8));
        segments.Set(1, buffer.Segment(8, 24));
        ITransport client = _client!;

        // Warm up the path, then measure it: the prefix goes out as a pooled gather segment, so nothing is allocated.
        for (int i = 0; i < 64; i++)
        {
            client.SendDatagram(segments.At(0), 2, (ulong)i, TransportSendFlags.None);
            _network.Advance(250);
        }

        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 64; i++) client.SendDatagram(segments.At(0), 2, (ulong)(1000 + i), TransportSendFlags.None);
        });
    }

    public void Dispose()
    {
        _client?.Dispose();
        _server?.Dispose();
        if (!_network.IsDisposed) _network.RunUntilIdle(10_000_000);
        for (int i = _owned.Count - 1; i >= 0; i--) _owned[i].Dispose();
        _network.Dispose();
    }
}
