using System.Net;
using System.Text;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Simulation;

public class ConnectionTests
{
    [Fact]
    public void CreatePair_RaisesCapabilityBeforeConnected_OnBothEnds()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000, MaxDatagramPayload = 1100 }, connect: false);
        Assert.Equal(TransportState.Connecting, h.A.State);
        Assert.True(h.A.IsClient);
        Assert.False(h.B.IsClient);
        Assert.Equal(4, h.Network.PendingEvents);
        Assert.True(h.Network.RunUntilIdle(1_000_000));
        foreach ((RecordingSink sink, SimulatedTransport self, SimulatedTransport peer) in new[] { (h.SinkA, h.A, h.B), (h.SinkB, h.B, h.A) })
        {
            IReadOnlyList<RecordedEvent> events = sink.Events;
            Assert.Equal(2, events.Count);
            Assert.Equal(RecordedEventKind.DatagramCapabilityChanged, events[0].Kind);
            Assert.True(events[0].Enabled);
            Assert.Equal(1100, events[0].MaxPayload);
            Assert.Equal(0, events[0].TimeMicros);
            Assert.Equal(RecordedEventKind.Connected, events[1].Kind);
            Assert.Equal(20_000, events[1].TimeMicros);
            Assert.Equal(SimulatedConnector.DefaultAlpn, events[1].Alpn);
            Assert.Equal(1100, events[1].Capabilities.MaxDatagramPayload);
            Assert.True(events[1].Capabilities.StreamPriority);
            Assert.Equal(self.LocalEndPoint, events[1].LocalEndPoint);
            Assert.Equal(peer.LocalEndPoint, events[1].RemoteEndPoint);
            Assert.Equal(TransportState.Connected, self.State);
        }
        Assert.Same(h.Network, h.A.Network);
        Assert.Equal(1, h.Network.Seed);
    }

    [Fact]
    public void ConnectDelay_OverridesTheRoundTrip()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000, ConnectDelayMicros = 3 }, connect: false);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(3, h.SinkA.OfKind(RecordedEventKind.Connected)[0].TimeMicros);
    }

    [Fact]
    public void Close_CancelsInFlightSends_ShutsStreamsDown_ThenClosesBothEnds()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        TransportStreamId stream = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, stream, "data"u8, 11));
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, "dg"u8, 12));
        long t0 = h.Clock.NowMicros;
        h.A.Close(42, "bye"u8);
        Assert.Equal(TransportState.Closing, h.A.State);
        Assert.Equal(TransportStatus.InvalidState, Sim.SendDatagram(h.A, "x"u8, 1));
        Assert.Equal(TransportStatus.InvalidState, h.A.OpenStream(StreamKind.Bidirectional, 0, 0, out _));
        h.A.Close(1, default); // ignored

        h.Network.Advance(0);
        string[] a = Sim.Describe(h.SinkA.Events);
        Assert.Equal(
        [
            $"{t0} StreamStarted {stream} ctx=7 Success",
            $"{t0} DatagramSendStateChanged ctx=12 Sent",
            $"{t0} StreamSendCompleted {stream} ctx=11 canceled=True",
            $"{t0} DatagramSendStateChanged ctx=12 Canceled",
            $"{t0} StreamShutdownComplete {stream}",
            $"{t0} Closed Local code=42 status=0",
        ], a);
        Assert.Equal(TransportState.Closed, h.A.State);

        h.Network.RunUntilIdle(1_000_000);
        IReadOnlyList<RecordedEvent> b = h.SinkB.Events;
        Assert.Equal(RecordedEventKind.Closed, b[^1].Kind);
        Assert.Equal(TransportCloseReason.Peer, b[^1].CloseReason);
        Assert.Equal(42UL, b[^1].ErrorCode);
        Assert.Equal(t0 + 10_000, b[^1].TimeMicros);
        Assert.Equal(RecordedEventKind.StreamShutdownComplete, b[^2].Kind);
        Assert.Equal("bye", Encoding.ASCII.GetString(h.B.PeerCloseReason.Span));
        Assert.True(h.A.PeerCloseReason.IsEmpty);
        Assert.Equal(TransportState.Closed, h.B.State);

        int countA = h.SinkA.Count, countB = h.SinkB.Count;
        h.B.Close(0, default);
        h.Network.Advance(1_000_000);
        Assert.Equal(countA, h.SinkA.Count);
        Assert.Equal(countB, h.SinkB.Count);
    }

    [Fact]
    public void Close_ReasonLongerThan512Bytes_Throws()
    {
        using SimHarness h = new();
        Assert.Throws<ArgumentOutOfRangeException>(() => h.A.Close(0, new byte[513]));
        h.A.Close(0, new byte[512]);
    }

    [Fact]
    public void Dispose_ClosesWithCodeZero_AndIsIdempotent()
    {
        using SimHarness h = new();
        h.A.Dispose();
        h.A.Dispose();
        h.Network.RunUntilIdle(1_000_000);
        RecordedEvent closed = Assert.Single(h.SinkB.OfKind(RecordedEventKind.Closed));
        Assert.Equal(TransportCloseReason.Peer, closed.CloseReason);
        Assert.Equal(0UL, closed.ErrorCode);
    }

    [Fact]
    public void SimultaneousClose_BothEndsReportLocal()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000 });
        h.A.Close(1, default);
        h.B.Close(2, default);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(TransportCloseReason.Local, Assert.Single(h.SinkA.OfKind(RecordedEventKind.Closed)).CloseReason);
        Assert.Equal(TransportCloseReason.Local, Assert.Single(h.SinkB.OfKind(RecordedEventKind.Closed)).CloseReason);
    }

    [Fact]
    public void Disconnect_ClosesBothEndsWithTransportReason()
    {
        using SimHarness h = new(new LinkOptions { DisconnectAtMicros = 50_000 }, connect: false);
        h.Network.AdvanceTo(49_999);
        h.SinkA.Clear();
        h.SinkB.Clear();
        h.Network.Advance(1);
        foreach (RecordingSink sink in new[] { h.SinkA, h.SinkB })
        {
            RecordedEvent closed = Assert.Single(sink.Events);
            Assert.Equal(TransportCloseReason.Transport, closed.CloseReason);
            Assert.Equal(SimulatedTransport.StatusDisconnected, closed.TransportStatus);
            Assert.Equal(50_000, closed.TimeMicros);
        }
        Assert.True(h.SinkA.IsClosed);
    }

    [Fact]
    public void Disconnect_AfterLocalClose_DoesNotCloseTwice()
    {
        using SimHarness h = new(new LinkOptions { DisconnectAtMicros = 0 }, connect: false);
        h.A.Close(5, default);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(TransportCloseReason.Local, Assert.Single(h.SinkA.OfKind(RecordedEventKind.Closed)).CloseReason);
        Assert.Equal(TransportCloseReason.Transport, Assert.Single(h.SinkB.OfKind(RecordedEventKind.Closed)).CloseReason);
    }

    [Fact]
    public void GetStatistics_ReportsRttCongestionWindowAndCounters()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000, JitterMicros = 2_000 });
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[100], 1));
        h.Network.RunUntilIdle(1_000_000);
        h.A.GetStatistics(out TransportStatistics a);
        h.B.GetStatistics(out TransportStatistics b);
        Assert.Equal(20_000u, a.RttMicros);
        Assert.Equal(20_000u, a.MinRttMicros);
        Assert.Equal(24_000u, a.MaxRttMicros);
        Assert.Equal(2_000u, a.RttVarianceMicros);
        Assert.Equal(16u << 20, a.CongestionWindowBytes);
        Assert.Equal(1200 + SimulatedTransport.PathOverheadBytes, a.PathMtu);
        Assert.Equal(100UL, a.SendTotalBytes);
        Assert.Equal(1UL, a.SendTotalPackets);
        Assert.Equal(100UL, b.RecvTotalBytes);
        Assert.Equal(1UL, b.RecvTotalPackets);
        Assert.Equal(0UL, a.BytesInFlight);

        using SimHarness capped = new(new LinkOptions { DelayMicros = 10_000, BandwidthBitsPerSecond = 8_000_000 });
        capped.A.GetStatistics(out TransportStatistics c);
        Assert.Equal(20_000u, c.CongestionWindowBytes); // 1 MB/s * 20 ms
    }

    [Fact]
    public void Network_AdvanceArgumentsAndDisposal()
    {
        VirtualClock clock = new(100);
        SimulatedNetwork network = new(clock, 0);
        Assert.Equal(clock, network.Clock);
        Assert.Throws<ArgumentOutOfRangeException>(() => network.Advance(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => network.AdvanceTo(99));
        Assert.Throws<ArgumentOutOfRangeException>(() => network.RunUntilIdle(-1));
        Assert.Equal(0, network.Advance(5));
        Assert.Equal(105, network.NowMicros);
        Assert.True(network.RunUntilIdle(0));

        RecordingSink a = new(), b = new();
        network.CreatePair(a, b, new LinkOptions { DelayMicros = 1_000_000 });
        Assert.False(network.RunUntilIdle(10));
        Assert.Equal(115, network.NowMicros);
        Assert.Equal(2, network.PendingEvents);

        network.Dispose();
        Assert.Throws<ObjectDisposedException>(() => network.Advance(1));
        Assert.Throws<ObjectDisposedException>(() => network.AdvanceTo(200));
        Assert.Throws<ObjectDisposedException>(() => network.RunUntilIdle(1));
        Assert.Throws<ObjectDisposedException>(() => network.CreatePair(a, b));
        Assert.Equal(0, network.PendingEvents);
        Assert.Throws<ArgumentNullException>(() => new SimulatedNetwork(null!, 0));
    }

    [Fact]
    public void Network_ExternallyAdvancedClock_DeliversOverdueEventsWithoutRewinding()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000 });
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, "x"u8, 1));
        h.Clock.AdvanceMicros(10_000);
        long now = h.Clock.NowMicros;
        h.Network.Advance(0);
        Assert.Equal(now, h.Clock.NowMicros);
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.DatagramReceived));
    }

    [Fact]
    public void CreatePair_ValidatesArguments()
    {
        using SimulatedNetwork network = new(new VirtualClock(), 0);
        RecordingSink sink = new();
        Assert.Throws<ArgumentNullException>(() => network.CreatePair(null!, sink));
        Assert.Throws<ArgumentNullException>(() => network.CreatePair(sink, null!));
    }

    public static TheoryData<Action<LinkOptions>> InvalidOptions => new()
    {
        o => o.DelayMicros = -1,
        o => o.JitterMicros = -1,
        o => o.LossPercent = 101,
        o => o.StreamLossPercent = -0.1,
        o => o.ReorderPercent = double.NaN,
        o => o.BandwidthBitsPerSecond = -1,
        o => o.MaxQueueBytes = -1,
        o => o.MaxDatagramPayload = 0,
        o => o.MaxDatagramPayload = 65_001,
        o => o.DisconnectAtMicros = -1,
        o => o.ConnectDelayMicros = -1,
        o => o.MtuChanges.Add(new MtuChange(-1, 1000)),
        o => o.MtuChanges.Add(new MtuChange(0, 0)),
    };

    [Theory]
    [MemberData(nameof(InvalidOptions))]
    public void LinkOptions_Validation_Rejects(Action<LinkOptions> mutate)
    {
        using SimulatedNetwork network = new(new VirtualClock(), 0);
        LinkOptions options = new();
        mutate(options);
        Assert.Throws<ArgumentOutOfRangeException>(() => network.CreatePair(new RecordingSink(), new RecordingSink(), options));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SimulatedConnector(network, options));
    }

    [Fact]
    public void LinkOptions_AreCopiedAtCreation()
    {
        LinkOptions options = new() { MaxDatagramPayload = 1000 };
        using SimulatedNetwork network = new(new VirtualClock(), 0);
        (SimulatedTransport a, _) = network.CreatePair(new RecordingSink(), new RecordingSink(), options);
        options.MaxDatagramPayload = 500;
        options.MtuChanges.Add(new MtuChange(0, 100));
        network.RunUntilIdle(1_000);
        Assert.Equal(1000, a.Capabilities.MaxDatagramPayload);
        Assert.Equal(1_000 * 2 + 2 * 3 + 1000, new LinkOptions { DelayMicros = 1_000, JitterMicros = 3 }.RetransmitDelayMicros);
    }

    // ---------------------------------------------------------------- connector / listener

    private sealed class ListenerProbe
    {
        public readonly List<NewConnectionInfo> PreHandshakes = new();
        public readonly List<SimulatedTransport> Accepted = new();
        public PreHandshakeDecision Decision = PreHandshakeDecision.Accept;
        public Func<SimulatedTransport, ITransportSink?> Accept = _ => null;

        public PreHandshakeDecision OnPreHandshake(in NewConnectionInfo info)
        {
            PreHandshakes.Add(info);
            return Decision;
        }

        public ITransportSink? OnAccept(ITransport transport, in NewConnectionInfo info)
        {
            SimulatedTransport simulated = (SimulatedTransport)transport;
            Accepted.Add(simulated);
            return Accept(simulated);
        }
    }

    private static (SimulatedNetwork Network, VirtualClock Clock, SimulatedListener Listener, ListenerProbe Probe) CreateServer()
    {
        VirtualClock clock = new();
        SimulatedNetwork network = new(clock, 9);
        SimulatedListener listener = new(network);
        ListenerProbe probe = new();
        listener.Start(probe.OnPreHandshake, probe.OnAccept);
        return (network, clock, listener, probe);
    }

    [Fact]
    public void ConnectorAndListener_Accept_ConnectBothEnds()
    {
        (SimulatedNetwork network, VirtualClock clock, SimulatedListener listener, ListenerProbe probe) = CreateServer();
        using SimulatedNetwork _ = network;
        RecordingSink serverSink = new(clock);
        probe.Accept = t =>
        {
            serverSink.Transport = t;
            t.UpdatePeerStreamLimits(4, 2); // before connect: travels with the handshake
            return serverSink;
        };
        SimulatedConnector connector = new(network, new LinkOptions { DelayMicros = 10_000 }) { Alpn = "game/2" };
        Assert.Equal("game/2", connector.Alpn);
        RecordingSink clientSink = new(clock);
        SimulatedTransport client = (SimulatedTransport)connector.Connect(listener.LocalEndPoint, "example.test", clientSink);
        clientSink.Transport = client;
        Assert.Equal(TransportState.Connecting, client.State);
        Assert.Equal(listener.LocalEndPoint, client.RemoteEndPoint);

        network.AdvanceTo(10_000);
        NewConnectionInfo info = Assert.Single(probe.PreHandshakes);
        Assert.Equal("example.test", info.ServerName);
        Assert.Equal(client.LocalEndPoint, info.RemoteEndPoint);
        ReadOnlySpan<byte> alpn = info.Alpn;
        Assert.Equal("game/2", Encoding.ASCII.GetString(alpn[..info.AlpnLength]));
        SimulatedTransport server = Assert.Single(probe.Accepted);
        Assert.False(server.IsClient);
        Assert.Equal(listener.LocalEndPoint, server.LocalEndPoint);
        Assert.Equal(RecordedEventKind.DatagramCapabilityChanged, Assert.Single(serverSink.Events).Kind);

        network.RunUntilIdle(1_000_000);
        Assert.Equal(20_000, clientSink.OfKind(RecordedEventKind.Connected)[0].TimeMicros);
        Assert.Equal(RecordedEventKind.DatagramCapabilityChanged, clientSink.Events[0].Kind);
        Assert.Equal("game/2", clientSink.Events[1].Alpn);
        Assert.Equal(30_000, serverSink.OfKind(RecordedEventKind.Connected)[0].TimeMicros);

        // The limits the server set in accept apply to the client from its OnConnected.
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 1, 0, out TransportStreamId u1));
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 2, 0, out TransportStreamId u2));
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 3, 0, out TransportStreamId u3));
        Assert.Equal(TransportStatus.Success, client.StartStream(u1));
        Assert.Equal(TransportStatus.Success, client.StartStream(u2));
        Assert.Equal(TransportStatus.Success, client.StartStream(u3)); // beyond the limit: refused asynchronously, as MsQuic does

        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(client, "ping"u8, 1));
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(server, "pong"u8, 2));
        network.RunUntilIdle(1_000_000);
        Assert.Equal("ping"u8.ToArray(), serverSink.OfKind(RecordedEventKind.DatagramReceived)[0].Data);
        Assert.Equal("pong"u8.ToArray(), clientSink.OfKind(RecordedEventKind.DatagramReceived)[0].Data);
        Assert.Equal(2, serverSink.CountOf(RecordedEventKind.PeerStreamStarted));
        Assert.Equal(2, client.GetQuicStreamId(u1));
        Assert.Equal(6, client.GetQuicStreamId(u2));
        Assert.Equal(-1, client.GetQuicStreamId(u3));
        Assert.Equal(TransportStatus.StreamLimitReached, Assert.Single(clientSink.OfKind(RecordedEventKind.StreamStarted), e => e.StreamId == u3).Status);
        TransportStreamId serverUni = Assert.Single(serverSink.OfKind(RecordedEventKind.PeerStreamStarted), e => e.StreamKind == StreamKind.Unidirectional && server.GetQuicStreamId(e.StreamId) == 2).StreamId;
        Assert.True(serverUni.IsValid);
    }

    [Fact]
    public void PreHandshakeReject_ClosesClientRefused_WithoutAccept()
    {
        (SimulatedNetwork network, VirtualClock clock, SimulatedListener listener, ListenerProbe probe) = CreateServer();
        using SimulatedNetwork _ = network;
        probe.Decision = PreHandshakeDecision.Reject;
        RecordingSink clientSink = new(clock);
        new SimulatedConnector(network, new LinkOptions { DelayMicros = 10_000 }).Connect(listener.LocalEndPoint, null, clientSink);
        network.RunUntilIdle(1_000_000);
        Assert.Single(probe.PreHandshakes);
        Assert.Empty(probe.Accepted);
        RecordedEvent closed = Assert.Single(clientSink.Events);
        Assert.Equal(TransportCloseReason.Transport, closed.CloseReason);
        Assert.Equal(SimulatedTransport.StatusConnectionRefused, closed.TransportStatus);
        Assert.Equal(20_000, closed.TimeMicros);
    }

    [Fact]
    public void AcceptReturningNull_ClosesClientRefused()
    {
        (SimulatedNetwork network, VirtualClock clock, SimulatedListener listener, ListenerProbe probe) = CreateServer();
        using SimulatedNetwork _ = network;
        RecordingSink clientSink = new(clock);
        ITransport client = new SimulatedConnector(network).Connect(listener.LocalEndPoint, null, clientSink);
        network.RunUntilIdle(1_000_000);
        SimulatedTransport rejected = Assert.Single(probe.Accepted);
        Assert.Equal(TransportState.Closed, rejected.State);
        Assert.Equal(TransportStatus.InvalidState, Sim.SendDatagram(rejected, "x"u8, 1));
        rejected.Close(0, default); // no-op
        Assert.Equal(SimulatedTransport.StatusConnectionRefused, Assert.Single(clientSink.Events).TransportStatus);
        Assert.Equal(TransportState.Closed, client.State);
    }

    [Fact]
    public void NoListener_OrStoppedListener_IsUnreachable()
    {
        (SimulatedNetwork network, VirtualClock clock, SimulatedListener listener, ListenerProbe probe) = CreateServer();
        using SimulatedNetwork _ = network;
        listener.Stop();
        listener.Stop();
        RecordingSink first = new(clock), second = new(clock);
        SimulatedConnector connector = new(network);
        connector.Connect(listener.LocalEndPoint, null, first);
        connector.Connect(new DnsEndPoint("nowhere.test", 443), null, second);
        network.RunUntilIdle(1_000_000);
        Assert.Equal(SimulatedTransport.StatusUnreachable, Assert.Single(first.Events).TransportStatus);
        Assert.Equal(SimulatedTransport.StatusUnreachable, Assert.Single(second.Events).TransportStatus);
        Assert.Empty(probe.PreHandshakes);
    }

    [Fact]
    public void ClientClosedBeforeTheAttemptArrives_NeverReachesTheListener()
    {
        (SimulatedNetwork network, VirtualClock clock, SimulatedListener listener, ListenerProbe probe) = CreateServer();
        using SimulatedNetwork _ = network;
        RecordingSink clientSink = new(clock);
        ITransport client = new SimulatedConnector(network, new LinkOptions { DelayMicros = 1_000 }).Connect(listener.LocalEndPoint, null, clientSink);
        client.Close(3, default);
        network.RunUntilIdle(1_000_000);
        Assert.Empty(probe.PreHandshakes);
        Assert.Equal(TransportCloseReason.Local, Assert.Single(clientSink.Events).CloseReason);
    }

    [Fact]
    public void ClientClosedWhileServerConnecting_ServerClosesWithoutConnecting()
    {
        (SimulatedNetwork network, VirtualClock clock, SimulatedListener listener, ListenerProbe probe) = CreateServer();
        using SimulatedNetwork _ = network;
        RecordingSink serverSink = new(clock), clientSink = new(clock);
        probe.Accept = _ => serverSink;
        ITransport client = new SimulatedConnector(network, new LinkOptions { DelayMicros = 10_000 }).Connect(listener.LocalEndPoint, null, clientSink);
        network.AdvanceTo(10_000); // accepted; client connects at 20 ms, server at 30 ms
        client.Close(8, default);
        network.RunUntilIdle(1_000_000);
        Assert.Equal(0, serverSink.CountOf(RecordedEventKind.Connected));
        RecordedEvent closed = Assert.Single(serverSink.OfKind(RecordedEventKind.Closed));
        Assert.Equal(TransportCloseReason.Peer, closed.CloseReason);
        Assert.Equal(8UL, closed.ErrorCode);
        Assert.Equal(0, clientSink.CountOf(RecordedEventKind.Connected));
    }

    [Fact]
    public void Listener_LifecycleRules()
    {
        using SimulatedNetwork network = new(new VirtualClock(), 0);
        ListenerProbe probe = new();
        IPEndPoint fixedEndPoint = new(IPAddress.Loopback, 4433);
        SimulatedListener first = new(network, fixedEndPoint);
        Assert.Equal(fixedEndPoint, first.LocalEndPoint);
        SimulatedListener anyPort = new(network, new IPEndPoint(IPAddress.Loopback, 0));
        Assert.NotEqual(0, anyPort.LocalEndPoint.Port);
        Assert.Equal(IPAddress.Loopback, anyPort.LocalEndPoint.Address);

        Assert.Throws<ArgumentNullException>(() => first.Start(null!, probe.OnAccept));
        Assert.Throws<ArgumentNullException>(() => first.Start(probe.OnPreHandshake, null!));
        first.Start(probe.OnPreHandshake, probe.OnAccept);
        Assert.Throws<InvalidOperationException>(() => first.Start(probe.OnPreHandshake, probe.OnAccept));
        SimulatedListener clash = new(network, fixedEndPoint);
        Assert.Throws<InvalidOperationException>(() => clash.Start(probe.OnPreHandshake, probe.OnAccept));
        clash.Stop(); // not started: no effect on the other listener
        first.Dispose();
        Assert.Throws<ObjectDisposedException>(() => first.Start(probe.OnPreHandshake, probe.OnAccept));
        clash.Start(probe.OnPreHandshake, probe.OnAccept);
        clash.Dispose();
        Assert.Throws<ArgumentNullException>(() => new SimulatedListener(null!));
    }

    [Fact]
    public void Connector_ValidatesArguments()
    {
        SimulatedNetwork network = new(new VirtualClock(), 0);
        SimulatedConnector connector = new(network);
        Assert.Throws<ArgumentNullException>(() => new SimulatedConnector(null!));
        Assert.Throws<ArgumentNullException>(() => connector.Connect(null!, null, new RecordingSink()));
        Assert.Throws<ArgumentNullException>(() => connector.Connect(new IPEndPoint(IPAddress.Loopback, 1), null, null!));
        Assert.Throws<ArgumentException>(() => connector.Alpn = "");
        Assert.Throws<ArgumentException>(() => connector.Alpn = new string('a', 256));
        Assert.Throws<ArgumentException>(() => connector.Alpn = "é");
        network.Dispose();
        Assert.Throws<ObjectDisposedException>(() => connector.Connect(new IPEndPoint(IPAddress.Loopback, 1), null, new RecordingSink()));
    }

    [Fact]
    public void Connector_TimelineRunsFromConnect()
    {
        (SimulatedNetwork network, VirtualClock clock, SimulatedListener listener, ListenerProbe probe) = CreateServer();
        using SimulatedNetwork _ = network;
        RecordingSink serverSink = new(clock), clientSink = new(clock);
        probe.Accept = _ => serverSink;
        LinkOptions options = new() { DelayMicros = 1_000, DisconnectAtMicros = 100_000 };
        options.MtuChanges.Add(new MtuChange(50_000, 900));
        new SimulatedConnector(network, options).Connect(listener.LocalEndPoint, null, clientSink);
        network.RunUntilIdle(1_000_000);
        Assert.Equal(900, clientSink.OfKind(RecordedEventKind.DatagramCapabilityChanged)[^1].MaxPayload);
        Assert.Equal(900, serverSink.OfKind(RecordedEventKind.DatagramCapabilityChanged)[^1].MaxPayload);
        Assert.Equal(SimulatedTransport.StatusDisconnected, clientSink.Events[^1].TransportStatus);
        Assert.Equal(100_000, serverSink.Events[^1].TimeMicros);
    }
}
