using System.Net;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// <see cref="QuiclyPeer.Reconnect"/> (PROTOCOL.md §4.1): a client peer resumes its session over a new transport, keeps its
/// identity, handlers and statistics, and hands the sends of the lost connection back as
/// <see cref="DeliveryStatus.Disconnected"/>.
/// </summary>
public class ReconnectTests
{
    private static readonly byte[] SessionToken = [9, 8, 7, 6];
    private const ulong SessionId = 4242;

    /// <summary>Accepts a fresh session in epoch 1 and a presented token as a resume in epoch 2.</summary>
    private static void AcceptResumes(SessionHarness h) =>
        h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
            AdmissionResult.Accept(SessionToken, SessionId, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);

    /// <summary>Cuts the connection the way a lost link does: the server's transport closes without a QUICLY Close.</summary>
    private static void CutTheConnection(SessionHarness h) =>
        h.Server!.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);

    [Fact]
    public void A_Client_Resumes_Its_Session_Over_A_New_Transport()
    {
        using SessionHarness h = new(connect: false, authToken: "secret"u8.ToArray(), client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer oldServer = h.Server!;
        h.Client.Index = 11;
        h.Client.Tag = 0xABCD;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Client.RegisterHandler(2, Handlers.Collect(got));
        Assert.Equal(1u, h.Client.Epoch);
        Assert.Equal(SessionId, h.Client.SessionId);
        Assert.Equal(SessionToken, h.Client.SessionToken.ToArray());

        // Traffic before the loss, so the statistics are not empty afterwards.
        oldServer.SendCopy(new SendHeader(2), [1]);
        h.Run(200_000, 1_000);
        Assert.Single(got);
        h.Client.GetStatistics(out PeerStatistics before);
        Assert.True(before.DatagramsReceived > 0);

        // A tracked ordered message the lost connection still held must complete Disconnected.
        SendResult pending = h.Client.SendCopy(new SendHeader(4), new byte[256], SendOptions.Tracked);
        Assert.True(pending.IsAdmitted);

        CutTheConnection(h);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.Equal(DeliveryStatus.Disconnected, h.Client.GetDeliveryStatus(pending.Token));

        h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", "secret"u8);
        Assert.Equal(PeerState.Reconnecting, h.Client.State);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();

        // The session carried over: a new epoch on the same session id, with the old token presented in the Hello.
        Assert.Equal(2u, h.Client.Epoch);
        Assert.Equal(SessionId, h.Client.SessionId);
        Assert.Equal(SessionToken, h.Admission.LastSessionToken);
        Assert.Equal("secret"u8.ToArray(), h.Admission.LastAuthToken);
        Assert.Equal(CloseSource.None, h.Client.CloseReason.Source);
        Assert.Equal(HelloStatus.Accepted, h.Client.HandshakeStatus);

        // Identity, handlers and statistics are kept.
        Assert.Equal(11, h.Client.Index);
        Assert.Equal(0xABCDul, h.Client.Tag);
        h.Client.GetStatistics(out PeerStatistics after);
        Assert.True(after.DatagramsReceived >= before.DatagramsReceived);
        Assert.True(after.PingsSent >= before.PingsSent);
        Assert.Equal(0, h.Client.Core.Segments.Used);

        h.Server!.SendCopy(new SendHeader(2), [2]);
        h.Run(200_000, 1_000);
        Assert.Equal(2, got.Count);
        Assert.Equal(2, got[1].Payload[0]);
        Assert.Equal(2u, got[1].Header.Epoch);

        // The ordered channel opened a fresh stream on both ends (a second stream would be a protocol violation).
        List<(ReceiveHeader Header, byte[] Payload)> ordered = [];
        h.Server.RegisterHandler(4, Handlers.Collect(ordered));
        Assert.True(h.Client.SendCopy(new SendHeader(4), new byte[300]).IsAdmitted);
        h.Run(500_000, 1_000);
        Assert.Single(ordered);
        Assert.Equal(300, ordered[0].Payload.Length);
        Assert.Equal(PeerState.Connected, h.Client.State);

        Assert.Contains((PeerState.Closed, PeerState.Reconnecting), h.ClientEvents);
        Assert.Contains((PeerState.Reconnecting, PeerState.Handshaking), h.ClientEvents);
    }

    [Fact]
    public void Reconnect_Settles_The_Lost_Connection_When_The_Host_Did_Not_Poll()
    {
        using SessionHarness h = new(connect: false, client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer oldServer = h.Server!;
        SendResult pending = h.Client.SendCopy(new SendHeader(4), new byte[128], SendOptions.Tracked);
        Assert.True(pending.IsAdmitted);

        // Deliver the transport's close without ever polling the client, so Reconnect has to settle the peer itself.
        CutTheConnection(h);
        h.Network.Advance(500_000);
        Assert.True(h.Client.Core.IsTransportClosed);
        Assert.NotEqual(PeerState.Closed, h.Client.State);
        Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(pending.Token));

        h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.Equal(DeliveryStatus.Disconnected, h.Client.GetDeliveryStatus(pending.Token));
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();
        Assert.Equal(2u, h.Client.Epoch);
    }

    [Fact]
    public void An_In_Flight_Carrier_Is_Completed_And_Its_Segments_Reclaimed()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 50_000 }, connect: false,
            client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer oldServer = h.Server!;

        SendResult inFlight = h.Client.SendCopy(new SendHeader(4), new byte[512], SendOptions.Tracked);
        Assert.True(inFlight.IsAdmitted);
        h.Client.Flush();
        h.Network.Advance(0);
        Assert.True(h.Client.Core.Segments.Used > 0, "the carrier should hold a segment run");

        CutTheConnection(h);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.Equal(DeliveryStatus.Disconnected, h.Client.GetDeliveryStatus(inFlight.Token));

        h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.Equal(0, h.Client.Core.Segments.Used);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();

        List<(ReceiveHeader Header, byte[] Payload)> ordered = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(ordered));
        Assert.True(h.Client.SendCopy(new SendHeader(4), new byte[64]).IsAdmitted);
        h.Run(1_000_000, 10_000);
        Assert.Single(ordered);
    }

    [Fact]
    public void A_Resumed_Session_Reaches_The_Engines_As_An_Epoch_Reset()
    {
        // The admission policy has to mint the session token on the first connect, so it is installed before the handshake.
        TestEngine? clientEngine = null;
        using SessionHarness h = new(table: TestTables.Plumbing, connect: false,
            client: o =>
            {
                QuietOptions.Apply(o);
                o.EngineFactory = m => m == ChannelMode.UnreliableUnordered ? clientEngine = new TestEngine(m) : null;
            },
            server: o =>
            {
                QuietOptions.Apply(o);
                o.EngineFactory = m => m == ChannelMode.UnreliableUnordered ? new TestEngine(m) : null;
            });
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        TestEngine client = clientEngine!;
        QuiclyPeer oldServer = h.Server!;
        Assert.Equal(1, client.EpochResets);

        CutTheConnection(h);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();

        Assert.Equal(2, client.EpochResets);
        Assert.Equal(1, client.PeerClosedCalls);
        Assert.Equal(2u, h.Client.Epoch);
    }

    [Fact]
    public void A_Refused_Resume_Starts_A_Fresh_Session_In_Epoch_One()
    {
        using SessionHarness h = new(connect: false, client: QuietOptions.Apply, server: QuietOptions.Apply);
        // The server ignores the presented token and mints a new session (PROTOCOL.md §4.1 status 3 turned into a fresh one).
        h.Admission.Handler = static (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Accept(SessionToken, 0, epoch: 1);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer oldServer = h.Server!;
        ulong first = h.Client.SessionId;

        CutTheConnection(h);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();

        Assert.Equal(1u, h.Client.Epoch);
        Assert.NotEqual(first, h.Client.SessionId);
    }

    [Fact]
    public void Reconnect_Refuses_A_Server_Peer_A_Live_Connection_And_A_Disposed_Peer()
    {
        SessionHarness h = new(client: QuietOptions.Apply, server: QuietOptions.Apply);
        try
        {
            // A server cannot resume in place: its accept callback must return a sink before the Hello is read.
            Assert.Throws<InvalidOperationException>(() => h.Server!.Reconnect(h.Connector, h.Listener.LocalEndPoint, null, default));
            // The lost connection's transport must have reported its close first.
            Assert.Throws<InvalidOperationException>(() => h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, null, default));
            Assert.Throws<ArgumentNullException>(() => h.Client.Reconnect(null!, h.Listener.LocalEndPoint, null, default));
            Assert.Throws<ArgumentNullException>(() => h.Client.Reconnect(h.Connector, null!, null, default));
            Assert.Throws<ArgumentException>(() => h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, null, new byte[ControlCodec.MaxTokenLength + 1]));
        }
        finally
        {
            h.Dispose();
        }

        Assert.Throws<ObjectDisposedException>(() => h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, null, default));
    }

    [Fact]
    public void Reconnect_Cannot_Run_Inside_Poll()
    {
        using SessionHarness h = new(connect: false, client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        Exception? caught = null;
        h.Client.StateChanged += (peer, _, to) =>
        {
            if (to == PeerState.Closed)
            {
                caught = Record.Exception(() => peer.Reconnect(h.Connector, h.Listener.LocalEndPoint, null, default));
            }
        };

        CutTheConnection(h);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.IsType<InvalidOperationException>(caught);
    }

    [Fact]
    public void A_Connector_That_Throws_Leaves_The_Peer_Closed()
    {
        using SessionHarness h = new(connect: false, client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        CutTheConnection(h);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));

        Assert.Throws<InvalidOperationException>(() => h.Client.Reconnect(new ThrowingReconnectConnector(), h.Listener.LocalEndPoint, null, default));
        Assert.Equal(PeerState.Closed, h.Client.State);
        // No transport, so nothing can call back and Poll stays a no-op.
        Assert.Equal(0, h.Client.Poll());
        Assert.False(h.Client.HasPendingWork);
    }

    private sealed class ThrowingReconnectConnector : ITransportConnector
    {
        public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) =>
            throw new InvalidOperationException("the connector refused to start");
    }
}
