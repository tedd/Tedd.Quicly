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
    public void Transport_Outcome_Counters_Are_Kept_Across_A_Reconnect_And_Keep_Counting()
    {
        // 1 Mbit/s: a 1 000-byte datagram keeps the link busy for 8 ms, so a second one sent in the same instant is dropped
        // by the transport (CancelOnBlocked).
        using SessionHarness h = new(link: new LinkOptions { BandwidthBitsPerSecond = 1_000_000 }, connect: false,
            client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer oldServer = h.Server!;
        QuiclyPeer client = h.Client;
        h.Run(50_000);

        void SendTwoAtOnce()
        {
            SendResult first = client.SendCopy(new SendHeader(2), new byte[1_000], SendOptions.Tracked);
            client.Flush();
            SendResult second = client.SendCopy(new SendHeader(2), new byte[1_000], SendOptions.Tracked);
            client.Flush();
            Assert.True(h.RunUntil(() => client.GetDeliveryStatus(first.Token) != DeliveryStatus.Pending
                && client.GetDeliveryStatus(second.Token) != DeliveryStatus.Pending));
            Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(first.Token));
            Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(second.Token));
        }

        SendTwoAtOnce();
        client.GetStatistics(out PeerStatistics before);
        Assert.True(client.GetChannelStatistics(2, out ChannelStatistics channelBefore));
        Assert.Equal(1, before.DatagramsCanceled);
        Assert.Equal(1, before.DatagramsAcknowledged);
        Assert.Equal(1, channelBefore.TransportCanceled);

        CutTheConnection(h);
        Assert.True(h.RunUntil(() => client.State == PeerState.Closed));
        client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.True(h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();
        h.Run(50_000);

        // Totals since the peer was created: the reconnect reset nothing.
        client.GetStatistics(out PeerStatistics resumed);
        Assert.True(client.GetChannelStatistics(2, out ChannelStatistics channelResumed));
        Assert.Equal(1, resumed.DatagramsCanceled);
        Assert.Equal(1, resumed.DatagramsAcknowledged);
        Assert.Equal(0, resumed.DatagramsLost);
        Assert.Equal(1, channelResumed.TransportCanceled);
        Assert.Equal(0, channelResumed.TransportLost);

        SendTwoAtOnce();
        client.GetStatistics(out PeerStatistics after);
        Assert.True(client.GetChannelStatistics(2, out ChannelStatistics channelAfter));
        Assert.Equal(2, after.DatagramsCanceled);
        Assert.Equal(2, after.DatagramsAcknowledged);
        Assert.Equal(2, channelAfter.TransportCanceled);
        Assert.Equal(4, channelAfter.Sent);
    }

    [Fact]
    public void An_Expiry_Waiting_For_Its_First_Pass_Does_Not_Outlive_The_Lost_Connection()
    {
        using SessionHarness h = new(connect: false, client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer oldServer = h.Server!;

        // Channel 3 has the default expiry; the message is queued and no scheduler pass follows, so its deadline is still
        // waiting for one when the connection is lost.
        SendResult pending = h.Client.SendCopy(new SendHeader(3, 1), [1], SendOptions.Tracked);
        Assert.True(pending.IsAdmitted);
        Assert.True(h.Client.Core.HasPendingExpiry);
        CutTheConnection(h);
        h.Network.Advance(500_000);

        h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.Equal(DeliveryStatus.Disconnected, h.Client.GetDeliveryStatus(pending.Token));
        Assert.False(h.Client.Core.HasPendingExpiry);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();

        // The resumed connection expires nothing it should not: a new expiring send is delivered.
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(3, Handlers.Collect(got));
        SendResult next = h.Client.SendCopy(new SendHeader(3, 1), [2], SendOptions.Tracked);
        Assert.True(next.IsAdmitted);
        h.Client.Flush();
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(next.Token) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(next.Token));
        Assert.Equal(new byte[] { 2 }, Assert.Single(got).Payload);
        Assert.True(h.Client.GetChannelStatistics(3, out ChannelStatistics statistics));
        Assert.Equal(0, statistics.Expired);
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
    public void A_Connector_That_Throws_Leaves_The_Peer_Closed_And_Re_Armable()
    {
        using SessionHarness h = new(connect: false, client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        CutTheConnection(h);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        CloseReason lost = h.Client.CloseReason;
        Assert.True(h.Client.CanReconnect);

        Assert.Throws<InvalidOperationException>(() => h.Client.Reconnect(new ThrowingReconnectConnector(), h.Listener.LocalEndPoint, null, default));
        Assert.Equal(PeerState.Closed, h.Client.State);
        // No transport, so nothing can call back and Poll stays a no-op.
        Assert.Equal(0, h.Client.Poll());
        Assert.False(h.Client.HasPendingWork);
        // The peer is where the lost connection left it: the close is still observable, so it can be re-armed again.
        Assert.True(h.Client.Core.IsTransportClosed);
        Assert.True(h.Client.CanReconnect);
        Assert.Equal(lost, h.Client.CloseReason);
        Assert.Equal(HelloStatus.Accepted, h.Client.HandshakeStatus);
        Assert.Equal(long.MaxValue, h.Client.NextDeadlineMicros);
        Assert.DoesNotContain((PeerState.Closed, PeerState.Reconnecting), h.ClientEvents);
    }

    [Fact]
    public void A_Connector_That_Throws_Once_Resumes_In_Place_On_The_Second_Attempt()
    {
        using SessionHarness h = new(connect: false, authToken: "secret"u8.ToArray(), client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer oldServer = h.Server!;
        h.Client.Index = 11;
        h.Client.Tag = 0xABCD;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Client.RegisterHandler(2, Handlers.Collect(got));
        oldServer.SendCopy(new SendHeader(2), [1]);
        h.Run(200_000, 1_000);
        Assert.Single(got);
        h.Client.GetStatistics(out PeerStatistics before);
        Assert.True(before.DatagramsReceived > 0);

        CutTheConnection(h);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        FailingConnector connector = new(h.Connector, failures: 1);
        Assert.Throws<InvalidOperationException>(() => h.Client.Reconnect(connector, h.Listener.LocalEndPoint, "test", "secret"u8));
        Assert.True(h.Client.CanReconnect);
        h.Client.GetStatistics(out PeerStatistics afterFailure);
        Assert.Equal(before.DatagramsReceived, afterFailure.DatagramsReceived);

        // The second attempt reconnects the same peer in place and resumes the session.
        h.Client.Reconnect(connector, h.Listener.LocalEndPoint, "test", "secret"u8);
        Assert.Equal(PeerState.Reconnecting, h.Client.State);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();
        Assert.Equal(2, connector.Attempts);

        Assert.Equal(2u, h.Client.Epoch);
        Assert.Equal(SessionId, h.Client.SessionId);
        Assert.Equal(SessionToken, h.Admission.LastSessionToken);
        Assert.Equal("secret"u8.ToArray(), h.Admission.LastAuthToken);
        Assert.False(h.Client.CanReconnect);

        // Identity, handlers and statistics survived the failed attempt as well as the resume.
        Assert.Equal(11, h.Client.Index);
        Assert.Equal(0xABCDul, h.Client.Tag);
        h.Client.GetStatistics(out PeerStatistics after);
        Assert.True(after.DatagramsReceived >= before.DatagramsReceived);
        Assert.Equal(0, h.Client.Core.Segments.Used);
        h.Server!.SendCopy(new SendHeader(2), [2]);
        h.Run(200_000, 1_000);
        Assert.Equal(2, got.Count);
        Assert.Equal(2, got[1].Payload[0]);
        Assert.Equal(2u, got[1].Header.Epoch);
    }

    [Fact]
    public void A_Connector_That_Keeps_Throwing_Keeps_The_Peer_Re_Armable_And_Leaks_Nothing()
    {
        using SessionHarness h = new(connect: false, client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer oldServer = h.Server!;
        SendResult pending = h.Client.SendCopy(new SendHeader(4), new byte[256], SendOptions.Tracked);
        Assert.True(pending.IsAdmitted);
        CutTheConnection(h);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));

        FailingConnector connector = new(h.Connector, failures: 3);
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            // Every failure reaches the caller and leaves the peer exactly as re-armable as before it.
            Assert.Throws<InvalidOperationException>(() => h.Client.Reconnect(connector, h.Listener.LocalEndPoint, "test", default));
            Assert.True(h.Client.CanReconnect, $"attempt {attempt} left the peer unable to reconnect");
            Assert.Equal(PeerState.Closed, h.Client.State);
            Assert.True(h.Client.Core.IsTransportClosed);
            Assert.Equal(0, h.Client.Poll());
            h.Client.Flush();
        }

        // Nothing of the lost connection is still held: the tracked send is done and every table is empty.
        Assert.Equal(DeliveryStatus.Disconnected, h.Client.GetDeliveryStatus(pending.Token));
        h.Client.GetStatistics(out PeerStatistics statistics);
        Assert.Equal(0, statistics.SendEntriesInUse);
        Assert.Equal(0, statistics.SendBytesOutstanding);
        Assert.Equal(0, statistics.ReceiveBytesOutstanding);
        Assert.Equal(0, h.Client.Core.Segments.Used);
        Assert.Equal(0, h.Client.Core.Streams.Count);
        oldServer.Dispose();

        // And Dispose is still clean: without a transport the close counts as seen, so the native memory goes at once.
        h.DisposeClient();
        Assert.True(h.Client.IsFreed);
        Assert.False(h.Client.CanReconnect);
    }

    [Fact]
    public void A_Connector_That_Returns_No_Transport_Leaves_The_Peer_Re_Armable()
    {
        using SessionHarness h = new(connect: false, client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer oldServer = h.Server!;
        CutTheConnection(h);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));

        FailingConnector connector = new(h.Connector, failures: 1, returnNull: true);
        Assert.Throws<InvalidOperationException>(() => h.Client.Reconnect(connector, h.Listener.LocalEndPoint, "test", default));
        Assert.True(h.Client.CanReconnect);
        Assert.Equal(PeerState.Closed, h.Client.State);
        Assert.True(h.Client.Core.IsTransportClosed);

        h.Client.Reconnect(connector, h.Listener.LocalEndPoint, "test", default);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();
        Assert.Equal(2u, h.Client.Epoch);
    }

    [Fact]
    public void A_Failed_Attempt_Still_Reports_The_Loss_To_A_Host_That_Never_Polled()
    {
        using SessionHarness h = new(connect: false, client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer oldServer = h.Server!;
        SendResult pending = h.Client.SendCopy(new SendHeader(4), new byte[128], SendOptions.Tracked);
        Assert.True(pending.IsAdmitted);

        // The transport's close is delivered but never polled, so the failed attempt has to settle the peer itself.
        CutTheConnection(h);
        h.Network.Advance(500_000);
        Assert.True(h.Client.Core.IsTransportClosed);
        Assert.NotEqual(PeerState.Closed, h.Client.State);

        Assert.Throws<InvalidOperationException>(() => h.Client.Reconnect(new ThrowingReconnectConnector(), h.Listener.LocalEndPoint, "test", default));
        Assert.Equal(DeliveryStatus.Disconnected, h.Client.GetDeliveryStatus(pending.Token));
        Assert.True(h.Client.CanReconnect);
        Assert.Equal(PeerState.Closed, h.Client.State);

        // The Closed transition the host had not seen yet is still raised, exactly once, and no Reconnecting was invented.
        h.Client.Poll();
        h.Client.Poll();
        Assert.Single(h.ClientEvents.FindAll(e => e == (PeerState.Connected, PeerState.Closed)));
        Assert.DoesNotContain((PeerState.Closed, PeerState.Reconnecting), h.ClientEvents);
        Assert.NotEqual(CloseSource.None, h.Client.CloseReason.Source);

        h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();
        Assert.Equal(2u, h.Client.Epoch);
    }

    [Fact]
    public void CanReconnect_Answers_The_Preconditions_Of_An_In_Place_Resume()
    {
        SessionHarness h = new(connect: false, client: QuietOptions.Apply, server: QuietOptions.Apply);
        try
        {
            AcceptResumes(h);
            Assert.True(h.RunUntilConnected());
            // A live connection is not reconnected in place, and a server peer never is (PROTOCOL.md §4.1).
            Assert.False(h.Client.CanReconnect);
            Assert.False(h.Server!.CanReconnect);

            bool insidePoll = true;
            h.Client.StateChanged += (peer, _, to) =>
            {
                if (to == PeerState.Closed)
                {
                    insidePoll = peer.CanReconnect;
                }
            };

            CutTheConnection(h);
            Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
            // Inside Poll a Reconnect would throw, so the probe says no there too.
            Assert.False(insidePoll);
            Assert.True(h.Client.CanReconnect);
            Assert.False(h.Server.CanReconnect);
        }
        finally
        {
            h.Dispose();
        }

        Assert.False(h.Client.CanReconnect);
        Assert.True(h.Client.IsDisposed);
    }

    [Fact]
    public void CanReconnect_Does_Not_Allocate()
    {
        using SessionHarness h = new(connect: false, client: QuietOptions.Apply, server: QuietOptions.Apply);
        AcceptResumes(h);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int permitted = 0;
        AllocationAssert.NoAllocations(() =>
        {
            if (client.CanReconnect)
            {
                permitted++;
            }

            if (server.CanReconnect)
            {
                permitted++;
            }
        });

        Assert.Equal(0, permitted);
        CutTheConnection(h);
        Assert.True(h.RunUntil(() => client.State == PeerState.Closed));
        AllocationAssert.NoAllocations(() =>
        {
            if (client.CanReconnect)
            {
                permitted++;
            }
        });

        Assert.True(permitted > 0);
    }

    private sealed class ThrowingReconnectConnector : ITransportConnector
    {
        public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) =>
            throw new InvalidOperationException("the connector refused to start");
    }

    /// <summary>Fails the first <paramref name="failures"/> attempts — by throwing, or by returning no transport at all.</summary>
    private sealed class FailingConnector(ITransportConnector inner, int failures, bool returnNull = false) : ITransportConnector
    {
        public int Attempts { get; private set; }

        public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
        {
            if (++Attempts <= failures)
            {
                return returnNull ? null! : throw new InvalidOperationException("the connector refused to start");
            }

            return inner.Connect(endpoint, serverName, sink);
        }
    }
}
