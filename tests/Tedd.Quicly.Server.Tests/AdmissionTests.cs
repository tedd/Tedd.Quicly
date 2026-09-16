using System.Net;
using System.Text;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

public class AdmissionTests
{
    [Fact]
    public async Task Fresh_Client_Is_Admitted_With_A_Session_Token()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.ConnectAdmitted();
        Assert.Equal(1u, client.Epoch);
        Assert.NotEqual(0UL, client.SessionId);
        Assert.Equal(SessionTokenAuthority.TokenLength, client.SessionToken.Length);
        AdmittedPeer admitted = Assert.Single(f.Admitted);
        Assert.Equal(client.SessionId, admitted.SessionId);
        Assert.Equal(1u, admitted.Epoch);
        PeerSlot slot = f.Server.Peers[admitted.Index];
        Assert.Equal(PeerSlotState.Admitted, slot.State);
        Assert.True(slot.IsOccupied);
        Assert.Same(admitted.Peer, f.Server.GetPeer(admitted.Index));
        Assert.Same(admitted.Peer, f.Server.GetPeer(admitted.Index, slot.Generation));
        Assert.Null(f.Server.GetPeer(admitted.Index, slot.Generation + 1));
        Assert.Null(f.Server.GetPeer(-1));
        Assert.Null(f.Server.GetPeer(10_000));
        Assert.Null(f.Server.GetPeer(-1, 1));
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(1, statistics.AdmittedPeers);
        Assert.Equal(1, statistics.Connections);
        Assert.Equal(0, statistics.UnadmittedConnections);
        Assert.Equal(1, statistics.Sessions);
        Assert.Equal(1, statistics.SessionsCreated);
        Assert.Equal(1, statistics.ConnectionsAccepted);
        Assert.True(statistics.PollAllCalls > 0);
        Assert.True(statistics.PeersPolled > 0);
    }

    [Fact]
    public async Task Validator_Sees_The_Token_And_The_Connection()
    {
        byte[]? seen = null;
        bool resume = true;
        ulong session = 1;
        ulong tag = 1;
        IPEndPoint? remote = null;
        QuiclyPeer? peer = null;
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
        {
            seen = c.AuthToken.ToArray();
            resume = c.IsResume;
            session = c.SessionId;
            tag = c.SessionTag;
            remote = c.RemoteEndPoint;
            peer = c.Peer;
            return AuthTokenDecision.Accept;
        });
        f.ConnectAdmitted("alice");
        Assert.Equal("alice", Encoding.UTF8.GetString(seen!));
        Assert.False(resume);
        Assert.Equal(0UL, session);
        Assert.Equal(0UL, tag);
        Assert.NotNull(remote);
        Assert.Same(f.Admitted[0].Peer, peer);
    }

    [Fact]
    public async Task Rejected_Token_Gets_The_Undistinguished_Status_And_Is_Reported()
    {
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = static (in AuthTokenContext _) => AuthTokenDecision.Reject);
        QuiclyPeer client = f.Connect("mallory");
        Assert.True(f.RunUntil(() => client.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, client.HandshakeStatus);
        Assert.Equal(QuiclyErrorCode.AdmissionRejected, client.CloseReason.Code);
        Assert.Null(client.CloseReason.Reason);
        AdmissionFailure failure = Assert.Single(f.FailuresOf(AdmissionFailureReason.AuthTokenRejected));
        Assert.Equal(AdmissionStage.Hello, failure.Stage);
        Assert.True(failure.IsTokenFailure);
        Assert.NotNull(failure.RemoteEndPoint);
        Assert.Empty(f.Admitted);
        Assert.True(f.RunUntil(() => f.Server.PeerCount == 0));
        Assert.Empty(f.Closed);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(1, statistics.AdmissionsRejected);
    }

    [Fact]
    public async Task Unknown_Validator_Answers_Are_Rejections()
    {
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = static (in AuthTokenContext _) => (AuthTokenDecision)77);
        QuiclyPeer client = f.Connect();
        Assert.True(f.RunUntil(() => client.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, client.HandshakeStatus);
    }

    [Fact]
    public async Task Throwing_Validator_Answers_Internal_Error()
    {
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = static (in AuthTokenContext _) => throw new InvalidOperationException("boom"));
        QuiclyPeer client = f.Connect();
        Assert.True(f.RunUntil(() => client.State == PeerState.Closed));
        Assert.Equal(HelloStatus.InternalError, client.HandshakeStatus);
        AdmissionFailure failure = Assert.Single(f.FailuresOf(AdmissionFailureReason.PolicyFault));
        Assert.IsType<InvalidOperationException>(failure.Exception);
        Assert.False(failure.IsTokenFailure);
    }

    [Fact]
    public async Task Pending_Validation_Completes_From_Another_Thread()
    {
        QuiclyPeer? pending = null;
        byte[]? copied = null;
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
        {
            pending = c.Peer;
            copied = c.AuthToken.ToArray();
            return AuthTokenDecision.Pending;
        });
        QuiclyPeer client = f.Connect("slow");
        Assert.True(f.RunUntil(() => pending is not null));
        f.Run(50_000);
        Assert.Equal(PeerState.Handshaking, client.State);
        Assert.Equal(PeerSlotState.Handshaking, f.Server.Peers[pending!.Index].State);
        Assert.Equal("slow", Encoding.UTF8.GetString(copied!));
        await Task.Run(() => f.Server.CompleteAdmission(pending, accepted: true), TestContext.Current.CancellationToken);
        Assert.True(f.RunUntil(() => client.State == PeerState.Connected));
        Assert.Single(f.Admitted);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(1, statistics.AdmissionsPending);

        // A second decision for the same connection is ignored.
        f.Server.CompleteAdmission(pending, accepted: false);
        f.Run(10_000);
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public async Task Pending_Validation_Can_Reject_With_A_Server_Side_Reason()
    {
        QuiclyPeer? pending = null;
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
        {
            pending = c.Peer;
            return AuthTokenDecision.Pending;
        });
        QuiclyPeer client = f.Connect();
        Assert.True(f.RunUntil(() => pending is not null));
        f.Server.CompleteAdmission(pending!, accepted: false, reason: "token expired upstream");
        Assert.True(f.RunUntil(() => client.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, client.HandshakeStatus);
        Assert.Null(client.CloseReason.Reason);
        Assert.Equal("token expired upstream", Assert.Single(f.FailuresOf(AdmissionFailureReason.AuthTokenRejected)).Detail);
    }

    [Fact]
    public async Task Pending_Validation_That_Never_Completes_Times_Out()
    {
        QuiclyPeer? pending = null;
        await using ServerFixture f = new(o =>
        {
            o.PeerOptions.AdmissionTimeout = TimeSpan.FromMilliseconds(200);
            o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
            {
                pending = c.Peer;
                return AuthTokenDecision.Pending;
            };
        });
        QuiclyPeer client = f.Connect();
        Assert.True(f.RunUntil(() => client.State == PeerState.Closed, maxMicros: 2_000_000));
        Assert.Equal(QuiclyErrorCode.Timeout, client.CloseReason.Code);
        Assert.True(f.RunUntil(() => f.Server.PeerCount == 0));

        // A decision arriving after the connection was released is ignored.
        f.Server.CompleteAdmission(pending!, accepted: true);
        f.Run(5_000);
        Assert.Empty(f.Admitted);
        Assert.Throws<ArgumentNullException>(() => f.Server.CompleteAdmission(null!, true));
        Assert.Throws<ArgumentNullException>(() => f.Server.CompleteAdmission(null!, AdmissionResult.Accept()));
        Assert.Throws<ArgumentException>(() => f.Server.CompleteAdmission(pending!, AdmissionResult.Pending));
    }

    [Fact]
    public async Task Pending_Decision_After_Shutdown_Began_Is_Refused()
    {
        QuiclyPeer? pending = null;
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
        {
            pending = c.Peer;
            return AuthTokenDecision.Pending;
        });
        QuiclyPeer client = f.Connect();
        Assert.True(f.RunUntil(() => pending is not null));
        f.Server.BeginShutdown(); // closes the waiting connection too
        f.Server.CompleteAdmission(pending!, accepted: true);
        f.Server.PollAll();
        Assert.True(f.RunUntil(() => client.State == PeerState.Closed));
        Assert.Equal("server shutting down", client.CloseReason.Reason);
        Assert.Empty(f.Admitted);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(0, statistics.SessionsCreated);
    }

    [Fact]
    public async Task Alpn_And_Server_Name_Are_Checked_Before_The_Handshake()
    {
        await using ServerFixture f = new(o => o.Admission.AllowedServerNames.Add("game.test"));
        f.Connector.Alpn = "h3";
        QuiclyPeer wrongAlpn = f.Connect();
        Assert.True(f.RunUntil(() => wrongAlpn.State == PeerState.Closed));
        Assert.Equal(CloseSource.Transport, wrongAlpn.CloseReason.Source);
        Assert.Equal(SimulatedTransport.StatusConnectionRefused, wrongAlpn.CloseReason.TransportStatus);
        Assert.Equal(AdmissionStage.PreHandshake, Assert.Single(f.FailuresOf(AdmissionFailureReason.AlpnNotAllowed)).Stage);

        f.Connector.Alpn = ServerAdmissionOptions.DefaultAlpn;
        QuiclyPeer wrongName = f.Connect(serverName: "evil.test");
        QuiclyPeer noName = f.Connect(serverName: null);
        QuiclyPeer upperCase = f.Connect(serverName: "GAME.TEST");
        Assert.True(f.RunUntil(() => wrongName.State == PeerState.Closed && noName.State == PeerState.Closed && upperCase.State == PeerState.Connected));
        Assert.Equal(2, f.FailuresOf(AdmissionFailureReason.ServerNameNotAllowed).Count);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(3, statistics.ConnectionsRefused);
    }

    [Fact]
    public async Task Empty_Alpn_List_Accepts_Any_Alpn()
    {
        await using ServerFixture f = new(o => o.Admission.AllowedAlpns.Clear());
        f.Connector.Alpn = "anything";
        f.ConnectAdmitted();
    }

    [Fact]
    public async Task Unadmitted_Connections_Are_Capped()
    {
        await using ServerFixture f = new(o =>
        {
            o.Admission.MaxUnadmittedConnections = 2;
            o.PeerOptions.AdmissionTimeout = TimeSpan.FromMilliseconds(300);
        });
        RecordingSink silent1 = new();
        RecordingSink silent2 = new();
        RecordingSink refused = new();
        using ITransport t1 = f.Connector.Connect(f.SimListener.LocalEndPoint, "game.test", silent1);
        using ITransport t2 = f.Connector.Connect(f.SimListener.LocalEndPoint, "game.test", silent2);
        f.Run(5_000);
        using ITransport t3 = f.Connector.Connect(f.SimListener.LocalEndPoint, "game.test", refused);
        f.Run(5_000);
        Assert.True(refused.IsClosed);
        Assert.Single(f.FailuresOf(AdmissionFailureReason.TooManyUnadmittedConnections));
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(2, statistics.UnadmittedConnections);

        // The silent connections time out and free their places.
        Assert.True(f.RunUntil(() => f.Server.PeerCount == 0, maxMicros: 2_000_000));
        f.ConnectAdmitted();
    }

    [Fact]
    public async Task A_Full_Slot_Table_Refuses_Before_The_Handshake()
    {
        await using ServerFixture f = new(o =>
        {
            o.MaxPeers = 1;
            o.Admission.MaxUnadmittedConnections = 1;
        });
        f.ConnectAdmitted();
        using ITransport silent = f.Connector.Connect(f.SimListener.LocalEndPoint, "game.test", new RecordingSink());
        f.Run(5_000);
        QuiclyPeer third = f.Connect();
        Assert.True(f.RunUntil(() => third.State == PeerState.Closed));
        Assert.Single(f.FailuresOf(AdmissionFailureReason.ServerAtCapacity));
    }

    [Fact]
    public async Task Hello_Beyond_Max_Peers_Is_Answered_Server_Full()
    {
        await using ServerFixture f = new(o => o.MaxPeers = 1);
        f.ConnectAdmitted();
        QuiclyPeer second = f.Connect();
        Assert.True(f.RunUntil(() => second.State == PeerState.Closed));
        Assert.Equal(HelloStatus.ServerFull, second.HandshakeStatus);
        Assert.Single(f.FailuresOf(AdmissionFailureReason.ServerFull));
    }

    [Fact]
    public async Task Connections_Per_Address_Are_Limited()
    {
        ManualListener listener = new();
        await using ServerFixture f = new(o => o.Admission.MaxConnectionsPerAddress = 2, listener: listener);
        PairConnector connector = new(f.Network, listener);
        QuiclyPeer a = f.Connect(connector: connector);
        QuiclyPeer b = f.Connect(connector: connector);
        QuiclyPeer c = f.Connect(connector: connector);
        Assert.Equal(PreHandshakeDecision.Reject, connector.LastDecision);
        Assert.True(f.RunUntil(() => a.State == PeerState.Connected && b.State == PeerState.Connected && c.State == PeerState.Closed));
        Assert.Single(f.FailuresOf(AdmissionFailureReason.TooManyConnectionsFromAddress));

        // The IPv4-mapped form is the same address; another address is not affected.
        connector.RemoteAddress = new IPEndPoint(IPAddress.Parse("::ffff:192.0.2.10"), 1);
        f.Connect(connector: connector);
        Assert.Equal(PreHandshakeDecision.Reject, connector.LastDecision);
        connector.RemoteAddress = new IPEndPoint(IPAddress.Parse("192.0.2.11"), 1);
        QuiclyPeer d = f.Connect(connector: connector);
        Assert.Equal(PreHandshakeDecision.Accept, connector.LastDecision);
        Assert.True(f.RunUntil(() => d.State == PeerState.Connected));

        // Closing one frees a place for its address.
        a.Close();
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        connector.RemoteAddress = new IPEndPoint(IPAddress.Parse("192.0.2.10"), 2);
        QuiclyPeer e = f.Connect(connector: connector);
        Assert.True(f.RunUntil(() => e.State == PeerState.Connected));
    }

    [Fact]
    public async Task Ipv6_Addresses_Are_Counted_Per_Prefix()
    {
        ManualListener listener = new();
        await using ServerFixture f = new(o => o.Admission.MaxConnectionsPerAddress = 1, listener: listener);
        PairConnector connector = new(f.Network, listener) { RemoteAddress = new IPEndPoint(IPAddress.Parse("2001:db8:1:2::1"), 1) };
        QuiclyPeer first = f.Connect(connector: connector);
        connector.RemoteAddress = new IPEndPoint(IPAddress.Parse("2001:db8:1:2::ffff"), 1);
        f.Connect(connector: connector);
        Assert.Equal(PreHandshakeDecision.Reject, connector.LastDecision);
        connector.RemoteAddress = new IPEndPoint(IPAddress.Parse("2001:db8:1:3::1"), 1);
        f.Connect(connector: connector);
        Assert.Equal(PreHandshakeDecision.Accept, connector.LastDecision);
        Assert.True(f.RunUntil(() => first.State == PeerState.Connected));
    }

    [Fact]
    public async Task Accept_Rechecks_The_Limits_And_Survives_Bad_Transports()
    {
        ManualListener listener = new();
        await using ServerFixture f = new(o => o.Admission.MaxConnectionsPerAddress = 1, listener: listener);
        PairConnector connector = new(f.Network, listener);
        QuiclyPeer first = f.Connect(connector: connector);
        Assert.True(connector.LastAccepted);

        // Accept without the pre-handshake check (as if two connections raced): the limit holds.
        NewConnectionInfo info = default;
        info.RemoteEndPoint = connector.RemoteAddress;
        (SimulatedTransport _, SimulatedTransport server) = f.Network.CreatePair(new RecordingSink(), new RecordingSink());
        Assert.Null(listener.Accept!(server, in info));
        f.Server.PollAll(); // raises the refusal the listener's callback queued
        Assert.Single(f.FailuresOf(AdmissionFailureReason.TooManyConnectionsFromAddress));

        // A transport the peer refuses: the reservation is undone.
        info.RemoteEndPoint = new IPEndPoint(IPAddress.Parse("198.51.100.7"), 1);
        Assert.Null(listener.Accept!(null!, in info));
        f.Server.PollAll();
        AdmissionFailure failure = Assert.Single(f.FailuresOf(AdmissionFailureReason.PeerCreationFailed));
        Assert.IsType<ArgumentNullException>(failure.Exception);
        Assert.Equal(1, f.Server.PeerCount);

        // A connection without a known address is admitted without per-address accounting.
        connector.RemoteAddress = null!;
        QuiclyPeer anonymous = f.Connect(connector: connector);
        Assert.True(f.RunUntil(() => first.State == PeerState.Connected && anonymous.State == PeerState.Connected));
    }

    [Fact]
    public async Task Moving_To_A_Full_Address_Closes_The_Connection()
    {
        ManualListener listener = new();
        await using ServerFixture f = new(o => o.Admission.MaxConnectionsPerAddress = 1, listener: listener);
        PairConnector connector = new(f.Network, listener);
        QuiclyPeer home = f.ConnectAdmitted(connector: connector);
        connector.RemoteAddress = new IPEndPoint(IPAddress.Parse("192.0.2.20"), 1);
        QuiclyPeer roaming = f.ConnectAdmitted(connector: connector);
        ITransportSink serverSink = connector.LastServerSink!.Target!;

        TransportConnectedInfo moved = default;
        moved.RemoteEndPoint = new IPEndPoint(IPAddress.Parse("192.0.2.20"), 9);
        serverSink.OnPeerAddressChanged(in moved); // same address key: nothing changes
        moved.RemoteEndPoint = null;
        serverSink.OnPeerAddressChanged(in moved); // unknown address: uncounted
        moved.RemoteEndPoint = new IPEndPoint(IPAddress.Parse("192.0.2.30"), 9);
        serverSink.OnPeerAddressChanged(in moved);
        f.Run(5_000);
        Assert.Equal(PeerState.Connected, roaming.State);

        moved.RemoteEndPoint = new IPEndPoint(IPAddress.Parse("192.0.2.10"), 9); // the address of `home`, which is full
        serverSink.OnPeerAddressChanged(in moved);
        Assert.True(f.RunUntil(() => roaming.State == PeerState.Closed));
        Assert.Equal(QuiclyErrorCode.LimitExceeded, roaming.CloseReason.Code);
        Assert.Equal(PeerState.Connected, home.State);
    }

    [Fact]
    public async Task Token_Failures_Rate_Limit_The_Address()
    {
        ManualListener listener = new();
        await using ServerFixture f = new(o =>
        {
            o.Admission.AuthFailureBurst = 3;
            o.Admission.AuthFailureRefillInterval = TimeSpan.FromSeconds(10);
            o.Admission.AuthTokenValidator = static (in AuthTokenContext c) => c.AuthToken.SequenceEqual("good"u8) ? AuthTokenDecision.Accept : AuthTokenDecision.Reject;
        }, listener: listener);
        PairConnector connector = new(f.Network, listener);
        for (int i = 0; i < 3; i++)
        {
            QuiclyPeer client = f.Connect("bad", connector: connector);
            Assert.True(f.RunUntil(() => client.State == PeerState.Closed));
            Assert.Equal(HelloStatus.Rejected, client.HandshakeStatus);
        }

        QuiclyPeer blocked = f.Connect("good", connector: connector);
        Assert.Equal(PreHandshakeDecision.Reject, connector.LastDecision);
        f.Server.PollAll(); // raises the refusal the pre-handshake callback queued
        Assert.Equal(AdmissionStage.PreHandshake, Assert.Single(f.FailuresOf(AdmissionFailureReason.AddressRateLimited)).Stage);
        Assert.True(f.RunUntil(() => blocked.State == PeerState.Closed));

        connector.RemoteAddress = new IPEndPoint(IPAddress.Parse("192.0.2.99"), 1);
        f.ConnectAdmitted("good", connector: connector);

        // One refill interval later the blocked address may try again.
        f.Run(10_000_000, step: 100_000);
        connector.RemoteAddress = new IPEndPoint(IPAddress.Parse("192.0.2.10"), 1);
        f.ConnectAdmitted("good", connector: connector);
    }

    [Fact]
    public async Task An_Address_Blocked_After_Its_Handshake_Is_Refused_At_Hello()
    {
        ManualListener listener = new();
        await using ServerFixture f = new(o => o.Admission.AuthFailureBurst = 1, listener: listener);
        PairConnector connector = new(f.Network, listener);
        QuiclyPeer client = f.Connect(connector: connector);
        Assert.True(connector.LastAccepted);
        f.Server.RateLimiter.RecordFailure(connector.RemoteAddress.Address);
        Assert.True(f.RunUntil(() => client.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, client.HandshakeStatus);
        Assert.Equal(AdmissionStage.Hello, Assert.Single(f.FailuresOf(AdmissionFailureReason.AddressRateLimited)).Stage);
    }

    [Fact]
    public async Task Stopping_Server_Refuses_New_Connections()
    {
        ManualListener listener = new();
        await using ServerFixture f = new(listener: listener);
        PairConnector connector = new(f.Network, listener);
        QuiclyPeer admitted = f.ConnectAdmitted(connector: connector);
        QuiclyPeer serverPeer = f.ServerPeerOf(admitted);
        f.Server.BeginShutdown();
        Assert.Equal(1, listener.StopCalls);
        connector.RemoteAddress = new IPEndPoint(IPAddress.Parse("192.0.2.50"), 1);
        f.Connect(connector: connector);
        Assert.Equal(PreHandshakeDecision.Reject, connector.LastDecision);
        f.Server.PollAll(); // raises the refusal the pre-handshake callback queued
        Assert.Single(f.FailuresOf(AdmissionFailureReason.ServerStopping));

        NewConnectionInfo info = default;
        (SimulatedTransport _, SimulatedTransport server) = f.Network.CreatePair(new RecordingSink(), new RecordingSink());
        Assert.Null(listener.Accept!(server, in info));

        HelloInfo hello = new() { RemoteEndPoint = serverPeer.RemoteEndPoint };
        AdmissionResult late = f.Server.DefaultAdmissionPolicy.Admit(in hello, serverPeer);
        Assert.Equal(AdmissionDecision.Reject, late.Decision);
        Assert.Equal(HelloStatus.ServerFull, late.Status);
        bool threw = false;
        try
        {
            f.Server.DefaultAdmissionPolicy.Admit(in hello, null!);
        }
        catch (ArgumentNullException)
        {
            threw = true;
        }

        Assert.True(threw);
    }

    [Fact]
    public async Task Custom_Policy_Accepts_Without_Sessions()
    {
        ScriptedPolicy policy = new();
        await using ServerFixture f = new(start: false);
        f.Server.AdmissionPolicy = policy;
        Assert.Same(policy, f.Server.AdmissionPolicy);
        await f.Server.StartAsync(TestContext.Current.CancellationToken);
        Assert.Throws<InvalidOperationException>(() => f.Server.AdmissionPolicy = policy);
        Assert.Throws<InvalidOperationException>(() => f.Server.StartAsync(TestContext.Current.CancellationToken).GetAwaiter().GetResult());
        QuiclyPeer client = f.ConnectAdmitted();
        Assert.Equal(1, policy.PreHandshakes);
        Assert.Equal(1, policy.Admits);
        Assert.NotEqual(0UL, client.SessionId);
        Assert.True(client.SessionToken.IsEmpty);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(0, statistics.Sessions);
        Assert.Equal(1, statistics.AdmittedPeers);
    }

    [Fact]
    public async Task Custom_Policy_Refusals_Faults_And_Pending_Decisions()
    {
        ScriptedPolicy policy = new();
        await using ServerFixture f = new(start: false);
        Assert.Throws<ArgumentNullException>(() => f.Server.AdmissionPolicy = null!);
        f.Server.AdmissionPolicy = policy;
        await f.Server.StartAsync(TestContext.Current.CancellationToken);

        // The simulated connector runs the pre-handshake callback at the next network step, so step after each connect.
        policy.Pre = () => PreHandshakeDecision.Reject;
        QuiclyPeer dropped = f.Connect();
        f.Run(1_000);
        policy.Pre = () => throw new InvalidOperationException("pre");
        QuiclyPeer faultedEarly = f.Connect();
        f.Run(1_000);
        Assert.True(f.RunUntil(() => dropped.State == PeerState.Closed && faultedEarly.State == PeerState.Closed));
        Assert.Equal(AdmissionStage.PreHandshake, Assert.Single(f.FailuresOf(AdmissionFailureReason.PolicyFault)).Stage);
        policy.Pre = null;

        policy.Hello = _ => AdmissionResult.Reject(HelloStatus.ServerFull, "try later");
        QuiclyPeer full = f.Connect();
        Assert.True(f.RunUntil(() => full.State == PeerState.Closed));
        Assert.Equal(HelloStatus.ServerFull, full.HandshakeStatus);
        Assert.Equal("try later", full.CloseReason.Reason);

        policy.Hello = _ => throw new InvalidOperationException("hello");
        QuiclyPeer faulted = f.Connect();
        Assert.True(f.RunUntil(() => faulted.State == PeerState.Closed));
        Assert.Equal(HelloStatus.InternalError, faulted.HandshakeStatus);
        Assert.Equal(2, f.FailuresOf(AdmissionFailureReason.PolicyFault).Count);

        QuiclyPeer? pending = null;
        policy.Hello = peer =>
        {
            pending = peer;
            return AdmissionResult.Pending;
        };
        QuiclyPeer waiting = f.Connect();
        Assert.True(f.RunUntil(() => pending is not null));
        f.Server.CompleteAdmission(pending!, AdmissionResult.Accept());
        Assert.True(f.RunUntil(() => waiting.State == PeerState.Connected));

        pending = null;
        QuiclyPeer waitingToo = f.Connect();
        Assert.True(f.RunUntil(() => pending is not null));
        f.Server.CompleteAdmission(pending!, accepted: false, "no");
        Assert.True(f.RunUntil(() => waitingToo.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, waitingToo.HandshakeStatus);

        pending = null;
        QuiclyPeer waitingThree = f.Connect();
        Assert.True(f.RunUntil(() => pending is not null));
        f.Server.CompleteAdmission(pending!, accepted: true);
        Assert.True(f.RunUntil(() => waitingThree.State == PeerState.Connected));
    }

    [Fact]
    public async Task Server_Enforces_Capacity_On_Custom_Accepts()
    {
        ScriptedPolicy policy = new();
        await using ServerFixture f = new(o => o.MaxPeers = 1, start: false);
        f.Server.AdmissionPolicy = policy;
        await f.Server.StartAsync(TestContext.Current.CancellationToken);
        f.ConnectAdmitted();
        QuiclyPeer second = f.Connect();
        Assert.True(f.RunUntil(() => second.State == PeerState.Closed));
        Assert.Equal(HelloStatus.ServerFull, second.HandshakeStatus);

        QuiclyPeer? pending = null;
        policy.Hello = peer =>
        {
            pending = peer;
            return AdmissionResult.Pending;
        };
        QuiclyPeer third = f.Connect();
        Assert.True(f.RunUntil(() => pending is not null));
        f.Server.CompleteAdmission(pending!, AdmissionResult.Accept());
        Assert.True(f.RunUntil(() => third.State == PeerState.Closed));
        Assert.Equal(HelloStatus.ServerFull, third.HandshakeStatus);
    }

    [Fact]
    public async Task Failure_Handlers_That_Throw_Are_Counted()
    {
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = static (in AuthTokenContext _) => AuthTokenDecision.Reject);
        f.Server.AdmissionFailed += _ => throw new InvalidOperationException("handler");
        QuiclyPeer client = f.Connect();
        Assert.True(f.RunUntil(() => client.State == PeerState.Closed));
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(1, statistics.EventHandlerFaults);
    }

    /// <summary>A policy the test scripts.</summary>
    private sealed class ScriptedPolicy : IAdmissionPolicy
    {
        public Func<PreHandshakeDecision>? Pre { get; set; }

        public Func<QuiclyPeer, AdmissionResult>? Hello { get; set; }

        public int PreHandshakes { get; private set; }

        public int Admits { get; private set; }

        public PreHandshakeDecision PreHandshake(in NewConnectionInfo info)
        {
            PreHandshakes++;
            return Pre?.Invoke() ?? PreHandshakeDecision.Accept;
        }

        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer)
        {
            Admits++;
            return Hello?.Invoke(peer) ?? AdmissionResult.Accept();
        }
    }
}
