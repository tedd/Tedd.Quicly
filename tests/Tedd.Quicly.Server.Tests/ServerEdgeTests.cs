using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Certificates;

namespace Tedd.Quicly.Server.Tests;

public class ServerEdgeTests
{
    [Fact]
    public async Task Internal_Helpers_Ignore_Peers_Of_Other_Hosts()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.ConnectAdmitted(); // a client peer is not in the server's slot table
        Assert.False(f.Server.TryReserve(client, allowOverCapacity: true));
        f.Server.Unreserve(client);
        f.Server.BindSession(client, new SessionRecord());
        f.Server.SetPending(client, new PendingAdmission());
        Assert.Equal(1, f.Server.AdmittedCount);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(0, statistics.UnadmittedConnections);
    }

    [Fact]
    public async Task A_Server_Peer_The_Application_Admitted_Itself_Is_Closed_And_Never_Admitted_By_The_Server()
    {
        QuiclyPeer? pending = null;
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
        {
            pending = c.Peer;
            return AuthTokenDecision.Pending;
        });
        QuiclyPeer client = f.Connect();
        Assert.True(f.RunUntil(() => pending is not null));
        f.Server.CompleteAdmission(pending!, AdmissionResult.Accept());
        pending!.CompleteAdmission(AdmissionResult.Accept()); // bypasses MaxPeers and the sessions: the server closes the peer
        Assert.True(f.RunUntil(() => client.State == PeerState.Closed));
        Assert.Equal(QuiclyErrorCode.InternalError, client.CloseReason.Code);
        Assert.Empty(f.Admitted);
        AdmissionFailure failure = Assert.Single(f.FailuresOf(AdmissionFailureReason.PolicyFault));
        Assert.Contains("QuiclyServer.CompleteAdmission", failure.Detail, StringComparison.Ordinal);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(0, statistics.SessionsCreated);
        Assert.Equal(0, statistics.AdmittedPeers);
    }

    [Fact]
    public async Task Collected_Sets_Are_Pruned()
    {
        await using ServerFixture f = new();
        CreateAndDrop(f.Server);
        Collect();
        PeerSet kept = f.Server.CreateSet(); // prunes the collected set
        QuiclyPeer client = f.ConnectAdmitted();
        kept.Add(f.ServerPeerOf(client));
        CreateAndDrop(f.Server);
        Collect();
        client.Close();
        Assert.True(f.RunUntil(() => f.Closed.Count == 1)); // releasing the slot prunes the collected set as well
        Assert.True(kept.IsEmpty);
        GC.KeepAlive(kept);
    }

    [Fact]
    public async Task Shutdown_Closes_Connections_Accepted_Since_The_Last_Poll()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.Connect();
        f.Network.Advance(1_000); // accepted by the listener's callback, not activated yet
        Assert.Equal(1, f.Server.PeerCount);
        Assert.Equal(0, f.Server.Peers.Length);
        f.Server.BeginShutdown();
        Assert.Equal(1, f.Server.Peers.Length);
        Assert.Equal(PeerState.Closing, f.Server.Peers[0].Peer!.State); // activated and closed at once
        Assert.True(f.RunUntil(() => f.Server.IsShutdownComplete));
        Assert.Equal(PeerState.Closed, client.State);
        Assert.Empty(f.Admitted);
    }

    [Fact]
    public async Task A_Consumer_Failure_Reaches_A_Well_Behaved_Handler()
    {
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned("CN=game.test", TimeSpan.FromDays(1), true, "game.test");
        List<CertificateConsumerFailure> failures = [];
        await using ServerFixture f = new(o =>
        {
            o.Certificate = ServerCertificateOptions.Static(certificate);
            o.CertificateConsumers.Add(CertificateConsumers.FromDelegate(_ => throw new InvalidOperationException("consumer")));
        }, start: false);
        f.Server.CertificateConsumerFailed += failures.Add;
        await f.Server.StartAsync(TestContext.Current.CancellationToken);
        Assert.Empty(failures); // found on the thread that applied the certificate: raised from the next PollAll
        f.Server.PollAll();
        Assert.Single(failures);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(0, statistics.EventHandlerFaults);
    }

    [Fact]
    public async Task Work_Marked_For_A_Released_Slot_Is_Ignored()
    {
        await using ServerFixture f = new();
        QuiclyPeer first = f.ConnectAdmitted();
        QuiclyPeer second = f.ConnectAdmitted();
        int freed = f.ServerPeerOf(first).Index;
        first.Close();
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        Assert.Null(f.Server.GetPeer(freed));
        Assert.True(f.Server.Peers.Length > freed);
        f.Server.MarkWork(freed);
        Assert.Equal(0, f.Server.PollAll());
        Assert.Equal(PeerState.Connected, second.State);
    }

    /// <summary>
    /// Several payloads in flight at once: each one's peer references are tracked separately, and a payload is forgotten as
    /// soon as its last reference is gone (the peers release without telling the server, so the tracking is pruned from the
    /// leases' reference counts).
    /// </summary>
    [Fact]
    public async Task Shared_Payloads_Are_Tracked_Per_Payload_And_Pruned_When_Free()
    {
        await using ServerFixture f = new();
        QuiclyPeer peer = f.ServerPeerOf(f.ConnectAdmitted());
        f.Clients[0].RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        PeerSet set = f.Server.CreateSet();
        set.Add(peer);
        Assert.True(f.Server.Allocator.TryRent(64, out BufferLease firstBlock));
        Assert.True(f.Server.Allocator.TryRent(1536, out BufferLease secondBlock));
        SharedLease datagram = f.Server.SharedLeases.Share(in firstBlock, 1);
        SharedLease ordered = f.Server.SharedLeases.Share(in secondBlock, 1);

        Assert.Equal(1, f.Server.SendShared(set, new SendHeader(2), datagram, 64).AdmittedCount);
        Assert.Equal(1, f.Server.SendShared(set, new SendHeader(4), ordered, 1536).AdmittedCount);
        f.Server.GetStatistics(out ServerStatistics both);
        Assert.Equal(2, both.SharedSendsOutstanding);

        // The datagram's reference goes at its Sent notice, the ordered one's at the acknowledgement.
        Assert.True(f.RunUntil(() => f.Server.SharedLeases.GetReferenceCount(in datagram) == 1
            && f.Server.SharedLeases.GetReferenceCount(in ordered) == 1));
        f.Server.GetStatistics(out ServerStatistics released);
        Assert.Equal(0, released.SharedSendsOutstanding);
        Assert.Equal(2, released.SharedSendsAdmitted);
        Assert.True(f.Server.SharedLeases.Release(in datagram));
        Assert.True(f.Server.SharedLeases.Release(in ordered));
    }

    [Fact]
    public void Sweep_Keeps_Sessions_Still_Within_Their_Grace()
    {
        SessionRegistry registry = new(4);
        SessionRecord early = new() { SessionId = 1 };
        SessionRecord late = new() { SessionId = 2 };
        SessionRecord middle = new() { SessionId = 3 };
        registry.Disconnect(early, 100);
        registry.Disconnect(late, 300);
        registry.Disconnect(middle, 200);
        registry.Disconnect(middle, 250); // already listed: only its deadline moves
        Assert.Equal(3, registry.DisconnectedCount);
        Assert.Equal(100, registry.EarliestExpiry);
        registry.Remove(late); // detached from the middle of the list
        Assert.Equal(2, registry.DisconnectedCount);
        List<SessionEndInfo> ended = [];
        registry.Sweep(150, ended);
        Assert.Equal(1UL, Assert.Single(ended).SessionId);
        Assert.True(ended[0].Expired);
        Assert.Equal(250, registry.EarliestExpiry);
        Assert.Equal(1, registry.DisconnectedCount);
    }

    [Fact]
    public async Task Dispose_Reports_A_Listener_That_Fails_To_Dispose()
    {
        ThrowingListener listener = new();
        ServerFixture f = new(listener: listener, start: false);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await f.Server.DisposeAsync());
        Assert.Throws<ObjectDisposedException>(() => f.Server.PollAll());
        await f.DisposeAsync();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void CreateAndDrop(QuiclyServer server) => server.CreateSet().Add(0);

    private static void Collect()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    private sealed class ThrowingListener : ITransportListener
    {
        public IPEndPoint LocalEndPoint { get; } = new(IPAddress.Loopback, 1);

        public void Start(PreHandshakeCallback preHandshake, AcceptCallback accept)
        {
        }

        public void Stop()
        {
        }

        public void Dispose() => throw new InvalidOperationException("listener");
    }
}
