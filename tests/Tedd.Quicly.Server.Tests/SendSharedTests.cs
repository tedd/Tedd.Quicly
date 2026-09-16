using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server.Tests;

/// <summary>
/// <see cref="QuiclyServer.SendShared"/> over the peers' own <see cref="QuiclyPeer.SendShared"/>: one serialisation reaches
/// N peers with no copy, each peer that admits it takes exactly one reference and releases it exactly once (transport done,
/// send discarded, session closed), and a peer that refuses takes none.
/// </summary>
public class SendSharedTests
{
    private static SharedLease Share(QuiclyServer server, int size, out BufferLease block)
    {
        Assert.True(server.Allocator.TryRent(size, out block));
        server.Allocator.GetSpan(in block).Slice(0, size).Fill(0x5A);
        return server.SharedLeases.Share(in block, 1);
    }

    private static QuiclyPeer[] Connect(ServerFixture f, int count)
    {
        QuiclyPeer[] peers = new QuiclyPeer[count];
        for (int i = 0; i < count; i++)
        {
            peers[i] = f.ServerPeerOf(f.ConnectAdmitted());
        }

        return peers;
    }

    [Fact]
    public async Task Sends_To_A_Subset_And_Every_Peer_Releases_Its_Own_Reference()
    {
        await using ServerFixture f = new();
        QuiclyPeer[] peers = Connect(f, 3);
        List<byte[]> received = [];
        f.Clients[0].RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => received.Add(payload.ToArray()));
        PeerSet set = f.Server.CreateSet();
        set.Add(peers[0]);
        set.Add(peers[2]);
        set.Add(40); // a free slot
        SharedLease lease = Share(f.Server, 100, out _);

        SharedSendResult result = f.Server.SendShared(set, new SendHeader(2), lease, 100);

        Assert.Equal(2, result.AdmittedCount);
        Assert.Equal(1, result.RejectedCount);
        Assert.Equal(SendStatus.NotConnected, result.GetStatus(40));
        Assert.Equal(SendStatus.Admitted, result.GetStatus(peers[0].Index));
        Assert.Equal(SendStatus.Admitted, result.GetStatus(peers[2].Index));
        Assert.Throws<ArgumentOutOfRangeException>(() => result.GetStatus(peers[1].Index));
        Assert.True(result.Admitted!.IsReadOnly);
        Assert.True(result.Rejected!.IsReadOnly);
        Assert.False(result.Admitted.Contains(peers[1]) || result.Rejected.Contains(peers[1]));
        Assert.False(result.AllAdmitted);

        // The caller's reference plus one per peer that admitted it.
        Assert.Equal(3, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.GetStatistics(out ServerStatistics sent);
        Assert.Equal(2, sent.SharedSendsOutstanding);
        Assert.Equal(2, sent.SharedSendsAdmitted);

        Assert.True(f.RunUntil(() => f.Server.SharedLeases.GetReferenceCount(in lease) == 1));
        f.Server.GetStatistics(out ServerStatistics done);
        Assert.Equal(0, done.SharedSendsOutstanding);
        Assert.Equal(100, Assert.Single(received).Length);
        Assert.Equal(0x5A, received[0][99]);
        Assert.True(f.Server.SharedLeases.Release(in lease));
    }

    [Fact]
    public async Task A_Peer_That_Refuses_Takes_No_Reference()
    {
        await using ServerFixture f = new();
        QuiclyPeer peer = f.ServerPeerOf(f.ConnectAdmitted());
        PeerSet set = f.Server.CreateSet();
        set.Add(peer);
        SharedLease lease = Share(f.Server, 4096, out _);

        // 4 096 bytes never fit an unreliable datagram channel, and the payload of a shared lease is never compressed.
        SharedSendResult result = f.Server.SendShared(set, new SendHeader(2), lease, 4096);

        Assert.Equal(0, result.AdmittedCount);
        Assert.Equal(1, result.RejectedCount);
        Assert.Equal(SendStatus.TooLarge, result.GetStatus(peer.Index));
        Assert.Equal(1, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(0, statistics.SharedSendsOutstanding);
        Assert.Equal(0, statistics.SharedSendsAdmitted);
        Assert.True(f.Server.SharedLeases.Release(in lease));
    }

    [Fact]
    public async Task Many_Sends_Of_One_Payload_Are_Tracked_Together_And_Forgotten_When_It_Is_Free()
    {
        await using ServerFixture f = new();
        QuiclyPeer peer = f.ServerPeerOf(f.ConnectAdmitted());
        f.Clients[0].RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        PeerSet set = f.Server.CreateSet();
        set.Add(peer);
        SharedLease lease = Share(f.Server, 64, out _);
        for (int i = 0; i < 100; i++)
        {
            Assert.Equal(1, f.Server.SendShared(set, new SendHeader(2), lease, 64).AdmittedCount);
        }

        Assert.Equal(101, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.GetStatistics(out ServerStatistics sent);
        Assert.Equal(100, sent.SharedSendsOutstanding);
        Assert.Equal(100, sent.SharedSendsAdmitted);

        Assert.True(f.RunUntil(() => f.Server.SharedLeases.GetReferenceCount(in lease) == 1));
        f.Server.GetStatistics(out ServerStatistics done);
        Assert.Equal(0, done.SharedSendsOutstanding);
        Assert.True(f.Server.SharedLeases.Release(in lease));

        // The tracking is gone with the payload: the next send of a new payload starts from zero.
        SharedLease next = Share(f.Server, 64, out _);
        Assert.Equal(1, f.Server.SendShared(set, new SendHeader(2), next, 64).AdmittedCount);
        f.Server.GetStatistics(out ServerStatistics again);
        Assert.Equal(1, again.SharedSendsOutstanding);
        Assert.True(f.RunUntil(() => f.Server.SharedLeases.GetReferenceCount(in next) == 1));
        Assert.True(f.Server.SharedLeases.Release(in next));
    }

    [Fact]
    public async Task A_Closing_Peer_Drops_The_References_Of_Its_Queued_Sends()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.ConnectAdmitted();
        QuiclyPeer serverPeer = f.ServerPeerOf(client);
        PeerSet set = f.Server.CreateSet();
        set.Add(serverPeer);
        SharedLease lease = Share(f.Server, 1536, out _);

        // An ordered channel releases at the acknowledgement; this one is never flushed, so the close completes it Disconnected.
        Assert.Equal(1, f.Server.SendShared(set, new SendHeader(4), lease, 1536, SendOptions.Tracked).AdmittedCount);
        Assert.Equal(2, f.Server.SharedLeases.GetReferenceCount(in lease));

        serverPeer.Close();
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        Assert.Equal(1, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(0, statistics.SharedSendsOutstanding);
        Assert.True(f.Server.SharedLeases.Release(in lease));
    }

    [Fact]
    public async Task Shutdown_Keeps_The_References_Until_The_Transports_Closed()
    {
        await using ServerFixture f = new(o => o.ShutdownTimeout = TimeSpan.Zero);
        QuiclyPeer[] peers = Connect(f, 2);
        PeerSet set = f.Server.CreateSet();
        set.Add(peers[0]);
        set.Add(peers[1]);
        SharedLease lease = Share(f.Server, 1536, out _);
        Assert.Equal(2, f.Server.SendShared(set, new SendHeader(4), lease, 1536).AdmittedCount);
        Assert.Equal(3, f.Server.SharedLeases.GetReferenceCount(in lease));

        await f.Server.StopAsync(TestContext.Current.CancellationToken);
        // Forced close: the peers were disposed, their transports have not reported their close (ADR 0008 invariant 1).
        Assert.Equal(3, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.GetStatistics(out ServerStatistics forced);
        Assert.Equal(2, forced.SharedSendsOutstanding);

        f.Network.RunUntilIdle(1_000_000);
        Assert.Equal(1, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.GetStatistics(out ServerStatistics released);
        Assert.Equal(0, released.SharedSendsOutstanding);
        Assert.True(f.Server.SharedLeases.Release(in lease));
    }

    [Fact]
    public async Task A_Peer_The_Application_Disposed_Is_Reported_NotConnected()
    {
        await using ServerFixture f = new();
        QuiclyPeer peer = f.ServerPeerOf(f.ConnectAdmitted());
        PeerSet set = f.Server.CreateSet();
        set.Add(peer);
        peer.Dispose();
        SharedLease lease = Share(f.Server, 64, out _);

        SharedSendResult result = f.Server.SendShared(set, new SendHeader(2), lease, 64);

        Assert.Equal(0, result.AdmittedCount);
        Assert.Equal(SendStatus.NotConnected, result.GetStatus(peer.Index));
        Assert.Equal(1, f.Server.SharedLeases.GetReferenceCount(in lease));
        Assert.True(f.Server.SharedLeases.Release(in lease));
        Assert.True(f.RunUntil(() => f.Server.PeerCount == 0)); // the slot goes back on the next poll
    }

    [Fact]
    public async Task Arguments_Are_Validated()
    {
        await using ServerFixture f = new();
        PeerSet set = f.Server.CreateSet();
        SharedLease lease = Share(f.Server, 64, out BufferLease block);
        Assert.Throws<ArgumentNullException>(() => f.Server.SendShared(null!, new SendHeader(2), lease, 1));
        Assert.Throws<ArgumentException>(() => f.Server.SendShared(new PeerSet(f.Server.Capacity + 1), new SendHeader(2), lease, 1));
        Assert.Throws<ArgumentException>(() => f.Server.SendShared(set, new SendHeader(2), default, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => f.Server.SendShared(set, new SendHeader(2), lease, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => f.Server.SendShared(set, new SendHeader(2), lease, block.Length + 1));
        SharedSendResult empty = f.Server.SendShared(set, new SendHeader(2), lease, 0);
        Assert.True(empty.AllAdmitted);
        Assert.Equal(0, empty.AdmittedCount);
        Assert.True(f.Server.SharedLeases.Release(in lease));
        Assert.Throws<ArgumentException>(() => f.Server.SendShared(set, new SendHeader(2), lease, 1)); // no reference left
        SharedSendResult none = default;
        Assert.Throws<ArgumentOutOfRangeException>(() => none.GetStatus(0));
    }

    [Fact]
    public async Task An_Empty_Payload_Is_Admitted_And_Gives_Its_Reference_Back()
    {
        await using ServerFixture f = new();
        QuiclyPeer peer = f.ServerPeerOf(f.ConnectAdmitted());
        PeerSet set = f.Server.CreateSet();
        set.Add(peer);
        SharedLease lease = Share(f.Server, 64, out _);

        Assert.Equal(1, f.Server.SendShared(set, new SendHeader(2), lease, 0).AdmittedCount);

        Assert.Equal(2, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.GetStatistics(out ServerStatistics sent);
        Assert.Equal(1, sent.SharedSendsOutstanding);
        Assert.Equal(1, sent.SharedSendsAdmitted);
        Assert.True(f.RunUntil(() => f.Server.SharedLeases.GetReferenceCount(in lease) == 1));
        Assert.True(f.Server.SharedLeases.Release(in lease));
    }

    [Fact]
    public async Task Steady_State_Does_Not_Allocate()
    {
        await using ServerFixture f = new();
        QuiclyPeer[] peers = Connect(f, 8);
        foreach (QuiclyPeer client in f.Clients)
        {
            client.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        }

        PeerSet set = f.Server.CreateSet();
        foreach (QuiclyPeer peer in peers)
        {
            set.Add(peer);
        }

        SharedLease lease = Share(f.Server, 512, out _);
        QuiclyServer server = f.Server;
        SendHeader header = new(2);
        AllocationAssert.NoAllocations(() =>
        {
            if (server.SendShared(set, header, lease, 512).AdmittedCount != 8)
            {
                throw new InvalidOperationException("not admitted");
            }

            server.FlushAll();
            f.Network.Advance(1_000);
            server.PollAll();
            f.PumpClients();
        }, iterations: 200);

        Assert.True(f.RunUntil(() => f.Server.SharedLeases.GetReferenceCount(in lease) == 1));
        Assert.True(f.Server.SharedLeases.Release(in lease));
    }
}
