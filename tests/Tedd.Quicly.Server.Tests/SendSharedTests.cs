using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Server.Tests;

public class SendSharedTests
{
    private static SharedLease Share(QuiclyServer server, int size, out BufferLease block)
    {
        Assert.True(server.Allocator.TryRent(size, out block));
        server.Allocator.GetSpan(in block).Slice(0, size).Fill(0x5A);
        return server.SharedLeases.Share(in block, 1);
    }

    private static async Task<(ServerFixture Fixture, QuiclyPeer[] ServerPeers)> ConnectAsync(int count, Action<ServerOptions>? configure = null)
    {
        ServerFixture f = new(configure);
        QuiclyPeer[] peers = new QuiclyPeer[count];
        for (int i = 0; i < count; i++)
        {
            peers[i] = f.ServerPeerOf(f.ConnectAdmitted());
        }

        await Task.CompletedTask;
        return (f, peers);
    }

    [Fact]
    public async Task Sends_To_A_Subset_Through_The_Peers_Public_Api()
    {
        (ServerFixture f, QuiclyPeer[] peers) = await ConnectAsync(3);
        await using ServerFixture scope = f;
        PeerSet set = f.Server.CreateSet();
        set.Add(peers[0]);
        set.Add(peers[2]);
        set.Add(40); // a free slot
        SharedLease lease = Share(f.Server, 100, out _);
        SharedSendResult result = f.Server.SendShared(set, new SendHeader(2), lease, 100);

        Assert.Equal(3, result.AdmittedCount + result.RejectedCount);
        Assert.Equal(SendStatus.NotConnected, result.GetStatus(40));
        Assert.Throws<ArgumentOutOfRangeException>(() => result.GetStatus(peers[1].Index));
        Assert.True(result.Admitted!.IsReadOnly);
        Assert.True(result.Rejected!.IsReadOnly);
        Assert.False(result.Admitted.Contains(peers[1]) || result.Rejected.Contains(peers[1]));

        // Wave C1 step 1 ships placeholder engines, which refuse every send (NotSupported); with the datagram engines the
        // two peers admit it and hold a reference each until their transport released the payload.
        // TODO(C1 merge): once the session layer's datagram engines replace the placeholders, assert SendStatus.Admitted for
        // both peers (and AdmittedCount == 2) instead of accepting NotSupported.
        foreach (QuiclyPeer peer in new[] { peers[0], peers[2] })
        {
            Assert.True(result.GetStatus(peer.Index) is SendStatus.NotSupported or SendStatus.Admitted);
        }

        Assert.Equal(1 + result.AdmittedCount, f.Server.SharedLeases.GetReferenceCount(in lease));
        Assert.True(f.RunUntil(() => f.Server.SharedLeases.GetReferenceCount(in lease) == 1));
        Assert.True(f.Server.SharedLeases.Release(in lease));
    }

    [Fact]
    public async Task Admitted_Sends_Hold_A_Reference_Until_The_Payload_Is_Released()
    {
        (ServerFixture f, QuiclyPeer[] peers) = await ConnectAsync(4);
        await using ServerFixture scope = f;
        FakePort port = new();
        f.Server.SharedPort = port;
        PeerSet set = f.Server.CreateSet();
        foreach (QuiclyPeer peer in peers)
        {
            set.Add(peer);
        }

        port.Decide = peer => peer == peers[1] ? SendStatus.QueueFull : SendStatus.Admitted;
        SharedLease lease = Share(f.Server, 300, out BufferLease block);
        SharedSendResult result = f.Server.SendShared(set, new SendHeader(4, 9), lease, 300, SendOptions.Immediate);
        Assert.Equal(3, result.AdmittedCount);
        Assert.Equal(1, result.RejectedCount);
        Assert.Equal(SendStatus.QueueFull, result.GetStatus(peers[1].Index));
        Assert.Equal(SendStatus.Admitted, result.GetStatus(peers[0].Index));
        Assert.True(result.Admitted!.Contains(peers[3]));
        Assert.Equal(4, port.Sends);
        Assert.Equal(300, port.LastLength);
        Assert.True(port.LastOptions.Track);
        Assert.Equal(SendMode.Immediate, port.LastOptions.Mode);
        unsafe
        {
            Assert.True(port.LastPayload == f.Server.Allocator.GetPointer(in block));
        }

        Assert.Equal(4, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(3, statistics.SharedSendsOutstanding);

        // Not released yet: polling keeps the references.
        foreach (QuiclyPeer peer in peers)
        {
            f.Server.MarkWork(peer.Index);
        }

        f.Server.PollAll();
        Assert.Equal(4, f.Server.SharedLeases.GetReferenceCount(in lease));

        port.Released = true;
        foreach (QuiclyPeer peer in peers)
        {
            f.Server.MarkWork(peer.Index);
        }

        f.Server.PollAll();
        Assert.Equal(1, f.Server.SharedLeases.GetReferenceCount(in lease));
        Assert.Equal(3, port.Releases);
        f.Server.GetStatistics(out statistics);
        Assert.Equal(0, statistics.SharedSendsOutstanding);
        Assert.True(f.Server.SharedLeases.Release(in lease));
    }

    [Fact]
    public async Task A_Closing_Peer_Drops_Its_References()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.ConnectAdmitted();
        QuiclyPeer serverPeer = f.ServerPeerOf(client);
        FakePort port = new();
        f.Server.SharedPort = port;
        PeerSet set = f.Server.CreateSet();
        set.Add(serverPeer);
        SharedLease lease = Share(f.Server, 64, out _);
        for (int i = 0; i < 100; i++)
        {
            f.Server.SendShared(set, new SendHeader(2), lease, 64); // more than the tracker's initial 64 entries
        }

        Assert.Equal(101, f.Server.SharedLeases.GetReferenceCount(in lease));
        client.Close();
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        Assert.Equal(100, port.Abandons);
        Assert.Equal(1, f.Server.SharedLeases.GetReferenceCount(in lease));

        // Tracker entries are reused after being freed.
        QuiclyPeer other = f.ServerPeerOf(f.ConnectAdmitted());
        set.Clear();
        set.Add(other);
        f.Server.SendShared(set, new SendHeader(2), lease, 64);
        Assert.Equal(2, f.Server.SharedLeases.GetReferenceCount(in lease));
        port.Released = true;
        f.Server.MarkWork(other.Index);
        f.Server.PollAll();
        Assert.Equal(1, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.SharedLeases.Release(in lease);
    }

    [Fact]
    public async Task Shutdown_Drops_Outstanding_References()
    {
        (ServerFixture f, QuiclyPeer[] peers) = await ConnectAsync(2, o => o.ShutdownTimeout = TimeSpan.Zero);
        await using ServerFixture scope = f;
        f.Server.SharedPort = new FakePort();
        PeerSet set = f.Server.CreateSet();
        set.Add(peers[0]);
        set.Add(peers[1]);
        SharedLease lease = Share(f.Server, 64, out _);
        f.Server.SendShared(set, new SendHeader(2), lease, 64);
        Assert.Equal(3, f.Server.SharedLeases.GetReferenceCount(in lease));
        await f.Server.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, f.Server.SharedLeases.GetReferenceCount(in lease)); // forced close: kept until the transports closed
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(2, statistics.SharedSendsOutstanding);
        f.Network.RunUntilIdle(1_000_000);
        Assert.Equal(1, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.GetStatistics(out statistics);
        Assert.Equal(0, statistics.SharedSendsOutstanding);
        f.Server.SharedLeases.Release(in lease);
    }

    [Fact]
    public async Task A_Throwing_Send_Returns_Its_Reference()
    {
        (ServerFixture f, QuiclyPeer[] peers) = await ConnectAsync(1);
        await using ServerFixture scope = f;
        f.Server.SharedPort = new FakePort { ThrowOnSend = true };
        PeerSet set = f.Server.CreateSet();
        set.Add(peers[0]);
        SharedLease lease = Share(f.Server, 64, out _);
        Assert.Throws<InvalidOperationException>(() => f.Server.SendShared(set, new SendHeader(2), lease, 64));
        Assert.Equal(1, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.SharedLeases.Release(in lease);
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
    public async Task Default_Port_Handles_Released_And_Abandoned_Waits()
    {
        await using ServerFixture f = new();
        QuiclyPeer peer = f.ServerPeerOf(f.ConnectAdmitted());
        SharedEntry entry = default; // token (0, 0) is never live: the wait completes at once
        Assert.True(PeerSharedSendPort.Instance.TryRelease(peer, ref entry));
        Assert.True(entry.Armed);

        SharedEntry notArmed = default;
        PeerSharedSendPort.Instance.Abandon(peer, ref notArmed);
        SharedEntry completed = new() { Armed = true, Wait = new ValueTask<DeliveryStatus>(DeliveryStatus.Delivered) };
        PeerSharedSendPort.Instance.Abandon(peer, ref completed);
        Assert.Equal(default, completed.Wait);
    }

    [Fact]
    public async Task Steady_State_Does_Not_Allocate()
    {
        (ServerFixture f, QuiclyPeer[] peers) = await ConnectAsync(8);
        await using ServerFixture scope = f;
        FakePort port = new() { Released = true };
        f.Server.SharedPort = port;
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
            SharedSendResult result = server.SendShared(set, header, lease, 512);
            if (result.AdmittedCount != 8)
            {
                throw new InvalidOperationException("not admitted");
            }

            foreach (int index in set)
            {
                server.MarkWork(index);
            }

            server.PollAll();
        });
        Assert.Equal(1, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.SharedLeases.Release(in lease);
    }

    /// <summary>Stands in for peers whose engines admit sends (the step-1 placeholders refuse them).</summary>
    private sealed unsafe class FakePort : ISharedSendPort
    {
        public Func<QuiclyPeer, SendStatus> Decide { get; set; } = static _ => SendStatus.Admitted;

        public bool Released { get; set; }

        public bool ThrowOnSend { get; set; }

        public int Sends { get; private set; }

        public int Releases { get; private set; }

        public int Abandons { get; private set; }

        public byte* LastPayload { get; private set; }

        public int LastLength { get; private set; }

        public SendOptions LastOptions { get; private set; }

        public SendResult Send(QuiclyPeer peer, in SendHeader header, byte* payload, int length, SendOptions options)
        {
            if (ThrowOnSend)
            {
                throw new InvalidOperationException("send");
            }

            Sends++;
            LastPayload = payload;
            LastLength = length;
            LastOptions = options;
            SendStatus status = Decide(peer);
            return new SendResult(status, status == SendStatus.Admitted ? new SendToken(Sends & 0xFFFF, 1) : default);
        }

        public bool TryRelease(QuiclyPeer peer, ref SharedEntry entry)
        {
            if (!Released)
            {
                return false;
            }

            Releases++;
            return true;
        }

        public void Abandon(QuiclyPeer peer, ref SharedEntry entry) => Abandons++;
    }
}
