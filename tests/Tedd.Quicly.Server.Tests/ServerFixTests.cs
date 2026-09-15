using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

/// <summary>Fixes of the server review follow-up that the review tests do not pin down on their own.</summary>
public class ServerFixTests
{
    [Fact]
    public async Task A_Budget_Serves_The_Peers_With_Work_Round_Robin()
    {
        await using ServerFixture f = new();
        QuiclyPeer[] servers = [f.ServerPeerOf(f.ConnectAdmitted()), f.ServerPeerOf(f.ConnectAdmitted()), f.ServerPeerOf(f.ConnectAdmitted())];
        Assert.Equal([0, 1, 2], servers.Select(p => p.Index).ToArray());
        f.Run(50_000);
        List<int> dispatchedBy = [];
        f.Server.PollOverride = (peer, budget) =>
        {
            peer.Poll(0);
            if (budget == 0)
            {
                return 0;
            }

            dispatchedBy.Add(peer.Index); // every peer always holds one more message
            return 1;
        };

        for (int round = 0; round < 6; round++)
        {
            foreach (QuiclyPeer peer in servers)
            {
                f.Server.MarkWork(peer.Index);
            }

            Assert.Equal(1, f.Server.PollAll(maxItems: 1));
        }

        f.Server.PollOverride = null;
        Assert.Equal([0, 1, 2, 0, 1, 2], dispatchedBy);
    }

    [Fact]
    public async Task A_Connection_Activated_By_A_Handler_During_The_Deadline_Scan_Keeps_Its_Deadline()
    {
        await using ServerFixture f = new(o => o.PeerOptions.AdmissionTimeout = TimeSpan.FromMilliseconds(300));
        using ITransport first = f.Connector.Connect(f.SimListener.LocalEndPoint, "game.test", new RecordingSink()); // never says Hello
        f.Run(5_000);
        QuiclyPeer waiting = f.Server.Peers.ToArray().Single(s => s.IsOccupied).Peer!;
        ITransport? second = null;
        waiting.StateChanged += (_, _, to) =>
        {
            if (to == PeerState.Closing && second is null)
            {
                // Its admission timeout fires in the deadline scan (no transport callback marks it); a connection arrives now
                // and the handler activates it.
                second = f.Connector.Connect(f.SimListener.LocalEndPoint, "game.test", new RecordingSink());
                f.Network.Advance(1_000);
                f.Server.FlushAll();
            }
        };

        for (int i = 0; i < 1_000 && second is null; i++)
        {
            f.Network.Advance(1_000);
            f.Server.PollAll();
        }

        Assert.NotNull(second);
        long earliest = (long)typeof(QuiclyServer).GetField("_earliestDeadline", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(f.Server)!;
        long activated = f.Server.Peers.ToArray().Where(s => s.IsOccupied && s.Peer != waiting).Min(s => s.Peer!.NextDeadlineMicros);
        Assert.True(earliest <= activated, "The deadline scan overwrote the deadline of the connection activated during it.");
        second.Dispose();
    }

    [Fact]
    public async Task A_Resume_Of_A_Connection_Already_Closing_For_A_Resumable_Reason_Is_Not_Counted_As_Replaced()
    {
        await using ServerFixture f = new();
        SimulatedConnector slow = new(f.Network, new LinkOptions { DelayMicros = 100_000 });
        QuiclyPeer player = f.ConnectAdmitted(connector: slow);
        QuiclyPeer serverPeer = f.ServerPeerOf(player);
        serverPeer.Close(new CloseReason(QuiclyErrorCode.Timeout, "no heartbeat")); // a lost connection: the session survives it
        QuiclyPeer resumed = f.Resume(player);
        Assert.True(f.RunUntil(() => resumed.State == PeerState.Connected));
        Assert.Equal(player.SessionId, resumed.SessionId);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(1, statistics.SessionsResumed);
        Assert.Equal(0, statistics.SessionsReplaced);
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        Assert.Empty(f.Ended);
    }

    [Fact]
    public async Task A_Policy_That_Accepts_Everyone_Still_Admits_Nobody_During_A_Shutdown()
    {
        await using ServerFixture f = new(start: false);
        f.Server.AdmissionPolicy = new AcceptEveryone();
        await f.Server.StartAsync(TestContext.Current.CancellationToken);
        QuiclyPeer client = f.Connect();
        f.Network.Advance(1_000); // accepted by the listener's callback, activated only after the shutdown began
        f.Server.BeginShutdown();
        Assert.True(f.RunUntil(() => f.Server.IsShutdownComplete));
        Assert.Equal(PeerState.Closed, client.State);
        Assert.Empty(f.Admitted);
    }

    [Fact]
    public void The_Event_Queue_Is_Bounded_And_Not_Reentrant()
    {
        EventQueue<int> queue = new(2);
        Assert.False(queue.HasItems);
        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3); // over capacity: dropped
        Assert.True(queue.HasItems);
        Assert.Equal(1, queue.Dropped);
        List<int> raised = [];
        queue.Drain(item =>
        {
            raised.Add(item);
            queue.Drain(raised.Add); // from inside a handler: returns at once
            queue.Enqueue(10 + item);
        });
        Assert.Equal([1, 2], raised);
        Assert.True(queue.HasItems);
        queue.Drain(raised.Add);
        Assert.Equal([1, 2, 11, 12], raised);
        Assert.False(queue.HasItems);
    }

    [Fact]
    public async Task A_Held_Shared_Reference_Whose_Pool_Is_Gone_Is_Dropped_Quietly()
    {
        SlabAllocator pool = new(Pools.Small());
        await using ServerFixture f = new(o =>
        {
            o.PeerOptions.AllocatorOptions = null;
            o.PeerOptions.Allocator = pool;
        }, start: false);
        Assert.True(f.Server.Allocator.TryRent(64, out BufferLease block));
        SharedLease lease = f.Server.SharedLeases.Share(in block, 1);
        pool.Dispose(); // the application disposed the pool it supplied while a transport still held the payload
        f.Server.ReleaseHeldShared(in lease); // the transport's close drops the reference: nothing throws on its thread
    }

    [Fact]
    public async Task A_Listener_Whose_Stop_Throws_Still_Stops_The_Server_And_Its_Side_Services()
    {
        await using ServerFixture f = new(o => o.Http = new ServerHttpOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, 0) }, listener: new StopThrowingListener());
        IPEndPoint http = Assert.IsType<IPEndPoint>(f.Server.HttpEndPoint);
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Server.StopAsync(TestContext.Current.CancellationToken));
        Assert.Null(f.Server.HttpEndPoint);
        Assert.False(f.Server.IsRunning);
        await f.Server.StopAsync(TestContext.Current.CancellationToken); // stopped: a second call returns at once
        using TcpClient probe = new();
        await Assert.ThrowsAnyAsync<SocketException>(async () => await probe.ConnectAsync(http.Address, http.Port, TestContext.Current.CancellationToken));
    }

    private sealed class AcceptEveryone : IAdmissionPolicy
    {
        public PreHandshakeDecision PreHandshake(in NewConnectionInfo info) => PreHandshakeDecision.Accept;

        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }

    private sealed class StopThrowingListener : ITransportListener
    {
        public IPEndPoint LocalEndPoint { get; } = new(IPAddress.Loopback, 4433);

        public void Start(PreHandshakeCallback preHandshake, AcceptCallback accept)
        {
        }

        public void Stop() => throw new InvalidOperationException("listener stop");

        public void Dispose()
        {
        }
    }
}
