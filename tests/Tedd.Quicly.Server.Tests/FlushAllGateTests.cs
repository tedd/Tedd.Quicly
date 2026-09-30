using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

/// <summary>
/// FlushAll skips a peer only when a Flush would do nothing (QuiclyPeer.FlushGate.cs). Each test sets up a peer that looks
/// idle to a naive "nothing buffered and no deadline due" gate but still has something only a Flush does: a send queued by
/// another thread, a pass clock or a tick the next send depends on, an ack the transport thread left, a waiting SendAsync or
/// FlushAsync, the game thread's identity. All of them fail against the naive gate.
/// </summary>
public class FlushAllGateTests
{
    private const long Tick = 16_000;

    private static readonly ChannelTable LatestTable = ChannelTable.Create()
        .Add(2, "state", ChannelMode.UnreliableUnordered)
        .Add(4, "chat", ChannelMode.ReliableOrdered)
        .Add(6, "latest", ChannelMode.ReliableLatest)
        .Build();

    private static readonly ChannelTable LimitedTable = ChannelTable.Create()
        .Add(2, "state", ChannelMode.UnreliableUnordered)
        .Add(4, "chat", ChannelMode.ReliableOrdered, o => o.QueueLimitBytes = 100)
        .Build();

    private static readonly ChannelTable BulkTable = ChannelTable.Create()
        .Add(2, "state", ChannelMode.UnreliableUnordered)
        .Add(8, "assets", ChannelMode.Bulk)
        .Build();

    /// <summary>Connects a client with <paramref name="table"/> and runs until it is admitted.</summary>
    private static QuiclyPeer ConnectAdmitted(ServerFixture f, ChannelTable table)
    {
        QuiclyPeer client = f.Connect(table: table);
        Assert.True(f.RunUntil(() => client.State == PeerState.Connected), "not admitted: " + client.State);
        return client;
    }

    /// <summary>Game ticks: network, clients, then the server's PollAll and FlushAll(tick).</summary>
    private static void Ticks(ServerFixture f, int count, ref uint tick)
    {
        for (int i = 0; i < count; i++)
        {
            f.Network.Advance(Tick);
            f.PumpClients();
            f.Server.PollAll();
            f.Server.FlushAll(++tick);
        }
    }

    /// <summary>Moves the network and the clients only: nothing on the server runs.</summary>
    private static void Deliver(ServerFixture f, long micros = 60_000)
    {
        for (long t = 0; t < micros; t += 1_000)
        {
            f.Network.Advance(1_000);
            f.PumpClients();
        }
    }

    [Fact]
    public async Task Idle_Peers_Are_Not_Flushed_And_A_Peer_With_A_Send_Is()
    {
        await using ServerFixture f = new();
        QuiclyPeer a = f.ConnectAdmitted();
        f.ConnectAdmitted();
        uint tick = 0;
        Ticks(f, 300, ref tick);
        long before = f.Server.PeersFlushed;
        for (int i = 0; i < 100; i++)
        {
            f.Server.FlushAll(++tick);
        }

        Assert.Equal(before, f.Server.PeersFlushed);

        QuiclyPeer peer = f.ServerPeerOf(a);
        Assert.True(peer.SendCopy(new SendHeader(2), [1, 2, 3]).IsAdmitted);
        f.Server.FlushAll(++tick);
        Assert.Equal(before + 1, f.Server.PeersFlushed);
        f.Server.FlushAll(++tick);
        Assert.Equal(before + 1, f.Server.PeersFlushed);
    }

    [Fact]
    public async Task A_Send_Queued_By_Another_Thread_Goes_Out_With_The_Next_FlushAll()
    {
        await using ServerFixture f = new(o => o.PeerOptions.ThreadSafeSend = true);
        QuiclyPeer client = f.ConnectAdmitted();
        List<byte[]> received = [];
        client.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => received.Add(payload.ToArray()));
        QuiclyPeer peer = f.ServerPeerOf(client);
        uint tick = 0;
        Ticks(f, 300, ref tick);

        SendResult result = default;
        Thread sender = new(() => result = peer.SendCopy(new SendHeader(4), [7, 8, 9]));
        sender.Start();
        sender.Join();
        Assert.Equal(SendStatus.Admitted, result.Status);

        f.Server.FlushAll(++tick); // no PollAll: FlushAll alone admits the queued send and transmits it
        Deliver(f);
        Assert.Equal([7, 8, 9], Assert.Single(received));
    }

    [Fact]
    public async Task Concurrent_Sends_From_Another_Thread_Are_All_Transmitted_By_FlushAll()
    {
        await using ServerFixture f = new(o => o.PeerOptions.ThreadSafeSend = true);
        QuiclyPeer client = f.ConnectAdmitted();
        List<int> received = [];
        client.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => received.Add(payload[0] | (payload[1] << 8)));
        QuiclyPeer peer = f.ServerPeerOf(client);
        uint tick = 0;
        Ticks(f, 300, ref tick);

        const int Count = 400;
        int sent = 0;
        Thread sender = new(() =>
        {
            byte[] payload = new byte[2];
            for (int i = 0; i < Count; i++)
            {
                payload[0] = (byte)i;
                payload[1] = (byte)(i >> 8);
                while (peer.SendCopy(new SendHeader(4), payload).Status == SendStatus.QueueFull)
                {
                    Thread.Yield();
                }

                Volatile.Write(ref sent, i + 1);
                if ((i & 7) == 0)
                {
                    Thread.Sleep(0);
                }
            }
        });
        sender.Start();

        // FlushAll only (no PollAll): every send must be taken by the FlushAll that follows it, or by the next one when the
        // two race.
        while (Volatile.Read(ref sent) < Count)
        {
            f.Server.FlushAll(++tick);
            f.Network.Advance(1_000);
            f.PumpClients();
        }

        sender.Join();
        f.Server.FlushAll(++tick);
        Deliver(f, 200_000);
        f.Server.FlushAll(++tick);
        Deliver(f, 200_000);
        Assert.Equal(Enumerable.Range(0, Count), received);
    }

    [Fact]
    public async Task A_Send_After_A_Quiet_Stretch_Keeps_Its_Whole_Expiry()
    {
        // A send after many skipped flushes is served by the next FlushAll with its whole expiry ahead of it. (The expiry
        // starts at that pass, PROTOCOL.md §4.5, so this no longer depends on SkipFlush moving the pass clock on; that
        // clock still serves ReliableLatest's budgets and retry timers.)
        await using ServerFixture f = new(o =>
        {
            o.PeerOptions.PingInterval = TimeSpan.FromSeconds(10);
            o.PeerOptions.FastLockDuration = TimeSpan.Zero;
        });
        QuiclyPeer client = f.ConnectAdmitted();
        List<byte[]> received = [];
        client.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => received.Add(payload.ToArray()));
        QuiclyPeer peer = f.ServerPeerOf(client);
        uint tick = 0;
        Ticks(f, 40, ref tick); // 640 ms without a ping or anything else for the server peer

        Assert.True(peer.SendCopy(new SendHeader(4), [5, 5], new SendOptions { ExpiryMicros = 50_000 }).IsAdmitted);
        Ticks(f, 5, ref tick);
        Assert.Equal([5, 5], Assert.Single(received));
        Assert.True(peer.GetChannelStatistics(4, out ChannelStatistics statistics));
        Assert.Equal(0, statistics.Expired);
    }

    [Fact]
    public async Task A_Long_Server_Tick_Does_Not_Expire_Sends_To_A_Quiet_Peer()
    {
        // PollAll does not poll a peer without pending work, so a quiet peer's last pass is the previous FlushAll. A server
        // tick longer than a message's expiry must not expire what that tick sends: the message was never held back.
        await using ServerFixture f = new(o =>
        {
            o.PeerOptions.PingInterval = TimeSpan.FromSeconds(10);
            o.PeerOptions.FastLockDuration = TimeSpan.Zero;
        });
        QuiclyPeer client = f.ConnectAdmitted();
        List<byte[]> received = [];
        client.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => received.Add(payload.ToArray()));
        QuiclyPeer peer = f.ServerPeerOf(client);
        uint tick = 0;
        Ticks(f, 40, ref tick);

        // One 100 ms server tick: the clock moves and nothing on the server runs.
        f.Network.Advance(100_000);
        f.PumpClients();
        Assert.True(peer.SendCopy(new SendHeader(4), [6, 6], new SendOptions { ExpiryMicros = 50_000 }).IsAdmitted);
        f.Server.PollAll();
        f.Server.FlushAll(++tick);
        Deliver(f);
        Assert.Equal([6, 6], Assert.Single(received));
        Assert.True(peer.GetChannelStatistics(4, out ChannelStatistics statistics));
        Assert.Equal(0, statistics.Expired);
    }

    [Fact]
    public async Task An_Immediate_Send_After_Skipped_Flushes_Carries_The_Last_Tick()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.ConnectAdmitted();
        List<uint> ticks = [];
        client.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> _) => ticks.Add(header.SenderTick));
        QuiclyPeer peer = f.ServerPeerOf(client);
        uint tick = 0;
        Ticks(f, 290, ref tick);
        for (int i = 0; i < 10; i++)
        {
            f.Server.FlushAll(++tick);
        }

        // Two messages in one pass share a container, which carries the tick of the last flush (PROTOCOL.md §2.2).
        Assert.True(peer.SendCopy(new SendHeader(2), [1]).IsAdmitted);
        Assert.True(peer.SendCopy(new SendHeader(2), [2], SendOptions.Immediate).IsAdmitted);
        Deliver(f);
        Assert.Equal([tick, tick], ticks);
    }

    [Fact]
    public async Task The_Ack_Of_A_ReliableLatest_Value_Goes_Out_In_A_FlushAll_Although_Only_A_Pass_Consumes_It()
    {
        // The transport thread queues the ack a ReliableLatest value is owed; the work signal it raises gets the value polled, but
        // only a Flush sends the ack, so the gate must see the engine's level (HasUnsentControl) and not skip the peer.
        await using ServerFixture f = new(o => o.Channels = LatestTable);
        QuiclyPeer client = ConnectAdmitted(f, LatestTable);
        uint tick = 0;
        Ticks(f, 300, ref tick);

        SendResult result = client.SendCopy(new SendHeader(6, 42), [1, 2, 3, 4], SendOptions.Tracked);
        Assert.True(result.IsAdmitted);
        Ticks(f, 10, ref tick);
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(result.Token));
    }

    [Fact]
    public async Task A_Waiting_SendAsync_Is_Retried_By_The_FlushAll_That_Drains_The_Completion_That_Made_Room()
    {
        await using ServerFixture f = new(o => o.Channels = LimitedTable);
        QuiclyPeer client = ConnectAdmitted(f, LimitedTable);
        List<int> received = [];
        client.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => received.Add(payload.Length));
        QuiclyPeer peer = f.ServerPeerOf(client);
        uint tick = 0;
        Assert.True(peer.SendCopy(new SendHeader(4), [1]).IsAdmitted); // opens the channel's stream (a start counts as queued)
        Ticks(f, 300, ref tick);
        received.Clear();

        Assert.True(peer.SendCopy(new SendHeader(4), new byte[100]).IsAdmitted);
        ValueTask<SendResult> waiting = peer.SendAsync(new SendHeader(4), new byte[50]);
        Assert.False(waiting.IsCompleted); // 100 B queued or in flight: the channel's 100 B limit is reached

        f.Server.FlushAll(++tick); // hands the 100 B message to the transport (still counted until it completes)
        Deliver(f);                 // the client receives it; its completion waits in the server peer's ring
        Assert.Equal([100], received);
        Assert.False(waiting.IsCompleted);

        f.Server.FlushAll(++tick); // no PollAll: this Flush drains the completion, retries the waiter and sends it
        Assert.True(waiting.IsCompletedSuccessfully);
        Assert.Equal(SendStatus.Admitted, (await waiting).Status);
        Deliver(f);
        Assert.Equal([100, 50], received);
    }

    [Fact]
    public async Task A_FlushAsync_Whose_Messages_Were_Canceled_Completes_In_The_Next_FlushAll()
    {
        await using ServerFixture f = new(o => o.PeerOptions.MaxSendBytesPerSecond = 1_000);
        QuiclyPeer client = f.ConnectAdmitted();
        QuiclyPeer peer = f.ServerPeerOf(client);
        uint tick = 0;
        Assert.True(peer.SendCopy(new SendHeader(4), [1]).IsAdmitted); // opens the channel's stream (a start holds FlushAsync)
        Ticks(f, 300, ref tick);

        // The first message overdraws the send cap by about two seconds; the second is held back behind it.
        Assert.True(peer.SendCopy(new SendHeader(4), new byte[2_000]).IsAdmitted);
        f.Server.FlushAll(++tick);
        SendResult held = peer.SendCopy(new SendHeader(4), new byte[10], SendOptions.Tracked);
        Assert.True(held.IsAdmitted);
        ValueTask flushed = peer.FlushAsync();
        Assert.False(flushed.IsCompleted);

        Assert.True(peer.TryCancel(held.Token));
        f.Server.FlushAll(++tick); // no PollAll: the cancel completes here, and with it the FlushAsync
        Assert.True(flushed.IsCompletedSuccessfully);
        Assert.Equal(DeliveryStatus.Canceled, peer.GetDeliveryStatus(held.Token));
    }

    [Fact]
    public async Task A_Message_Held_By_The_Send_Cap_Goes_Out_When_The_Cap_Refills()
    {
        await using ServerFixture f = new(o => o.PeerOptions.MaxSendBytesPerSecond = 10_000);
        QuiclyPeer client = f.ConnectAdmitted();
        List<int> received = [];
        client.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => received.Add(payload.Length));
        QuiclyPeer peer = f.ServerPeerOf(client);
        uint tick = 0;
        Ticks(f, 300, ref tick);

        Assert.True(peer.SendCopy(new SendHeader(4), new byte[2_000]).IsAdmitted); // about 200 ms of cap
        Ticks(f, 1, ref tick);
        Assert.True(peer.SendCopy(new SendHeader(4), new byte[10]).IsAdmitted);
        long due = f.Network.NowMicros + 200_000;
        while (received.Count < 2 && f.Network.NowMicros < due + 100_000)
        {
            Ticks(f, 1, ref tick);
        }

        Assert.Equal([2_000, 10], received);
        Assert.InRange(f.Network.NowMicros, due - 40_000, due + 2 * Tick);
    }

    [Fact]
    public async Task FlushAll_On_Another_Thread_Makes_It_The_Game_Thread()
    {
        await using ServerFixture f = new(o => o.PeerOptions.ThreadSafeSend = true);
        QuiclyPeer client = f.ConnectAdmitted();
        QuiclyPeer peer = f.ServerPeerOf(client);
        uint tick = 0;
        Ticks(f, 300, ref tick);

        SendResult result = default;
        Thread game = new(() =>
        {
            f.Server.FlushAll(tick + 1);
            result = peer.SendCopy(new SendHeader(4), [1], SendOptions.Tracked); // a game-thread send: tracking is allowed
        });
        game.Start();
        game.Join();
        Assert.Equal(SendStatus.Admitted, result.Status);
        Assert.NotEqual(default, result.Token);
    }

    [Fact]
    public async Task Bulk_Tables_Keep_Flushing_Every_Peer()
    {
        await using ServerFixture f = new(o => o.Channels = BulkTable);
        ConnectAdmitted(f, BulkTable);
        uint tick = 0;
        Ticks(f, 50, ref tick);
        long before = f.Server.PeersFlushed;
        f.Server.FlushAll(++tick);
        Assert.Equal(before + 1, f.Server.PeersFlushed);
    }

    [Fact]
    public async Task Pings_Pongs_And_Their_Completions_Do_Not_Make_FlushAll_Flush_An_Idle_Peer()
    {
        await using ServerFixture f = new();
        f.ConnectAdmitted();
        f.ConnectAdmitted();
        uint tick = 0;
        Ticks(f, 300, ref tick); // past the fast lock: one ping a second per peer from here on
        long before = f.Server.PeersFlushed;
        f.Server.GetStatistics(out ServerStatistics polledBefore);
        Ticks(f, 250, ref tick); // four seconds: pings sent, completed and answered
        f.Server.GetStatistics(out ServerStatistics polledAfter);
        Assert.True(polledAfter.PeersPolled - polledBefore.PeersPolled >= 8, "the peers were polled for their pings");
        Assert.Equal(before, f.Server.PeersFlushed);
    }

    [Fact]
    public async Task A_Loss_Notice_Drained_By_PollAll_Is_Retried_By_The_Next_FlushAll()
    {
        // The transport reports the lost ReliableLatest datagram one round trip later; the engine queues an immediate retry
        // when the completion is routed, which PollAll does. The retry must go out with the next FlushAll, not with the
        // timer backstop (clamp(1.5 x RTT, 20 ms, 1 s)).
        await using ServerFixture f = new(o => o.Channels = LatestTable);
        SimulatedConnector lossy = new(f.Network, new LinkOptions { DelayMicros = 1_000, LossPercent = 100 });
        QuiclyPeer client = f.Connect(connector: lossy, table: LatestTable);
        Assert.True(f.RunUntil(() => client.State == PeerState.Connected), "not admitted: " + client.State);
        QuiclyPeer peer = f.ServerPeerOf(client);
        uint tick = 0;
        Ticks(f, 30, ref tick);

        Assert.True(peer.SendCopy(new SendHeader(6, 7), [1, 2, 3]).IsAdmitted);
        long sent = f.Network.NowMicros;
        long retried = -1;
        while (f.Network.NowMicros < sent + 30_000)
        {
            f.Network.Advance(500);
            f.PumpClients();
            f.Server.PollAll();
            f.Server.FlushAll(++tick);
            Assert.True(peer.GetChannelStatistics(6, out ChannelStatistics statistics));
            if (statistics.Retries > 0)
            {
                retried = f.Network.NowMicros;
                break;
            }
        }

        Assert.InRange(retried - sent, 1_000, 10_000);
    }
}
