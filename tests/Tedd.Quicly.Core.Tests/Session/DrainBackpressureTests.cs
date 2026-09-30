using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// What a channel without a handler costs the rest of the peer when nobody drains it (docs/design/session-layer.md §4.4
/// "Channels without a handler", PROTOCOL.md §7). An unreliable channel that is left undrained across a Poll keeps a
/// bounded backlog and loses its own oldest messages, counted in <c>DrainQueueDrops</c>; it closes the receive ring for
/// one Poll interval at most, never stops mailbox dispatch, never pins the receive budget and never keeps the host busy.
/// A channel that is drained every frame loses nothing the ring and the budget took, and neither does a channel with a
/// handler whose messages a Drain of another channel met. A reliable channel loses nothing and therefore still holds the
/// ring once the queue pool is full — but the mailboxes are dispatched all the same, and the backlog of the unreliable
/// channels cannot keep the queue nodes reserved for it.
/// </summary>
/// <remarks>
/// The server's receive ring holds 64 messages in most tests, so with the table below (four unreliable ring channels, two
/// reliable ones) the queue pool is 64 nodes: 16 reserved for each reliable channel, 32 for the backlog of the unreliable
/// channels together, and a fair share of 8 for each of those. The client sends in batches of at most 32 with the server
/// stepped in between, so a burst alone never fills the ring: every drop the tests see is a drain-queue drop.
/// </remarks>
public class DrainBackpressureTests
{
    private const int Ring = 64;
    private const int DatagramNodes = 32;
    private const int Batch = 32;

    /// <summary>
    /// 2, 3, 6 unordered · 8 keyed sequenced · 9 keyed sequenced, coalescing (a mailbox, never queued) · 10 ordered ·
    /// 11 reliable unordered. Nothing expires.
    /// </summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(3, "b", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(6, "c", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(8, "keyed", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.ExpiryMicros = 0; })
        .Add(9, "latest", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.CoalesceOnReceive = true; o.MaxKeys = 64; o.ExpiryMicros = 0; })
        .Add(10, "stream", ChannelMode.ReliableOrdered)
        .Add(11, "group", ChannelMode.ReliableUnordered)
        .Build();

    private static SessionHarness Harness(int ring = Ring) =>
        new(table: Table, client: DatagramKit.Quiet, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = ring;
        });

    private static byte[] Payload(int index, int size = 4)
    {
        byte[] payload = new byte[size];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static int IndexOf(ReadOnlySpan<byte> payload) => BinaryPrimitives.ReadInt32LittleEndian(payload);

    private static ChannelStatistics Channel(QuiclyPeer peer, ushort channel)
    {
        Assert.True(peer.GetChannelStatistics(channel, out ChannelStatistics statistics));
        return statistics;
    }

    private static MessageHandler CollectIndices(List<int> into) =>
        (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => into.Add(IndexOf(payload));

    /// <summary>
    /// Sends <paramref name="count"/> messages numbered from <paramref name="first"/> on an unreliable channel, in batches
    /// the server's ring takes whole: after each batch the network runs until the server's transport side has accepted it
    /// (the channel's <c>Received</c>, which needs no Poll), then <paramref name="serverStep"/> runs — a Poll, a Drain, or
    /// nothing. The server is never polled by this helper itself.
    /// </summary>
    private static void Deliver(SessionHarness h, ushort channel, int first, int count, Action serverStep, int size = 4, ulong key = 0)
    {
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long arrived = Channel(server, channel).Received;
        for (int sent = 0; sent < count;)
        {
            int n = Math.Min(Batch, count - sent);
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(channel, key), Payload(first + sent + i, size)).Status);
            }

            client.Flush();
            sent += n;
            arrived += n;
            for (int step = 0; step < 1_000 && Channel(server, channel).Received < arrived; step++)
            {
                h.Network.Advance(1_000);
                client.Poll();
                client.Flush();
            }

            Assert.Equal(arrived, Channel(server, channel).Received);
            serverStep();
        }
    }

    private static List<int> DrainAll(QuiclyPeer peer, ushort channel)
    {
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        int n;
        while ((n = peer.Drain(channel, buffer)) > 0)
        {
            for (int i = 0; i < n; i++)
            {
                got.Add(IndexOf(buffer[i].Payload));
            }

            peer.Release(buffer.AsSpan(0, n));
        }

        return got;
    }

    // ------------------------------------------------------------------ unreliable channels: evict, never hold

    [Fact]
    public void Undrained_Unreliable_Channel_Does_Not_Stop_A_Handled_Channel()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> handled = [];
        server.RegisterHandler(3, CollectIndices(handled));

        Deliver(h, 2, 0, 200, () => server.Poll());
        Deliver(h, 3, 0, 100, () => server.Poll());

        Assert.Equal(Enumerable.Range(0, 100), handled);
        PeerStatistics peer = DatagramKit.Statistics(server);
        Assert.Equal(0, peer.ReceiveRingDrops);
        Assert.Equal(0, Channel(server, 3).RingDrops);
        Assert.Equal(0, Channel(server, 3).DrainQueueDrops);

        // The backlog of channel 2 is the unreliable share of the pool, and it lost its oldest messages, not its newest.
        Assert.Equal(200 - DatagramNodes, Channel(server, 2).DrainQueueDrops);
        Assert.Equal(200 - DatagramNodes, peer.DrainQueueDrops);
        Assert.Equal(200, Channel(server, 2).Received);
        Assert.Equal(Enumerable.Range(200 - DatagramNodes, DatagramNodes), DrainAll(server, 2));
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void Undrained_Channel_Does_Not_Stop_Mailbox_Dispatch()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        Dictionary<ulong, int> latest = [];
        server.RegisterHandler(9, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) => latest[header.Key] = IndexOf(payload));

        Deliver(h, 2, 0, 200, () => server.Poll());
        for (int round = 0; round < 3; round++)
        {
            for (ulong key = 1; key <= 8; key++)
            {
                Deliver(h, 9, (round * 100) + (int)key, 1, () => { }, key: key);
            }
        }

        server.Poll();

        Assert.Equal(8, latest.Count);
        for (ulong key = 1; key <= 8; key++)
        {
            Assert.Equal(200 + (int)key, latest[key]);
        }
    }

    [Fact]
    public void Drain_Of_One_Channel_Is_Not_Blocked_By_Another()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;

        Deliver(h, 2, 0, 200, () => server.Poll());
        Deliver(h, 3, 0, 8, () => server.Poll());

        Assert.Equal(Enumerable.Range(0, 8), DrainAll(server, 3));
        Assert.Equal(0, Channel(server, 3).DrainQueueDrops);
    }

    [Fact]
    public void A_Diligent_Drain_Channel_Is_Not_Evicted_By_A_Flooded_One()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> got = [];

        for (int round = 0; round < 10; round++)
        {
            Deliver(h, 2, round * 32, 32, () => { });
            Deliver(h, 3, round * 8, 8, () => { });
            server.Poll();
            got.AddRange(DrainAll(server, 3));
        }

        // Channel 3 stays at its fair share of 8 and is drained every round: the room it needs is taken from the channel
        // that hogs the backlog, never from its own queue.
        Assert.Equal(Enumerable.Range(0, 80), got);
        Assert.Equal(0, Channel(server, 3).DrainQueueDrops);
        Assert.True(Channel(server, 2).DrainQueueDrops > 0, "the flooded channel lost nothing");
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
    }

    [Fact]
    public void Undrained_Channel_Cannot_Pin_The_Receive_Budget()
    {
        // The default ring (4 096) and budget (256 KiB): 600 messages of 1 000 bytes sit in 1 536-byte blocks, 900 KiB if
        // they were all kept. The unreliable backlog may pin a quarter of the budget.
        using SessionHarness h = Harness(ring: 4096);
        QuiclyPeer server = h.Server!;
        List<int> handled = [];
        server.RegisterHandler(3, CollectIndices(handled));

        Deliver(h, 2, 0, 600, () => server.Poll(), size: 1_000);
        Deliver(h, 3, 0, 50, () => server.Poll());

        PeerStatistics peer = DatagramKit.Statistics(server);
        Assert.Equal(0, peer.OutOfReceiveBuffers);
        Assert.Equal(0, peer.ReceiveRingDrops);
        Assert.True(peer.ReceiveBytesOutstanding <= 65_536, $"{peer.ReceiveBytesOutstanding} bytes are pinned by a channel nobody drains");
        Assert.True(peer.ReceiveBytesOutstanding > 32_768, "the backlog was cut far below its byte limit");
        Assert.Equal(Enumerable.Range(0, 50), handled);

        // What is left is the newest part of the flood, in order.
        List<int> kept = DrainAll(server, 2);
        Assert.Equal(Enumerable.Range(600 - kept.Count, kept.Count), kept);
        Assert.Equal(600 - kept.Count, Channel(server, 2).DrainQueueDrops);
    }

    [Fact]
    public void A_Drain_Of_Another_Channel_Loses_Nothing_Of_A_Handled_Channel_And_Keeps_The_Work_Probe_Exact()
    {
        // A Drain of channel 2 meets the messages of channel 3, which has a handler, and queues them for the next Poll; more
        // of them than the pool holds (64), so the next one is held and the rest stay in the ring. Nothing of a handled
        // channel is evicted: the Poll dispatches the queue, the held message and the ring, in order. The count of queued
        // messages that a Poll still has to dispatch must be exact afterwards, or HasPendingWork stays set for good and a
        // host that polls while there is work never sleeps again.
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> handled = [];
        server.RegisterHandler(3, CollectIndices(handled));
        ReceivedMessage[] buffer = new ReceivedMessage[4];

        Deliver(h, 3, 0, 100, () => Assert.Equal(0, server.Drain(2, buffer)));
        Assert.True(server.HasPendingWork, "queued messages of a handled channel are work for the next Poll");
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);

        server.Poll();
        server.Flush();

        Assert.Equal(Enumerable.Range(0, 100), handled);
        Assert.Equal(0, Channel(server, 3).DrainQueueDrops);
        Assert.False(server.HasPendingWork, "the peer reports work although every queue is empty");
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Handled_Channel_Met_By_A_Drain_Takes_Its_Room_From_The_Backlog_Nobody_Drains()
    {
        // Channel 2 is never drained and keeps its backlog of 32; channel 10 (reliable, no handler) holds the other 32
        // nodes. A Drain of channel 6 then meets 20 messages of channel 3, which has a handler: there is no free node, so
        // each one evicts the oldest message of the backlog nobody drains — never a message of the handled channel, and
        // nothing is held.
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<int> handled = [];
        server.RegisterHandler(3, CollectIndices(handled));
        ReceivedMessage[] buffer = new ReceivedMessage[4];

        Deliver(h, 2, 0, 64, () => server.Poll());
        for (int i = 0; i < 32; i++)
        {
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(10), Payload(i)).Status);
        }

        Assert.True(h.RunUntil(() => Channel(server, 10).Received == 32), "the reliable messages did not arrive");
        server.Poll();
        Assert.Equal(32, Channel(server, 2).DrainQueueDrops);

        Deliver(h, 3, 0, 20, () => Assert.Equal(0, server.Drain(6, buffer)));
        server.Poll();

        Assert.Equal(Enumerable.Range(0, 20), handled);
        Assert.Equal(0, Channel(server, 3).DrainQueueDrops);
        Assert.Equal(32 + 20, Channel(server, 2).DrainQueueDrops);
        Assert.Equal(Enumerable.Range(64 - 12, 12), DrainAll(server, 2));
        Assert.Equal(Enumerable.Range(0, 32), DrainAll(server, 10));
    }

    // ------------------------------------------------------------------ a channel that is drained loses nothing

    [Fact]
    public void A_Burst_Larger_Than_The_Pool_Survives_Poll_Then_Drain()
    {
        // 600 messages in one frame on a channel that is drained after every Poll: more than the 512 nodes the backlog may
        // keep, more than fits next to a reliable channel's backlog. They are queued while a node is free, the next one is
        // held, the rest stay in the ring, and the Drain takes all three in order. Nothing is evicted: the channel had
        // nothing queued when the Poll began.
        using SessionHarness h = Harness(ring: 4096);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        for (int i = 0; i < 600; i++)
        {
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(10), Payload(i)).Status);
        }

        Assert.True(h.RunUntil(() => Channel(server, 10).Received == 600), "the reliable messages did not arrive");
        for (int frame = 0; frame < 3; frame++)
        {
            Deliver(h, 2, frame * 600, 600, () => { });
            server.Poll();
            Assert.True(server.HasPendingWork, "a held message is work until the Drain");
            Assert.Equal(Enumerable.Range(frame * 600, 600), DrainAll(server, 2));
        }

        Assert.Equal(0, Channel(server, 2).DrainQueueDrops);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
        Assert.Equal(Enumerable.Range(0, 600), DrainAll(server, 10));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_Channel_Drained_Every_Frame_Loses_Nothing_Whatever_The_Order_Of_Poll_And_Drain(bool pollBetween)
    {
        // Channels 2 and 3 are both drained every frame, 400 messages of 100 bytes each per frame (102 KiB of blocks each,
        // far more than the 64 KiB a backlog may pin). Drain(2) walks the ring and queues channel 3's messages; whether the
        // Poll comes before both drains or between them, channel 3 was drained empty in the frame before, so what is
        // queued for it when the Poll begins is not backlog.
        using SessionHarness h = Harness(ring: 4096);
        QuiclyPeer server = h.Server!;

        // The frame before the first burst: the host has been running, and drained both channels after its last Poll.
        server.Poll();
        Assert.Empty(DrainAll(server, 2));
        Assert.Empty(DrainAll(server, 3));
        for (int frame = 0; frame < 4; frame++)
        {
            Deliver(h, 3, frame * 400, 400, () => { }, size: 100);
            Deliver(h, 2, frame * 400, 400, () => { }, size: 100);
            if (!pollBetween)
            {
                server.Poll();
            }

            Assert.Equal(Enumerable.Range(frame * 400, 400), DrainAll(server, 2));
            if (pollBetween)
            {
                server.Poll();
            }

            Assert.Equal(Enumerable.Range(frame * 400, 400), DrainAll(server, 3));
            server.Flush();
        }

        Assert.Equal(0, DatagramKit.Statistics(server).DrainQueueDrops);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
        Assert.Equal(0, DatagramKit.Statistics(server).OutOfReceiveBuffers);
    }

    // ------------------------------------------------------------------ a channel that is not drained: one Poll interval

    [Fact]
    public void A_Backlog_That_Survives_A_Poll_Is_Cut_To_Its_Limits()
    {
        // 400 messages of 100 bytes (256-byte blocks: 102 400 bytes) arrive in one frame and are queued by the Poll. Nobody
        // drains, so the next Poll makes them backlog and cuts it to a quarter of the receive budget (64 KiB: 256 of those
        // blocks), oldest first. A consumer that comes back gets the newest 256, in order.
        using SessionHarness h = Harness(ring: 4096);
        QuiclyPeer server = h.Server!;

        Deliver(h, 2, 0, 400, () => { }, size: 100);
        server.Poll();
        Assert.Equal(0, Channel(server, 2).DrainQueueDrops);
        Assert.Equal(400 * 256, DatagramKit.Statistics(server).ReceiveBytesOutstanding);

        server.Poll();

        Assert.Equal(144, Channel(server, 2).DrainQueueDrops);
        Assert.Equal(144, DatagramKit.Statistics(server).DrainQueueDrops);
        Assert.Equal(65_536, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
        Assert.Equal(Enumerable.Range(144, 256), DrainAll(server, 2));
    }

    [Fact]
    public void An_Undrained_Unreliable_Channel_Holds_The_Ring_For_One_Poll_Interval_At_Most()
    {
        // Channel 10 (reliable, no handler, not drained) has 40 of the 64 nodes. A burst of 56 on channel 2 then finds 24
        // free: the 25th is held — the channel might be drained in this frame — and the ring stays closed behind it. It
        // is not drained, so the next Poll makes its queue backlog: the held message and the rest of the ring are queued by
        // evicting the channel's own oldest, the ring is open again, and a handled channel behind the burst is served.
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<int> handled = [];
        server.RegisterHandler(3, CollectIndices(handled));
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(10), Payload(i)).Status);
        }

        Assert.True(h.RunUntil(() => Channel(server, 10).Received == 40), "the reliable messages did not arrive");
        server.Poll();

        Deliver(h, 2, 0, 56, () => { });
        Deliver(h, 3, 0, 4, () => { });
        server.Poll();
        Assert.Empty(handled);
        Assert.Equal(0, Channel(server, 2).DrainQueueDrops);
        Assert.True(server.HasPendingWork, "a held message is work for the next Poll");

        server.Poll();
        server.Flush();

        Assert.Equal(Enumerable.Range(0, 4), handled);
        Assert.Equal(32, Channel(server, 2).DrainQueueDrops);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
        Assert.False(server.HasPendingWork, "a channel nobody drains keeps the peer reporting work");
        Assert.Equal(Enumerable.Range(32, 24), DrainAll(server, 2));
        Assert.Equal(Enumerable.Range(0, 40), DrainAll(server, 10));
    }

    [Fact]
    public void A_Poll_That_Dispatches_Nothing_Still_Ends_The_Hold_Of_An_Undrained_Channel()
    {
        // The same hold, and a host that polls with maxItems 0: the held message of a channel without a handler is not a
        // dispatch, so the limit must not keep it — and the ring behind it — waiting.
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(10), Payload(i)).Status);
        }

        Assert.True(h.RunUntil(() => Channel(server, 10).Received == 40), "the reliable messages did not arrive");
        server.Poll();
        Deliver(h, 2, 0, 25, () => { });
        server.Poll();
        Assert.Equal(0, Channel(server, 2).DrainQueueDrops);

        server.Poll(0);

        Assert.Equal(1, Channel(server, 2).DrainQueueDrops);
        Assert.Equal(Enumerable.Range(1, 24), DrainAll(server, 2));
    }

    [Fact]
    public void A_Backlog_Cut_At_The_Pass_Start_Frees_The_Nodes_Reserved_For_Reliable_Channels()
    {
        // A burst of 60 on channel 2 takes 60 of the 64 nodes in one pass (a channel that might be drained this frame
        // may borrow the reserved ones). It is not drained. The next Poll cuts the backlog to its 32 nodes before anything
        // else, so 32 reliable messages that arrive then are queued, not held.
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;

        Deliver(h, 2, 0, 60, () => { });
        server.Poll();
        Assert.Equal(0, Channel(server, 2).DrainQueueDrops);
        for (int i = 0; i < 32; i++)
        {
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(10), Payload(i)).Status);
        }

        Assert.True(h.RunUntil(() => Channel(server, 10).Received == 32), "the reliable messages did not arrive");
        server.Poll();
        server.Flush();

        Assert.Equal(28, Channel(server, 2).DrainQueueDrops);
        Assert.False(server.HasPendingWork, "a reliable message is held although its reserved nodes are free");
        Assert.Equal(Enumerable.Range(0, 32), DrainAll(server, 10));
        Assert.Equal(Enumerable.Range(28, 32), DrainAll(server, 2));
    }

    [Fact]
    public void Undrained_Unreliable_Channel_Does_Not_Keep_The_Host_Busy()
    {
        // Before the backlog was bounded, the message that did not fit was held, the ring stayed closed behind it, and the
        // pending-work probe stayed set for as long as nobody drained: a signal-driven host polled the peer in a loop.
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;

        Deliver(h, 2, 0, 200, () => server.Poll());
        Deliver(h, 8, 0, 100, () => server.Poll(), key: 7);

        for (int i = 0; i < 20; i++)
        {
            server.Poll();
            server.Flush();
            Assert.False(server.HasPendingWork, "a channel nobody drains keeps the peer reporting work");
            h.Network.Advance(1_000);
            h.Client.Poll();
            h.Client.Flush();
        }
    }

    // ------------------------------------------------------------------ reliable channels: never lose, still hold

    [Fact]
    public void Unreliable_Backlog_Leaves_The_Reserved_Nodes_To_Reliable_Channels()
    {
        // The flood on channel 2 is capped at the unreliable share of the pool (32 of 64 nodes), so a reliable channel
        // without a handler still finds its reserved nodes free: its messages are queued, nothing is held, the ring stays
        // open and a handled channel keeps receiving.
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<int> handled = [];
        server.RegisterHandler(3, CollectIndices(handled));

        Deliver(h, 2, 0, 200, () => server.Poll());
        for (int i = 0; i < 32; i++)
        {
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(10), Payload(i)).Status);
        }

        Assert.True(h.RunUntil(() => Channel(server, 10).Received == 32), "the reliable messages did not arrive");
        Deliver(h, 3, 0, 10, () => server.Poll());

        Assert.Equal(Enumerable.Range(0, 10), handled);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
        Assert.Equal(Enumerable.Range(0, 32), DrainAll(server, 10));
        Assert.Equal(0, Channel(server, 10).DrainQueueDrops);
    }

    [Fact]
    public void A_Held_Reliable_Message_Does_Not_Stop_Mailbox_Dispatch()
    {
        // A reliable channel nobody drains fills the pool, its next message is held and the ring stays closed — that much
        // is the documented limit. The mailboxes do not pass through the ring, though: a coalescing channel's handler must
        // keep running (the transport thread keeps accepting those values either way).
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        Dictionary<ulong, int> latest = [];
        server.RegisterHandler(9, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) => latest[header.Key] = IndexOf(payload));

        for (int i = 0; i < 200; i++)
        {
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(10), Payload(i)).Status);
        }

        h.Run(100_000);
        Assert.True(Channel(server, 10).Received > Ring, "the reliable flood did not fill the queue pool");
        Assert.True(server.HasPendingWork, "a held message is work the application has to resolve");

        for (ulong key = 1; key <= 8; key++)
        {
            Deliver(h, 9, (int)key, 1, () => { }, key: key);
        }

        server.Poll();

        Assert.Equal(8, latest.Count);
        for (ulong key = 1; key <= 8; key++)
        {
            Assert.Equal((int)key, latest[key]);
        }
    }

    [Fact]
    public void An_Undrained_Reliable_Channel_Loses_Nothing()
    {
        // The other half of the contract: a reliable message is never evicted. Far more than the pool and the ring hold is
        // sent while nobody drains; the streams are back-pressured, and a consumer that starts draining gets every message,
        // in order.
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        for (int i = 0; i < 500; i++)
        {
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(10), Payload(i)).Status);
        }

        h.Run(200_000);
        Assert.True(Channel(server, 10).Received < 500, "nothing was back-pressured: the test does not reach the limit");

        List<int> got = [];
        Assert.True(h.RunUntil(() =>
        {
            got.AddRange(DrainAll(server, 10));
            return got.Count == 500;
        }), $"{got.Count} of 500 reliable messages arrived");

        Assert.Equal(Enumerable.Range(0, 500), got);
        Assert.Equal(0, Channel(server, 10).DrainQueueDrops);
        Assert.Equal(0, DatagramKit.Statistics(server).DrainQueueDrops);
    }

    // ------------------------------------------------------------------ the queues themselves

    private static ReceiveEntry Entry(PeerCore core, uint sequence, int size = 1)
    {
        Assert.True(core.TryRentReceive(size, out BufferLease lease));
        return new ReceiveEntry { Sequence = sequence, Lease = lease };
    }

    private static uint Take(ReceiveQueues queues, PeerCore core, int channel)
    {
        Assert.True(queues.TryTake(channel, out ReceiveEntry entry));
        core.ReturnReceive(in entry.Lease);
        return entry.Sequence;
    }

    [Fact]
    public void ReceiveQueues_Keep_A_Backlog_Within_Its_Limits_And_Evict_From_The_Longest_Queue()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;
        byte d = ReceiveQueueClass.Datagram;

        // Channels 0-2 unreliable, 3 reliable. 8 nodes: 2 reserved for the reliable channel, 6 for the backlog of the
        // unreliable ones, a fair share of 2 each; 1 024 bytes of backlog, a fair share of 256 each.
        using ReceiveQueues queues = new(new ReceiveQueueLayout(8, 2, 6, 1024, 2, 256), [d, d, d, ReceiveQueueClass.Reliable]);
        Assert.True(queues.IsDatagram(0));
        Assert.False(queues.IsDatagram(3));

        // Queued in one pass and left there: backlog from the next pass on. Past the limit it loses its own oldest.
        for (uint i = 1; i <= 6; i++)
        {
            Assert.True(queues.TryAppendDatagram(0, Entry(core, i), core, mayHold: true));
        }

        Assert.False(queues.IsBacklogged(0));
        queues.BeginPass(core);
        queues.BeginPass(core);
        Assert.True(queues.IsBacklogged(0));
        Assert.Equal(6, queues.BacklogNodes);
        Assert.Equal(0, queues.Drops(0));
        Assert.True(queues.TryAppendDatagram(0, Entry(core, 7), core, mayHold: true));
        Assert.Equal(1, queues.Drops(0));
        Assert.Equal(6, queues.Count(0));
        Assert.Equal(6, queues.Used);

        // A channel that is not backlog uses the free nodes, outside the limit...
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 100), core, mayHold: true));
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 101), core, mayHold: true));
        Assert.Equal(1, queues.Drops(0));
        Assert.Equal(8, queues.Used);

        // ...and with the pool full it takes its room from the backlog, not from itself.
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 102), core, mayHold: true));
        Assert.Equal(2, queues.Drops(0));
        Assert.Equal(0, queues.Drops(1));
        Assert.Equal(5, queues.Count(0));
        Assert.Equal(3, queues.Count(1));

        // Two passes later it is backlog too (nobody drained it), and the backlog of both is cut to the six nodes at once,
        // from the channel with the most.
        queues.BeginPass(core);
        queues.BeginPass(core);
        Assert.True(queues.IsBacklogged(1));
        Assert.Equal(6, queues.BacklogNodes);
        Assert.Equal(4, queues.Drops(0));
        Assert.Equal(0, queues.Drops(1));
        Assert.Equal(3, queues.Count(0));

        // A backlogged channel at its share, and no other one larger: its own oldest goes.
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 103), core, mayHold: true));
        Assert.Equal(1, queues.Drops(1));
        Assert.Equal(4, queues.Drops(0));

        // A backlogged channel under its share takes its room from the one that is further over — the fix for a channel
        // that evicted its own messages at its even share while another one held several times as much — and from itself
        // again once it is the largest.
        queues.BeginPass(core);
        Assert.Equal(5u, Take(queues, core, 0));
        Assert.Equal(6u, Take(queues, core, 0));
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 104), core, mayHold: true));
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 105), core, mayHold: true));
        Assert.Equal(6, queues.BacklogNodes);
        Assert.Equal(5, queues.Count(1));
        Assert.True(queues.TryAppendDatagram(0, Entry(core, 8), core, mayHold: true));
        Assert.True(queues.TryAppendDatagram(0, Entry(core, 9), core, mayHold: true));
        Assert.Equal(4, queues.Drops(0));
        Assert.Equal(3, queues.Drops(1));
        Assert.True(queues.TryAppendDatagram(0, Entry(core, 10), core, mayHold: true));
        Assert.Equal(5, queues.Drops(0));
        Assert.Equal(3, queues.Drops(1));

        // The reserved nodes are still free for the reliable channel, which is never evicted.
        Assert.True(queues.TryAppend(3, Entry(core, 200)));
        Assert.True(queues.TryAppend(3, Entry(core, 201)));
        Assert.Equal(8, queues.Used);
        Assert.False(queues.TryAppend(3, default));
        Assert.Equal(8, core.Counters.DrainQueueDrops);

        // Oldest first.
        Assert.Equal(8u, Take(queues, core, 0));
        Assert.Equal(9u, Take(queues, core, 0));
        Assert.Equal(10u, Take(queues, core, 0));
        Assert.False(queues.IsBacklogged(0));
        Assert.Equal(103u, Take(queues, core, 1));
        Assert.Equal(104u, Take(queues, core, 1));
        Assert.Equal(105u, Take(queues, core, 1));
        Assert.Equal(200u, Take(queues, core, 3));
        Assert.Equal(201u, Take(queues, core, 3));
        Assert.Equal(0, queues.Used);
        Assert.Equal(0, queues.BacklogNodes);
        Assert.Equal(0, core.ReceiveBytesOutstanding);
    }

    [Fact]
    public void ReceiveQueues_Bound_The_Bytes_Of_A_Backlog()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;
        byte d = ReceiveQueueClass.Datagram;

        // Plenty of nodes; 1 024 bytes of backlog, a fair share of 512 per channel. The blocks are 256 bytes (a 100-byte
        // message).
        using ReceiveQueues queues = new(new ReceiveQueueLayout(64, 0, 64, 1024, 32, 512), [d, d]);

        // In the pass that queues them there is no byte limit: they were charged to the budget when they arrived.
        for (uint i = 1; i <= 5; i++)
        {
            Assert.True(queues.TryAppendDatagram(0, Entry(core, i, 100), core, mayHold: true));
        }

        Assert.Equal(1280, queues.Bytes(0));
        Assert.Equal(0, queues.Drops(0));

        // Not drained through a whole pass: backlog, cut to the limit where the next pass starts.
        queues.BeginPass(core);
        Assert.Equal(0, queues.Drops(0));
        queues.BeginPass(core);
        Assert.Equal(1, queues.Drops(0));
        Assert.Equal(1024, queues.BacklogBytes);

        // Byte pressure, the appender over its byte share: its own oldest goes.
        Assert.True(queues.TryAppendDatagram(0, Entry(core, 6, 100), core, mayHold: true));
        Assert.Equal(2, queues.Drops(0));
        Assert.Equal(1024, queues.Bytes(0));

        // The other channel becomes backlog as well; under its share, it takes its room from the one that pins the most.
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 100, 100), core, mayHold: true));
        queues.BeginPass(core);
        queues.BeginPass(core);
        Assert.True(queues.IsBacklogged(1));
        Assert.Equal(3, queues.Drops(0));
        Assert.Equal(0, queues.Drops(1));
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 101, 100), core, mayHold: true));
        Assert.Equal(4, queues.Drops(0));
        Assert.Equal(0, queues.Drops(1));
        Assert.Equal(512, queues.Bytes(0));
        Assert.Equal(512, queues.Bytes(1));

        // One message larger than the whole byte limit empties the backlog and is then taken: a single message always fits.
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 102, 1500), core, mayHold: true));
        Assert.Equal(1, queues.Used);
        Assert.Equal(6, queues.Drops(0));
        Assert.Equal(2, queues.Drops(1));
        Assert.Equal(1536, queues.Bytes(1));
        Assert.Equal(1536, queues.BacklogBytes);
        queues.BeginPass(core);
        Assert.Equal(1, queues.Used);
        Assert.Equal(102u, Take(queues, core, 1));
        Assert.Equal(0, core.ReceiveBytesOutstanding);
    }

    [Fact]
    public void ReceiveQueues_A_Drained_Channel_Is_Not_Backlog()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;
        byte d = ReceiveQueueClass.Datagram;
        using ReceiveQueues queues = new(new ReceiveQueueLayout(8, 0, 8, 1024, 4, 512), [d, d]);

        // Queued and drained empty in the same pass, then queued again (a Drain of the other channel met its messages):
        // the next pass start does not make that backlog, because the application had drained the channel.
        queues.BeginPass(core);
        Assert.True(queues.TryAppendDatagram(0, Entry(core, 1), core, mayHold: true));
        Assert.Equal(1u, Take(queues, core, 0));
        for (uint i = 2; i <= 9; i++)
        {
            Assert.True(queues.TryAppendDatagram(0, Entry(core, i), core, mayHold: true));
        }

        queues.BeginPass(core);
        Assert.False(queues.IsBacklogged(0));

        // The pool is full of its messages and nothing is backlog: the caller holds the next one, whichever channel's.
        Assert.False(queues.TryAppendDatagram(0, default, core, mayHold: true));
        Assert.False(queues.TryAppendDatagram(1, default, core, mayHold: true));
        Assert.Equal(0, core.Counters.DrainQueueDrops);

        // A take that finds the queue empty counts as a drain too.
        Assert.False(queues.TryTake(1, out _));
        Assert.True(queues.TryTake(0, out ReceiveEntry taken));
        core.ReturnReceive(in taken.Lease);
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 100), core, mayHold: true));
        queues.BeginPass(core);
        Assert.False(queues.IsBacklogged(1));

        // Channel 0 was not drained empty in that pass: backlog now, and a message that was held across the pass start
        // (mayHold false) evicts its oldest.
        Assert.True(queues.IsBacklogged(0));
        Assert.Equal(7, queues.BacklogNodes);
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 101), core, mayHold: false));
        Assert.Equal(1, queues.Drops(0));

        // Drained empty, the channel stops being backlog at once.
        while (queues.TryTake(0, out taken))
        {
            core.ReturnReceive(in taken.Lease);
        }

        Assert.False(queues.IsBacklogged(0));
        Assert.Equal(0, queues.BacklogNodes);
        queues.ReleaseAll(core);
        Assert.Equal(0, core.ReceiveBytesOutstanding);
    }

    [Fact]
    public void ReceiveQueues_Hold_Or_Drop_The_New_Message_When_Nothing_Can_Be_Evicted()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;

        // The whole pool is taken by a channel that is never evicted (more than its reservation: a reliable backlog may
        // take every free node).
        using ReceiveQueues queues = new(new ReceiveQueueLayout(2, 1, 1, 1024, 1, 1024), [ReceiveQueueClass.Datagram, ReceiveQueueClass.Reliable]);
        Assert.True(queues.TryAppend(1, Entry(core, 1)));
        Assert.True(queues.TryAppend(1, Entry(core, 2)));

        // The caller holds it for the rest of the pass: the application may drain the reliable channel in this frame.
        ReceiveEntry entry = Entry(core, 3);
        Assert.False(queues.TryAppendDatagram(0, in entry, core, mayHold: true));
        Assert.Equal(0, queues.Drops(0));
        Assert.Equal(0, core.Counters.DrainQueueDrops);

        // Held across a pass start and still no room: an unreliable message is not held a second time.
        queues.BeginPass(core);
        Assert.True(queues.TryAppendDatagram(0, in entry, core, mayHold: false));

        Assert.Equal(1, queues.Drops(0));
        Assert.Equal(0, queues.Drops(1));
        Assert.Equal(1, core.Counters.DrainQueueDrops);
        Assert.Equal(0, queues.Count(0));
        Assert.Equal(2, queues.Count(1));
        queues.ReleaseAll(core);
        Assert.Equal(0, core.ReceiveBytesOutstanding);
    }

    [Fact]
    public void ReceiveQueues_Count_What_A_Poll_Has_To_Dispatch_And_Never_Evict_A_Handled_Channel()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;
        byte d = ReceiveQueueClass.Datagram;
        using ReceiveQueues queues = new(new ReceiveQueueLayout(4, 0, 4, 1024, 2, 512), [d, d]);

        Assert.True(queues.TryAppendDatagram(0, Entry(core, 1), core, mayHold: true));
        Assert.True(queues.TryAppendDatagram(0, Entry(core, 2), core, mayHold: true));
        Assert.Equal(0, queues.QueuedHandled);

        queues.SetHandled(0, true);
        queues.SetHandled(0, true);
        Assert.Equal(2, queues.QueuedHandled);
        Assert.True(queues.TryAppendDatagram(0, Entry(core, 3), core, mayHold: true));
        Assert.True(queues.TryAppendDatagram(0, Entry(core, 4), core, mayHold: true));
        Assert.Equal(4, queues.QueuedHandled);

        // The pool is full of the handled channel's messages. Its next one is held, whatever the caller allows; a message
        // of the other channel is held, or dropped itself — never at the handled channel's cost. A pass start changes
        // nothing: a handled channel is no backlog.
        queues.BeginPass(core);
        queues.BeginPass(core);
        Assert.False(queues.IsBacklogged(0));
        Assert.False(queues.TryAppendDatagram(0, default, core, mayHold: true));
        Assert.False(queues.TryAppendDatagram(0, default, core, mayHold: false));
        Assert.False(queues.TryAppendDatagram(1, default, core, mayHold: true));
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 100), core, mayHold: false));
        Assert.Equal(1, queues.Drops(1));
        Assert.Equal(0, queues.Drops(0));
        Assert.Equal(4, queues.QueuedHandled);

        // With a backlog of the other channel in the pool, the handled channel takes its room from there.
        Assert.Equal(1u, Take(queues, core, 0));
        Assert.Equal(2u, Take(queues, core, 0));
        Assert.Equal(2, queues.QueuedHandled);
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 101), core, mayHold: true));
        Assert.True(queues.TryAppendDatagram(1, Entry(core, 102), core, mayHold: true));
        queues.BeginPass(core);
        queues.BeginPass(core);
        Assert.True(queues.IsBacklogged(1));
        Assert.True(queues.TryAppendDatagram(0, Entry(core, 5), core, mayHold: true));
        Assert.Equal(3, queues.QueuedHandled);
        Assert.Equal(2, queues.Drops(1));
        Assert.Equal(0, queues.Drops(0));
        Assert.Equal(1, queues.BacklogNodes);

        Assert.Equal(3u, Take(queues, core, 0));
        Assert.Equal(2, queues.QueuedHandled);
        queues.SetHandled(0, false);
        Assert.Equal(0, queues.QueuedHandled);

        // A channel that gets a handler stops being backlog: the next Poll dispatches what it has queued.
        queues.SetHandled(1, true);
        Assert.Equal(1, queues.QueuedHandled);
        Assert.False(queues.IsBacklogged(1));
        Assert.Equal(0, queues.BacklogNodes);
        Assert.Equal(0, queues.BacklogBytes);

        queues.ReleaseAll(core);
        Assert.Equal(0, queues.QueuedHandled);
        Assert.Equal(0, queues.Used);
        Assert.Equal(2, queues.Drops(1));
        Assert.Equal(0, core.ReceiveBytesOutstanding);
    }

    private static ChannelTable TableOf(int reliable, int unreliable)
    {
        ChannelTableBuilder builder = ChannelTable.Create();
        int id = ChannelDefinition.MinId;
        for (int i = 0; i < unreliable; i++)
        {
            builder.Add((ushort)id, "u" + id, (i & 1) == 0 ? ChannelMode.UnreliableUnordered : ChannelMode.UnreliableSequenced);
            id++;
        }

        for (int i = 0; i < reliable; i++)
        {
            builder.Add((ushort)id, "r" + id, (i & 1) == 0 ? ChannelMode.ReliableOrdered : ChannelMode.ReliableUnordered);
            id++;
        }

        // Classes that count for neither: a mailbox channel, ReliableLatest, Bulk.
        builder.Add((ushort)id++, "coalescing", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.CoalesceOnReceive = true; });
        builder.Add((ushort)id++, "latest", ChannelMode.ReliableLatest);
        builder.Add((ushort)id, "bulk", ChannelMode.Bulk);
        return builder.Build();
    }

    [Theory]
    //          ring  Qr    Qu  budget   capacity reliable datagramNodes datagramBytes nodeFair byteFair
    [InlineData(4096, 0, 1, 262_144, 1024, 0, 1024, 65_536, 1024, 65_536)]
    [InlineData(4096, 1, 1, 262_144, 1024, 512, 512, 65_536, 512, 65_536)]
    [InlineData(4096, 2, 4, 262_144, 1024, 256, 512, 65_536, 128, 16_384)]
    [InlineData(64, 2, 4, 262_144, 64, 16, 32, 65_536, 8, 16_384)]
    [InlineData(8, 7, 2, 262_144, 14, 1, 7, 65_536, 3, 32_768)]
    [InlineData(2, 3, 0, 262_144, 6, 1, 3, 65_536, 0, 0)]
    [InlineData(4096, 2000, 0, 262_144, 4000, 1, 2000, 65_536, 0, 0)]
    [InlineData(64, 2, 4, 1, 64, 16, 32, 1, 8, 1)]
    [InlineData(64, 0, 0, 262_144, 64, 0, 64, 65_536, 0, 0)]
    public void ReceiveQueueLayout_Boundaries(int ring, int reliable, int unreliable, int budget,
        int capacity, int reliableNodes, int datagramNodes, int datagramBytes, int nodeFair, int byteFair)
    {
        ChannelTable table = TableOf(reliable, unreliable);
        ReceiveQueueLayout layout = ReceiveQueueLayout.Compute(table.All, ring, budget);

        Assert.Equal(capacity, layout.Capacity);
        Assert.Equal(reliableNodes, layout.ReliableNodes);
        Assert.Equal(datagramNodes, layout.DatagramNodes);
        Assert.Equal(datagramBytes, layout.DatagramBytes);
        Assert.Equal(nodeFair, layout.NodeFair);
        Assert.Equal(byteFair, layout.ByteFair);

        // The unreliable channels can never occupy the nodes reserved for the reliable ones.
        Assert.Equal(layout.Capacity, layout.DatagramNodes + (reliable * layout.ReliableNodes));
        Assert.True(layout.DatagramNodes >= 1);
    }

    [Fact]
    public void ReceiveQueueClass_Follows_The_Channel_Definition()
    {
        Assert.Equal(ReceiveQueueClass.Datagram, ReceiveQueueClass.Of(Table[2]!));
        Assert.Equal(ReceiveQueueClass.Datagram, ReceiveQueueClass.Of(Table[8]!));
        Assert.Equal(ReceiveQueueClass.Other, ReceiveQueueClass.Of(Table[9]!));
        Assert.Equal(ReceiveQueueClass.Reliable, ReceiveQueueClass.Of(Table[10]!));
        Assert.Equal(ReceiveQueueClass.Reliable, ReceiveQueueClass.Of(Table[11]!));
        Assert.Equal(ReceiveQueueClass.Other, ReceiveQueueClass.Of(TestTables.AllModes[6]!));
        Assert.Equal(ReceiveQueueClass.Other, ReceiveQueueClass.Of(TestTables.AllModes[7]!));
    }
}
