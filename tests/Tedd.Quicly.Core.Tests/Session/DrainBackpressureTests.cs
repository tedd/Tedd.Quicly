using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// What a channel without a handler costs the rest of the peer when nobody drains it (docs/design/session-layer.md §4.4
/// "Channels without a handler", PROTOCOL.md §7). An unreliable channel keeps a bounded backlog and loses its own oldest
/// messages, counted in <c>DrainQueueDrops</c>; it never closes the receive ring, never stops mailbox dispatch, never pins
/// the receive budget and never keeps the host busy. A reliable channel loses nothing and therefore still holds the ring
/// once the queue pool is full — but the mailboxes are dispatched all the same, and the unreliable channels cannot take the
/// queue nodes reserved for it.
/// </summary>
/// <remarks>
/// The server's receive ring holds 64 messages in most tests, so with the table below (four unreliable ring channels, two
/// reliable ones) the queue pool is 64 nodes: 16 reserved for each reliable channel, 32 for the unreliable channels
/// together, and a fair share of 8 for each of those. The client sends in batches of at most 32 with the server stepped in
/// between, so a burst alone never fills the ring: every drop the tests see is a drain-queue drop.
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
    public void Eviction_Of_A_Handled_Channel_Keeps_The_Work_Probe_Exact()
    {
        // A Drain of channel 2 meets the messages of channel 3, which has a handler, and queues them for the next Poll; more
        // of them than the backlog holds, so the oldest are evicted. The count of queued messages that a Poll still has to
        // dispatch must follow the evictions, or HasPendingWork stays set for good and a host that polls while there is work
        // never sleeps again.
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> handled = [];
        server.RegisterHandler(3, CollectIndices(handled));
        ReceivedMessage[] buffer = new ReceivedMessage[4];

        Deliver(h, 3, 0, 100, () => Assert.Equal(0, server.Drain(2, buffer)));
        Assert.True(server.HasPendingWork, "queued messages of a handled channel are work for the next Poll");

        server.Poll();
        server.Flush();

        Assert.Equal(Enumerable.Range(100 - DatagramNodes, DatagramNodes), handled);
        Assert.Equal(100 - DatagramNodes, Channel(server, 3).DrainQueueDrops);
        Assert.False(server.HasPendingWork, "the peer reports work although every queue is empty");
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
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
    public void ReceiveQueues_Evicts_From_The_Longest_Queue()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;
        byte d = ReceiveQueueClass.Datagram;

        // Channels 0-2 unreliable, 3 reliable. 8 nodes: 2 reserved for the reliable channel, 6 for the unreliable ones,
        // a fair share of 2 each; 1 024 bytes for the unreliable ones, a fair share of 256 each.
        using ReceiveQueues queues = new(new ReceiveQueueLayout(8, 2, 6, 1024, 2, 256), [d, d, d, ReceiveQueueClass.Reliable]);
        Assert.True(queues.IsEvicting(0));
        Assert.False(queues.IsEvicting(3));

        // One channel alone may use the whole unreliable share; past it, it loses its own oldest.
        for (uint i = 1; i <= 6; i++)
        {
            Assert.True(queues.AppendEvicting(0, Entry(core, i), core));
        }

        Assert.Equal(0, queues.Drops(0));
        Assert.True(queues.AppendEvicting(0, Entry(core, 7), core));
        Assert.Equal(1, queues.Drops(0));
        Assert.Equal(6, queues.Count(0));
        Assert.Equal(6, queues.Used);

        // A channel under its share takes its room from the longest queue, not from itself...
        Assert.True(queues.AppendEvicting(1, Entry(core, 100), core));
        Assert.True(queues.AppendEvicting(1, Entry(core, 101), core));
        Assert.Equal(3, queues.Drops(0));
        Assert.Equal(0, queues.Drops(1));
        Assert.Equal(4, queues.Count(0));

        // ...and once it is at its share, from its own.
        Assert.True(queues.AppendEvicting(1, Entry(core, 102), core));
        Assert.Equal(1, queues.Drops(1));
        Assert.Equal(3, queues.Drops(0));
        Assert.Equal(2, queues.Count(1));

        // The reserved nodes are still free for the reliable channel, which is never evicted...
        Assert.True(queues.TryAppend(3, Entry(core, 200)));
        Assert.True(queues.TryAppend(3, Entry(core, 201)));
        Assert.Equal(8, queues.Used);
        Assert.False(queues.TryAppend(3, default));

        // ...and the unreliable channels keep evicting among themselves with the pool full.
        Assert.True(queues.AppendEvicting(2, Entry(core, 300), core));
        Assert.Equal(4, queues.Drops(0));
        Assert.Equal(2, queues.Count(3));
        Assert.Equal(5, core.Counters.DrainQueueDrops);

        // Oldest first: channel 0 kept its newest three, channel 1 its newest two.
        Assert.Equal(5u, Take(queues, core, 0));
        Assert.Equal(6u, Take(queues, core, 0));
        Assert.Equal(7u, Take(queues, core, 0));
        Assert.Equal(101u, Take(queues, core, 1));
        Assert.Equal(102u, Take(queues, core, 1));
        Assert.Equal(300u, Take(queues, core, 2));
        Assert.Equal(200u, Take(queues, core, 3));
        Assert.Equal(201u, Take(queues, core, 3));
        Assert.Equal(0, queues.Used);
        Assert.Equal(0, core.ReceiveBytesOutstanding);
    }

    [Fact]
    public void ReceiveQueues_Bounds_The_Bytes_Of_Unreliable_Channels()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;
        byte d = ReceiveQueueClass.Datagram;

        // Plenty of nodes; 1 024 bytes, a fair share of 512 per channel. The blocks are 256 bytes (a 100-byte message).
        using ReceiveQueues queues = new(new ReceiveQueueLayout(64, 0, 64, 1024, 32, 512), [d, d]);
        for (uint i = 1; i <= 4; i++)
        {
            Assert.True(queues.AppendEvicting(0, Entry(core, i, 100), core));
        }

        Assert.Equal(1024, queues.Bytes(0));
        Assert.Equal(0, queues.Drops(0));

        // Byte pressure, the appender over its byte share: its own oldest goes.
        Assert.True(queues.AppendEvicting(0, Entry(core, 5, 100), core));
        Assert.Equal(1, queues.Drops(0));
        Assert.Equal(1024, queues.Bytes(0));

        // Byte pressure, the appender under its share: the channel that pins the most bytes loses.
        Assert.True(queues.AppendEvicting(1, Entry(core, 100, 100), core));
        Assert.Equal(2, queues.Drops(0));
        Assert.Equal(0, queues.Drops(1));
        Assert.Equal(768, queues.Bytes(0));
        Assert.Equal(256, queues.Bytes(1));

        // One message larger than the whole byte limit empties the class and is then taken: a single message always fits.
        Assert.True(queues.AppendEvicting(1, Entry(core, 101, 1500), core));
        Assert.Equal(1, queues.Used);
        Assert.Equal(5, queues.Drops(0));
        Assert.Equal(1, queues.Drops(1));
        Assert.Equal(1536, queues.Bytes(1));
        Assert.Equal(101u, Take(queues, core, 1));
        Assert.Equal(0, core.ReceiveBytesOutstanding);
    }

    [Fact]
    public void ReceiveQueues_Drops_The_New_Message_When_Nothing_Can_Be_Evicted()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;

        // The whole pool is taken by a channel that is never evicted (more than its reservation: a handled reliable
        // channel met by a Drain of another channel can do that).
        using ReceiveQueues queues = new(new ReceiveQueueLayout(2, 1, 1, 1024, 1, 1024), [ReceiveQueueClass.Datagram, ReceiveQueueClass.Reliable]);
        Assert.True(queues.TryAppend(1, Entry(core, 1)));
        Assert.True(queues.TryAppend(1, Entry(core, 2)));

        Assert.False(queues.AppendEvicting(0, Entry(core, 3), core));

        Assert.Equal(1, queues.Drops(0));
        Assert.Equal(0, queues.Drops(1));
        Assert.Equal(1, core.Counters.DrainQueueDrops);
        Assert.Equal(0, queues.Count(0));
        Assert.Equal(2, queues.Count(1));
        queues.ReleaseAll(core);
        Assert.Equal(0, core.ReceiveBytesOutstanding);
    }

    [Fact]
    public void ReceiveQueues_Count_What_A_Poll_Has_To_Dispatch()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;
        byte d = ReceiveQueueClass.Datagram;
        using ReceiveQueues queues = new(new ReceiveQueueLayout(4, 0, 4, 1024, 2, 512), [d, d]);

        Assert.True(queues.AppendEvicting(0, Entry(core, 1), core));
        Assert.True(queues.AppendEvicting(0, Entry(core, 2), core));
        Assert.Equal(0, queues.QueuedHandled);

        queues.SetHandled(0, true);
        queues.SetHandled(0, true);
        Assert.Equal(2, queues.QueuedHandled);
        Assert.True(queues.AppendEvicting(0, Entry(core, 3), core));
        Assert.True(queues.AppendEvicting(0, Entry(core, 4), core));
        Assert.Equal(4, queues.QueuedHandled);

        // Evictions of the handled channel — its own, and one forced by the other channel — are not left in the count.
        Assert.True(queues.AppendEvicting(0, Entry(core, 5), core));
        Assert.Equal(4, queues.QueuedHandled);
        Assert.True(queues.AppendEvicting(1, Entry(core, 100), core));
        Assert.Equal(3, queues.QueuedHandled);
        Assert.Equal(2, queues.Drops(0));

        Assert.Equal(3u, Take(queues, core, 0));
        Assert.Equal(2, queues.QueuedHandled);
        queues.SetHandled(0, false);
        Assert.Equal(0, queues.QueuedHandled);
        queues.SetHandled(1, true);
        Assert.Equal(1, queues.QueuedHandled);

        queues.ReleaseAll(core);
        Assert.Equal(0, queues.QueuedHandled);
        Assert.Equal(0, queues.Used);
        Assert.Equal(2, queues.Drops(0));
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
