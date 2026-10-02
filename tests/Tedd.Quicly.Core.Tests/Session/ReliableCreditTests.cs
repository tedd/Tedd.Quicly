using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// A reliable stream channel (ReliableOrdered, ReliableUnordered) without a handler that the application does not drain
/// holds back only its own streams (docs/design/session-layer.md §4.4 "Channels without a handler", PROTOCOL.md §7). Its
/// undelivered messages are bounded by a per-channel credit on the stream receive path (<see cref="ReceiveCredit"/>), so
/// they can never fill the drain queues, stop the receive ring and with it every other channel — and nothing of it is lost:
/// the channel's sender is held by QUIC flow control until the application drains or registers a handler.
/// </summary>
/// <remarks>
/// The server's receive ring holds 64 messages in most tests, so with <see cref="TestTables.Plumbing"/> (two reliable
/// channels) the drain queues have 64 nodes, 16 of them reserved for each reliable channel: a reliable channel without a
/// handler has at most 16 messages waiting for the application.
/// </remarks>
public class ReliableCreditTests
{
    private const int Ring = 64;

    /// <summary>Messages a reliable channel without a handler may have waiting at <see cref="Ring"/>.</summary>
    private const int Limit = 16;

    private const ushort Datagrams = 3;
    private const ushort Ordered = 10;
    private const ushort Groups = 11;

    /// <summary>2, 3 unordered datagrams · 6 unordered LZ4 · 8 keyed sequenced · 10 ordered stream · 11 group streams.</summary>
    private static readonly ChannelTable Table = TestTables.Plumbing;

    private static SessionHarness Harness(Action<PeerOptions>? server = null, Action<PeerOptions>? client = null, bool connect = true) =>
        new(table: Table, connect: connect,
            client: o =>
            {
                // One group per flush, no pings: the streams and the counts in the tests are exact.
                GroupKit.Prompt(o);
                client?.Invoke(o);
            },
            server: o =>
            {
                GroupKit.Prompt(o);
                o.ReceiveRingCapacity = Ring;
                server?.Invoke(o);
            });

    private static byte[] Payload(int index, int size = 4)
    {
        byte[] payload = new byte[size];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static int IndexOf(ReadOnlySpan<byte> payload) => BinaryPrimitives.ReadInt32LittleEndian(payload);

    private static MessageHandler Collect(List<int> into) =>
        (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => into.Add(IndexOf(payload));

    private static ChannelStatistics Channel(QuiclyPeer peer, ushort channel) => DatagramKit.ChannelStats(peer, channel);

    /// <summary>Messages of a channel the transport thread accepted and the application has not been given yet.</summary>
    private static int Waiting(QuiclyPeer peer, ushort channel) => peer.Core.Credit.Waiting(peer.Core.ChannelIndexOf(channel));

    /// <summary>Sends numbered messages in batches of sixteen with both ends pumped in between, so a burst alone never fills a ring.</summary>
    private static void Send(SessionHarness h, QuiclyPeer sender, ushort channel, int first, int count, int size = 4)
    {
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(SendStatus.Admitted, sender.SendCopy(new SendHeader(channel), Payload(first + i, size)).Status);
            if ((i & 15) == 15)
            {
                h.Run(2_000);
            }
        }

        h.Run(2_000);
    }

    /// <summary>Sends numbered messages while only the sender is pumped: the receiver is late with its Poll.</summary>
    private static void SendToALateReceiver(SessionHarness h, QuiclyPeer sender, ushort channel, int first, int count)
    {
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(SendStatus.Admitted, sender.SendCopy(new SendHeader(channel), Payload(first + i)).Status);
        }

        for (int step = 0; step < 8; step++)
        {
            sender.Poll();
            sender.Flush();
            h.Network.Advance(1_000);
        }
    }

    private static List<int> DrainAll(QuiclyPeer peer, ushort channel, int batch = 16)
    {
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[batch];
        int taken;
        while ((taken = peer.Drain(channel, buffer)) > 0)
        {
            for (int i = 0; i < taken; i++)
            {
                got.Add(IndexOf(buffer[i].Payload));
            }

            peer.Release(buffer.AsSpan(0, taken));
        }

        return got;
    }

    /// <summary>Pumps both ends and drains <paramref name="channel"/> until <paramref name="count"/> messages came out of it.</summary>
    private static List<int> DrainUntil(SessionHarness h, QuiclyPeer peer, ushort channel, int count)
    {
        List<int> got = [];
        Assert.True(h.RunUntil(() =>
        {
            got.AddRange(DrainAll(peer, channel));
            return got.Count >= count;
        }, 5_000_000), $"only {got.Count} of {count} messages came out of channel {channel}");
        return got;
    }

    // ------------------------------------------------------------------ an undrained channel holds back only itself

    [Fact]
    public void An_Undrained_Ordered_Channel_Holds_Back_Only_Itself()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> groups = [];
        List<int> datagrams = [];
        server.RegisterHandler(Groups, Collect(groups));
        server.RegisterHandler(Datagrams, Collect(datagrams));

        // Channel 10 has no handler and is not drained: far more messages than the ring and the drain queues hold.
        Send(h, h.Client, Ordered, 0, 500);
        Assert.Equal(Limit, Waiting(server, Ordered));
        Assert.Equal(Limit, Channel(server, Ordered).Received);
        Assert.Equal(1, Channel(server, Ordered).BacklogHolds);

        Send(h, h.Client, Groups, 0, 100);
        Send(h, h.Client, Datagrams, 0, 100);
        Assert.True(h.RunUntil(() => groups.Count == 100 && datagrams.Count == 100, 2_000_000),
            $"the other channels stalled behind the undrained one: {groups.Count} group messages, {datagrams.Count} datagrams");
        Assert.Equal(Enumerable.Range(0, 100), groups.Order());
        Assert.Equal(Enumerable.Range(0, 100), datagrams);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.Equal(0, statistics.ReceiveRingDrops);
        Assert.Equal(0, statistics.OutOfReceiveBuffers);
        Assert.Equal(0, Channel(server, Groups).BacklogHolds);
        Assert.Equal(1, Channel(server, Ordered).BacklogHolds);
        Assert.Equal(1, statistics.StreamReceivePends);

        // Nothing of the undrained channel was lost: draining it yields every message, in order.
        Assert.Equal(Enumerable.Range(0, 500), DrainUntil(h, server, Ordered, 500));
        Assert.Equal(0, Waiting(server, Ordered));
    }

    [Fact]
    public void An_Undrained_Group_Channel_Holds_Back_Only_Itself()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> ordered = [];
        List<int> datagrams = [];
        server.RegisterHandler(Ordered, Collect(ordered));
        server.RegisterHandler(Datagrams, Collect(datagrams));

        // The ordered channel's stream is open before the flood: the connection's stream slots are shared, and an
        // undrained group channel ends up holding all of those that are free (the test after this one).
        Send(h, h.Client, Ordered, 0, 1);
        Assert.Single(ordered);

        // Channel 11 has no handler and is not drained; its groups arrive on several streams at once.
        Send(h, h.Client, Groups, 0, 500);
        Assert.Equal(Limit, Waiting(server, Groups));

        Send(h, h.Client, Ordered, 1, 100);
        Send(h, h.Client, Datagrams, 0, 100);
        Assert.True(h.RunUntil(() => ordered.Count == 101 && datagrams.Count == 100, 2_000_000),
            $"the other channels stalled behind the undrained one: {ordered.Count} ordered messages, {datagrams.Count} datagrams");
        Assert.Equal(Enumerable.Range(0, 101), ordered);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.Equal(0, statistics.ReceiveRingDrops);
        Assert.Equal(0, statistics.StreamsReset);

        Assert.Equal(Enumerable.Range(0, 500), DrainUntil(h, server, Groups, 500).Order());
        Assert.Equal(0, Waiting(server, Groups));
    }

    [Fact]
    public void An_Undrained_Group_Channel_Gives_The_Stream_Slots_It_Took_Back_When_It_Is_Drained()
    {
        // What the credit cannot confine (PROTOCOL.md §7): the connection's stream limit is one number for all channels,
        // and every group of the undrained channel holds a slot until it is read. Its sender keeps sending, so it ends up
        // with every free slot, and the ordered channel — which has not opened its stream yet — has to wait for one.
        // Datagram channels do not care, nothing is lost, and draining the channel gives the slots back.
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> ordered = [];
        List<int> datagrams = [];
        server.RegisterHandler(Ordered, Collect(ordered));
        server.RegisterHandler(Datagrams, Collect(datagrams));

        Send(h, h.Client, Groups, 0, 500);
        Send(h, h.Client, Ordered, 0, 20);
        Send(h, h.Client, Datagrams, 0, 100);
        h.Run(100_000);
        Assert.Equal(Enumerable.Range(0, 100), datagrams);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);

        Assert.Equal(Enumerable.Range(0, 500), DrainUntil(h, server, Groups, 500).Order());
        Assert.True(h.RunUntil(() => ordered.Count == 20, 2_000_000), $"{ordered.Count} of 20 ordered messages arrived after the group channel was drained");
        Assert.Equal(Enumerable.Range(0, 20), ordered);
        Assert.Equal(0, DatagramKit.Statistics(server).StreamsReset);
    }

    [Fact]
    public void A_Channel_That_Is_Out_Of_Credit_Is_Not_Work_For_The_Host()
    {
        // A host that polls while the peer reports work must come to rest: the streams the channel holds back are resumed
        // when the application takes messages, not by every Poll, and they do not keep HasPendingWork set.
        CountingWorkSignal signal = new();
        using SessionHarness h = Harness(server: o => o.WorkSignal = signal);
        QuiclyPeer server = h.Server!;

        Send(h, h.Client, Ordered, 0, 200);
        Send(h, h.Client, Groups, 0, 200);
        h.Run(100_000);
        Assert.Equal(Limit, Waiting(server, Ordered));
        Assert.Equal(Limit, Waiting(server, Groups));
        Assert.False(server.HasPendingWork, "a channel that waits for the application is reported as work a Poll could do");

        int signals = signal.Calls;
        long pends = DatagramKit.Statistics(server).StreamReceivePends;
        h.Run(100_000);
        Assert.Equal(signals, signal.Calls);
        Assert.Equal(pends, DatagramKit.Statistics(server).StreamReceivePends);
        Assert.False(server.HasPendingWork);
    }

    // ------------------------------------------------------------------ how the credit comes back

    [Fact]
    public void Draining_Gives_Credit_Back_Without_A_Poll()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        Send(h, h.Client, Ordered, 0, 100);
        Assert.Equal(Limit, Channel(server, Ordered).Received);

        // The application takes four of what waits. No Poll follows: the Drain itself resumes the stream, and the
        // transport delivers four more messages into the ring.
        ReceivedMessage[] four = new ReceivedMessage[4];
        Assert.Equal(4, server.Drain(Ordered, four));
        Assert.Equal(Enumerable.Range(0, 4), four.Select(m => IndexOf(m.Payload)));
        server.Release(four);
        h.Network.Advance(5_000);
        Assert.Equal(Limit + 4, Channel(server, Ordered).Received);
        Assert.Equal(Limit, Waiting(server, Ordered));
        Assert.Equal(CreditState.Unread, server.Core.Credit.State(server.Core.ChannelIndexOf(Ordered)));
    }

    // ------------------------------------------------------------------ a channel the application drains

    [Fact]
    public void A_Channel_The_Application_Drains_Is_Limited_By_The_Ring_Alone()
    {
        // The share of a channel nobody reads is not the price of reading with Drain. A Drain that leaves nothing queued
        // shows that the application reads the channel, and from then on it is given what a handler would be given: a
        // burst as large as the ring, every frame, without one stream being held back.
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        ReceiveCredit credit = server.Core.Credit;
        int index = server.Core.ChannelIndexOf(Ordered);
        Assert.Equal(CreditState.Unread, credit.State(index));

        // The first frame of a consumer: there is nothing to take yet.
        Assert.Empty(DrainAll(server, Ordered));
        Assert.Equal(CreditState.Drained, credit.State(index));
        Assert.Equal(Ring, credit.DrainedCountLimit);

        int sent = 0;
        for (int frame = 0; frame < 10; frame++)
        {
            SendToALateReceiver(h, h.Client, Ordered, sent, 60);
            Assert.Equal(60, Waiting(server, Ordered));
            server.Poll();
            Assert.Equal(Enumerable.Range(sent, 60), DrainAll(server, Ordered));
            server.Flush();
            sent += 60;
        }

        Assert.Equal(0, credit.Pends(index));
        Assert.Equal(CreditState.Drained, credit.State(index));
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Drained_Channel_Takes_No_More_Than_The_Ring_Holds()
    {
        // What a drained channel may have waiting is the ring's capacity in messages: the drain queues have a node for
        // every one of them, whatever else is queued, so it cannot be the channel the ring stops for.
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> datagrams = [];
        server.RegisterHandler(Datagrams, Collect(datagrams));
        Assert.Empty(DrainAll(server, Groups));

        // Three hundred messages while the application polls and drains another channel, which moves them out of the ring
        // each time: the ring never stops the transport thread, the credit does.
        ReceivedMessage[] buffer = new ReceivedMessage[4];
        for (int i = 0; i < 300; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Groups), Payload(i)).Status);
            if ((i & 15) == 15)
            {
                h.Client.Poll();
                h.Client.Flush();
                h.Network.Advance(2_000);
                Assert.Equal(0, server.Drain(2, buffer));
            }
        }

        h.Client.Flush();
        for (int step = 0; step < 20; step++)
        {
            h.Network.Advance(2_000);
            h.Client.Poll();
            h.Client.Flush();
            Assert.Equal(0, server.Drain(2, buffer));
        }

        Assert.Equal(Ring, Waiting(server, Groups));
        Send(h, h.Client, Datagrams, 0, 50);
        Assert.Equal(Enumerable.Range(0, 50), datagrams);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
        Assert.Equal(Enumerable.Range(0, 300), DrainUntil(h, server, Groups, 300).Order());
    }

    [Fact]
    public void A_Channel_The_Application_Stops_Draining_Is_Held_To_Its_Share_After_One_Pass()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        ReceiveCredit credit = server.Core.Credit;
        int index = server.Core.ChannelIndexOf(Ordered);
        List<int> datagrams = [];
        server.RegisterHandler(Datagrams, Collect(datagrams));
        Assert.Empty(DrainAll(server, Ordered));
        Assert.Equal(CreditState.Drained, credit.State(index));

        // A burst arrives, and from here on the host only polls. The Poll that queues the burst cannot know yet; the next
        // one finds the queue as it was left, a whole pass long, and the channel is one nobody reads.
        SendToALateReceiver(h, h.Client, Ordered, 0, 60);
        server.Poll();
        Assert.Equal(CreditState.Drained, credit.State(index));
        server.Poll();
        Assert.Equal(CreditState.Unread, credit.State(index));

        // It keeps what it has and accepts nothing more, and the other channels are not touched.
        Send(h, h.Client, Ordered, 60, 200);
        Assert.Equal(60, Waiting(server, Ordered));
        Assert.Equal(60, Channel(server, Ordered).Received);
        Send(h, h.Client, Datagrams, 0, 100);
        Assert.Equal(Enumerable.Range(0, 100), datagrams);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
        Assert.False(server.HasPendingWork, "a channel that waits for the application is reported as work a Poll could do");

        // The application comes back: everything is there, in order, and the channel is read again.
        Assert.Equal(Enumerable.Range(0, 260), DrainUntil(h, server, Ordered, 260));
        Assert.Equal(CreditState.Drained, credit.State(index));
    }

    [Fact]
    public void A_Consumer_That_Never_Empties_The_Channel_Stays_Within_The_Share()
    {
        // A consumer that takes a few messages per frame and never catches up is not reading the channel in the sense
        // that matters here: what waits for it is its backlog, and that is held to the share.
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Ordered);
        ReceivedMessage[] two = new ReceivedMessage[2];
        List<int> got = [];
        int sent = 0;
        while (sent < 400 && h.Client.SendCopy(new SendHeader(Ordered), Payload(sent)).Status == SendStatus.Admitted)
        {
            sent++;
        }

        Assert.True(sent >= 300, $"only {sent} messages were admitted");
        for (int frame = 0; frame < 120; frame++)
        {
            h.Run(2_000);
            int taken = server.Drain(Ordered, two);
            for (int i = 0; i < taken; i++)
            {
                got.Add(IndexOf(two[i].Payload));
            }

            server.Release(two.AsSpan(0, taken));
            Assert.True(Waiting(server, Ordered) <= Limit, $"{Waiting(server, Ordered)} messages wait in frame {frame}");
        }

        Assert.Equal(CreditState.Unread, server.Core.Credit.State(index));
        Assert.Equal(Enumerable.Range(0, got.Count), got);
        Assert.True(got.Count >= 200, $"{got.Count} messages in 120 frames of two");
    }

    [Fact]
    public void Registering_A_Handler_Resumes_A_Channel_That_Was_Out_Of_Credit()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        Send(h, h.Client, Ordered, 0, 300);
        Assert.Equal(Limit, Waiting(server, Ordered));

        List<int> got = [];
        server.RegisterHandler(Ordered, Collect(got));
        Assert.True(h.RunUntil(() => got.Count == 300, 2_000_000), $"{got.Count} of 300 messages reached the handler");
        Assert.Equal(Enumerable.Range(0, 300), got);
    }

    [Fact]
    public void A_Handler_Registered_Over_A_Backlog_Keeps_The_Limit_Until_The_Backlog_Is_Dispatched()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Ordered);
        Send(h, h.Client, Ordered, 0, 40);
        Assert.Equal(Limit, Waiting(server, Ordered));

        List<int> got = [];
        server.RegisterHandler(Ordered, Collect(got));
        Assert.True(server.Core.Credit.IsLimited(index), "the limit was lifted while the channel's backlog was still queued");
        Assert.True(h.RunUntil(() => got.Count == 40), $"{got.Count} of 40 messages reached the handler");
        Assert.False(server.Core.Credit.IsLimited(index), "the limit stayed although the handler has seen the backlog");

        // With a handler the ring alone limits the channel: a burst far above the credit arrives while the host is late.
        SendToALateReceiver(h, h.Client, Ordered, 40, 60);
        Assert.Equal(60, Waiting(server, Ordered));
        Assert.True(h.RunUntil(() => got.Count == 100));
        Assert.Equal(Enumerable.Range(0, 100), got);
    }

    [Fact]
    public void A_Handler_That_Comes_And_Goes_Between_Polls_Cannot_Grow_The_Backlog()
    {
        // The handler is registered whenever messages arrive and gone whenever the host polls, and nobody drains. Were the
        // limit lifted the moment a handler is registered, every round would move a ring's worth of messages into the
        // channel's queue for good.
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        MessageHandler handler = Collect(got);
        server.RegisterHandler(Ordered, handler);
        h.Run(2_000);
        int sent = 0;
        for (int round = 0; round < 20; round++)
        {
            SendToALateReceiver(h, h.Client, Ordered, sent, 30);
            sent += 30;
            Assert.True(server.UnregisterHandler(Ordered));
            server.Poll();
            server.Flush();
            server.RegisterHandler(Ordered, handler);
        }

        // The first round's burst arrived under the handler and was queued when it left; after that the channel was never
        // without a backlog, so its limit stayed and nothing more was accepted.
        Assert.Empty(got);
        Assert.Equal(30, Waiting(server, Ordered));
        Assert.Equal(30, Channel(server, Ordered).Received);

        // A Poll that finds the handler in place dispatches the backlog, lifts the limit, and the rest follows in order.
        Assert.True(h.RunUntil(() => got.Count == sent, 5_000_000), $"{got.Count} of {sent} messages reached the handler");
        Assert.Equal(Enumerable.Range(0, sent), got);
    }

    [Fact]
    public void A_Handler_Removed_Over_A_Backlog_Does_Not_Stop_The_Other_Channels()
    {
        // The channel had a handler, so nothing limited it but the ring, and a late host left sixty of its messages there.
        // The handler is removed before the Poll. The drain queues are as full as two undrained channels can make them
        // (the group channel at its limit, an unreliable one at its own), so the sixty do not fit the nodes that are
        // left — and they still must not stop the ring for everybody else.
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> handled = [];
        List<int> datagrams = [];
        server.RegisterHandler(Ordered, Collect(handled));
        server.RegisterHandler(Datagrams, Collect(datagrams));
        Send(h, h.Client, Ordered, 0, 1);
        Send(h, h.Client, Groups, 0, 200);
        Send(h, h.Client, 2, 0, 200);
        Assert.Single(handled);

        SendToALateReceiver(h, h.Client, Ordered, 1, 60);
        Assert.Equal(60, Waiting(server, Ordered));
        Assert.True(server.UnregisterHandler(Ordered));

        Send(h, h.Client, Datagrams, 0, 100);
        Assert.True(h.RunUntil(() => datagrams.Count == 100), $"{datagrams.Count} of 100 datagrams arrived: the ring stopped behind the removed handler's backlog");
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
        Assert.False(server.HasPendingWork, "the backlog of a removed handler is reported as work a Poll could do");

        // The backlog is all there, in order, and once it is drained the channel runs within its limit again.
        Send(h, h.Client, Ordered, 61, 100);
        Assert.Equal(60, Waiting(server, Ordered));
        Assert.Equal(Enumerable.Range(1, 160), DrainUntil(h, server, Ordered, 160));
        Assert.Equal(Enumerable.Range(0, 200), DrainUntil(h, server, Groups, 200).Order());
    }

    [Fact]
    public void A_Slow_Drain_Consumer_Gets_Every_Message_In_Order()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> ordered = [];
        List<int> groups = [];
        ReceivedMessage[] buffer = new ReceivedMessage[5];
        int sentOrdered = 0;
        int sentGroups = 0;
        Assert.True(h.RunUntil(() =>
        {
            // Twelve messages per channel and pump, as long as the channel admits them: a sender whose receiver is behind
            // is refused on that channel (QueueFull) once its own queue limit is reached, and tries again later.
            for (int i = 0; i < 12 && sentOrdered < 2_000 && h.Client.SendCopy(new SendHeader(Ordered), Payload(sentOrdered)).IsAdmitted; i++)
            {
                sentOrdered++;
            }

            for (int i = 0; i < 12 && sentGroups < 2_000 && h.Client.SendCopy(new SendHeader(Groups), Payload(sentGroups)).IsAdmitted; i++)
            {
                sentGroups++;
            }

            // Five messages of each channel per pump: far less than arrives.
            int taken = server.Drain(Ordered, buffer);
            for (int i = 0; i < taken; i++)
            {
                ordered.Add(IndexOf(buffer[i].Payload));
            }

            server.Release(buffer.AsSpan(0, taken));
            taken = server.Drain(Groups, buffer);
            for (int i = 0; i < taken; i++)
            {
                groups.Add(IndexOf(buffer[i].Payload));
            }

            server.Release(buffer.AsSpan(0, taken));
            Assert.True(Waiting(server, Ordered) <= Limit);
            Assert.True(Waiting(server, Groups) <= Limit);
            return ordered.Count == 2_000 && groups.Count == 2_000;
        }, 60_000_000), $"{ordered.Count} of {sentOrdered} ordered and {groups.Count} of {sentGroups} group messages came out");
        Assert.Equal(Enumerable.Range(0, 2_000), ordered);
        Assert.Equal(Enumerable.Range(0, 2_000), groups.Order());
        Assert.Equal(0, DatagramKit.Statistics(server).StreamsReset);
    }

    // ------------------------------------------------------------------ bytes

    [Fact]
    public void An_Undrained_Channel_With_Large_Messages_Cannot_Pin_The_Receive_Budget()
    {
        // The default ring: 256 messages may wait per reliable channel, which would be a megabyte of 4 KiB blocks against a
        // receive budget of 256 KiB. The byte limit (a quarter of the budget, shared by the two reliable channels: 32 KiB)
        // is what holds the channel back here, so the other channel still finds the budget it needs.
        using SessionHarness h = new(table: Table, client: o => { GroupKit.Prompt(o); OrderedKit.Roomy(o); }, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        List<int> groups = [];
        server.RegisterHandler(Groups, Collect(groups));
        int index = server.Core.ChannelIndexOf(Ordered);
        int byteLimit = server.Core.Credit.ByteLimit;
        Assert.Equal(32 * 1024, byteLimit);

        Send(h, h.Client, Ordered, 0, 200, size: 4_000);
        h.Run(100_000);
        Assert.InRange(server.Core.Credit.WaitingBytes(index), byteLimit, byteLimit + 4_096);
        Assert.InRange(Waiting(server, Ordered), 8, 9);

        Send(h, h.Client, Groups, 0, 50, size: 4_000);
        Assert.True(h.RunUntil(() => groups.Count == 50), $"{groups.Count} of 50 group messages arrived next to an undrained channel of large messages");
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.Equal(0, statistics.OutOfReceiveBuffers);
        Assert.True(statistics.ReceiveBytesOutstanding <= byteLimit + 4_096, $"{statistics.ReceiveBytesOutstanding} receive bytes are held");

        Assert.Equal(Enumerable.Range(0, 200), DrainUntil(h, server, Ordered, 200));
    }

    [Fact]
    public void A_Message_Larger_Than_The_Byte_Share_Waits_Until_The_Channel_Is_Read()
    {
        using SessionHarness h = new(table: Table, client: o => { GroupKit.Prompt(o); OrderedKit.Roomy(o); }, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        Assert.True(60_000 > server.Core.Credit.ByteLimit);

        Send(h, h.Client, Ordered, 0, 3, size: 60_000);
        h.Run(100_000);

        // The share of a channel nobody reads is strict: a message whose block alone is above it is not started, even on
        // an empty channel, so an unread channel pins nothing beyond its share. It costs the host nothing while it waits.
        Assert.Equal(0, Waiting(server, Ordered));
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
        Assert.Equal(1, Channel(server, Ordered).BacklogHolds);
        Assert.False(server.HasPendingWork);

        // The first Drain finds nothing, and shows that the application reads the channel: the messages arrive.
        Assert.Equal(Enumerable.Range(0, 3), DrainUntil(h, server, Ordered, 3));
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Channel_The_Application_Drains_Takes_One_Message_Of_Any_Size_And_Half_The_Budget()
    {
        using SessionHarness h = new(table: Table, client: o => { GroupKit.Prompt(o); OrderedKit.Roomy(o); }, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        ReceiveCredit credit = server.Core.Credit;

        // The channels no handler reads share half the 256 KiB budget; the other reliable channel has nothing waiting, so
        // this one may take all of it.
        Assert.Equal(128 * 1024, credit.DrainedByteLimit);

        // The consumer's first frame: nothing to take, and from then on the channel is one the application reads.
        Assert.Empty(DrainAll(server, Ordered));
        Assert.Equal(CreditState.Drained, credit.State(server.Core.ChannelIndexOf(Ordered)));

        // Messages of 60 000 bytes take a 64 KiB block each: two of them are half the budget, and the third waits. Only the
        // sender and the network run meanwhile (the receiving host is late), so the channel stays one that is drained.
        SendSlowly(h, Ordered, 0, 5, 60_000);
        Assert.Equal(2, Waiting(server, Ordered));
        Assert.Equal(1, Channel(server, Ordered).BacklogHolds);
        Assert.Equal(Enumerable.Range(0, 5), DrainUntil(h, server, Ordered, 5));
    }

    /// <summary>
    /// The half is shared by the reliable channels no handler reads, not divided among the table's channels: a second
    /// drained channel takes what the first leaves of it (one message on an empty channel always), and a channel with a
    /// handler takes nothing of it (review RC-2 of the recheck round; SC-1 of the third).
    /// </summary>
    [Fact]
    public void The_Channels_No_Handler_Reads_Share_Half_The_Budget()
    {
        using SessionHarness h = new(table: Table, client: o => { GroupKit.Prompt(o); OrderedKit.Roomy(o); }, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        Assert.Empty(DrainAll(server, Ordered));
        Assert.Empty(DrainAll(server, Groups));

        // The first drained channel takes the half: two 64 KiB blocks.
        SendSlowly(h, Ordered, 0, 3, 60_000);
        Assert.Equal(2, Waiting(server, Ordered));

        // The second one: its first message (an empty channel takes one), and nothing more while the half is full.
        SendSlowly(h, Groups, 0, 3, 60_000);
        Assert.Equal(1, Waiting(server, Groups));
        Assert.True(Channel(server, Groups).BacklogHolds >= 1);

        // Both are read in the end, in full.
        Assert.Equal(Enumerable.Range(0, 3), DrainUntil(h, server, Ordered, 3));
        Assert.Equal(Enumerable.Range(0, 3), DrainUntil(h, server, Groups, 3).Order());
    }

    /// <summary>Sends <paramref name="count"/> messages while only the sender and the network run (the receiving host is late).</summary>
    private static void SendSlowly(SessionHarness h, ushort channel, int first, int count, int size)
    {
        for (int i = first; i < first + count; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(channel), Payload(i, size)).Status);
            for (int step = 0; step < 50; step++)
            {
                h.Client.Poll();
                h.Client.Flush();
                h.Network.AdvanceTo(h.Network.NowMicros + 1_000);
            }
        }
    }

    // ------------------------------------------------------------------ what is not counted, and what is given back

    [Fact]
    public void Responses_Are_Not_Held_Back_On_A_Channel_Without_A_Handler()
    {
        // A client that only sends requests has no handler on the channel. A response never waits for the application —
        // Poll hands it to its request — so it takes no credit: with a limit of one message, fifty responses arrive
        // without the stream being held back once.
        using SessionHarness h = new(table: OrderedTables.Main, server: DatagramKit.Quiet, client: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 8;
        });
        QuiclyPeer client = h.Client;
        int index = client.Core.ChannelIndexOf(10);
        Assert.Equal(1, client.Core.Credit.CountLimit);
        h.Server!.RegisterHandler(10, (QuiclyPeer self, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
            Assert.Equal(SendStatus.Admitted, self.Respond(in header, payload).Status));

        ValueTask<ReceiveLease>[] pending = new ValueTask<ReceiveLease>[50];
        for (int i = 0; i < pending.Length; i++)
        {
            pending[i] = client.SendRequestAsync(new SendHeader(10), Payload(i), TimeSpan.FromSeconds(30));
        }

        int completed = 0;
        Assert.True(h.RunUntil(() =>
        {
            completed = 0;
            foreach (ValueTask<ReceiveLease> request in pending)
            {
                completed += request.IsCompleted ? 1 : 0;
            }

            return completed == pending.Length;
        }), $"{completed} of {pending.Length} responses arrived");
        for (int i = 0; i < pending.Length; i++)
        {
            ReceiveLease response = pending[i].GetAwaiter().GetResult();
            Assert.Equal(i, IndexOf(response.Payload));
            client.Release(in response);
        }

        Assert.Equal(0, client.Core.Credit.Pends(index));
        Assert.Equal(0, Waiting(client, 10));
    }

    [Fact]
    public void A_Group_That_Ends_In_The_Middle_Of_A_Message_Gives_Its_Credit_Back()
    {
        using ServerHarness h = new(table: Table, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = Ring;
        });
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;

        // Forty groups, each abandoned by its sender half-way through a message: far more than the channel's limit.
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(Groups, (ulong)i, 100, [1, 2, 3]), out TransportStreamId id));
            Assert.True(h.RunUntil(() => Waiting(server, Groups) == 1), $"group {i}: {Waiting(server, Groups)} messages are counted as waiting");
            h.Raw.Transport.AbortStream(id, 0x77, StreamAbortDirection.Send);
            Assert.True(h.RunUntil(() => Waiting(server, Groups) == 0 && GroupKit.OpenPeerGroups(server, Groups) == 0),
                $"group {i}: its half-received message still counts against the channel");
        }

        // The whole credit is there for complete messages.
        byte[][] messages = new byte[Limit][];
        for (int i = 0; i < Limit; i++)
        {
            messages[i] = Payload(i);
        }

        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, 1_000, messages), out _, fin: true));
        Assert.True(h.RunUntil(() => Channel(server, Groups).Received == Limit));
        Assert.Equal(Enumerable.Range(0, Limit), DrainAll(server, Groups));
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Stream_That_Waits_For_Credit_Goes_On_When_Another_Stream_Gives_Its_Message_Up()
    {
        // The credit a half-received message holds comes back on the transport thread when its stream ends, without the
        // application taking anything — and here there is nothing it could take. The stream that was held back for that
        // credit must go on all the same.
        using ServerHarness h = new(table: Table, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = Ring;
        });
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;
        int share = server.Core.Credit.ByteLimit;
        Assert.Equal(2 * 16_384, share);

        // Two messages of 10 000 bytes, started and not finished: their blocks (16 KiB each) are the channel's byte share.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(Groups, 1, 10_000, [1, 2, 3]), out TransportStreamId large));
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(Groups, 3, 10_000, [1, 2, 3]), out TransportStreamId other));
        Assert.True(h.RunUntil(() => Waiting(server, Groups) == 2));
        Assert.Equal(share, server.Core.Credit.WaitingBytes(server.Core.ChannelIndexOf(Groups)));

        // A third group with one small message waits for credit: nothing is left of the share for its block.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, 2, Payload(42)), out _, fin: true));
        Assert.True(h.RunUntil(() => server.Core.Credit.ParkedStreams == 1), "the third group was not held back");
        h.Run(20_000);
        Assert.Equal(0, Channel(server, Groups).Received);
        Assert.Equal(1, server.Core.Credit.ParkedStreams);

        h.Raw.Transport.AbortStream(large, 0x77, StreamAbortDirection.Send);
        Assert.True(h.RunUntil(() => Channel(server, Groups).Received == 1), "the group that waited for credit never went on");
        Assert.Equal([42], DrainAll(server, Groups));
        h.Raw.Transport.AbortStream(other, 0x77, StreamAbortDirection.Send);
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(server, Groups) == 0));
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }

    [Fact]
    public void Groups_That_End_While_They_Wait_For_Credit_Do_Not_Keep_The_Others_Waiting()
    {
        // Streams waiting for a channel's credit go on oldest first, one per message of credit. A stream its sender gave
        // up while it waited takes nothing, so its turn must not be kept: here every turn of the last Drain would go to
        // such a stream, and with the channel empty after it nothing would ever resume the group that is still there.
        using ServerHarness h = new(table: Table, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = 16;
        });
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;

        // A small ring, so that more streams than the channel has credit fit under the session's stream limit.
        int limit = server.Core.Credit.CountLimit;
        Assert.True(limit + 2 <= server.Core.PeerUnidirectionalStreamLimit, $"a credit of {limit} against {server.Core.PeerUnidirectionalStreamLimit} streams");
        byte[][] messages = new byte[limit][];
        for (int i = 0; i < limit; i++)
        {
            messages[i] = Payload(i);
        }

        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, 1, messages), out _, fin: true));
        Assert.True(h.RunUntil(() => Waiting(server, Groups) == limit));

        TransportStreamId[] abandoned = new TransportStreamId[limit + 1];
        for (int i = 0; i < abandoned.Length; i++)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, (ulong)(100 + i), Payload(500 + i)), out abandoned[i]));
        }

        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, 2, Payload(42)), out _, fin: true));
        Assert.True(h.RunUntil(() => server.Core.Credit.ParkedStreams == abandoned.Length + 1), $"{server.Core.Credit.ParkedStreams} groups wait for credit");

        foreach (TransportStreamId id in abandoned)
        {
            h.Raw.Transport.AbortStream(id, 0x77, StreamAbortDirection.Send);
        }

        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(server, Groups) == 1), $"{GroupKit.OpenPeerGroups(server, Groups)} groups are open");
        ReceivedMessage[] buffer = new ReceivedMessage[limit];
        Assert.Equal(limit, server.Drain(Groups, buffer));
        server.Release(buffer);

        Assert.True(h.RunUntil(() => Channel(server, Groups).Received == limit + 1), "the group that waited behind the abandoned ones never went on");
        Assert.Equal([42], DrainAll(server, Groups));
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(server, Groups) == 0));
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }

    [Fact]
    public void An_Ordered_Stream_That_Ends_In_The_Middle_Of_A_Message_Gives_Its_Credit_Back()
    {
        using ServerHarness h = new(table: Table, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = Ring;
        });
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;
        ChannelDefinition ordered = Table[Ordered]!;

        // The preamble, one whole message and the start of a second one.
        byte[] frames = new byte[64];
        int length = StreamFraming.WritePreamble(frames, ordered.Id);
        StreamMessageHeader whole = default;
        whole.Length = 4;
        length += StreamFraming.WriteFrameHeader(frames.AsSpan(length), ordered, in whole);
        Payload(7).CopyTo(frames.AsSpan(length));
        length += 4;
        StreamMessageHeader partial = default;
        partial.Length = 100;
        length += StreamFraming.WriteFrameHeader(frames.AsSpan(length), ordered, in partial);
        frames[length++] = 1;
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(frames.AsSpan(0, length), out TransportStreamId id));
        Assert.True(h.RunUntil(() => Waiting(server, Ordered) == 2));

        h.Raw.Transport.AbortStream(id, 0x77, StreamAbortDirection.Send);
        Assert.True(h.RunUntil(() => Waiting(server, Ordered) == 1), "the half-received message still counts against the channel");
        Assert.Equal([7], DrainAll(server, Ordered));
        Assert.Equal(0, Waiting(server, Ordered));
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }

    [Fact]
    public void Every_Stream_A_Transport_Admits_By_Itself_Can_Wait_For_Credit()
    {
        // The link grants 1 024 unidirectional streams by itself, as an MsQuic client does, so the server can have far more
        // group streams open at the client than the session's own limit of nine. Each of them is held back for the credit
        // of the undrained channel: none may be lost from the lists that remember them, and none may keep the host busy.
        CountingWorkSignal signal = new();
        using SessionHarness h = new(link: new LinkOptions { PeerUnidiStreams = 1024 }, table: Table, server: GroupKit.Prompt, client: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = Ring;
            o.WorkSignal = signal;
        });
        QuiclyPeer client = h.Client;
        Assert.Equal(9, client.Core.PeerUnidirectionalStreamLimit);
        Assert.Equal(1024, client.Core.PeerStreamCapacity);

        // A hundred groups of sixteen: the first fills the credit and is done, the other ninety-nine streams wait.
        Send(h, h.Server!, Groups, 0, 1_600);
        h.Run(100_000);
        Assert.Equal(Limit, Waiting(client, Groups));
        Assert.Equal(99, GroupKit.OpenPeerGroups(client, Groups));
        Assert.Equal(99, client.Core.Credit.ParkedStreams);
        Assert.False(client.HasPendingWork);
        int signals = signal.Calls;
        h.Run(100_000);
        Assert.Equal(signals, signal.Calls);

        Assert.Equal(Enumerable.Range(0, 1_600), DrainUntil(h, client, Groups, 1_600).Order());

        // A message the application takes lets one waiting stream go, not all of them: the streams were held back once
        // when they arrived, and after that at most once per message that was taken.
        long pends = client.Core.Credit.Pends(client.Core.ChannelIndexOf(Groups));
        Assert.True(pends <= 99 + 1_600, $"the streams were held back {pends} times");
        PeerStatistics statistics = DatagramKit.Statistics(client);
        Assert.Equal(0, statistics.StreamsReset);
        Assert.Equal(0, statistics.CallbackFaults);
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(client, Groups) == 0));
    }

    [Fact]
    public void A_Reconnect_Starts_With_Fresh_Credit()
    {
        // The client is the receiver here (only a client reconnects): its ordered channel is out of credit and its stream
        // is held back when the connection is lost.
        using SessionHarness h = new(connect: false, table: Table, server: GroupKit.Prompt, client: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = Ring;
        });
        h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
            AdmissionResult.Accept(ResumeToken, 4242, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer client = h.Client;
        QuiclyPeer oldServer = h.Server!;
        Send(h, oldServer, Ordered, 0, 100);
        Assert.Equal(Limit, Waiting(client, Ordered));
        Assert.Equal(1, client.Core.Credit.ParkedStreams);

        oldServer.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);
        Assert.True(h.RunUntil(() => client.State == PeerState.Closed));
        client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.Equal(0, Waiting(client, Ordered));
        Assert.Equal(0, client.Core.Credit.ParkedStreams);
        Assert.True(h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();

        // The lost connection's backlog is gone with it; the resumed one has the whole credit and delivers in order.
        Send(h, h.Server!, Ordered, 1_000, 40);
        Assert.Equal(Limit, Waiting(client, Ordered));
        Assert.Equal(Enumerable.Range(1_000, 40), DrainUntil(h, client, Ordered, 40));
    }

    private static readonly byte[] ResumeToken = [9, 8, 7, 6];

    // ------------------------------------------------------------------ the credit itself

    [Fact]
    public void ReceiveCredit_Limits_The_Count_Across_The_Counter_Wrap()
    {
        using ReceiveCredit credit = new(channels: 2, streams: 4, countLimit: 7, byteLimit: 1_000_000);
        credit.Enable(1);
        credit.SetCountersForTest(1, 0xFFFF_FFFC, 0xFFFF_FF00);

        // Seven messages are taken across the 32-bit wrap of both counters, the eighth is refused.
        for (int i = 0; i < 7; i++)
        {
            Assert.True(credit.TryTake(1, 0, 0) != CreditTake.Blocked, $"message {i} was refused");
            credit.NoteTaken(1, 64, shared: true);
        }

        Assert.False(credit.TryTake(1, 0, 0) != CreditTake.Blocked);
        Assert.Equal(7, credit.Waiting(1));
        Assert.Equal(7 * 64, credit.WaitingBytes(1));

        // One comes back: exactly one more is accepted.
        credit.NoteReturned(1, 64, shared: true);
        Assert.True(credit.TryTake(1, 0, 0) != CreditTake.Blocked);
        credit.NoteTaken(1, 64, shared: true);
        Assert.False(credit.TryTake(1, 0, 0) != CreditTake.Blocked);

        // A channel that was never enabled is not limited and not counted.
        Assert.False(credit.IsEnabled(0));
        Assert.True(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
    }

    [Fact]
    public void ReceiveCredit_Limits_The_Bytes_And_Always_Accepts_One_Message()
    {
        using ReceiveCredit credit = new(channels: 1, streams: 4, countLimit: 100, byteLimit: 1_000);
        credit.Enable(0);
        credit.SetCountersForTest(0, 0xFFFF_FFFF, 0xFFFF_FE00);

        // Empty: a message above the byte limit is accepted, and the next one waits for it.
        Assert.True(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
        credit.NoteTaken(0, 4_096, shared: true);
        Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
        credit.NoteReturned(0, 4_096, shared: true);

        // Below the limit messages are accepted until the bytes that wait reach it (the check precedes the message).
        Assert.True(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
        credit.NoteTaken(0, 600, shared: true);
        Assert.True(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
        credit.NoteTaken(0, 600, shared: true);
        Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
        Assert.Equal(1_200, credit.WaitingBytes(0));

        // A message given up half-way (its stream ended) is taken back by the thread that took it.
        credit.Untake(0, 600, shared: true);
        Assert.True(credit.TryTake(0, 0, 0) != CreditTake.Blocked);

        // An empty message counts as a message, not as bytes.
        credit.NoteTaken(0, 0, shared: true);
        Assert.Equal(2, credit.Waiting(0));
        Assert.Equal(600, credit.WaitingBytes(0));
    }

    [Fact]
    public void ReceiveCredit_Resumes_Only_The_Streams_Of_A_Channel_That_Got_Credit_Back()
    {
        using ReceiveCredit credit = new(channels: 3, streams: 8, countLimit: 1, byteLimit: 1_000);
        ResumeRecorder transport = new();
        credit.Enable(0);
        credit.Enable(1);
        TransportStreamId a = new(1, 1);
        TransportStreamId b = new(2, 1);
        TransportStreamId c = new(3, 1);
        credit.NoteTaken(0, 10, shared: true);
        credit.NoteTaken(1, 10, shared: true);
        Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
        Assert.True(credit.NotePended(a, 0));
        Assert.True(credit.NotePended(b, 1));
        Assert.True(credit.NotePended(c, 1));
        Assert.True(credit.HasWork, "streams that were just held back are collected by the next Poll");
        Assert.Equal(1, credit.Pends(0));
        Assert.Equal(2, credit.Pends(1));

        // Nothing came back: the streams are collected and keep waiting, and from then on they are not work.
        credit.Resume(transport);
        Assert.Empty(transport.Resumed);
        Assert.Equal(3, credit.ParkedStreams);
        Assert.False(credit.HasWork);

        // Channel 1 gets one message of credit back: the older of its two streams is resumed, the other one and channel
        // 0's stay. Nothing is left to do until the application takes another message.
        credit.NoteReturned(1, 10, shared: true);
        credit.Resume(transport);
        Assert.Equal([b], transport.Resumed);
        Assert.Equal(2, credit.ParkedStreams);
        Assert.False(credit.HasWork);
        credit.Resume(transport);
        Assert.Equal([b], transport.Resumed);

        // The resumed stream takes the credit; when that message is taken too, it is the other stream's turn.
        Assert.True(credit.TryTake(1, 0, 0) != CreditTake.Blocked);
        credit.NoteTaken(1, 10, shared: true);
        credit.NoteReturned(1, 10, shared: true);
        credit.Resume(transport);
        Assert.Equal([b, c], transport.Resumed);
        Assert.Equal(1, credit.ParkedStreams);

        // Lifting a limit (a handler was registered) resumes the channel's streams too.
        transport.Resumed.Clear();
        credit.SetLimited(0, limited: false);
        Assert.True(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
        credit.Resume(transport);
        Assert.Equal([a], transport.Resumed);
        Assert.Equal(0, credit.ParkedStreams);

        // A stream held back while its channel's credit came back at the same moment: the transport thread sees that the
        // channel is no longer out of credit and asks for another look, which resumes it although nothing is marked.
        transport.Resumed.Clear();
        credit.SetLimited(0, limited: true);
        credit.NoteReturned(0, 10, shared: true);
        credit.Resume(transport);
        Assert.True(credit.NotePended(a, 0));
        Assert.True(credit.HasWork);
        credit.Resume(transport);
        Assert.Equal([a], transport.Resumed);

        // A reconnect forgets counts and streams, and keeps the limits.
        credit.NoteTaken(1, 10, shared: true);
        Assert.True(credit.NotePended(b, 1));
        credit.Reset();
        Assert.Equal(0, credit.Waiting(1));
        Assert.False(credit.HasWork);
        Assert.True(credit.IsLimited(1));
        transport.Resumed.Clear();
        credit.Resume(transport);
        Assert.Empty(transport.Resumed);
    }

    [Fact]
    public void ReceiveCredit_Resumes_No_More_Streams_Than_The_Channel_Has_Credit_For()
    {
        // Many streams can wait for one channel (every open group of a ReliableUnordered channel). Each message the
        // application takes lets one of them go, oldest first — not all of them, which would be held back again at once.
        using ReceiveCredit credit = new(channels: 1, streams: 64, countLimit: 8, byteLimit: 1_000_000);
        ResumeRecorder transport = new();
        credit.Enable(0);
        for (int i = 0; i < 8; i++)
        {
            credit.NoteTaken(0, 10, shared: true);
        }

        TransportStreamId[] streams = new TransportStreamId[40];
        for (int i = 0; i < streams.Length; i++)
        {
            streams[i] = new TransportStreamId(i + 1, 1);
            Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
            Assert.True(credit.NotePended(streams[i], 0));
        }

        credit.Resume(transport);
        Assert.Empty(transport.Resumed);

        // Three messages are taken: three streams go on, and each takes one message.
        for (int i = 0; i < 3; i++)
        {
            credit.NoteReturned(0, 10, shared: true);
        }

        credit.Resume(transport);
        Assert.Equal(streams[..3], transport.Resumed);
        Assert.Equal(37, credit.ParkedStreams);
        for (int i = 0; i < 3; i++)
        {
            Assert.True(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
            credit.NoteTaken(0, 10, shared: true);
        }

        // The channel is full again; further looks resume nothing.
        credit.Resume(transport);
        Assert.Equal(3, transport.Resumed.Count);
        Assert.False(credit.HasWork);

        // Everything is taken: the whole credit is there, and it lets eight streams go, not thirty-seven.
        for (int i = 0; i < 8; i++)
        {
            credit.NoteReturned(0, 10, shared: true);
        }

        credit.Resume(transport);
        Assert.Equal(streams[..11], transport.Resumed);
        Assert.Equal(29, credit.ParkedStreams);
    }

    [Fact]
    public void ReceiveCredit_Does_Not_Keep_A_Turn_For_A_Stream_That_Ended()
    {
        using ReceiveCredit credit = new(channels: 1, streams: 8, countLimit: 1, byteLimit: 1_000);
        ResumeRecorder transport = new();
        credit.Enable(0);
        TransportStreamId gone = new(1, 1);
        TransportStreamId live = new(2, 1);
        credit.NoteTaken(0, 10, shared: true);
        Assert.True(credit.NotePended(gone, 0));
        Assert.True(credit.NotePended(live, 0));
        credit.Resume(transport);
        Assert.Equal(2, credit.ParkedStreams);
        Assert.False(credit.HasWork);

        // The older stream ends. It would get the one message of credit and take nothing, and with the channel empty
        // nothing would ever look at the other stream again. So everything is resumed once: the stream that is still
        // there is held back again, and is then the oldest.
        credit.NoteGone();
        Assert.True(credit.HasWork);
        credit.Resume(transport);
        Assert.Equal([gone, live], transport.Resumed);
        Assert.Equal(0, credit.ParkedStreams);
        Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
        Assert.True(credit.NotePended(live, 0));
        credit.Resume(transport);
        Assert.Equal(1, credit.ParkedStreams);

        transport.Resumed.Clear();
        credit.NoteReturned(0, 10, shared: true);
        credit.Resume(transport);
        Assert.Equal([live], transport.Resumed);
    }

    [Fact]
    public void ReceiveCredit_Forgets_Streams_That_Ended_While_They_Waited_When_Its_List_Is_Full()
    {
        // The lists hold every stream the peer may have open, but a stream that ended while it waited stays listed. When
        // the list is full everything on it is resumed: the dead ones are forgotten there (the transport ignores a stale
        // id) and the live ones come straight back.
        using ReceiveCredit credit = new(channels: 1, streams: 4, countLimit: 1, byteLimit: 1_000);
        ResumeRecorder transport = new();
        credit.Enable(0);
        credit.NoteTaken(0, 10, shared: true);
        for (int round = 0; round < 5; round++)
        {
            for (int i = 0; i < 4; i++)
            {
                Assert.True(credit.NotePended(new TransportStreamId((round * 4) + i + 1, 1), 0));
            }

            credit.Resume(transport);
        }

        Assert.Equal(16, transport.Resumed.Count);
        Assert.Equal(4, credit.ParkedStreams);
    }

    [Fact]
    public void ReceiveCredit_Lists_Streams_That_Ended_While_They_Waited_Until_Its_List_Has_Grown_Eight_Times()
    {
        // Between two looks of the game thread a peer can have streams held back, reset them and open new ones: every
        // one leaves an entry. The list grows for them, up to ListGrowth times the streams that can be alive; past that
        // NotePended says so (the session then closes the connection), and a look of the game thread makes room again.
        using ReceiveCredit credit = new(channels: 1, streams: 4, countLimit: 1, byteLimit: 1_000);
        ResumeRecorder transport = new();
        credit.Enable(0);
        credit.NoteTaken(0, 10, shared: true);
        int listed = 0;
        for (uint generation = 1; generation <= 100; generation++)
        {
            if (!credit.NotePended(new TransportStreamId((int)(generation % 4) + 1, generation), 0))
            {
                break;
            }

            credit.NoteGone();
            listed++;
        }

        // Rings of 4, 8, 16 and 32 entries: each one that was replaced keeps what it held.
        Assert.Equal(4 * ((2 * ReceiveCredit.ListGrowth) - 1), listed);
        credit.Resume(transport);
        Assert.Equal(listed, transport.Resumed.Count);
        Assert.Equal(0, credit.ParkedStreams);
        TransportStreamId live = new(1, 1_000);
        Assert.True(credit.NotePended(live, 0));
        credit.NoteReturned(0, 10, shared: true);
        credit.Resume(transport);
        Assert.Equal(live, transport.Resumed[^1]);
    }

    [Fact]
    public void A_Peer_That_Keeps_Resetting_The_Groups_This_End_Holds_Back_Is_Disconnected()
    {
        // No sender of this library resets a group it started. A peer that does it over and over while the receiving game
        // thread is late fills the list of held-back streams with entries of streams that are gone; when even the grown
        // list is full, a stream that is alive could not be remembered and would never be resumed, so the connection is
        // closed (LimitExceeded) instead of leaving a reliable stream stalled for good.
        using ServerHarness h = new(table: Table, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = 16;
        });
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Groups);
        int limit = server.Core.Credit.CountLimit;
        byte[][] messages = new byte[limit][];
        for (int i = 0; i < limit; i++)
        {
            messages[i] = Payload(i);
        }

        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, 1, messages), out _, fin: true));
        Assert.True(h.RunUntil(() => Waiting(server, Groups) == limit && GroupKit.OpenPeerGroups(server, Groups) == 0));

        // From here on only the network runs: the server's game thread is late.
        void AdvanceNetworkOnly(long micros)
        {
            long end = h.Network.NowMicros + micros;
            while (h.Network.NowMicros < end)
            {
                h.Network.AdvanceTo(Math.Min(end, h.Network.NowMicros + 1_000));
            }
        }

        int perRound = server.Core.PeerUnidirectionalStreamLimit - 1;
        int listLimit = SpscRing<int>.RoundUpCapacity(server.Core.PeerStreamCapacity + 2) * ReceiveCredit.ListGrowth;
        long pends = server.Core.Credit.Pends(index);
        ulong group = 100;
        long before = -1;
        for (int round = 0; round < 200 && server.Core.Credit.Pends(index) != before; round++)
        {
            // Until a round is no longer counted: the transport thread asked for the close and ignores what follows.
            before = server.Core.Credit.Pends(index);
            TransportStreamId[] ids = new TransportStreamId[perRound];
            int opened = 0;
            while (opened < ids.Length && h.Raw.OpenUni(GroupKit.GroupStream(Groups, group++, Payload(500)), out ids[opened]) == TransportStatus.Success)
            {
                opened++;
            }

            AdvanceNetworkOnly(20_000);
            for (int i = 0; i < opened; i++)
            {
                h.Raw.Transport.AbortStream(ids[i], 0x77, StreamAbortDirection.Send);
            }

            AdvanceNetworkOnly(20_000);
        }

        long held = server.Core.Credit.Pends(index) - pends;
        Assert.InRange(held, listLimit, 2L * listLimit);
        Assert.True(h.RunUntil(() => server.State == PeerState.Closed, 2_000_000), $"the peer is {server.State}");
        Assert.Equal(QuiclyErrorCode.LimitExceeded, server.CloseReason.Code);
        Assert.Equal(CloseSource.Local, server.CloseReason.Source);
    }

    [Fact]
    public void ReceiveCredit_Keeps_The_Streams_That_Were_Held_Back_Before_Its_Lists_Were_Made_Larger()
    {
        // A transport that reports a larger stream grant replaces the list the transport thread writes. No in-tree
        // transport does that while streams are held back, and nothing here rests on it: what the old list holds is
        // still collected, it is still work for the host until then, and the streams go on when their channel has credit.
        using ReceiveCredit credit = new(channels: 1, streams: 2, countLimit: 1, byteLimit: 1_000);
        ResumeRecorder transport = new();
        credit.Enable(0);
        credit.NoteTaken(0, 10, shared: true);
        TransportStreamId a = new(1, 1);
        TransportStreamId b = new(2, 1);
        TransportStreamId c = new(3, 1);
        Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
        Assert.True(credit.NotePended(a, 0));
        Assert.True(credit.NotePended(b, 0));

        credit.SetStreamCapacity(64);
        Assert.True(credit.HasWork, "streams in the list that was replaced are not reported as work");
        Assert.True(credit.NotePended(c, 0));
        credit.Resume(transport);
        Assert.Empty(transport.Resumed);
        Assert.Equal(3, credit.ParkedStreams);
        Assert.False(credit.HasWork);

        // Oldest first, one per message of credit.
        credit.NoteReturned(0, 10, shared: true);
        credit.Resume(transport);
        Assert.Equal([a], transport.Resumed);
        Assert.Equal(2, credit.ParkedStreams);

        // A reconnect empties the replaced list too.
        credit.SetStreamCapacity(256);
        Assert.True(credit.NotePended(a, 0));
        credit.SetStreamCapacity(1024);
        credit.Reset();
        Assert.False(credit.HasWork);
        credit.Resume(transport);
        Assert.Equal(0, credit.ParkedStreams);
    }

    // ------------------------------------------------------------------ the queues of a channel that lost its handler

    private static uint TakeSequence(ReceiveQueues queues, int channel)
    {
        Assert.True(queues.TryTake(channel, out ReceiveEntry entry));
        return entry.Sequence;
    }

    [Fact]
    public void ReceiveQueues_Keep_A_Reliable_Channel_Without_A_Handler_To_Its_Share_Of_The_Pool()
    {
        // Eight nodes: two reserved for each of the reliable channels 1 and 2, four for the unreliable channel 0.
        using ReceiveQueues queues = new(new ReceiveQueueLayout(8, 2, 4, 1024, 4, 1024, excessNodes: 200),
            [ReceiveQueueClass.Datagram, ReceiveQueueClass.Reliable, ReceiveQueueClass.Reliable]);
        Assert.Equal(0, queues.ExcessCapacity);

        // A hundred messages for a reliable channel nobody reads (its handler was removed over them): none is refused,
        // and the pool keeps six nodes for everybody else.
        for (uint i = 0; i < 100; i++)
        {
            Assert.True(queues.TryAppend(1, new ReceiveEntry { Sequence = i }), $"message {i} was refused");
        }

        Assert.Equal(100, queues.Count(1));
        Assert.Equal(100, queues.Used);
        Assert.Equal(98, queues.ExcessUsed);
        Assert.Equal(8, queues.Capacity);
        Assert.InRange(queues.ExcessCapacity, 98, 200);
        for (uint i = 0; i < 4; i++)
        {
            Assert.True(queues.TryAppend(0, new ReceiveEntry { Sequence = 1_000 + i }));
        }

        Assert.True(queues.TryAppend(2, new ReceiveEntry { Sequence = 2_000 }));
        Assert.True(queues.TryAppend(2, new ReceiveEntry { Sequence = 2_001 }));
        Assert.Equal(98, queues.ExcessUsed);

        // The pool is full now, of other channels: the reliable channels still lose nothing and hold nothing.
        Assert.False(queues.TryAppend(0, new ReceiveEntry { Sequence = 1_004 }));
        Assert.True(queues.TryAppend(2, new ReceiveEntry { Sequence = 2_002 }));
        Assert.True(queues.TryAppend(1, new ReceiveEntry { Sequence = 100 }));
        Assert.Equal(100, queues.ExcessUsed);

        // Everything comes out in the order it went in, and the nodes beyond the pool are used again.
        for (uint i = 0; i <= 100; i++)
        {
            Assert.Equal(i, TakeSequence(queues, 1));
        }

        Assert.Equal([2_000u, 2_001u, 2_002u], new[] { TakeSequence(queues, 2), TakeSequence(queues, 2), TakeSequence(queues, 2) });
        Assert.Equal(0, queues.ExcessUsed);
        int grown = queues.ExcessCapacity;
        for (uint i = 0; i < 50; i++)
        {
            Assert.True(queues.TryAppend(1, new ReceiveEntry { Sequence = i }));
        }

        Assert.Equal(grown, queues.ExcessCapacity);
        Assert.Equal(48, queues.ExcessUsed);
    }

    [Fact]
    public void ReceiveQueues_Move_The_Backlog_Of_A_Removed_Handler_Out_Of_The_Pool()
    {
        using ReceiveQueues queues = new(new ReceiveQueueLayout(8, 2, 4, 1024, 4, 1024, excessNodes: 200),
            [ReceiveQueueClass.Datagram, ReceiveQueueClass.Reliable]);

        // A Drain of another channel met the handled channel's messages: they wait in the pool for the next Poll, and
        // once the pool is full the peer holds the next one (the Poll dispatches it). Nothing beyond the pool is used for
        // a channel that has a handler: a Poll empties its queue.
        queues.SetHandled(1, true);
        for (uint i = 0; i < 8; i++)
        {
            Assert.True(queues.TryAppend(1, new ReceiveEntry { Sequence = i }));
        }

        Assert.False(queues.TryAppend(1, new ReceiveEntry { Sequence = 8 }));
        Assert.Equal(8, queues.QueuedHandled);
        Assert.Equal(0, queues.ExcessCapacity);

        // The handler is removed: the channel keeps its two reserved nodes, and the other six are free again.
        queues.SetHandled(1, false);
        Assert.Equal(0, queues.QueuedHandled);
        Assert.Equal(8, queues.Count(1));
        Assert.Equal(6, queues.ExcessUsed);
        for (uint i = 0; i < 6; i++)
        {
            Assert.True(queues.TryAppend(0, new ReceiveEntry { Sequence = 1_000 + i }), $"node {i} of the pool was not given back");
        }

        // The held message and what follows it go behind the queue, which kept its order.
        Assert.True(queues.TryAppend(1, new ReceiveEntry { Sequence = 8 }));
        Assert.True(queues.TryAppend(1, new ReceiveEntry { Sequence = 9 }));
        for (uint i = 0; i < 10; i++)
        {
            Assert.Equal(i, TakeSequence(queues, 1));
        }

        Assert.Equal(0, queues.ExcessUsed);
        Assert.Equal(6, queues.Used);
    }

    [Fact]
    public void ReceiveQueues_Hold_A_Reliable_Message_Only_When_Every_Node_Beyond_The_Pool_Is_In_Use()
    {
        // The nodes beyond the pool are bounded by what a channel can have on the way when its handler is removed. Past
        // that bound (which a peer cannot reach) the pool is used, and then the message is the caller's to hold.
        using ReceiveQueues queues = new(new ReceiveQueueLayout(4, 1, 3, 1024, 3, 1024, excessNodes: 3),
            [ReceiveQueueClass.Datagram, ReceiveQueueClass.Reliable]);
        for (uint i = 0; i < 7; i++)
        {
            Assert.True(queues.TryAppend(1, new ReceiveEntry { Sequence = i }), $"message {i} was refused");
        }

        Assert.Equal(3, queues.ExcessUsed);
        Assert.Equal(3, queues.ExcessCapacity);
        Assert.False(queues.TryAppend(1, new ReceiveEntry { Sequence = 7 }));
        for (uint i = 0; i < 7; i++)
        {
            Assert.Equal(i, TakeSequence(queues, 1));
        }

        Assert.Equal(0, queues.Used);
    }

    [Fact]
    public void The_Nodes_Beyond_The_Pool_Cover_What_A_Removed_Handler_Can_Leave_Behind()
    {
        // Per reliable channel: the receive ring (as the ring rounds it), the one message the peer may hold, and the pool.
        ChannelDefinition[] channels = Table.All.ToArray();
        ReceiveQueueLayout layout = ReceiveQueueLayout.Compute(channels, 100, 1 << 20);
        int reliable = channels.Count(c => ReceiveQueueClass.Of(c) == ReceiveQueueClass.Reliable);
        Assert.True(reliable >= 2);
        Assert.Equal(reliable * (128 + 1 + layout.Capacity), layout.ExcessNodes);
    }

    // ------------------------------------------------------------------ allocation

    [Fact]
    public void A_Consumer_At_The_Share_Of_An_Unread_Channel_Does_Not_Allocate()
    {
        // The whole cycle, every tick, for a consumer that never catches up: the channel runs out of credit, its stream is
        // held back and collected by the Poll, the Drain takes a few messages, gives their credit back and resumes the
        // stream.
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: Table, client: GroupKit.Prompt, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = Ring;
        });
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Ordered);
        ReceivedMessage[] buffer = new ReceivedMessage[6];
        byte[] payload = new byte[64];
        long drained = 0;
        void Tick()
        {
            for (int i = 0; i < 24; i++)
            {
                client.SendCopy(new SendHeader(Ordered), payload);
            }

            client.Flush();
            network.Advance(2_000);
            client.Poll();
            server.Poll();
            int taken = server.Drain(Ordered, buffer);
            server.Release(buffer.AsSpan(0, taken));
            drained += taken;
            network.Advance(2_000);
            server.Poll();
            taken = server.Drain(Ordered, buffer);
            server.Release(buffer.AsSpan(0, taken));
            drained += taken;
        }

        for (int i = 0; i < 1_200; i++)
        {
            Tick();
        }

        long pendsBefore = server.Core.Credit.Pends(index);
        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 120; i++)
            {
                Tick();
            }
        });
        Assert.True(drained > 1_200 * 10, $"{drained} messages drained");
        Assert.Equal(CreditState.Unread, server.Core.Credit.State(index));
        Assert.True(server.Core.Credit.Pends(index) > pendsBefore, "the channel never ran out of credit in the measured windows");
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void A_Consumer_That_Drains_Every_Frame_Does_Not_Allocate()
    {
        // The consumer the credit must not cost anything: forty messages a tick, more than the channel's share of the
        // queue pool, all of them drained in the tick. No stream is held back, and the nodes beyond the pool that the
        // first bursts needed are there for the rest.
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: Table, client: GroupKit.Prompt, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = Ring;
        });
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Ordered);
        ReceivedMessage[] buffer = new ReceivedMessage[2 * Ring];
        byte[] payload = new byte[64];
        long drained = 0;
        void Tick()
        {
            for (int i = 0; i < 40; i++)
            {
                client.SendCopy(new SendHeader(Ordered), payload);
            }

            client.Flush();
            network.Advance(2_000);
            client.Poll();
            network.Advance(2_000);
            server.Poll();
            int taken = server.Drain(Ordered, buffer);
            server.Release(buffer.AsSpan(0, taken));
            drained += taken;
        }

        for (int i = 0; i < 1_200; i++)
        {
            Tick();
        }

        // Only the very first burst met a channel nobody had read yet.
        long before = drained;
        long pendsBefore = server.Core.Credit.Pends(index);
        Assert.InRange(pendsBefore, 0, 1);
        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 120; i++)
            {
                Tick();
            }
        });
        Assert.True(drained - before > 120 * 30, $"{drained - before} messages drained in the measured windows");
        Assert.Equal(CreditState.Drained, server.Core.Credit.State(index));
        Assert.Equal(pendsBefore, server.Core.Credit.Pends(index));
        Assert.Equal(PeerState.Connected, client.State);
    }

    /// <summary>Counts the work signals of a peer.</summary>
    private sealed class CountingWorkSignal : IPeerWorkSignal
    {
        public int Calls;

        public void OnWork(QuiclyPeer peer) => Calls++;
    }

    /// <summary>An <see cref="ITransport"/> that records the streams it is asked to resume and does nothing else.</summary>
    private sealed unsafe class ResumeRecorder : ITransport
    {
        public List<TransportStreamId> Resumed { get; } = [];

        public TransportCapabilities Capabilities => default;

        public TransportState State => default;

        public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed) => Resumed.Add(id);

        public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags) => throw new NotSupportedException();

        public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id) => throw new NotSupportedException();

        public TransportStatus StartStream(TransportStreamId id) => throw new NotSupportedException();

        public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags) => throw new NotSupportedException();

        public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => throw new NotSupportedException();

        public void SetStreamPriority(TransportStreamId id, ushort priority) => throw new NotSupportedException();

        public long GetQuicStreamId(TransportStreamId id) => throw new NotSupportedException();

        public void CloseStream(TransportStreamId id) => throw new NotSupportedException();

        public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional) => throw new NotSupportedException();

        public void Close(ulong errorCode, ReadOnlySpan<byte> reason) => throw new NotSupportedException();

        public void GetStatistics(out TransportStatistics statistics) => throw new NotSupportedException();

        public void Dispose()
        {
        }
    }
}
