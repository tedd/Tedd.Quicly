using System.Buffers.Binary;
using System.Reflection;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review of credit-v3 (the per-channel receive credit of the reliable stream channels), lens: accounting and
/// behaviour at the session level. Each test states a promise of PROTOCOL.md §7 "Channels nobody drains", the release
/// notes or the XML docs of Poll / Drain / RegisterHandler / UnregisterHandler / HasPendingWork. A test marked FINDING fails
/// on credit-v3 for the reason its comment gives (A to E, most severe first); a test marked GUARD passes and pins
/// something that is not obvious.
/// </summary>
/// <remarks>
/// What fixed each finding (the comments of the tests describe the code as it was reviewed):
/// A — the byte share of a channel nobody reads is strict (<c>ReceiveCredit.TryTake</c> counts the block of the message
/// that asks, so a message larger than the share is not started), and a channel the application drains may have half the
/// receive budget waiting, not all of it. Three of the A tests asserted what the reviewed code accepted before their
/// last assertion; those lines now assert what the fix accepts, and say so.
/// B — <c>Drain</c> rents the decode buffer of a compressed message of a reliable channel before it takes the message,
/// and leaves the message queued when there is none.
/// C, D — <c>Route</c> puts a message of a channel that has messages queued behind them.
/// E — every Poll settles the limit of a channel whose handler was registered over a backlog until it is lifted.
/// </remarks>
public class ReviewCreditAccountingTests
{
    private const int Ring = 64;

    /// <summary>Messages a reliable channel without a handler may have waiting at <see cref="Ring"/> (two reliable channels).</summary>
    private const int Limit = 16;

    private const ushort Datagrams = 3;
    private const ushort Ordered = 10;
    private const ushort Groups = 11;

    /// <summary>2, 3 unordered datagrams · 6 unordered LZ4 · 8 keyed sequenced · 10 ordered stream · 11 group streams.</summary>
    private static readonly ChannelTable Table = TestTables.Plumbing;

    private static SessionHarness Harness(Action<PeerOptions>? server = null, Action<PeerOptions>? client = null, ChannelTable? table = null) =>
        new(table: table ?? Table,
            client: o =>
            {
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

    private static int Waiting(QuiclyPeer peer, ushort channel) => peer.Core.Credit.Waiting(peer.Core.ChannelIndexOf(channel));

    private static int WaitingBytes(QuiclyPeer peer, ushort channel) => peer.Core.Credit.WaitingBytes(peer.Core.ChannelIndexOf(channel));

    private static CreditState State(QuiclyPeer peer, ushort channel) => peer.Core.Credit.State(peer.Core.ChannelIndexOf(channel));

    private static int Queued(QuiclyPeer peer, ushort channel) => peer.DrainQueues.Count(peer.Core.ChannelIndexOf(channel));

    /// <summary>Sends numbered messages in batches of sixteen with both ends pumped in between.</summary>
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

    /// <summary>Pumps only the sender and the network: the receiver is late with its Poll.</summary>
    private static void PumpSenderOnly(SessionHarness h, QuiclyPeer sender, int steps = 8)
    {
        for (int step = 0; step < steps; step++)
        {
            sender.Poll();
            sender.Flush();
            h.Network.Advance(1_000);
        }
    }

    /// <summary>Sends numbered messages while only the sender is pumped: the receiver is late with its Poll.</summary>
    private static void SendToALateReceiver(SessionHarness h, QuiclyPeer sender, ushort channel, int first, int count, int size = 4)
    {
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(SendStatus.Admitted, sender.SendCopy(new SendHeader(channel), Payload(first + i, size)).Status);
        }

        PumpSenderOnly(h, sender);
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

    private static List<int> DrainUntil(SimFixture h, QuiclyPeer peer, ushort channel, int count)
    {
        List<int> got = [];
        Assert.True(h.RunUntil(() =>
        {
            got.AddRange(DrainAll(peer, channel));
            return got.Count >= count;
        }, 5_000_000), $"only {got.Count} of {count} messages came out of channel {channel}");
        return got;
    }

    private static readonly FieldInfo HasHeldField = typeof(QuiclyPeer).GetField("_hasHeld", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo HeldField = typeof(QuiclyPeer).GetField("_held", BindingFlags.Instance | BindingFlags.NonPublic)!;

    /// <summary>The channel of the message the peer holds (the "held" entry that closes the receive ring), or -1.</summary>
    private static int HeldChannel(QuiclyPeer peer)
    {
        if (!(bool)HasHeldField.GetValue(peer)!)
        {
            return -1;
        }

        return ((ReceiveEntry)HeldField.GetValue(peer)!).Channel;
    }

    // ================================================================== FINDING E: a handler that throws keeps the limit of a channel nobody reads

    /// <summary>
    /// FINDING E (promise 3: "a channel with a handler has no such limit"; RegisterHandler: a channel registered over a
    /// backlog loses the limit "once the next Poll has dispatched what was queued").
    /// <para>
    /// A handler is registered over a backlog, so the channel keeps the share of a channel nobody reads until the backlog
    /// is dispatched. The only place that lifts it afterwards is the <c>SettleCreditLimit</c> behind the dispatch loop of
    /// <c>DispatchQueued</c> (QuiclyPeer.Poll.cs). A handler that throws on the <em>last</em> queued message (handler
    /// exceptions propagate out of Poll: session-layer.md §4.4) leaves that loop before the call, with the queue already
    /// empty. No later Poll enters <c>DispatchQueued</c> for the channel again (<c>QueuedHandled</c> is 0), so the channel
    /// stays <see cref="CreditState.Unread"/> for as long as the handler stays: a channel <em>with</em> a handler is held
    /// to the share of a channel nobody reads, its stream is held back in every Poll interval that brings more than the
    /// share, and <c>BacklogHolds</c> keeps rising on a channel that is read at every Poll.
    /// </para>
    /// </summary>
    [Fact]
    public void A_Handler_That_Throws_On_The_Last_Message_Of_Its_Backlog_Still_Loses_The_Limit()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Ordered);
        Send(h, h.Client, Ordered, 0, Limit);
        Assert.Equal(Limit, Queued(server, Ordered));

        List<int> got = [];
        bool thrown = false;
        server.RegisterHandler(Ordered, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            int message = IndexOf(payload);
            got.Add(message);
            if (message == Limit - 1 && !thrown)
            {
                thrown = true;
                throw new InvalidOperationException("the application's handler failed once");
            }
        });
        Assert.True(server.Core.Credit.IsLimited(index), "the limit is kept while the backlog is queued (as documented)");

        // The Poll that dispatches the backlog: the handler sees all of it, and fails on the last message.
        Assert.Throws<InvalidOperationException>(() => server.Poll());
        Assert.Equal(Enumerable.Range(0, Limit), got);
        Assert.Equal(0, Queued(server, Ordered));
        Assert.Equal(0, Waiting(server, Ordered));

        // The host carries on. The handler has seen what waited for it; from here on the ring alone limits the channel.
        h.Run(10_000);
        long holds = Channel(server, Ordered).BacklogHolds;
        SendToALateReceiver(h, h.Client, Ordered, Limit, 60);
        int waiting = Waiting(server, Ordered);
        Assert.True(h.RunUntil(() => got.Count == Limit + 60), $"{got.Count} of {Limit + 60} messages reached the handler");
        Assert.Equal(Enumerable.Range(0, Limit + 60), got);
        Assert.True(
            waiting == 60 && !server.Core.Credit.IsLimited(index) && Channel(server, Ordered).BacklogHolds == holds,
            $"a channel with a handler is still held to the share of a channel nobody reads: state {State(server, Ordered)}, "
            + $"{waiting} of a burst of 60 were accepted while the host was late, and its stream was held back "
            + $"{Channel(server, Ordered).BacklogHolds - holds} more times");
    }

    // ================================================================== FINDINGS C and D: a handler gets an ordered channel out of order

    /// <summary>
    /// FINDING C (promise 2: "nothing of the unread channel is lost or reordered ... when the application drains it or
    /// registers a handler"; ReliableOrdered: exact order).
    /// <para>
    /// The unread channel has messages in its drain queue, and more of its messages are in the receive ring behind a
    /// message of another channel whose handler registers the unread channel's handler (an application that starts
    /// reading a channel when a "welcome" message arrives). <c>DispatchQueued</c> has already run in that Poll, and
    /// <c>Route</c> (QuiclyPeer.Poll.cs) dispatches a ring message to a handler without looking at the channel's queue, so
    /// the handler gets the newer messages from the ring first and the queued, older ones in the next Poll.
    /// </para>
    /// <para>
    /// <c>Route</c> is not new in credit-v3 (the same lines are on group-on-drops); the promise that registering a handler
    /// over an unread channel keeps its order is.
    /// </para>
    /// </summary>
    [Fact]
    public void A_Handler_Registered_From_Inside_Another_Handler_Gets_The_Channels_Backlog_First()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        server.RegisterHandler(Datagrams, (QuiclyPeer self, in ReceiveHeader _, ReadOnlySpan<byte> _) =>
            self.RegisterHandler(Ordered, Collect(got)));

        // Three messages of the unread channel wait in its drain queue.
        Send(h, h.Client, Ordered, 0, 3);
        Assert.Equal(3, Queued(server, Ordered));

        // The host is late. First the message that makes the application register the handler, then three more messages
        // of the ordered channel: the ring holds them in that order.
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Datagrams), Payload(0)).Status);
        PumpSenderOnly(h, h.Client);
        SendToALateReceiver(h, h.Client, Ordered, 3, 3);
        Assert.Equal(6, Waiting(server, Ordered));

        h.Run(10_000);
        Assert.Equal(6, got.Count);
        Assert.True(got.SequenceEqual(Enumerable.Range(0, 6)),
            $"the handler of a ReliableOrdered channel got its messages out of order: {string.Join(",", got)}");
    }

    /// <summary>
    /// FINDING D, the root cause of C on a channel that has a handler all along (not new in credit-v3, and outside the
    /// credit's own promises: reported because it is the same two lines of <c>Route</c>).
    /// A handler that drains another channel makes <c>Drain</c> move the ring's messages of its own channel into the drain
    /// queue; when that Drain stops with the ring not empty (its span is full), the Poll goes on dispatching the ring, and
    /// the handler gets the ring's messages before the queued, older ones.
    /// </summary>
    [Fact]
    public void A_Drain_Of_Another_Channel_From_Inside_A_Handler_Does_Not_Reorder_The_Handlers_Channel()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        ReceivedMessage[] one = new ReceivedMessage[1];
        server.RegisterHandler(Ordered, (QuiclyPeer self, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            got.Add(IndexOf(payload));
            if (got.Count == 1)
            {
                // The application reads the datagram channel with Drain, one message at a time.
                int taken = self.Drain(Datagrams, one);
                self.Release(one.AsSpan(0, taken));
            }
        });

        // The ring, in arrival order: ordered 0, ordered 1, a datagram, ordered 2.
        SendToALateReceiver(h, h.Client, Ordered, 0, 2);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Datagrams), Payload(0)).Status);
        PumpSenderOnly(h, h.Client);
        SendToALateReceiver(h, h.Client, Ordered, 2, 1);
        Assert.Equal(3, Waiting(server, Ordered));

        h.Run(10_000);
        Assert.Equal(3, got.Count);
        Assert.True(got.SequenceEqual(Enumerable.Range(0, 3)),
            $"the handler of a ReliableOrdered channel got its messages out of order: {string.Join(",", got)}");
    }

    // ================================================================== FINDING A: the byte share does not keep the receive budget for the channels that are read

    private sealed class CountingWorkSignal : IPeerWorkSignal
    {
        public int Calls;

        public void OnWork(QuiclyPeer peer) => Calls++;
    }

    /// <summary>
    /// FINDING A (promise 1: past its share only the unread channel's own streams are held back, every other channel keeps
    /// flowing, nothing is dropped, <c>HasPendingWork</c> does not stay true; <c>ReceiveCredit.ByteLimitFor</c>: the
    /// reliable channels share a quarter of the receive budget, "which leaves half of it to the traffic that is read").
    /// <para>
    /// The byte limit is a threshold for <em>starting</em> a message, and "one message always fits on an empty channel,
    /// whatever its size": so what an unread channel pins is not its share but up to its share plus one message, counted
    /// in pool blocks. With default options (a receive budget of 256 KiB, messages of up to 64 KiB, and a pool whose
    /// block above 16 KiB is 64 KiB) every unread channel that was sent one message above 16 KiB holds a quarter of the
    /// whole budget. Four of them hold all of it, each with one message — far inside the 256 messages it may have.
    /// From then on a channel that <em>is</em> read gets nothing: its datagrams are dropped for want of a receive buffer,
    /// its stream is held back, and because that hold is the budget's kind every Poll resumes the stream only for it to be
    /// held back again, so the host is kept polling until the application drains the channels it does not read.
    /// </para>
    /// </summary>
    [Fact]
    public void Unread_Channels_With_One_Message_Each_Leave_The_Receive_Budget_To_The_Channels_That_Are_Read()
    {
        // 2 datagrams · 4, 5, 7, 8 ordered (nobody reads them) · 9 ordered (read) · 6, 10 ordered (idle). Everything default.
        CountingWorkSignal signal = new();
        using SessionHarness h = Harness(table: OrderedTables.Main, server: o =>
        {
            o.ReceiveRingCapacity = 4096;
            o.WorkSignal = signal;
        });
        QuiclyPeer server = h.Server!;
        ReceiveCredit credit = server.Core.Credit;
        Assert.Equal(256 * 1024, h.ServerOptions.ReceiveBudgetBytes);
        List<int> read = [];
        List<int> datagrams = [];
        server.RegisterHandler(9, Collect(read));
        server.RegisterHandler(2, Collect(datagrams));

        // Control: while nothing waits on the channels nobody reads, the traffic that is read arrives.
        Send(h, h.Client, 2, 0, 20);
        Send(h, h.Client, 9, 0, 20);
        Assert.True(h.RunUntil(() => read.Count == 20 && datagrams.Count == 20, 2_000_000));
        Assert.Equal(0, DatagramKit.Statistics(server).OutOfReceiveBuffers);
        read.Clear();
        datagrams.Clear();

        ushort[] unread = [4, 5, 7, 8];
        foreach (ushort channel in unread)
        {
            Send(h, h.Client, channel, 0, 1, size: 20_000);
        }

        h.Run(50_000);
        foreach (ushort channel in unread)
        {
            // As fixed: the byte share is strict, so a message whose block (64 KiB) is larger than the share is not
            // accepted while nobody reads the channel. Its stream waits in the transport. (As reviewed, each channel
            // had accepted its one message, and the four of them held the whole budget.)
            Assert.Equal(0, Waiting(server, channel));
            Assert.Equal(1, Channel(server, channel).BacklogHolds);
        }

        long pinned = DatagramKit.Statistics(server).ReceiveBytesOutstanding;
        Assert.Equal(0, pinned);

        // The same traffic again.
        Send(h, h.Client, 2, 20, 20);
        Send(h, h.Client, 9, 20, 20);
        bool arrived = h.RunUntil(() => read.Count == 20 && datagrams.Count == 20, 2_000_000);
        long pends = DatagramKit.Statistics(server).StreamReceivePends;
        int signals = signal.Calls;
        h.Run(100_000);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        long holds = 0;
        foreach (ushort channel in unread)
        {
            holds += Channel(server, channel).BacklogHolds;
        }

        Assert.Equal(unread.Length, holds);
        Assert.Equal(pends, statistics.StreamReceivePends);
        Assert.Equal(signals, signal.Calls);
        Assert.False(server.HasPendingWork);
        Assert.True(arrived && statistics.OutOfReceiveBuffers == 0,
            $"four unread channels with one message each hold {pinned} of {h.ServerOptions.ReceiveBudgetBytes} receive-budget bytes (the byte share of a "
            + $"channel is {credit.ByteLimit}, its message share {credit.CountLimit}). The channels that are read: {read.Count} of 20 ordered messages and "
            + $"{datagrams.Count} of 20 datagrams arrived in two seconds, {statistics.OutOfReceiveBuffers} messages were dropped for want of a receive "
            + $"buffer (ReceiveRingDrops {statistics.ReceiveRingDrops}). In the 100 ms after that the read channel's stream was held back "
            + $"{statistics.StreamReceivePends - pends} more times and the host was called {signal.Calls - signals} more times "
            + $"(BacklogHolds of the unread channels: {holds}; HasPendingWork {server.HasPendingWork})");
    }

    /// <summary>
    /// FINDING A with one channel and one message. A channel whose <c>MaxMessageSize</c> was raised above
    /// 64 KiB (the receiver accepts messages up to its receive budget) and that nobody reads: its first message is accepted
    /// because the channel is empty, and takes the pool's largest block, which is the whole default receive budget.
    /// </summary>
    [Fact]
    public void One_Large_Message_On_An_Unread_Channel_Leaves_The_Receive_Budget_To_The_Channels_That_Are_Read()
    {
        ChannelTable table = ChannelTable.Create()
            .Add(3, "moves", ChannelMode.UnreliableUnordered)
            .Add(10, "snapshots", ChannelMode.ReliableOrdered, o => o.MaxMessageSize = 200_000)
            .Add(11, "events", ChannelMode.ReliableUnordered)
            .Build();
        using SessionHarness h = Harness(table: table);
        QuiclyPeer server = h.Server!;
        List<int> groups = [];
        List<int> datagrams = [];
        server.RegisterHandler(Groups, Collect(groups));
        server.RegisterHandler(Datagrams, Collect(datagrams));

        Send(h, h.Client, Ordered, 0, 1, size: 100_000);
        h.Run(50_000);

        // As fixed: not accepted while nobody reads the channel (its block is the whole budget, its share a fraction).
        Assert.Equal(0, Waiting(server, Ordered));
        Assert.Equal(1, Channel(server, Ordered).BacklogHolds);
        int pinned = WaitingBytes(server, Ordered);

        Send(h, h.Client, Datagrams, 0, 20);
        Send(h, h.Client, Groups, 0, 20);
        bool arrived = h.RunUntil(() => groups.Count == 20 && datagrams.Count == 20, 2_000_000);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(arrived && statistics.OutOfReceiveBuffers == 0,
            $"an unread channel with one message waiting holds {pinned} of {h.ServerOptions.ReceiveBudgetBytes} receive-budget bytes (its byte share is "
            + $"{server.Core.Credit.ByteLimit}). The channels that are read: {groups.Count} of 20 group messages and {datagrams.Count} of 20 datagrams "
            + $"arrived in two seconds, {statistics.OutOfReceiveBuffers} messages were dropped for want of a receive buffer, "
            + $"{statistics.StreamReceivePends} stream holds (BacklogHolds {Channel(server, Ordered).BacklogHolds}), HasPendingWork {server.HasPendingWork}");

        // And it is not lost: the application reads the channel, and the message arrives.
        List<int> got = DrainUntil(h, server, Ordered, 1);
        Assert.Equal([0], got);
    }

    /// <summary>
    /// FINDING A, its consequence for a reliable channel that <em>is</em> read and compresses (promise 1: "nothing is
    /// dropped"). A compressed message is decoded when it is dispatched, into a second lease of its raw size, and "a
    /// compressed message ... is dropped [when] decoding it would have exceeded ... the receive budget"
    /// (TROUBLESHOOTING.md). Three unread channels with one message above 16 KiB each hold three quarters of the default
    /// receive budget for as long as nobody drains them, so every message of the read channel whose raw size needs the
    /// fourth 64 KiB block arrives, is acknowledged to its sender, and is dropped at the handler's door: reliable
    /// messages of a channel with a handler are lost because other channels are not read.
    /// </summary>
    [Fact]
    public void Unread_Channels_Do_Not_Make_A_Compressed_Channel_That_Is_Read_Lose_Messages()
    {
        // 4, 5, 7 ordered (nobody reads them) · 6 ordered, LZ4 (read). Everything default but the ring.
        using SessionHarness h = Harness(table: OrderedTables.Main, server: o => o.ReceiveRingCapacity = 4096);
        QuiclyPeer server = h.Server!;
        List<int> packed = [];
        server.RegisterHandler(6, Collect(packed));

        // Control: three messages of 20 000 bytes that LZ4 packs into a few dozen (one per pump: the sender's budget holds
        // one at a time) arrive while nothing waits on the channels nobody reads.
        for (int i = -3; i < 0; i++)
        {
            Send(h, h.Client, 6, i, 1, size: 20_000);
        }

        Assert.True(h.RunUntil(() => packed.Count == 3, 2_000_000));
        Assert.Equal([-3, -2, -1], packed);
        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
        packed.Clear();

        ushort[] unread = [4, 5, 7];
        foreach (ushort channel in unread)
        {
            Send(h, h.Client, channel, 0, 1, size: 20_000);
        }

        h.Run(50_000);
        long pinned = DatagramKit.Statistics(server).ReceiveBytesOutstanding;

        // Ten more of the same.
        for (int i = 0; i < 10; i++)
        {
            Send(h, h.Client, 6, i, 1, size: 20_000);
        }

        h.Run(200_000);
        int whileUnread = packed.Count;

        // The application comes back for the channels it did not read. What was dropped does not come back.
        foreach (ushort channel in unread)
        {
            Assert.Equal([0], DrainUntil(h, server, channel, 1));
        }

        h.Run(200_000);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(packed.SequenceEqual(Enumerable.Range(0, 10)) && statistics.DecodeFailures == 0,
            $"three unread channels with one message each held {pinned} of {h.ServerOptions.ReceiveBudgetBytes} receive-budget bytes. The compressed channel "
            + $"that has a handler received {Channel(server, 6).Received - 3} of 10 messages from its stream and its handler saw {whileUnread} of them, "
            + $"{packed.Count} after the unread channels were drained; {statistics.DecodeFailures} were dropped as DecodeFailures");
    }

    /// <summary>
    /// GUARD (promise 5, the comment in <c>ReturnCredit</c>: "before the message is decoded"). A compressed message is
    /// counted with the lease it arrived in and gives back exactly that, whether it is drained, dispatched or dropped
    /// because it cannot be decoded (a decode budget of one byte per second drops every message here): the counts return
    /// to zero and the channel's stream is never left without credit.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void Compressed_Messages_Give_Back_Exactly_The_Credit_They_Took(bool groups, bool undecodable)
    {
        ChannelTable table = groups ? GroupTables.Main : OrderedTables.Main;
        ushort channel = groups ? (ushort)7 : (ushort)6;
        using SessionHarness h = Harness(table: table, server: o =>
        {
            if (undecodable)
            {
                o.DecodedBytesPerSecond = 1;
            }
        });
        QuiclyPeer server = h.Server!;
        ReceiveCredit credit = server.Core.Credit;

        // Nobody reads the channel: the share waits, counted in the blocks of the compressed messages.
        Send(h, h.Client, channel, 0, 120, size: 3_000);
        h.Run(50_000);
        int limit = credit.CountLimit;
        Assert.Equal(limit, Waiting(server, channel));
        Assert.True(WaitingBytes(server, channel) <= limit * 256, $"{WaitingBytes(server, channel)} bytes are counted for {limit} compressed messages");

        // Half by Drain, the rest by a handler.
        List<int> got = [];
        Assert.True(h.RunUntil(() =>
        {
            got.AddRange(DrainAll(server, channel));
            return Channel(server, channel).Received >= 60;
        }, 5_000_000), $"the stream stopped at {Channel(server, channel).Received} messages: {Waiting(server, channel)} waiting");
        server.RegisterHandler(channel, Collect(got));
        Assert.True(h.RunUntil(() => Channel(server, channel).Received == 120 && Waiting(server, channel) == 0, 5_000_000),
            $"the stream stopped at {Channel(server, channel).Received} messages: {Waiting(server, channel)} waiting");
        h.Run(20_000);

        Assert.Equal(0, Waiting(server, channel));
        Assert.Equal(0, WaitingBytes(server, channel));
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.Equal(0, statistics.ReceiveBytesOutstanding);
        if (undecodable)
        {
            Assert.Empty(got);
            Assert.Equal(120, statistics.DecodeFailures);
        }
        else
        {
            Assert.Equal(0, statistics.DecodeFailures);
            Assert.Equal(Enumerable.Range(0, 120), groups ? got.Order() : got);
        }
    }

    /// <summary>
    /// FINDING B (promise 2: "nothing of the unread channel is lost ... when the application drains it"), for a channel that
    /// compresses. <c>Drain</c> decodes every message it hands out into a second lease of its raw size, and the caller can
    /// release nothing before the call returns; a message whose decoded lease does not fit the receive budget is dropped
    /// (<c>TryDecode</c>, counted as <c>DecodeFailures</c>) — and a dropped message does not fill the caller's span, so the
    /// loop of <c>Drain</c> goes on taking from the queue and dropping until the queue is empty. The share of an unread
    /// channel is counted in the blocks of the <em>compressed</em> messages, so far more raw bytes wait than the budget can
    /// hold decoded (37 messages of 20 000 bytes here, against a budget of four such blocks): the application that comes
    /// back and calls Drain with a span of sixteen gets three messages and loses the other 34 in that one call, although
    /// the sender was told every message was delivered. The mechanism is older than the credit (the same lines are on
    /// group-on-drops); the promise is new. The control is <see cref="Compressed_Messages_Give_Back_Exactly_The_Credit_They_Took"/>:
    /// the same channel with messages whose decoded batch fits the budget loses nothing.
    /// </summary>
    [Fact]
    public void An_Unread_Compressed_Channel_Loses_Nothing_When_It_Is_Drained()
    {
        // 6: ordered, LZ4. Everything default but the ring.
        using SessionHarness h = Harness(table: OrderedTables.Main, server: o => o.ReceiveRingCapacity = 4096);
        QuiclyPeer server = h.Server!;

        // Forty messages of 20 000 bytes that LZ4 packs into a few dozen (one per pump: the sender's budget holds one at a time).
        for (int i = 0; i < 40; i++)
        {
            Send(h, h.Client, 6, i, 1, size: 20_000);
        }

        h.Run(50_000);
        int waiting = Waiting(server, 6);
        int waitingBytes = WaitingBytes(server, 6);
        Assert.True(waiting >= 16, $"{waiting} messages wait");

        // The application comes back and drains, sixteen messages at a time, releasing each batch.
        List<int> got = [];
        h.RunUntil(() =>
        {
            got.AddRange(DrainAll(server, 6));
            return Channel(server, 6).Received == 40 && Waiting(server, 6) == 0;
        }, 5_000_000);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(got.SequenceEqual(Enumerable.Range(0, 40)) && statistics.DecodeFailures == 0,
            $"{waiting} compressed messages ({waitingBytes} bytes of blocks, the byte share is {server.Core.Credit.ByteLimit}) waited on the unread channel. "
            + $"Drained in batches of sixteen, {got.Count} of the 40 messages the stream delivered came out ({string.Join(",", got.Take(12))}...); "
            + $"{statistics.DecodeFailures} were dropped as DecodeFailures");
    }

    /// <summary>
    /// FINDING A by another road (promise 3: "a narrower limit takes nothing back"; PROTOCOL.md §7: "a host that stops
    /// draining it is confined within two Poll intervals"; promise 1 for the channel it then is). A channel the
    /// application drains has no byte limit but the receive budget, and keeps what it accepted when it is demoted. The
    /// drain queues have a node for every one of those messages, so the ring stays open — but the receive budget has
    /// nothing left: a burst of 64 messages of 4 000 bytes is all of it. From then on the channel is one nobody reads that
    /// holds the whole budget, and the channels that are read lose their datagrams and have their streams held back in
    /// every Poll, until the application drains it.
    /// </summary>
    [Fact]
    public void A_Channel_The_Application_Stops_Draining_Leaves_The_Receive_Budget_To_The_Channels_That_Are_Read()
    {
        CountingWorkSignal signal = new();
        using SessionHarness h = Harness(client: OrderedKit.Roomy, server: o => o.WorkSignal = signal);
        QuiclyPeer server = h.Server!;
        List<int> groups = [];
        List<int> datagrams = [];
        server.RegisterHandler(Groups, Collect(groups));
        server.RegisterHandler(Datagrams, Collect(datagrams));

        // The first frame of a consumer that drains: nothing to take yet.
        Assert.Empty(DrainAll(server, Ordered));
        Assert.Equal(CreditState.Drained, State(server, Ordered));

        // A burst within the ring and the budget arrives, and from here on the host only polls.
        for (int i = 0; i < Ring; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Ordered), Payload(i, 4_000)).Status);
        }

        PumpSenderOnly(h, h.Client, 40);

        // As fixed: a channel the application drains may have half the receive budget waiting, so that what it keeps
        // when the application stops coming leaves the other half. (As reviewed, it took all 64 messages: the budget.)
        int accepted = (int)(h.ServerOptions.ReceiveBudgetBytes / 2 / 4_096);
        Assert.Equal(accepted, Waiting(server, Ordered));
        Assert.True(accepted < Ring);
        server.Poll();
        server.Poll();
        Assert.Equal(CreditState.Unread, State(server, Ordered));
        Assert.Equal(-1, HeldChannel(server));
        long pinned = DatagramKit.Statistics(server).ReceiveBytesOutstanding;

        // The traffic that is read.
        Send(h, h.Client, Datagrams, 0, 20);
        Send(h, h.Client, Groups, 0, 20);
        bool arrived = h.RunUntil(() => groups.Count == 20 && datagrams.Count == 20, 2_000_000);
        long pends = DatagramKit.Statistics(server).StreamReceivePends;
        int signals = signal.Calls;
        h.Run(100_000);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(arrived && statistics.OutOfReceiveBuffers == 0,
            $"a channel the application stopped draining is Unread and holds {pinned} of {h.ServerOptions.ReceiveBudgetBytes} receive-budget bytes (its byte share "
            + $"is {server.Core.Credit.ByteLimit}). The channels that are read: {groups.Count} of 20 group messages and {datagrams.Count} of 20 datagrams arrived "
            + $"in two seconds, {statistics.OutOfReceiveBuffers} messages were dropped for want of a receive buffer. In the 100 ms after that a stream was "
            + $"held back {statistics.StreamReceivePends - pends} more times and the host was called {signal.Calls - signals} more times "
            + $"(BacklogHolds of the unread channel: {Channel(server, Ordered).BacklogHolds})");
    }

    // ================================================================== guards

    /// <summary>
    /// GUARD (the question "does a stream that is parked for credit get reset for being idle?"): no. A stream is held back
    /// for credit only at the start of a message, where the mid-message idle watch is not armed — after the preamble, or
    /// after the end of the message before — so streams that wait for the application far longer than
    /// <see cref="PeerOptions.StreamIdleTimeout"/> are not reset, and everything they carry arrives when the channel is drained.
    /// </summary>
    [Fact]
    public void A_Stream_That_Waits_For_Credit_Is_Not_Reset_For_Being_Idle()
    {
        using ServerHarness h = new(table: Table, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = Ring;
            o.StreamIdleTimeout = TimeSpan.FromMilliseconds(200);
        });
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;

        // A group that takes all of the credit but one message, and ends.
        byte[][] first = new byte[Limit - 1][];
        for (int i = 0; i < first.Length; i++)
        {
            first[i] = Payload(i);
        }

        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, 1, first), out _, fin: true));
        Assert.True(h.RunUntil(() => Waiting(server, Groups) == Limit - 1));

        // A group of three: its first message takes the last credit, and it waits at the boundary before the second.
        Assert.Equal(TransportStatus.Success,
            h.Raw.OpenUni(GroupKit.GroupStream(Groups, 2, Payload(100), Payload(101), Payload(102)), out _, fin: true));

        // A group that waits right behind its preamble, and one that waits there without having ended.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, 3, Payload(200), Payload(201)), out _, fin: true));
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, 4, Payload(300)), out TransportStreamId open));
        Assert.True(h.RunUntil(() => server.Core.Credit.ParkedStreams == 3), $"{server.Core.Credit.ParkedStreams} groups wait for credit");
        Assert.Equal(Limit, Waiting(server, Groups));

        // Ten times the idle timeout, the host polling all the time.
        h.Run(2_000_000);
        PeerStatistics statistics = h.Statistics();
        Assert.Equal(0, statistics.StreamIdleTimeouts);
        Assert.Equal(0, statistics.StreamsReset);
        Assert.Equal(3, server.Core.Credit.ParkedStreams);
        Assert.False(server.HasPendingWork);

        List<int> got = DrainUntil(h, server, Groups, Limit - 1 + 6);
        Assert.Equal([.. Enumerable.Range(0, Limit - 1), 100, 101, 102, 200, 201, 300], got.Order());
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.Raw.Transport, open, [], TransportSendFlags.Fin));
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(server, Groups) == 0));
        Assert.Equal(0, h.Statistics().StreamsReset);
        Assert.Equal(0, Waiting(server, Groups));
    }

    /// <summary>
    /// GUARD (promise 5, empty payloads): a message without payload takes no lease, so only the message count limits the
    /// channel — and that it does, for both stream modes.
    /// </summary>
    [Theory]
    [InlineData(Ordered)]
    [InlineData(Groups)]
    public void Empty_Messages_Are_Counted_By_Number(ushort channel)
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        for (int i = 0; i < 100; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(channel), []).Status);
            if ((i & 15) == 15)
            {
                h.Run(2_000);
            }
        }

        h.Run(50_000);
        Assert.Equal(Limit, Waiting(server, channel));
        Assert.Equal(0, WaitingBytes(server, channel));
        Assert.False(server.HasPendingWork);

        int taken = 0;
        ReceivedMessage[] buffer = new ReceivedMessage[7];
        Assert.True(h.RunUntil(() =>
        {
            int count;
            while ((count = server.Drain(channel, buffer)) > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    Assert.Equal(0, buffer[i].Header.Length);
                }

                server.Release(buffer.AsSpan(0, count));
                taken += count;
            }

            return taken >= 100;
        }, 5_000_000), $"{taken} of 100 empty messages came out");
        Assert.Equal(100, taken);
        Assert.Equal(0, Waiting(server, channel));
    }

    private static readonly byte[] ResumeToken = [9, 8, 7, 6];

    /// <summary>
    /// GUARD (promise 5, "reconnect"): an in-place Reconnect of a peer that was not polled to Closed — messages of an
    /// unread channel in its drain queue beyond its share (it was drained, then left alone), its stream parked for
    /// credit, and a half-received message of a second channel staged in its engine. The counts start over at zero, no
    /// receive byte stays held, the way the application reads each channel is kept, and the resumed connection delivers
    /// everything in order within the share.
    /// </summary>
    [Fact]
    public void A_Reconnect_With_Messages_Queued_Staged_And_Streams_Parked_Starts_With_Exact_Counts()
    {
        using SessionHarness h = new(connect: false, table: Table, link: new LinkOptions { BandwidthBitsPerSecond = 1_000_000 },
            server: GroupKit.Prompt, client: o =>
            {
                GroupKit.Prompt(o);
                o.ReceiveRingCapacity = Ring;
            });
        h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
            AdmissionResult.Accept(ResumeToken, 4242, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer client = h.Client;
        QuiclyPeer oldServer = h.Server!;
        ReceiveCredit credit = client.Core.Credit;

        // The ordered channel is drained once, takes a burst under the ring's limit, and is then left alone for two Polls.
        Assert.Empty(DrainAll(client, Ordered));
        Assert.Equal(CreditState.Drained, State(client, Ordered));
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal(SendStatus.Admitted, oldServer.SendCopy(new SendHeader(Ordered), Payload(i)).Status);
        }

        for (int step = 0; step < 50 && Waiting(client, Ordered) < 40; step++)
        {
            oldServer.Poll();
            oldServer.Flush();
            h.Network.Advance(1_000);
        }

        Assert.Equal(40, Waiting(client, Ordered));
        client.Poll();
        client.Poll();
        Assert.Equal(CreditState.Unread, State(client, Ordered));
        Assert.Equal(40, Queued(client, Ordered));

        // More of it: the stream is parked for credit.
        Send(h, oldServer, Ordered, 40, 20);
        Assert.Equal(1, credit.ParkedStreams);
        Assert.Equal(40, Waiting(client, Ordered));

        // A group message of 12 000 bytes that is still arriving when the connection is lost (its 16 KiB block is within
        // the byte share of a channel nobody reads, which is strict: a larger one would not be started).
        Assert.Equal(SendStatus.Admitted, oldServer.SendCopy(new SendHeader(Groups), Payload(7, 12_000)).Status);
        Assert.True(h.RunUntil(() => Waiting(client, Groups) == 1, 2_000_000, step: 200), "the group message did not start");
        Assert.Equal(0, Channel(client, Groups).Received);
        Assert.True(WaitingBytes(client, Groups) > 0);

        // The connection is lost, and the host reconnects without having polled the peer to Closed.
        oldServer.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);
        for (int step = 0; step < 2_000 && !client.CanReconnect; step++)
        {
            h.Network.Advance(1_000);
        }

        Assert.True(client.CanReconnect);
        client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        foreach (ushort channel in new[] { Ordered, Groups })
        {
            Assert.Equal(0, Waiting(client, channel));
            Assert.Equal(0, WaitingBytes(client, channel));
            Assert.Equal(0, Queued(client, channel));
        }

        Assert.Equal(0, credit.ParkedStreams);
        Assert.Equal(0, client.DrainQueues.Used);
        Assert.Equal(0, client.DrainQueues.ExcessUsed);
        Assert.Equal(0, DatagramKit.Statistics(client).ReceiveBytesOutstanding);
        Assert.Equal(CreditState.Unread, State(client, Ordered));
        Assert.True(h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();

        // The resumed connection: within the share, nothing lost, in order.
        Send(h, h.Server!, Ordered, 1_000, 60);
        Send(h, h.Server!, Groups, 2_000, 60);
        h.Run(100_000);
        Assert.Equal(credit.CountLimit, Waiting(client, Ordered));
        Assert.Equal(credit.CountLimit, Waiting(client, Groups));
        Assert.False(client.HasPendingWork);
        Assert.Equal(Enumerable.Range(1_000, 60), DrainUntil(h, client, Ordered, 60));
        Assert.Equal(Enumerable.Range(2_000, 60), DrainUntil(h, client, Groups, 60).Order());
        Assert.Equal(0, Waiting(client, Ordered));
        Assert.Equal(0, Waiting(client, Groups));
        Assert.Equal(0, DatagramKit.Statistics(client).StreamsReset);
    }

    // ================================================================== scripted runs

    [Flags]
    private enum Moves
    {
        None = 0,

        /// <summary>A handler may register the handler of another channel.</summary>
        NestedRegister = 1,

        /// <summary>A handler may call Drain (its own channel or another one).</summary>
        NestedDrain = 2,

        /// <summary>A handler may throw.</summary>
        Throwing = 4,

        /// <summary>At the end every channel with a handler must be <see cref="CreditState.Handled"/>.</summary>
        CheckSettled = 8,
    }

    private sealed class ScriptFault : Exception;

    /// <summary>
    /// One seeded run of random application moves against a sender that floods every reliable channel of the table and one
    /// datagram channel: Poll, Poll(maxItems), Drain with spans of any size, handlers that come and go (also from inside a
    /// handler), a late receiver, messages of four sizes (one above the byte share of an unread channel). Checked after
    /// every move: after a Poll no reliable channel without a handler is the held entry (promise 4), the nodes beyond the
    /// pool stay within <c>ExcessNodes</c>, while the application does nothing a channel in <see cref="CreditState.Unread"/>
    /// or <see cref="CreditState.Drained"/> never gets more waiting than the limit of its state, and a Poll that ran to its
    /// end leaves <c>HasPendingWork</c> clear unless it left work for the next Poll. Checked at the end: every reliable
    /// message came out exactly once, the ordered channels in order, the credit counts are back at zero, nothing is left
    /// allocated and nothing was reset.
    /// </summary>
    /// <returns>Null, or what went wrong.</returns>
    private static string? RunScript(int seed, int ring, Moves moves, ChannelTable table, int steps = 300)
    {
        Random random = new(seed);
        using SessionHarness h = Harness(table: table, server: o => o.ReceiveRingCapacity = ring);
        QuiclyPeer server = h.Server!;
        QuiclyPeer client = h.Client;
        ReceiveCredit credit = server.Core.Credit;
        ReceiveQueueLayout layout = ReceiveQueueLayout.Compute(server.Core.Channels, ring, h.ServerOptions.ReceiveBudgetBytes);
        int ringCapacity = server.Core.ReceiveRing.Capacity;
        List<ushort> reliableList = [];
        ushort datagram = 0;
        foreach (ChannelDefinition definition in server.Core.Channels)
        {
            if (definition.Mode is ChannelMode.ReliableOrdered or ChannelMode.ReliableUnordered)
            {
                reliableList.Add(definition.Id);
            }
            else if (definition.Mode == ChannelMode.UnreliableUnordered && datagram == 0 && !definition.CoalesceOnReceive)
            {
                datagram = definition.Id;
            }
        }

        ushort[] reliable = [.. reliableList];
        ushort[] channels = [.. reliable, datagram];
        Dictionary<ushort, List<int>> got = [];
        Dictionary<ushort, int> sent = [];
        foreach (ushort channel in channels)
        {
            got[channel] = [];
            sent[channel] = 0;
        }

        HashSet<ushort> handled = [];
        ReceivedMessage[] outer = new ReceivedMessage[48];
        ReceivedMessage[] inner = new ReceivedMessage[48];
        string? failure = null;

        void Take(ushort channel, int max, ReceivedMessage[] into)
        {
            int taken = server.Drain(channel, into.AsSpan(0, max));
            for (int i = 0; i < taken; i++)
            {
                got[channel].Add(IndexOf(into[i].Payload));
            }

            server.Release(into.AsSpan(0, taken));
        }

        MessageHandler handler = null!;
        handler = (QuiclyPeer self, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            ushort channel = header.Channel;
            got[channel].Add(IndexOf(payload));
            int roll = random.Next(100);
            if (roll < 4)
            {
                self.UnregisterHandler(channel);
                handled.Remove(channel);
            }
            else if (roll < 12 && (moves & Moves.NestedDrain) != 0)
            {
                Take(channels[random.Next(channels.Length)], random.Next(1, 9), inner);
            }
            else if (roll < 18 && (moves & Moves.NestedRegister) != 0)
            {
                ushort other = channels[random.Next(channels.Length)];
                if (handled.Add(other))
                {
                    self.RegisterHandler(other, handler);
                }
            }
            else if (roll < 20 && (moves & Moves.Throwing) != 0)
            {
                throw new ScriptFault();
            }
        };

        bool PollServer(int maxItems = int.MaxValue)
        {
            bool completed = true;
            try
            {
                server.Poll(maxItems);
            }
            catch (ScriptFault)
            {
                completed = false;
            }

            server.Flush();
            return completed;
        }

        string Describe()
        {
            List<string> parts = [];
            foreach (ushort channel in reliable)
            {
                parts.Add($"{channel} {State(server, channel)} waiting {Waiting(server, channel)}/{WaitingBytes(server, channel)} B got {got[channel].Count} of {sent[channel]}"
                    + (handled.Contains(channel) ? " handler" : string.Empty));
            }

            return string.Join("; ", parts) + $"; parked {credit.ParkedStreams}, credit work {credit.HasWork}, ring empty {server.Core.ReceiveRing.IsEmpty}, "
                + $"budget held {DatagramKit.Statistics(server).ReceiveBytesOutstanding}";
        }

        string? Check(string after, bool polled)
        {
            int held = HeldChannel(server);
            if (polled && held >= 0 && Array.IndexOf(reliable, (ushort)held) >= 0 && !handled.Contains((ushort)held))
            {
                return $"after {after}: a message of reliable channel {held}, which has no handler, is still the held entry after a Poll";
            }

            if (server.DrainQueues.ExcessUsed > layout.ExcessNodes)
            {
                return $"after {after}: {server.DrainQueues.ExcessUsed} nodes beyond the pool are in use, ExcessNodes is {layout.ExcessNodes}";
            }

            foreach (ushort channel in reliable)
            {
                int waiting = Waiting(server, channel);
                if (waiting < 0 || waiting > ringCapacity + 1 + layout.Capacity)
                {
                    return $"after {after}: channel {channel} has {waiting} messages waiting";
                }

                if (WaitingBytes(server, channel) < 0)
                {
                    return $"after {after}: channel {channel} has {WaitingBytes(server, channel)} bytes waiting";
                }
            }

            return null;
        }

        // After a Poll that ran to its end and a Flush: the probe may stay set only for what the next Poll can do — streams the
        // ring's or the budget's hold left for it, messages queued for a handler, a datagram held for its Drain. A reliable
        // channel that waits for the application is none of that.
        string? Idle(string at)
        {
            if (!server.HasPendingWork || HeldChannel(server) >= 0 || !server.Core.PendedStreams.IsEmpty || server.DrainQueues.QueuedHandled != 0)
            {
                return null;
            }

            return $"{at}: HasPendingWork is set after a Poll and a Flush ({Describe()})";
        }

        // Only the sender and the network run: whatever arrives is admitted by the credit alone.
        string? Arrive()
        {
            Span<int> before = stackalloc int[reliable.Length];
            Span<int> bytesBefore = stackalloc int[reliable.Length];
            for (int i = 0; i < reliable.Length; i++)
            {
                before[i] = Waiting(server, reliable[i]);
                bytesBefore[i] = WaitingBytes(server, reliable[i]);
            }

            client.Poll();
            client.Flush();
            h.Network.Advance(1_000);
            for (int i = 0; i < reliable.Length; i++)
            {
                int index = server.Core.ChannelIndexOf(reliable[i]);
                int limit = credit.State(index) switch
                {
                    CreditState.Unread => credit.CountLimit,
                    CreditState.Drained => credit.DrainedCountLimit,
                    _ => int.MaxValue,
                };
                int waiting = Waiting(server, reliable[i]);
                if (waiting > Math.Max(before[i], limit))
                {
                    return $"channel {reliable[i]} ({credit.State(index)}) went from {before[i]} to {waiting} messages waiting; its limit is {limit}";
                }

                // A message is accepted on an empty channel, or while less than the byte limit waits: never more than the
                // limit and one block (the largest block is 64 KiB here).
                int bytes = WaitingBytes(server, reliable[i]);
                if (credit.State(index) == CreditState.Unread && bytes > Math.Max(bytesBefore[i], credit.ByteLimit + 65_536))
                {
                    return $"channel {reliable[i]} (Unread) went from {bytesBefore[i]} to {bytes} bytes waiting; its limit is {credit.ByteLimit}";
                }
            }

            return null;
        }

        for (int step = 0; step < steps && failure is null; step++)
        {
            int roll = random.Next(100);
            string move;
            if (roll < 30)
            {
                ushort channel = channels[random.Next(channels.Length)];
                int count = random.Next(1, 31);
                int sizeRoll = random.Next(100);
                int size = channel == datagram || sizeRoll < 78 ? 4 : sizeRoll < 93 ? 600 : sizeRoll < 98 ? 5_000 : 40_000;
                size = Math.Min(size, table[channel]!.MaxMessageSize);
                for (int i = 0; i < count; i++)
                {
                    if (!client.SendCopy(new SendHeader(channel), Payload(sent[channel], size)).IsAdmitted)
                    {
                        break;
                    }

                    sent[channel]++;
                }

                move = $"send {channel}";
            }
            else if (roll < 50)
            {
                failure = Arrive();
                move = "arrive";
            }
            else if (roll < 65)
            {
                bool completed = PollServer();
                move = completed ? "Poll" : "Poll that threw";
                failure = completed ? Idle($"step {step}") : null;
            }
            else if (roll < 70)
            {
                int max = random.Next(0, 12);
                move = PollServer(max) ? $"Poll({max})" : "Poll that threw";
            }
            else if (roll < 85)
            {
                ushort channel = channels[random.Next(channels.Length)];
                int max = random.Next(1, 49);
                Take(channel, max, outer);
                move = $"Drain({channel}, {max})";
            }
            else if (roll < 90)
            {
                ushort channel = channels[random.Next(channels.Length)];
                int before;
                do
                {
                    before = got[channel].Count;
                    Take(channel, outer.Length, outer);
                }
                while (got[channel].Count != before);
                move = $"drain {channel} empty";
            }
            else if (roll < 95)
            {
                ushort channel = channels[random.Next(channels.Length)];
                if (handled.Add(channel))
                {
                    server.RegisterHandler(channel, handler);
                }

                move = $"register {channel}";
            }
            else
            {
                ushort channel = channels[random.Next(channels.Length)];
                if (handled.Remove(channel))
                {
                    server.UnregisterHandler(channel);
                }

                move = $"unregister {channel}";
            }

            failure ??= Check($"step {step} ({move})", move.StartsWith("Poll", StringComparison.Ordinal) && !move.EndsWith("threw", StringComparison.Ordinal));
        }

        if (failure is not null)
        {
            return failure;
        }

        // The application settles: it polls, and drains every channel that has no handler, until everything came out.
        // A compressed message that cannot be decoded within the decode budget or the receive budget is dropped and counted
        // (PROTOCOL.md §7, TROUBLESHOOTING.md "DecodeFailures"): those are the only messages that may be missing.
        long Undecoded() => DatagramKit.Statistics(server).DecodeFailures;

        int Missing(bool compressed)
        {
            int missing = 0;
            foreach (ushort channel in reliable)
            {
                if ((table[channel]!.Compression != ChannelCompression.None) == compressed)
                {
                    missing += sent[channel] - got[channel].Count;
                }
            }

            return missing;
        }

        bool Done() => Missing(compressed: false) <= 0 && Missing(compressed: true) <= Undecoded();

        void DrainUnhandled()
        {
            foreach (ushort channel in channels)
            {
                if (!handled.Contains(channel))
                {
                    Take(channel, outer.Length, outer);
                }
            }
        }

        for (int i = 0; i < 40_000 && !Done() && failure is null; i++)
        {
            client.Poll();
            client.Flush();
            failure = Check("settling", polled: PollServer());
            DrainUnhandled();
            h.Network.Advance(1_000);
        }

        if (failure is not null)
        {
            return failure;
        }

        if (!Done())
        {
            return $"stalled: {Describe()}";
        }

        for (int i = 0; i < 20; i++)
        {
            client.Poll();
            client.Flush();
            PollServer();
            DrainUnhandled();
            h.Network.Advance(1_000);
        }

        foreach (ushort channel in reliable)
        {
            if (table[channel]!.Compression != ChannelCompression.None)
            {
                // Messages may be missing (counted as DecodeFailures), but none twice, and an ordered channel stays in order.
                bool ordered = table[channel]!.Mode == ChannelMode.ReliableOrdered;
                List<int> seen = ordered ? got[channel] : [.. got[channel].Order()];
                for (int i = 1; i < seen.Count; i++)
                {
                    if (seen[i] <= seen[i - 1])
                    {
                        return $"compressed channel {channel} delivered a message twice or out of order at position {i}: {seen[i - 1]},{seen[i]}";
                    }
                }
            }
            else if (table[channel]!.Mode == ChannelMode.ReliableOrdered)
            {
                if (!got[channel].SequenceEqual(Enumerable.Range(0, sent[channel])))
                {
                    int at = 0;
                    while (at < got[channel].Count && got[channel][at] == at)
                    {
                        at++;
                    }

                    return $"ordered channel {channel} is out of order or not exact at position {at}: "
                        + string.Join(",", got[channel].Skip(Math.Max(0, at - 2)).Take(12)) + $" ({got[channel].Count} of {sent[channel]})";
                }
            }
            else if (!got[channel].Order().SequenceEqual(Enumerable.Range(0, sent[channel])))
            {
                return $"group channel {channel} did not deliver every message once: {got[channel].Count} of {sent[channel]}, {got[channel].Distinct().Count()} distinct";
            }

            if (Waiting(server, channel) != 0 || WaitingBytes(server, channel) != 0)
            {
                return $"channel {channel}: {Waiting(server, channel)} messages and {WaitingBytes(server, channel)} bytes are still counted as waiting";
            }

            if ((moves & Moves.CheckSettled) != 0 && handled.Contains(channel) && State(server, channel) != CreditState.Handled)
            {
                return $"channel {channel} has a handler and nothing queued, and is still {State(server, channel)}";
            }
        }

        PeerStatistics statistics = DatagramKit.Statistics(server);
        if (server.DrainQueues.ExcessUsed != 0 || server.DrainQueues.Used != 0 || statistics.ReceiveBytesOutstanding != 0)
        {
            return $"left behind: {server.DrainQueues.Used} queued, {server.DrainQueues.ExcessUsed} beyond the pool, {statistics.ReceiveBytesOutstanding} receive bytes";
        }

        if (statistics.StreamsReset != 0 || statistics.CallbackFaults != 0 || statistics.DecodeFailures != Missing(compressed: true))
        {
            return $"{statistics.StreamsReset} streams reset, {statistics.CallbackFaults} callback faults, {statistics.DecodeFailures} decode failures for "
                + $"{Missing(compressed: true)} missing compressed messages";
        }

        if (PollServer() && Idle("the end") is { } busy)
        {
            return busy;
        }

        return null;
    }

    private static void RunScripts(int seeds, int ring, Moves moves, ChannelTable? table = null)
    {
        // REVIEW_CREDIT_SEEDS widens a run by hand (the review ran every guard below with 3 000 seeds once).
        if (int.TryParse(Environment.GetEnvironmentVariable("REVIEW_CREDIT_SEEDS"), out int wider) && wider > seeds)
        {
            seeds = wider;
        }

        List<string> failures = [];
        for (int seed = 1; seed <= seeds; seed++)
        {
            string? failure = RunScript(seed, ring, moves, table ?? Table);
            if (failure is not null)
            {
                failures.Add($"seed {seed}: {failure}");
            }
        }

        Assert.True(failures.Count == 0, $"{failures.Count} of {seeds} runs failed (ring {ring}, {moves}):\n" + string.Join("\n", failures.Take(6)));
    }

    /// <summary>
    /// GUARD: the moves the documents allow, with nothing nested in a handler but the removal of the handler itself, keep
    /// every promise that is checked by <see cref="RunScript"/>, on rings from the smallest to one above the queue pool's cap.
    /// </summary>
    [Theory]
    [InlineData(64)]
    [InlineData(16)]
    [InlineData(100)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(2048)]
    public void Scripted_Application_Moves_Keep_The_Promises(int ring) => RunScripts(400, ring, Moves.CheckSettled);

    /// <summary>GUARD: the same with seven ordered channels (one LZ4, one request/response) and with eight group channels.</summary>
    [Theory]
    [InlineData(64, false)]
    [InlineData(8, false)]
    [InlineData(64, true)]
    [InlineData(8, true)]
    public void Scripted_Application_Moves_Keep_The_Promises_With_Many_Reliable_Channels(int ring, bool groups) =>
        RunScripts(200, ring, Moves.CheckSettled, groups ? GroupTables.Main : OrderedTables.Main);

    /// <summary>GUARD: handlers that throw (the exception leaves Poll) lose no message, leak no credit and stall nothing.</summary>
    [Fact]
    public void Scripted_Application_Moves_With_Throwing_Handlers() => RunScripts(400, 64, Moves.Throwing);

    /// <summary>
    /// FINDING C again, found by the script: with handlers that register the handler of another channel, an ordered
    /// channel comes out of order.
    /// </summary>
    [Fact]
    public void Scripted_Application_Moves_With_Nested_Registration() => RunScripts(60, 64, Moves.NestedRegister);

    /// <summary>FINDING D again, found by the script: with handlers that call Drain, an ordered channel comes out of order.</summary>
    [Fact]
    public void Scripted_Application_Moves_With_Nested_Drains() => RunScripts(60, 64, Moves.NestedDrain);
}
