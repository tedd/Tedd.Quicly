using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The group engine's remaining paths: the <see cref="QuiclyPeer.FlushAsync"/> watermark over groups whose start is still
/// unconfirmed, in-place reconnect, a segment arena too small for a whole group, immediate sends, the send cap, the entry-table
/// reserve, a channel whose groups all wait, receive back-pressure from the budget, cancelling inside a group, closing with
/// carriers in flight, and the transport-thread paths that peer input can never reach.
/// </summary>
public class GroupEngineTests
{
    private static readonly ChannelTable Table = GroupTables.Main;

    [Fact]
    public async Task FlushAsync_Waits_For_A_Group_Whose_Start_Is_Still_Unconfirmed()
    {
        // One stream of credit: the second group's start is refused and comes back, so it still counts as queued.
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 }, table: GroupTables.FourGroups,
            serverTable: GroupTables.OneStream);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        Assert.True(h.Client.SendCopy(new SendHeader(5), [1]).IsAdmitted);
        h.Client.Flush();
        h.Network.Advance(2_000);
        h.Client.Poll();

        Assert.True(h.Client.SendCopy(new SendHeader(5), [2]).IsAdmitted);
        ValueTask flush = h.Client.FlushAsync();
        Assert.False(flush.IsCompleted);
        Assert.True(h.RunUntil(() => flush.IsCompleted), "FlushAsync never completed");
        await flush;
        Assert.True(h.RunUntil(() => got.Count == 2));

        // Nothing is left queued, so a later FlushAsync completes synchronously.
        ValueTask again = h.Client.FlushAsync();
        Assert.True(again.IsCompleted);
        await again;
    }

    [Fact]
    public void A_Reconnect_Drops_The_Lost_Connections_Groups()
    {
        using SessionHarness h = new(table: Table, client: QuietOptions.Apply, server: QuietOptions.Apply);
        QuiclyPeer oldServer = h.Server!;
        for (int i = 0; i < 40; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 200)).IsAdmitted);
        }

        h.Client.Flush();
        Assert.True(GroupKit.Groups(h.Client, 5) > 0);

        // The link dies without a QUICLY Close, as a lost connection does.
        oldServer.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));

        h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.Equal(PeerState.Reconnecting, h.Client.State);

        // The lost connection's groups, streams and bookkeeping are gone.
        Assert.Equal(0, GroupKit.Groups(h.Client, 5));
        Assert.Equal(0, GroupKit.Stats(h.Client, 5).QueuedMessages);
        Assert.Equal(0, GroupKit.Stats(h.Client, 5).InFlightMessages);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();

        // The channel works over the new transport.
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        Assert.True(h.Client.SendCopy(new SendHeader(5), [9]).IsAdmitted);
        Assert.True(h.RunUntil(() => got.Count == 1));
        Assert.Equal(new byte[] { 9 }, got[0].Payload);
    }

    [Fact]
    public void A_Tiny_Segment_Arena_Still_Delivers_Every_Group()
    {
        // Eight segments: a carrier takes three messages with the preamble, and the arena stays full while it is in flight,
        // so a group drains a carrier per round trip.
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 }, table: Table, client: o => o.SegmentArenaCapacity = 8);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        for (int i = 0; i < 60; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 30)).IsAdmitted);
        }

        Assert.True(h.RunUntil(() => got.Count == 60));
        for (int i = 0; i < 60; i++)
        {
            Assert.Equal(DatagramKit.Payload(i, 30), got[i].Payload);
        }
    }

    [Fact]
    public void An_Immediate_Group_Send_Opens_Its_Stream_Inside_The_Send()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.GroupMinInterval = TimeSpan.FromSeconds(1);
        });
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        Assert.True(h.Client.SendCopy(new SendHeader(5), [0]).IsAdmitted);
        h.Client.Flush();

        // The churn bound holds the next group back...
        Assert.True(h.Client.SendCopy(new SendHeader(5), [1]).IsAdmitted);
        h.Client.Flush();
        long held = DatagramKit.Statistics(h.Client).StreamSends;
        h.Network.Advance(1_000);
        h.Client.Flush();
        Assert.Equal(held, DatagramKit.Statistics(h.Client).StreamSends);

        // ... but an immediate message seals its group and runs the pass inside the send (PROTOCOL.md §4.5).
        Assert.True(h.Client.SendCopy(new SendHeader(5), [2], SendOptions.Immediate).IsAdmitted);
        Assert.Equal(held + 1, DatagramKit.Statistics(h.Client).StreamSends);
        Assert.True(h.RunUntil(() => got.Count == 3));
        Assert.Equal(new byte[] { 0 }, got[0].Payload);
        Assert.Equal(new byte[] { 1 }, got[1].Payload);
        Assert.Equal(new byte[] { 2 }, got[2].Payload);
    }

    [Fact]
    public void The_Send_Cap_Hands_A_Group_Over_In_Pieces()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.MaxSendBytesPerSecond = 20_000;
            o.GroupMinInterval = TimeSpan.Zero;
        });
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        for (int i = 0; i < 40; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 1_000)).IsAdmitted);
        }

        // 40 KB through a 20 KB/s cap: the pass stops mid-group and the rest follows as the bucket refills.
        Assert.True(h.RunUntil(() => got.Count == 40, 10_000_000));
        Assert.Equal(40, GroupKit.Stats(h.Client, 5).Sent);
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal(DatagramKit.Payload(i, 1_000), got[i].Payload);
        }
    }

    [Fact]
    public void A_Full_Send_Table_Answers_QueueFull_But_Keeps_Entries_For_The_Carriers()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            o.SendTableCapacity = 16;
            DatagramKit.Quiet(o);
        });
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(h.Client).SendEntriesInUse == 0));
        int admitted = 0;
        SendResult result;
        while ((result = h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(admitted, 10))).IsAdmitted)
        {
            admitted++;
        }

        // The reserve (one entry per group channel plus one) keeps room for the carriers that drain the groups.
        Assert.Equal(SendStatus.QueueFull, result.Status);
        Assert.InRange(admitted, 4, 8);
        Assert.True(GroupKit.Stats(h.Client, 5).QueueFull > 0);
        Assert.True(h.RunUntil(() => got.Count == admitted));
    }

    [Fact]
    public void A_Channel_Whose_Groups_All_Wait_For_Credit_Answers_QueueFull()
    {
        // One stream of credit and a long delay: groups pile up until the engine has no group record left.
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 50_000 }, table: GroupTables.FourGroups,
            serverTable: GroupTables.OneStream, client: o =>
            {
                DatagramKit.Quiet(o);
                o.GroupMinInterval = TimeSpan.Zero;
            });
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        int admitted = 0;
        SendStatus status = SendStatus.Admitted;
        for (int i = 0; i < 60 && status == SendStatus.Admitted; i++)
        {
            status = h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 10)).Status;
            if (status == SendStatus.Admitted)
            {
                admitted++;

                // One group per message, so every group record is taken.
                h.Client.Flush();
            }
        }

        Assert.Equal(SendStatus.QueueFull, status);
        Assert.InRange(admitted, 8, 30);

        // Every group that was admitted still arrives: a group waits, it never fails (PROTOCOL.md §3.2).
        Assert.True(h.RunUntil(() => got.Count == admitted, 30_000_000), $"{got.Count} of {admitted} arrived");
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    [Fact]
    public void An_Exhausted_Receive_Budget_Holds_A_Group_Back_Until_Leases_Are_Released()
    {
        using SessionHarness h = new(table: Table, server: o => o.ReceiveBudgetBytes = 16 * 1024);
        List<ReceiveLease> kept = [];
        List<byte[]> payloads = [];
        h.Server!.RegisterHandler(5, (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            payloads.Add(payload.ToArray());
            kept.Add(peer.Retain(in header));
        });
        for (int i = 0; i < 8; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 4_000)).IsAdmitted);
        }

        h.Run(50_000);

        // 4 000-byte payloads take 4 KiB leases: four fill the budget and the fifth holds the group back.
        Assert.Equal(4, payloads.Count);
        Assert.True(DatagramKit.Statistics(h.Server).StreamReceivePends > 0);
        Assert.True(h.RunUntil(() =>
        {
            foreach (ReceiveLease lease in kept)
            {
                h.Server.Release(in lease);
            }

            kept.Clear();
            return payloads.Count == 8;
        }));
        for (int i = 0; i < 8; i++)
        {
            Assert.Equal(DatagramKit.Payload(i, 4_000), payloads[i]);
        }

        Assert.Equal(0, DatagramKit.Statistics(h.Server).ReceiveRingDrops);
    }

    [Fact]
    public void Cancelling_A_Message_Inside_A_Group_Keeps_The_Others()
    {
        using SessionHarness h = new(table: Table);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        SendResult first = h.Client.SendCopy(new SendHeader(5), [1], SendOptions.Tracked);
        SendResult middle = h.Client.SendCopy(new SendHeader(5), [2], SendOptions.Tracked);
        SendResult last = h.Client.SendCopy(new SendHeader(5), [3], SendOptions.Tracked);
        Assert.True(first.IsAdmitted && middle.IsAdmitted && last.IsAdmitted);

        // The middle of the group's FIFO: its neighbours stay linked.
        Assert.True(h.Client.TryCancel(middle.Token));
        Assert.Equal(2, GroupKit.Stats(h.Client, 5).QueuedMessages);
        Assert.True(h.RunUntil(() => got.Count == 2));
        Assert.Equal(new byte[] { 1 }, got[0].Payload);
        Assert.Equal(new byte[] { 3 }, got[1].Payload);
        Assert.Equal(DeliveryStatus.Canceled, h.Client.GetDeliveryStatus(middle.Token));
        Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(first.Token));
        Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(last.Token));

        // A message already handed to a carrier cannot be cancelled any more.
        Assert.False(h.Client.TryCancel(first.Token));
    }

    [Fact]
    public void Closing_While_A_Groups_Carriers_Are_In_Flight_Completes_Them_Disconnected()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 20_000 }, table: Table, client: GroupKit.Roomy);
        List<SendToken> tokens = [];
        for (int i = 0; i < 40; i++)
        {
            SendResult result = h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 1_024), SendOptions.Tracked);
            Assert.True(result.IsAdmitted);
            tokens.Add(result.Token);
        }

        h.Client.Flush();
        h.Client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.True(tokens.TrueForAll(token => h.Client.GetDeliveryStatus(token) != DeliveryStatus.Pending));
        Assert.Contains(DeliveryStatus.Disconnected, tokens.Select(token => h.Client.GetDeliveryStatus(token)));
    }

    [Fact]
    public void The_Engine_Refuses_Transport_Input_That_Cannot_Reach_It_From_The_Wire()
    {
        // The peer resolves channels and roles before an engine sees a stream or a datagram, so these are the defensive
        // paths: they are driven directly, the way the placeholder engine's are.
        using SessionHarness h = new(table: Table);
        GroupStreamEngine engine = GroupKit.Engine(h.Client);

        MessageHeader header = default;
        header.Channel = 5;
        engine.OnDatagram(in header, [1], 0);
        Assert.Equal(1, GroupKit.Stats(h.Client, 5).Dropped);

        // Channel 2 is a datagram channel: not this engine's.
        StreamAccept accept = engine.OnStreamOpened(new TransportStreamId(9, 1), 2, 0);
        Assert.Equal(StreamAcceptAction.Reset, accept.Action);
        Assert.Equal(QuiclyErrorCode.UnsupportedChannel, accept.ResetCode);

        long cookie = 0;
        scoped StreamMessageContext context = default;
        context.Cookie = ref cookie;
        context.Phase = StreamMessagePhase.BulkHeader;
        Assert.Equal(StreamConsumeAction.ResetStream, engine.OnStreamMessage(ref context).Action);

        // Payload bytes the parser would never produce: a Chunk with no staged message (no Start reserved a lease), and one
        // longer than the frame header promised. Neither may be written through the record's lease pointer.
        TransportStreamId stream = new(11, 1);
        StreamAccept opened = engine.OnStreamOpened(stream, 5, 0);
        Assert.True(opened.Accepted);
        long record = opened.Cookie;
        scoped StreamMessageContext chunk = default;
        chunk.Cookie = ref record;
        chunk.Channel = 5;
        chunk.ChannelIndex = h.Client.Core.ChannelIndexOf(5);
        chunk.Phase = StreamMessagePhase.Chunk;
        chunk.Chunk = [1, 2];
        Assert.Equal(StreamConsumeAction.ResetStream, engine.OnStreamMessage(ref chunk).Action);
        Assert.Equal(QuiclyErrorCode.ProtocolViolation, engine.OnStreamMessage(ref chunk).Code);

        chunk.Phase = StreamMessagePhase.Start;
        chunk.Header = new StreamMessageHeader { Length = 1 };
        Assert.Equal(StreamConsumeAction.Continue, engine.OnStreamMessage(ref chunk).Action);
        chunk.Phase = StreamMessagePhase.Chunk;
        Assert.Equal(StreamConsumeAction.ResetStream, engine.OnStreamMessage(ref chunk).Action);

        // One byte does fit, and the reset that follows gives the staging lease and the reservation back.
        chunk.Chunk = [1];
        Assert.Equal(StreamConsumeAction.Continue, engine.OnStreamMessage(ref chunk).Action);
        // A peer stream's close names its record (the cookie). A cookie that does not is not trusted: the record is found by
        // the stream's id, and a close for a stream the engine does not hold releases nothing.
        engine.OnPeerStreamClosed(new TransportStreamId(78, 3), record, aborted: true, 5);
        Assert.Equal(1, GroupKit.OpenPeerGroups(h.Client, 5));
        engine.OnPeerStreamClosed(stream, record + 100_000, aborted: true, (ulong)QuiclyErrorCode.ProtocolViolation);
        Assert.Equal(0, GroupKit.OpenPeerGroups(h.Client, 5));
        Assert.Equal(0, DatagramKit.Statistics(h.Client).ReceiveBytesOutstanding);
        engine.OnPeerStreamClosed(stream, record, aborted: true, (ulong)QuiclyErrorCode.ProtocolViolation);
        Assert.Equal(0, GroupKit.OpenPeerGroups(h.Client, 5));

        // Neither an invalid stream id nor one this engine never opened matches anything.
        engine.OnStreamClosed(default, aborted: false, 0);
        engine.OnStreamClosed(new TransportStreamId(77, 3), aborted: true, 5);

        // A context that is not one of this engine's is ignored; a start that failed for any other reason is a notice of a
        // stream that never was, and an old serial makes the game thread drop it.
        engine.OnStreamStarted(new TransportStreamId(5, 1), PeerCore.ControlStreamContext, TransportStatus.Success);
        engine.OnStreamStarted(
            new TransportStreamId(5, 1),
            PeerCore.MakeEngineStreamContext(ChannelMode.ReliableUnordered, 0, 0x7F_FFFF),
            TransportStatus.InvalidState);
        h.Client.Flush();
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(0, DatagramKit.Statistics(h.Client).CallbackFaults);
    }

    [Fact]
    public void A_Resumed_Epoch_Restarts_The_Group_Id_Counters()
    {
        using SessionHarness h = new(table: Table);
        Assert.True(h.Client.SendCopy(new SendHeader(5), [1]).IsAdmitted);
        h.Client.Flush();
        Assert.Equal(1ul, GroupKit.GroupsFormed(h.Client, 5));
        GroupStreamEngine engine = GroupKit.Engine(h.Client);

        // Group ids are per-channel counters: a fresh epoch keeps counting, a resumed one restarts (PROTOCOL.md §1, §4.1).
        engine.OnEpochReset(resumed: false);
        Assert.Equal(1ul, GroupKit.GroupsFormed(h.Client, 5));
        engine.OnEpochReset(resumed: true);
        Assert.Equal(0ul, GroupKit.GroupsFormed(h.Client, 5));
    }
}
