using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The group stream's lifecycle and limits (PROTOCOL.md §3.2, §6, §7; docs/design/session-layer.md §7.5): a group that waits
/// for stream credit and goes out on a new stream, refusals (MsQuic-style asynchronous and synchronous), the receive-side
/// <see cref="ChannelDefinition.MaxGroups"/> limit, a malformed group, a stalled group, the group interval, back-pressure,
/// expiry, cancellation, close, and a group the peer stops.
/// </summary>
public class GroupStreamTests
{
    private static readonly ChannelTable Table = GroupTables.Main;

    [Fact]
    public void A_Refused_Group_Waits_For_Credit_And_Goes_Out_On_A_New_Stream()
    {
        // The server's table grants one unidirectional stream (MaxGroups 1) while the client's allows four groups; MaxGroups
        // is not part of the table hash, so the two tables still match.
        Assert.Equal(GroupTables.OneStream.Hash, GroupTables.FourGroups.Hash);
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 }, table: GroupTables.FourGroups,
            serverTable: GroupTables.OneStream);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        List<SendToken> tokens = [];
        for (int i = 0; i < 3; i++)
        {
            SendResult result = h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 40), SendOptions.Tracked);
            Assert.True(result.IsAdmitted);
            tokens.Add(result.Token);

            // One group per flush, with the group interval between them.
            h.Client.Flush();
            h.Network.Advance(2_000);
            h.Client.Poll();
        }

        Assert.Equal(3ul, GroupKit.GroupsFormed(h.Client, 5));
        Assert.True(GroupKit.Engine(h.Client).StreamsRefused >= 1, "the peer's stream limit refused no start");
        Assert.True(h.RunUntil(() => got.Count == 3), $"{got.Count} of 3 messages arrived");
        Assert.All(tokens, token => Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(token)));
        Assert.Equal(0, GroupKit.Stats(h.Client, 5).Expired);
        Assert.Equal(3, GroupKit.Stats(h.Client, 5).Sent);
        Assert.True(h.RunUntil(() => GroupKit.Groups(h.Client, 5) == 0), "a group was left behind");
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    [Fact]
    public void A_Start_Refused_Asynchronously_Is_Sent_Again_On_A_New_Stream()
    {
        AsyncRefusalConnector? refusal = null;
        using SessionHarness h = new(table: Table, connector: c => refusal = new AsyncRefusalConnector(c));
        AsyncRefusalTransport transport = refusal!.Transport!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        transport.RefuseStarts = 1;
        List<SendToken> tokens = [];
        for (int i = 0; i < 4; i++)
        {
            SendResult result = h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 30), SendOptions.Tracked);
            Assert.True(result.IsAdmitted);
            tokens.Add(result.Token);
        }

        h.Client.Flush();
        Assert.Equal(1, transport.Refused);
        Assert.Equal(GroupStreamEngine.GroupPhase.Starting, Assert.Single(GroupKit.Phases(h.Client, 5)));
        Assert.Equal(4, GroupKit.Stats(h.Client, 5).InFlightMessages);

        // MsQuic: OnStreamStarted(StreamLimitReached), then the canceled send, then the shutdown.
        transport.Deliver();
        h.Client.Poll();
        Assert.Equal(GroupStreamEngine.GroupPhase.Waiting, Assert.Single(GroupKit.Phases(h.Client, 5)));
        Assert.Equal(1, GroupKit.Engine(h.Client).StreamsRefused);
        Assert.Equal(4, GroupKit.Stats(h.Client, 5).QueuedMessages);
        Assert.Equal(0, GroupKit.Stats(h.Client, 5).InFlightMessages);
        Assert.Equal(0, GroupKit.Stats(h.Client, 5).Sent);
        Assert.All(tokens, token => Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(token)));

        // The group waits: it never fails, and nothing moves until the peer grants credit again.
        h.Run(20_000);
        Assert.Empty(got);
        transport.GrantCredit();
        Assert.True(h.RunUntil(() => got.Count == 4));
        Assert.True(h.RunUntil(() => tokens.TrueForAll(token => h.Client.GetDeliveryStatus(token) == DeliveryStatus.Delivered)));
        for (int i = 0; i < 4; i++)
        {
            Assert.Equal(DatagramKit.Payload(i, 30), got[i].Payload);
        }
    }

    [Fact]
    public void A_Start_Refused_Synchronously_Waits_For_Credit_And_Uses_A_New_Stream()
    {
        AsyncRefusalConnector? refusal = null;
        using SessionHarness h = new(table: Table, connector: c => refusal = new AsyncRefusalConnector(c));
        AsyncRefusalTransport transport = refusal!.Transport!;
        transport.Synchronous = true;
        transport.RefuseStarts = 1;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        for (int i = 0; i < 3; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 30)).IsAdmitted);
        }

        // The refused stream was released at once (it never started) and the messages stayed in their group.
        h.Client.Flush();
        Assert.Equal(GroupStreamEngine.GroupPhase.Waiting, Assert.Single(GroupKit.Phases(h.Client, 5)));
        Assert.Equal(1, GroupKit.Engine(h.Client).StreamsRefused);
        Assert.Equal(3, GroupKit.Stats(h.Client, 5).QueuedMessages);
        h.Run(20_000);
        Assert.Empty(got);
        transport.GrantCredit();
        Assert.True(h.RunUntil(() => got.Count == 3));
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(DatagramKit.Payload(i, 30), got[i].Payload);
        }
    }

    [Fact]
    public void The_Receive_Limit_Resets_Peer_Group_Streams_Beyond_MaxGroups()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit());
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(9, Handlers.Collect(got));

        // Channel 9 accepts two concurrent peer groups; each stream here holds a message whose payload is incomplete.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(9, 1, 4, [1]), out TransportStreamId a));
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(9, 2, 4, [2]), out TransportStreamId b));
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(h.Server, 9) == 2));
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(9, 3, 4, [3]), out TransportStreamId excess));
        ulong code = 0;
        Assert.True(h.RunUntil(() => Aborted(h.Raw, excess, out code)));
        Assert.Equal((ulong)QuiclyErrorCode.LimitExceeded, code);
        Assert.Equal(PeerState.Connected, h.Server.State);
        Assert.Equal(2, GroupKit.OpenPeerGroups(h.Server, 9));

        // The accepted groups finish, their slots come back, and a later group is accepted again.
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.Raw.Transport, a, [2, 3, 4], TransportSendFlags.Fin));
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.Raw.Transport, b, [3, 4, 5], TransportSendFlags.Fin));
        Assert.True(h.RunUntil(() => got.Count == 2 && GroupKit.OpenPeerGroups(h.Server, 9) == 0));
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, got[0].Payload);
        Assert.Equal(new byte[] { 2, 3, 4, 5 }, got[1].Payload);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(9, 4, [9, 9, 9, 9]), out _, fin: true));
        Assert.True(h.RunUntil(() => got.Count == 3));
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Malformed_Group_Is_Reset_While_Other_Groups_Keep_Flowing()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit());
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(5, 1, [1, 2]), out _, fin: true));
        Assert.True(h.RunUntil(() => got.Count == 1));

        // A non-minimal length varint: the group is reset with ProtocolViolation and the connection survives (PROTOCOL.md §6).
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni([0x05, 0x02, 0x40, 0x01, 0xAA], out TransportStreamId bad));
        ulong code = 0;
        Assert.True(h.RunUntil(() => Aborted(h.Raw, bad, out code)));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, code);
        Assert.Equal(PeerState.Connected, h.Server.State);

        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(5, 3, [3, 4, 5]), out _, fin: true));
        Assert.True(h.RunUntil(() => got.Count == 2));
        Assert.Equal(new byte[] { 1, 2 }, got[0].Payload);
        Assert.Equal(new byte[] { 3, 4, 5 }, got[1].Payload);
        Assert.Equal(1, h.Statistics().StreamsReset);
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
        Assert.Equal(0, GroupKit.OpenPeerGroups(h.Server, 5));
    }

    [Fact]
    public void A_Group_That_Stalls_Mid_Message_Is_Reset_By_The_Idle_Timeout()
    {
        using ServerHarness h = new(table: Table, server: o =>
        {
            DatagramKit.Quiet(o);
            o.StreamIdleTimeout = TimeSpan.FromMilliseconds(50);
        });
        Assert.True(h.Admit());

        // A frame header that promises 2 000 bytes and three of them: the staging lease and the ring slot are pinned.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(5, 1, 2_000, [1, 2, 3]), out TransportStreamId stalled));
        Assert.True(h.RunUntil(() => h.Statistics().ReceiveBytesOutstanding > 0));
        ulong code = 0;
        Assert.True(h.RunUntil(() => Aborted(h.Raw, stalled, out code)));
        Assert.Equal((ulong)QuiclyErrorCode.Timeout, code);
        Assert.Equal(1, h.Statistics().StreamIdleTimeouts);
        Assert.True(h.RunUntil(() => h.Statistics().ReceiveBytesOutstanding == 0 && GroupKit.OpenPeerGroups(h.Server!, 5) == 0));
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void The_Group_Interval_Bounds_Stream_Churn()
    {
        using SessionHarness slow = new(table: Table, client: o => o.GroupMinInterval = TimeSpan.FromMilliseconds(10));
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        slow.Server!.RegisterHandler(5, Handlers.Collect(got));
        for (int i = 0; i < 20; i++)
        {
            Assert.True(slow.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 20)).IsAdmitted);
            slow.Client.Flush();
            slow.Network.Advance(1_000);
            slow.Client.Poll();
        }

        // Twenty flushes over 20 ms, one stream per 10 ms: a handful of groups instead of twenty.
        Assert.InRange(GroupKit.GroupsFormed(slow.Client, 5), 2ul, 5ul);
        Assert.True(slow.RunUntil(() => got.Count == 20));

        // Without the bound every flush forms its own group.
        using SessionHarness prompt = new(table: Table, client: GroupKit.Prompt);
        List<(ReceiveHeader Header, byte[] Payload)> all = [];
        prompt.Server!.RegisterHandler(5, Handlers.Collect(all));
        for (int i = 0; i < 20; i++)
        {
            Assert.True(prompt.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 20)).IsAdmitted);
            prompt.Client.Flush();
            prompt.Network.Advance(1_000);
            prompt.Client.Poll();
        }

        Assert.True(GroupKit.GroupsFormed(prompt.Client, 5) >= 15, $"{GroupKit.GroupsFormed(prompt.Client, 5)} groups");
        Assert.True(prompt.RunUntil(() => all.Count == 20));
    }

    [Fact]
    public void A_Message_The_Receiver_Could_Never_Buffer_Resets_Only_Its_Group()
    {
        using SessionHarness h = new(table: Table, server: o => o.ReceiveBudgetBytes = 8 * 1024);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        SendToken big = h.Client.SendCopy(new SendHeader(5), new byte[10_000], SendOptions.Tracked).Token;
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(big) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Failed, h.Client.GetDeliveryStatus(big));
        Assert.Equal(1, GroupKit.Stats(h.Server, 5).ReceiveTooLarge);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server.State);

        // The channel is not closed: the next group is delivered.
        Assert.True(h.Client.SendCopy(new SendHeader(5), [1, 2, 3]).IsAdmitted);
        Assert.True(h.RunUntil(() => got.Count == 1));
        Assert.Equal(new byte[] { 1, 2, 3 }, got[0].Payload);
    }

    [Fact]
    public void A_Group_The_Peer_Stops_Fails_Without_Closing_The_Channel()
    {
        using ClientHarness h = new(link: new LinkOptions { DelayMicros = 5_000, PeerUnidiStreams = 8 }, table: Table);
        Assert.True(h.Accept());
        SendToken token = h.Client.SendCopy(new SendHeader(5), [1], SendOptions.Tracked).Token;
        h.Client.Flush();
        Assert.True(h.RunUntil(() => h.Sink.CountOf(RecordedEventKind.PeerStreamStarted) >= 2));
        TransportStreamId group = h.Sink.OfKind(RecordedEventKind.PeerStreamStarted)[1].StreamId;
        h.ServerTransport!.AbortStream(group, 77, StreamAbortDirection.Receive);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(token) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Failed, h.Client.GetDeliveryStatus(token));

        // Only that group failed: the channel still admits and sends (PROTOCOL.md §3.2, groups are independent).
        Assert.Equal(PeerState.Connected, h.Client.State);
        SendToken next = h.Client.SendCopy(new SendHeader(5), [2], SendOptions.Tracked).Token;
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(next) == DeliveryStatus.Delivered));
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    [Fact]
    public void The_Group_Queue_Limit_Counts_Bytes_In_Flight()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 20_000 }, table: Table, client: GroupKit.Roomy, server: GroupKit.Roomy);
        byte[] block = new byte[16 * 1024];
        int admitted = 0;
        while (h.Client.SendCopy(new SendHeader(10), block).IsAdmitted)
        {
            admitted++;
        }

        // 64 KiB limit: four 16 KiB messages, queued or in flight, until the peer acknowledges them.
        Assert.Equal(4, admitted);
        h.Client.Flush();
        Assert.Equal(SendStatus.QueueFull, h.Client.SendCopy(new SendHeader(10), block).Status);
        Assert.True(h.RunUntil(() => GroupKit.Stats(h.Client, 10).InFlightMessages == 0 && GroupKit.Stats(h.Client, 10).QueuedMessages == 0));
        Assert.True(h.Client.SendCopy(new SendHeader(10), block).IsAdmitted);
        Assert.True(GroupKit.Stats(h.Client, 10).QueueFull > 0);
    }

    [Fact]
    public void The_Sender_Refuses_Group_Messages_Above_The_Limit()
    {
        using SessionHarness h = new(table: Table);
        Assert.Equal(SendStatus.TooLarge, h.Client.SendCopy(new SendHeader(12), new byte[1_001]).Status);
        Assert.True(h.Client.SendCopy(new SendHeader(12), new byte[1_000]).IsAdmitted);
        Assert.Equal(1, GroupKit.Stats(h.Client, 12).TooLarge);
    }

    [Fact]
    public void A_Queued_Group_Message_Can_Be_Canceled()
    {
        using SessionHarness h = new(table: Table);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        SendResult result = h.Client.SendCopy(new SendHeader(5), [1, 2, 3], SendOptions.Tracked);
        Assert.True(h.Client.TryCancel(result.Token));
        Assert.Equal(0, GroupKit.Stats(h.Client, 5).QueuedMessages);
        h.Client.Flush();
        Assert.Equal(DeliveryStatus.Canceled, h.Client.GetDeliveryStatus(result.Token));
        Assert.False(h.Client.TryCancel(result.Token));

        // The empty group never opened a stream; the next message still goes out.
        Assert.True(h.Client.SendCopy(new SendHeader(5), [4]).IsAdmitted);
        Assert.True(h.RunUntil(() => got.Count == 1));
        Assert.Equal(new byte[] { 4 }, got[0].Payload);
    }

    [Fact]
    public void Messages_That_Expire_Before_Their_Group_Opens_Are_Dropped()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.GroupMinInterval = TimeSpan.FromMilliseconds(50);
        });
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        Assert.True(h.Client.SendCopy(new SendHeader(5), [0]).IsAdmitted);
        h.Client.Flush();

        // The next group waits for the interval, which outlasts this message's expiry (PROTOCOL.md §4.5).
        SendToken expiring = h.Client.SendCopy(new SendHeader(5), [1], new SendOptions { Track = true, ExpiryMicros = 5_000 }).Token;
        h.Network.Advance(20_000);
        h.Client.Flush();
        Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(expiring));
        h.Network.Advance(40_000);
        h.Client.Flush();
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(expiring) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Expired, h.Client.GetDeliveryStatus(expiring));
        Assert.Equal(1, GroupKit.Stats(h.Client, 5).Expired);
        Assert.True(h.RunUntil(() => got.Count == 1));
        Assert.Equal(new byte[] { 0 }, got[0].Payload);
        h.Run(100_000);
        Assert.Single(got);
    }

    [Fact]
    public void Closing_Completes_Queued_Group_Messages_Disconnected()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000 }, table: GroupTables.FourGroups,
            serverTable: GroupTables.OneStream);
        Assert.True(h.Client.SendCopy(new SendHeader(5), [1]).IsAdmitted);
        h.Client.Flush();
        h.Network.Advance(2_000);
        h.Client.Poll();

        // The only stream is taken, so this group waits for credit.
        SendToken queued = h.Client.SendCopy(new SendHeader(5), [2], SendOptions.Tracked).Token;
        h.Client.Flush();
        h.Client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.Equal(DeliveryStatus.Disconnected, h.Client.GetDeliveryStatus(queued));
    }

    [Fact]
    public void A_Full_Receive_Ring_Holds_A_Group_Back_Until_Poll_Makes_Room()
    {
        using SessionHarness h = new(table: Table, server: o => o.ReceiveRingCapacity = 4);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        for (int i = 0; i < 40; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 50)).IsAdmitted);
        }

        Assert.True(h.RunUntil(() => got.Count == 40));
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal(DatagramKit.Payload(i, 50), got[i].Payload);
        }

        Assert.True(DatagramKit.Statistics(h.Server).StreamReceivePends > 0);
        Assert.Equal(0, DatagramKit.Statistics(h.Server).ReceiveRingDrops);
    }

    private static bool Aborted(RawClient raw, TransportStreamId id, out ulong code)
    {
        foreach (RecordedEvent recorded in raw.Sink.OfKind(RecordedEventKind.StreamAborted))
        {
            if (recorded.StreamId == id)
            {
                code = recorded.ErrorCode;
                return true;
            }
        }

        code = 0;
        return false;
    }
}
