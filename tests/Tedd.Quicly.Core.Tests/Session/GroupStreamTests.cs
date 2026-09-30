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
/// for stream credit and goes out on a new stream, refusals (MsQuic-style asynchronous and synchronous), a receiver that
/// holds more than <see cref="ChannelDefinition.MaxGroups"/> peer streams open because it fell behind (none is reset; the
/// connection's stream limit is the bound), a malformed group, a stalled group, the group interval, back-pressure,
/// expiry, cancellation, close, and a group the peer stops.
/// </summary>
public class GroupStreamTests
{
    private static readonly ChannelTable Table = GroupTables.Main;
    private static readonly byte[] ResumeToken = [9, 8, 7, 6];

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

        // The last group's bytes are acknowledged one one-way delay after they were dispatched.
        Assert.True(h.RunUntil(() => tokens.TrueForAll(token => h.Client.GetDeliveryStatus(token) == DeliveryStatus.Delivered)));
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

    /// <summary>
    /// A start that fails for a reason other than the peer's stream limit (docs/design/session-layer.md §7.5): the open
    /// succeeded, so the stream is released, but nothing refused the group — it must not be parked on the current credit
    /// generation, because no <c>OnStreamsAvailable</c> is coming (the stream never started, so no credit was ever taken). The
    /// next pass opens a new stream for it.
    /// </summary>
    [Fact]
    public void A_Start_That_Fails_For_Another_Reason_Is_Retried_At_The_Next_Pass()
    {
        StartSendFailureConnector? failing = null;
        using SessionHarness h = new(table: Table, client: GroupKit.Prompt, server: GroupKit.Prompt,
            connector: c => failing = new StartSendFailureConnector(c));
        StartSendFailureTransport transport = failing!.Transport!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        transport.FailStarts = 1;
        Assert.True(h.Client.SendCopy(new SendHeader(5), [7, 8]).IsAdmitted);

        h.Client.Flush();
        Assert.Equal(1, transport.Failed);

        // The group kept its messages and its place, and no stream limit was involved.
        Assert.Equal(GroupStreamEngine.GroupPhase.Waiting, Assert.Single(GroupKit.Phases(h.Client, 5)));
        Assert.Equal(0, GroupKit.Engine(h.Client).StreamsRefused);
        Assert.Equal(1, GroupKit.Stats(h.Client, 5).QueuedMessages);

        Assert.True(h.RunUntil(() => got.Count == 1), "the group never went out after a send that failed with the start");
        Assert.Equal(new byte[] { 7, 8 }, got[0].Payload);
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    [Fact]
    public void A_Receiver_That_Falls_Behind_Loses_No_Group()
    {
        // The sender keeps MaxGroups (8) by its own count, and it counts a stream as closed once its data and FIN are
        // acknowledged. A receiver that is late with its Poll still has those streams open, so the sender's ninth stream
        // arrives while eight are open here. Resetting it (the old receive-side MaxGroups rule) destroyed a group whose
        // messages the sender had already completed as Delivered: silent loss on a reliable channel.
        using SessionHarness h = new(table: TestTables.Plumbing, client: GroupKit.Prompt, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 64;
        });
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        server.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => got.Add(BitConverter.ToInt32(payload)));

        // A hitch: the server is not polled for 80 ms while the client sends twenty groups of sixteen messages.
        List<SendToken> tokens = SendGroupsToALateReceiver(h, h.Client, 11, 320);
        int openWhileBehind = GroupKit.OpenPeerGroups(server, 11);
        Assert.True(h.RunUntil(() => got.Count == 320, 2_000_000),
            $"received {got.Count} of 320 messages; the server reset {DatagramKit.Statistics(server).StreamsReset} streams, and the sender "
            + $"reported {tokens.Count(token => h.Client.GetDeliveryStatus(token) == DeliveryStatus.Delivered)} of them Delivered");
        Assert.Equal(Enumerable.Range(0, 320), got.Order());
        Assert.Equal(0, DatagramKit.Statistics(server).StreamsReset);
        Assert.All(tokens, token => Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(token)));

        // The scenario was the one meant: the receiver held more streams of the channel open than its MaxGroups.
        Assert.True(openWhileBehind > 8, $"only {openWhileBehind} peer streams were open at the receiver while it was behind");
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(server, 11) == 0));
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Reconnect_Gives_Back_Every_Receive_Record_Of_A_Peer_That_Was_Past_MaxGroups()
    {
        // The client is the receiver here (only a client reconnects). Its connection is lost while it holds more than
        // MaxGroups peer streams open; the resumed connection starts with every receive record free and takes the same
        // load again.
        using SessionHarness h = new(connect: false, table: TestTables.Plumbing, server: GroupKit.Prompt, client: o =>
        {
            QuietOptions.Apply(o);
            o.ReceiveRingCapacity = 64;
        });
        h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
            AdmissionResult.Accept(ResumeToken, 4242, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer client = h.Client;
        QuiclyPeer oldServer = h.Server!;
        List<int> got = [];
        client.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => got.Add(BitConverter.ToInt32(payload)));

        SendGroupsToALateReceiver(h, oldServer, 11, 320);
        Assert.True(GroupKit.OpenPeerGroups(client, 11) > 8, "the scenario needs more than MaxGroups peer streams open at the receiver");

        // The link is lost; the client is still not polled, so Reconnect settles the lost connection itself.
        oldServer.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);
        for (int step = 0; step < 100 && !client.Core.IsTransportClosed; step++)
        {
            h.Network.Advance(1_000);
        }

        Assert.True(client.Core.IsTransportClosed);
        client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.Equal(0, GroupKit.OpenPeerGroups(client, 11));
        Assert.Equal(0, DatagramKit.Statistics(client).ReceiveBytesOutstanding);
        Assert.True(h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();
        Assert.Equal(2u, client.Epoch);

        // The same load on the resumed connection: nothing of the lost one is in the way.
        got.Clear();
        SendGroupsToALateReceiver(h, h.Server!, 11, 320);
        Assert.True(GroupKit.OpenPeerGroups(client, 11) > 8);
        Assert.True(h.RunUntil(() => got.Count == 320, 2_000_000), $"received {got.Count} of 320 messages after the reconnect");
        Assert.Equal(Enumerable.Range(0, 320), got.Order());
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(client, 11) == 0));
        Assert.Equal(0, DatagramKit.Statistics(client).ReceiveBytesOutstanding);
        Assert.Equal(0, DatagramKit.Statistics(client).StreamsReset);
    }

    [Fact]
    public void A_Reconnect_To_A_Transport_That_Grants_More_Streams_Makes_Room_Before_Its_Streams_Arrive()
    {
        // The first transport grants nothing by itself, so the session's own limit (9) is all the peer can open and all the
        // client keeps records for. The transport of the resumed connection grants 1 024 in its own configuration, as an
        // MsQuic client does by default: the session learns that when the transport connects — before any stream of the
        // connection exists — and a late client then holds what the server opens instead of resetting it.
        using SessionHarness h = new(connect: false, table: TestTables.Plumbing, server: GroupKit.Prompt, client: o =>
        {
            QuietOptions.Apply(o);
            o.ReceiveRingCapacity = 64;
        });
        h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
            AdmissionResult.Accept(ResumeToken, 4242, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer client = h.Client;
        QuiclyPeer oldServer = h.Server!;
        Assert.Equal(9, client.Core.PeerUnidirectionalStreamLimit);
        Assert.Equal(9, client.Core.PeerStreamCapacity);
        List<int> got = [];
        client.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => got.Add(BitConverter.ToInt32(payload)));

        oldServer.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);
        Assert.True(h.RunUntil(() => client.State == PeerState.Closed));
        SimulatedConnector wide = new(h.Network, new LinkOptions { PeerUnidiStreams = 1024 });
        client.Reconnect(wide, h.Listener.LocalEndPoint, "test", default);
        Assert.True(h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();
        Assert.Equal(1024, client.Core.PeerStreamCapacity);
        Assert.True(client.Core.PendedStreams.Capacity >= 1026, $"the pended-stream ring holds {client.Core.PendedStreams.Capacity}");

        SendGroupsToALateReceiver(h, h.Server!, 11, 640);
        int open = GroupKit.OpenPeerGroups(client, 11);
        Assert.True(h.RunUntil(() => got.Count == 640, 2_000_000),
            $"received {got.Count} of 640 messages; the client reset {DatagramKit.Statistics(client).StreamsReset} streams");
        Assert.Equal(Enumerable.Range(0, 640), got.Order());
        Assert.True(open > 9, $"only {open} peer streams were open at the client while it was behind");
        PeerStatistics statistics = DatagramKit.Statistics(client);
        Assert.Equal(0, statistics.StreamsReset);
        Assert.Equal(0, statistics.CallbackFaults);
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(client, 11) == 0));
        Assert.Equal(0, DatagramKit.Statistics(client).ReceiveBytesOutstanding);
    }

    [Fact]
    public void The_Peer_Stream_Capacity_Only_Grows_And_The_Ring_It_Replaces_Stays_Readable()
    {
        using SessionHarness h = new(table: TestTables.Plumbing);
        PeerCore core = h.Server!.Core;
        int limit = core.PeerUnidirectionalStreamLimit;
        Assert.Equal(limit, core.PeerStreamCapacity);
        SpscRing<TransportStreamId> first = core.PendedStreams;

        // A grant at or below the session's own limit changes nothing.
        core.SetTransportPeerStreams(0);
        core.SetTransportPeerStreams(limit);
        Assert.Equal(limit, core.PeerStreamCapacity);
        Assert.Same(first, core.PendedStreams);

        // A larger one replaces the ring; the old one is kept, because another thread may still be reading it.
        core.SetTransportPeerStreams(200);
        Assert.Equal(200, core.PeerStreamCapacity);
        Assert.NotSame(first, core.PendedStreams);
        Assert.True(core.PendedStreams.Capacity >= 202);
        Assert.True(first.IsEmpty);
        Assert.False(first.TryDequeue(out _));

        // The capacity never shrinks: a transport attached later with a smaller grant keeps what is there.
        SpscRing<TransportStreamId> second = core.PendedStreams;
        core.SetTransportPeerStreams(50);
        Assert.Equal(200, core.PeerStreamCapacity);
        Assert.Same(second, core.PendedStreams);

        // The session still works, and disposing it frees both rings.
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server.RegisterHandler(11, Handlers.Collect(got));
        Assert.True(h.Client.SendCopy(new SendHeader(11), [1, 2, 3]).IsAdmitted);
        Assert.True(h.RunUntil(() => got.Count == 1));
    }

    /// <summary>
    /// Sends <paramref name="count"/> numbered, tracked messages, one group per sixteen, while only the sender is pumped:
    /// the receiver is late with its Poll for 4 ms per group.
    /// </summary>
    private static List<SendToken> SendGroupsToALateReceiver(SimFixture h, QuiclyPeer sender, ushort channel, int count)
    {
        List<SendToken> tokens = [];
        for (int i = 0; i < count; i++)
        {
            SendResult result = sender.SendCopy(new SendHeader(channel), DatagramKit.Payload(i, 4), SendOptions.Tracked);
            Assert.True(result.IsAdmitted);
            tokens.Add(result.Token);
            if ((i & 15) == 15)
            {
                for (int step = 0; step < 4; step++)
                {
                    sender.Poll();
                    sender.Flush();
                    h.Network.Advance(1_000);
                }
            }
        }

        return tokens;
    }

    [Fact]
    public void Peer_Group_Streams_Beyond_MaxGroups_Are_Accepted()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit());
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(9, Handlers.Collect(got));

        // Channel 9 has MaxGroups 2: a bound on its sender. Three streams are open here at once, each holding a message
        // whose payload is incomplete, and none of them is reset.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(9, 1, 4, [1]), out TransportStreamId a));
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(9, 2, 4, [2]), out TransportStreamId b));
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(9, 3, 4, [3]), out TransportStreamId c));
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(h.Server, 9) == 3));
        h.Run(50_000);
        Assert.Equal(0, h.Raw.Sink.CountOf(RecordedEventKind.StreamAborted));
        Assert.Equal(0, h.Statistics().StreamsReset);
        Assert.Equal(PeerState.Connected, h.Server.State);

        // All three finish, their records come back, and a later group is accepted again.
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.Raw.Transport, a, [2, 3, 4], TransportSendFlags.Fin));
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.Raw.Transport, b, [3, 4, 5], TransportSendFlags.Fin));
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.Raw.Transport, c, [4, 5, 6], TransportSendFlags.Fin));
        Assert.True(h.RunUntil(() => got.Count == 3 && GroupKit.OpenPeerGroups(h.Server, 9) == 0));
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, got[0].Payload);
        Assert.Equal(new byte[] { 2, 3, 4, 5 }, got[1].Payload);
        Assert.Equal(new byte[] { 3, 4, 5, 6 }, got[2].Payload);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(9, 4, [9, 9, 9, 9]), out _, fin: true));
        Assert.True(h.RunUntil(() => got.Count == 4));
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }

    [Fact]
    public void The_Connection_Stream_Limit_Bounds_A_Peer_That_Opens_Every_Stream_On_One_Channel()
    {
        // A hostile peer ignores MaxGroups and opens group streams on one channel until the connection's stream limit stops
        // it. Every one of them has a receive record (the records are sized from that limit, not from the channel), the
        // limit itself refuses the next stream, and nothing the engine holds grows past it.
        using ServerHarness h = new(table: Table, server: QuietOptions.Apply);
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;
        int limit = server.Core.PeerUnidirectionalStreamLimit;
        Assert.True(limit > 8, "the table must grant more streams than one channel's MaxGroups");
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(9, Handlers.Collect(got));
        server.RegisterHandler(5, Handlers.Collect(got));

        // All but one of the streams on channel 9 (MaxGroups 2), each with a message that is only half there.
        List<TransportStreamId> streams = [];
        for (int i = 0; i < limit - 1; i++)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(9, (ulong)i, 4, [(byte)i]), out TransportStreamId id));
            streams.Add(id);
        }

        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(server, 9) == limit - 1));

        // The last stream goes to another channel: the records are shared, so one channel cannot take them all from another.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(5, 1, [7, 7]), out _, fin: true));
        Assert.True(h.RunUntil(() => got.Count == 1), "a group of another channel found no receive record");
        Assert.Equal(new byte[] { 7, 7 }, got[0].Payload);

        // Past the limit the transport refuses the stream; the engine never sees it.
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(server, 5) == 0));
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStreamStart(9, 1_000, 4, [1]), out TransportStreamId last));
        streams.Add(last);
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(server, 9) == limit));
        TransportStatus beyond = h.Raw.OpenUni(GroupKit.GroupStreamStart(9, 1_001, 4, [1]), out _);
        h.Run(50_000);
        Assert.True(beyond == TransportStatus.StreamLimitReached || h.Raw.Sink.OfKind(RecordedEventKind.StreamStarted).Any(e => e.Status == TransportStatus.StreamLimitReached),
            "the connection's stream limit did not refuse a stream beyond it");
        Assert.Equal(limit, GroupKit.OpenPeerGroups(server, 9));
        Assert.Equal(0, h.Statistics().StreamsReset);
        Assert.Equal(PeerState.Connected, server.State);

        // Every stream is completed: all messages arrive and every record and lease comes back.
        foreach (TransportStreamId id in streams)
        {
            Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.Raw.Transport, id, [1, 2, 3], TransportSendFlags.Fin));
        }

        Assert.True(h.RunUntil(() => got.Count == limit + 1 && GroupKit.OpenPeerGroups(server, 9) == 0));
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
        Assert.Equal(0, h.Statistics().CallbackFaults);
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
    public void Whole_And_Split_Messages_Of_One_Group_Arrive_Intact_And_Only_A_Split_One_Is_Watched()
    {
        using ServerHarness h = new(table: Table, server: o =>
        {
            DatagramKit.Quiet(o);
            o.StreamIdleTimeout = TimeSpan.FromMilliseconds(50);
        });
        Assert.True(h.Admit());
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));

        // Two messages that each arrive whole (one engine event apiece): the stream is not watched between messages, so
        // waiting past the idle timeout resets nothing.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(5, 1, [1, 2], [3, 4, 5]), out TransportStreamId id));
        Assert.True(h.RunUntil(() => got.Count == 2));
        h.Run(200_000);
        Assert.Equal(0, h.Statistics().StreamIdleTimeouts);
        Assert.False(Aborted(h.Raw, id, out _));

        // A message split across two writes (staged, so its lease is held in between), then a whole one with the FIN.
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.Raw.Transport, id, [4, 6, 7], TransportSendFlags.None));
        Assert.True(h.RunUntil(() => h.Statistics().ReceiveBytesOutstanding > 0));
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.Raw.Transport, id, [8, 9, 2, 10, 11], TransportSendFlags.Fin));
        Assert.True(h.RunUntil(() => got.Count == 4 && GroupKit.OpenPeerGroups(h.Server, 5) == 0));
        Assert.Equal(new byte[] { 1, 2 }, got[0].Payload);
        Assert.Equal(new byte[] { 3, 4, 5 }, got[1].Payload);
        Assert.Equal(new byte[] { 6, 7, 8, 9 }, got[2].Payload);
        Assert.Equal(new byte[] { 10, 11 }, got[3].Payload);
        Assert.Equal(0, h.Statistics().StreamIdleTimeouts);
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
        Assert.Equal(PeerState.Connected, h.Server.State);
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
        // A bandwidth-capped link keeps the group's later carriers in flight while the peer's STOP_SENDING travels, so the
        // reset really does catch the group half sent.
        LinkOptions link = new() { DelayMicros = 5_000, BandwidthBitsPerSecond = 8_000_000, PeerUnidiStreams = 8 };
        using ClientHarness h = new(link: link, table: Table, client: GroupKit.Roomy);
        Assert.True(h.Accept());
        List<SendToken> tokens = [];
        for (int i = 0; i < 200; i++)
        {
            SendResult result = h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 200), SendOptions.Tracked);
            Assert.True(result.IsAdmitted);
            tokens.Add(result.Token);
        }

        h.Client.Flush();
        Assert.True(h.RunUntil(() => h.Sink.CountOf(RecordedEventKind.PeerStreamStarted) >= 2));
        TransportStreamId group = h.Sink.OfKind(RecordedEventKind.PeerStreamStarted)[1].StreamId;
        h.ServerTransport!.AbortStream(group, 77, StreamAbortDirection.Receive);
        Assert.True(h.RunUntil(() => tokens.TrueForAll(token => h.Client.GetDeliveryStatus(token) != DeliveryStatus.Pending)));
        Assert.Contains(DeliveryStatus.Failed, tokens.Select(token => h.Client.GetDeliveryStatus(token)));

        // Only that group failed: the channel still admits and sends (PROTOCOL.md §3.2, groups are independent).
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(0, GroupKit.Stats(h.Client, 5).QueuedMessages);
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
