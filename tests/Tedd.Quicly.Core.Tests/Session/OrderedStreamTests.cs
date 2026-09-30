using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The persistent ordered stream's lifecycle and limits (PROTOCOL.md §3, §6, §7; docs/design/session-layer.md §7.2): stream
/// credit, refused starts (synchronous and MsQuic-style asynchronous), a stream stopped by the peer, a duplicate stream,
/// size limits, receive back-pressure, expiry, close, and the send table reserve.
/// </summary>
public class OrderedStreamTests
{
    private static readonly ChannelTable Table = OrderedTables.Main;

    [Fact]
    public void A_Server_Sending_Before_It_Has_Stream_Credit_Waits_For_It()
    {
        // The client raises the server's stream limit when the HelloAck arrives; the credit reaches the server a one-way delay later.
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000 }, table: Table);
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Client.RegisterHandler(4, Handlers.Collect(got));
        for (int i = 0; i < 3; i++)
        {
            Assert.True(server.SendCopy(new SendHeader(4), OrderedKit.Payload(i, 10)).IsAdmitted);
        }

        server.Flush();

        // The simulator refuses like MsQuic: the start is accepted; OnStreamStarted(StreamLimitReached), the canceled send and
        // the shutdown follow at the next step.
        Assert.Equal(ReliableOrderedEngine.StreamPhase.Starting, OrderedKit.Phase(server, 4));
        h.Network.Advance(0);
        server.Poll();
        Assert.Equal(ReliableOrderedEngine.StreamPhase.Blocked, OrderedKit.Phase(server, 4));
        Assert.Equal(3, OrderedKit.Stats(server, 4).QueuedMessages);
        Assert.Equal(0, OrderedKit.Stats(server, 4).Sent);

        Assert.True(h.RunUntil(() => got.Count == 3));
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(OrderedKit.Payload(i, 10), got[i].Payload);
        }

        Assert.Equal(ReliableOrderedEngine.StreamPhase.Open, OrderedKit.Phase(server, 4));

        // The refused stream was released; the messages went out on the next one.
        Assert.Equal(2u, OrderedKit.StreamSerial(server, 4));
    }

    [Fact]
    public void A_Start_Refused_Asynchronously_Is_Sent_Again_On_A_New_Stream()
    {
        AsyncRefusalConnector? refusal = null;
        using SessionHarness h = new(table: Table, connector: c => refusal = new AsyncRefusalConnector(c));
        AsyncRefusalTransport transport = refusal!.Transport!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        transport.RefuseStarts = 1;
        List<SendToken> tokens = [];
        for (int i = 0; i < 5; i++)
        {
            SendResult result = h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(i, 20), SendOptions.Tracked);
            Assert.True(result.IsAdmitted);
            tokens.Add(result.Token);
        }

        h.Client.Flush();
        Assert.Equal(1, transport.Refused);
        Assert.Equal(ReliableOrderedEngine.StreamPhase.Starting, OrderedKit.Phase(h.Client, 4));
        Assert.Equal(5, OrderedKit.Stats(h.Client, 4).InFlightMessages);

        // MsQuic: OnStreamStarted(StreamLimitReached), then the canceled send, then the shutdown.
        transport.Deliver();
        h.Client.Poll();
        Assert.Equal(ReliableOrderedEngine.StreamPhase.Blocked, OrderedKit.Phase(h.Client, 4));
        Assert.Equal(5, OrderedKit.Stats(h.Client, 4).QueuedMessages);
        Assert.Equal(0, OrderedKit.Stats(h.Client, 4).InFlightMessages);
        Assert.Equal(0, OrderedKit.Stats(h.Client, 4).Sent);
        Assert.All(tokens, token => Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(token)));

        // Nothing moves until the peer grants credit again.
        h.Run(20_000);
        Assert.Empty(got);
        transport.GrantCredit();
        Assert.True(h.RunUntil(() => got.Count == 5));
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(OrderedKit.Payload(i, 20), got[i].Payload);
        }

        Assert.Equal(2u, OrderedKit.StreamSerial(h.Client, 4));
        Assert.True(h.RunUntil(() => tokens.TrueForAll(token => h.Client.GetDeliveryStatus(token) == DeliveryStatus.Delivered)));
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
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        for (int i = 0; i < 3; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(i, 20)).IsAdmitted);
        }

        // The refused stream was released at once (it never started) and the messages stayed first in the queue.
        h.Client.Flush();
        Assert.Equal(ReliableOrderedEngine.StreamPhase.Blocked, OrderedKit.Phase(h.Client, 4));
        Assert.Equal(3, OrderedKit.Stats(h.Client, 4).QueuedMessages);
        h.Run(20_000);
        Assert.Empty(got);
        transport.GrantCredit();
        Assert.True(h.RunUntil(() => got.Count == 3));
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(OrderedKit.Payload(i, 20), got[i].Payload);
        }

        Assert.Equal(2u, OrderedKit.StreamSerial(h.Client, 4));
    }

    [Fact]
    public void A_Stream_Stopped_By_The_Peer_Closes_The_Channel()
    {
        using ClientHarness h = new(link: new LinkOptions { DelayMicros = 5_000, PeerUnidiStreams = 4 }, table: Table);
        Assert.True(h.Accept());
        SendToken first = h.Client.SendCopy(new SendHeader(4), [1], SendOptions.Tracked).Token;
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(first) == DeliveryStatus.Delivered));
        TransportStreamId ordered = h.Sink.OfKind(RecordedEventKind.PeerStreamStarted)[1].StreamId;

        SendToken inFlight = h.Client.SendCopy(new SendHeader(4), [2], SendOptions.Tracked).Token;
        h.Client.Flush();
        h.ServerTransport!.AbortStream(ordered, 77, StreamAbortDirection.Receive);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(inFlight) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Failed, h.Client.GetDeliveryStatus(inFlight));
        Assert.Equal(ReliableOrderedEngine.StreamPhase.Closed, OrderedKit.Phase(h.Client, 4));
        Assert.Equal(SendStatus.ChannelClosed, h.Client.SendCopy(new SendHeader(4), [3]).Status);

        // The connection and the other channels live on.
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.True(h.Client.SendCopy(new SendHeader(5, 1), [4]).IsAdmitted);
    }

    [Fact]
    public void A_Second_Persistent_Stream_For_A_Channel_Is_A_Protocol_Violation()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni([0x04, 0x01, 0x07], out _));
        Assert.True(h.RunUntil(() => OrderedKit.Stats(h.Server!, 4).Received == 1));
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni([0x04, 0x01, 0x08], out _));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }

    [Fact]
    public void A_Frame_Above_The_Channel_Limit_Is_A_Protocol_Violation()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit());
        byte[] frame = new byte[16];
        frame[0] = 9; // channel 9: MaxMessageSize 1000
        int length = 1 + VarInt.Write(frame.AsSpan(1), 1_001);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(frame.AsSpan(0, length), out _));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }

    [Fact]
    public void A_Frame_Cut_By_A_Fin_Is_A_Protocol_Violation()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit());
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni([0x04, 0x05, 1, 2], out _, fin: true));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }

    [Fact]
    public void The_Sender_Refuses_Messages_Above_The_Limit()
    {
        using SessionHarness h = new(table: Table);
        Assert.Equal(SendStatus.TooLarge, h.Client.SendCopy(new SendHeader(9), new byte[1_001]).Status);
        Assert.True(h.Client.SendCopy(new SendHeader(9), new byte[1_000]).IsAdmitted);
        Assert.Equal(1, OrderedKit.Stats(h.Client, 9).TooLarge);
    }

    [Fact]
    public void A_Full_Receive_Ring_Holds_The_Stream_Back_Until_Poll_Makes_Room()
    {
        using SessionHarness h = new(table: Table, server: o => o.ReceiveRingCapacity = 4);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        for (int i = 0; i < 40; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(i, 50)).IsAdmitted);
        }

        Assert.True(h.RunUntil(() => got.Count == 40));
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal(OrderedKit.Payload(i, 50), got[i].Payload);
        }

        PeerStatistics stats = DatagramKit.Statistics(h.Server);
        Assert.True(stats.StreamReceivePends > 0);
        Assert.Equal(0, stats.ReceiveRingDrops);
    }

    [Fact]
    public void An_Exhausted_Receive_Budget_Holds_The_Stream_Back_Until_Leases_Are_Released()
    {
        using SessionHarness h = new(table: Table, server: o => o.ReceiveBudgetBytes = 16 * 1024);
        List<ReceiveLease> kept = [];
        List<byte[]> payloads = [];
        h.Server!.RegisterHandler(4, (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            payloads.Add(payload.ToArray());
            kept.Add(peer.Retain(in header));
        });
        for (int i = 0; i < 8; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(i, 4_000)).IsAdmitted);
        }

        h.Run(50_000);

        // 4 000-byte payloads take 4 KiB leases: four fill the budget, the fifth holds the stream back.
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
            Assert.Equal(OrderedKit.Payload(i, 4_000), payloads[i]);
        }
    }

    [Fact]
    public void An_Exhausted_Receive_Budget_Holds_Back_Small_Messages_Without_Keeping_Their_Ring_Slots()
    {
        // 300-byte messages mostly arrive whole in one segment (one engine event each). A message that finds the budget
        // exhausted is un-read after its ring slot was reserved, so the reservation must be handed back: with a four-slot
        // ring, a few leaked reservations would stall the stream for good.
        using SessionHarness h = new(table: Table, server: o =>
        {
            o.ReceiveBudgetBytes = 2 * 1024;
            o.ReceiveRingCapacity = 4;
        });
        List<ReceiveLease> kept = [];
        List<byte[]> payloads = [];
        h.Server!.RegisterHandler(4, (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            payloads.Add(payload.ToArray());
            kept.Add(peer.Retain(in header));
        });
        for (int i = 0; i < 40; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(i, 300)).IsAdmitted);
        }

        h.Run(50_000);
        Assert.InRange(payloads.Count, 1, 39);
        Assert.True(DatagramKit.Statistics(h.Server).StreamReceivePends > 0);
        Assert.True(h.RunUntil(() =>
        {
            foreach (ReceiveLease lease in kept)
            {
                h.Server.Release(in lease);
            }

            kept.Clear();
            return payloads.Count == 40;
        }));
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal(OrderedKit.Payload(i, 300), payloads[i]);
        }

        Assert.Equal(0, DatagramKit.Statistics(h.Server).ReceiveRingDrops);
    }

    [Fact]
    public void A_Message_Larger_Than_The_Receive_Budget_Closes_The_Connection_With_LimitExceeded()
    {
        using SessionHarness h = new(table: Table, server: o => o.ReceiveBudgetBytes = 8 * 1024);
        Assert.True(h.Client.SendCopy(new SendHeader(4), new byte[10_000]).IsAdmitted);
        Assert.True(h.RunUntilClosed());
        Assert.Equal(QuiclyErrorCode.LimitExceeded, h.Server!.CloseReason.Code);
        Assert.Equal(QuiclyErrorCode.LimitExceeded, h.Client.CloseReason.Code);
    }

    [Fact]
    public void Messages_Whose_Expiry_Passes_Before_They_Reach_The_Stream_Are_Dropped()
    {
        using SessionHarness h = new(table: Table, client: o => o.MaxSendBytesPerSecond = 20_000);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        List<SendToken> tokens = [];
        for (int i = 0; i < 40; i++)
        {
            tokens.Add(h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(i, 1_000), new SendOptions { Track = true, ExpiryMicros = 300_000 }).Token);
        }

        h.Run(2_000_000);
        long expired = OrderedKit.Stats(h.Client, 4).Expired;
        Assert.InRange(expired, 1, 39);
        Assert.Equal(40 - expired, got.Count);
        int delivered = 0;
        for (int i = 0; i < 40; i++)
        {
            DeliveryStatus status = h.Client.GetDeliveryStatus(tokens[i]);
            Assert.True(status is DeliveryStatus.Delivered or DeliveryStatus.Expired, $"message {i}: {status}");
            if (status == DeliveryStatus.Delivered)
            {
                Assert.Equal(OrderedKit.Payload(i, 1_000), got[delivered++].Payload);
            }
        }

        Assert.Equal(got.Count, delivered);
    }

    [Fact]
    public void Closing_Completes_Queued_Ordered_Messages_Disconnected()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000 }, table: Table);
        QuiclyPeer server = h.Server!;
        SendToken token = server.SendCopy(new SendHeader(4), [1], SendOptions.Tracked).Token;
        server.Flush();
        h.Network.Advance(0);
        server.Poll();
        Assert.Equal(ReliableOrderedEngine.StreamPhase.Blocked, OrderedKit.Phase(server, 4));
        server.Close();
        Assert.True(h.RunUntilClosed());
        Assert.Equal(DeliveryStatus.Disconnected, server.GetDeliveryStatus(token));
    }

    [Fact]
    public void A_Full_Send_Table_Answers_QueueFull_But_Keeps_Entries_To_Drain_The_Queues()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            o.SendTableCapacity = 16;
            DatagramKit.Quiet(o);
        });
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(h.Client).SendEntriesInUse == 0));
        int admitted = 0;
        SendResult result;
        while ((result = h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(admitted, 10))).IsAdmitted)
        {
            admitted++;
        }

        Assert.Equal(SendStatus.QueueFull, result.Status);

        // One entry per ordered channel (seven) plus one stays free for the stream sends that drain the queues.
        Assert.Equal(16 - 8, admitted);
        Assert.True(h.RunUntil(() => got.Count == admitted));
    }

    [Fact]
    public void The_Ordered_Queue_Limit_Counts_Bytes_In_Flight()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 20_000 }, table: Table, client: OrderedKit.Roomy, server: OrderedKit.Roomy);
        byte[] block = new byte[16 * 1024];
        int admitted = 0;
        while (h.Client.SendCopy(new SendHeader(7), block).IsAdmitted)
        {
            admitted++;
        }

        // 64 KiB limit: four 16 KiB messages, queued or in flight, until the peer acknowledges them.
        Assert.Equal(4, admitted);
        h.Client.Flush();
        Assert.Equal(SendStatus.QueueFull, h.Client.SendCopy(new SendHeader(7), block).Status);
        Assert.True(h.RunUntil(() => OrderedKit.Stats(h.Client, 7).InFlightMessages == 0 && OrderedKit.Stats(h.Client, 7).QueuedMessages == 0));
        Assert.True(h.Client.SendCopy(new SendHeader(7), block).IsAdmitted);

        // A message larger than the limit still goes alone.
        Assert.True(h.RunUntil(() => OrderedKit.Stats(h.Client, 7).InFlightMessages == 0 && OrderedKit.Stats(h.Client, 7).QueuedMessages == 0));
        Assert.True(h.Client.SendCopy(new SendHeader(7), new byte[65_000]).IsAdmitted);
    }

    [Fact]
    public void A_Receiver_That_Stops_Polling_Pushes_Back_Until_The_Sender_Gets_QueueFull_And_Recovers_When_It_Polls_Again()
    {
        // Back-pressure end to end: the receiver's ring fills, its stream is held back (Pend), the transport's flow-control
        // window fills, the sender's stream sends stop completing, and its channel reaches its queue limit (64 KiB).
        LinkOptions link = new() { DelayMicros = 5_000, StreamReceiveWindowBytes = 64 * 1024 };
        using SessionHarness h = new(link: link, table: Table, client: OrderedKit.Roomy, server: o =>
        {
            OrderedKit.Roomy(o);
            o.ReceiveRingCapacity = 16;
        });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int received = 0;
        string? failure = null;
        server.RegisterHandler(7, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            if (failure is null && !OrderedKit.Matches(payload, received, 1_024))
            {
                failure = $"message {received} out of order";
            }

            received++;
        });
        int sent = 0;

        // The receiver polls: everything flows.
        for (int i = 0; i < 20; i++)
        {
            Assert.True(client.SendCopy(new SendHeader(7), OrderedKit.Payload(sent, 1_024)).IsAdmitted);
            sent++;
        }

        Assert.True(h.RunUntil(() => received == sent));

        // The receiver stops polling: the sender keeps sending until its channel answers QueueFull.
        int admittedWhileStalled = 0;
        SendStatus status = SendStatus.Admitted;
        for (int ms = 0; ms < 2_000 && status == SendStatus.Admitted; ms++)
        {
            while ((status = client.SendCopy(new SendHeader(7), OrderedKit.Payload(sent, 1_024)).Status) == SendStatus.Admitted)
            {
                sent++;
                admittedWhileStalled++;
            }

            client.Flush();
            h.Network.Advance(1_000);
            client.Poll();
            if (status == SendStatus.QueueFull)
            {
                // Completions may still free room while the window fills; only a queue that stays full counts.
                h.Network.Advance(20_000);
                client.Poll();
                status = client.SendCopy(new SendHeader(7), OrderedKit.Payload(sent, 1_024)).Status;
                if (status == SendStatus.Admitted)
                {
                    sent++;
                    admittedWhileStalled++;
                }
            }
        }

        Assert.Equal(SendStatus.QueueFull, status);
        Assert.InRange(received, 20, 20 + 16);
        Assert.True(admittedWhileStalled >= 64, $"{admittedWhileStalled} messages admitted before the push-back");
        Assert.True(DatagramKit.Statistics(server).StreamReceivePends > 0);
        Assert.True(OrderedKit.Stats(client, 7).InFlightMessages > 0);

        // The receiver polls again: the stream resumes, the window reopens, the sender's queue drains and it sends again.
        Assert.True(h.RunUntil(() => received == sent));
        Assert.True(client.SendCopy(new SendHeader(7), OrderedKit.Payload(sent, 1_024)).IsAdmitted);
        sent++;
        Assert.True(h.RunUntil(() => received == sent));
        Assert.Null(failure);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
    }

    [Fact]
    public void A_Tiny_Segment_Arena_Still_Delivers_Everything_In_Order()
    {
        // Eight segments: a stream send carries at most four messages (three with the preamble), and the arena stays full
        // while that send is in flight, so the queue drains one send per round trip.
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 }, table: Table, client: o => o.SegmentArenaCapacity = 8);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        for (int i = 0; i < 100; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(i, 30)).IsAdmitted);
        }

        Assert.True(h.RunUntil(() => got.Count == 100));
        for (int i = 0; i < 100; i++)
        {
            Assert.Equal(OrderedKit.Payload(i, 30), got[i].Payload);
        }

        Assert.True(DatagramKit.Statistics(h.Client).StreamSends >= 25);
    }

    [Fact]
    public void An_Immediate_Ordered_Send_Goes_Out_Without_A_Flush()
    {
        using SessionHarness h = new(table: Table);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        long sends = DatagramKit.Statistics(h.Client).StreamSends;
        Assert.True(h.Client.SendCopy(new SendHeader(4), [1], SendOptions.Immediate).IsAdmitted);

        // The send ran its own scheduler pass: the stream send is with the transport before any Flush.
        Assert.Equal(sends + 1, DatagramKit.Statistics(h.Client).StreamSends);
        Assert.True(h.RunUntil(() => got.Count == 1));
        Assert.Equal(new byte[] { 1 }, got[0].Payload);
    }

    [Fact]
    public void A_Message_That_Expires_Behind_The_Head_Is_Left_Out_Of_The_Stream_Send()
    {
        // A send cap of 20 000 B/s: the burst is 666 bytes, so a 1 000-byte message leaves a debt of some 340 bytes that
        // takes about 17 ms to repay.
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.MaxSendBytesPerSecond = 20_000;
        }, server: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        Assert.True(h.Client.SendCopy(new SendHeader(4), [0]).IsAdmitted);
        Assert.True(h.RunUntil(() => got.Count == 1));
        h.Run(100_000); // the cap's bucket is full again
        Assert.True(h.Client.SendCopy(new SendHeader(4), new byte[1_000]).IsAdmitted);
        h.Client.Flush();
        Assert.Equal(0, OrderedKit.Stats(h.Client, 4).QueuedMessages);

        // The first pass starts the middle message's expiry and the cap holds all three back; by the next pass, 20 ms
        // later, it has waited out its 1 ms. The head has no expiry, so the expired message is met behind it.
        Assert.True(h.Client.SendCopy(new SendHeader(4), [1]).IsAdmitted);
        SendToken expiring = h.Client.SendCopy(new SendHeader(4), [2], new SendOptions { Track = true, ExpiryMicros = 1_000 }).Token;
        Assert.True(h.Client.SendCopy(new SendHeader(4), [3]).IsAdmitted);
        h.Client.Flush();
        Assert.Equal(3, OrderedKit.Stats(h.Client, 4).QueuedMessages);
        Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(expiring));
        h.Network.Advance(20_000);
        h.Client.Flush();
        Assert.True(h.RunUntil(() => got.Count == 4 && h.Client.GetDeliveryStatus(expiring) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Expired, h.Client.GetDeliveryStatus(expiring));
        Assert.Equal(1_000, got[1].Payload.Length);
        Assert.Equal(new byte[] { 1 }, got[2].Payload);
        Assert.Equal(new byte[] { 3 }, got[3].Payload);
        Assert.Equal(1, OrderedKit.Stats(h.Client, 4).Expired);
        h.Run(100_000);
        Assert.Equal(4, got.Count);
    }
}
