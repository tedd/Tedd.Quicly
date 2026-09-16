using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The ReliableLatest engine's edges: the admission refusals, cancellation, the close path, the 30-second version budget,
/// a supersede from the middle of the queue, a datagram limit that shrinks under a queued value, a refused large-value
/// stream, the receive-side rejections of a group stream, the ack sweep after the hand-off ring overflows, and the checks
/// the engine makes on channels that are not its own.
/// </summary>
public class LatestEdgeTests
{
    private static readonly ChannelTable Table = LatestTables.Main;

    private static SessionHarness Harness(Action<PeerOptions>? client = null, Action<PeerOptions>? server = null, LinkOptions? link = null)
    {
        SessionHarness harness = new(link: link ?? new LinkOptions { DelayMicros = 2_000 }, table: Table,
            client: o =>
            {
                LatestKit.Quiet(o);
                client?.Invoke(o);
            },
            server: o =>
            {
                LatestKit.Quiet(o);
                server?.Invoke(o);
            });
        harness.Run(50_000);
        return harness;
    }

    [Fact]
    public async Task FlushAsync_Waits_Until_A_Queued_Value_Was_Handed_Over()
    {
        // The send cap holds the values back, so the flush's watermark is only reached later (the engine's OldestQueuedStamp).
        using SessionHarness h = Harness(client: o => o.MaxSendBytesPerSecond = 4_000);
        h.Client.SendCopy(new SendHeader(2, 1), LatestKit.Payload(1, 900));
        h.Client.SendCopy(new SendHeader(2, 2), LatestKit.Payload(2, 900));
        ValueTask flush = h.Client.FlushAsync();
        Assert.False(flush.IsCompleted);
        Assert.True(h.RunUntil(() => flush.IsCompleted, 10_000_000), "the flush never completed");
        await flush;
        Assert.True(DatagramKit.ChannelStats(h.Client, 2).Sent >= 2);
    }

    [Fact]
    public void A_Value_That_Was_Not_Transmitted_Can_Be_Canceled()
    {
        using SessionHarness h = Harness();
        SendResult queued = h.Client.SendCopy(new SendHeader(2, 5), LatestKit.Payload(1, 48), SendOptions.Tracked);
        Assert.True(h.Client.TryCancel(queued.Token));
        h.Run(50_000);
        Assert.Equal(DeliveryStatus.Canceled, h.Client.GetDeliveryStatus(queued.Token));
        Assert.Equal(0, LatestKit.LiveKeys(h.Client, 2));

        // Once it is with the transport it can only be superseded, not canceled.
        SendResult sent = h.Client.SendCopy(new SendHeader(2, 6), LatestKit.Payload(2, 48), SendOptions.Tracked);
        h.Client.Flush();
        Assert.False(h.Client.TryCancel(sent.Token));
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(sent.Token) == DeliveryStatus.Delivered), "the value was not delivered");
    }

    [Fact]
    public void Closing_The_Session_Completes_Every_Live_Value_Disconnected()
    {
        using SessionHarness h = Harness();
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(8);
        SendResult first = h.Client.SendCopy(new SendHeader(2, 1), LatestKit.Payload(1, 32), SendOptions.Tracked);
        SendResult second = h.Client.SendCopy(new SendHeader(2, 2), LatestKit.Payload(2, 32), SendOptions.Tracked);
        h.Run(30_000);
        Assert.Equal(2, LatestKit.LiveKeys(h.Client, 2));

        h.Client.Close();
        Assert.True(h.RunUntilClosed(), "the session did not close");
        Assert.Equal(DeliveryStatus.Disconnected, h.Client.GetDeliveryStatus(first.Token));
        Assert.Equal(DeliveryStatus.Disconnected, h.Client.GetDeliveryStatus(second.Token));
        Assert.Equal(0, LatestKit.LiveKeys(h.Client, 2));
    }

    [Fact]
    public void Admission_Refuses_A_Value_That_Is_Too_Large_Or_Whose_Key_Table_Is_Full()
    {
        using SessionHarness h = Harness();
        Assert.Equal(SendStatus.TooLarge, h.Client.SendCopy(new SendHeader(2, 1), new byte[70_000]).Status);
        Assert.Equal(1, DatagramKit.ChannelStats(h.Client, 2).TooLarge);

        // Channel 6 holds four keys and never evicts (PROTOCOL.md §7).
        for (ulong key = 0; key < 4; key++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(6, key), LatestKit.Payload(1, 16)).Status);
        }

        Assert.Equal(SendStatus.KeyTableFull, h.Client.SendCopy(new SendHeader(6, 99), LatestKit.Payload(1, 16)).Status);
        Assert.Equal(1, DatagramKit.ChannelStats(h.Client, 6).SendKeyTableFull);
    }

    [Fact]
    public void Admission_Refuses_A_Value_Beyond_The_Channel_Queue_Limit_Or_The_Send_Budget()
    {
        // Channel 10 admits 200 bytes of unacknowledged values.
        using SessionHarness h = Harness();
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(10, 1), LatestKit.Payload(1, 150)).Status);
        Assert.Equal(SendStatus.QueueFull, h.Client.SendCopy(new SendHeader(10, 2), LatestKit.Payload(2, 150)).Status);
        Assert.Equal(1, DatagramKit.ChannelStats(h.Client, 10).QueueFull);

        // A small send budget refuses when no payload buffer is left.
        using SessionHarness tight = Harness(client: o => o.SendBudgetBytes = 2048);
        SendStatus last = SendStatus.Admitted;
        for (ulong key = 0; key < 64 && last == SendStatus.Admitted; key++)
        {
            last = tight.Client.SendCopy(new SendHeader(2, key), LatestKit.Payload(1, 64)).Status;
        }

        Assert.Equal(SendStatus.OutOfBuffers, last);
    }

    [Fact]
    public void An_Immediate_Value_Leaves_Inside_The_Send_Call()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(7, LatestKit.Collect(received));

        // Channel 7 has priority 250, so its datagrams carry the Priority flag too (PROTOCOL.md §4.5).
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(7, 1), LatestKit.Payload(3, 64), SendOptions.Immediate).Status);
        Assert.Equal(1, DatagramKit.ChannelStats(h.Client, 7).Sent);
        Assert.True(h.RunUntil(() => received.Count == 1), "the immediate value did not arrive");
    }

    [Fact]
    public void A_Version_That_Runs_Out_Of_Time_Completes_Failed()
    {
        // Four retry bytes per second: the version cannot reach 16 transmissions inside its 30-second budget, so time is
        // what ends it (PROTOCOL.md §4.4).
        using SessionHarness h = Harness(client: o => o.MaxRetryBytesPerSecond = 4);
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(64);
        SendResult result = h.Client.SendCopy(new SendHeader(2, 3), LatestKit.Payload(1, 64), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) is not DeliveryStatus.Pending, 45_000_000),
            "the version never finished");
        Assert.Equal(DeliveryStatus.Failed, h.Client.GetDeliveryStatus(result.Token));
        Assert.True(DatagramKit.ChannelStats(h.Client, 2).Retries < 15, "it should have failed on the 30 s budget, not the transmissions");
        Assert.Equal(0, LatestKit.LiveKeys(h.Client, 2));
    }

    [Fact]
    public void A_Value_Superseded_From_The_Middle_Of_The_Queue_Is_Unlinked()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        SendToken a = h.Client.SendCopy(new SendHeader(2, 1), LatestKit.Payload(1, 32), SendOptions.Tracked).Token;
        SendToken b = h.Client.SendCopy(new SendHeader(2, 2), LatestKit.Payload(2, 32), SendOptions.Tracked).Token;
        SendToken c = h.Client.SendCopy(new SendHeader(2, 3), LatestKit.Payload(3, 32), SendOptions.Tracked).Token;

        // The middle of the queue is replaced, so it must be unlinked without disturbing the values around it.
        SendToken newest = h.Client.SendCopy(new SendHeader(2, 2), LatestKit.Payload(4, 32), SendOptions.Tracked).Token;
        Assert.True(
            h.RunUntil(
                () => h.Client.GetDeliveryStatus(a) == DeliveryStatus.Delivered
                    && h.Client.GetDeliveryStatus(c) == DeliveryStatus.Delivered
                    && h.Client.GetDeliveryStatus(newest) == DeliveryStatus.Delivered,
                5_000_000),
            $"a {h.Client.GetDeliveryStatus(a)}, c {h.Client.GetDeliveryStatus(c)}, newest {h.Client.GetDeliveryStatus(newest)}");
        Assert.Equal(3, received.Count);
        Assert.Equal(DeliveryStatus.Superseded, h.Client.GetDeliveryStatus(b));
        Assert.True(LatestKit.Matches(received.Single(v => v.Key == 2).Payload, 4, 32));
    }

    [Fact]
    public void A_Datagram_Limit_That_Shrinks_Below_A_Queued_Value_Moves_It_To_A_Group_Stream()
    {
        LinkOptions link = new() { DelayMicros = 2_000, MtuChanges = { new MtuChange(400_000, 300) } };
        using SessionHarness h = Harness(client: LatestKit.Roomy, server: LatestKit.Roomy, link: link);
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        // Admitted while a datagram could carry it; by the next pass the path's limit is 300 bytes.
        h.Client.SendCopy(new SendHeader(2, 4), LatestKit.Payload(5, 600));
        h.Network.AdvanceTo(h.Network.NowMicros + 450_000);
        Assert.True(h.RunUntil(() => received.Count == 1, 5_000_000), "the value never arrived");
        Assert.True(LatestKit.Matches(received[0].Payload, 5, 600));
        Assert.True(DatagramKit.Statistics(h.Client).StreamSends >= 1, "it should have gone out on a group stream");
    }

    [Fact]
    public void A_Refused_Large_Value_Stream_Waits_For_Credit_And_Then_Goes_Out()
    {
        AsyncRefusalConnector connector = null!;
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000 }, table: Table,
            client: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            server: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            connector: inner => connector = new AsyncRefusalConnector(inner));
        h.Run(50_000);
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        AsyncRefusalTransport transport = connector.Transport!;
        transport.Synchronous = true;
        transport.RefuseStarts = 1;

        SendResult result = h.Client.SendCopy(new SendHeader(2, 8), LatestKit.Payload(6, 8_000), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        h.Run(30_000);
        Assert.Equal(1, transport.Refused);
        Assert.Empty(received);

        // The stream never started, so the value waits for credit and then goes out on a new one (PROTOCOL.md §3.2).
        transport.GrantCredit();
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 10_000_000),
            $"status {h.Client.GetDeliveryStatus(result.Token)}");
        Assert.True(LatestKit.Matches(received[^1].Payload, 6, 8_000));
    }

    [Fact]
    public void A_Large_Value_Whose_Start_Is_Refused_Asynchronously_Is_Retransmitted()
    {
        AsyncRefusalConnector connector = null!;
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000 }, table: Table,
            client: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            server: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            connector: inner => connector = new AsyncRefusalConnector(inner));
        h.Run(50_000);
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        AsyncRefusalTransport transport = connector.Transport!;
        transport.RefuseStarts = 1;

        SendResult result = h.Client.SendCopy(new SendHeader(2, 9), LatestKit.Payload(7, 8_000), SendOptions.Tracked);
        h.Client.Flush();
        transport.Deliver();
        transport.GrantCredit();
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 10_000_000),
            $"status {h.Client.GetDeliveryStatus(result.Token)}");
        Assert.True(LatestKit.Matches(received[^1].Payload, 7, 8_000));
        Assert.True(DatagramKit.ChannelStats(h.Client, 2).Retries >= 1, "the refused start should have been retransmitted");
    }

    [Fact]
    public void Group_Stream_Values_The_Receiver_Cannot_Take_Are_Rejected()
    {
        using ServerHarness h = new(table: Table, server: o =>
        {
            LatestKit.Quiet(o);
            o.ReceiveBudgetBytes = 4096;
        });
        Assert.True(h.Admit(), "the raw client was not admitted");

        // A compressed value whose decoded size could never be staged by this peer.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(LatestKit.GroupStream(Table, 5, 1, 1, new byte[100], rawLength: 8192), out _, fin: true));
        h.Run(50_000);

        // A key table that is full (channel 6 holds four keys).
        for (ulong key = 0; key < 4; key++)
        {
            h.Raw.SendDatagram(LatestKit.Frame(Table, 6, 1, key, LatestKit.Payload(1, 8)));
        }

        h.Run(50_000);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(LatestKit.GroupStream(Table, 6, 1, 99, new byte[16]), out _, fin: true));
        h.Run(50_000);

        // A newer large value, then a stale one for the same key: the stale stream is consumed and the current version re-acked.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(LatestKit.GroupStream(Table, 2, 5, 7, new byte[1500]), out _, fin: true));
        h.Run(50_000);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(LatestKit.GroupStream(Table, 2, 3, 7, new byte[1500]), out _, fin: true));
        h.Run(50_000);

        // Nothing drains the mailboxes, so the receive budget runs out and a further large value has nowhere to go.
        for (ulong key = 20; key < 24; key++)
        {
            h.Raw.OpenUni(LatestKit.GroupStream(Table, 2, 1, key, new byte[1400]), out _, fin: true);
            h.Run(20_000);
        }

        // Two group streams at once on a channel that accepts one: the second is reset LimitExceeded (PROTOCOL.md §7).
        byte[] value = LatestKit.GroupStream(Table, 8, 1, 1, new byte[64]);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(value.AsSpan(0, value.Length - 16).ToArray(), out _));
        h.Run(20_000);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(LatestKit.GroupStream(Table, 8, 2, 2, new byte[64]), out _, fin: true));
        h.Run(20_000);

        // A KeyRetired for a key the full receive table has no room for is counted, and the connection survives.
        h.Raw.SendControl(Frames.KeyRetiredFrame(6, 99));
        h.Run(20_000);

        HashSet<LatestRejectReason> reasons = [.. LatestKit.RejectEntries(h.Raw.Sink).Select(entry => entry.Reason)];
        Assert.Contains(LatestRejectReason.DecodeError, reasons);
        Assert.Contains(LatestRejectReason.KeyTableFull, reasons);
        Assert.Contains(LatestRejectReason.RingFull, reasons);
        Assert.True(DatagramKit.ChannelStats(h.Server!, 6).ReceiveKeyTableFull >= 1, "the retirement of an unknown key was not counted");
        Assert.True(DatagramKit.ChannelStats(h.Server!, 2).Dropped >= 1, "the stale large value was not counted");
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void Acks_Beyond_The_Hand_Off_Ring_Are_Found_By_The_Sweep()
    {
        // More keys acknowledge in one pass than the ring holds, so the receive side falls back to its per-channel sweep.
        using SessionHarness h = Harness(client: LatestKit.Roomy, server: LatestKit.Roomy);
        byte[] payload = new byte[16];
        for (ulong key = 0; key < 1_024; key++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(4, key), payload).Status);
        }

        for (ulong key = 0; key < 200; key++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2, key), payload).Status);
        }

        Assert.True(h.RunUntil(() => LatestKit.LiveKeys(h.Client, 4) == 0 && LatestKit.LiveKeys(h.Client, 2) == 0, 5_000_000),
            $"{LatestKit.LiveKeys(h.Client, 4)} + {LatestKit.LiveKeys(h.Client, 2)} keys are still live");
        Assert.Equal(1_024, DatagramKit.ChannelStats(h.Server!, 4).Received);
        Assert.Equal(200, DatagramKit.ChannelStats(h.Server!, 2).Received);
    }

    [Fact]
    public void The_Engine_Refuses_What_Does_Not_Belong_To_It()
    {
        using SessionHarness h = Harness();
        ReliableLatestEngine engine = LatestKit.Engine(h.Client);

        // Channel 3 is UnreliableUnordered: no ReliableLatest channel of this engine owns it.
        Assert.Equal(SendStatus.InvalidChannel, engine.RetireKey(Table[3]!, 1));
        Assert.Equal(StreamAcceptAction.Reset, engine.OnStreamOpened(new TransportStreamId(9, 1), 3, 1).Action);

        // A control message of another mode is accepted and ignored; a malformed or foreign batch is refused.
        Assert.True(engine.OnControl(ControlType.BulkProgress, default, onStream: true, 0));
        Assert.False(engine.OnControl(ControlType.LatestAck, default, onStream: true, 0));
        Assert.False(engine.OnControl(ControlType.LatestReject, default, onStream: true, 0));
        Assert.False(engine.OnControl(ControlType.KeyRetired, default, onStream: true, 0));
        Assert.False(engine.OnControl(ControlType.LatestAck, AckBody(3, 1, 1), onStream: false, 0));
        Assert.False(engine.OnControl(ControlType.LatestReject, RejectBody(3, 1, 1), onStream: false, 0));
        Assert.False(engine.OnControl(ControlType.KeyRetired, RetiredBody(3, 1), onStream: true, 0));
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    private static byte[] RetiredBody(ushort channel, ulong key)
    {
        byte[] frame = new byte[32];
        Assert.True(ControlCodec.TryWrite(frame, new KeyRetired(channel, key), out int length));
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(frame.AsSpan(0, length), out _, out ReadOnlySpan<byte> body, out _));
        return body.ToArray();
    }

    private static byte[] AckBody(ushort channel, ulong key, uint version)
    {
        byte[] frame = new byte[64];
        LatestAckBatchWriter writer = new(frame, ControlCarrier.Datagram);
        Assert.True(writer.TryAdd(new LatestAckEntry(channel, key, version)));
        int length = writer.Finish();
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadDatagram(frame.AsSpan(0, length), out _, out ReadOnlySpan<byte> body, out _));
        return body.ToArray();
    }

    private static byte[] RejectBody(ushort channel, ulong key, uint version)
    {
        byte[] frame = new byte[64];
        LatestRejectBatchWriter writer = new(frame, ControlCarrier.Datagram);
        Assert.True(writer.TryAdd(new LatestRejectEntry(channel, key, version, LatestRejectReason.RingFull)));
        int length = writer.Finish();
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadDatagram(frame.AsSpan(0, length), out _, out ReadOnlySpan<byte> body, out _));
        return body.ToArray();
    }

    [Fact]
    public void A_Rejected_Value_Is_Retried_After_A_Back_Off()
    {
        // The receiver's budget is nearly full and nothing polls it, so it answers LatestReject(RingFull); the sender backs
        // off and delivers the value once the receiver has room again (PROTOCOL.md §2.3, §4.4).
        using SessionHarness h = Harness(server: o => o.ReceiveBudgetBytes = 2048);
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        SendResult held = h.Client.SendCopy(new SendHeader(2, 1), LatestKit.Payload(1, 900), SendOptions.Tracked);
        h.Run(200_000);
        SendResult rejected = h.Client.SendCopy(new SendHeader(2, 2), LatestKit.Payload(2, 900), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(h.Server!, 2).OutOfBuffers > 0, 2_000_000), "the value was not rejected");

        // With a handler the mailbox drains, the budget frees and the retry gets through.
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(rejected.Token) == DeliveryStatus.Delivered, 10_000_000),
            $"status {h.Client.GetDeliveryStatus(rejected.Token)}");
        Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(held.Token));
        Assert.True(DatagramKit.ChannelStats(h.Client, 2).Retries >= 1, "the rejected value was not retried");
    }

    [Fact]
    public void A_Lost_Transmission_Of_A_Superseded_Value_Is_Not_Retried()
    {
        using SessionHarness h = Harness();
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(1);
        SendToken stale = h.Client.SendCopy(new SendHeader(2, 21), LatestKit.Payload(1, 32), SendOptions.Tracked).Token;
        h.Client.Flush();

        // The dropped transmission belongs to a value the next one replaces, so its loss must not schedule a retry.
        SendToken newest = h.Client.SendCopy(new SendHeader(2, 21), LatestKit.Payload(2, 32), SendOptions.Tracked).Token;
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(newest) == DeliveryStatus.Delivered, 5_000_000),
            $"status {h.Client.GetDeliveryStatus(newest)}");
        Assert.Equal(DeliveryStatus.Superseded, h.Client.GetDeliveryStatus(stale));
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server!, 2).Received);
    }

    [Fact]
    public void A_Value_Superseded_While_It_Waits_For_A_Retry_Is_Unlinked_From_The_Retry_Queue()
    {
        // A tiny retry budget parks both values in the retry queue; replacing the second unlinks it from that queue's tail.
        using SessionHarness h = Harness(client: o => o.MaxRetryBytesPerSecond = 8);
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(2);
        SendToken first = h.Client.SendCopy(new SendHeader(2, 31), LatestKit.Payload(1, 64), SendOptions.Tracked).Token;
        SendToken parked = h.Client.SendCopy(new SendHeader(2, 32), LatestKit.Payload(2, 64), SendOptions.Tracked).Token;
        h.Run(200_000);

        SendToken newest = h.Client.SendCopy(new SendHeader(2, 32), LatestKit.Payload(3, 64), SendOptions.Tracked).Token;
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(parked) == DeliveryStatus.Superseded, 2_000_000),
            $"status {h.Client.GetDeliveryStatus(parked)}");
        Assert.True(
            h.RunUntil(
                () => h.Client.GetDeliveryStatus(newest) == DeliveryStatus.Delivered && h.Client.GetDeliveryStatus(first) == DeliveryStatus.Delivered,
                60_000_000),
            $"newest {h.Client.GetDeliveryStatus(newest)}, first {h.Client.GetDeliveryStatus(first)}");
    }

    [Fact]
    public void A_Gather_Of_Several_Pages_Becomes_One_Value()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        BufferLease first = h.Client.RentBuffer(32);
        BufferLease second = h.Client.RentBuffer(32);
        int pageLength = first.Length;
        h.Client.GetBufferSpan(in first).Fill(1);
        h.Client.GetBufferSpan(in second).Fill(2);
        Assert.Equal(SendStatus.Admitted, h.Client.SendGather(new SendHeader(2, 41), [first, second]).Status);
        Assert.True(h.RunUntil(() => received.Count == 1), "the gathered value did not arrive");

        // Each page contributes its whole lease (ARCHITECTURE.md §4.1), copied into the value the engine keeps for retries.
        Assert.Equal(2 * pageLength, received[0].Payload.Length);
        Assert.Equal(1, received[0].Payload[0]);
        Assert.Equal(2, received[0].Payload[pageLength]);
    }

    [Fact]
    public void Disposing_The_Receiver_Releases_A_Half_Received_Large_Value()
    {
        using SessionHarness h = Harness(client: LatestKit.Roomy, server: LatestKit.Roomy,
            link: new LinkOptions { DelayMicros = 2_000, BandwidthBitsPerSecond = 2_000_000 });
        h.Client.SendCopy(new SendHeader(2, 51), LatestKit.Payload(1, 40_000));
        h.Client.Flush();
        h.Network.Advance(20_000);
        h.Server!.Poll();
        Assert.True(DatagramKit.Statistics(h.Server).ReceiveBytesOutstanding > 0, "nothing of the value was staged");

        // The engine's Dispose returns the staging lease of the value that never finished.
        h.DisposeServer();
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    [Fact]
    public void Reconnecting_Drops_Everything_The_Lost_Connection_Held()
    {
        using SessionHarness h = Harness(client: LatestKit.Roomy, server: LatestKit.Roomy,
            link: new LinkOptions { DelayMicros = 2_000, BandwidthBitsPerSecond = 2_000_000 });
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(4);
        h.Client.SendCopy(new SendHeader(2, 61), LatestKit.Payload(1, 64));
        h.Client.SendCopy(new SendHeader(2, 62), LatestKit.Payload(2, 40_000));
        h.Run(30_000);
        Assert.True(LatestKit.LiveKeys(h.Client, 2) > 0, "no key was live");
        long staged = DatagramKit.Statistics(h.Server!).ReceiveBytesOutstanding;
        Assert.True(staged > 1_000, $"only {staged} bytes were staged, so the large value was not in flight");

        // The hook is called here directly (a real resume completes every value Disconnected first, PROTOCOL.md §4.1): what
        // it must do is forget the lost connection's streams, queues and keys, and release what the peer's group streams
        // staged. Values already sitting in a mailbox belong to the peer's own reconnect step (ReleaseAllReceived).
        LatestKit.Engine(h.Client).OnReconnecting();
        LatestKit.Engine(h.Server!).OnReconnecting();
        Assert.Equal(0, LatestKit.LiveKeys(h.Client, 2));
        Assert.True(DatagramKit.Statistics(h.Server!).ReceiveBytesOutstanding < staged / 2,
            $"{DatagramKit.Statistics(h.Server!).ReceiveBytesOutstanding} bytes of {staged} are still staged");
    }

    [Fact]
    public void Admission_Refuses_When_The_Send_Table_Reserve_Is_Reached()
    {
        using SessionHarness h = Harness(client: o => o.SendTableCapacity = 16);
        SendStatus last = SendStatus.Admitted;
        int admitted = 0;
        for (ulong key = 0; key < 64 && last == SendStatus.Admitted; key++)
        {
            last = h.Client.SendCopy(new SendHeader(2, key), LatestKit.Payload(1, 16)).Status;
            admitted += last == SendStatus.Admitted ? 1 : 0;
        }

        // The reserve keeps entries for the transmissions that drain the queue, so a full table cannot deadlock.
        Assert.Equal(SendStatus.QueueFull, last);
        Assert.InRange(admitted, 1, 16);
        h.Run(500_000);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2, 0), LatestKit.Payload(2, 16)).Status);
    }

    [Fact]
    public void An_Owned_Lease_That_Compresses_Goes_Back_At_Admission()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(5, LatestKit.Collect(received));
        BufferLease lease = h.Client.RentBuffer(512);
        h.Client.GetBufferSpan(in lease).Clear();
        long outstanding = DatagramKit.Statistics(h.Client).SendBytesOutstanding;

        // Channel 5 compresses: the value keeps the LZ4 block and the caller's lease goes straight back to the pool.
        Assert.Equal(SendStatus.Admitted, h.Client.SendOwned(new SendHeader(5, 1), lease, 512).Status);
        Assert.True(DatagramKit.Statistics(h.Client).SendBytesOutstanding < outstanding + 512, "the owned lease was not returned");
        Assert.True(h.RunUntil(() => received.Count == 1), "the compressed value did not arrive");
        Assert.Equal(512, received[0].Payload.Length);
    }

    [Fact]
    public void Retiring_A_Key_Without_A_Control_Stream_Reports_NotConnected()
    {
        using SessionHarness h = Harness();
        ReliableLatestEngine engine = LatestKit.Engine(h.Client);
        h.Client.Close();
        Assert.True(h.RunUntilClosed(), "the session did not close");
        Assert.Equal(SendStatus.NotConnected, engine.RetireKey(Table[2]!, 1));
    }

    [Fact]
    public void Retiring_A_Key_Whose_Value_The_Application_Has_Not_Polled_Reports_The_Retirement()
    {
        using SessionHarness h = Harness();
        SendResult result = h.Client.SendCopy(new SendHeader(2, 12), LatestKit.Payload(1, 24), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered), "the value was not delivered");

        // The value is still in the key's mailbox (no handler took it); the retirement replaces it.
        Assert.Equal(SendStatus.Admitted, h.Client.RetireKey(2, 12));
        h.Run(100_000);
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        h.Run(50_000);
        Assert.Single(received);
        Assert.Equal(ReceiveFlags.KeyRetired, received[0].Flags);
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server, 2).ReceiveSuperseded);
    }

    [Fact]
    public void A_Shared_Value_Holds_One_Reference_Until_It_Is_Superseded_Or_Retired()
    {
        // ARCHITECTURE.md §4.1: the entry takes one reference on admission and releases it exactly once, on every path the
        // value can end on — here a supersede and a key retirement rather than an ack.
        using SharedPool pool = new();
        using SessionHarness h = Harness(client: o => o.Allocator = pool.Allocator);
        SharedLease stale = pool.Share(64, seed: 3);
        SharedLease newest = pool.Share(64, seed: 4);

        SendResult first = h.Client.SendShared(new SendHeader(2, 71), pool.Table, in stale, 64, SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, first.Status);
        Assert.Equal(2, pool.Count(in stale));
        SendResult second = h.Client.SendShared(new SendHeader(2, 71), pool.Table, in newest, 64, SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, second.Status);
        Assert.Equal(2, pool.Count(in newest));

        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(first.Token) == DeliveryStatus.Superseded, 5_000_000),
            $"status {h.Client.GetDeliveryStatus(first.Token)}");
        Assert.Equal(1, pool.Count(in stale));

        Assert.Equal(SendStatus.Admitted, h.Client.RetireKey(2, 71));
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(second.Token) is not DeliveryStatus.Pending, 5_000_000),
            "the live value never finished");
        Assert.Equal(1, pool.Count(in newest));

        // The host still owns its own reference on both blocks, so it releases the last one itself.
        Assert.True(pool.Table.Release(in stale));
        Assert.True(pool.Table.Release(in newest));
    }

    [Fact]
    public void A_Shared_Value_That_Needs_A_Group_Stream_Is_Retained_Until_It_Is_Acknowledged()
    {
        // The same contract on the large-value path: the stream reads the shared block directly, so the reference must last
        // until the LatestAck, not until the stream send completes (PROTOCOL.md §3.2, §4.3).
        using SharedPool pool = new();
        using SessionHarness h = Harness(client: o => o.Allocator = pool.Allocator);
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        SharedLease shared = pool.Share(1_400, seed: 5);

        SendResult result = h.Client.SendShared(new SendHeader(2, 72), pool.Table, in shared, 1_400, SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        Assert.Equal(2, pool.Count(in shared));
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 10_000_000),
            $"status {h.Client.GetDeliveryStatus(result.Token)}");
        Assert.Single(received);
        Assert.Equal(1_400, received[0].Payload.Length);
        Assert.True(DatagramKit.Statistics(h.Client).StreamSends >= 1, "the shared value did not go out on a group stream");
        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }

    [Fact]
    public void A_Shared_Value_A_Full_Key_Table_Refuses_Takes_No_Reference()
    {
        using SharedPool pool = new();
        using SessionHarness h = Harness(client: o => o.Allocator = pool.Allocator);
        SharedLease shared = pool.Share(64, seed: 6);

        // Channel 6 holds four keys and never evicts, so the fifth key is refused — and a refused admission must leave the
        // caller's own reference count untouched.
        for (ulong key = 0; key < 4; key++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(6, key), LatestKit.Payload(1, 16)).Status);
        }

        Assert.Equal(SendStatus.KeyTableFull, h.Client.SendShared(new SendHeader(6, 99), pool.Table, in shared, 64).Status);
        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }

    [Fact]
    public void An_Ack_For_A_Version_That_Never_Left_This_Host_Completes_Nothing()
    {
        // PROTOCOL.md §4.3: Delivered means a LatestAck covering the version. Nothing is flushed here, so no version of this
        // key has ever been transmitted and an ack naming one cannot be evidence of anything — whatever version it claims.
        using SessionHarness h = Harness();
        SendResult result = h.Client.SendCopy(new SendHeader(2, 81), LatestKit.Payload(1, 64), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        ReliableLatestEngine engine = LatestKit.Engine(h.Client);
        Assert.True(engine.OnControl(ControlType.LatestAck, AckBody(2, 81, 1), onStream: false, 0));
        Assert.True(engine.OnControl(ControlType.LatestAck, AckBody(2, 81, 99), onStream: false, 0));

        // Admission drains the notices the transport thread handed over, so they have been applied by the time it returns.
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2, 82), LatestKit.Payload(2, 64)).Status);
        Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(result.Token));
        Assert.Equal(0u, LatestKit.AckedVersion(h.Client, 2, 81));
        Assert.Equal(2, LatestKit.LiveKeys(h.Client, 2));

        // The guard is not over-strict: the peer's own ack of the version that really goes out still completes it.
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {h.Client.GetDeliveryStatus(result.Token)}");
    }

    [Fact]
    public void A_Value_That_Never_Got_A_Transmission_Fails_On_Its_Version_Budget()
    {
        // Every stream start is refused synchronously and no credit ever arrives, so this large value is admitted and then
        // waits without a single transmission: only its own 30-second budget can end it (PROTOCOL.md §4.4).
        AsyncRefusalConnector connector = null!;
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000 }, table: Table,
            client: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            server: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            connector: inner => connector = new AsyncRefusalConnector(inner));
        h.Run(50_000);
        AsyncRefusalTransport transport = connector.Transport!;
        transport.Synchronous = true;
        transport.RefuseStarts = int.MaxValue;

        SendResult result = h.Client.SendCopy(new SendHeader(2, 91), LatestKit.Payload(9, 8_000), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        h.Run(1_000_000, step: 50_000);
        Assert.Equal(0, DatagramKit.ChannelStats(h.Client, 2).Sent);
        Assert.Equal(1, LatestKit.LiveKeys(h.Client, 2));

        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) is not DeliveryStatus.Pending, 45_000_000, step: 25_000),
            "the value that was never transmitted never finished, so it pinned its entry and its lease");
        Assert.Equal(DeliveryStatus.Failed, h.Client.GetDeliveryStatus(result.Token));
        Assert.Equal(0, LatestKit.LiveKeys(h.Client, 2));
    }

    [Fact]
    public void A_Retirement_Waiting_In_The_Mailbox_Is_Not_Displaced_By_A_New_Holder_Of_The_Key()
    {
        // PROTOCOL.md §3.4 type 0x17: the application must see the retirement. The new value of the same key id arrives while
        // the notice is still unclaimed (no Poll in between), so it must wait rather than replace it in the mailbox.
        using SessionHarness h = Harness();
        SendResult first = h.Client.SendCopy(new SendHeader(2, 101), LatestKit.Payload(1, 32), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(first.Token) == DeliveryStatus.Delivered), "the value was not delivered");

        Assert.Equal(SendStatus.Admitted, h.Client.RetireKey(2, 101));
        h.Client.Flush();
        h.Network.Advance(10_000);

        // Only the transport thread ran, so the retirement is in the mailbox; now a new holder of the same key sends.
        SendResult again = h.Client.SendCopy(new SendHeader(2, 101), LatestKit.Payload(2, 32), SendOptions.Tracked);
        h.Client.Flush();
        h.Network.Advance(10_000);

        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(again.Token) == DeliveryStatus.Delivered, 10_000_000),
            $"status {h.Client.GetDeliveryStatus(again.Token)}");
        Assert.Contains(received, value => value.Flags == ReceiveFlags.KeyRetired);
        Assert.Contains(received, value => LatestKit.Matches(value.Payload, 2, 32));
    }

    [Fact]
    public void A_Large_Value_Of_A_New_Epoch_Is_Accepted_On_A_Group_Stream()
    {
        // PROTOCOL.md §4.1: the epoch restarts the sender's version counter, so the receive side must forget its per-key
        // versions before the next value whichever way that value arrives — a group stream included.
        using ServerHarness h = new(table: Table, server: o =>
        {
            LatestKit.Quiet(o);
            LatestKit.Roomy(o);
        });
        Assert.True(h.Admit(), "the raw client was not admitted");
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(LatestKit.GroupStream(Table, 2, 7, 3, new byte[1_500]), out _, fin: true));
        h.Run(50_000);
        Assert.Single(received);
        Assert.Equal(7u, received[0].Version);

        // A new epoch, and its first value of this key is large too: version 1 must be accepted, not dropped as "not newer"
        // against the epoch that ended (which would re-ack version 7 and tell the sender a value it never got had arrived).
        LatestKit.Engine(h.Server).OnEpochReset(resumed: true);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(LatestKit.GroupStream(Table, 2, 1, 3, new byte[1_500]), out _, fin: true));
        h.Run(50_000);
        Assert.Equal(2, received.Count);
        Assert.Equal(1u, received[1].Version);
        Assert.Equal(PeerState.Connected, h.Server.State);
    }

    [Fact]
    public void A_Reported_Start_That_Took_No_Stream_Slot_Does_Not_Release_One()
    {
        // The per-channel slot is paired with the stream id it was counted for, so a transport that reports OnStreamStarted
        // for a start its own SubmitStream refused cannot hand back a slot the live stream still holds — which would let the
        // sender open more streams than the channel's MaxGroups and make the receiver reset a live stream of ours (§7).
        using SessionHarness h = Harness(client: LatestKit.Roomy, server: LatestKit.Roomy,
            link: new LinkOptions { DelayMicros = 2_000, BandwidthBitsPerSecond = 8_000_000 });
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(8, LatestKit.Collect(received));

        // Channel 8 opens one group stream at a time; this value holds that one slot.
        SendToken first = h.Client.SendCopy(new SendHeader(8, 1), LatestKit.Payload(1, 20_000), SendOptions.Tracked).Token;
        h.Client.Flush();
        h.Network.Advance(1_000);

        ReliableLatestEngine engine = LatestKit.Engine(h.Client);
        int dense = h.Client.Core.ChannelIndexOf(8);
        ulong context = PeerCore.MakeEngineStreamContext(ChannelMode.ReliableLatest, dense, 99);
        TransportStreamId phantom = new(4_000, 1);
        engine.OnStreamStarted(phantom, context, TransportStatus.StreamLimitReached);
        engine.OnStreamClosed(phantom, aborted: true, 0);

        SendToken second = h.Client.SendCopy(new SendHeader(8, 2), LatestKit.Payload(2, 20_000), SendOptions.Tracked).Token;
        Assert.True(h.RunUntil(
            () => h.Client.GetDeliveryStatus(first) == DeliveryStatus.Delivered && h.Client.GetDeliveryStatus(second) == DeliveryStatus.Delivered,
            30_000_000), $"first {h.Client.GetDeliveryStatus(first)}, second {h.Client.GetDeliveryStatus(second)}");
        Assert.Equal(0, DatagramKit.Statistics(h.Server).StreamsReset);
        Assert.Equal(2, received.Count(value => value.Payload.Length == 20_000));
    }

    [Fact]
    public void A_Retired_Key_Gives_Its_Receive_Slot_Back_When_The_Table_Is_Full()
    {
        // Channel 6 holds four keys and never evicts a live one (PROTOCOL.md §7). A key whose retirement the application has
        // already seen is not live, so its slot must not keep a fifth key out for the rest of the epoch.
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(6, LatestKit.Collect(received));
        for (ulong key = 0; key < 4; key++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(6, key), LatestKit.Payload((int)key, 16)).Status);
        }

        Assert.True(h.RunUntil(() => received.Count == 4, 5_000_000), $"{received.Count} of 4 values arrived");
        Assert.Equal(SendStatus.Admitted, h.Client.RetireKey(6, 0));
        Assert.True(h.RunUntil(() => received.Count == 5, 5_000_000), "the retirement was not delivered");
        Assert.Equal(ReceiveFlags.KeyRetired, received[4].Flags);

        // The fifth key now fits on both sides: the retired key's slot was reclaimed, not held for the epoch.
        SendResult fifth = h.Client.SendCopy(new SendHeader(6, 9), LatestKit.Payload(9, 16), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, fifth.Status);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(fifth.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {h.Client.GetDeliveryStatus(fifth.Token)}");
        Assert.Contains(received, value => value.Key == 9 && LatestKit.Matches(value.Payload, 9, 16));
    }

    [Fact]
    public void The_Transport_Thread_Hooks_Ignore_Streams_That_Are_Not_This_Engines()
    {
        using SessionHarness h = Harness();
        ReliableLatestEngine engine = LatestKit.Engine(h.Client);

        // A close for an invalid id, a start whose context is not an engine context, one of another mode, one naming a
        // channel index outside this engine and one naming a channel of another engine: none of them records anything.
        engine.OnStreamClosed(default, aborted: false, 0);
        engine.OnStreamStarted(default, 0, TransportStatus.Success);
        engine.OnStreamStarted(new TransportStreamId(5_000, 1),
            PeerCore.MakeEngineStreamContext(ChannelMode.ReliableOrdered, 0, 1), TransportStatus.Success);
        engine.OnStreamStarted(new TransportStreamId(5_001, 1),
            PeerCore.MakeEngineStreamContext(ChannelMode.ReliableLatest, 9_999, 1), TransportStatus.Success);
        engine.OnStreamStarted(new TransportStreamId(5_002, 1),
            PeerCore.MakeEngineStreamContext(ChannelMode.ReliableLatest, h.Client.Core.ChannelIndexOf(3), 1), TransportStatus.Success);

        // A message event whose cookie names a staging record that is not in use: the records are pooled, so a record is only
        // ever read for the stream it was handed to.
        long[] cookie = [0];
        StreamMessageContext message = default;
        message.Cookie = ref cookie[0];
        message.Id = new TransportStreamId(5_003, 1);
        Assert.Equal(StreamConsumeAction.ResetStream, engine.OnStreamMessage(ref message).Action);

        // A shutdown notice the game thread has not applied yet is dropped with the epoch that ends, not carried into the new
        // one (its slot is still given back).
        TransportStreamId phantom = new(5_004, 1);
        engine.OnStreamStarted(phantom, PeerCore.MakeEngineStreamContext(ChannelMode.ReliableLatest, h.Client.Core.ChannelIndexOf(2), 7),
            TransportStatus.Success);
        engine.OnStreamClosed(phantom, aborted: true, 0);
        engine.OnEpochReset(resumed: true);
        Assert.Equal(1u, LatestKit.NextVersion(h.Client, 2));

        // The session is untouched by all of it, and a value still goes out.
        SendResult result = h.Client.SendCopy(new SendHeader(2, 111), LatestKit.Payload(1, 32), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {h.Client.GetDeliveryStatus(result.Token)}");
        Assert.Equal(PeerState.Connected, h.Client.State);
    }
}
