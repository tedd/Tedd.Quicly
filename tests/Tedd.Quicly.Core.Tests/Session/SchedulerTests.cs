using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>The scheduler and the packer (docs/design/session-layer.md §7.1, PROTOCOL.md §2.2 and §4.5) over SimulatedTransport.</summary>
public class SchedulerTests
{
    private static readonly ChannelTable Table = DatagramTables.Main;

    [Fact]
    public void Buffered_Messages_Are_Packed_Into_Fewer_Datagrams()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        int received = 0;
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        PeerStatistics before = DatagramKit.Statistics(h.Client);
        SimulatedLinkStatistics linkBefore = DatagramKit.LinkStatistics(h.Client);
        byte[] payload = new byte[64];
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), payload).Status);
        }

        // Buffered: nothing reaches the transport before Flush.
        Assert.Equal(linkBefore.DatagramsSent, DatagramKit.LinkStatistics(h.Client).DatagramsSent);
        h.Client.Flush();
        PeerStatistics after = DatagramKit.Statistics(h.Client);
        SimulatedLinkStatistics linkAfter = DatagramKit.LinkStatistics(h.Client);

        // Entries of 67 bytes (length byte, 1-byte header, 64-byte payload): 17 fit a 1 200-byte container.
        Assert.Equal(3, after.DatagramsSent - before.DatagramsSent);
        Assert.Equal(3, after.ContainersSent - before.ContainersSent);
        Assert.Equal(40, after.MessagesPacked - before.MessagesPacked);
        Assert.Equal(3, linkAfter.DatagramsSent - linkBefore.DatagramsSent);
        // Packing saves datagrams (and their per-packet overhead), not payload bytes: one length byte per message, two per container.
        Assert.Equal((40 * 67) + (3 * 2), linkAfter.DatagramBytesSent - linkBefore.DatagramBytesSent);
        Assert.Equal((40 * 67) + (3 * 2), after.DatagramBytesSent - before.DatagramBytesSent);
        Assert.True(h.RunUntil(() => received == 40));
        ChannelStatistics stats = DatagramKit.ChannelStats(h.Client, 2);
        Assert.Equal(40, stats.Sent);
        Assert.Equal(40 * 64, stats.BytesSent);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(h.Client).SendEntriesInUse == 0));
        Assert.Equal(0, DatagramKit.Statistics(h.Client).SendBytesOutstanding);
    }

    [Fact]
    public void A_Message_That_Fits_Next_To_Nothing_Is_Sent_Alone()
    {
        using ClientHarness h = new(table: Table, client: DatagramKit.Quiet);
        Assert.True(h.Accept());
        h.Client.SendCopy(new SendHeader(2), [42]);
        h.Client.Flush();
        Assert.True(h.RunUntil(() => DatagramKit.ApplicationDatagrams(h.Sink).Count == 1));
        Assert.Equal(new byte[] { 2, 42 }, DatagramKit.ApplicationDatagrams(h.Sink)[0]);
        PeerStatistics single = DatagramKit.Statistics(h.Client);
        Assert.Equal(1, single.DatagramsSent);
        Assert.Equal(0, single.ContainersSent);

        // Two messages that do not fit one datagram together each go out alone, without a copy.
        byte[] large = new byte[700];
        h.Client.SendCopy(new SendHeader(2), large);
        h.Client.SendCopy(new SendHeader(2), large);
        h.Client.Flush();
        Assert.True(h.RunUntil(() => DatagramKit.ApplicationDatagrams(h.Sink).Count == 3));
        Assert.All(DatagramKit.ApplicationDatagrams(h.Sink).Skip(1), d => Assert.Equal(701, d.Length));
        Assert.Equal(0, DatagramKit.Statistics(h.Client).ContainersSent);
    }

    [Fact]
    public void Containers_Carry_The_Flush_Tick_And_Messages_Sent_Alone_Carry_None()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.SendCopy(new SendHeader(2), [2]);
        h.Client.Flush(tick: 1234);
        h.Client.SendCopy(new SendHeader(2), [3]);
        h.Client.Flush(tick: 1235);
        Assert.True(h.RunUntil(() => got.Count == 3));
        Assert.Equal(new byte[] { 1, 2, 3 }, got.Select(g => g.Payload[0]).ToArray());
        Assert.Equal(1234u, got[0].Header.SenderTick);
        Assert.Equal(1234u, got[1].Header.SenderTick);
        Assert.Equal(0u, got[2].Header.SenderTick);
    }

    [Fact]
    public void The_Container_Header_Has_A_Tick_Only_When_Flush_Passed_One()
    {
        using ClientHarness h = new(table: Table, client: DatagramKit.Quiet);
        Assert.True(h.Accept());
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.SendCopy(new SendHeader(2), [2]);
        h.Client.Flush(tick: 99);
        h.Client.SendCopy(new SendHeader(2), [3]);
        h.Client.SendCopy(new SendHeader(2), [4]);
        h.Client.Flush();
        Assert.True(h.RunUntil(() => DatagramKit.ApplicationDatagrams(h.Sink).Count == 2));
        List<byte[]> datagrams = DatagramKit.ApplicationDatagrams(h.Sink);
        Assert.Equal(ParseStatus.Ok, PackedContainer.TryParse(datagrams[0], out PackedContainerReader first));
        Assert.True(first.HasTick);
        Assert.Equal(99u, first.Tick);
        Assert.Equal(2, first.Count);
        Assert.Equal(ParseStatus.Ok, PackedContainer.TryParse(datagrams[1], out PackedContainerReader second));
        Assert.False(second.HasTick);
        Assert.Equal(2, second.Count);
        Assert.Equal(new byte[] { 3, 4 }, DatagramKit.Messages(datagrams[1], Table).Select(m => m.Payload[0]).ToArray());
    }

    [Fact]
    public void Container_Size_Follows_A_Change_Of_The_Datagram_Limit()
    {
        LinkOptions link = new();
        link.MtuChanges.Add(new MtuChange(500_000, 400));
        using ClientHarness h = new(link: link, table: Table, client: DatagramKit.Quiet);
        Assert.True(h.Accept());
        Assert.True(h.Network.NowMicros < 400_000);
        byte[] payload = new byte[64];
        for (int i = 0; i < 20; i++)
        {
            h.Client.SendCopy(new SendHeader(2), payload);
        }

        h.Client.Flush();
        Assert.True(h.RunUntil(() => DatagramKit.ApplicationDatagrams(h.Sink).Count == 2));
        Assert.Contains(DatagramKit.ApplicationDatagrams(h.Sink), d => d.Length > 1_000);

        h.Run(600_000 - h.Network.NowMicros);
        Assert.Equal(400, DatagramKit.Statistics(h.Client).MaxDatagramPayload);
        Assert.Equal(SendStatus.TooLarge, h.Client.SendCopy(new SendHeader(2), new byte[400]).Status);
        for (int i = 0; i < 20; i++)
        {
            h.Client.SendCopy(new SendHeader(2), payload);
        }

        h.Client.Flush();
        Assert.True(h.RunUntil(() => DatagramKit.ApplicationDatagrams(h.Sink).Skip(2).Sum(d => DatagramKit.Messages(d, Table).Count) == 20));
        List<byte[]> after = DatagramKit.ApplicationDatagrams(h.Sink).Skip(2).ToList();
        Assert.All(after, d => Assert.True(d.Length <= 400, $"{d.Length}-byte datagram after the limit shrank to 400"));
        Assert.Equal(4, after.Count);
    }

    [Fact]
    public void A_Message_Queued_Before_The_Datagram_Limit_Shrank_Fails()
    {
        LinkOptions link = new();
        link.MtuChanges.Add(new MtuChange(500_000, 300));
        using SessionHarness h = new(link: link, table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        SendResult large = h.Client.SendCopy(new SendHeader(2), new byte[600], SendOptions.Tracked);
        Assert.True(large.IsAdmitted);
        h.Network.AdvanceTo(510_000);
        h.Client.Flush();
        Assert.Equal(DeliveryStatus.Failed, h.Client.GetDeliveryStatus(large.Token));
        Assert.Equal(1, DatagramKit.ChannelStats(h.Client, 2).TooLarge);
        Assert.Equal(0, DatagramKit.ChannelStats(h.Client, 2).Sent);
        Assert.Equal(SendStatus.TooLarge, h.Client.SendCopy(new SendHeader(2), new byte[300]).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), new byte[299]).Status);
        Assert.Equal(2, DatagramKit.ChannelStats(h.Client, 2).TooLarge);
    }

    [Fact]
    public void Higher_Priority_Channels_Go_First_And_A_Channel_Keeps_Its_Admission_Order()
    {
        using ClientHarness h = new(table: Table, client: DatagramKit.Quiet);
        Assert.True(h.Accept());
        h.Client.SendCopy(new SendHeader(8), [80]);
        h.Client.SendCopy(new SendHeader(2), [20]);
        h.Client.SendCopy(new SendHeader(7), [70]);
        h.Client.SendCopy(new SendHeader(2), [21]);
        h.Client.SendCopy(new SendHeader(7), [71]);
        h.Client.Flush();
        Assert.True(h.RunUntil(() => DatagramKit.ApplicationDatagrams(h.Sink).Count == 1));
        List<(MessageHeader Header, byte[] Payload)> messages = DatagramKit.Messages(DatagramKit.ApplicationDatagrams(h.Sink)[0], Table);
        Assert.Equal(new byte[] { 70, 71, 20, 21, 80 }, messages.Select(m => m.Payload[0]).ToArray());
        Assert.Equal(new ushort[] { 7, 7, 2, 2, 8 }, messages.Select(m => m.Header.Channel).ToArray());
    }

    [Fact]
    public void The_Schedule_Order_Is_Priority_Then_Channel_Id()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        ushort[] ids = h.Client.Core.ScheduleOrder.ToArray().Select(i => h.Client.Core.GetChannel(i).Id).ToArray();
        Assert.Equal(7, ids[0]);
        Assert.Equal(8, ids[^1]);
        Assert.Equal(new ushort[] { 2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 15 }, ids[1..^1]);
    }

    [Fact]
    public void Under_A_Send_Cap_Higher_Priority_Channels_Are_Served_First()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.MaxSendBytesPerSecond = 3_000;
        }, server: DatagramKit.Quiet);
        byte[] payload = new byte[64];
        for (int tick = 1; tick <= 60; tick++)
        {
            for (int i = 0; i < 5; i++)
            {
                h.Client.SendCopy(new SendHeader(8), payload);
                h.Client.SendCopy(new SendHeader(7), payload);
            }

            h.Client.Flush((uint)tick);
            h.Network.Advance(16_667);
            h.Client.Poll();
            h.Server!.Poll();
        }

        Assert.InRange(DatagramKit.ChannelStats(h.Client, 7).Sent, 30, 70);
        Assert.Equal(0, DatagramKit.ChannelStats(h.Client, 8).Sent);
    }

    [Fact]
    public void The_Send_Cap_Limits_The_Bytes_Handed_To_The_Transport()
    {
        const int Rate = 20_000;
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.MaxSendBytesPerSecond = Rate;
        }, server: DatagramKit.Quiet);
        PeerStatistics start = DatagramKit.Statistics(h.Client);
        byte[] payload = new byte[64];
        long blockedDeadline = long.MaxValue;
        for (int tick = 1; tick <= 120; tick++)
        {
            for (int i = 0; i < 10; i++)
            {
                h.Client.SendCopy(new SendHeader(2), payload);
            }

            h.Client.Flush((uint)tick);
            blockedDeadline = Math.Min(blockedDeadline, h.Client.NextDeadlineMicros - h.Network.NowMicros);
            h.Network.Advance(16_667);
            h.Client.Poll();
            h.Server!.Poll();
        }

        // Two seconds at 20 000 B/s plus the burst (two flush intervals' worth) and at most one overdraft.
        long bytes = DatagramKit.Statistics(h.Client).DatagramBytesSent - start.DatagramBytesSent;
        Assert.InRange(bytes, 38_500, 42_000);

        // Held-back work makes the next deadline the refill time instead of the (distant) ping timer.
        Assert.InRange(blockedDeadline, 1, 20_000);
        Assert.True(DatagramKit.ChannelStats(h.Client, 2).Sent < 1_200);
    }

    [Fact]
    public void Under_A_Send_Cap_No_Message_Older_Than_Its_Expiry_Is_Delivered()
    {
        const long Expiry = 50_000;
        const long Delay = 10_000;
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = Delay }, table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.MaxSendBytesPerSecond = 12_000;
        }, server: DatagramKit.Quiet);
        List<long> ages = [];
        h.Server!.RegisterHandler(14, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
            ages.Add(header.ReceivedMicros - BinaryPrimitives.ReadInt64LittleEndian(payload)));
        byte[] payload = new byte[64];
        for (int tick = 1; tick <= 180; tick++)
        {
            for (ulong key = 0; key < 16; key++)
            {
                BinaryPrimitives.WriteInt64LittleEndian(payload, h.Network.NowMicros);
                h.Client.SendCopy(new SendHeader(14, key), payload);
            }

            h.Client.Flush((uint)tick);
            h.Network.Advance(16_667);
            h.Client.Poll();
            h.Server.Poll();
        }

        h.Run(200_000);
        Assert.True(ages.Count > 300, $"{ages.Count} delivered");
        Assert.All(ages, age => Assert.InRange(age, Delay, Expiry + Delay));
        Assert.True(DatagramKit.ChannelStats(h.Client, 14).Expired > 1_000);
    }

    [Fact]
    public void An_Immediate_Send_Goes_Out_At_Once_Together_With_What_Is_Buffered()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        h.Client.Flush(tick: 5);
        PeerStatistics before = DatagramKit.Statistics(h.Client);
        SimulatedLinkStatistics linkBefore = DatagramKit.LinkStatistics(h.Client);
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.SendCopy(new SendHeader(2), [2]);
        Assert.Equal(linkBefore.DatagramsSent, DatagramKit.LinkStatistics(h.Client).DatagramsSent);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), [3], SendOptions.Immediate).Status);
        PeerStatistics after = DatagramKit.Statistics(h.Client);
        Assert.Equal(1, after.ContainersSent - before.ContainersSent);
        Assert.Equal(3, after.MessagesPacked - before.MessagesPacked);
        Assert.Equal(linkBefore.DatagramsSent + 1, DatagramKit.LinkStatistics(h.Client).DatagramsSent);
        h.Network.Advance(1_000);
        h.Server.Poll();
        Assert.Equal(new byte[] { 1, 2, 3 }, got.Select(g => g.Payload[0]).ToArray());
        Assert.All(got, g => Assert.Equal(5u, g.Header.SenderTick));
    }

    [Fact]
    public void Send_Flags_Follow_Priority_And_Never_Delay_Or_Cancel_Without_The_Capability()
    {
        FlagRecordingConnector? recorder = null;
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet,
            connector: c => recorder = new FlagRecordingConnector(c));
        FlagRecordingTransport transport = recorder!.Transport!;
        transport.Datagrams.Clear();
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.Flush();
        h.Client.SendCopy(new SendHeader(7), [1]);
        h.Client.Flush();
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.SendCopy(new SendHeader(2), [2]);
        h.Client.Flush();
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.SendCopy(new SendHeader(7), [2]);
        h.Client.Flush();
        h.Client.SendCopy(new SendHeader(2), [1], SendOptions.Immediate);
        TransportSendFlags[] expected =
        [
            TransportSendFlags.None,
            TransportSendFlags.Priority,
            TransportSendFlags.None,
            TransportSendFlags.Priority,
            TransportSendFlags.Priority,
        ];
        Assert.Equal(expected, transport.Datagrams.Select(d => d.Flags).ToArray());
        Assert.DoesNotContain(transport.Datagrams, d => (d.Flags & (TransportSendFlags.DelaySend | TransportSendFlags.CancelOnBlocked)) != 0);
    }

    [Fact]
    public void A_Datagram_The_Transport_Refuses_Fails_Its_Messages_And_Is_Not_Counted()
    {
        FlagRecordingConnector? recorder = null;
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet,
            connector: c => recorder = new FlagRecordingConnector(c));
        FlagRecordingTransport transport = recorder!.Transport!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        long sentBefore = DatagramKit.Statistics(h.Client).DatagramsSent;
        transport.RefuseDatagrams = TransportStatus.Failed;
        SendResult alone = h.Client.SendCopy(new SendHeader(2), [1], SendOptions.Tracked);
        h.Client.Flush();
        Assert.Equal(DeliveryStatus.Failed, h.Client.GetDeliveryStatus(alone.Token));
        SendResult first = h.Client.SendCopy(new SendHeader(2), [2], SendOptions.Tracked);
        SendResult second = h.Client.SendCopy(new SendHeader(2), [3], SendOptions.Tracked);
        h.Client.Flush();
        Assert.Equal(DeliveryStatus.Failed, h.Client.GetDeliveryStatus(first.Token));
        Assert.Equal(DeliveryStatus.Failed, h.Client.GetDeliveryStatus(second.Token));
        Assert.Equal(2, transport.Refused);
        Assert.Equal(0, DatagramKit.ChannelStats(h.Client, 2).Sent);
        Assert.Equal(0, DatagramKit.ChannelStats(h.Client, 2).BytesSent);
        Assert.Equal(sentBefore, DatagramKit.Statistics(h.Client).DatagramsSent);

        transport.RefuseDatagrams = null;
        h.Client.SendCopy(new SendHeader(2), [4]);
        Assert.True(h.RunUntil(() => got.Count == 1));
        Assert.Equal(4, got[0].Payload[0]);
        Assert.Equal(1, DatagramKit.ChannelStats(h.Client, 2).Sent);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(h.Client).SendEntriesInUse == 0));
        Assert.Equal(0, DatagramKit.Statistics(h.Client).SendBytesOutstanding);
    }

    [Fact]
    public void Messages_Wait_While_Datagrams_Are_Unavailable_And_The_Limit_Is_Checked_At_Admission()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        DatagramEngine engine = (DatagramEngine)h.Client.Core.GetEngine(ChannelMode.UnreliableUnordered)!;
        int channel = h.Client.Core.ChannelIndexOf(2);
        long sent = DatagramKit.Statistics(h.Client).DatagramsSent;

        // The test thread stands in for the transport thread.
        h.Client.TransportSink.OnDatagramCapabilityChanged(false, 0);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), [1]).Status);
        h.Client.Flush();
        Assert.Equal(1, engine.QueuedMessages(channel));
        Assert.Equal(sent, DatagramKit.Statistics(h.Client).DatagramsSent);

        h.Client.TransportSink.OnDatagramCapabilityChanged(true, 100);
        Assert.Equal(SendStatus.TooLarge, h.Client.SendCopy(new SendHeader(2), new byte[100]).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), new byte[99]).Status);
        h.Client.Flush();
        Assert.Equal(0, engine.QueuedMessages(channel));
        Assert.Equal(sent + 2, DatagramKit.Statistics(h.Client).DatagramsSent);
        Assert.True(h.RunUntil(() => got.Count == 2));
        h.Client.TransportSink.OnDatagramCapabilityChanged(true, 1200);
    }

    [Fact]
    public void Datagram_Engines_Have_No_Streams()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        ChannelEngine engine = h.Client.Core.GetEngine(ChannelMode.UnreliableSequenced)!;
        Assert.Equal(StreamAcceptAction.Reset, engine.OnStreamOpened(new TransportStreamId(1, 1), 3, 0).Action);
        StreamMessageContext context = default;
        Assert.Equal(StreamConsumeAction.ResetStream, engine.OnStreamMessage(ref context).Action);
        engine.OnStreamClosed(new TransportStreamId(1, 1), aborted: true, 0);
        long deadline = long.MaxValue;
        engine.Tick(0, ref deadline);
        Assert.Equal(long.MaxValue, deadline);
        FlushContext flush = default;
        engine.Flush(ref flush);
        Assert.False(engine.TryCancel(0));
    }
}
