using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The ReliableLatest receive side against a hand-written peer: coalesced LatestAck datagrams (PROTOCOL.md §2.3), a
/// <c>LatestReject</c> for every local drop reason, the control-stream fallback, and the rule that an ack naming a channel
/// of another mode is a protocol violation.
/// </summary>
public class LatestReceiveTests
{
    private static readonly ChannelTable Table = LatestTables.Main;

    [Fact]
    public void Acks_Of_Many_Keys_Are_Coalesced_Into_One_Datagram()
    {
        using ServerHarness h = new(table: Table, server: LatestKit.Quiet);
        Assert.True(h.Admit(), "the raw client was not admitted");
        for (uint key = 0; key < 30; key++)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(LatestKit.Frame(Table, 2, key + 1, key, LatestKit.Payload((int)key, 16))));
        }

        Assert.True(h.RunUntil(() => LatestKit.ControlDatagrams(h.Raw.Sink, ControlType.LatestAck) > 0, 1_000_000), "no ack arrived");
        h.Run(50_000);
        Assert.Equal(1, LatestKit.ControlDatagrams(h.Raw.Sink, ControlType.LatestAck));
        List<LatestAckEntry> acks = LatestKit.AckEntries(h.Raw.Sink);
        Assert.Equal(30, acks.Count);
        Assert.All(acks, entry => Assert.Equal(2, entry.Channel));
        Assert.Equal(30, acks.Select(entry => entry.Key).Distinct().Count());
        Assert.All(acks, entry => Assert.Equal((uint)entry.Key + 1, entry.Version));
    }

    [Fact]
    public void Only_The_Highest_Version_Of_A_Key_Is_Acknowledged_And_Duplicates_Are_Re_Acked()
    {
        using ServerHarness h = new(table: Table, server: LatestKit.Quiet);
        Assert.True(h.Admit(), "the raw client was not admitted");

        // Three versions of one key before the server's next pass: one ack for the highest (§2.3 de-duplication).
        h.Raw.SendDatagram(LatestKit.Frame(Table, 2, 1, 4, LatestKit.Payload(1, 16)));
        h.Raw.SendDatagram(LatestKit.Frame(Table, 2, 2, 4, LatestKit.Payload(2, 16)));
        h.Raw.SendDatagram(LatestKit.Frame(Table, 2, 3, 4, LatestKit.Payload(3, 16)));
        Assert.True(h.RunUntil(() => LatestKit.AckEntries(h.Raw.Sink).Count > 0, 1_000_000), "no ack arrived");
        h.Run(50_000);
        List<LatestAckEntry> acks = LatestKit.AckEntries(h.Raw.Sink);
        Assert.Single(acks);
        Assert.Equal(3u, acks[0].Version);

        // An older version is dropped but re-acked, so a sender whose ack was lost stops retransmitting.
        h.Raw.SendDatagram(LatestKit.Frame(Table, 2, 2, 4, LatestKit.Payload(2, 16)));
        Assert.True(h.RunUntil(() => LatestKit.AckEntries(h.Raw.Sink).Count > 1, 1_000_000), "the duplicate was not re-acked");
        Assert.Equal(3u, LatestKit.AckEntries(h.Raw.Sink)[^1].Version);
        Assert.True(DatagramKit.ChannelStats(h.Server!, 2).Dropped >= 1);
    }

    [Fact]
    public void Every_Local_Drop_Reason_Is_Reported_With_A_LatestReject()
    {
        // A small receive budget and a channel with four keys make every reason of PROTOCOL.md §2.3 reachable.
        using ServerHarness h = new(table: Table, server: o =>
        {
            LatestKit.Quiet(o);
            o.ReceiveBudgetBytes = 4096;
        });
        Assert.True(h.Admit(), "the raw client was not admitted");

        // 4: the key table of channel 6 holds four keys and never evicts.
        for (ulong key = 0; key < 5; key++)
        {
            h.Raw.SendDatagram(LatestKit.Frame(Table, 6, 1, key, LatestKit.Payload(1, 8)));
        }

        // 3: a compressed value whose decoded size could never be staged by this peer.
        h.Raw.SendDatagram(LatestKit.Frame(Table, 5, 1, 1, LatestKit.Payload(2, 100), rawLength: 8192));

        // 1: no receive buffer left (nothing polls the server, so the mailboxes hold what arrived).
        for (ulong key = 100; key < 106; key++)
        {
            h.Raw.SendDatagram(LatestKit.Frame(Table, 2, 1, key, LatestKit.Payload(3, 1000)));
        }

        // 2: a group stream whose value is larger than anything this peer can stage.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(LatestKit.GroupStream(Table, 2, 1, 200, new byte[6000]), out _, fin: true));

        Assert.True(h.RunUntil(() => Reasons(h).Count >= 4, 2_000_000), $"reasons seen: {string.Join(", ", Reasons(h))}");
        HashSet<LatestRejectReason> reasons = Reasons(h);
        Assert.Contains(LatestRejectReason.KeyTableFull, reasons);
        Assert.Contains(LatestRejectReason.DecodeError, reasons);
        Assert.Contains(LatestRejectReason.RingFull, reasons);
        Assert.Contains(LatestRejectReason.TooLarge, reasons);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    private static HashSet<LatestRejectReason> Reasons(ServerHarness h) =>
        [.. LatestKit.RejectEntries(h.Raw.Sink).Select(entry => entry.Reason)];

    [Fact]
    public void An_Ack_For_A_Channel_Of_Another_Mode_Is_A_Protocol_Violation_On_The_Control_Stream()
    {
        using ServerHarness h = new(table: Table, server: LatestKit.Quiet);
        Assert.True(h.Admit(), "the raw client was not admitted");
        byte[] buffer = new byte[64];
        LatestAckBatchWriter writer = new(buffer, ControlCarrier.Stream);

        // Channel 3 is UnreliableUnordered, so no ReliableLatest engine owns it.
        Assert.True(writer.TryAdd(new LatestAckEntry(3, 1, 1)));
        h.Raw.SendControl(buffer.AsSpan(0, writer.Finish()).ToArray());
        Assert.True(h.RunUntil(() => h.Raw.IsClosed, 2_000_000), "the connection stayed open");
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }

    [Fact]
    public void A_Malformed_Ack_Datagram_Is_Dropped_And_Counted()
    {
        using ServerHarness h = new(table: Table, server: LatestKit.Quiet);
        Assert.True(h.Admit(), "the raw client was not admitted");
        long before = h.Statistics().MalformedDatagrams;

        // A LatestAck datagram naming a channel of another mode is dropped, and the connection survives (PROTOCOL.md §6).
        byte[] buffer = new byte[64];
        LatestAckBatchWriter writer = new(buffer, ControlCarrier.Datagram);
        Assert.True(writer.TryAdd(new LatestAckEntry(3, 1, 1)));
        h.Raw.SendDatagram(buffer.AsSpan(0, writer.Finish()).ToArray());
        h.Run(100_000);
        Assert.Equal(PeerState.Connected, h.Server!.State);
        Assert.True(h.Statistics().MalformedDatagrams > before);
    }

    [Fact]
    public void Acks_Fall_Back_To_The_Control_Stream_When_No_Datagram_Can_Be_Sent()
    {
        // The client's transport refuses every datagram, so its acks for the server's values go on the control stream.
        FlagRecordingConnector connector = null!;
        using SessionHarness h = new(table: LatestTables.Single, client: LatestKit.Quiet, server: LatestKit.Quiet,
            connector: inner => connector = new FlagRecordingConnector(inner));
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Client.RegisterHandler(2, LatestKit.Collect(received));
        connector.Transport!.RefuseDatagrams = TransportStatus.OutOfMemory;

        SendResult result = h.Server!.SendCopy(new SendHeader(2, 1), LatestKit.Payload(1, 32), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        Assert.True(h.RunUntil(() => h.Server.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {h.Server.GetDeliveryStatus(result.Token)}");
        Assert.Single(received);
        Assert.True(connector.Transport.Refused > 0, "no datagram was refused");
        Assert.True(DatagramKit.Statistics(h.Client).ControlSendFailures > 0, "the ack datagram was not refused, so no fallback ran");
    }
}
