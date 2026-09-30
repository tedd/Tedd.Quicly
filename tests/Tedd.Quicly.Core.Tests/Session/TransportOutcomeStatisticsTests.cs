using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// What the transport did with datagrams after they were handed to it is visible in statistics: per channel in message
/// units (<see cref="ChannelStatistics.TransportCanceled"/>, <see cref="ChannelStatistics.TransportLost"/>) and per peer in
/// datagram units (<see cref="PeerStatistics.DatagramsAcknowledged"/>, <see cref="PeerStatistics.DatagramsLost"/>,
/// <see cref="PeerStatistics.DatagramsCanceled"/>), for loose messages, packed containers (finished by the packer itself or
/// routed to their owners), fragments, and carriers that report no datagram states. A cancel caused by the connection
/// closing is not counted (PROTOCOL.md §4.3, §4.5; docs/design/session-layer.md §7.1).
/// </summary>
public class TransportOutcomeStatisticsTests
{
    private static readonly ChannelTable Table = DatagramTables.Main;

    /// <summary>
    /// A link whose serializer is busy for 8 ms per 1 000-byte datagram: a datagram sent with <c>CancelOnBlocked</c> while
    /// another one is being serialized is canceled by the simulator instead of queued.
    /// </summary>
    private static SessionHarness BusyLink(LinkOptions? link = null, Action<PeerOptions>? client = null,
        Func<Tedd.Quicly.Core.Transport.ITransportConnector, Tedd.Quicly.Core.Transport.ITransportConnector>? connector = null)
    {
        SessionHarness h = new(link: link ?? new LinkOptions { BandwidthBitsPerSecond = 1_000_000 }, table: Table,
            client: o =>
            {
                DatagramKit.Quiet(o);
                client?.Invoke(o);
            },
            server: DatagramKit.Quiet, connector: connector);

        // Let the handshake's last control datagram leave the serializer first.
        h.Run(10_000);
        return h;
    }

    private static bool Settled(QuiclyPeer peer, params SendToken[] tokens)
    {
        foreach (SendToken token in tokens)
        {
            if (peer.GetDeliveryStatus(token) == DeliveryStatus.Pending)
            {
                return false;
            }
        }

        return true;
    }

    [Fact]
    public void A_Loose_Datagram_The_Transport_Cancels_Is_Counted()
    {
        using SessionHarness h = BusyLink();
        QuiclyPeer client = h.Client;
        PeerStatistics before = DatagramKit.Statistics(client);
        long canceledBefore = DatagramKit.LinkStatistics(client).DatagramsCanceled;
        SendToken first = client.SendCopy(new SendHeader(2), new byte[1_000], SendOptions.Tracked).Token;
        client.Flush();
        SendToken second = client.SendCopy(new SendHeader(2), new byte[1_000], SendOptions.Tracked).Token;
        client.Flush();
        Assert.True(h.RunUntil(() => Settled(client, first, second)));
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(first));
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(second));

        // The cancelled message was handed to the transport and stays counted as sent; the local Expired counter is for
        // messages that never reached the transport.
        ChannelStatistics channel = DatagramKit.ChannelStats(client, 2);
        Assert.Equal(2, channel.Sent);
        Assert.Equal(1, channel.TransportCanceled);
        Assert.Equal(0, channel.TransportLost);
        Assert.Equal(0, channel.Expired);

        PeerStatistics after = DatagramKit.Statistics(client);
        Assert.Equal(2, after.DatagramsSent - before.DatagramsSent);
        Assert.Equal(1, after.DatagramsCanceled - before.DatagramsCanceled);
        Assert.Equal(DatagramKit.LinkStatistics(client).DatagramsCanceled - canceledBefore, after.DatagramsCanceled - before.DatagramsCanceled);
        Assert.Equal(1, after.DatagramsAcknowledged - before.DatagramsAcknowledged);
        Assert.Equal(0, after.DatagramsLost - before.DatagramsLost);
    }

    [Fact]
    public void Members_Of_A_Canceled_Direct_Container_Are_Counted_Per_Message_And_Once_Per_Datagram()
    {
        using SessionHarness h = BusyLink();
        QuiclyPeer client = h.Client;
        PeerStatistics before = DatagramKit.Statistics(client);

        // The blocker occupies the serializer; the container of the next pass meets it busy.
        Assert.True(client.SendCopy(new SendHeader(2), new byte[1_000]).IsAdmitted);
        client.Flush();
        SendToken a = client.SendCopy(new SendHeader(2), [1], SendOptions.Tracked).Token;
        SendToken b = client.SendCopy(new SendHeader(2), [2], SendOptions.Tracked).Token;
        SendToken c = client.SendCopy(new SendHeader(3, 7), [3], SendOptions.Tracked).Token;
        client.Flush();
        Assert.Equal(1, DatagramKit.Statistics(client).ContainersSent - before.ContainersSent);
        Assert.True(h.RunUntil(() => Settled(client, a, b, c)));
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(a));
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(b));
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(c));

        ChannelStatistics events = DatagramKit.ChannelStats(client, 2);
        Assert.Equal(3, events.Sent);
        Assert.Equal(2, events.TransportCanceled);
        Assert.Equal(0, events.TransportLost);
        ChannelStatistics moves = DatagramKit.ChannelStats(client, 3);
        Assert.Equal(1, moves.Sent);
        Assert.Equal(1, moves.TransportCanceled);
        Assert.Equal(0, moves.TransportLost);

        // One datagram was dropped, whatever it carried.
        Assert.Equal(1, DatagramKit.Statistics(client).DatagramsCanceled - before.DatagramsCanceled);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).SendEntriesInUse == 0));
    }

    [Fact]
    public void A_Fragmented_Message_With_Canceled_Fragments_Counts_Each_Fragment()
    {
        using SessionHarness h = BusyLink();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(13, Handlers.Collect(got));
        PeerStatistics before = DatagramKit.Statistics(client);

        // Three fragments, each its own datagram: the first takes the idle serializer, the other two meet it busy.
        SendToken token = client.SendCopy(new SendHeader(13), new byte[FragmentKit.LengthFor(Table[13]!, 3)], SendOptions.Tracked).Token;
        client.Flush();
        Assert.Equal(3, DatagramKit.Statistics(client).FragmentsSent - before.FragmentsSent);
        Assert.True(h.RunUntil(() => Settled(client, token)));
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(token));

        ChannelStatistics channel = DatagramKit.ChannelStats(client, 13);
        Assert.Equal(3, channel.Sent);
        Assert.Equal(2, channel.TransportCanceled);
        Assert.Equal(0, channel.TransportLost);
        PeerStatistics after = DatagramKit.Statistics(client);
        Assert.Equal(2, after.DatagramsCanceled - before.DatagramsCanceled);
        Assert.Equal(1, after.DatagramsAcknowledged - before.DatagramsAcknowledged);
        Assert.Equal(0, after.SendEntriesInUse);
        Assert.Equal(0, after.SendBytesOutstanding);

        // The fragment that travelled is wasted: the receiver gives the partial up when another fragment of the channel
        // arrives after the reassembly window.
        h.Run(FragmentKit.Window(server) + 50_000);
        Assert.True(client.SendCopy(new SendHeader(13), new byte[FragmentKit.LengthFor(Table[13]!, 2)]).IsAdmitted);
        client.Flush();
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(server).ReassembliesExpired == 1));
        Assert.Empty(got);
    }

    [Fact]
    public void A_Canceled_Routed_Container_Counts_Its_Fragment_And_Its_Message()
    {
        using SessionHarness h = BusyLink();
        QuiclyPeer client = h.Client;
        PeerStatistics before = DatagramKit.Statistics(client);

        // Two fragments of about 600 bytes never share a datagram, but the second leaves room for a small message: the first
        // fragment goes out alone and takes the serializer, the second and the small message share a container that meets
        // it busy. A fragment has no DirectCompletion, so the container's members are routed to their engine.
        SendToken fragmented = client.SendCopy(new SendHeader(13), new byte[FragmentKit.LengthFor(Table[13]!, 2)], SendOptions.Tracked).Token;
        SendToken small = client.SendCopy(new SendHeader(13), [1, 2, 3], SendOptions.Tracked).Token;
        client.Flush();
        Assert.Equal(1, DatagramKit.Statistics(client).ContainersSent - before.ContainersSent);
        Assert.True(h.RunUntil(() => Settled(client, fragmented, small)));
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(fragmented));
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(small));

        ChannelStatistics channel = DatagramKit.ChannelStats(client, 13);
        Assert.Equal(3, channel.Sent);
        Assert.Equal(2, channel.TransportCanceled);
        Assert.Equal(0, channel.TransportLost);
        Assert.Equal(1, DatagramKit.Statistics(client).DatagramsCanceled - before.DatagramsCanceled);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).SendEntriesInUse == 0));
    }

    [Fact]
    public void Members_Of_A_Lost_Container_Are_Counted_Lost()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000 }, table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        h.Run(10_000);
        PeerStatistics before = DatagramKit.Statistics(client);
        DatagramKit.TransportOf(client).DropNextDatagrams(1);
        SendToken unordered = client.SendCopy(new SendHeader(2), [1], SendOptions.Tracked).Token;
        SendToken sequenced = client.SendCopy(new SendHeader(3, 7), [2], SendOptions.Tracked).Token;
        Assert.True(client.SendCopy(new SendHeader(2), [3]).IsAdmitted);
        Assert.True(client.SendCopy(new SendHeader(3, 8), [4]).IsAdmitted);
        client.Flush();
        Assert.Equal(1, DatagramKit.Statistics(client).ContainersSent - before.ContainersSent);
        Assert.True(h.RunUntil(() => Settled(client, unordered, sequenced)));
        Assert.Equal(DeliveryStatus.Lost, client.GetDeliveryStatus(unordered));
        Assert.Equal(DeliveryStatus.Lost, client.GetDeliveryStatus(sequenced));
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).SendEntriesInUse == 0));

        foreach (ushort id in new ushort[] { 2, 3 })
        {
            ChannelStatistics channel = DatagramKit.ChannelStats(client, id);
            Assert.Equal(2, channel.Sent);
            Assert.Equal(2, channel.TransportLost);
            Assert.Equal(0, channel.TransportCanceled);
        }

        PeerStatistics after = DatagramKit.Statistics(client);
        Assert.Equal(1, after.DatagramsLost - before.DatagramsLost);
        Assert.Equal(0, after.DatagramsCanceled - before.DatagramsCanceled);
        Assert.Equal(0, after.DatagramsAcknowledged - before.DatagramsAcknowledged);
    }

    [Fact]
    public void A_Lost_Fragment_Is_Counted_Lost()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000 }, table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        h.Run(10_000);
        PeerStatistics before = DatagramKit.Statistics(client);
        DatagramKit.TransportOf(client).DropNextDatagrams(1);
        SendToken token = client.SendCopy(new SendHeader(13), new byte[FragmentKit.LengthFor(Table[13]!, 3)], SendOptions.Tracked).Token;
        client.Flush();
        Assert.True(h.RunUntil(() => Settled(client, token)));
        Assert.Equal(DeliveryStatus.Lost, client.GetDeliveryStatus(token));

        ChannelStatistics channel = DatagramKit.ChannelStats(client, 13);
        Assert.Equal(3, channel.Sent);
        Assert.Equal(1, channel.TransportLost);
        Assert.Equal(0, channel.TransportCanceled);
        PeerStatistics after = DatagramKit.Statistics(client);
        Assert.Equal(1, after.DatagramsLost - before.DatagramsLost);
        Assert.Equal(2, after.DatagramsAcknowledged - before.DatagramsAcknowledged);
        Assert.Equal(0, after.DatagramsCanceled - before.DatagramsCanceled);
    }

    [Fact]
    public void Cancels_At_Close_Are_Not_Counted()
    {
        // The transport cancels everything it still holds when the connection closes, including datagrams already on the
        // wire: that says nothing about congestion, so none of it is counted. The link is slow and the transport is made to
        // queue (no CancelOnBlocked), so all five datagrams are still held when the close starts.
        using SessionHarness h = BusyLink(
            link: new LinkOptions { BandwidthBitsPerSecond = 8_000 },
            client: o => o.CloseLinger = TimeSpan.Zero,
            connector: c => new FlagRecordingConnector(c, cancelOnBlocked: false));
        QuiclyPeer client = h.Client;
        h.Run(2_000_000);
        PeerStatistics before = DatagramKit.Statistics(client);
        SimulatedTransport transport = DatagramKit.TransportOf(client);
        transport.GetLinkStatistics(out SimulatedLinkStatistics linkBefore);
        SendToken[] tokens = new SendToken[5];
        for (int i = 0; i < tokens.Length; i++)
        {
            tokens[i] = client.SendCopy(new SendHeader(2), new byte[900], SendOptions.Tracked).Token;
        }

        client.Flush();
        Assert.Equal(5, DatagramKit.Statistics(client).DatagramsSent - before.DatagramsSent);
        client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.All(tokens, t => Assert.Equal(DeliveryStatus.Disconnected, client.GetDeliveryStatus(t)));
        transport.GetLinkStatistics(out SimulatedLinkStatistics linkAfter);
        Assert.True(linkAfter.DatagramsCanceled - linkBefore.DatagramsCanceled >= 5, "the simulator did not cancel the queued datagrams");

        ChannelStatistics channel = DatagramKit.ChannelStats(client, 2);
        Assert.Equal(5, channel.Sent);
        Assert.Equal(0, channel.TransportCanceled);
        Assert.Equal(0, channel.TransportLost);
        PeerStatistics after = DatagramKit.Statistics(client);
        Assert.Equal(0, after.DatagramsCanceled - before.DatagramsCanceled);
        Assert.Equal(0, after.DatagramsLost - before.DatagramsLost);
    }

    [Fact]
    public void Without_Send_State_Reporting_Only_Cancels_Are_Counted()
    {
        // A carrier that reports no datagram states still reports a datagram it never sent; loss and acknowledgement stay
        // unknown, so a send ends Sent and the counters for them stay 0.
        using SessionHarness h = BusyLink(link: new LinkOptions { BandwidthBitsPerSecond = 1_000_000, DatagramSendStateReporting = false });
        QuiclyPeer client = h.Client;
        PeerStatistics before = DatagramKit.Statistics(client);
        SendToken a = client.SendCopy(new SendHeader(2), new byte[1_000], SendOptions.Tracked).Token;
        client.Flush();
        SendToken b = client.SendCopy(new SendHeader(2), new byte[1_000], SendOptions.Tracked).Token;
        client.Flush();
        h.Run(50_000);
        DatagramKit.TransportOf(client).DropNextDatagrams(1);
        SendToken c = client.SendCopy(new SendHeader(2), new byte[1_000], SendOptions.Tracked).Token;
        client.Flush();
        h.Run(50_000);
        Assert.Equal(DeliveryStatus.Sent, client.GetDeliveryStatus(a));
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(b));
        Assert.Equal(DeliveryStatus.Sent, client.GetDeliveryStatus(c));

        ChannelStatistics channel = DatagramKit.ChannelStats(client, 2);
        Assert.Equal(3, channel.Sent);
        Assert.Equal(1, channel.TransportCanceled);
        Assert.Equal(0, channel.TransportLost);
        PeerStatistics after = DatagramKit.Statistics(client);
        Assert.Equal(1, after.DatagramsCanceled - before.DatagramsCanceled);
        Assert.Equal(0, after.DatagramsLost - before.DatagramsLost);
        Assert.Equal(0, after.DatagramsAcknowledged - before.DatagramsAcknowledged);
    }

    [Theory]
    [InlineData(CompletionMode.PollOnly)]
    [InlineData(CompletionMode.ThreadPool)]
    public void Outcome_Counters_Balance_At_Quiescence(CompletionMode mode)
    {
        // Every datagram the scheduler handed over ends acknowledged, lost or cancelled, and what the transport neither
        // cancelled nor lost is exactly what the other end received — per channel, in the unit of Sent.
        using SessionHarness h = BusyLink(
            link: new LinkOptions { BandwidthBitsPerSecond = 2_000_000, LossPercent = 10, DelayMicros = 1_000 },
            client: o => o.CompletionMode = mode);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        MessageHandler count = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++;
        server.RegisterHandler(2, count);
        server.RegisterHandler(3, count);
        server.RegisterHandler(13, count);
        h.Run(50_000);
        PeerStatistics before = DatagramKit.Statistics(client);
        PeerStatistics serverBefore = DatagramKit.Statistics(server);
        SimulatedLinkStatistics linkBefore = DatagramKit.LinkStatistics(client);
        byte[] small = new byte[120];
        byte[] fragmented = new byte[FragmentKit.LengthFor(Table[13]!, 2)];
        int sends = 0;
        for (int step = 0; sends < 500; step++)
        {
            // Two or three messages per flush (so some travel packed), a fragmented one now and then, and a second flush in
            // the same instant every few steps (which meets the serializer busy).
            Assert.True(client.SendCopy(new SendHeader(2), small).IsAdmitted);
            Assert.True(client.SendCopy(new SendHeader(3, (ulong)(step % 9)), small).IsAdmitted);
            sends += 2;
            if (step % 3 == 0)
            {
                Assert.True(client.SendCopy(new SendHeader(13), fragmented).IsAdmitted);
                sends++;
            }

            client.Flush();
            if (step % 4 == 0)
            {
                Assert.True(client.SendCopy(new SendHeader(2), small).IsAdmitted);
                sends++;
                client.Flush();
            }

            h.Run(step % 5 == 0 ? 1_000 : 12_000);
        }

        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).SendEntriesInUse == 0));
        h.Run(50_000);
        PeerStatistics after = DatagramKit.Statistics(client);
        PeerStatistics serverAfter = DatagramKit.Statistics(server);
        SimulatedLinkStatistics linkAfter = DatagramKit.LinkStatistics(client);
        long sent = after.DatagramsSent - before.DatagramsSent;
        long acknowledged = after.DatagramsAcknowledged - before.DatagramsAcknowledged;
        long lost = after.DatagramsLost - before.DatagramsLost;
        long canceled = after.DatagramsCanceled - before.DatagramsCanceled;
        Assert.True(canceled > 0, "the workload produced no cancel");
        Assert.True(lost > 0, "the workload produced no loss");
        Assert.True(acknowledged > 0, "the workload delivered nothing");
        Assert.Equal(sent, acknowledged + lost + canceled);
        Assert.Equal(linkAfter.DatagramsCanceled - linkBefore.DatagramsCanceled, canceled);
        Assert.Equal(linkAfter.DatagramsLost - linkBefore.DatagramsLost, lost);

        foreach (ushort id in new ushort[] { 2, 3 })
        {
            ChannelStatistics sender = DatagramKit.ChannelStats(client, id);
            ChannelStatistics receiver = DatagramKit.ChannelStats(server, id);
            Assert.Equal(0, receiver.Dropped);
            Assert.Equal(0, receiver.RingDrops);
            Assert.Equal(0, receiver.OutOfBuffers);
            Assert.True(sender.TransportCanceled > 0 && sender.TransportLost > 0, $"channel {id}: {sender.TransportCanceled} cancelled, {sender.TransportLost} lost");
            Assert.Equal(receiver.Received, sender.Sent - sender.TransportCanceled - sender.TransportLost);
        }

        // A fragment is one Sent of its channel; the receiver counts the fragments it got before reassembly.
        ChannelStatistics fragments = DatagramKit.ChannelStats(client, 13);
        Assert.True(fragments.TransportCanceled > 0 && fragments.TransportLost > 0, $"channel 13: {fragments.TransportCanceled} cancelled, {fragments.TransportLost} lost");
        Assert.Equal(serverAfter.FragmentsReceived - serverBefore.FragmentsReceived, fragments.Sent - fragments.TransportCanceled - fragments.TransportLost);
        Assert.Equal(0, serverAfter.FragmentsDropped - serverBefore.FragmentsDropped);
        Assert.True(received > 0);
        Assert.Equal(PeerState.Connected, client.State);
    }
}
