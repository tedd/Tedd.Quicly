using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Second adversarial review of fix/group-stream-receive-limit: the claim of PROTOCOL.md §7 (12119f6) that the receive-side
/// limit resets which remain for large ReliableLatest values and for Bulk transfers cannot be reached by a sender that keeps
/// the limit, through a receiver that is late with its Poll — nor through the larger capacity a transport's own grant now
/// gives the receiver. These are the tests that would show a loss; they pass on the branch ("review: guard").
/// </summary>
public class ReviewGroup2LatestGuardTests
{
    private static readonly ChannelTable GroupAndLatest = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(11, "events", ChannelMode.ReliableUnordered)
        .Add(12, "state", ChannelMode.ReliableLatest)
        .Add(13, "one-at-a-time", ChannelMode.ReliableLatest, o => o.MaxGroups = 1)
        .Build();

    private static readonly ChannelTable GroupAndBulk = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(5, "world", ChannelMode.Bulk, o =>
        {
            o.Priority = 0;
            o.MaxGroups = 1;
        })
        .Add(11, "events", ChannelMode.ReliableUnordered)
        .Build();

    /// <summary>
    /// Large values on many keys of two ReliableLatest channels (one with a single stream), set as fast as the sender's own
    /// per-channel cap lets them out, while the receiver is late again and again and its group streams pile up. The sender
    /// retransmits unacknowledged values on new streams the whole time. The receiver must reset none of them
    /// <c>LimitExceeded</c>, every key must end at its last value, and no value may end <c>Failed</c>.
    /// </summary>
    [Theory]
    [InlineData(0, false)]
    [InlineData(1024, false)]
    [InlineData(0, true)]
    [InlineData(1024, true)]
    public void Guard_Large_Latest_Values_To_A_Late_Receiver_Are_Never_Reset_By_The_Receive_Limit(int transportGrant, bool clientIsReceiver)
    {
        LinkOptions link = new() { DelayMicros = 1_000, PeerUnidiStreams = (ushort)transportGrant };
        using SessionHarness h = new(link: link, table: GroupAndLatest,
            client: o =>
            {
                GroupKit.Prompt(o);
                LatestKit.Roomy(o);
                o.ReceiveRingCapacity = 64;
            },
            server: o =>
            {
                GroupKit.Prompt(o);
                LatestKit.Roomy(o);
                o.ReceiveRingCapacity = 64;
            });
        QuiclyPeer sender = clientIsReceiver ? h.Server! : h.Client;
        QuiclyPeer receiver = clientIsReceiver ? h.Client : h.Server!;
        int groups = 0;
        Dictionary<(ushort Channel, ulong Key), int> got = [];
        receiver.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => groups++);
        MessageHandler latest = (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
            got[(header.Channel, header.Key)] = BitConverter.ToInt32(payload);
        receiver.RegisterHandler(12, latest);
        receiver.RegisterHandler(13, latest);

        Dictionary<(ushort Channel, ulong Key), int> set = [];
        List<SendToken> tokens = [];
        int groupsSent = 0;
        int value = 0;
        int failed = 0;
        int mostHeld = 0;
        byte[] large = new byte[6_000];

        // A finished token's slot is reused by a later send, so the statuses are collected as the values finish.
        void Harvest()
        {
            for (int i = tokens.Count - 1; i >= 0; i--)
            {
                DeliveryStatus status = sender.GetDeliveryStatus(tokens[i]);
                if (status is DeliveryStatus.Pending or DeliveryStatus.Sent)
                {
                    continue;
                }

                failed += status is DeliveryStatus.Delivered or DeliveryStatus.Superseded ? 0 : 1;
                tokens.RemoveAt(i);
            }
        }

        for (int hitch = 0; hitch < 6; hitch++)
        {
            // The receiver is not polled for 300 ms of link time; the sender keeps its own pace.
            for (int step = 0; step < 300; step++)
            {
                if (sender.SendCopy(new SendHeader(11), DatagramKit.Payload(groupsSent, 4)).IsAdmitted)
                {
                    groupsSent++;
                }

                Harvest();
                if (step % 3 == 0)
                {
                    ushort channel = (step % 6) == 0 ? (ushort)12 : (ushort)13;
                    ulong key = (ulong)(step / 6 % 5);
                    BitConverter.TryWriteBytes(large, ++value);
                    SendResult result = sender.SendCopy(new SendHeader(channel, key), large, SendOptions.Tracked);
                    if (result.IsAdmitted)
                    {
                        set[(channel, key)] = value;
                        tokens.Add(result.Token);
                    }
                }

                sender.Poll();
                sender.Flush();
                h.Network.Advance(1_000);
            }

            mostHeld = Math.Max(mostHeld, GroupKit.OpenPeerGroups(receiver, 11));

            // It catches up for a moment, not necessarily all the way.
            h.Run(40_000);
        }

        bool settled = h.RunUntil(() => groups == groupsSent && set.All(pair => got.TryGetValue(pair.Key, out int id) && id == pair.Value), 20_000_000);
        PeerStatistics statistics = DatagramKit.Statistics(receiver);
        Assert.True(settled && statistics.StreamsReset == 0,
            $"groups {groups} of {groupsSent}; {set.Count(pair => got.TryGetValue(pair.Key, out int id) && id == pair.Value)} of {set.Count} keys at their last value; "
            + $"the receiver reset {statistics.StreamsReset} streams");
        Assert.Equal(0, DatagramKit.Statistics(sender).StreamsReset);
        h.Run(200_000);
        Harvest();
        Assert.True(failed == 0 && tokens.Count == 0, $"{failed} values ended neither Delivered nor Superseded; {tokens.Count} are still pending");

        // Not vacuous: the receiver held more group streams than the channel's MaxGroups, and the large values travelled on streams.
        Assert.True(mostHeld > 8, $"the receiver held at most {mostHeld} group streams");
        Assert.True(DatagramKit.ChannelStats(sender, 12).Sent + DatagramKit.ChannelStats(sender, 13).Sent >= 60, "too few large values were transmitted");
    }

    /// <summary>
    /// Bulk transfers one after another on a channel with a single stream, each started the moment the one before it has
    /// completed, to a receiver that is late with its Poll while its group streams hold the connection's stream slots. A
    /// transfer must wait and complete; none may be reset by the receiver's per-channel or per-direction limit.
    /// </summary>
    [Fact]
    public void Guard_Bulk_Transfers_To_A_Late_Receiver_Are_Never_Reset_By_The_Receive_Limit()
    {
        const int size = 96 * 1024;
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: GroupAndBulk,
            client: o =>
            {
                BulkKit.Quiet(o);
                o.GroupMinInterval = TimeSpan.Zero;
            },
            server: o =>
            {
                BulkKit.Receiver(router)(o);
                o.ReceiveRingCapacity = 64;
            });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int groups = 0;
        server.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => groups++);

        int groupsSent = 0;
        List<BulkTransfer> transfers = [];
        BulkTransfer? running = null;
        for (int hitch = 0; hitch < 8; hitch++)
        {
            for (int step = 0; step < 200; step++)
            {
                if (client.SendCopy(new SendHeader(11), DatagramKit.Payload(groupsSent, 4)).IsAdmitted)
                {
                    groupsSent++;
                }

                if (running is null || running.IsFinished)
                {
                    running = client.BeginBulkSendAsync(new BulkDescriptor(5, (ulong)transfers.Count, 1, size), new PatternSource(size)).AsTask().GetAwaiter().GetResult();
                    transfers.Add(running);
                }

                client.Poll();
                client.Flush();
                h.Network.Advance(1_000);
            }

            // The receiver catches up until the transfer in progress is through, and the next one starts against a late receiver again.
            BulkTransfer current = running!;
            Assert.True(h.RunUntil(() => current.IsFinished, 10_000_000), $"transfer {transfers.Count - 1} is {current.Status} after the receiver caught up");
        }

        Assert.True(h.RunUntil(() => groups == groupsSent && transfers.TrueForAll(transfer => transfer.IsFinished), 20_000_000), $"groups {groups} of {groupsSent}");
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(transfers.TrueForAll(transfer => transfer.Status == BulkStatus.Completed) && statistics.StreamsReset == 0,
            $"{transfers.Count} transfers: {string.Join(", ", transfers.Select(transfer => transfer.Status))}; the receiver reset {statistics.StreamsReset} streams");
        Assert.True(transfers.Count >= 8, $"only {transfers.Count} transfers ran");
        Assert.All(router.Sinks, sink => Assert.Equal(0, ((PatternSink)sink).Mismatches));
    }
}
