using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review of fix/group-stream-receive-limit (lens: protocol, security and limits). Each test states a property the
/// branch claims and fails where the branch does not hold it.
/// </summary>
public class ReviewGroupLimitsTests
{
    /// <summary>
    /// The fix sizes the receive records from <c>PeerCore.PeerUnidirectionalStreamLimit</c> and relies on the transport to
    /// keep the peer's open streams within that number ("without one, the transport let the peer past that limit"). That holds
    /// only where the transport's grant started at or below the session's limit. An MsQuic <em>client</em> does not start
    /// there: <c>MsQuicTransportOptions.ClientPeerUnidiStreamCount</c> defaults to 1 024, that credit is granted in the
    /// handshake, and the client's later <c>UpdatePeerStreamLimits(0, PeerUnidirectionalStreamLimit)</c> cannot take it back
    /// (QUIC never does; the simulator models exactly this: a lower limit is ignored once advertised). The simulated link
    /// below starts with the same 1 024. The server keeps MaxGroups, the client is late with its Poll, the server's streams
    /// pile up past the client's 9 records (8 groups + 1 ordered), and the "record exhausted" backstop resets groups whose
    /// messages the server already completed Delivered: the defect the branch set out to remove, in the server-to-client
    /// direction.
    /// </summary>
    [Fact]
    public void A_Late_Client_Whose_Transport_Granted_Its_Default_Stream_Credit_Loses_No_Group()
    {
        LinkOptions link = new() { PeerUnidiStreams = 1024 };
        using SessionHarness h = new(link: link, table: TestTables.Plumbing, server: GroupKit.Prompt, client: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 64;
        });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        Assert.Equal(9, client.Core.PeerUnidirectionalStreamLimit);
        List<int> got = [];
        client.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => got.Add(BitConverter.ToInt32(payload)));

        List<SendToken> tokens = SendGroupsToALateReceiver(h, server, 11, 320);
        bool all = h.RunUntil(() => got.Count == 320, 2_000_000);
        int delivered = tokens.Count(token => server.GetDeliveryStatus(token) == DeliveryStatus.Delivered);
        Assert.True(all,
            $"received {got.Count} of 320 messages; the client reset {DatagramKit.Statistics(client).StreamsReset} streams, and the server "
            + $"reported {delivered} of them Delivered");
        Assert.Equal(0, DatagramKit.Statistics(client).StreamsReset);
    }

    private static readonly ChannelTable GroupAndLatest = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(11, "events", ChannelMode.ReliableUnordered)
        .Add(12, "state", ChannelMode.ReliableLatest)
        .Build();

    /// <summary>
    /// PROTOCOL.md §7 (new text): when a late receiver's ReliableUnordered streams hold every stream slot of the connection,
    /// "a group, a large value or a transfer that needs a new stream <b>waits</b> — on any channel — until that channel is
    /// read". A large ReliableLatest value does not wait. Its start is refused asynchronously (MsQuic and the simulator both
    /// report the refusal through OnStreamStarted and cancel the send), the canceled completion is retransmitted "at once",
    /// and every refused start is one of the version's sixteen transmissions (<c>ReliableLatestEngine.MaxTransmissions</c>):
    /// the value completes <see cref="DeliveryStatus.Failed"/> while the receiver is merely late, and never arrives. Before
    /// the branch the receiver kept the ReliableLatest channel's share of the stream limit free (it reset the excess group
    /// streams), so the branch moved the loss from the group channel to the latest channel.
    /// </summary>
    [Fact]
    public void A_Large_Latest_Value_Waits_While_A_Late_Receiver_Holds_Every_Stream_Slot()
    {
        using SessionHarness h = new(table: GroupAndLatest, client: GroupKit.Prompt, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 64;
        });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int limit = server.Core.PeerUnidirectionalStreamLimit;
        List<int> groups = [];
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> values = [];
        server.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => groups.Add(BitConverter.ToInt32(payload)));
        server.RegisterHandler(12, LatestKit.Collect(values));

        // The server is late with its Poll: the client's group streams pile up at it until they hold every stream slot.
        SendGroupsToALateReceiver(h, client, 11, 320);
        Assert.Equal(limit, GroupKit.OpenPeerGroups(server, 11));

        // A large value (it does not fit a datagram, so it needs a stream of its own) is set while the server is still late.
        SendResult sent = client.SendCopy(new SendHeader(12, 7), LatestKit.Payload(1, 4_000), SendOptions.Tracked);
        Assert.True(sent.IsAdmitted);
        long failedAfter = -1;
        long start = h.Network.NowMicros;
        for (int step = 0; step < 2_000; step++)
        {
            client.Poll();
            client.Flush();
            h.Network.Advance(1_000);
            if (failedAfter < 0 && client.GetDeliveryStatus(sent.Token) == DeliveryStatus.Failed)
            {
                failedAfter = h.Network.NowMicros - start;
            }
        }

        // The server catches up: every group arrives, the slots come back, and the value that waited goes out.
        Assert.True(h.RunUntil(() => groups.Count == 320, 2_000_000), $"received {groups.Count} of 320 group messages");
        bool arrived = h.RunUntil(() => values.Count == 1, 5_000_000);
        Assert.True(arrived,
            $"the large value never arrived: its token is {client.GetDeliveryStatus(sent.Token)}"
            + (failedAfter >= 0 ? $", Failed {failedAfter / 1000} ms into the receiver's hitch" : string.Empty)
            + $"; the client counted {GroupKit.Stats(client, 12).Sent} transmissions of it");
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(sent.Token));
    }

    /// <summary>The same sender loop as <c>GroupStreamTests.SendGroupsToALateReceiver</c>: one group per sixteen messages, 4 ms apart, the receiver not polled.</summary>
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
}
