using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// ReliableUnordered delivery over the simulator (PROTOCOL.md §3.2, ADR 0003): group independence under loss, byte-exact
/// delivery of thousands of messages under loss, reordering, jitter and a bandwidth cap, messages delivered as they complete,
/// the group bound of <see cref="ChannelDefinition.GroupMaxBytes"/>, keys, compression and empty payloads.
/// </summary>
public class GroupDeliveryTests
{
    private static readonly ChannelTable Table = GroupTables.Main;

    [Fact]
    public void Loss_In_One_Group_Never_Delays_Another()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000 }, table: Table);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        Assert.True(h.Client.SendCopy(new SendHeader(5), OrderedKit.Payload(1, 40)).IsAdmitted);

        // The first group's only packet is lost once, so it arrives a retransmission late.
        DatagramKit.TransportOf(h.Client).LoseNextStreamPackets(1);
        h.Client.Flush();
        h.Network.Advance(2_000);
        Assert.True(h.Client.SendCopy(new SendHeader(5), OrderedKit.Payload(2, 40)).IsAdmitted);
        h.Client.Flush();
        Assert.True(h.RunUntil(() => got.Count == 2));

        // The second group travelled on its own stream, so the first group's loss delayed only the first group.
        Assert.True(OrderedKit.Matches(got[0].Payload, 2, 40), "the second group did not arrive first");
        Assert.True(OrderedKit.Matches(got[1].Payload, 1, 40));
        Assert.Equal(2ul, GroupKit.GroupsFormed(h.Client, 5));
        Assert.True(DatagramKit.LinkStatistics(h.Client).StreamRetransmissions > 0, "the link lost no stream packet");
    }

    [Fact]
    public void Thousands_Of_Messages_Of_Random_Sizes_Arrive_Byte_Exact_Under_Loss_Reorder_Jitter_And_Bandwidth()
    {
        LinkOptions link = new()
        {
            DelayMicros = 5_000,
            JitterMicros = 2_000,
            LossPercent = 5,
            ReorderPercent = 5,
            StreamLossPercent = 3,
            BandwidthBitsPerSecond = 800_000_000,
            StreamReceiveWindowBytes = 1 << 20,
        };
        using SessionHarness h = new(link: link, table: Table, client: GroupKit.Roomy, server: GroupKit.Roomy, seed: 7);
        const int Count = 3_000;
        Random random = new(4321);
        int[] sizes = new int[Count];
        for (int i = 0; i < Count; i++)
        {
            // Log-uniform over 4 B … 16 KiB (the first four bytes carry the message id).
            sizes[i] = (int)Math.Clamp(Math.Round(Math.Exp(random.NextDouble() * Math.Log(16_384))), 4, 16_384);
        }

        sizes[0] = 4;
        sizes[1] = 65_536;
        sizes[2] = 1_200;
        long total = 0;
        foreach (int size in sizes)
        {
            total += size;
        }

        bool[] seen = new bool[Count];
        int received = 0;
        string? failure = null;
        h.Server!.RegisterHandler(5, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            received++;
            if (failure is not null)
            {
                return;
            }

            if (payload.Length < 4)
            {
                failure = $"a message of {payload.Length} bytes carries no id";
                return;
            }

            int id = BitConverter.ToInt32(payload);
            if ((uint)id >= Count)
            {
                failure = $"unknown message id {id}";
            }
            else if (seen[id])
            {
                failure = $"message {id} arrived twice";
            }
            else if (payload.Length != sizes[id] || !payload.SequenceEqual(DatagramKit.Payload(id, sizes[id])))
            {
                failure = $"message {id}: {payload.Length} bytes, expected {sizes[id]}";
            }
            else
            {
                seen[id] = true;
            }
        });

        int sent = 0;
        long deadline = h.Network.NowMicros + 300_000_000;
        while (received < Count && h.Network.NowMicros < deadline)
        {
            while (sent < Count)
            {
                SendStatus status = h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(sent, sizes[sent])).Status;
                if (status != SendStatus.Admitted)
                {
                    Assert.True(status is SendStatus.QueueFull or SendStatus.OutOfBuffers, $"send {sent}: {status}");
                    break;
                }

                sent++;
            }

            h.Run(1_000);
        }

        Assert.Null(failure);
        Assert.Equal(Count, received);
        Assert.True(h.RunUntil(() => GroupKit.Stats(h.Client, 5).InFlightMessages == 0));
        ChannelStatistics sender = GroupKit.Stats(h.Client, 5);
        Assert.Equal(Count, sender.Sent);
        Assert.Equal(total, sender.BytesSent);
        Assert.Equal(0, sender.QueuedMessages);
        ChannelStatistics receiver = GroupKit.Stats(h.Server, 5);
        Assert.Equal(Count, receiver.Received);
        Assert.Equal(total, receiver.BytesReceived);
        Assert.Equal(0, DatagramKit.Statistics(h.Server).ReceiveRingDrops);
        Assert.True(DatagramKit.LinkStatistics(h.Client).StreamRetransmissions > 0, "the link lost no stream packet");
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server.State);
    }

    [Fact]
    public void A_Groups_Messages_Are_Delivered_As_They_Complete()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 1_000, BandwidthBitsPerSecond = 8_000_000 }, table: Table);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        for (int i = 0; i < 40; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(5), OrderedKit.Payload(i, 1_024)).IsAdmitted);
        }

        h.Client.Flush();

        // One flush, one group (PROTOCOL.md §3.2), and 40 KiB of it takes 40 ms on this link.
        Assert.Equal(1ul, GroupKit.GroupsFormed(h.Client, 5));
        h.Run(12_000);
        Assert.InRange(got.Count, 1, 39);
        Assert.True(h.RunUntil(() => got.Count == 40));
        for (int i = 0; i < 40; i++)
        {
            Assert.True(OrderedKit.Matches(got[i].Payload, i, 1_024), $"message {i}");
        }
    }

    [Fact]
    public void One_Flush_Of_A_Hundred_Messages_Is_One_Group_On_One_Stream()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        long before = DatagramKit.Statistics(h.Client).StreamSends;
        for (int i = 0; i < 100; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(5), OrderedKit.Payload(i, 64)).IsAdmitted);
        }

        h.Client.Flush();
        Assert.Equal(1ul, GroupKit.GroupsFormed(h.Client, 5));
        Assert.True(h.RunUntil(() => got.Count == 100));

        // 31 messages (62 segments plus the preamble) in the first carrier, 32 in the next ones: four stream sends.
        Assert.InRange(DatagramKit.Statistics(h.Client).StreamSends - before, 4, 6);
        for (int i = 0; i < 100; i++)
        {
            Assert.True(OrderedKit.Matches(got[i].Payload, i, 64), $"message {i}");
        }
    }

    [Fact]
    public void A_Group_That_Reaches_GroupMaxBytes_Is_Sealed_At_Once()
    {
        using SessionHarness h = new(table: Table);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(8, Handlers.Collect(got));
        for (int i = 0; i < 4; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(8), DatagramKit.Payload(i, 100)).IsAdmitted);
        }

        // Channel 8 bounds a group at 256 bytes: the third message opens the next group, with no flush in between.
        Assert.Equal(2ul, GroupKit.GroupsFormed(h.Client, 8));
        Assert.Equal(2, GroupKit.Groups(h.Client, 8));
        Assert.True(h.RunUntil(() => got.Count == 4));
        List<int> ids = [];
        foreach ((ReceiveHeader _, byte[] payload) in got)
        {
            Assert.Equal(100, payload.Length);
            int id = BitConverter.ToInt32(payload);
            Assert.Equal(DatagramKit.Payload(id, 100), payload);
            ids.Add(id);
        }

        ids.Sort();
        Assert.Equal([0, 1, 2, 3], ids);
    }

    [Fact]
    public void Group_Frames_Carry_Keys_Compressed_And_Empty_Payloads()
    {
        using SessionHarness h = new(table: Table);
        List<(ReceiveHeader Header, byte[] Payload)> keyed = [];
        List<(ReceiveHeader Header, byte[] Payload)> packed = [];
        List<(ReceiveHeader Header, byte[] Payload)> plain = [];
        h.Server!.RegisterHandler(6, Handlers.Collect(keyed));
        h.Server.RegisterHandler(7, Handlers.Collect(packed));
        h.Server.RegisterHandler(5, Handlers.Collect(plain));
        byte[] compressible = new byte[600];
        Assert.True(h.Client.SendCopy(new SendHeader(6, 77), OrderedKit.Payload(3, 20)).IsAdmitted);
        Assert.True(h.Client.SendCopy(new SendHeader(7), compressible).IsAdmitted);
        Assert.True(h.Client.SendCopy(new SendHeader(5), []).IsAdmitted);
        Assert.True(h.Client.SendCopy(new SendHeader(5), [7]).IsAdmitted);
        Assert.True(h.RunUntil(() => keyed.Count == 1 && packed.Count == 1 && plain.Count == 2));

        Assert.Equal(77ul, keyed[0].Header.Key);
        Assert.True(OrderedKit.Matches(keyed[0].Payload, 3, 20));

        // The payload travelled as an LZ4 block and was decoded in Poll.
        Assert.Equal(compressible, packed[0].Payload);
        Assert.Equal(600, packed[0].Header.RawLength);
        Assert.Equal(ReceiveFlags.Compressed, packed[0].Header.Flags & ReceiveFlags.Compressed);

        Assert.Empty(plain[0].Payload);
        Assert.Equal(new byte[] { 7 }, plain[1].Payload);
    }
}
