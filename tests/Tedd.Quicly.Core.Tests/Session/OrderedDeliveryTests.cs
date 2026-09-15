using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// ReliableOrdered delivery over the simulator (PROTOCOL.md §3.1, ADR 0003): order and byte-exactness under loss, jitter,
/// reordering and a bandwidth limit, messages cut into hundreds of packets, gathered stream sends, the frame's optional
/// fields, compression, gathered pages and mixed traffic.
/// </summary>
public class OrderedDeliveryTests
{
    private static readonly ChannelTable Table = OrderedTables.Main;

    [Fact]
    public void Ten_Thousand_Messages_Of_Random_Sizes_Arrive_In_Order_And_Byte_Exact_Under_Loss_Jitter_And_Bandwidth()
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
        using SessionHarness h = new(link: link, table: Table, client: OrderedKit.Roomy, server: OrderedKit.Roomy, seed: 7);
        const int Count = 10_000;
        Random random = new(1234);
        int[] sizes = new int[Count];
        for (int i = 0; i < Count; i++)
        {
            // Log-uniform over 1 B … 64 KiB.
            sizes[i] = (int)Math.Clamp(Math.Round(Math.Exp(random.NextDouble() * Math.Log(65_536))), 1, 65_536);
        }

        sizes[0] = 1;
        sizes[1] = 65_536;
        long total = 0;
        foreach (int size in sizes)
        {
            total += size;
        }

        int received = 0;
        string? failure = null;
        h.Server!.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            if (failure is null && (received >= Count || !OrderedKit.Matches(payload, received, sizes[received])))
            {
                failure = $"message {received}: {payload.Length} bytes, expected {(received < Count ? sizes[received] : -1)}";
            }

            received++;
        });
        byte[] buffer = new byte[65_536];
        int sent = 0;
        long deadline = h.Network.NowMicros + 300_000_000;
        while (received < Count && h.Network.NowMicros < deadline)
        {
            while (sent < Count)
            {
                Span<byte> payload = buffer.AsSpan(0, sizes[sent]);
                OrderedKit.Fill(payload, sent);
                SendStatus status = h.Client.SendCopy(new SendHeader(4), payload).Status;
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
        Assert.True(h.RunUntil(() => OrderedKit.Stats(h.Client, 4).InFlightMessages == 0));
        ChannelStatistics sender = OrderedKit.Stats(h.Client, 4);
        Assert.Equal(Count, sender.Sent);
        Assert.Equal(total, sender.BytesSent);
        Assert.Equal(0, sender.QueuedMessages);
        ChannelStatistics receiver = OrderedKit.Stats(h.Server, 4);
        Assert.Equal(Count, receiver.Received);
        Assert.Equal(total, receiver.BytesReceived);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server.State);
        Assert.Equal(0, DatagramKit.Statistics(h.Server).ReceiveRingDrops);
        Assert.True(DatagramKit.LinkStatistics(h.Client).StreamRetransmissions > 0, "the link lost stream packets");
    }

    [Fact]
    public void One_Flush_Of_A_Hundred_Messages_Costs_Four_Stream_Sends()
    {
        using SessionHarness h = new(table: Table);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));

        // The first send opens the stream; nothing else goes out until its start is confirmed.
        Assert.True(h.Client.SendCopy(new SendHeader(4), [0]).IsAdmitted);
        Assert.True(h.RunUntil(() => got.Count == 1));
        long before = DatagramKit.Statistics(h.Client).StreamSends;
        for (int i = 1; i <= 100; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(i, 64)).IsAdmitted);
        }

        h.Client.Flush();

        // 32 messages (64 segments) per stream send: 32 + 32 + 32 + 4.
        Assert.Equal(before + 4, DatagramKit.Statistics(h.Client).StreamSends);
        Assert.True(h.RunUntil(() => got.Count == 101));
        for (int i = 1; i <= 100; i++)
        {
            Assert.Equal(OrderedKit.Payload(i, 64), got[i].Payload);
        }
    }

    [Fact]
    public void A_Message_Cut_Into_Hundreds_Of_Packets_Is_Reassembled()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 1_000, MaxDatagramPayload = 100 }, table: Table,
            client: OrderedKit.Roomy, server: OrderedKit.Roomy);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        long packets = DatagramKit.LinkStatistics(h.Client).StreamPacketsSent;
        byte[] big = OrderedKit.Payload(1, 60_000);
        byte[] small = OrderedKit.Payload(2, 3);
        Assert.True(h.Client.SendCopy(new SendHeader(4), big).IsAdmitted);
        Assert.True(h.Client.SendCopy(new SendHeader(4), small).IsAdmitted);
        Assert.True(h.RunUntil(() => got.Count == 2));
        Assert.Equal(big, got[0].Payload);
        Assert.Equal(small, got[1].Payload);
        Assert.True(DatagramKit.LinkStatistics(h.Client).StreamPacketsSent - packets >= 600);
    }

    [Fact]
    public void Keys_Request_Ids_And_Compression_Survive_The_Frame()
    {
        using SessionHarness h = new(table: Table);
        List<(ReceiveHeader Header, byte[] Payload)> keyed = [];
        List<(ReceiveHeader Header, byte[] Payload)> packed = [];
        List<(ReceiveHeader Header, byte[] Payload)> rpc = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(keyed));
        h.Server.RegisterHandler(6, Handlers.Collect(packed));
        h.Server.RegisterHandler(10, Handlers.Collect(rpc));
        byte[] compressible = new byte[2_000];
        byte[] noise = OrderedKit.Noise(2_000, 5);
        byte[] tiny = [1, 2, 3];
        Assert.True(h.Client.SendCopy(new SendHeader(5, 7), [1]).IsAdmitted);
        Assert.True(h.Client.SendCopy(new SendHeader(5, 1UL << 40), [2]).IsAdmitted);
        Assert.True(h.Client.SendCopy(new SendHeader(6), compressible).IsAdmitted);
        Assert.True(h.Client.SendCopy(new SendHeader(6), noise).IsAdmitted);
        Assert.True(h.Client.SendCopy(new SendHeader(6), tiny).IsAdmitted);
        Assert.True(h.Client.SendCopy(new SendHeader(10), [9]).IsAdmitted);
        Assert.True(h.RunUntil(() => keyed.Count == 2 && packed.Count == 3 && rpc.Count == 1));

        Assert.Equal(7UL, keyed[0].Header.Key);
        Assert.Equal(1UL << 40, keyed[1].Header.Key);
        Assert.Equal(compressible, packed[0].Payload);
        Assert.Equal(2_000, packed[0].Header.RawLength);
        Assert.Equal(2_000, packed[0].Header.Length);
        Assert.True(packed[0].Header.Flags.HasFlag(ReceiveFlags.Compressed));
        Assert.Equal(noise, packed[1].Payload);
        Assert.Equal(0, packed[1].Header.RawLength);
        Assert.Equal(tiny, packed[2].Payload);
        Assert.Equal(0, packed[2].Header.RawLength);
        Assert.Equal(0u, rpc[0].Header.RequestId);
        Assert.Equal(new byte[] { 9 }, rpc[0].Payload);

        // The compressible payload travelled as a short LZ4 block.
        Assert.True(OrderedKit.Stats(h.Client, 6).BytesSent < 2_000 + 2_000 + 3);
    }

    [Fact]
    public void A_Single_Gathered_Page_Is_Sent_As_It_Is_And_Several_Pages_Are_Joined()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        BufferLease page = h.Client.RentBuffer(100);
        byte[] expectedPage = OrderedKit.Payload(5, page.Length);
        expectedPage.CopyTo(h.Client.GetBufferSpan(page));
        long outstanding = DatagramKit.Statistics(h.Client).SendBytesOutstanding;
        Assert.True(h.Client.SendGather(new SendHeader(4), [page]).IsAdmitted);

        // The page itself is the payload: nothing was rented or returned.
        Assert.Equal(outstanding, DatagramKit.Statistics(h.Client).SendBytesOutstanding);

        BufferLease a = h.Client.RentBuffer(64);
        BufferLease b = h.Client.RentBuffer(64);
        byte[] first = OrderedKit.Payload(6, a.Length);
        byte[] second = OrderedKit.Payload(7, b.Length);
        first.CopyTo(h.Client.GetBufferSpan(a));
        second.CopyTo(h.Client.GetBufferSpan(b));
        Assert.True(h.Client.SendGather(new SendHeader(4), [a, b]).IsAdmitted);
        Assert.True(h.RunUntil(() => got.Count == 2));
        Assert.Equal(expectedPage, got[0].Payload);
        Assert.Equal([.. first, .. second], got[1].Payload);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(h.Client).SendBytesOutstanding == 0));
    }

    [Fact]
    public void Movement_Datagrams_Keep_Flowing_While_A_Large_Ordered_Transfer_Runs()
    {
        LinkOptions link = new() { DelayMicros = 20_000, BandwidthBitsPerSecond = 8_000_000, StreamReceiveWindowBytes = 64 * 1024 };
        using SessionHarness h = new(link: link, table: Table, client: o => { OrderedKit.Roomy(o); DatagramKit.Quiet(o); }, server: OrderedKit.Roomy);
        int ordered = 0;
        int moves = 0;
        long worstLatency = 0;
        h.Server!.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => ordered++);
        h.Server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            worstLatency = Math.Max(worstLatency, header.ReceivedMicros - BitConverter.ToInt64(payload));
            moves++;
        });
        byte[] block = OrderedKit.Payload(3, 32 * 1024);
        byte[] move = new byte[40];
        int queued = 0;
        int movesSent = 0;
        for (int tick = 0; tick < 240; tick++)
        {
            // 2 MiB of ordered data, queued as fast as the channel takes it; one movement datagram per 60 Hz tick.
            while (queued < 64 && h.Client.SendCopy(new SendHeader(4), block).IsAdmitted)
            {
                queued++;
            }

            BitConverter.TryWriteBytes(move, h.Clock.NowMicros);
            if (h.Client.SendCopy(new SendHeader(2), move).IsAdmitted)
            {
                movesSent++;
            }

            h.Run(16_667);
        }

        Assert.True(ordered < 64, "the transfer was still running while the movement flowed");
        Assert.True(moves >= movesSent * 95 / 100, $"{moves} of {movesSent} movement datagrams arrived");
        Assert.True(worstLatency < 40_000, $"worst movement latency {worstLatency} µs");
        Assert.True(h.RunUntil(() => ordered == 64, 30_000_000));
    }
}
