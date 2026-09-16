using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;
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
    public void Request_Ids_Reach_The_Handler_And_A_Response_Nothing_Awaits_Is_Dropped()
    {
        // The receive side of PROTOCOL.md §3.1 RequestId: 0 = plain, odd = request, even ≥ 2 = response to RequestId − 1.
        // The frames are written by hand, so the response belongs to no request of this end: since wave C2d a response is
        // offered to the channel's engine instead of the handler, and one nothing awaits is dropped and counted (§7.8).
        using ServerHarness h = new(table: OrderedTables.Main);
        Assert.True(h.Admit(), "The raw client was not admitted.");
        ChannelDefinition rpc = h.Table[10]!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(10, Handlers.Collect(got));

        byte[] frames = new byte[64];
        int written = StreamFraming.WritePreamble(frames, rpc.Id);
        written += WriteMessage(frames.AsSpan(written), rpc, 1, 0xA1);
        written += WriteMessage(frames.AsSpan(written), rpc, 2, 0xB2);
        written += WriteMessage(frames.AsSpan(written), rpc, 0, 0xC3);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(frames.AsSpan(0, written), out TransportStreamId _));
        Assert.True(h.RunUntil(() => got.Count == 2), $"{got.Count} of 2 frames were delivered.");

        Assert.Equal(1u, got[0].Header.RequestId);
        Assert.True(got[0].Header.Flags.HasFlag(ReceiveFlags.IsRequest));
        Assert.False(got[0].Header.Flags.HasFlag(ReceiveFlags.IsResponse));
        Assert.Equal(new byte[] { 0xA1 }, got[0].Payload);

        // The response (RequestId 2) never reaches the handler; it is counted and its lease returned.
        Assert.Equal(1, h.Statistics().ResponsesUnmatched);

        Assert.Equal(0u, got[1].Header.RequestId);
        Assert.Equal(ReceiveFlags.None, got[1].Header.Flags);
        Assert.Equal(new byte[] { 0xC3 }, got[1].Payload);

        static int WriteMessage(Span<byte> destination, ChannelDefinition channel, uint requestId, byte payload)
        {
            StreamMessageHeader header = default;
            header.Length = 1;
            header.RequestId = requestId;
            int written = StreamFraming.WriteFrameHeader(destination, channel, in header);
            destination[written] = payload;
            return written + 1;
        }
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
        // DropWhenBlocked is on (the simulator honours CancelOnBlocked): a datagram that meets a busy link is dropped. The send
        // cap keeps the transfer below the link rate, and within a pass the scheduler hands the tick's movement datagram to
        // the transport before the ordered channel's stream data, so movement never waits behind the transfer.
        LinkOptions link = new() { DelayMicros = 20_000, BandwidthBitsPerSecond = 8_000_000 };
        using SessionHarness h = new(link: link, table: Table, server: OrderedKit.Roomy, client: o =>
        {
            OrderedKit.Roomy(o);
            DatagramKit.Quiet(o);
            o.MaxSendBytesPerSecond = 500_000;
        });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        const int Blocks = 2_048;
        int ordered = 0;
        int moves = 0;
        long worstLatency = 0;
        server.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => ordered++);
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            worstLatency = Math.Max(worstLatency, header.ReceivedMicros - BitConverter.ToInt64(payload));
            moves++;
        });
        byte[] block = OrderedKit.Payload(3, 1_024);
        byte[] move = new byte[40];
        int queued = 0;
        int movesSent = 0;
        uint tick = 0;
        for (int i = 0; i < 180; i++)
        {
            // 2 MiB of ordered data queued as fast as the channel takes it, one movement datagram per 60 Hz tick, one flush per tick.
            while (queued < Blocks && client.SendCopy(new SendHeader(4), block).IsAdmitted)
            {
                queued++;
            }

            BitConverter.TryWriteBytes(move, h.Clock.NowMicros);
            if (client.SendCopy(new SendHeader(2), move).IsAdmitted)
            {
                movesSent++;
            }

            client.Flush(++tick);
            for (int ms = 0; ms < 16; ms++)
            {
                h.Network.Advance(1_000);
                server.Poll();
                server.Flush();
                client.Poll();
            }

            h.Network.Advance(667);
            server.Poll();
            server.Flush();
            client.Poll();
        }

        Assert.InRange(ordered, Blocks / 3, Blocks - 1);
        Assert.True(moves >= movesSent * 95 / 100, $"{moves} of {movesSent} movement datagrams arrived");
        Assert.True(worstLatency < 30_000, $"worst movement latency {worstLatency} µs");
        Assert.True(h.RunUntil(() => ordered == Blocks, 30_000_000));
    }
}
