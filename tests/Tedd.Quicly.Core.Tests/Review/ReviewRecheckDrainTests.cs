using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Review (recheck lens) of the pass-and-backlog rule of the drain queues (016038f): what a host that polls and then
/// drains every one of its channels completely, every frame, still loses.
/// </summary>
public class ReviewRecheckDrainTests
{
    private const int Batch = 32;

    /// <summary>2, 3 unordered, both read with Drain. Nothing expires.</summary>
    private static readonly ChannelTable TwoDrained = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(3, "b", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Build();

    private static byte[] Payload(int index, int size)
    {
        byte[] payload = new byte[size];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static void Burst(SessionHarness h, ushort channel, int count, int size)
    {
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long arrived = DatagramKit.ChannelStats(server, channel).Received;
        for (int sent = 0; sent < count;)
        {
            int n = Math.Min(Batch, count - sent);
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(channel), Payload(sent + i, size)).Status);
            }

            client.Flush();
            sent += n;
            arrived += n;
            for (int step = 0; step < 1_000 && DatagramKit.ChannelStats(server, channel).Received < arrived; step++)
            {
                h.Network.Advance(1_000);
                client.Poll();
                client.Flush();
            }

            Assert.Equal(arrived, DatagramKit.ChannelStats(server, channel).Received);
        }

        // The precondition: the ring and the budget took the whole burst, which is all main needed to deliver it.
        PeerStatistics peer = DatagramKit.Statistics(server);
        Assert.Equal(0, peer.ReceiveRingDrops);
        Assert.Equal(0, peer.OutOfReceiveBuffers);
    }

    private static List<int> DrainAll(QuiclyPeer peer, ushort channel)
    {
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[4096];
        int n;
        while ((n = peer.Drain(channel, buffer)) > 0)
        {
            for (int i = 0; i < n; i++)
            {
                got.Add(BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload));
            }

            peer.Release(buffer.AsSpan(0, n));
        }

        return got;
    }

    /// <summary>
    /// A host reads two unreliable channels with Drain and does everything the documentation asks: every frame it polls,
    /// then drains channel 2 until Drain returns 0, then channel 3 until Drain returns 0. A burst on both channels arrives
    /// in one frame, channel 3's messages first, each part larger than the queue pool (the ring and the budget take all
    /// of it: asserted). The Poll fills the pool with channel 3 and holds its next message; Drain(2) can take nothing;
    /// Drain(3) empties channel 3 and, walking the ring, fills the pool with channel 2 and holds channel 2's next message.
    /// That held message is still there when the next frame's Poll begins, and the Poll drops it (mayHold: false in
    /// ReceiveQueues.TryAppendDatagram: the channel is not backlogged, the pool is full, there is no backlog to evict, so
    /// DropNew) although the channel was drained empty in the frame before and is drained again right after the Poll. The
    /// message is lost from the middle of the burst; main held it and delivered every message.
    /// </summary>
    [Fact]
    public void A_Host_That_Polls_And_Drains_Two_Channels_Every_Frame_Loses_Nothing_Of_A_Burst_On_Both()
    {
        const int count = 1100; // per channel; every option at its default: a 4 096-message ring, a 1 024-node queue pool
        using SessionHarness h = new(table: TwoDrained, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer server = h.Server!;

        // A first, quiet frame: both channels are drained (empty).
        server.Poll();
        Assert.Empty(DrainAll(server, 2));
        Assert.Empty(DrainAll(server, 3));

        Burst(h, 3, count, 4);
        Burst(h, 2, count, 4);

        List<int> a = [];
        List<int> b = [];
        for (int frame = 0; frame < 4; frame++)
        {
            server.Poll();
            a.AddRange(DrainAll(server, 2));
            b.AddRange(DrainAll(server, 3));
            server.Flush();
        }

        PeerStatistics stats = DatagramKit.Statistics(server);
        Assert.True(b.Count == count, $"channel 3: {b.Count} of {count}");
        Assert.True(a.Count == count,
            $"channel 2, drained completely every frame: {a.Count} of {count} messages reached the application " +
            $"(DrainQueueDrops {DatagramKit.ChannelStats(server, 2).DrainQueueDrops}, ReceiveRingDrops {stats.ReceiveRingDrops}, " +
            $"OutOfReceiveBuffers {stats.OutOfReceiveBuffers}); first missing index {Enumerable.Range(0, count).Except(a).DefaultIfEmpty(-1).First()}");
        Assert.Equal(Enumerable.Range(0, count), a);
    }

    /// <summary>
    /// Guard: bursts of 300 per channel do not fill the pool (1 024), and an ordinary two-channel burst is delivered whole.
    /// The failing case needs each channel's part of one frame's traffic to exceed the pool.
    /// </summary>
    [Fact]
    public void Guard_Two_Drained_Channels_With_Bursts_Below_The_Pool_Lose_Nothing()
    {
        using SessionHarness h = new(table: TwoDrained, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer server = h.Server!;
        server.Poll();
        int a = 0;
        int b = 0;
        for (int round = 0; round < 5; round++)
        {
            Burst(h, 3, 300, 4);
            Burst(h, 2, 300, 4);
            server.Poll();
            a += DrainAll(server, 2).Count;
            b += DrainAll(server, 3).Count;
            server.Flush();
        }

        Assert.Equal(1500, a);
        Assert.Equal(1500, b);
        Assert.Equal(0, DatagramKit.Statistics(server).DrainQueueDrops);
    }
}
