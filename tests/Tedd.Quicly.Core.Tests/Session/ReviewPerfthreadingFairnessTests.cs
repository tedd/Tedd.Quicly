using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Review (perf-threading lens) of fix/localhost-drops, the drain-queue eviction (<c>ReceiveQueues.AppendEvicting</c> /
/// <c>PickVictim</c>). The documents promise that an unreliable channel nobody drains "costs only its own oldest messages",
/// "never affects another channel" (PROTOCOL.md §7) and that "a channel that is drained diligently is not evicted while
/// another one is the hog" (session-layer.md §4.4, the <c>AppendEvicting</c> summary). The victim rule breaks that: the
/// appending channel evicts <em>its own</em> oldest message as soon as it is at its even share of the pressured resource,
/// however far over its share the hog is. So once one channel is left undrained, every other Drain-style unreliable channel
/// is cut from the whole backlog (what it may use while it is alone) down to <c>1 / Qu</c> of it per Poll, and loses the
/// rest of each frame's messages although it is drained every frame and the hog still holds several times its share.
/// The test the branch added for this (<c>A_Diligent_Drain_Channel_Is_Not_Evicted_By_A_Flooded_One</c>) sends exactly the
/// even share (8 of 8) per round, so it cannot see it.
/// </summary>
/// <remarks>
/// Fixed since: only a backlog that was left undrained across a Poll is limited and evicted (<c>ReceiveQueues.BeginPass</c>,
/// <c>TryAppendDatagram</c>), a channel that is drained every frame is outside those limits, and among backlogged channels the
/// victim is the one furthest over its share. The description above is what the tests found; they now pin the fix.
/// </remarks>
public class ReviewPerfthreadingFairnessTests
{
    /// <summary>2, 3, 6 unordered · 8 keyed sequenced · 10 ordered · 11 reliable unordered: four unreliable ring channels, two reliable.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(3, "b", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(6, "c", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(8, "keyed", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.ExpiryMicros = 0; })
        .Add(10, "stream", ChannelMode.ReliableOrdered)
        .Add(11, "group", ChannelMode.ReliableUnordered)
        .Build();

    private static byte[] Payload(int index, int size)
    {
        byte[] payload = new byte[size];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static ChannelStatistics Channel(QuiclyPeer peer, ushort channel)
    {
        Assert.True(peer.GetChannelStatistics(channel, out ChannelStatistics statistics));
        return statistics;
    }

    /// <summary>Sends <paramref name="count"/> messages and runs the network until the server's transport side has them; the server is not polled.</summary>
    private static void Deliver(SessionHarness h, ushort channel, int first, int count, int size)
    {
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long arrived = Channel(server, channel).Received + count;
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(channel), Payload(first + i, size)).Status);
        }

        client.Flush();
        for (int step = 0; step < 1_000 && Channel(server, channel).Received < arrived; step++)
        {
            h.Network.Advance(1_000);
            client.Poll();
            client.Flush();
        }

        Assert.Equal(arrived, Channel(server, channel).Received);
    }

    private static List<int> DrainAll(QuiclyPeer peer, ushort channel)
    {
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[16];
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

    [Fact]
    public void A_Diligently_Drained_Channel_Loses_Nothing_While_Another_Channel_Hogs_The_Node_Share()
    {
        // Ring 64: a pool of 64, 32 nodes for the four unreliable channels together, an even share of 8. Channel 3 is drained
        // after every Poll and gets 12 messages per round: far less than the 32 it may queue while it is alone.
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 64;
        });
        QuiclyPeer server = h.Server!;

        // Alone, the 12 messages of a round fit.
        Deliver(h, 3, 0, 12, 4);
        server.Poll();
        Assert.Equal(Enumerable.Range(0, 12), DrainAll(server, 3));
        Assert.Equal(0, Channel(server, 3).DrainQueueDrops);

        // Channel 2 is never drained.
        List<int> got = [];
        for (int round = 0; round < 10; round++)
        {
            Deliver(h, 2, round * 32, 32, 4);
            Deliver(h, 3, 100 + (round * 12), 12, 4);
            server.Poll();
            got.AddRange(DrainAll(server, 3));
        }

        ChannelStatistics hog = Channel(server, 2);
        ChannelStatistics diligent = Channel(server, 3);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
        Assert.True(hog.DrainQueueDrops > 0, "the flooded channel lost nothing");
        int hogQueued = server.Drain(2, new ReceivedMessage[64]);
        Assert.True(
            diligent.DrainQueueDrops == 0,
            $"the channel that is drained every round lost {diligent.DrainQueueDrops} of 120 messages ({got.Count} delivered) to its own eviction, while the channel nobody drains still held {hogQueued} messages against an even share of 8");
        Assert.Equal(Enumerable.Range(100, 120), got);
    }

    [Fact]
    public void A_Diligently_Drained_Channel_Loses_Nothing_While_Another_Channel_Hogs_The_Byte_Share()
    {
        // Every default: ring 4 096, receive budget 256 KiB, so 64 KiB for the unreliable channels together and an even
        // share of 16 KiB. 1 000-byte messages sit in 1 536-byte blocks. Channel 3 gets 20 per frame (30 720 block bytes,
        // less than half of what it may queue while it is alone) and is drained after every Poll.
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer server = h.Server!;

        // Alone, a frame's 20 messages fit.
        Deliver(h, 3, 0, 20, 1_000);
        server.Poll();
        Assert.Equal(Enumerable.Range(0, 20), DrainAll(server, 3));
        Assert.Equal(0, Channel(server, 3).DrainQueueDrops);

        // A channel the application does not read fills its backlog once and stays there.
        for (int batch = 0; batch < 4; batch++)
        {
            Deliver(h, 2, batch * 25, 25, 1_000);
            server.Poll();
        }

        List<int> got = [];
        for (int round = 0; round < 10; round++)
        {
            Deliver(h, 3, 100 + (round * 20), 20, 1_000);
            server.Poll();
            got.AddRange(DrainAll(server, 3));
        }

        PeerStatistics peer = DatagramKit.Statistics(server);
        ChannelStatistics diligent = Channel(server, 3);
        Assert.Equal(0, peer.ReceiveRingDrops);
        Assert.Equal(0, peer.OutOfReceiveBuffers);
        Assert.True(
            diligent.DrainQueueDrops == 0,
            $"the channel that is drained every frame lost {diligent.DrainQueueDrops} of 200 messages ({got.Count} delivered), while the channel nobody drains pins {peer.ReceiveBytesOutstanding} bytes against an even share of 16 384");
        Assert.Equal(Enumerable.Range(100, 200), got);
    }
}
