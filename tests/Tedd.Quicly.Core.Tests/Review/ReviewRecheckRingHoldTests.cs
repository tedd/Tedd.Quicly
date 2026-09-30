using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Review (recheck lens) of the pass-and-backlog rule (016038f): "an unreliable channel that nobody drains closes the ring
/// for one Poll interval at most, once, and afterwards costs only its own oldest messages" (QuiclyPeer.TryQueue remarks,
/// RELEASE-NOTES.md "Known limits", TROUBLESHOOTING.md). With <em>two</em> such channels that is not true: a channel
/// whose backlog was evicted to nothing by the other one's burst is no longer backlogged at the next pass start
/// (ReceiveQueues.BeginPass: count 0), so its next burst is "current" again, fills the pool by evicting the other
/// channel's whole backlog, and is held. The two take turns, and the ring is closed in every Poll interval.
/// </summary>
public class ReviewRecheckRingHoldTests
{
    private const int Batch = 32;

    /// <summary>2, 3 unordered, never read · 6 unordered with a handler. Nothing expires.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "unread-a", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(3, "unread-b", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(6, "handled", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Build();

    private static SessionHarness Harness() => new(table: Table, client: DatagramKit.Quiet, server: o =>
    {
        DatagramKit.Quiet(o);
        o.ReceiveBudgetBytes = 16 << 20;
        o.ReceiveRingCapacity = 16_384; // the queue pool stays 1 024; ring and budget are out of the picture
        o.AllocatorOptions = new SlabAllocatorOptions { FreeListShards = 1 };
    });

    /// <summary>Sends <paramref name="count"/> small messages and runs the network until the server's transport side saw them (queued or dropped at the ring).</summary>
    private static void Send(SessionHarness h, ushort channel, int count)
    {
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        byte[] payload = new byte[4];
        static long Seen(QuiclyPeer peer, ushort ch)
        {
            ChannelStatistics c = DatagramKit.ChannelStats(peer, ch);
            return c.Received + c.RingDrops + c.OutOfBuffers;
        }

        long seen = Seen(server, channel);
        for (int sent = 0; sent < count;)
        {
            int n = Math.Min(Batch, count - sent);
            for (int i = 0; i < n; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(payload, sent + i);
                Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(channel), payload).Status);
            }

            client.Flush();
            sent += n;
            seen += n;
            for (int step = 0; step < 1_000 && Seen(server, channel) < seen; step++)
            {
                h.Network.Advance(1_000);
                client.Poll();
                client.Flush();
            }
        }
    }

    /// <summary>
    /// Every frame 1 100 messages arrive on each of two channels nobody reads and 50 on a channel with a handler (2 250
    /// messages a frame into a 4 096-message ring, which a Poll that reads the ring empties every frame). The first frame
    /// may hold the ring once (documented). From the third frame on, every Poll must dispatch what arrived for the handler.
    /// </summary>
    [Fact]
    public void Two_Channels_Nobody_Drains_Do_Not_Keep_The_Ring_Closed_Frame_After_Frame()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        int handled = 0;
        server.RegisterHandler(6, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => handled++);

        const int Frames = 10;
        int heldFrames = 0;
        List<int> handledAfterPoll = [];
        for (int frame = 0; frame < Frames; frame++)
        {
            Send(h, 2, 1100);
            Send(h, 3, 1100);
            Send(h, 6, 50);
            server.Poll();
            server.Flush();
            handledAfterPoll.Add(handled);
            if (frame >= 2 && server.HasPendingPollWork)
            {
                heldFrames++; // a message is held and the ring behind it was not read
            }
        }

        PeerStatistics stats = DatagramKit.Statistics(server);
        Assert.True(heldFrames == 0 && handled == Frames * 50,
            $"the ring was left closed after the Poll in {heldFrames} of the {Frames - 2} frames after the second; the handler saw {handled} of {Frames * 50} messages " +
            $"(after each Poll: {string.Join(",", handledAfterPoll)}); ReceiveRingDrops {stats.ReceiveRingDrops}, RingDrops of the handled channel " +
            $"{DatagramKit.ChannelStats(server, 6).RingDrops}, DrainQueueDrops {stats.DrainQueueDrops}");
    }

    /// <summary>Guard: the same traffic on ONE channel nobody reads holds the ring in the first frame only, as documented.</summary>
    [Fact]
    public void Guard_One_Channel_Nobody_Drains_Holds_The_Ring_Once()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        int handled = 0;
        server.RegisterHandler(6, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => handled++);

        const int Frames = 10;
        int heldFrames = 0;
        for (int frame = 0; frame < Frames; frame++)
        {
            Send(h, 2, 2200);
            Send(h, 6, 50);
            server.Poll();
            server.Flush();
            if (frame >= 2 && server.HasPendingPollWork)
            {
                heldFrames++;
            }
        }

        ChannelStatistics c6 = DatagramKit.ChannelStats(server, 6);
        ChannelStatistics s6 = DatagramKit.ChannelStats(h.Client, 6);
        PeerStatistics ps = DatagramKit.Statistics(server);
        Assert.True(heldFrames == 0 && handled == Frames * 50, $"held {heldFrames}, handled {handled}; server ch6 Received {c6.Received} RingDrops {c6.RingDrops} OOB {c6.OutOfBuffers} Dropped {c6.Dropped}; client ch6 Sent {s6.Sent} Canceled {s6.TransportCanceled} Lost {s6.TransportLost} Expired {s6.Expired}; peer ReceiveRingDrops {ps.ReceiveRingDrops} OutOfReceiveBuffers {ps.OutOfReceiveBuffers} high {ps.ReceiveRingHighWater} DQD {ps.DrainQueueDrops}");
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
    }
}
