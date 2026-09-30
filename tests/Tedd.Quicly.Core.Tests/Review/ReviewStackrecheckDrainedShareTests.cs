using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review, lens: recheck of 85bbb2e (SC-1: "the reliable channels share half the budget as their Drained
/// threshold"). The threshold is now half the receive budget divided by the number of ReliableOrdered/ReliableUnordered
/// channels in the TABLE — handled channels and channels nobody uses included — not among the channels that are drained.
/// A test marked FINDING fails at e8af1f8 for the reason its comment gives.
/// </summary>
public class ReviewStackrecheckDrainedShareTests
{
    private const ushort Drained = 4;

    /// <summary>One reliable channel, read with Drain.</summary>
    private static readonly ChannelTable OneReliable = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(Drained, "drained", ChannelMode.ReliableOrdered)
        .Build();

    /// <summary>The same drained channel, plus seven reliable channels the application reads with handlers (idle here).</summary>
    private static readonly ChannelTable EightReliable = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(Drained, "drained", ChannelMode.ReliableOrdered)
        .Add(20, "h0", ChannelMode.ReliableOrdered)
        .Add(21, "h1", ChannelMode.ReliableOrdered)
        .Add(22, "h2", ChannelMode.ReliableOrdered)
        .Add(23, "h3", ChannelMode.ReliableUnordered)
        .Add(24, "h4", ChannelMode.ReliableUnordered)
        .Add(25, "h5", ChannelMode.ReliableOrdered)
        .Add(26, "h6", ChannelMode.ReliableOrdered)
        .Build();

    /// <summary>
    /// A host that polls and drains once per 16 ms frame; the sender sends a burst of <paramref name="count"/> messages of
    /// 1 000 bytes at once. Returns the frames it took to drain them all (or -1), and the channel's BacklogHolds.
    /// </summary>
    private static (int Frames, long Holds, int DrainedLimit) FramesToDrainBurst(ChannelTable table, int count)
    {
        using SessionHarness h = new(table: table, client: o => { GroupKit.Prompt(o); OrderedKit.Roomy(o); }, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        foreach (ChannelDefinition channel in table.All)
        {
            if (channel.Id != Drained && channel.IsStreamMode)
            {
                server.RegisterHandler(channel.Id, static (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
            }
        }

        ReceivedMessage[] buffer = new ReceivedMessage[256];
        List<int> got = [];

        void Frame()
        {
            // 16 ms in which only the sender's host and the network run, then one Poll, one Drain loop, one Flush.
            for (int step = 0; step < 16; step++)
            {
                h.Network.AdvanceTo(h.Network.NowMicros + 1_000);
                h.Client.Poll();
                h.Client.Flush();
            }

            server.Poll();
            int taken;
            while ((taken = server.Drain(Drained, buffer)) > 0)
            {
                for (int i = 0; i < taken; i++)
                {
                    got.Add(BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload));
                }

                server.Release(buffer.AsSpan(0, taken));
            }

            server.Flush();
        }

        // The consumer is running before the burst: the channel is one the application drains.
        Frame();
        Frame();
        byte[] payload = new byte[1_000];
        for (int i = 0; i < count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(payload, i);
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Drained), payload).Status);
        }

        for (int frame = 1; frame <= 400; frame++)
        {
            Frame();
            if (got.Count == count)
            {
                Assert.Equal(Enumerable.Range(0, count), got);
                return (frame, DatagramKit.ChannelStats(server, Drained).BacklogHolds, server.Core.Credit.DrainedByteLimit);
            }
        }

        return (-1, DatagramKit.ChannelStats(server, Drained).BacklogHolds, server.Core.Credit.DrainedByteLimit);
    }

    /// <summary>
    /// FINDING (minor, introduced by 85bbb2e). SC-1 asked that channels drained and then abandoned cannot pin the whole
    /// budget together; the review suggested a peer-wide total of what the DRAINED channels have waiting. The fix instead
    /// divides half the budget by every reliable channel of the table. A channel with a handler is never limited by the
    /// Drained threshold and an idle channel has nothing waiting, so neither can pin anything under that threshold — yet
    /// each of them shrinks what the one drained channel may take per frame. Here seven idle handled channels cut the
    /// drained channel's threshold from 128 KiB to 16 KiB, and a burst of 400 messages of 1 000 bytes takes many times
    /// the frames it takes with the channel alone (each frame beyond the first is a frame of added latency for the
    /// messages behind; at d567ba5 both tables drained the burst in the same number of frames).
    /// </summary>
    [Fact]
    public void Handled_Reliable_Channels_Do_Not_Shrink_What_A_Drained_Channel_Takes_Per_Frame()
    {
        (int alone, long aloneHolds, int aloneLimit) = FramesToDrainBurst(OneReliable, 400);
        (int beside, long besideHolds, int besideLimit) = FramesToDrainBurst(EightReliable, 400);
        Assert.True(alone > 0 && beside > 0 && beside <= alone + 1,
            $"a burst of 400 x 1 000 B to a channel drained every 16 ms frame: {alone} frame(s) when it is the table's only reliable channel "
            + $"(Drained threshold {aloneLimit} B, {aloneHolds} backlog holds), {beside} frame(s) next to seven idle handled reliable channels "
            + $"(threshold {besideLimit} B, {besideHolds} holds)");
    }
}
