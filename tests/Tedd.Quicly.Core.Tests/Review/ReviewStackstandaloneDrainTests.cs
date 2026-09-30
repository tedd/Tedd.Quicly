using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review (lens: standalone) of 17c089b: 6e173a0 (Route queues a ring message behind its channel's queue) and
/// 0ba04a7 (Drain rents the decode buffer before it takes a compressed message of a reliable channel). A test marked
/// FINDING fails at d567ba5 for the reason its comment gives; a test marked GUARD passes and pins a promise.
/// </summary>
public class ReviewStackstandaloneDrainTests
{
    /// <summary><see cref="OrderedTables.Main"/>: ReliableOrdered with LZ4, MinCompressSize 16.</summary>
    private const ushort Packed = 6;

    /// <summary>Two ReliableOrdered channels with LZ4 (MinCompressSize 16), read with Drain.</summary>
    private const ushort PackedA = 20;
    private const ushort PackedB = 21;

    private static readonly ChannelTable TwoPacked = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(PackedA, "packed-a", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; })
        .Add(PackedB, "packed-b", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; })
        .Build();

    private static byte[] Payload(int index, int size)
    {
        // Zeroes behind the index: LZ4 packs 20 000 of them into a block of 256 bytes.
        byte[] payload = new byte[size];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static long Received(QuiclyPeer peer, ushort channel) => DatagramKit.ChannelStats(peer, channel).Received;

    /// <summary>Sends while only the client and the network run: the server's host is late with its Poll.</summary>
    private static void SendToALateReceiver(SessionHarness h, ushort channel, int first, int count, int size)
    {
        for (int i = 0; i < count; i++)
        {
            int attempts = 0;
            while (h.Client.SendCopy(new SendHeader(channel), Payload(first + i, size)).Status != SendStatus.Admitted)
            {
                Assert.True(++attempts < 1000, "the client never admitted the send");
                PumpClient(h, 1);
            }

            PumpClient(h, 1);
        }

        PumpClient(h, 8);
    }

    private static void PumpClient(SessionHarness h, int steps)
    {
        for (int step = 0; step < steps; step++)
        {
            h.Client.Poll();
            h.Client.Flush();
            h.Network.Advance(1_000);
        }
    }

    private static int DrainInto(QuiclyPeer peer, ushort channel, ReceivedMessage[] buffer, List<int> got)
    {
        int taken = peer.Drain(channel, buffer);
        for (int i = 0; i < taken; i++)
        {
            got.Add(BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload));
        }

        peer.Release(buffer.AsSpan(0, taken));
        return taken;
    }

    /// <summary>
    /// FINDING (order). Drain's queue loop stops at a compressed message it has no decode buffer for and leaves it queued,
    /// but the call then goes on to the held slot and the receive ring, and takes the channel's next message from there
    /// when that one needs no buffer (an uncompressed message: shorter than MinCompressSize, or one LZ4 did not shrink).
    /// A ReliableOrdered channel read with Drain is handed its messages out of order: 0, 1, 2, 8, 3, 4, ...
    /// </summary>
    [Fact]
    public void FINDING_Drain_That_Leaves_A_Compressed_Message_Queued_Does_Not_Hand_Out_A_Newer_Message_Of_The_Channel()
    {
        using SessionHarness h = new(table: OrderedTables.Main, client: GroupKit.Prompt, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;

        // Eight compressed messages of 20 000 bytes; the server's Polls move them to the channel's drain queue.
        for (int i = 0; i < 8; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), Payload(i, 20_000)).Status);
            h.Run(2_000);
        }

        h.Run(50_000);
        Assert.Equal(8, Received(server, Packed));

        // Message 8 is four bytes (under MinCompressSize: sent uncompressed) and reaches the receive ring before the next Poll.
        SendToALateReceiver(h, Packed, 8, 1, 4);
        Assert.Equal(9, Received(server, Packed));

        // One Drain with a span of sixteen: the default budget (256 KiB) decodes three of the queued messages into 64 KiB
        // blocks, the fourth waits in the queue — and message 8 must wait behind it.
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        DrainInto(server, Packed, buffer, got);
        h.RunUntil(() =>
        {
            while (DrainInto(server, Packed, buffer, got) > 0)
            {
            }

            return got.Count >= 9;
        }, 1_000_000);

        Assert.True(got.SequenceEqual(Enumerable.Range(0, 9)),
            $"a ReliableOrdered channel read with Drain was handed its messages out of order: {string.Join(",", got)}");
    }

    /// <summary>
    /// FINDING (permanent stall). Drain waits for a decode buffer while the receive budget is pinned by the very messages
    /// that wait: a channel the application drains may have half the receive budget waiting (ReceiveCredit.DrainedByteLimit),
    /// so two such channels that compress fill the whole budget with small compressed blocks after a hitch of the host.
    /// No 64 KiB decode buffer can ever be rented, CanEverRentReceive still says it could (it only subtracts the message's
    /// own block), so both channels wait for good: every Drain returns 0, nothing is counted (DecodeFailures 0), the
    /// streams stay held back, and — the budget being gone — the receive of every other stream of the peer stops too.
    /// Before 0ba04a7 these messages were dropped (counted) and the channel went on; now nothing ever arrives again.
    /// Default options.
    /// </summary>
    [Fact]
    public void FINDING_Two_Compressed_Channels_Read_With_Drain_Do_Not_Stall_For_Good_After_A_Hitch()
    {
        using SessionHarness h = new(table: TwoPacked, client: c =>
        {
            GroupKit.Prompt(c);
            c.SendBudgetBytes = 4 * 1024 * 1024;
        }, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        ReceivedMessage[] buffer = new ReceivedMessage[16];

        // The application reads both channels with Drain, every frame; they are empty now.
        List<int> a = [];
        List<int> b = [];
        Assert.Equal(0, DrainInto(server, PackedA, buffer, a));
        Assert.Equal(0, DrainInto(server, PackedB, buffer, b));

        // The host hitches while 600 compressed messages of 20 000 bytes are sent on each channel.
        const int Count = 600;
        for (int i = 0; i < Count; i += 50)
        {
            SendToALateReceiver(h, PackedA, i, 50, 20_000);
            SendToALateReceiver(h, PackedB, i, 50, 20_000);
        }

        // The host is back: it polls and drains both channels completely every frame, releasing each batch.
        bool done = h.RunUntil(() =>
        {
            while (DrainInto(server, PackedA, buffer, a) > 0)
            {
            }

            while (DrainInto(server, PackedB, buffer, b) > 0)
            {
            }

            return a.Count == Count && b.Count == Count;
        }, 20_000_000);

        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(done && a.SequenceEqual(Enumerable.Range(0, Count)) && b.SequenceEqual(Enumerable.Range(0, Count)),
            $"after 20 s: channel A delivered {a.Count} of {Count}, channel B {b.Count} of {Count}; received A {Received(server, PackedA)}, "
            + $"B {Received(server, PackedB)}; DecodeFailures {statistics.DecodeFailures}; receive bytes outstanding {statistics.ReceiveBytesOutstanding}");
    }

    /// <summary>
    /// FINDING (reliable loss on the handler path, the "known limit" of 0ba04a7). A channel with a handler has no credit
    /// limit, so a burst of small compressed messages fills the receive budget in the ring; the first dispatch then finds
    /// no 64 KiB block to decode into and TryDecode drops the message, and so on until the dropped compressed blocks
    /// free 64 KiB. Every one of them was acknowledged to its sender. Default options, a plain handler that retains nothing.
    /// The loss is avoidable (the transport knows each message's raw length when it charges the budget; reserving the decode
    /// block there, or holding the message instead of dropping it, keeps it), and the release notes promise that a reliable
    /// message is not lost to receive-side pressure.
    /// </summary>
    [Fact]
    public void FINDING_A_Handler_Of_A_Compressed_Reliable_Channel_Loses_Nothing_To_A_Burst()
    {
        using SessionHarness h = new(table: OrderedTables.Main, client: c =>
        {
            GroupKit.Prompt(c);
            c.SendBudgetBytes = 4 * 1024 * 1024;
        }, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        server.RegisterHandler(Packed, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
            got.Add(BinaryPrimitives.ReadInt32LittleEndian(payload)));

        const int Count = 1_200;
        SendToALateReceiver(h, Packed, 0, Count, 20_000);

        bool done = h.RunUntil(() => got.Count == Count, 20_000_000);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(done && got.SequenceEqual(Enumerable.Range(0, Count)) && statistics.DecodeFailures == 0,
            $"the handler got {got.Count} of {Count} messages of a ReliableOrdered channel; DecodeFailures {statistics.DecodeFailures}; "
            + $"first gap after {got.TakeWhile((v, i) => v == i).Count()}");
    }
}
