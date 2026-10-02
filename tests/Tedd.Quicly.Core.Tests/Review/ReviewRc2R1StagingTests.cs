using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review, RC2 round 1 of fix/stack-review (8f6c47c), lens: the decoded-size staging of RC2-1
/// (<c>PeerCore.LimitedStagingLength</c> = RawLength + <c>Lz4Block.GetInPlaceMargin(Length)</c>) with the pool and the
/// budget a peer gets by default. A test marked FINDING fails at 8f6c47c for the reason its comment gives.
/// </summary>
/// <remarks>
/// The in-place margin is at least 32 bytes, so a compressed message whose decoded size is a block size of the pool, or a
/// few dozen bytes below it, is staged in the NEXT size class — four times larger in the default compact pool
/// (4 KiB → 16 KiB, 16 KiB → 64 KiB, 64 KiB → 256 KiB). A channel's default MaxMessageSize is exactly 64 KiB, the size an
/// application that chunks a blob sends, and with the default per-peer pool (one 256 KiB block) and the default
/// ReceiveBudgetBytes (256 KiB) one such message pins the peer's whole receive budget and the pool's only 256 KiB block
/// from its first byte until the application drains it. At 6d41167 (and 0.2.1) the same message was staged at its wire
/// length (a 1 536-byte block here) and decoded into a 64 KiB block.
/// </remarks>
public class ReviewRc2R1StagingTests
{
    private const ushort Moves = 2;
    private const ushort Packed = 6;

    /// <summary>Default MaxMessageSize (64 KiB) and LZ4; the server keeps every other default (compact pool, 256 KiB budget).</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(Moves, "moves", ChannelMode.UnreliableUnordered)
        .Add(Packed, "packed", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; })
        .Build();

    /// <summary><paramref name="size"/> bytes whose first <paramref name="noise"/> LZ4 cannot shrink and the rest zeroes; the index leads.</summary>
    private static byte[] Payload(int index, int size, int noise)
    {
        byte[] payload = new byte[size];
        new Random(index + 1).NextBytes(payload.AsSpan(0, Math.Min(noise, size)));
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static void Client(PeerOptions o)
    {
        GroupKit.Prompt(o);
        OrderedKit.Roomy(o);
    }

    private static void MarkDrained(SessionHarness h, ushort channel)
    {
        ReceivedMessage[] buffer = new ReceivedMessage[4];
        for (int i = 0; i < 2; i++)
        {
            h.Run(20_000);
            Assert.Equal(0, h.Server!.Drain(channel, buffer));
        }
    }

    private static int Rented(SlabAllocator allocator, int blockSize)
    {
        SlabStatistics statistics = allocator.GetStatistics();
        for (int i = 0; i < statistics.ClassCount; i++)
        {
            if (statistics[i].BlockSize == blockSize)
            {
                return statistics[i].Rented;
            }
        }

        throw new InvalidOperationException($"no class of {blockSize} bytes");
    }

    /// <summary>
    /// FINDING (major, regression against 6d41167 and 0.2.1). One compressed message of the channel's default maximum size
    /// (65 536 bytes decoded, about 1.3 KB on the wire) arrives on a drained channel while the host is one frame late. It
    /// is staged in the pool's only 256 KiB block (65 536 + margin does not fit 64 KiB) and so holds the whole default
    /// receive budget: a datagram that arrives in the same frame is refused (OutOfReceiveBuffers) and lost, and the next
    /// message of the channel cannot start. At 6d41167 the message held a 1 536-byte block and was decoded into a
    /// 64 KiB one; the budget had room for the datagram and for three more such messages.
    /// </summary>
    [Fact]
    public void FINDING_A_Compressed_Message_Of_The_Default_Maximum_Size_Pins_The_Whole_Default_Receive_Budget()
    {
        using SessionHarness h = new(table: Table, client: Client, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        Assert.Equal(256 * 1024, server.Core.ReceiveBudgetBytes);
        Assert.Equal(64 * 1024, Table[Packed]!.MaxMessageSize);
        MarkDrained(h, Packed);

        byte[] chunk = Payload(0, 64 * 1024, 1_000);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), chunk).Status);
        Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(server, Packed).Received == 1));

        // What the message costs while it waits for the application's next Drain.
        long outstanding = server.Core.ReceiveBytesOutstanding;
        int large = Rented(server.Core.Allocator, 262_144);

        // A datagram in the same frame.
        long refusedBefore = DatagramKit.Statistics(server).OutOfReceiveBuffers;
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Moves), new byte[64]).Status);
        h.Run(20_000);
        long refused = DatagramKit.Statistics(server).OutOfReceiveBuffers - refusedBefore;

        // The message itself still arrives intact.
        ReceivedMessage[] buffer = new ReceivedMessage[4];
        int taken = server.Drain(Packed, buffer);
        Assert.Equal(1, taken);
        Assert.True(buffer[0].Payload.SequenceEqual(chunk));
        server.Release(buffer.AsSpan(0, taken));

        Assert.True(outstanding <= 128 * 1024 && refused == 0,
            $"one 64 KiB compressed message holds {outstanding} of the {server.Core.ReceiveBudgetBytes}-byte receive budget "
            + $"({large} of the pool's 256 KiB blocks; 6d41167 held a 1 536-byte block for it), and {refused} datagram(s) "
            + "of the same frame were refused for want of receive buffers");
    }

    /// <summary>
    /// FINDING (same root cause, throughput). Compressed messages of exactly 16 KiB decoded on a drained channel are staged
    /// in 64 KiB blocks (16 384 + margin does not fit 16 KiB), four times what a 16 000-byte one takes: the half of the
    /// default budget that drained channels share (128 KiB) holds two of them, so a host that is one frame late gets two of
    /// eight. At 6d41167 each was staged in a 1 536-byte block and all eight arrived; at their decoded size without the
    /// class jump (16 KiB blocks) the half would hold eight.
    /// </summary>
    [Fact]
    public void FINDING_Compressed_Messages_Of_A_Block_Size_Are_Staged_In_The_Next_Class_Four_Times_Larger()
    {
        using SessionHarness h = new(table: Table, client: Client, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        MarkDrained(h, Packed);

        const int Count = 8;
        List<byte[]> sent = [];
        for (int i = 0; i < Count; i++)
        {
            byte[] payload = Payload(i, 16 * 1024, 500);
            sent.Add(payload);
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), payload).Status);
        }

        // The host is late: the transport stages what it can before the application drains.
        h.Run(200_000);
        long received = DatagramKit.ChannelStats(server, Packed).Received;
        int blocks64 = Rented(server.Core.Allocator, 65_536);

        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        Assert.True(h.RunUntil(() =>
        {
            int taken = server.Drain(Packed, buffer);
            for (int i = 0; i < taken; i++)
            {
                int index = BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload);
                Assert.True(buffer[i].Payload.SequenceEqual(sent[index]));
                got.Add(index);
            }

            server.Release(buffer.AsSpan(0, taken));
            return got.Count == Count;
        }));

        Assert.True(received >= Count,
            $"{received} of {Count} compressed 16 KiB messages could be staged while the host was late ({blocks64} 64 KiB blocks rented)");
    }
}
