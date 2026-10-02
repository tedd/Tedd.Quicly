using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review, RC2 round 2 of fix/stack-review (89efabc), lens: the stack tail of the in-place decode (8a7ca5d,
/// 33f5716) against the qualification RC2R1-2 put on the Fixed entry.
/// </summary>
/// <remarks>
/// RELEASE-NOTES "Fixed" now says a compressed message of a reliable channel no handler reads is never dropped for want of a
/// buffer "when its decoded size fits the largest pool block within <c>ReceiveBudgetBytes</c>", and PROTOCOL §7, the
/// TROUBLESHOOTING row and <c>PeerStatistics.DecodeFailures</c> say the same: only a decoded size above that block is the
/// edge band. But <c>Lz4Block.InPlaceTailCapacity</c> (2 KiB) covers the in-place margin only for a compressed length up
/// to about 504 KiB. A reliable channel may carry 1 MiB (<c>ChannelDefinition.ReliableMaxMessageSize</c>), so a compressed
/// length above 504 KiB needs <c>RawLength + (C &gt;&gt; 8) + 32 - 2048</c> bytes (<c>GetInPlaceLength</c>). When that is above
/// <c>MaxStageLength</c> while the decoded size is not, <c>LimitedStagingLength</c> puts the message in the edge band: it is
/// staged at its wire length, needs a second block of its decoded size, and is dropped and counted when none is free —
/// after the sender was told Delivered. A test marked FINDING fails at 89efabc for the reason its comment gives.
/// </remarks>
public class ReviewRc2R2InPlaceTailTests
{
    private const ushort Chat = 4;

    /// <summary>ReliableOrdered, LZ4 (MinCompressSize 16), MaxMessageSize 1 MiB (the largest a reliable channel may have).</summary>
    private const ushort Huge = 9;

    private const int OneMiB = 1024 * 1024;

    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(Chat, "chat", ChannelMode.ReliableOrdered)
        .Add(Huge, "huge", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; o.MaxMessageSize = ChannelDefinition.ReliableMaxMessageSize; })
        .Build();

    /// <summary><paramref name="size"/> bytes whose first <paramref name="noise"/> LZ4 cannot shrink and the rest zeroes; the index leads.</summary>
    private static byte[] Payload(int index, int size, int noise)
    {
        byte[] payload = new byte[size];
        new Random(index + 1).NextBytes(payload.AsSpan(0, Math.Min(noise, size)));
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static int CompressedLength(byte[] payload)
    {
        byte[] buffer = new byte[Lz4Block.GetMaxCompressedLength(payload.Length)];
        return Lz4Block.Compress(payload, buffer);
    }

    private static void DrainAll(QuiclyPeer peer, ushort channel, List<int> got, Dictionary<int, byte[]> sent)
    {
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        int taken;
        while ((taken = peer.Drain(channel, buffer)) > 0)
        {
            for (int i = 0; i < taken; i++)
            {
                int index = BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload);
                got.Add(index);
                Assert.True(sent.TryGetValue(index, out byte[]? expected) && buffer[i].Payload.SequenceEqual(expected),
                    $"message {index} is not the payload that was sent");
            }

            peer.Release(buffer.AsSpan(0, taken));
        }
    }

    private static void Send(QuiclyPeer client, ushort channel, Dictionary<int, byte[]> sent, int index, byte[] payload)
    {
        sent[index] = payload;
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(channel), payload).Status);
    }

    /// <summary>
    /// FINDING (minor): the in-place block of a compressed message a reliable channel may carry is its decoded size only up
    /// to a compressed length of about 504 KiB. Above it the stack tail is too small and the block must be larger than the
    /// decoded size, so a decoded size equal to a pool block size (the largest one within the budget, say) needs the next
    /// class or a second buffer. At 89efabc GetInPlaceLength(600 000, 1 MiB) is 1 MiB + 299, not 1 MiB.
    /// </summary>
    [Fact]
    public void FINDING_The_In_Place_Block_Is_The_Decoded_Size_For_Every_Size_A_Reliable_Channel_Carries()
    {
        int max = ChannelDefinition.ReliableMaxMessageSize;
        List<string> grown = [];
        foreach (int compressed in new[] { 504 * 1024, 516_352, 600_000, 800_000, max - 1, max })
        {
            foreach (int raw in new[] { compressed, max })
            {
                long block = Lz4Block.GetInPlaceLength(compressed, raw);
                if (block != Math.Max(compressed, raw))
                {
                    grown.Add($"C {compressed} R {raw}: {block} (+{block - Math.Max(compressed, raw)})");
                }
            }
        }

        Assert.True(grown.Count == 0,
            "the in-place block is larger than the decoded size for compressed lengths a reliable channel can carry: "
            + string.Join("; ", grown));
    }

    /// <summary>
    /// FINDING (minor): a receiver whose pool's largest block within the budget is 1 MiB (ReceiveBudgetBytes 1 MiB, one
    /// 1 MiB block) gets a compressed 1 MiB message (600 000 bytes of noise, about 600 KB on the wire) on a drained channel.
    /// Its decoded size fits the largest pool block within the budget, so by the Fixed entry it is never dropped for want of
    /// a buffer. At 89efabc GetInPlaceLength is 1 MiB + 299 > MaxStageLength, so it is in the edge band: staged at its wire
    /// length in the pool's only 1 MiB block, it needs a second 1 MiB block to decode into, finds none, and is dropped
    /// (DecodeFailures 1) after the sender was told Delivered; the channel gives [1], not [0, 1].
    /// </summary>
    [Fact]
    public void FINDING_A_Decoded_Size_Of_The_Largest_Block_Is_Not_Dropped_When_The_Wire_Length_Is_Above_504_KiB()
    {
        byte[] big = Payload(0, OneMiB, 600_000);
        int compressedLength = CompressedLength(big);
        Assert.InRange(compressedLength, 516_352, OneMiB - 1); // above the stack tail's reach, still compressed

        using SessionHarness h = new(table: Table, client: o =>
        {
            GroupKit.Prompt(o);
            OrderedKit.Roomy(o);
            o.AllocatorOptions = new SlabAllocatorOptions
            {
                FreeListShards = 2,
                SizeClasses = [.. o.AllocatorOptions!.SizeClasses!, new SizeClassDefinition(OneMiB, 2)],
            };
        }, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveBudgetBytes = OneMiB;
            o.AllocatorOptions = new SlabAllocatorOptions
            {
                FreeListShards = 2,
                SizeClasses = [.. PeerCore.CreateCompactAllocatorOptions().SizeClasses!, new SizeClassDefinition(OneMiB, 1)],
            };
        });
        QuiclyPeer server = h.Server!;
        Assert.Equal(OneMiB, server.Core.MaxStageLength);

        Dictionary<int, byte[]> sent = new();
        List<int> got = [];

        // A drained channel: an application that drains every frame.
        for (int i = 0; i < 2; i++)
        {
            h.Run(20_000);
            DrainAll(server, Huge, got, sent);
        }

        Assert.Empty(got);

        Send(h.Client, Huge, sent, 0, big);
        Send(h.Client, Huge, sent, 1, BitConverter.GetBytes(1));
        Assert.True(h.RunUntil(() =>
        {
            DrainAll(server, Huge, got, sent);
            return got.Contains(1);
        }), $"the channel gave [{string.Join(",", got)}]");

        long failures = DatagramKit.Statistics(server).DecodeFailures;
        Assert.True(got.SequenceEqual([0, 1]) && failures == 0,
            $"a 1 MiB message ({compressedLength} B on the wire) whose decoded size is the largest pool block within the budget "
            + $"was staged at {server.Core.LimitedStagingLength(compressedLength, OneMiB)} B (edge band) and the channel gave "
            + $"[{string.Join(",", got)}] with DecodeFailures {failures}");
        Assert.Equal(0, server.Core.ReceiveBytesOutstanding);
    }
}
