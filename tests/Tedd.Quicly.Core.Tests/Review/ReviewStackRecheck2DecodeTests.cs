using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review, recheck round 2 of fix/stack-review (1b33437), lens: a1480f8 (RC-1 fix,
/// <c>PeerCore.CanEverRentDecode(length, heldBlock)</c>). A test marked FINDING fails at 1b33437 for the reason its comment
/// gives.
/// </summary>
/// <remarks>
/// <para>
/// The RC-1 fix subtracts only the message's OWN block from the pool's blocks a decode could use. The other compressed
/// messages that wait for their decode also hold blocks of those classes, and none of them gives its block back before it
/// is decoded. When the waiting messages together hold every block of the classes their decodes could use, each one is
/// told "wait" (the pool has blocks besides its own), no one can ever rent, and every one waits for good: a circular wait
/// with the same effect as RC-1 (the waiting messages pin their peers' receive budgets, the channels give nothing more).
/// </para>
/// <para>
/// At d567ba5 and in 0.2.1 these messages were dropped and counted (DecodeFailures) and the peers went on. The shapes below
/// are the two remedies the release notes give for RC-1's known limit ("a pool with two blocks of 256 KiB", "or a shared
/// allocator, as a server has"), and the one with a raised receive budget.
/// </para>
/// </remarks>
public class ReviewStackRecheck2DecodeTests
{
    /// <summary>ReliableOrdered with LZ4 (MinCompressSize 16) and MaxMessageSize 256 KiB.</summary>
    private const ushort PackedA = 6;

    /// <summary>A second channel like <see cref="PackedA"/>.</summary>
    private const ushort PackedB = 7;

    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(4, "chat", ChannelMode.ReliableOrdered)
        .Add(PackedA, "packed-a", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; o.MaxMessageSize = 256 * 1024; })
        .Add(PackedB, "packed-b", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; o.MaxMessageSize = 256 * 1024; })
        .Build();

    /// <summary>200 000 bytes whose first 100 000 LZ4 cannot shrink: compressed about 100 KB (a 256 KiB block), raw 200 000 (a 256 KiB decode).</summary>
    private static byte[] Large(int index)
    {
        byte[] payload = new byte[200_000];
        new Random(index + 1).NextBytes(payload.AsSpan(0, 100_000));
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    /// <summary>The compact pool with <paramref name="blocks"/> blocks of 256 KiB (the release notes' remedy is two).</summary>
    private static SlabAllocatorOptions CompactWith256KiBBlocks(int blocks)
    {
        SizeClassDefinition[] classes = PeerCore.CreateCompactAllocatorOptions().SizeClasses!;
        classes[^1] = new SizeClassDefinition(classes[^1].BlockSize, blocks);
        return new SlabAllocatorOptions { FreeListShards = 2, SizeClasses = classes };
    }

    private static void Client(PeerOptions o)
    {
        GroupKit.Prompt(o);
        OrderedKit.Roomy(o);
    }

    /// <summary>Drains <paramref name="channel"/> until it returns 0, releasing straight away; returns the indices it got.</summary>
    private static void DrainAll(QuiclyPeer peer, ushort channel, List<int> into)
    {
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        int taken;
        while ((taken = peer.Drain(channel, buffer)) > 0)
        {
            for (int i = 0; i < taken; i++)
            {
                into.Add(BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload));
            }

            peer.Release(buffer.AsSpan(0, taken));
        }
    }

    /// <summary>
    /// FINDING (critical; a whole-peer stall like RC-1, with the release notes' own remedy). The receiver follows the
    /// remedy the release notes give for RC-1's known limit — the compact pool with TWO blocks of 256 KiB — and raises the
    /// receive budget to 1 MiB (the application sends messages of up to 256 KiB, so a larger budget is the obvious setting
    /// next to a raised MaxMessageSize). The application drains the channel every frame and releases at once.
    /// <para>
    /// Two compressed messages of 200 000 bytes (about 100 KB compressed) arrive back to back on one ReliableOrdered
    /// channel. Each compressed form takes a 256 KiB block (the drained threshold is half the budget, 512 KiB, so the second
    /// is started behind the first), so both blocks of the class are held by messages that wait for their decode. The head's
    /// decode needs a 256 KiB block: <c>CanEverRentDecode(200 000, 262 144)</c> counts 2 - 1 = 1 block besides its own and
    /// answers "can", <c>TryRentDecode</c> finds none (the other one is held by the message behind it, which cannot be decoded
    /// before the head), and the head waits for ever. Nothing of the channel comes again.
    /// </para>
    /// </summary>
    [Fact]
    public void Two_Large_Compressed_Messages_On_One_Channel_With_The_Two_Block_Remedy_Are_Not_Stuck_For_Good()
    {
        using SessionHarness h = new(table: Table, client: Client, server: o =>
        {
            GroupKit.Prompt(o);
            o.AllocatorOptions = CompactWith256KiBBlocks(2);
            o.ReceiveBudgetBytes = 1024 * 1024;
        });
        QuiclyPeer server = h.Server!;
        List<int> got = [];

        // The application drains the channel every frame from the start.
        h.Run(20_000);
        DrainAll(server, PackedA, got);
        h.Run(20_000);
        DrainAll(server, PackedA, got);

        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(PackedA), Large(0)).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(PackedA), Large(1)).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(PackedA), BitConverter.GetBytes(2)).Status);

        bool done = h.RunUntil(() =>
        {
            DrainAll(server, PackedA, got);
            return got.Contains(2);
        }, 2_000_000);

        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(done,
            $"two seconds of Poll + Drain + Release every frame: the channel gave [{string.Join(",", got)}] (message 2 never came); "
            + $"Received {OrderedKit.Stats(server, PackedA).Received}, ReceiveBytesOutstanding {statistics.ReceiveBytesOutstanding} of 1048576, "
            + $"DecodeFailures {statistics.DecodeFailures}, StreamReceivePends {statistics.StreamReceivePends}");
    }

    /// <summary>
    /// FINDING (critical, same cause). The same remedy pool (two blocks of 256 KiB), a receive budget of 512 KiB, and two
    /// drained LZ4 channels. The host misses a few frames (a hitch) while one large compressed message arrives on each
    /// channel: each is the first message of an empty drained channel, so each is accepted and takes one 256 KiB block. Each
    /// head then waits for the block the other holds; both channels are stuck for good, with the whole 512 KiB budget.
    /// </summary>
    [Fact]
    public void Two_Channels_Each_Holding_A_Large_Compressed_Head_Do_Not_Wait_For_Each_Other_For_Good()
    {
        using SessionHarness h = new(table: Table, client: Client, server: o =>
        {
            GroupKit.Prompt(o);
            o.AllocatorOptions = CompactWith256KiBBlocks(2);
            o.ReceiveBudgetBytes = 512 * 1024;
        });
        QuiclyPeer server = h.Server!;
        List<int> a = [];
        List<int> b = [];

        h.Run(20_000);
        DrainAll(server, PackedA, a);
        DrainAll(server, PackedB, b);
        h.Run(20_000);
        DrainAll(server, PackedA, a);
        DrainAll(server, PackedB, b);

        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(PackedA), Large(0)).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(PackedB), Large(10)).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(PackedA), BitConverter.GetBytes(1)).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(PackedB), BitConverter.GetBytes(11)).Status);

        // The hitch: the host polls but does not drain until both large messages are in.
        Assert.True(h.RunUntil(() => OrderedKit.Stats(server, PackedA).Received >= 1 && OrderedKit.Stats(server, PackedB).Received >= 1, 2_000_000),
            "the two large messages did not arrive");

        bool done = h.RunUntil(() =>
        {
            DrainAll(server, PackedA, a);
            DrainAll(server, PackedB, b);
            return a.Contains(1) && b.Contains(11);
        }, 2_000_000);

        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(done,
            $"two seconds of Drain + Release every frame after the hitch: A gave [{string.Join(",", a)}], B gave [{string.Join(",", b)}]; "
            + $"ReceiveBytesOutstanding {statistics.ReceiveBytesOutstanding} of 524288, DecodeFailures {statistics.DecodeFailures}");
    }

    /// <summary>
    /// FINDING (critical, same cause, across peers). The other remedy the release notes give: a shared allocator
    /// (<c>PeerOptions.Allocator</c>, as a server has). Two server peers share one pool with two blocks of 256 KiB (a
    /// server's default pool has twelve; see the Server test of this round for that shape), each at the default 256 KiB
    /// budget. Each peer's client sends one large compressed message on a drained channel while the host is in a hitch;
    /// each takes one of the two blocks, and each head waits for the block the other peer's head holds. Both peers are
    /// stalled for good — their whole receive budget is pinned, so every other stream channel of theirs stops too.
    /// </summary>
    [Fact]
    public void Two_Peers_Sharing_A_Pool_Do_Not_Wait_For_Each_Others_Block_For_Good()
    {
        using SlabAllocator shared = new(CompactWith256KiBBlocks(2));
        using SessionHarness h1 = new(table: Table, client: Client, server: o =>
        {
            GroupKit.Prompt(o);
            o.Allocator = shared;
        });
        using SessionHarness h2 = new(table: Table, client: Client, server: o =>
        {
            GroupKit.Prompt(o);
            o.Allocator = shared;
        });
        QuiclyPeer s1 = h1.Server!;
        QuiclyPeer s2 = h2.Server!;
        List<int> got1 = [];
        List<int> got2 = [];

        for (int i = 0; i < 2; i++)
        {
            h1.Run(20_000);
            h2.Run(20_000);
            DrainAll(s1, PackedA, got1);
            DrainAll(s2, PackedA, got2);
        }

        Assert.Equal(SendStatus.Admitted, h1.Client.SendCopy(new SendHeader(PackedA), Large(0)).Status);
        Assert.Equal(SendStatus.Admitted, h1.Client.SendCopy(new SendHeader(PackedA), BitConverter.GetBytes(1)).Status);
        Assert.Equal(SendStatus.Admitted, h2.Client.SendCopy(new SendHeader(PackedA), Large(20)).Status);
        Assert.Equal(SendStatus.Admitted, h2.Client.SendCopy(new SendHeader(PackedA), BitConverter.GetBytes(21)).Status);

        // The hitch: both hosts poll but do not drain until both large messages are in.
        Assert.True(h1.RunUntil(() => OrderedKit.Stats(s1, PackedA).Received >= 1, 2_000_000), "peer 1's message did not arrive");
        Assert.True(h2.RunUntil(() => OrderedKit.Stats(s2, PackedA).Received >= 1, 2_000_000), "peer 2's message did not arrive");

        bool done = false;
        for (int frame = 0; frame < 2_000 && !done; frame++)
        {
            h1.Run(1_000);
            h2.Run(1_000);
            DrainAll(s1, PackedA, got1);
            DrainAll(s2, PackedA, got2);
            done = got1.Contains(1) && got2.Contains(21);
        }

        PeerStatistics st1 = DatagramKit.Statistics(s1);
        PeerStatistics st2 = DatagramKit.Statistics(s2);
        Assert.True(done,
            $"two seconds of Drain + Release every frame on both peers: peer 1 gave [{string.Join(",", got1)}], peer 2 gave [{string.Join(",", got2)}]; "
            + $"ReceiveBytesOutstanding {st1.ReceiveBytesOutstanding} / {st2.ReceiveBytesOutstanding} of 262144, "
            + $"DecodeFailures {st1.DecodeFailures} / {st2.DecodeFailures}");
    }
}
