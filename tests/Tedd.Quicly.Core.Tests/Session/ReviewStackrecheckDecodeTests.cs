using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Adversarial review, lens: recheck of the fix commits on fix/stack-review after d567ba5 (b32655c, a184b81: the decode
/// buffer may take the receive budget past its limit and falls back to a larger size class; CanEverRentDecode no longer
/// subtracts the block the waiting message holds). A test marked FINDING fails at e8af1f8 for the reason its comment gives.
/// </summary>
/// <remarks>
/// Fixed since (recheck round, RC-1): <c>PeerCore.CanEverRentDecode(raw, heldBlock)</c> counted the pool's blocks of the
/// classes a decode could use minus the message's own, so the message was dropped and counted again, as at d567ba5 and in
/// 0.2.1. That left the circular wait among several waiting messages (recheck round 2, RC2-1). Since then nothing waits for
/// a decode buffer: a compressed message is decoded in place in its own block when the block holds its raw size and the
/// in-place margin (<c>Lz4Block.TryDecompressInPlace</c>; a channel no handler reads stages it in such a block,
/// <c>PeerCore.LimitedStagingLength</c>), so the message below is delivered, through Drain and to a handler alike. The
/// description below is what the test found at e8af1f8; the test now pins the fix, and the tests after it the handler's
/// side and a pool with a second block.
/// </remarks>
public class ReviewStackrecheckDecodeTests
{
    /// <summary>ReliableOrdered with LZ4 (MinCompressSize 16) and MaxMessageSize 256 KiB (the default is 64 KiB; up to 1 MiB is allowed).</summary>
    private const ushort Packed = 6;

    /// <summary>ReliableOrdered, no compression, defaults.</summary>
    private const ushort Chat = 4;

    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(Chat, "chat", ChannelMode.ReliableOrdered)
        .Add(Packed, "packed", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; o.MaxMessageSize = 256 * 1024; })
        .Build();

    /// <summary>
    /// A payload of <paramref name="size"/> bytes whose first <paramref name="noise"/> bytes LZ4 cannot shrink and the rest
    /// are zeroes: it compresses to a little over <paramref name="noise"/> bytes.
    /// </summary>
    private static byte[] Payload(int index, int size, int noise)
    {
        byte[] payload = new byte[size];
        new Random(index + 1).NextBytes(payload.AsSpan(0, noise));
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    /// <summary>
    /// FINDING (critical, regression of b32655c against d567ba5 and v0.2.1). The receiver runs the library's defaults: a
    /// private compact pool, whose largest class has ONE block of 256 KiB, and a receive budget of 256 KiB; the channel's
    /// MaxMessageSize is raised to 256 KiB (allowed up to 1 MiB). A compressed ReliableOrdered message of 200 000 bytes that LZ4 packs to about 100 KB arrives in that one 256 KiB block (its
    /// compressed size is above the 64 KiB class). Its decode needs a 256 KiB block too — there is no second one.
    /// <para>
    /// <c>CanEverRentDecode</c> now only asks whether the block is not larger than the budget (256 KiB &lt;= 256 KiB: yes),
    /// no longer whether it fits next to the block the message itself holds, so <c>TryRentForDecode</c> answers "wait".
    /// <c>TryRentDecode</c> can never succeed: the only block of the class is the message's own. The message waits at the
    /// head of the channel for ever, holding the whole receive budget: every Drain of the channel returns 0, the message
    /// behind it never comes, and every other channel of the peer stops receiving (their messages are held for the budget).
    /// At d567ba5 the same message was dropped and counted in DecodeFailures (<c>CanEverRentReceive(raw, held)</c>: 256 KiB
    /// &lt;= 256 KiB - 256 KiB is false) and the peer went on. The Drain remarks promise "a Drain that finds everything
    /// released gets the next message's buffer (unless a shared pool has no block ...)" — the pool here is the private
    /// default one.
    /// </para>
    /// </summary>
    [Fact]
    public void A_Compressed_Message_Whose_Block_Is_The_Pools_Only_Block_Of_Its_Decode_Class_Does_Not_Stall_The_Peer()
    {
        // The client gets a roomy pool so that it can send the message; the server runs the defaults.
        using SessionHarness h = new(table: Table, client: o =>
        {
            GroupKit.Prompt(o);
            OrderedKit.Roomy(o);
        }, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        List<int> chat = [];
        server.RegisterHandler(Chat, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => chat.Add(BinaryPrimitives.ReadInt32LittleEndian(payload)));

        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), Payload(0, 200_000, 100_000)).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), Payload(1, 2_000, 0)).Status);

        // The application drains the channel every frame and releases what it got straight away.
        List<int> got = [];
        bool intact = true;
        byte[] large = Payload(0, 200_000, 100_000);
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        bool chatSent = false;
        h.RunUntil(() =>
        {
            // Once the large message has arrived: a message on a handled channel of the same peer, which should not care
            // what the drained channel does.
            if (!chatSent && DatagramKit.ChannelStats(server, Packed).Received >= 1)
            {
                Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Chat), BitConverter.GetBytes(42)).Status);
                chatSent = true;
            }

            int taken;
            while ((taken = server.Drain(Packed, buffer)) > 0)
            {
                for (int i = 0; i < taken; i++)
                {
                    int index = BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload);
                    got.Add(index);
                    intact &= index != 0 || buffer[i].Payload.SequenceEqual(large);
                }

                server.Release(buffer.AsSpan(0, taken));
            }

            return got.Contains(1) && chat.Count == 1;
        }, 2_000_000);

        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(got.Contains(1) && chat.Count == 1,
            $"two seconds of Poll + Drain + Release every frame: the drained channel gave [{string.Join(",", got)}] (message 1 {(got.Contains(1) ? "arrived" : "never came")}), "
            + $"the handled channel {chat.Count} of 1; ReceiveBytesOutstanding {statistics.ReceiveBytesOutstanding} of 262144, DecodeFailures {statistics.DecodeFailures}, "
            + $"StreamReceivePends {statistics.StreamReceivePends}, the drained channel's Received {DatagramKit.ChannelStats(server, Packed).Received} (chat sent after it arrived: {chatSent})");

        // As fixed: the message is decoded in its own block (the pool has no other of its class), intact and in order.
        Assert.Equal([0, 1], got);
        Assert.True(intact, "the message decoded in its own block is not the payload that was sent");
        Assert.Equal(0, statistics.DecodeFailures);
        Assert.Equal(0, statistics.ReceiveBytesOutstanding);
    }

    /// <summary>
    /// The same message on a channel read by a handler: decoded in its own block too (the pool has no other of its class),
    /// and the handler gets it and the message behind it, as every channel of the peer gets its own.
    /// </summary>
    [Fact]
    public void The_Same_Message_To_A_Handler_Is_Decoded_In_Its_Own_Block_And_The_Peer_Goes_On()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            GroupKit.Prompt(o);
            OrderedKit.Roomy(o);
        }, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        List<int> packed = [];
        List<int> chat = [];
        byte[] large = Payload(0, 200_000, 100_000);
        bool intact = true;
        server.RegisterHandler(Packed, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            int index = BinaryPrimitives.ReadInt32LittleEndian(payload);
            packed.Add(index);
            intact &= index != 0 || payload.SequenceEqual(large);
        });
        server.RegisterHandler(Chat, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => chat.Add(BinaryPrimitives.ReadInt32LittleEndian(payload)));

        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), large).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), Payload(1, 2_000, 0)).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Chat), BitConverter.GetBytes(42)).Status);

        Assert.True(h.RunUntil(() => packed.Contains(1) && chat.Count == 1, 2_000_000),
            $"the handled channel gave [{string.Join(",", packed)}], chat {chat.Count} of 1, DecodeFailures {DatagramKit.Statistics(server).DecodeFailures}");
        Assert.Equal([0, 1], packed);
        Assert.True(intact, "the message decoded in its own block is not the payload that was sent");
        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
    }

    /// <summary>
    /// A pool with a second block of 256 KiB (the remedy the release notes gave before RC2-1) decodes the message through
    /// <c>Drain</c> as well — in place, in its own block — and in order with the message behind it.
    /// </summary>
    [Fact]
    public void A_Pool_With_Two_Blocks_Of_The_Class_Decodes_The_Message()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            GroupKit.Prompt(o);
            OrderedKit.Roomy(o);
        }, server: o =>
        {
            GroupKit.Prompt(o);
            SizeClassDefinition[] classes = PeerCore.CreateCompactAllocatorOptions().SizeClasses!;
            classes[^1] = new SizeClassDefinition(classes[^1].BlockSize, 2);
            o.AllocatorOptions = new SlabAllocatorOptions { FreeListShards = 2, SizeClasses = classes };
        });
        QuiclyPeer server = h.Server!;
        byte[] large = Payload(0, 200_000, 100_000);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), large).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), Payload(1, 2_000, 0)).Status);

        List<int> got = [];
        bool intact = true;
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        Assert.True(h.RunUntil(() =>
        {
            int taken;
            while ((taken = server.Drain(Packed, buffer)) > 0)
            {
                for (int i = 0; i < taken; i++)
                {
                    int index = BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload);
                    got.Add(index);
                    intact &= index != 0 || buffer[i].Payload.SequenceEqual(large);
                }

                server.Release(buffer.AsSpan(0, taken));
            }

            return got.Count == 2;
        }, 2_000_000), $"the drained channel gave [{string.Join(",", got)}], DecodeFailures {DatagramKit.Statistics(server).DecodeFailures}");
        Assert.Equal([0, 1], got);
        Assert.True(intact);
        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
    }
}
