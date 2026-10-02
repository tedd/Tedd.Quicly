using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// RC2-1: a compressed message of a reliable channel that no handler reads is staged in a block that holds its decoded size
/// (<see cref="PeerCore.LimitedStagingLength"/>), rented all or nothing with its ring slot and its credit at its start, and
/// decoded in place in that block. No decode waits for a buffer, so messages can no longer hold blocks while they wait
/// for each other's; what cannot be decoded in place is tried once in a second buffer and otherwise dropped and counted.
/// </summary>
public class CompressedStagingTests
{
    private const ushort Moves = 2;
    private const ushort Chat = 4;

    /// <summary>ReliableOrdered, LZ4 (MinCompressSize 16), MaxMessageSize 256 KiB.</summary>
    private const ushort Packed = 6;

    /// <summary>ReliableUnordered, LZ4 (MinCompressSize 16), MaxMessageSize 256 KiB.</summary>
    private const ushort Groups = 7;

    /// <summary>ReliableOrdered request/response, LZ4 (MinCompressSize 16).</summary>
    private const ushort Rpc = 8;

    /// <summary>ReliableOrdered, LZ4 (MinCompressSize 16), MaxMessageSize 1 MiB: decoded sizes beyond the largest block.</summary>
    private const ushort Huge = 9;

    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(Moves, "moves", ChannelMode.UnreliableUnordered)
        .Add(Chat, "chat", ChannelMode.ReliableOrdered)
        .Add(Packed, "packed", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; o.MaxMessageSize = 256 * 1024; })
        .Add(Groups, "groups", ChannelMode.ReliableUnordered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; o.MaxMessageSize = 256 * 1024; })
        .Add(Rpc, "rpc", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; o.RequestResponse = true; })
        .Add(Huge, "huge", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; o.MaxMessageSize = 1024 * 1024; })
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

    private static SlabAllocatorOptions CompactWith(int blockSize, int blocks)
    {
        SizeClassDefinition[] classes = PeerCore.CreateCompactAllocatorOptions().SizeClasses!;
        for (int i = 0; i < classes.Length; i++)
        {
            if (classes[i].BlockSize == blockSize)
            {
                classes[i] = new SizeClassDefinition(blockSize, blocks);
            }
        }

        return new SlabAllocatorOptions { FreeListShards = 2, SizeClasses = classes };
    }

    private static SizeClassStatistics ClassOf(SlabAllocator allocator, int blockSize)
    {
        SlabStatistics statistics = allocator.GetStatistics();
        for (int i = 0; i < statistics.ClassCount; i++)
        {
            if (statistics[i].BlockSize == blockSize)
            {
                return statistics[i];
            }
        }

        throw new InvalidOperationException($"no class of {blockSize} bytes");
    }

    /// <summary>Drains a channel until it returns 0, checking every payload against what was sent; releases at once.</summary>
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

    /// <summary>Makes a channel a drained one (an empty Drain), as an application that drains every frame does.</summary>
    private static void MarkDrained(SessionHarness h, ushort channel)
    {
        List<int> none = [];
        for (int i = 0; i < 2; i++)
        {
            h.Run(20_000);
            DrainAll(h.Server!, channel, none, new Dictionary<int, byte[]>());
        }

        Assert.Empty(none);
    }

    /// <summary>Only the client and the network run: the server's host is late with its Poll.</summary>
    private static void PumpClient(SessionHarness h, int steps)
    {
        for (int step = 0; step < steps; step++)
        {
            h.Client.Poll();
            h.Client.Flush();
            h.Network.Advance(1_000);
        }
    }

    // ------------------------------------------------------------------ staging size

    [Fact]
    public void A_Compressed_Message_Of_A_Drained_Channel_Is_Staged_At_Its_Decoded_Size_And_Decoded_In_That_Block()
    {
        // About 60 KB compressed (a 64 KiB block at its wire length), 200 000 decoded: staged in the 256 KiB block.
        using SessionHarness h = new(table: Table, client: Client, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Packed);
        Dictionary<int, byte[]> sent = new();
        List<int> got = [];
        MarkDrained(h, Packed);

        Send(h.Client, Packed, sent, 0, Payload(0, 200_000, 60_000));
        Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(server, Packed).Received == 1));

        // Counted with the block it is staged in, in the credit and in the receive budget alike.
        Assert.Equal(262_144, server.Core.ReceiveBytesOutstanding);
        Assert.Equal(262_144, server.Core.Credit.WaitingBytes(index));
        Assert.Equal(0, ClassOf(server.Core.Allocator, 65_536).Rented);
        Assert.Equal(1, ClassOf(server.Core.Allocator, 262_144).Rented);

        // Decoded in place: no second block, ever.
        DrainAll(server, Packed, got, sent);
        Assert.Equal([0], got);
        Assert.Equal(0, ClassOf(server.Core.Allocator, 65_536).Peak);
        Assert.Equal(1, ClassOf(server.Core.Allocator, 262_144).Peak);
        Assert.Equal(0, server.Core.ReceiveBytesOutstanding);
        Assert.Equal(0, server.Core.Credit.WaitingBytes(index));
        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
    }

    [Fact]
    public void A_Compressed_Message_Of_A_Handled_Channel_Is_Staged_At_Its_Wire_Length()
    {
        // A handler never waits, so its messages keep the small block (the throughput of a handled channel), and the decode
        // goes to a second buffer when the block cannot take it in place.
        using SessionHarness h = new(table: Table, client: Client, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        byte[] payload = Payload(0, 200_000, 60_000);
        long outstanding = -1;
        bool intact = false;
        server.RegisterHandler(Packed, (QuiclyPeer peer, in ReceiveHeader _, ReadOnlySpan<byte> data) =>
        {
            outstanding = peer.Core.ReceiveBytesOutstanding;
            intact = data.SequenceEqual(payload);
        });

        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), payload).Status);
        Assert.True(h.RunUntil(() => outstanding >= 0));
        Assert.True(intact);
        Assert.Equal(262_144, outstanding); // the decoded block; the 64 KiB staging block went back after the decode
        Assert.Equal(1, ClassOf(server.Core.Allocator, 65_536).Peak);
        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
        Assert.Equal(0, server.Core.ReceiveBytesOutstanding);
    }

    // ------------------------------------------------------------------ nothing waits holding a block

    /// <summary>
    /// The shape the partial work of RC2-1 timed out: the head (about 60 KB compressed, 200 000 decoded) and the message
    /// behind it (about 100 KB compressed, 200 000 decoded) both need the pool's one 256 KiB block. The head is staged in it;
    /// the next start cannot have it and holds nothing while it waits (no ring slot, no lease, no credit), so the head is
    /// decoded in place, released, and the second follows — intact and in order, nothing dropped.
    /// </summary>
    [Fact]
    public void A_Start_That_Waits_For_The_Pools_Last_Block_Holds_Nothing_And_Every_Message_Arrives()
    {
        using SessionHarness h = new(table: Table, client: Client, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveBudgetBytes = 1024 * 1024;
        });
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Packed);
        Dictionary<int, byte[]> sent = new();
        List<int> got = [];
        MarkDrained(h, Packed);

        Send(h.Client, Packed, sent, 0, Payload(0, 200_000, 60_000));
        Send(h.Client, Packed, sent, 1, Payload(1, 200_000, 100_000));
        Send(h.Client, Packed, sent, 2, BitConverter.GetBytes(2));

        // While the second start waits: only the head is held, by its block, its credit and its slot.
        Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(server, Packed).Received == 1 && DatagramKit.Statistics(server).StreamReceivePends > 0));
        h.Run(20_000);
        Assert.Equal(1, DatagramKit.ChannelStats(server, Packed).Received);
        Assert.Equal(262_144, server.Core.ReceiveBytesOutstanding);
        Assert.Equal(1, server.Core.Credit.Waiting(index));
        Assert.Equal(262_144, server.Core.Credit.WaitingBytes(index));

        Assert.True(h.RunUntil(() =>
        {
            DrainAll(server, Packed, got, sent);
            return got.Contains(2);
        }, 3_000_000), $"the channel gave [{string.Join(",", got)}]");

        Assert.Equal([0, 1, 2], got);
        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
        Assert.Equal(0, server.Core.ReceiveBytesOutstanding);
        Assert.Equal(1, ClassOf(server.Core.Allocator, 262_144).Peak);
    }

    /// <summary>
    /// The strict share of a channel nobody reads counts the block the message is staged in: a compressed message whose
    /// wire block fits the share but whose decoded block does not waits until the channel is read, and its credit comes
    /// back whole.
    /// </summary>
    [Fact]
    public void The_Share_Of_A_Channel_Nobody_Reads_Counts_The_Decoded_Block()
    {
        using SessionHarness h = new(table: Table, client: Client, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Packed);
        Assert.True(server.Core.Credit.ByteLimit < 16_384, $"the share is {server.Core.Credit.ByteLimit}");
        Dictionary<int, byte[]> sent = new();
        List<int> got = [];

        // About 550 bytes compressed (a 1 536-byte block), 12 000 decoded (a 16 KiB block, larger than the share).
        Send(h.Client, Packed, sent, 0, Payload(0, 12_000, 500));
        Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(server, Packed).BacklogHolds > 0));
        h.Run(50_000);
        Assert.Equal(0, DatagramKit.ChannelStats(server, Packed).Received);
        Assert.Equal(0, server.Core.ReceiveBytesOutstanding);

        // The first Drain finds nothing and shows that the channel is read; the message follows.
        Assert.True(h.RunUntil(() =>
        {
            DrainAll(server, Packed, got, sent);
            return got.Count == 1;
        }));
        Assert.Equal(0, server.Core.Credit.WaitingBytes(index));
        Assert.Equal(0, server.Core.Credit.Waiting(index));
        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
    }

    // ------------------------------------------------------------------ handlers

    [Fact]
    public void A_Handler_Registered_Over_Messages_Staged_At_Their_Decoded_Size_Decodes_Them_In_Place_And_May_Retain_Them()
    {
        using SessionHarness h = new(table: Table, client: Client, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveBudgetBytes = 1024 * 1024;
        });
        QuiclyPeer server = h.Server!;
        Dictionary<int, byte[]> sent = new();
        MarkDrained(h, Packed);
        for (int i = 0; i < 3; i++)
        {
            Send(h.Client, Packed, sent, i, Payload(i, 12_000, 2_000));
        }

        Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(server, Packed).Received == 3));
        Assert.Equal(3 * 16_384, server.Core.ReceiveBytesOutstanding);

        List<int> got = [];
        ReceiveLease retained = default;
        server.RegisterHandler(Packed, (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> data) =>
        {
            int index = BinaryPrimitives.ReadInt32LittleEndian(data);
            Assert.True(data.SequenceEqual(sent[index]), $"message {index} is not the payload that was sent");
            got.Add(index);
            if (index == 1)
            {
                retained = peer.Retain(in header);
            }
        });

        Assert.True(h.RunUntil(() => got.Count == 3));
        Assert.Equal([0, 1, 2], got);
        Assert.True(retained.IsValid);
        Assert.True(retained.Payload.SequenceEqual(sent[1]));
        Assert.Equal(16_384, server.Core.ReceiveBytesOutstanding);
        server.Release(in retained);
        Assert.Equal(0, server.Core.ReceiveBytesOutstanding);
        Assert.Equal(3, ClassOf(server.Core.Allocator, 16_384).Peak);
        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
    }

    /// <summary>
    /// Messages staged at their wire length while the channel had a handler, read through Drain after the handler is
    /// removed, with the class their decode needs held by the application: tried once, dropped and counted, and the channel
    /// goes on (a known limit, as in 0.2.1). It never stalls.
    /// </summary>
    [Fact]
    public void A_Message_Staged_While_Handled_Without_A_Free_Decode_Buffer_Is_Dropped_Once_And_The_Channel_Goes_On()
    {
        using SessionHarness h = new(table: Table, client: Client, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        server.RegisterHandler(Packed, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        Dictionary<int, byte[]> sent = new();
        Send(h.Client, Packed, sent, 0, Payload(0, 200_000, 60_000));
        Send(h.Client, Packed, sent, 1, BitConverter.GetBytes(1));
        for (int step = 0; step < 2_000 && DatagramKit.ChannelStats(server, Packed).Received < 2; step++)
        {
            PumpClient(h, 1);
        }

        Assert.Equal(2, DatagramKit.ChannelStats(server, Packed).Received);

        Assert.True(server.UnregisterHandler(Packed));
        Assert.True(server.Core.Allocator.TryRent(262_144, out BufferLease held));
        try
        {
            List<int> got = [];
            DrainAll(server, Packed, got, sent);
            Assert.Equal([1], got);
            Assert.Equal(1, DatagramKit.Statistics(server).DecodeFailures);

            Send(h.Client, Packed, sent, 2, Payload(2, 2_000, 100));
            Assert.True(h.RunUntil(() =>
            {
                DrainAll(server, Packed, got, sent);
                return got.Contains(2);
            }));
            Assert.Equal(1, DatagramKit.Statistics(server).DecodeFailures);
        }
        finally
        {
            server.Core.Allocator.Return(in held);
        }

        Assert.Equal(0, server.Core.ReceiveBytesOutstanding);
    }

    /// <summary>
    /// The edge band: a decoded size whose block and margin do not fit the largest block within the budget (a channel whose
    /// MaxMessageSize is raised to 1 MiB, a message of 255.98 KiB) is staged at its wire length and decoded into a second
    /// buffer when one is free; with the class held by the application it is dropped and counted, never waited for.
    /// </summary>
    [Fact]
    public void The_Edge_Band_Is_Decoded_When_A_Buffer_Is_Free_And_Dropped_Never_Stalled_When_None_Is()
    {
        const int Raw = 262_130;
        using SessionHarness h = new(table: Table, client: Client, server: GroupKit.Prompt);
        QuiclyPeer server = h.Server!;
        Assert.True(server.Core.LimitedStagingLength(1_100, Raw) == 1_100, "the message is not in the edge band");
        Dictionary<int, byte[]> sent = new();
        List<int> got = [];
        MarkDrained(h, Huge);

        Send(h.Client, Huge, sent, 0, Payload(0, Raw, 1_000));
        Assert.True(h.RunUntil(() =>
        {
            DrainAll(server, Huge, got, sent);
            return got.Count == 1;
        }));
        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);

        Assert.True(server.Core.Allocator.TryRent(262_144, out BufferLease held));
        try
        {
            Send(h.Client, Huge, sent, 1, Payload(1, Raw, 1_000));
            Send(h.Client, Huge, sent, 2, BitConverter.GetBytes(2));
            Assert.True(h.RunUntil(() =>
            {
                DrainAll(server, Huge, got, sent);
                return got.Contains(2);
            }), $"the channel gave [{string.Join(",", got)}]");
            Assert.Equal([0, 2], got);
            Assert.Equal(1, DatagramKit.Statistics(server).DecodeFailures);
        }
        finally
        {
            server.Core.Allocator.Return(in held);
        }

        Assert.Equal(0, server.Core.ReceiveBytesOutstanding);
    }

    /// <summary>A compressed response (not credited, staged at its wire length) decodes in place when its block can take it.</summary>
    [Fact]
    public void A_Compressed_Response_Whose_Block_Holds_It_Decodes_In_Place()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            GroupKit.Prompt(o);
            o.AllocatorOptions = PeerCore.CreateCompactAllocatorOptions();
        }, server: o =>
        {
            GroupKit.Prompt(o);
            OrderedKit.Roomy(o);
        });
        QuiclyPeer server = h.Server!;

        // About 720 bytes compressed (a 1 536-byte block), 1 400 decoded: decodes in that block.
        byte[] response = Payload(7, 1_400, 700);
        server.RegisterHandler(Rpc, (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> _) => Assert.Equal(SendStatus.Admitted, peer.Respond(in header, response).Status));
        int before = ClassOf(h.Client.Core.Allocator, 1_536).Peak;

        ValueTask<ReceiveLease> pending = h.Client.SendRequestAsync(new SendHeader(Rpc), new byte[8], TimeSpan.FromSeconds(5));
        Assert.True(h.RunUntil(() => pending.IsCompleted));
        ReceiveLease lease = pending.Result;
        Assert.True(lease.Payload.SequenceEqual(response));
        Assert.True((lease.Header.Flags & ReceiveFlags.Compressed) != 0);
        Assert.Equal(Math.Max(before, 1), ClassOf(h.Client.Core.Allocator, 1_536).Peak);
        h.Client.Release(in lease);
        Assert.Equal(0, DatagramKit.Statistics(h.Client).DecodeFailures);
    }

    // ------------------------------------------------------------------ raw peers: declarations, malformed blocks

    private static byte[] CompressedGroupStream(ChannelTable table, ushort channel, ulong groupId, int length, int rawLength, ReadOnlySpan<byte> payload)
    {
        ChannelDefinition definition = table[channel]!;
        StreamMessageHeader header = default;
        header.Length = length;
        header.RawLength = rawLength;
        byte[] stream = new byte[32 + StreamFraming.MaxFrameHeaderLength + payload.Length];
        int written = StreamFraming.WriteGroupPreamble(stream, channel, groupId);
        written += StreamFraming.WriteFrameHeader(stream.AsSpan(written), definition, in header);
        payload.CopyTo(stream.AsSpan(written));
        return stream.AsSpan(0, written + payload.Length).ToArray();
    }

    private static ServerHarness RawHarness() => new(table: Table, server: o =>
    {
        GroupKit.Prompt(o);
        o.StreamIdleTimeout = TimeSpan.FromMilliseconds(200);
    });

    private static void MarkDrained(ServerHarness h, ushort channel)
    {
        for (int i = 0; i < 2; i++)
        {
            h.Run(20_000);
            Assert.Equal(0, h.Server!.Drain(channel, new ReceivedMessage[4]));
        }
    }

    /// <summary>
    /// A peer that declares a large decoded size and stalls pins what one that declares a large wire length pins: the same
    /// block, under the same caps, freed at the stream idle timeout. The decoded size adds no amplification.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_Stalled_Declaration_Pins_The_Same_Block_As_An_Uncompressed_One_Until_The_Idle_Timeout(bool compressed)
    {
        using ServerHarness h = RawHarness();
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;
        MarkDrained(h, Groups);

        byte[] stream = compressed
            ? CompressedGroupStream(Table, Groups, 1, length: 1_000, rawLength: 200_000, new byte[10])
            : CompressedGroupStream(Table, Groups, 1, length: 200_000, rawLength: 0, new byte[10]);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(stream, out _));
        Assert.True(h.RunUntil(() => server.Core.ReceiveBytesOutstanding > 0));
        Assert.Equal(262_144, server.Core.ReceiveBytesOutstanding);

        Assert.True(h.RunUntil(() => h.Statistics().StreamIdleTimeouts == 1, 2_000_000));
        Assert.True(h.RunUntil(() => server.Core.ReceiveBytesOutstanding == 0));
        Assert.Equal(0, server.Core.Credit.WaitingBytes(server.Core.ChannelIndexOf(Groups)));
    }

    /// <summary>A malformed block in a message staged at its decoded size: dropped once, its credit back once, the channel goes on.</summary>
    [Fact]
    public void A_Malformed_Block_Decoded_In_Place_Is_Dropped_Once_And_Its_Credit_Comes_Back_Once()
    {
        using ServerHarness h = RawHarness();
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Groups);
        MarkDrained(h, Groups);

        byte[] garbage = new byte[100];
        new Random(5).NextBytes(garbage);
        garbage[0] = 0xFF; // a literal run longer than the block: malformed whatever follows
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(CompressedGroupStream(Table, Groups, 1, 100, 1_000, garbage), out _, fin: true));
        Assert.True(h.RunUntil(() => GroupKit.Stats(server, Groups).Received == 1));
        Assert.Equal(1_536, server.Core.Credit.WaitingBytes(index));

        byte[] good = Payload(5, 1_000, 10);
        byte[] packed = new byte[Lz4Block.GetMaxCompressedLength(good.Length)];
        int length = Lz4Block.Compress(good, packed);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(CompressedGroupStream(Table, Groups, 2, length, good.Length, packed.AsSpan(0, length)), out _, fin: true));
        Assert.True(h.RunUntil(() => GroupKit.Stats(server, Groups).Received == 2));

        List<int> got = [];
        DrainAll(server, Groups, got, new Dictionary<int, byte[]> { [5] = good });
        Assert.Equal([5], got);
        Assert.Equal(1, h.Statistics().DecodeFailures);
        Assert.Equal(0, server.Core.Credit.Waiting(index));
        Assert.Equal(0, server.Core.Credit.WaitingBytes(index));
        Assert.Equal(0, server.Core.ReceiveBytesOutstanding);
    }

    // ------------------------------------------------------------------ shared pools

    [Fact]
    public void Peers_On_A_Shared_Pool_Give_Every_Block_Back_When_They_Are_Disposed()
    {
        using SlabAllocator shared = new(CompactWith(262_144, 4));
        Dictionary<int, byte[]> sent = new();
        for (int round = 0; round < 2; round++)
        {
            using SessionHarness h = new(table: Table, client: Client, server: o =>
            {
                GroupKit.Prompt(o);
                o.Allocator = shared;
            });
            QuiclyPeer server = h.Server!;
            MarkDrained(h, Packed);
            Send(h.Client, Packed, sent, 0, Payload(0, 200_000, 60_000));
            Send(h.Client, Packed, sent, 1, Payload(1, 200_000, 100_000));
            Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(server, Packed).Received >= 1));
            Assert.Equal(262_144, server.Core.ReceiveBytesOutstanding);
        }

        SlabStatistics statistics = shared.GetStatistics();
        for (int i = 0; i < statistics.ClassCount; i++)
        {
            Assert.Equal(0, statistics[i].Rented);
        }
    }

    // ------------------------------------------------------------------ the property across shapes

    public static TheoryData<int, int, int, int> Shapes() => new()
    {
        // budget, channels, peers on one pool, 256 KiB blocks in the pool
        { 128 * 1024, 1, 1, 1 },
        { 256 * 1024, 2, 1, 1 },
        { 512 * 1024, 2, 1, 2 },
        { 1024 * 1024, 1, 1, 2 },
        { 1024 * 1024, 2, 2, 2 },
        { 256 * 1024, 1, 3, 2 },
    };

    /// <summary>
    /// Random compressed messages up to what the shape can stage at its decoded size, random hitches of the host, Drain and
    /// Release every frame otherwise: every message arrives in order, the receive budget is never exceeded, nothing is
    /// dropped, and every block is back when the peers are gone.
    /// </summary>
    [Theory]
    [MemberData(nameof(Shapes))]
    public void Every_Message_Arrives_Within_The_Budget_Whatever_The_Pool_Shape(int budget, int channels, int peers, int largeBlocks)
    {
        using SlabAllocator shared = new(CompactWith(262_144, largeBlocks));
        ushort[] ids = [Packed, Groups];
        List<SessionHarness> harnesses = [];
        try
        {
            for (int p = 0; p < peers; p++)
            {
                harnesses.Add(new SessionHarness(table: Table, client: Client, seed: p + 1, server: o =>
                {
                    GroupKit.Prompt(o);
                    o.Allocator = shared;
                    o.ReceiveBudgetBytes = budget;
                }));
            }

            int stage = harnesses[0].Server!.Core.MaxStageLength;
            Random random = new(budget + (channels * 7) + (peers * 31) + largeBlocks);
            var sent = new Dictionary<int, byte[]>[peers, channels];
            var got = new List<int>[peers, channels];
            for (int p = 0; p < peers; p++)
            {
                for (int c = 0; c < channels; c++)
                {
                    sent[p, c] = new Dictionary<int, byte[]>();
                    got[p, c] = [];
                    for (int i = 0; i < 2; i++)
                    {
                        harnesses[p].Run(20_000);
                        DrainAll(harnesses[p].Server!, ids[c], got[p, c], sent[p, c]);
                    }
                }
            }

            const int Messages = 12;
            int[,] next = new int[peers, channels];
            for (int frame = 0; frame < 4_000; frame++)
            {
                bool hitch = random.Next(10) == 0;
                bool done = true;
                for (int p = 0; p < peers; p++)
                {
                    SessionHarness h = harnesses[p];
                    for (int c = 0; c < channels; c++)
                    {
                        if (next[p, c] < Messages && random.Next(3) == 0)
                        {
                            // Up to the largest decoded size the shape can stage, compressible to a tenth or less.
                            int raw = random.Next(16, stage - 2_000);
                            byte[] payload = Payload(next[p, c], raw, Math.Max(4, raw / random.Next(10, 200)));
                            if (h.Client.SendCopy(new SendHeader(ids[c]), payload).Status == SendStatus.Admitted)
                            {
                                sent[p, c][next[p, c]++] = payload;
                            }
                        }

                        done &= got[p, c].Count == Messages;
                    }

                    h.Run(1_000);
                    if (!hitch)
                    {
                        for (int c = 0; c < channels; c++)
                        {
                            DrainAll(h.Server!, ids[c], got[p, c], sent[p, c]);
                        }
                    }

                    Assert.True(h.Server!.Core.ReceiveBytesOutstanding <= budget,
                        $"peer {p} has {h.Server.Core.ReceiveBytesOutstanding} of {budget} outstanding at frame {frame}");
                }

                if (done)
                {
                    break;
                }
            }

            for (int p = 0; p < peers; p++)
            {
                for (int c = 0; c < channels; c++)
                {
                    List<int> expected = [.. Enumerable.Range(0, Messages)];
                    List<int> actual = ids[c] == Groups ? [.. got[p, c].Order()] : got[p, c];
                    Assert.True(expected.SequenceEqual(actual), $"peer {p} channel {ids[c]} gave [{string.Join(",", got[p, c])}]");
                }

                Assert.Equal(0, DatagramKit.Statistics(harnesses[p].Server!).DecodeFailures);
            }
        }
        finally
        {
            foreach (SessionHarness h in harnesses)
            {
                h.Dispose();
            }
        }

        SlabStatistics statistics = shared.GetStatistics();
        for (int i = 0; i < statistics.ClassCount; i++)
        {
            Assert.Equal(0, statistics[i].Rented);
        }
    }

    // ------------------------------------------------------------------ allocation

    [Fact]
    public void Decoding_In_Place_Does_Not_Allocate_Through_Drain_Or_A_Handler()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: Table, client: Client, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveBudgetBytes = 1024 * 1024;
        });
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        ReceivedMessage[] buffer = new ReceivedMessage[16];

        // Drained: staged in a 16 KiB block (its decoded size). Handled: staged at its wire length, a 1 536-byte block that
        // holds its 1 400 decoded bytes, so both decode in place.
        byte[] payload = Payload(0, 12_000, 1_000);
        byte[] small = Payload(1, 1_400, 700);
        long handled = 0;
        long drained = 0;
        server.RegisterHandler(Groups, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> data) => handled += data.Length);
        void Tick()
        {
            for (int i = 0; i < 4; i++)
            {
                client.SendCopy(new SendHeader(Packed), payload);
                client.SendCopy(new SendHeader(Groups), small);
            }

            client.Flush();
            network.Advance(2_000);
            client.Poll();
            network.Advance(2_000);
            server.Poll();
            int taken = server.Drain(Packed, buffer);
            for (int i = 0; i < taken; i++)
            {
                drained += buffer[i].Payload.Length;
            }

            server.Release(buffer.AsSpan(0, taken));
        }

        for (int i = 0; i < 600; i++)
        {
            Tick();
        }

        long drainedBefore = drained;
        long handledBefore = handled;
        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 60; i++)
            {
                Tick();
            }
        });
        Assert.True(drained - drainedBefore >= 60L * 12_000, $"{(drained - drainedBefore) / 12_000} messages drained in the measured windows");
        Assert.True(handled - handledBefore >= 60L * 1_400, $"{(handled - handledBefore) / 1_400} messages handled in the measured windows");
        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
        Assert.Equal(0, ClassOf(server.Core.Allocator, 65_536).Peak);    }
}
