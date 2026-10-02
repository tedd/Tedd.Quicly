using System.Buffers.Binary;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// <see cref="QuiclyPeer.Drain"/> on a reliable channel that compresses. The caller can release nothing before the call
/// returns, so a reliable message must not be dropped for want of a decode buffer. Since RC2-1 a compressed message of a
/// channel no handler reads is staged in a block that holds its decoded size and is decoded in place, so Drain needs no
/// second buffer for it at all; the strict share of a channel nobody reads counts that block.
/// </summary>
public class DrainCompressedTests
{
    /// <summary><see cref="OrderedTables.Main"/>: ReliableOrdered with LZ4.</summary>
    private const ushort Packed = 6;

    private static byte[] Payload(int index, int size)
    {
        // Zeroes behind the index: LZ4 packs 20 000 of them into a few dozen bytes.
        byte[] payload = new byte[size];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static List<int> DrainAll(QuiclyPeer peer, ushort channel, int batch)
    {
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[batch];
        int taken;
        while ((taken = peer.Drain(channel, buffer)) > 0)
        {
            for (int i = 0; i < taken; i++)
            {
                got.Add(BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload));
            }

            peer.Release(buffer.AsSpan(0, taken));
        }

        return got;
    }

    [Fact]
    public void Drain_Keeps_A_Compressed_Message_Of_A_Reliable_Channel_It_Has_No_Buffer_To_Decode()
    {
        // Default options: a receive budget of 256 KiB, and a 64 KiB block for a payload of 20 000 bytes. Forty compressed
        // messages that LZ4 packs into a few dozen bytes each; decoded, four of them are the whole budget.
        using SessionHarness h = new(table: OrderedTables.Main, client: GroupKit.Prompt, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = 4096;
        });
        QuiclyPeer server = h.Server!;
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), Payload(i, 20_000)).Status);
            h.Run(2_000);
        }

        h.Run(50_000);

        // Staged at their decoded size, none of them fits the share of a channel nobody reads: they wait in the transport
        // (before RC2-1 far more of them waited, in their compressed blocks, than the budget could hold decoded).
        Assert.Equal(0, DatagramKit.ChannelStats(server, Packed).Received);
        Assert.True(DatagramKit.ChannelStats(server, Packed).BacklogHolds > 0);

        // The application drains sixteen at a time and releases each batch before it drains again.
        List<int> got = [];
        h.RunUntil(() =>
        {
            got.AddRange(DrainAll(server, Packed, 16));
            return got.Count == 40;
        }, 2_000_000);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(got.SequenceEqual(Enumerable.Range(0, 40)) && statistics.DecodeFailures == 0,
            $"{got.Count} of 40 messages came out of Drain ({string.Join(",", got.Take(12))}...); {statistics.DecodeFailures} were dropped as DecodeFailures");
        Assert.Equal(0, statistics.ReceiveBytesOutstanding);
    }

    [Fact]
    public void Drain_Drops_A_Compressed_Message_That_Can_Never_Be_Decoded_And_Goes_On()
    {
        // A receive budget of 32 KiB: the 64 KiB block a payload of 60 000 bytes decodes into is larger than the whole
        // budget, so it cannot be staged at its decoded size (the edge band) and is staged at its wire length. A decode
        // buffer may take the budget past its limit, by itself, but not one larger than the budget: it is dropped and
        // counted, as in 0.2.1, and the message behind it arrives.
        using SessionHarness h = new(table: OrderedTables.Main, client: GroupKit.Prompt, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveBudgetBytes = 32 * 1024;
        });
        QuiclyPeer server = h.Server!;
        Assert.Equal(16_384, server.Core.MaxStageLength);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), Payload(0, 60_000)).Status);
        h.Run(20_000);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), Payload(1, 2_000)).Status);
        h.Run(50_000);
        Assert.Equal(1, DatagramKit.ChannelStats(server, Packed).Received);

        // The second (a 4 KiB block at its decoded size) does not fit the share of a channel nobody reads: it follows the
        // first Drain.
        List<int> got = [];
        Assert.True(h.RunUntil(() =>
        {
            got.AddRange(DrainAll(server, Packed, 16));
            return got.Count == 1;
        }, 1_000_000));
        Assert.Equal([1], got);
        Assert.Equal(1, DatagramKit.Statistics(server).DecodeFailures);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void Drain_Decodes_A_Message_Whose_Decoded_Block_Is_The_Whole_Budget()
    {
        // A receive budget of 64 KiB: a payload of 60 000 bytes is staged in a 64 KiB block — the whole budget — and decoded
        // in place in it; the message behind it waits, holding nothing, until the application released the first.
        using SessionHarness h = new(table: OrderedTables.Main, client: GroupKit.Prompt, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveBudgetBytes = 64 * 1024;
        });
        QuiclyPeer server = h.Server!;
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), Payload(0, 60_000)).Status);
        h.Run(20_000);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), Payload(1, 2_000)).Status);
        h.Run(50_000);

        List<int> got = [];
        h.RunUntil(() =>
        {
            got.AddRange(DrainAll(server, Packed, 16));
            return got.Count == 2;
        }, 1_000_000);
        Assert.Equal([0, 1], got);
        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }
}
