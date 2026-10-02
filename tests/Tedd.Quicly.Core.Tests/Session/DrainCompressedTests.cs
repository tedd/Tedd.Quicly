using System.Buffers.Binary;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// <see cref="QuiclyPeer.Drain"/> on a reliable channel that compresses. Every message it hands out is decoded into a
/// second buffer of its raw size, and the caller can release nothing before the call returns, so one call can need more
/// decode buffers than the receive budget has. A reliable message must not be dropped for that: it waits for the next
/// Drain.
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
        // messages wait in small blocks; decoded, four of them are the whole budget.
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

        // Far more of them wait than the budget can hold decoded (all forty, or the channel's share where a channel nobody
        // reads has one).
        Assert.True(DatagramKit.ChannelStats(server, Packed).Received >= 16);

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
        // A receive budget of 64 KiB: the 64 KiB block a payload of 60 000 bytes decodes into never fits next to the block
        // the message arrived in. Waiting for it would stop the channel for good, so it is dropped and counted, as before,
        // and the message behind it arrives.
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
        Assert.Equal(2, DatagramKit.ChannelStats(server, Packed).Received);

        List<int> got = DrainAll(server, Packed, 16);
        Assert.Equal([1], got);
        Assert.Equal(1, DatagramKit.Statistics(server).DecodeFailures);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }
}
