using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server.Tests.Review;

/// <summary>
/// Adversarial review, RC2 round 1 of fix/stack-review (8f6c47c), lens: the decoded-size staging of RC2-1 with the pool a
/// <see cref="QuiclyServer"/> builds by default (twelve 256 KiB blocks, 32 of 64 KiB, at ExpectedPeers ≤ 128) and a
/// channel at its default MaxMessageSize (64 KiB). A test marked FINDING fails at 8f6c47c for the reason its comment gives.
/// </summary>
public class ReviewRc2R1ServerPoolTests
{
    private const ushort Packed = 6;

    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "state", ChannelMode.UnreliableUnordered)
        .Add(Packed, "packed", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; })
        .Build();

    /// <summary>65 536 bytes (the channel's default maximum) whose first 1 000 LZ4 cannot shrink: about 1.3 KB compressed.</summary>
    private static byte[] Chunk(int index)
    {
        byte[] payload = new byte[64 * 1024];
        new Random(index + 1).NextBytes(payload.AsSpan(0, 1_000));
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static int DrainAll(QuiclyPeer peer)
    {
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        int total = 0;
        int taken;
        while ((taken = peer.Drain(Packed, buffer)) > 0)
        {
            total += taken;
            peer.Release(buffer.AsSpan(0, taken));
        }

        return total;
    }

    private static ChannelStatistics Stats(QuiclyPeer peer)
    {
        Assert.True(peer.GetChannelStatistics(Packed, out ChannelStatistics statistics));
        return statistics;
    }

    /// <summary>
    /// FINDING (major, regression against 6d41167 and 0.2.1; same root cause as the Core test
    /// ReviewRc2R1StagingTests.FINDING_A_Compressed_Message_Of_The_Default_Maximum_Size_Pins_The_Whole_Default_Receive_Budget).
    /// Sixteen clients each send one compressed message of the default maximum size (65 536 bytes, about 1.3 KB on the
    /// wire) during a server host hitch (the server polls, its application does not drain). Each is staged at its decoded
    /// size plus the in-place margin, which does not fit 64 KiB, so each takes one of the server's twelve 256 KiB blocks
    /// (and its peer's whole 256 KiB budget): four peers cannot even stage theirs until other peers' applications release
    /// theirs — peers now wait for each other on a message type that took a 1 536-byte block each at 6d41167.
    /// </summary>
    [Fact]
    public async Task FINDING_Sixteen_Peers_Each_Receiving_One_Default_Size_Compressed_Message_Do_Not_Wait_For_Each_Other()
    {
        const int Peers = 16;
        await using ServerFixture f = new(o =>
        {
            o.Channels = Table;
            o.PeerOptions.AllocatorOptions = null; // the server's default shared pool
        });
        f.ConfigureClient = o =>
        {
            o.Allocator = null;
            o.SendBudgetBytes = 1024 * 1024;
            o.SendTableCapacity = 256;
        };

        List<QuiclyPeer> clients = [];
        for (int i = 0; i < Peers; i++)
        {
            QuiclyPeer client = f.Connect(table: Table);
            Assert.True(f.RunUntil(() => client.State == PeerState.Connected), "client " + i + " was not admitted: " + client.State);
            clients.Add(client);
        }

        List<QuiclyPeer> servers = clients.ConvertAll(f.ServerPeerOf);
        for (int frame = 0; frame < 3; frame++)
        {
            f.Run(16_000);
            foreach (QuiclyPeer server in servers)
            {
                DrainAll(server);
            }
        }

        for (int i = 0; i < Peers; i++)
        {
            Assert.Equal(SendStatus.Admitted, clients[i].SendCopy(new SendHeader(Packed), Chunk(i)).Status);
        }

        // The hitch: a quarter of a second of Polls without a Drain.
        bool staged = f.RunUntil(() => servers.TrueForAll(s => Stats(s).Received >= 1), 250_000);
        int stagedPeers = servers.Count(s => Stats(s).Received >= 1);
        long outstanding = 0;
        foreach (QuiclyPeer server in servers)
        {
            server.GetStatistics(out PeerStatistics statistics);
            outstanding += statistics.ReceiveBytesOutstanding;
        }

        // Nothing is lost: once the applications drain, every message arrives.
        int[] got = new int[Peers];
        Assert.True(f.RunUntil(() =>
        {
            bool all = true;
            for (int i = 0; i < Peers; i++)
            {
                got[i] += DrainAll(servers[i]);
                all &= got[i] == 1;
            }

            return all;
        }, 2_000_000), "not every message arrived after the hitch");

        Assert.True(staged,
            $"{stagedPeers} of {Peers} peers could stage their one 64 KiB compressed message during the hitch; "
            + $"{outstanding} bytes of receive budget held over the peers for {stagedPeers} messages of about 1.3 KB on the wire");
    }
}
