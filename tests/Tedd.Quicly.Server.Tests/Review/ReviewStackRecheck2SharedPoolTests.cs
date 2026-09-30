using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server.Tests.Review;

/// <summary>
/// Adversarial review, recheck round 2 of fix/stack-review (1b33437), lens: a1480f8 (RC-1 fix,
/// <c>PeerCore.CanEverRentDecode(length, heldBlock)</c>) with the pool a <see cref="QuiclyServer"/> builds by default. A test
/// marked FINDING fails at 1b33437 for the reason its comment gives.
/// </summary>
public class ReviewStackRecheck2SharedPoolTests
{
    private const ushort Packed = 6;

    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "state", ChannelMode.UnreliableUnordered)
        .Add(4, "chat", ChannelMode.ReliableOrdered)
        .Add(Packed, "packed", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; o.MaxMessageSize = 256 * 1024; })
        .Build();

    /// <summary>200 000 bytes whose first 100 000 LZ4 cannot shrink: about 100 KB compressed (a 256 KiB block), 200 000 raw.</summary>
    private static byte[] Large(int index)
    {
        byte[] payload = new byte[200_000];
        new Random(index + 1).NextBytes(payload.AsSpan(0, 100_000));
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static void DrainAll(QuiclyPeer peer, List<int> into)
    {
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        int taken;
        while ((taken = peer.Drain(Packed, buffer)) > 0)
        {
            for (int i = 0; i < taken; i++)
            {
                into.Add(BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload));
            }

            peer.Release(buffer.AsSpan(0, taken));
        }
    }

    /// <summary>
    /// FINDING (critical: a server at its default pool; regression against d567ba5 and 0.2.1, which dropped and counted).
    /// The server builds its default shared pool (ExpectedPeers 16: the default size classes, twelve blocks of 256 KiB) and
    /// the default 256 KiB budget per peer; one channel compresses and allows 256 KiB messages. Twelve clients each send one
    /// compressed message of 200 000 bytes (about 100 KB compressed) during a host hitch of a few frames, then a small one.
    /// <para>
    /// Each compressed form takes one of the twelve 256 KiB blocks. Each peer's head then needs a 256 KiB block for its
    /// decode: <c>CanEverRentDecode</c> counts 12 - 1 = 11 blocks besides the message's own and says "wait", but every one of
    /// them is held by another peer's head that waits the same way. No peer ever decodes: twelve peers are stalled for good
    /// with their whole receive budget (every stream channel of theirs stops), and the server's 256 KiB class is pinned for
    /// every other peer too (a large message to or from any peer finds no block). The release notes name this very shape as
    /// the remedy for RC-1's known limit ("or a shared allocator, as a server has") and promise that a Drain which finds
    /// everything released gets its buffer "unless a shared pool's blocks ... are all in use by other peers for the moment".
    /// </para>
    /// </summary>
    [Fact]
    public async Task Twelve_Peers_With_A_Large_Compressed_Message_Each_Do_Not_Pin_The_Default_Server_Pool_For_Good()
    {
        const int Peers = 12;
        await using ServerFixture f = new(o =>
        {
            o.Channels = Table;
            o.PeerOptions.AllocatorOptions = null; // the server's default shared pool
        });
        Assert.Equal(12, f.Server.Sizing.SharedPoolBytes > 0 ? CountOf256KiB(f) : -1);
        f.ConfigureClient = o =>
        {
            o.Allocator = null;
            o.AllocatorOptions = new SlabAllocatorOptions
            {
                FreeListShards = 1,
                SizeClasses = [new(64, 1024), new(256, 1024), new(1536, 256), new(4096, 64), new(16384, 16), new(65536, 4), new(262144, 2)],
            };
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
        List<int>[] got = new List<int>[Peers];
        for (int i = 0; i < Peers; i++)
        {
            got[i] = [];
        }

        // The application drains the channel of every peer each frame (so the channels are drained ones).
        for (int frame = 0; frame < 3; frame++)
        {
            f.Run(16_000);
            for (int i = 0; i < Peers; i++)
            {
                DrainAll(servers[i], got[i]);
            }
        }

        for (int i = 0; i < Peers; i++)
        {
            Assert.Equal(SendStatus.Admitted, clients[i].SendCopy(new SendHeader(Packed), Large(i * 10)).Status);
            Assert.Equal(SendStatus.Admitted, clients[i].SendCopy(new SendHeader(Packed), BitConverter.GetBytes((i * 10) + 1)).Status);
        }

        // The hitch: the server polls but its application does not drain until every large message is in.
        Assert.True(f.RunUntil(() => servers.TrueForAll(s => Stats(s).Received >= 1), 2_000_000), "the large messages did not all arrive");

        bool done = false;
        for (int frame = 0; frame < 125 && !done; frame++)
        {
            f.Run(16_000);
            done = true;
            for (int i = 0; i < Peers; i++)
            {
                DrainAll(servers[i], got[i]);
                done &= got[i].Contains((i * 10) + 1);
            }
        }

        long outstanding = 0;
        long failures = 0;
        int stuck = 0;
        for (int i = 0; i < Peers; i++)
        {
            servers[i].GetStatistics(out PeerStatistics statistics);
            outstanding += statistics.ReceiveBytesOutstanding;
            failures += statistics.DecodeFailures;
            stuck += got[i].Contains((i * 10) + 1) ? 0 : 1;
        }

        Assert.True(done,
            $"two seconds of Poll + Drain + Release every 16 ms frame after the hitch: {stuck} of {Peers} peers never got their small message; "
            + $"ReceiveBytesOutstanding over the peers {outstanding}, DecodeFailures {failures}");
    }

    private static ChannelStatistics Stats(QuiclyPeer peer)
    {
        Assert.True(peer.GetChannelStatistics(Packed, out ChannelStatistics statistics));
        return statistics;
    }

    private static int CountOf256KiB(ServerFixture f)
    {
        foreach (SizeClassDefinition definition in SlabAllocatorOptions.CreateDefaultSizeClasses())
        {
            if (definition.BlockSize == 262_144)
            {
                return definition.BlockCount * (int)(f.Server.Sizing.SharedPoolBytes / (16L * 1024 * 1024));
            }
        }

        return 0;
    }
}
