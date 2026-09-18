using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Receive-lease recycling (<see cref="PeerCore.RecycleReceive"/> → <see cref="PeerCore.TryRentReceive"/>): a dispatched
/// message's lease is parked for the transport thread to reissue to the next message. A parked lease counts as released for
/// the application (statistics) and as held for the pool and the budget; a reuse must be exactly a return followed by a rent.
/// </summary>
public class RecycleReceiveTests
{
    private static readonly ChannelTable Table = DatagramTables.Main;

    private static SlabAllocatorOptions Pool() => new()
    {
        FreeListShards = 1,
        ValidateLeases = true,
        SizeClasses = [new(64, 256), new(256, 64), new(1536, 32), new(4096, 8)],
    };

    private static SessionHarness Harness(Action<PeerOptions>? server = null) => new(
        table: Table,
        client: DatagramKit.Quiet,
        server: o =>
        {
            DatagramKit.Quiet(o);
            o.AllocatorOptions = Pool();
            server?.Invoke(o);
        });

    private static void SendAndDeliver(SessionHarness h, ref int next, int count, int length, Func<int> received, ushort channel = 2)
    {
        int target = received() + count;
        for (int i = 0; i < count; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(channel), DatagramKit.Payload(next++, length)).IsAdmitted);
        }

        Assert.True(h.RunUntil(() => received() >= target, 1_000_000), $"{received()} of {target} messages arrived");
    }

    [Fact]
    public void Dispatched_Leases_Are_Reissued_To_The_Next_Messages_And_Count_As_Released()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        int received = 0;
        int corrupt = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            int id = BitConverter.ToInt32(payload);
            corrupt += payload.SequenceEqual(DatagramKit.Payload(id, 64)) ? 0 : 1;
            received++;
        });
        int next = 0;
        SendAndDeliver(h, ref next, 20, 64, () => received);
        PeerCore core = server.Core;
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
        Assert.Equal(20 * 64, core.RecycledBytes);
        SizeClassStatistics first = core.Allocator.GetClassStatistics(0);
        Assert.Equal(20, first.Rented); // parked blocks stay rented from the pool

        for (int round = 0; round < 10; round++)
        {
            SendAndDeliver(h, ref next, 20, 64, () => received);
            Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
            Assert.Equal(20 * 64, core.RecycledBytes);
        }

        // Every later message reused a parked block: the class never rented more than the first batch did.
        SizeClassStatistics after = core.Allocator.GetClassStatistics(0);
        Assert.Equal(20, after.Rented);
        Assert.Equal(first.Peak, after.Peak);
        Assert.Equal(0, corrupt);
        Assert.Equal(220, received);
        Assert.Equal(0, DatagramKit.Statistics(server).OutOfReceiveBuffers);
    }

    [Fact]
    public void A_Retained_Lease_Is_Held_Until_Released_Late_From_Another_Thread()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<ReceiveLease> retained = [];
        int received = 0;
        server.RegisterHandler(2, (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> _) =>
        {
            if (received++ % 10 == 0)
            {
                retained.Add(peer.Retain(in header));
            }
        });
        int next = 0;
        SendAndDeliver(h, ref next, 20, 64, () => received);
        Assert.Equal(2, retained.Count);
        Assert.Equal(2 * 64, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
        Assert.Equal(18 * 64, server.Core.RecycledBytes);

        // More traffic while the two are held: they stay counted, and their blocks are never handed to a new message.
        SendAndDeliver(h, ref next, 40, 64, () => received);
        Assert.Equal(6, retained.Count);
        Assert.Equal(6 * 64, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
        for (int i = 0; i < retained.Count; i++)
        {
            int id = BitConverter.ToInt32(retained[i].Payload);
            Assert.True(retained[i].Payload.SequenceEqual(DatagramKit.Payload(id, 64)), $"retained message {id} was overwritten");
        }

        ReceiveLease[] copy = retained.ToArray();
        Task.Run(() =>
        {
            foreach (ReceiveLease lease in copy)
            {
                server.Release(in lease);
            }
        }).Wait();
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
        long parked = server.Core.RecycledBytes;
        Assert.Equal(parked / 64, server.Core.Allocator.GetClassStatistics(0).Rented);
    }

    [Fact]
    public void A_Parked_Lease_Of_Another_Size_Class_Goes_Back_To_The_Pool()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        int received = 0;
        int corrupt = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            int id = BitConverter.ToInt32(payload);
            corrupt += payload.SequenceEqual(DatagramKit.Payload(id, payload.Length)) ? 0 : 1;
            received++;
        });
        int next = 0;
        SendAndDeliver(h, ref next, 20, 64, () => received);
        SlabAllocator pool = server.Core.Allocator;
        Assert.Equal(20, pool.GetClassStatistics(0).Rented);

        // 200-byte messages need the 256-byte class: each rent takes one parked 64-byte block and returns it.
        SendAndDeliver(h, ref next, 20, 200, () => received);
        Assert.Equal(0, pool.GetClassStatistics(0).Rented);
        Assert.Equal(20, pool.GetClassStatistics(1).Rented);
        Assert.Equal(20 * 256, server.Core.RecycledBytes);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);

        // And back: the 64-byte messages rent fresh blocks while the parked 256-byte ones go back.
        SendAndDeliver(h, ref next, 20, 64, () => received);
        Assert.Equal(0, pool.GetClassStatistics(1).Rented);
        Assert.Equal(20, pool.GetClassStatistics(0).Rented);
        Assert.Equal(0, corrupt);
        Assert.Equal(0, DatagramKit.Statistics(server).OutOfReceiveBuffers);
    }

    [Fact]
    public void Parked_Leases_Never_Make_A_Rent_Fail_That_Would_Fit_The_Budget_Without_Them()
    {
        // Budget 2 KiB: up to 256 bytes park, in blocks of at most 64.
        using SessionHarness h = Harness(o => o.ReceiveBudgetBytes = 2048);
        QuiclyPeer server = h.Server!;
        List<ReceiveLease> retained = [];
        int received = 0;
        server.RegisterHandler(2, (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            received++;
            if (payload.Length > 64)
            {
                retained.Add(peer.Retain(in header));
            }
        });
        int next = 0;
        SendAndDeliver(h, ref next, 7, 200, () => received); // 7 × 256 held
        SendAndDeliver(h, ref next, 4, 64, () => received);  // 4 × 64 rented, dispatched and parked: the budget is full
        Assert.Equal(7 * 256, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
        Assert.Equal(4 * 64, server.Core.RecycledBytes);

        // 1792 held + 256 = the budget exactly: it fits without the parked blocks, so it must fit with them.
        SendAndDeliver(h, ref next, 1, 200, () => received);
        Assert.Equal(0, DatagramKit.Statistics(server).OutOfReceiveBuffers);
        Assert.Equal(8 * 256, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
        Assert.Equal(0, server.Core.RecycledBytes);

        // One more does not fit, with or without recycling.
        Assert.True(h.Client.SendCopy(new SendHeader(2), DatagramKit.Payload(next++, 200)).IsAdmitted);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(server).OutOfReceiveBuffers == 1, 1_000_000));
        Assert.Equal(12, received);

        foreach (ReceiveLease lease in retained)
        {
            server.Release(in lease);
        }

        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void Closing_And_Disposing_With_Parked_Leases_Returns_Every_Block_To_A_Shared_Pool()
    {
        using SlabAllocator pool = new(Pool());
        int received = 0;
        using (SessionHarness h = Harness(o =>
        {
            o.AllocatorOptions = null;
            o.Allocator = pool;
        }))
        {
            QuiclyPeer server = h.Server!;
            MessageHandler count = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++;
            server.RegisterHandler(2, count);
            server.RegisterHandler(4, count);
            server.RegisterHandler(9, count);
            int next = 0;
            SendAndDeliver(h, ref next, 30, 64, () => received);
            SendAndDeliver(h, ref next, 10, 200, () => received);

            // Coalescing channels: the transport thread replaces a key's lease, the game thread takes the latest.
            for (int round = 0; round < 5; round++)
            {
                for (ulong key = 0; key < 8; key++)
                {
                    h.Client.SendCopy(new SendHeader(4, key), DatagramKit.Payload(next++, 48));
                    h.Client.SendCopy(new SendHeader(9, key), DatagramKit.Payload(next++, 100));
                }

                h.Run(20_000);
            }

            Assert.True(server.Core.RecycledBytes > 0);
            Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
            Assert.True(pool.GetStatistics().TotalRentedBytes > 0);
        }

        // The harness disposed both peers and delivered the transports' close: the parked blocks went back with the rest.
        Assert.Equal(0, pool.GetStatistics().TotalRentedBytes);
        Assert.True(received >= 40);
    }

    [Fact]
    public void Disposing_From_A_Handler_Returns_The_Parked_Leases()
    {
        using SlabAllocator pool = new(Pool());
        using (SessionHarness h = Harness(o =>
        {
            o.AllocatorOptions = null;
            o.Allocator = pool;
        }))
        {
            QuiclyPeer server = h.Server!;
            int received = 0;
            server.RegisterHandler(2, (QuiclyPeer peer, in ReceiveHeader _, ReadOnlySpan<byte> _) =>
            {
                if (++received == 25)
                {
                    peer.Dispose();
                }
            });
            int next = 0;
            SendAndDeliver(h, ref next, 20, 64, () => received);
            for (int i = 0; i < 20; i++)
            {
                h.Client.SendCopy(new SendHeader(2), DatagramKit.Payload(next++, 64));
            }

            h.StopPumpingServer();
            h.Client.Flush();
            h.Network.Advance(1_000);
            server.Poll();
            Assert.True(server.IsDisposed);
        }

        Assert.Equal(0, pool.GetStatistics().TotalRentedBytes);
    }
}
