using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Review (perf-threading lens) of fix/localhost-drops: zero-allocation checks (ADR 0008) of paths the branch added or
/// changed that its own zero-allocation tests do not reach — a Drain that queues the messages of a channel
/// <em>with</em> a handler next to the backlog of a channel nobody drains (which evicts, where the handled channel never
/// does), the Poll that then dispatches from the queues, datagrams the transport declares lost (counted
/// per channel and per peer, and retransmitted on ReliableLatest), the statistics snapshots with the new fields, and
/// Release on a disposed peer over a shared pool.
/// </summary>
public class ReviewPerfthreadingZeroAllocationTests
{
    /// <summary>2, 3 unordered · 6 unordered fragmenting · 8 keyed sequenced 16-bit · 12 ReliableLatest. Nothing expires.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(3, "unread", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(6, "frag", ChannelMode.UnreliableUnordered, o => { o.Fragmentation = true; o.MaxMessageSize = 4000; o.ExpiryMicros = 0; })
        .Add(8, "keyed", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 16; o.ExpiryMicros = 0; })
        .Add(12, "state", ChannelMode.ReliableLatest, o => o.MaxKeys = 16)
        .Build();

    [Fact]
    public void Guard_Draining_Past_A_Handled_Channel_Under_Loss_Does_Not_Allocate()
    {
        using SessionHarness h = new(
            link: new LinkOptions { LossPercent = 20 },
            table: Table,
            client: DatagramKit.Quiet,
            server: o =>
            {
                DatagramKit.Quiet(o);
                o.ReceiveRingCapacity = 64;
            });
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long handled = 0;
        long latest = 0;
        server.RegisterHandler(8, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => handled++);
        server.RegisterHandler(12, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => latest++);
        ReceivedMessage[] buffer = new ReceivedMessage[4];
        byte[] small = new byte[24];
        byte[] large = new byte[2500];
        uint tick = 0;
        long drained = 0;
        void Tick()
        {
            tick++;
            for (int i = 0; i < 6; i++)
            {
                client.SendCopy(new SendHeader(8, (ulong)(i & 3)), small);
                client.SendCopy(new SendHeader(2), small);
                client.Flush(tick);
            }

            client.SendCopy(new SendHeader(12, tick & 7), small);
            client.SendCopy(new SendHeader(3), small);
            client.SendCopy(new SendHeader(6), large);
            client.Flush(tick);
            network.Advance(2_000);
            client.Poll();

            // The Drain of channel 2 meets the messages of channel 8, which has a handler: they are queued for the next Poll,
            // and only every eighth tick polls. Channel 3 is never drained: what is queued for it is backlog, which evicts
            // its oldest as the pool fills; nothing of the handled channel is ever evicted.
            int n = server.Drain(2, buffer);
            server.Release(buffer.AsSpan(0, n));
            drained += n;
            n = server.Drain(6, buffer);
            server.Release(buffer.AsSpan(0, n));
            if ((tick & 7) == 0)
            {
                server.Poll();
                server.Flush();
            }

            server.GetStatistics(out PeerStatistics peer);
            server.GetChannelStatistics(8, out ChannelStatistics channel);
            client.GetChannelStatistics(12, out channel);
            client.GetStatistics(out peer);
        }

        for (int i = 0; i < 1_500; i++)
        {
            Tick();
        }

        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 400; i++)
            {
                Tick();
            }
        });

        // The paths the test is about were taken.
        PeerStatistics serverStats = DatagramKit.Statistics(server);
        PeerStatistics clientStats = DatagramKit.Statistics(client);
        Assert.True(handled > 1_000, $"{handled} handled");
        Assert.True(latest > 100, $"{latest} latest values");
        Assert.True(drained > 1_000, $"{drained} drained");
        Assert.Equal(0, DatagramKit.ChannelStats(server, 8).DrainQueueDrops);
        Assert.True(DatagramKit.ChannelStats(server, 3).DrainQueueDrops > 1_000, $"{DatagramKit.ChannelStats(server, 3).DrainQueueDrops} evictions of the channel nobody drains");
        Assert.True(clientStats.DatagramsLost > 1_000, $"{clientStats.DatagramsLost} datagrams lost");
        Assert.True(DatagramKit.ChannelStats(client, 8).TransportLost > 500, "no loss counted on channel 8");
        Assert.True(DatagramKit.ChannelStats(client, 6).TransportLost > 100, "no loss counted on the fragmenting channel");
        Assert.True(DatagramKit.ChannelStats(client, 12).TransportLost > 10, "no loss counted on the ReliableLatest channel");
        Assert.Equal(0, serverStats.ReceiveRingDrops);
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void Guard_Release_After_Dispose_Over_A_Shared_Pool_Does_Not_Allocate()
    {
        using SlabAllocator shared = new(PeerCore.CreateCompactAllocatorOptions());
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: o =>
        {
            DatagramKit.Quiet(o);
            o.Allocator = shared;
        });
        QuiclyPeer server = h.Server!;
        const int Count = 96;
        byte[] payload = new byte[24];
        for (int i = 0; i < Count; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), payload).Status);
        }

        Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(server, 2).Received == Count), "the messages never arrived");
        ReceivedMessage[] drained = new ReceivedMessage[Count];
        Assert.Equal(Count, server.Drain(2, drained));
        h.DisposeServer();
        h.Network.RunUntilIdle(1_000_000);
        Assert.True(server.IsFreed);

        // Three batches; an allocation per release shows in every one of them, the test host's own one-off in at most one.
        long least = long.MaxValue;
        for (int batch = 0; batch < 3; batch++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            server.Release(drained.AsSpan(batch * (Count / 3), Count / 3));
            least = Math.Min(least, GC.GetAllocatedBytesForCurrentThread() - before);
        }

        Assert.Equal(0, least);
        Assert.Equal(0, shared.GetStatistics().TotalRentedBytes);
    }
}
