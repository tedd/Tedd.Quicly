using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The peer's steady state must not allocate on the GC heap (ADR 0008). The simulator raises every transport callback
/// on the test thread, so <see cref="GC.GetAllocatedBytesForCurrentThread"/> covers the transport-thread paths too.
/// </summary>
public class SessionZeroAllocationTests
{
    [Fact]
    public void Idle_Poll_And_Flush_Do_Not_Allocate()
    {
        using SessionHarness h = new();
        h.Run(3_500_000, 10_000);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        AllocationAssert.NoAllocations(() =>
        {
            client.Poll();
            client.Flush();
            server.Poll();
            server.Flush();
        });
    }

    [Fact]
    public void Ping_Pong_Traffic_Does_Not_Allocate()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 },
            client: o => { o.PingInterval = TimeSpan.FromMilliseconds(20); o.PongsPerSecond = 100; },
            server: o => { o.PingInterval = TimeSpan.FromMilliseconds(20); o.PongsPerSecond = 100; });
        h.Run(4_000_000, 1_000);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        client.GetStatistics(out PeerStatistics before);
        AllocationAssert.NoAllocations(() =>
        {
            network.Advance(1_000);
            client.Poll();
            client.Flush();
            server.Poll();
            server.Flush();
        });
        client.GetStatistics(out PeerStatistics after);
        Assert.True(after.PingsSent - before.PingsSent > 500, $"{after.PingsSent - before.PingsSent} pings");
        Assert.True(after.RttSamples - before.RttSamples > 500);
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void Datagram_Send_And_Handler_Dispatch_Do_Not_Allocate()
    {
        using SessionHarness h = TestEngines.Create(out _, out _);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int received = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        byte[] payload = new byte[64];
        AllocationAssert.NoAllocations(() =>
        {
            client.SendCopy(new SendHeader(2), payload);
            network.Advance(1_000);
            client.Poll();
            client.Flush();
            server.Poll();
            server.Flush();
        });
        Assert.True(received >= 11_000, $"{received} messages");
    }

    [Fact]
    public void Drain_And_Release_Do_Not_Allocate()
    {
        using SessionHarness h = TestEngines.Create(out _, out _);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        ReceivedMessage[] buffer = new ReceivedMessage[8];
        byte[] payload = new byte[32];
        int drained = 0;
        AllocationAssert.NoAllocations(() =>
        {
            client.SendCopy(new SendHeader(3), payload);
            network.Advance(1_000);
            client.Poll();
            server.Poll();
            int n = server.Drain(3, buffer);
            server.Release(buffer.AsSpan(0, n));
            drained += n;
        });
        Assert.True(drained >= 11_000, $"{drained} messages");
    }

    [Fact]
    public void Evicting_From_An_Undrained_Channel_Does_Not_Allocate()
    {
        // Channel 2 has no handler and is never drained: once its backlog is at the limit (it is, long before the warm-up
        // ends), every Poll evicts the oldest queued message for the new one. Channel 3 is under its share and drained every
        // round, so its append takes the victim-scan path instead of the appender's own.
        using SessionHarness h = TestEngines.Create(out _, out _, both: o => o.ReceiveRingCapacity = 64);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        ReceivedMessage[] buffer = new ReceivedMessage[8];
        byte[] payload = new byte[32];
        int drained = 0;
        AllocationAssert.NoAllocations(() =>
        {
            client.SendCopy(new SendHeader(2), payload);
            client.SendCopy(new SendHeader(2), payload);
            client.SendCopy(new SendHeader(3), payload);
            network.Advance(1_000);
            client.Poll();
            server.Poll();
            int n = server.Drain(3, buffer);
            server.Release(buffer.AsSpan(0, n));
            drained += n;
        });
        Assert.True(drained >= 11_000, $"{drained} messages");
        server.GetStatistics(out PeerStatistics statistics);
        Assert.True(statistics.DrainQueueDrops >= 20_000, $"{statistics.DrainQueueDrops} evictions");
        Assert.Equal(0, statistics.ReceiveRingDrops);
        Assert.True(server.GetChannelStatistics(3, out ChannelStatistics drainedChannel));
        Assert.Equal(0, drainedChannel.DrainQueueDrops);
    }
}
