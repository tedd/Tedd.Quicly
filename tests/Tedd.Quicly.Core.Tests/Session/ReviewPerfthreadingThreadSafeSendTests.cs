using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Review (perf-threading lens) of fix/localhost-drops: the expiry bitmap (<c>PeerCore.StampExpiry</c> /
/// <c>ResolveExpiry</c>) and the transport-outcome counters are plain game-thread state. With
/// <see cref="PeerOptions.ThreadSafeSend"/> other threads send — buffered and Immediate, on expiring channels, under a send
/// cap that holds messages back past their expiry — while the game thread polls and flushes. Every foreign send must be
/// admitted on the game thread, so at quiescence every one of them is accounted for exactly once and nothing is left
/// marked, allocated or pending.
/// </summary>
public class ReviewPerfthreadingThreadSafeSendTests
{
    /// <summary>2 unordered, default expiry · 3 keyed sequenced, default expiry · 6 unordered fragmenting, 20 ms · 10 ordered, 50 ms.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
        .Add(3, "keyed", ChannelMode.UnreliableSequenced, o => o.Keyed = true)
        .Add(6, "frag", ChannelMode.UnreliableUnordered, o => { o.Fragmentation = true; o.MaxMessageSize = 4000; o.ExpiryMicros = 20_000; })
        .Add(10, "stream", ChannelMode.ReliableOrdered, o => o.ExpiryMicros = 50_000)
        .Build();

    private static readonly ushort[] Channels = [2, 3, 6, 10];

    [Theory]
    [InlineData(1, 0)]
    [InlineData(2, 400_000)]
    public void Foreign_Sends_On_Expiring_Channels_Are_Accounted_For_Exactly_Once(int seed, int sendCap)
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.ThreadSafeSend = true;
            o.SendTableCapacity = 4096;
            o.MaxSendBytesPerSecond = sendCap;
        }, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 1 << 16;
            o.ReceiveBudgetBytes = 64 * 1024 * 1024;
        }, seed: seed);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int[] received = new int[16];
        foreach (ushort channel in Channels)
        {
            server.RegisterHandler(channel, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> _) => received[header.Channel]++);
        }

        client.Poll();
        const int Threads = 4;
        const int PerThread = 4_000;
        long[] admitted = new long[Threads];
        string? failure = null;
        using Barrier start = new(Threads + 1);
        Thread[] producers = new Thread[Threads];
        for (int t = 0; t < Threads; t++)
        {
            int id = t;
            producers[t] = new Thread(() =>
            {
                Random random = new((seed * 100) + id);
                byte[] small = new byte[64];
                byte[] large = new byte[3000];
                start.SignalAndWait();
                for (int i = 0; i < PerThread; i++)
                {
                    int pick = random.Next(20);
                    ushort channel = pick switch { < 9 => (ushort)2, < 15 => (ushort)3, < 16 => (ushort)6, _ => (ushort)10 };
                    SendOptions options = random.Next(8) == 0 ? SendOptions.Immediate : default;
                    if (random.Next(4) == 0)
                    {
                        options = options with { ExpiryMicros = random.Next(1, 40_000) };
                    }

                    SendResult result;
                    while ((result = client.SendCopy(new SendHeader(channel, channel == 3 ? (ulong)random.Next(1, 9) : 0), channel == 6 ? large : small, options)).Status
                        is SendStatus.QueueFull or SendStatus.OutOfBuffers)
                    {
                        Thread.Yield();
                    }

                    if (result.Status != SendStatus.Admitted)
                    {
                        failure ??= $"thread {id}: {result.Status}";
                        return;
                    }

                    admitted[id]++;
                }
            })
            {
                IsBackground = true,
            };
            producers[t].Start();
        }

        start.SignalAndWait();
        Assert.True(h.RunUntil(() => producers.All(p => !p.IsAlive) || failure is not null, 600_000_000), "the producers did not finish");
        foreach (Thread producer in producers)
        {
            Assert.True(producer.Join(TimeSpan.FromSeconds(30)));
        }

        Assert.Null(failure);

        // Settle: the front is drained, everything queued is sent or expires, every completion comes back.
        h.Run(3_000_000);
        PeerStatistics stats = DatagramKit.Statistics(client);
        Assert.Equal(admitted.Sum(), stats.ThreadSafeSends);
        Assert.Equal(0, stats.SendEntriesInUse);
        Assert.Equal(0, stats.SendBytesOutstanding);
        Assert.False(client.Core.HasPendingExpiry, "an expiry still waits for a scheduler pass on a settled peer");
        Assert.False(client.HasPendingWork, "a settled sender still reports work");

        // Every foreign send was admitted by the game thread and then either handed to the transport or expired — or it was
        // refused at admission and counted. A fragmented message is handed over as several datagrams, so channel 6 is
        // left out of the per-message sum and checked through its own counters.
        long accounted = stats.ThreadSafeSendDrops;
        foreach (ushort channel in Channels)
        {
            ChannelStatistics c = DatagramKit.ChannelStats(client, channel);
            Assert.Equal(0, c.QueuedMessages);
            Assert.True(c.TransportCanceled + c.TransportLost <= c.Sent, $"channel {channel}: sent {c.Sent}, canceled {c.TransportCanceled}, lost {c.TransportLost}");
            if (channel != 6)
            {
                accounted += c.Sent + c.Expired;
                ChannelStatistics s = DatagramKit.ChannelStats(server, channel);
                Assert.True(c.Sent - c.TransportCanceled - c.TransportLost == s.Received + s.Dropped, $"channel {channel}: sent {c.Sent}, expired {c.Expired}, canceled {c.TransportCanceled}, lost {c.TransportLost}; receiver got {s.Received}, dropped {s.Dropped}, ring drops {s.RingDrops}, out of buffers {s.OutOfBuffers}; client {client.State}, server {server.State}, stream resets {DatagramKit.Statistics(server).StreamsReset}");
                Assert.True(s.Received == received[channel], $"channel {channel}: received {s.Received}, handled {received[channel]} [{string.Join(",", received)}]; server {server.State} fault {server.LastCallbackFault} client {client.State} fault {client.LastCallbackFault}; ring high {DatagramKit.Statistics(server).ReceiveRingHighWater}; pending {server.HasPendingWork}");
            }
        }

        long fragmented = stats.FragmentedMessagesSent;
        Assert.True(accounted + fragmented == stats.ThreadSafeSends, $"{accounted} sent or expired + {fragmented} fragmented of {stats.ThreadSafeSends} foreign sends");
        Assert.Equal(stats.DatagramsSent, stats.DatagramsAcknowledged + stats.DatagramsLost + stats.DatagramsCanceled);
        if (sendCap != 0)
        {
            Assert.True(DatagramKit.ChannelStats(client, 2).Expired > 0, "the send cap never held a message back past its expiry");
        }
    }
}
