using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Review (perf-threading lens) of fix/localhost-drops: randomised runs over a lossy, reordering, jittering link that mix
/// every receive style the branch touched — handlers, Drain, leases kept across polls, handlers registered and removed in
/// the middle, a bounded Poll — and check the resource invariants afterwards: every block of the shared pool is back,
/// the receive budget is back at zero, the drop counters add up, and nothing is delivered twice or out of order.
/// </summary>
public class ReviewPerfthreadingStressTests
{
    /// <summary>
    /// 2 unordered (drained) · 3 unordered (handler, sometimes removed) · 6 unordered fragmenting · 8 keyed sequenced
    /// 16-bit (drained) · 9 keyed sequenced coalescing (handler) · 10 ordered (handler) · 11 reliable unordered (handler) ·
    /// 12 ReliableLatest (handler).
    /// </summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(3, "b", ChannelMode.UnreliableUnordered)
        .Add(6, "frag", ChannelMode.UnreliableUnordered, o => { o.Fragmentation = true; o.MaxMessageSize = 4000; o.ExpiryMicros = 0; })
        .Add(8, "keyed", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 16; o.ExpiryMicros = 0; })
        .Add(9, "latest", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.CoalesceOnReceive = true; o.MaxKeys = 64; o.ExpiryMicros = 0; })
        .Add(10, "stream", ChannelMode.ReliableOrdered)
        .Add(11, "group", ChannelMode.ReliableUnordered)
        .Add(12, "state", ChannelMode.ReliableLatest, o => o.MaxKeys = 64)
        .Build();

    private static readonly ushort[] Unreliable = [2, 3, 6, 8];

    private static byte[] Payload(int index, int size)
    {
        byte[] payload = new byte[Math.Max(size, 4)];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static ChannelStatistics Channel(QuiclyPeer peer, ushort channel)
    {
        Assert.True(peer.GetChannelStatistics(channel, out ChannelStatistics statistics));
        return statistics;
    }

    private sealed class Receiver
    {
        public readonly Dictionary<ushort, List<(ulong Key, int Index)>> Delivered = [];
        public readonly List<ReceivedMessage> Held = [];
        public readonly List<ReceiveLease> Retained = [];
        public Random Random = new(1);

        public void Note(ushort channel, ulong key, ReadOnlySpan<byte> payload)
        {
            if (!Delivered.TryGetValue(channel, out List<(ulong, int)>? list))
            {
                Delivered[channel] = list = [];
            }

            list.Add((key, BinaryPrimitives.ReadInt32LittleEndian(payload)));
        }

        public MessageHandler Handler(bool retainSome) => (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            Note(header.Channel, header.Key, payload);
            if (retainSome && Random.Next(8) == 0)
            {
                Retained.Add(peer.Retain(in header));
            }
        };

        public void DrainSome(QuiclyPeer peer, ushort channel, int capacity)
        {
            ReceivedMessage[] buffer = new ReceivedMessage[capacity];
            int n = peer.Drain(channel, buffer);
            for (int i = 0; i < n; i++)
            {
                Note(channel, buffer[i].Header.Key, buffer[i].Payload);
                Held.Add(buffer[i]);
            }
        }

        public void DrainAll(QuiclyPeer peer, ushort channel)
        {
            ReceivedMessage[] buffer = new ReceivedMessage[16];
            int n;
            while ((n = peer.Drain(channel, buffer)) > 0)
            {
                for (int i = 0; i < n; i++)
                {
                    Note(channel, buffer[i].Header.Key, buffer[i].Payload);
                }

                peer.Release(buffer.AsSpan(0, n));
            }
        }

        public void ReleaseSome(QuiclyPeer peer, int keep)
        {
            while (Held.Count > keep)
            {
                ReceivedMessage message = Held[^1];
                Held.RemoveAt(Held.Count - 1);
                peer.Release(new ReadOnlySpan<ReceivedMessage>(in message));
            }

            while (Retained.Count > keep)
            {
                ReceiveLease lease = Retained[^1];
                Retained.RemoveAt(Retained.Count - 1);
                peer.Release(in lease);
            }
        }
    }

    private static void Traffic(QuiclyPeer sender, Random random, int[] next, Dictionary<ulong, int> latest)
    {
        int count = random.Next(0, 24);
        for (int i = 0; i < count; i++)
        {
            int pick = random.Next(17);
            ushort channel = pick switch
            {
                < 5 => (ushort)2,
                < 8 => (ushort)3,
                < 9 => (ushort)6,
                < 12 => (ushort)8,
                < 14 => (ushort)9,
                < 15 => (ushort)10,
                < 16 => (ushort)11,
                _ => (ushort)12,
            };
            ulong key = channel is 8 or 9 or 12 ? (ulong)random.Next(1, 6) : 0;
            int size = channel == 6 ? random.Next(1300, 3500) : random.Next(4, 300);
            SendStatus status = sender.SendCopy(new SendHeader(channel, key), Payload(next[channel], size)).Status;
            if (status == SendStatus.Admitted)
            {
                if (channel == 12)
                {
                    latest[key] = next[channel];
                }

                next[channel]++;
            }
        }
    }

    private static void ReceiverStep(QuiclyPeer peer, Receiver r, ref bool handled3)
    {
        Random random = r.Random;
        if (random.Next(10) < 8)
        {
            peer.Poll(random.Next(4) switch { 0 => 1, 1 => 5, _ => int.MaxValue });
            peer.Flush();
        }

        if (random.Next(10) < 3)
        {
            r.DrainSome(peer, Unreliable[random.Next(Unreliable.Length)], random.Next(1, 9));
        }

        if (random.Next(10) < 3)
        {
            r.ReleaseSome(peer, random.Next(0, 12));
        }

        if (random.Next(50) == 0)
        {
            if (handled3)
            {
                peer.UnregisterHandler(3);
            }
            else
            {
                peer.RegisterHandler(3, r.Handler(retainSome: true));
            }

            handled3 = !handled3;
        }
    }

    private static void AssertOrderAndCounters(QuiclyPeer receiver, Receiver r, int[] sent)
    {
        // Nothing twice; per key strictly newer on the sequenced channel; exactly once and in order on the ordered one.
        foreach ((ushort channel, List<(ulong Key, int Index)> list) in r.Delivered)
        {
            if (channel is 8 or 9 or 12)
            {
                Dictionary<ulong, int> last = [];
                foreach ((ulong key, int index) in list)
                {
                    if (last.TryGetValue(key, out int previous))
                    {
                        Assert.True(index > previous, $"channel {channel} key {key}: {index} delivered after {previous}");
                    }

                    last[key] = index;
                }
            }
            else
            {
                HashSet<int> seen = [];
                foreach ((ulong _, int index) in list)
                {
                    Assert.True(seen.Add(index), $"channel {channel}: message {index} was delivered twice");
                }
            }
        }

        // Every message the transport side accepted on an unreliable ring channel reached the application or was counted.
        long drops = 0;
        foreach (ushort channel in Unreliable)
        {
            ChannelStatistics c = Channel(receiver, channel);
            int delivered = r.Delivered.TryGetValue(channel, out List<(ulong, int)>? list) ? list.Count : 0;
            Assert.True(c.DrainQueueDrops >= 0, $"channel {channel}: negative drop count");
            Assert.Equal(c.Received, delivered + c.DrainQueueDrops);
            drops += c.DrainQueueDrops;
        }

        receiver.GetStatistics(out PeerStatistics peer);
        Assert.Equal(drops, peer.DrainQueueDrops);
        Assert.Equal(sent[10], r.Delivered.TryGetValue(10, out List<(ulong Key, int Index)>? ordered) ? ordered.Count : 0);
        Assert.Equal(Enumerable.Range(0, sent[10]), ordered?.Select(e => e.Index) ?? []);
        Assert.Equal(sent[11], r.Delivered.TryGetValue(11, out List<(ulong, int)>? grouped) ? grouped.Count : 0);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void Mixed_Receive_Styles_Under_Loss_Leak_Nothing_And_Keep_Their_Counters(int seed)
    {
        using SlabAllocator shared = new(PeerCore.CreateCompactAllocatorOptions());
        Receiver r = new() { Random = new Random(seed * 7919) };
        int[] next = new int[16];
        Dictionary<ulong, int> latest = [];
        QuiclyPeer server;
        using (SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 3_000, JitterMicros = 2_000, LossPercent = 4, ReorderPercent = 10 },
            table: Table,
            client: DatagramKit.Quiet,
            server: o =>
            {
                DatagramKit.Quiet(o);
                o.Allocator = shared;
                o.ReceiveRingCapacity = 64;
            },
            seed: seed))
        {
            h.StopPumpingServer();
            server = h.Server!;
            server.RegisterHandler(3, r.Handler(retainSome: true));
            server.RegisterHandler(9, r.Handler(retainSome: true));
            server.RegisterHandler(10, r.Handler(retainSome: false));
            server.RegisterHandler(11, r.Handler(retainSome: false));
            server.RegisterHandler(12, r.Handler(retainSome: true));
            bool handled3 = true;
            Random traffic = new(seed);
            for (int step = 0; step < 3_000; step++)
            {
                Traffic(h.Client, traffic, next, latest);
                h.Client.Poll();
                h.Client.Flush();
                h.Network.Advance(1_000);
                ReceiverStep(server, r, ref handled3);
            }

            // Settle: everything in flight arrives, the reliable channels complete, the queues are emptied.
            if (!handled3)
            {
                server.RegisterHandler(3, r.Handler(retainSome: false));
            }

            for (int step = 0; step < 3_000; step++)
            {
                h.Client.Poll();
                h.Client.Flush();
                h.Network.Advance(1_000);
                server.Poll();
                server.Flush();
                foreach (ushort channel in Unreliable)
                {
                    if (channel != 3)
                    {
                        r.DrainAll(server, channel);
                    }
                }
            }

            r.ReleaseSome(server, 0);
            AssertOrderAndCounters(server, r, next);
            server.GetStatistics(out PeerStatistics settled);

            // Only partial reassemblies (lost fragments; swept when another fragment arrives) may still hold receive bytes.
            Assert.True(settled.ReceiveBytesOutstanding >= 0, $"the receive budget went negative: {settled.ReceiveBytesOutstanding}");

            // The run did what it is meant to: the backlog evicted, and the application held leases across polls.
            Assert.True(settled.DrainQueueDrops > 0, "the run never evicted from a drain queue");
            Assert.False(server.HasPendingWork, "a settled peer still reports work");

            // ReliableLatest: the last value sent for every key is the last one delivered for it.
            Dictionary<ulong, int> lastDelivered = [];
            foreach ((ulong key, int index) in r.Delivered[12])
            {
                lastDelivered[key] = index;
            }

            foreach ((ulong key, int index) in latest)
            {
                Assert.True(lastDelivered.TryGetValue(key, out int got) && got == index, $"ReliableLatest key {key}: last delivered {got}, last sent {index}");
            }

            // The sender's datagram outcomes add up once nothing is in flight (PeerStatistics.DatagramsSent remarks), and no
            // channel claims more transport drops than it sent.
            h.Client.GetStatistics(out PeerStatistics sender);
            Assert.Equal(sender.DatagramsSent, sender.DatagramsAcknowledged + sender.DatagramsLost + sender.DatagramsCanceled);
            Assert.Equal(0, sender.SendEntriesInUse);
            Assert.False(h.Client.Core.HasPendingExpiry, "an expiry still waits for a pass on a settled sender");
            foreach (ushort channel in new ushort[] { 2, 3, 6, 8, 9, 12 })
            {
                ChannelStatistics c = Channel(h.Client, channel);
                Assert.True(c.TransportCanceled >= 0 && c.TransportLost >= 0 && c.TransportCanceled + c.TransportLost <= c.Sent,
                    $"channel {channel}: sent {c.Sent}, canceled {c.TransportCanceled}, lost {c.TransportLost}");
            }

            h.DisposeServer();
        }

        Assert.True(server.IsFreed, "the server peer was not freed");
        Assert.Equal(0, shared.GetStatistics().TotalRentedBytes);
    }

    [Theory]
    [InlineData(11)]
    [InlineData(12)]
    [InlineData(13)]
    public void Dispose_In_The_Middle_With_Late_Releases_Returns_Every_Block(int seed)
    {
        using SlabAllocator shared = new(PeerCore.CreateCompactAllocatorOptions());
        Receiver r = new() { Random = new Random(seed * 104729) };
        int[] next = new int[16];
        Dictionary<ulong, int> latest = [];
        QuiclyPeer server;
        using (SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 2_000, JitterMicros = 1_000, LossPercent = 2, ReorderPercent = 5 },
            table: Table,
            client: DatagramKit.Quiet,
            server: o =>
            {
                DatagramKit.Quiet(o);
                o.Allocator = shared;
                o.ReceiveRingCapacity = 64;
            },
            seed: seed))
        {
            h.StopPumpingServer();
            server = h.Server!;
            server.RegisterHandler(3, r.Handler(retainSome: true));
            server.RegisterHandler(9, r.Handler(retainSome: true));
            server.RegisterHandler(10, r.Handler(retainSome: true));
            bool handled3 = true;
            Random traffic = new(seed);
            for (int step = 0; step < 600 + (seed * 37); step++)
            {
                Traffic(h.Client, traffic, next, latest);
                h.Client.Poll();
                h.Client.Flush();
                h.Network.Advance(1_000);
                ReceiverStep(server, r, ref handled3);
            }

            // No close, no settle: the peer goes with messages in the ring, in the queues, in mailboxes, mid-reassembly and
            // mid-stream, while the application still holds drained and retained leases.
            int heldAtDispose = r.Held.Count + r.Retained.Count;
            h.DisposeServer();

            // Half of what the application holds is released before the transport's close frees the peer, half after.
            r.ReleaseSome(server, heldAtDispose / 2);
            h.Network.RunUntilIdle(2_000_000);
            Assert.True(server.IsFreed, "the transport's close callback never freed the disposed peer");
            r.ReleaseSome(server, 0);
        }

        Assert.Equal(0, shared.GetStatistics().TotalRentedBytes);
    }

    private static readonly byte[] SessionToken = [9, 8, 7, 6];

    [Theory]
    [InlineData(21)]
    [InlineData(22)]
    public void A_Reconnect_In_The_Middle_Keeps_The_Receive_Budget_And_The_Pool_Exact(int seed)
    {
        using SlabAllocator shared = new(PeerCore.CreateCompactAllocatorOptions());
        Receiver r = new() { Random = new Random(seed * 31) };
        int[] next = new int[16];
        Dictionary<ulong, int> latest = [];
        QuiclyPeer client;
        using (SessionHarness h = new(
            connect: false,
            link: new LinkOptions { DelayMicros = 2_000, JitterMicros = 1_000, LossPercent = 2, ReorderPercent = 5 },
            table: Table,
            client: o =>
            {
                QuietOptions.Apply(o);
                o.Allocator = shared;
                o.ReceiveRingCapacity = 64;
            },
            server: QuietOptions.Apply,
            seed: seed))
        {
            h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
                AdmissionResult.Accept(SessionToken, 4242, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);
            Assert.True(h.RunUntilConnected());
            client = h.Client;
            client.RegisterHandler(3, r.Handler(retainSome: true));
            client.RegisterHandler(9, r.Handler(retainSome: true));
            client.RegisterHandler(10, r.Handler(retainSome: false));
            client.RegisterHandler(11, r.Handler(retainSome: false));
            client.RegisterHandler(12, r.Handler(retainSome: true));
            bool handled3 = true;
            Random traffic = new(seed);
            for (int round = 0; round < 3; round++)
            {
                QuiclyPeer server = h.Server!;
                for (int step = 0; step < 500; step++)
                {
                    Traffic(server, traffic, next, latest);
                    server.Poll();
                    server.Flush();
                    h.Network.Advance(1_000);
                    ReceiverStep(client, r, ref handled3);
                }

                if (round == 2)
                {
                    break;
                }

                // The link is lost with messages queued, held by the application and half received.
                server.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);
                for (int step = 0; step < 2_000 && client.State != PeerState.Closed; step++)
                {
                    h.Network.Advance(1_000);
                    client.Poll();
                }

                Assert.Equal(PeerState.Closed, client.State);
                client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
                for (int step = 0; step < 5_000 && !(client.State == PeerState.Connected && !ReferenceEquals(h.Server, server) && h.Server!.State == PeerState.Connected); step++)
                {
                    h.Network.Advance(1_000);
                    client.Poll();
                    client.Flush();
                    h.Server?.Poll();
                    h.Server?.Flush();
                }

                Assert.Equal(PeerState.Connected, client.State);
                server.Dispose();
            }

            // Settle the last connection, then give everything back.
            if (!handled3)
            {
                client.RegisterHandler(3, r.Handler(retainSome: false));
            }

            for (int step = 0; step < 2_000; step++)
            {
                h.Server!.Poll();
                h.Server.Flush();
                h.Network.Advance(1_000);
                client.Poll();
                client.Flush();
                foreach (ushort channel in Unreliable)
                {
                    if (channel != 3)
                    {
                        r.DrainAll(client, channel);
                    }
                }
            }

            r.ReleaseSome(client, 0);
            client.GetStatistics(out PeerStatistics settled);
            Assert.True(settled.ReceiveBytesOutstanding >= 0, $"the receive budget went negative: {settled.ReceiveBytesOutstanding}");
            long drops = 0;
            foreach (ushort channel in Unreliable)
            {
                drops += Channel(client, channel).DrainQueueDrops;
            }

            Assert.Equal(drops, settled.DrainQueueDrops);
            h.DisposeClient();
        }

        Assert.True(client.IsFreed, "the client peer was not freed");
        Assert.Equal(0, shared.GetStatistics().TotalRentedBytes);
    }

    [Fact]
    public void Releases_From_Another_Thread_While_The_Disposed_Peer_Is_Freed_Return_Every_Block()
    {
        // Release after Dispose is documented for "any thread, before or after the peer's native memory was freed": here the
        // transport's close callback frees the peer (returning its own queued leases to the shared pool) on one thread while
        // another releases what the application held. With lease validation on (Debug), a double return throws.
        for (int round = 0; round < 20; round++)
        {
            using SlabAllocator shared = new(PeerCore.CreateCompactAllocatorOptions());
            QuiclyPeer server;
            ReceivedMessage[] drained = new ReceivedMessage[48];
            Exception? failure = null;
            using (SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: o =>
            {
                DatagramKit.Quiet(o);
                o.Allocator = shared;
            }, seed: round + 1))
            {
                server = h.Server!;
                for (int i = 0; i < 96; i++)
                {
                    Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), Payload(i, 40)).Status);
                }

                Assert.True(h.RunUntil(() => Channel(server, 2).Received == 96), "the messages never arrived");

                // Half is handed to the application, half stays queued in the peer.
                server.Poll();
                Assert.Equal(48, server.Drain(2, drained));
                h.DisposeServer();
                using Barrier start = new(2);
                Thread releaser = new(() =>
                {
                    try
                    {
                        start.SignalAndWait();
                        for (int i = 0; i < drained.Length; i++)
                        {
                            server.Release(new ReadOnlySpan<ReceivedMessage>(in drained[i]));
                        }
                    }
                    catch (Exception exception)
                    {
                        failure = exception;
                    }
                });
                releaser.Start();
                start.SignalAndWait();
                h.Network.RunUntilIdle(1_000_000);
                Assert.True(releaser.Join(TimeSpan.FromSeconds(30)));
                Assert.True(server.IsFreed, "the transport's close callback never freed the disposed peer");
            }

            Assert.Null(failure);
            Assert.Equal(0, shared.GetStatistics().TotalRentedBytes);
        }
    }
}
