using System.Buffers.Binary;
using System.Reflection;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Review (recheck 2, round 1) of 4c06333: the drain-queue rule as it stands, driven by randomised hosts over a small
/// ring and pool so that the full-pool, held and backlog states are reached in every run.
/// </summary>
public class ReviewRecheck2R1DrainModelTests
{
    private const int Ring = 64;

    /// <summary>2, 3, 4 unordered and read with Drain; 5 unordered with a handler. Nothing expires.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(3, "b", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(4, "c", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(5, "h", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Build();

    private static readonly FieldInfo QueuesField = typeof(QuiclyPeer).GetField("_queues", BindingFlags.NonPublic | BindingFlags.Instance)!;
    private static readonly FieldInfo HasHeldField = typeof(QuiclyPeer).GetField("_hasHeld", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static ReceiveQueues Queues(QuiclyPeer peer) => (ReceiveQueues)QueuesField.GetValue(peer)!;

    private static bool HasHeld(QuiclyPeer peer) => (bool)HasHeldField.GetValue(peer)!;

    private static readonly FieldInfo HandlersField = typeof(QuiclyPeer).GetField("_handlers", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private sealed class World : IDisposable
    {
        public readonly SessionHarness H;
        public readonly QuiclyPeer Server;
        public readonly int[] Sent = new int[8];
        public readonly List<int>[] Got = [[], [], [], [], [], [], [], []];
        private readonly ReceivedMessage[] _buffer = new ReceivedMessage[512];

        public World(int ring = Ring, int budget = 256 * 1024)
        {
            H = new SessionHarness(table: Table, client: DatagramKit.Quiet, server: o =>
            {
                DatagramKit.Quiet(o);
                o.ReceiveRingCapacity = ring;
                o.ReceiveBudgetBytes = budget;
            });
            Server = H.Server!;
        }

        public void Handle(ushort channel) =>
            Server.RegisterHandler(channel, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
                Got[header.Channel].Add(BinaryPrimitives.ReadInt32LittleEndian(payload)));

        public void Burst(ushort channel, int count, int size)
        {
            byte[] payload = new byte[Math.Max(4, size)];
            for (int i = 0; i < count; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(payload, Sent[channel]);
                Assert.Equal(SendStatus.Admitted, H.Client.SendCopy(new SendHeader(channel), payload).Status);
                Sent[channel]++;
                if ((i & 31) == 31)
                {
                    Deliver();
                }
            }

            Deliver();
        }

        public void Deliver()
        {
            H.Client.Flush();
            for (int step = 0; step < 4; step++)
            {
                H.Network.Advance(1_000);
                H.Client.Poll();
                H.Client.Flush();
            }
        }

        public int DrainOnce(ushort channel, int most)
        {
            int n = Server.Drain(channel, _buffer.AsSpan(0, most));
            for (int i = 0; i < n; i++)
            {
                Got[channel].Add(BinaryPrimitives.ReadInt32LittleEndian(_buffer[i].Payload));
            }

            Server.Release(_buffer.AsSpan(0, n));
            return n;
        }

        public void DrainAll(ushort channel, int most = 512)
        {
            while (DrainOnce(channel, most) > 0)
            {
            }
        }

        public void Dispose() => H.Dispose();
    }

    private static void AssertTotals(QuiclyPeer server, string where)
    {
        ReceiveQueues queues = Queues(server);
        int used = 0;
        int backlogNodes = 0;
        long backlogBytes = 0;
        int handled = 0;
        MessageHandler?[] handlers = (MessageHandler?[])HandlersField.GetValue(server)!;
        for (int ci = 0; ci < server.Core.ChannelCount; ci++)
        {
            Assert.True(queues.Count(ci) >= 0 && queues.Bytes(ci) >= 0, $"{where}: negative count or bytes on channel index {ci}");
            if (queues.Count(ci) == 0)
            {
                Assert.True(queues.Bytes(ci) == 0, $"{where}: channel index {ci} has {queues.Bytes(ci)} bytes over an empty queue");
            }

            used += queues.Count(ci);
            handled += handlers[ci] is null ? 0 : queues.Count(ci);
            if (queues.IsBacklogged(ci))
            {
                backlogNodes += queues.Count(ci);
                backlogBytes += queues.Bytes(ci);
            }
        }

        Assert.True(queues.QueuedHandled == handled, $"{where}: QueuedHandled {queues.QueuedHandled} but the channels with a handler hold {handled}");
        Assert.True(queues.Used == used, $"{where}: Used {queues.Used} but the channels hold {used}");
        Assert.True(queues.BacklogNodes == backlogNodes, $"{where}: BacklogNodes {queues.BacklogNodes} but the backlogged channels hold {backlogNodes}");
        Assert.True(queues.BacklogBytes == backlogBytes, $"{where}: BacklogBytes {queues.BacklogBytes} but the backlogged channels hold {backlogBytes}");
        long perChannel = 0;
        foreach (ushort channel in (ushort[])[2, 3, 4, 5])
        {
            perChannel += DatagramKit.ChannelStats(server, channel).DrainQueueDrops;
        }

        Assert.True(DatagramKit.Statistics(server).DrainQueueDrops == perChannel,
            $"{where}: peer DrainQueueDrops {DatagramKit.Statistics(server).DrainQueueDrops} but the channels sum to {perChannel}");
    }

    private static void AssertInOrder(List<int> got, string what)
    {
        for (int i = 1; i < got.Count; i++)
        {
            Assert.True(got[i] > got[i - 1], $"{what}: index {got[i]} was delivered after {got[i - 1]}");
        }
    }

    /// <summary>
    /// A diligent host: one Poll per frame and every Drain-style channel drained until 0 once per frame, in an order that
    /// is fixed for the run (every permutation of Poll, Drain(2), Drain(3), Drain(4) is a run). Random bursts on all four
    /// channels, several times the pool. Nothing may be evicted or dropped by the queues, every message the ring and the
    /// budget took must arrive, in order, and the queue totals must stay exact.
    /// </summary>
    [Theory]
    [MemberData(nameof(Orders))]
    public void A_Host_That_Polls_Once_And_Drains_Every_Channel_Every_Frame_Loses_Nothing_In_Any_Fixed_Order(string order, int seed)
    {
        using World w = new(ring: 4096, budget: 4 * 1024 * 1024);
        w.Handle(5);
        Random random = new(seed);
        int heldSeen = 0;
        for (int frame = 0; frame < 40; frame++)
        {
            if (frame >= 1 && frame < 30) // frame 0 is quiet: the harness polled without draining while it connected
            {
                int bursts = random.Next(0, 4);
                for (int b = 0; b < bursts; b++)
                {
                    w.Burst((ushort)random.Next(2, 6), random.Next(1, 1500), random.Next(4, 120));
                }
            }

            foreach (char step in order)
            {
                if (step == 'P')
                {
                    w.Server.Poll();
                }
                else
                {
                    w.DrainAll((ushort)(step - '0'), random.Next(1, 3) == 1 ? 7 : 512);
                }

                AssertTotals(w.Server, $"order {order} seed {seed} frame {frame} after {step}");
                heldSeen += HasHeld(w.Server) ? 1 : 0;
            }

            w.Server.Flush();
            Assert.True(DatagramKit.Statistics(w.Server).DrainQueueDrops == 0,
                $"order {order} seed {seed} frame {frame}: DrainQueueDrops {DatagramKit.Statistics(w.Server).DrainQueueDrops} for a host that drains every channel every frame");
        }

        PeerStatistics stats = DatagramKit.Statistics(w.Server);
        long missing = 0;
        foreach (ushort channel in (ushort[])[2, 3, 4, 5])
        {
            AssertInOrder(w.Got[channel], $"order {order} seed {seed} channel {channel}");
            missing += w.Sent[channel] - w.Got[channel].Count;
        }

        Assert.False(HasHeld(w.Server), "a message is still held after ten quiet frames");
        Assert.True(heldSeen > 0, "the run never reached a hold");
        Assert.Equal(0, Queues(w.Server).Used);
        Assert.True(missing == stats.ReceiveRingDrops + stats.OutOfReceiveBuffers,
            $"order {order} seed {seed}: {missing} messages did not arrive, ReceiveRingDrops {stats.ReceiveRingDrops}, OutOfReceiveBuffers {stats.OutOfReceiveBuffers}, DrainQueueDrops {stats.DrainQueueDrops}");
    }

    public static IEnumerable<object[]> Orders()
    {
        string[] orders = ["P234", "P432", "234P", "432P", "2P34", "23P4", "3P42", "4P23"];
        foreach (string order in orders)
        {
            for (int seed = 1; seed <= 3; seed++)
            {
                yield return [order, seed];
            }
        }
    }

    /// <summary>
    /// An arbitrary host: random bursts, Poll with random limits, partial and complete drains, and a handler that comes and
    /// goes on channel 4. After every call the queue totals must be exact (bytes and nodes per channel against the backlog
    /// totals, the peer's DrainQueueDrops against the channels'), and at the end every message is accounted for exactly
    /// once: delivered, evicted (DrainQueueDrops), or refused on arrival (ring, budget).
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    public void Any_Host_Keeps_The_Queue_Totals_Exact_And_Every_Message_Is_Counted_Once(int seed)
    {
        using World w = new(budget: seed % 2 == 0 ? 24 * 1024 : 256 * 1024);
        w.Handle(5);
        bool handled4 = false;
        int heldSeen = 0;
        Random random = new(seed);
        for (int step = 0; step < 1500; step++)
        {
            int action = random.Next(0, 10);
            string what;
            switch (action)
            {
                case 0:
                case 1:
                case 2:
                    ushort channel = (ushort)random.Next(2, 6);
                    int count = random.Next(1, 90);
                    w.Burst(channel, count, random.Next(4, 900));
                    what = $"burst {count} on {channel}";
                    break;
                case 3:
                case 4:
                case 5:
                    int limit = random.Next(0, 4) switch { 0 => 0, 1 => 1, 2 => 5, _ => int.MaxValue };
                    w.Server.Poll(limit);
                    what = $"Poll({limit})";
                    break;
                case 6:
                case 7:
                    ushort partial = (ushort)random.Next(2, 5);
                    if (partial == 4 && handled4)
                    {
                        partial = 2;
                    }

                    int most = random.Next(1, 100);
                    w.DrainOnce(partial, most);
                    what = $"Drain({partial}, {most})";
                    break;
                case 8:
                    ushort full = (ushort)random.Next(2, 5);
                    if (full == 4 && handled4)
                    {
                        full = 3;
                    }

                    w.DrainAll(full);
                    what = $"Drain({full}) until 0";
                    break;
                default:
                    if (handled4)
                    {
                        Assert.True(w.Server.UnregisterHandler(4));
                    }
                    else
                    {
                        w.Handle(4);
                    }

                    handled4 = !handled4;
                    what = handled4 ? "register 4" : "unregister 4";
                    break;
            }

            AssertTotals(w.Server, $"seed {seed} step {step} after {what}");
            heldSeen += HasHeld(w.Server) ? 1 : 0;
        }

        if (handled4)
        {
            w.Server.UnregisterHandler(4);
        }

        for (int frame = 0; frame < 8; frame++)
        {
            w.Server.Poll();
            w.DrainAll(2);
            w.DrainAll(3);
            w.DrainAll(4);
        }

        AssertTotals(w.Server, $"seed {seed} at the end");
        ReceiveQueues queues = Queues(w.Server);
        Assert.False(HasHeld(w.Server), "a message is still held after eight frames that drained everything");
        Assert.Equal(0, queues.Used);
        Assert.Equal(0, queues.BacklogNodes);
        Assert.Equal(0, queues.BacklogBytes);
        Assert.True(heldSeen > 0 && DatagramKit.Statistics(w.Server).DrainQueueDrops > 0, $"seed {seed}: the run never reached a hold ({heldSeen}) or an eviction");
        PeerStatistics stats = DatagramKit.Statistics(w.Server);
        long missing = 0;
        foreach (ushort channel in (ushort[])[2, 3, 4, 5])
        {
            missing += w.Sent[channel] - w.Got[channel].Count;
            Assert.Equal(w.Got[channel].Count, w.Got[channel].Distinct().Count());
        }

        Assert.True(missing == stats.ReceiveRingDrops + stats.OutOfReceiveBuffers + stats.DrainQueueDrops,
            $"seed {seed}: {missing} messages did not arrive, but ReceiveRingDrops {stats.ReceiveRingDrops} + OutOfReceiveBuffers {stats.OutOfReceiveBuffers} + DrainQueueDrops {stats.DrainQueueDrops} = {stats.ReceiveRingDrops + stats.OutOfReceiveBuffers + stats.DrainQueueDrops}");
    }

    /// <summary>
    /// No permanent hold: a host that never drains (three unread unreliable channels, one handled channel) and polls once
    /// per frame, under a flood on the unread channels that is several pools per frame. Each unread channel may close the
    /// ring once (twice at the session start); after that every Poll must leave the ring open, and the handled channel
    /// must keep being served.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void Unread_Unreliable_Channels_Stop_Holding_The_Ring_After_A_Bounded_Number_Of_Polls(int seed)
    {
        using World w = new(ring: 256);
        w.Handle(5);
        Random random = new(seed);
        List<int> heldFrames = [];
        for (int frame = 0; frame < 60; frame++)
        {
            foreach (ushort channel in (ushort[])[2, 3, 4])
            {
                w.Burst(channel, random.Next(0, 70), random.Next(4, 200));
            }

            w.Burst(5, 5, 8);
            w.Server.Poll();
            w.Server.Flush();
            if (HasHeld(w.Server))
            {
                heldFrames.Add(frame);
            }

            AssertTotals(w.Server, $"seed {seed} frame {frame}");
        }

        // Three unread channels: at most two intervals each (the session start counts as a drain).
        Assert.True(heldFrames.Count <= 6 && (heldFrames.Count == 0 || heldFrames[^1] < 20),
            $"seed {seed}: the ring was left closed after the Poll of frames [{string.Join(",", heldFrames)}]");
        for (int frame = 0; frame < 4; frame++)
        {
            w.Server.Poll();
        }

        PeerStatistics stats = DatagramKit.Statistics(w.Server);
        Assert.True(w.Got[5].Count + stats.ReceiveRingDrops + stats.OutOfReceiveBuffers >= w.Sent[5] && w.Got[5].Count > 250,
            $"seed {seed}: the handler saw {w.Got[5].Count} of {w.Sent[5]} (ReceiveRingDrops {stats.ReceiveRingDrops}, OutOfReceiveBuffers {stats.OutOfReceiveBuffers})");
    }
}

/// <summary>
/// The same diligent host with a reliable channel next to the unreliable ones (all read with Drain), which shares the
/// node pool: the reliable channel loses nothing at all, the unreliable ones nothing the ring and the budget took.
/// </summary>
public class ReviewRecheck2R1DrainReliableModelTests
{
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(3, "b", ChannelMode.UnreliableSequenced, o => o.ExpiryMicros = 0)
        .Add(6, "r", ChannelMode.ReliableOrdered)
        .Build();

    [Theory]
    [InlineData("P236", 1)]
    [InlineData("P632", 2)]
    [InlineData("6P23", 3)]
    [InlineData("236P", 4)]
    [InlineData("2P63", 5)]
    [InlineData("63P2", 6)]
    public void A_Diligent_Host_With_A_Reliable_Drain_Channel_Loses_Nothing(string order, int seed)
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveBudgetBytes = 4 * 1024 * 1024;
        });
        QuiclyPeer server = h.Server!;
        QuiclyPeer client = h.Client;
        int[] sent = new int[8];
        List<int>[] got = [[], [], [], [], [], [], [], []];
        ReceivedMessage[] buffer = new ReceivedMessage[300];
        Random random = new(seed);
        byte[] payload = new byte[64];
        void Deliver()
        {
            client.Flush();
            for (int step = 0; step < 4; step++)
            {
                h.Network.Advance(1_000);
                client.Poll();
                client.Flush();
            }
        }

        for (int frame = 0; frame < 60; frame++)
        {
            if (frame >= 1 && frame < 30)
            {
                for (int b = random.Next(0, 4); b > 0; b--)
                {
                    ushort channel = random.Next(0, 3) switch { 0 => 2, 1 => 3, _ => 6 };
                    int count = random.Next(1, channel == 6 ? 600 : 1500);
                    for (int i = 0; i < count; i++)
                    {
                        BinaryPrimitives.WriteInt32LittleEndian(payload, sent[channel]);
                        if (client.SendCopy(new SendHeader(channel), payload.AsSpan(0, random.Next(4, 64))).Status != SendStatus.Admitted)
                        {
                            break;
                        }

                        sent[channel]++;
                        if ((i & 31) == 31)
                        {
                            Deliver();
                        }
                    }
                }
            }

            Deliver();
            foreach (char step in order)
            {
                if (step == 'P')
                {
                    server.Poll();
                    continue;
                }

                ushort channel = (ushort)(step - '0');
                int n;
                while ((n = server.Drain(channel, buffer)) > 0)
                {
                    for (int i = 0; i < n; i++)
                    {
                        got[channel].Add(BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload));
                    }

                    server.Release(buffer.AsSpan(0, n));
                }
            }

            server.Flush();
            Assert.True(DatagramKit.Statistics(server).DrainQueueDrops == 0,
                $"order {order} seed {seed} frame {frame}: DrainQueueDrops {DatagramKit.Statistics(server).DrainQueueDrops}");
        }

        PeerStatistics stats = DatagramKit.Statistics(server);
        Assert.True(got[6].Count == sent[6], $"order {order}: the reliable channel delivered {got[6].Count} of {sent[6]}");
        Assert.Equal(Enumerable.Range(0, sent[6]), got[6]);
        long missing = sent[2] - got[2].Count + sent[3] - got[3].Count;
        Assert.True(missing == stats.ReceiveRingDrops + stats.OutOfReceiveBuffers,
            $"order {order} seed {seed}: {missing} unreliable messages did not arrive (2: {got[2].Count}/{sent[2]}, 3: {got[3].Count}/{sent[3]}), ReceiveRingDrops {stats.ReceiveRingDrops}, OutOfReceiveBuffers {stats.OutOfReceiveBuffers}");
        for (int i = 1; i < got[3].Count; i++)
        {
            Assert.True(got[3][i] > got[3][i - 1], "the sequenced channel went backwards");
        }
    }
}
