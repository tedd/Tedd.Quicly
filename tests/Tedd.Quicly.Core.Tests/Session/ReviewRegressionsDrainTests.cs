using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Review (regressions lens) of the drain-queue change (D1a): what a host that did everything right on main — it polls
/// and drains every frame — now loses. Every scenario here stays inside what main delivered: the burst fits the receive
/// ring and the receive budget (asserted as a precondition: no ring drop, no out-of-buffers), and the application takes
/// everything out in the same frame.
/// </summary>
/// <remarks>
/// Fixed since: eviction applies only to a backlog that was left undrained across a Poll, a channel with a handler is never
/// evicted, and a message that finds the pool full without such a backlog is held as on main (<c>ReceiveQueues.BeginPass</c>,
/// <c>TryAppendDatagram</c>); SequenceResyncs moved behind the fields of the previous release. The descriptions and the
/// numbers in the comments below are what the tests found on the branch before the fix; the tests now pin the fix.
/// </remarks>
public class ReviewRegressionsDrainTests
{
    private const int Batch = 32;

    /// <summary>2, 3 unordered · 10 ordered · 11 reliable unordered. Nothing expires.</summary>
    private static readonly ChannelTable Mixed = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(3, "b", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(10, "stream", ChannelMode.ReliableOrdered)
        .Add(11, "group", ChannelMode.ReliableUnordered)
        .Build();

    /// <summary>Only unreliable channels, so no part of the queue pool is reserved.</summary>
    private static readonly ChannelTable UnreliableOnly = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(3, "b", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Build();

    // Default PeerOptions on the receiving end: ReceiveRingCapacity 4 096, ReceiveBudgetBytes 256 KiB.
    private static SessionHarness Harness(ChannelTable table) =>
        new(table: table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);

    private static byte[] Payload(int index, int size)
    {
        byte[] payload = new byte[size];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static int IndexOf(ReadOnlySpan<byte> payload) => BinaryPrimitives.ReadInt32LittleEndian(payload);

    private static string Counters(QuiclyPeer peer, ushort channel)
    {
        ChannelStatistics c = DatagramKit.ChannelStats(peer, channel);
        PeerStatistics p = DatagramKit.Statistics(peer);
        return $"Received {c.Received}, RingDrops {c.RingDrops}, OutOfBuffers {c.OutOfBuffers}, ReceiveRingDrops {p.ReceiveRingDrops}, " +
            $"DrainQueueDrops {c.DrainQueueDrops}";
    }

    /// <summary>
    /// A burst of <paramref name="count"/> messages arrives at the server between two of its frames: the client sends in
    /// batches and the network runs until the server's transport side has accepted each batch. The server is not polled.
    /// </summary>
    private static void Burst(SessionHarness h, ushort channel, int count, int size)
    {
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long arrived = DatagramKit.ChannelStats(server, channel).Received;
        for (int sent = 0; sent < count;)
        {
            int n = Math.Min(Batch, count - sent);
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(channel), Payload(sent + i, size)).Status);
            }

            client.Flush();
            sent += n;
            arrived += n;
            for (int step = 0; step < 1_000 && DatagramKit.ChannelStats(server, channel).Received < arrived; step++)
            {
                h.Network.Advance(1_000);
                client.Poll();
                client.Flush();
            }

            Assert.Equal(arrived, DatagramKit.ChannelStats(server, channel).Received);
        }

        // The precondition that makes this a regression test: the ring and the budget took the whole burst, which is all
        // main needed to deliver it.
        PeerStatistics peer = DatagramKit.Statistics(server);
        Assert.Equal(0, peer.ReceiveRingDrops);
        Assert.Equal(0, peer.OutOfReceiveBuffers);
    }

    private static List<int> DrainAll(QuiclyPeer peer, ushort channel)
    {
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[4096];
        int n;
        while ((n = peer.Drain(channel, buffer)) > 0)
        {
            for (int i = 0; i < n; i++)
            {
                got.Add(IndexOf(buffer[i].Payload));
            }

            peer.Release(buffer.AsSpan(0, n));
        }

        return got;
    }

    /// <summary>
    /// The frame the documentation recommends for a Drain-style channel — <c>Poll()</c>, then <c>Drain(channel)</c>
    /// (TROUBLESHOOTING.md: "Drain it every tick (after Poll)") — loses the oldest part of a burst that main delivered
    /// whole. The limit is not the 4 096-message ring or the 256 KiB budget any more but whichever of these is smaller:
    /// half the 1 024-node pool (512 messages, when the table has a reliable channel) or 64 KiB counted in block sizes —
    /// 1 024 messages of up to 64 bytes, 256 messages of 65 to 256 bytes, 42 messages of 257 to 1 536 bytes.
    /// </summary>
    [Theory]
    [InlineData(600, 4)]      // node limit 512: 88 lost
    [InlineData(1500, 4)]     // 988 lost
    [InlineData(400, 100)]    // byte limit, 256-byte blocks: 144 lost
    [InlineData(100, 1000)]   // byte limit, 1 536-byte blocks: 58 lost
    [InlineData(160, 1000)]   // 118 lost
    public void A_Consumer_That_Polls_And_Drains_Every_Frame_Loses_Nothing_Of_A_Burst(int count, int size)
    {
        using SessionHarness h = Harness(Mixed);
        QuiclyPeer server = h.Server!;

        Burst(h, 2, count, size);
        server.Poll();
        List<int> got = DrainAll(server, 2);

        Assert.True(got.Count == count, $"{got.Count} of {count} messages of {size} bytes reached the application ({Counters(server, 2)})");
        Assert.Equal(Enumerable.Range(0, count), got);
    }

    /// <summary>The same with a table that has no reliable channel: the whole pool (1 024 nodes) is the limit.</summary>
    [Theory]
    [InlineData(1500, 4)]     // 476 lost
    [InlineData(100, 1000)]   // 58 lost
    public void A_Consumer_That_Polls_And_Drains_Every_Frame_Loses_Nothing_Of_A_Burst_Without_Reliable_Channels(int count, int size)
    {
        using SessionHarness h = Harness(UnreliableOnly);
        QuiclyPeer server = h.Server!;

        Burst(h, 2, count, size);
        server.Poll();
        List<int> got = DrainAll(server, 2);

        Assert.True(got.Count == count, $"{got.Count} of {count} messages of {size} bytes reached the application ({Counters(server, 2)})");
        Assert.Equal(Enumerable.Range(0, count), got);
    }

    /// <summary>
    /// Steady traffic, not one burst: 60 full-size datagrams per frame on a Drain-style channel (60 KiB/frame of payload,
    /// 92 KiB of blocks — well inside the 256 KiB budget), polled and drained every frame. Main delivered every message.
    /// </summary>
    [Fact]
    public void A_Steady_Stream_Drained_Every_Frame_Loses_Nothing()
    {
        using SessionHarness h = Harness(Mixed);
        QuiclyPeer server = h.Server!;
        int total = 0;
        const int Frames = 10;
        const int PerFrame = 60;
        for (int frame = 0; frame < Frames; frame++)
        {
            Burst(h, 2, PerFrame, 1000);
            server.Poll();
            total += DrainAll(server, 2).Count;
            server.Flush();
        }

        Assert.True(total == Frames * PerFrame, $"{total} of {Frames * PerFrame} messages reached the application ({Counters(server, 2)})");
    }

    /// <summary>
    /// A host that mixes the styles — channel 2 is drained, channel 3 has a handler — and drains before it polls. Drain(2)
    /// walks the ring and queues channel 3's messages for the next Poll; on main they all waited there (or in the ring
    /// behind a held one) and the Poll dispatched every one. Now the queue evicts, so the handler never sees the oldest.
    /// </summary>
    [Theory]
    [InlineData(600, 4)]      // 88 lost
    [InlineData(100, 1000)]   // 58 lost
    public void Drain_Of_One_Channel_Loses_Nothing_Of_A_Handled_Channel(int count, int size)
    {
        using SessionHarness h = Harness(Mixed);
        QuiclyPeer server = h.Server!;
        List<int> handled = [];
        server.RegisterHandler(3, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => handled.Add(IndexOf(payload)));
        ReceivedMessage[] buffer = new ReceivedMessage[64];

        Burst(h, 3, count, size);
        Assert.Equal(0, server.Drain(2, buffer));
        server.Poll();

        Assert.True(handled.Count == count, $"the handler saw {handled.Count} of {count} messages of {size} bytes ({Counters(server, 3)})");
        Assert.Equal(Enumerable.Range(0, count), handled);
    }

    /// <summary>
    /// With both channels busy there is no order of Poll and Drain left that loses nothing: Poll-then-Drain evicts the
    /// drained channel's burst, Drain-then-Poll evicts the handled channel's. Main delivered both in either order.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_Host_That_Uses_Both_Styles_Has_An_Order_That_Loses_Nothing(bool pollFirst)
    {
        using SessionHarness h = Harness(Mixed);
        QuiclyPeer server = h.Server!;
        List<int> handled = [];
        server.RegisterHandler(3, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => handled.Add(IndexOf(payload)));

        // Interleaved in the ring: 300 of each channel, 100 bytes each (256-byte blocks: 154 KiB of the 256 KiB budget).
        for (int round = 0; round < 10; round++)
        {
            Burst(h, 3, 30, 100);
            Burst(h, 2, 30, 100);
        }

        List<int> drained;
        if (pollFirst)
        {
            server.Poll();
            drained = DrainAll(server, 2);
        }
        else
        {
            drained = DrainAll(server, 2);
            server.Poll();
        }

        Assert.True(drained.Count == 300 && handled.Count == 300,
            $"drained {drained.Count} of 300 ({Counters(server, 2)}), handled {handled.Count} of 300 ({Counters(server, 3)})");
    }

    /// <summary>
    /// The mitigation the documentation gives (TROUBLESHOOTING.md, <c>PeerStatistics.DrainQueueDrops</c>): "raise
    /// ReceiveBudgetBytes and, for many small messages, ReceiveRingCapacity". The node pool is capped at 1 024 whatever the
    /// ring's capacity (the default ring of 4 096 is already past the cap), and half of it is reserved when the table has a
    /// reliable channel, so no option lifts the limit of 512 small messages per frame.
    /// </summary>
    [Fact]
    public void Raising_The_Ring_And_The_Budget_As_Documented_Lifts_The_Backlog_Limit()
    {
        using SessionHarness h = new(table: Mixed, client: DatagramKit.Quiet, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 16_384;
            o.ReceiveBudgetBytes = 4 * 1024 * 1024;
        });
        QuiclyPeer server = h.Server!;

        Burst(h, 2, 600, 4);
        server.Poll();
        List<int> got = DrainAll(server, 2);

        Assert.True(got.Count == 600, $"{got.Count} of 600 messages reached the application with a 16 384-message ring and a 4 MiB budget ({Counters(server, 2)})");
    }

    /// <summary>
    /// The half of the pool "reserved for reliable channels" is a floor for them, not a ceiling: a reliable backlog may
    /// take every node, and an unreliable message that then finds no free node is evicted against its own (small or
    /// empty) queue — or, with nothing of its class queued, dropped itself, newest first, every one of them. A consumer
    /// that drains both channels in the same frame got all of them on main (the first was held, the rest waited in the
    /// ring). With 1 000 reliable messages in front, channel 2 keeps 24 of 50; with 1 024, none.
    /// </summary>
    [Theory]
    [InlineData(1000, 50)]
    [InlineData(1024, 50)]
    public void Unreliable_Messages_Behind_A_Reliable_Burst_Reach_A_Consumer_That_Drains_Both(int reliable, int unreliable)
    {
        using SessionHarness h = Harness(Mixed);
        QuiclyPeer server = h.Server!;

        Burst(h, 10, reliable, 4);
        Burst(h, 2, unreliable, 4);
        server.Poll();
        List<int> stream = DrainAll(server, 10);
        List<int> datagrams = DrainAll(server, 2);

        Assert.Equal(Enumerable.Range(0, reliable), stream);
        Assert.True(datagrams.Count == unreliable,
            $"{datagrams.Count} of {unreliable} unreliable messages reached the application behind {reliable} reliable ones ({Counters(server, 2)})");
    }

    /// <summary>
    /// <see cref="PeerStatistics"/> is a public <c>LayoutKind.Sequential</c> struct documented as "fixed layout": a host
    /// that blits it (a telemetry writer, shared memory, a native exporter) reads fields by offset. 0.2.x ended with
    /// RequestsSent, RequestsTimedOut, ResponsesUnmatched; new fields must come after them. SequenceResyncs was inserted
    /// before RequestsSent, which moves those three by eight bytes.
    /// </summary>
    [Fact]
    public void New_PeerStatistics_Fields_Are_Appended_After_The_Fields_Of_The_Previous_Release()
    {
        int lastOld = (int)Marshal.OffsetOf<PeerStatistics>(nameof(PeerStatistics.ResponsesUnmatched));
        string[] added =
        [
            nameof(PeerStatistics.SequenceResyncs),
            nameof(PeerStatistics.DatagramsAcknowledged),
            nameof(PeerStatistics.DatagramsLost),
            nameof(PeerStatistics.DatagramsCanceled),
            nameof(PeerStatistics.DrainQueueDrops),
        ];
        foreach (string field in added)
        {
            int offset = (int)Marshal.OffsetOf<PeerStatistics>(field);
            Assert.True(offset > lastOld, $"{field} is at offset {offset}, before ResponsesUnmatched ({lastOld}): the offsets of the 0.2.x fields behind it changed");
        }
    }
}
