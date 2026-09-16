using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Receive-side delivery contracts of the unreliable modes (ADR 0003, PROTOCOL.md §2.1, §7) over SimulatedTransport.</summary>
public class DatagramDeliveryTests
{
    private static readonly ChannelTable Table = DatagramTables.Main;

    [Fact]
    public void Unordered_Messages_Arrive_Intact_And_Once_Under_Loss_Reorder_And_Jitter()
    {
        LinkOptions link = new() { DelayMicros = 20_000, JitterMicros = 15_000, LossPercent = 10, ReorderPercent = 20 };
        using SessionHarness h = new(link: link, table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet, seed: 7);
        HashSet<int> ids = [];
        List<int> order = [];
        int corrupt = 0;
        int duplicates = 0;
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            int id = BinaryPrimitives.ReadInt32LittleEndian(payload);
            if (!payload.SequenceEqual(DatagramKit.Payload(id, 64)))
            {
                corrupt++;
            }

            if (!ids.Add(id))
            {
                duplicates++;
            }

            order.Add(id);
        });
        int next = 0;
        for (int tick = 1; tick <= 100; tick++)
        {
            for (int i = 0; i < 10; i++)
            {
                h.Client.SendCopy(new SendHeader(2), DatagramKit.Payload(next++, 64));
            }

            h.Client.Flush((uint)tick);
            h.Network.Advance(16_667);
            h.Client.Poll();
            h.Server.Poll();
        }

        h.Run(500_000);
        Assert.Equal(0, corrupt);
        Assert.Equal(0, duplicates);
        Assert.InRange(ids.Count, 750, 990);
        Assert.True(DatagramKit.LinkStatistics(h.Client).DatagramsLost > 0);
        bool reordered = false;
        for (int i = 1; i < order.Count; i++)
        {
            reordered |= order[i] < order[i - 1];
        }

        Assert.True(reordered, "the link reordered nothing");
        Assert.Equal(ids.Count, DatagramKit.ChannelStats(h.Server, 2).Received);
    }

    [Fact]
    public void Sequenced_Delivers_Only_Newer_Values_Per_Key_Under_Reorder()
    {
        LinkOptions link = new() { DelayMicros = 30_000, JitterMicros = 20_000, ReorderPercent = 30 };
        using SessionHarness h = new(link: link, table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet, seed: 11);
        Dictionary<ulong, int> last = [];
        Dictionary<ulong, uint> lastSequence = [];
        int delivered = 0;
        int regressions = 0;
        h.Server!.RegisterHandler(3, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            int value = BinaryPrimitives.ReadInt32LittleEndian(payload);
            if (last.TryGetValue(header.Key, out int previous) && (value <= previous || (int)(header.Sequence - lastSequence[header.Key]) <= 0))
            {
                regressions++;
            }

            last[header.Key] = value;
            lastSequence[header.Key] = header.Sequence;
            delivered++;
        });
        int sent = 0;
        byte[] payload = new byte[16];
        for (int tick = 1; tick <= 300; tick++)
        {
            for (ulong key = 0; key < 4; key++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(payload, tick);
                h.Client.SendCopy(new SendHeader(3, key), payload);
                sent++;
            }

            h.Client.Flush((uint)tick);
            h.Network.Advance(16_667);
            h.Client.Poll();
            h.Server.Poll();
        }

        h.Run(500_000);
        Assert.Equal(0, regressions);
        Assert.True(delivered < sent, "reordering dropped nothing");
        ChannelStatistics stats = DatagramKit.ChannelStats(h.Server, 3);
        Assert.Equal(delivered, stats.Received);
        Assert.Equal(sent - delivered, stats.Dropped);
        Assert.All(last.Values, value => Assert.Equal(300, value));
        Assert.Equal(4, last.Count);
    }

    [Fact]
    public void Unkeyed_Sequenced_Values_Follow_The_Channel_Sequence_Across_The_16_Bit_Wrap()
    {
        using ServerHarness h = new(table: Table, server: DatagramKit.Quiet);
        Assert.True(h.Admit());
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(6, Handlers.Collect(got));
        foreach (uint sequence in new uint[] { 65_534, 65_535, 0, 65_533, 1, 1 })
        {
            h.Raw.SendDatagram(DatagramKit.Frame(Table, 6, sequence, 0, [(byte)sequence]));
        }

        h.Run(10_000);
        Assert.Equal(new uint[] { 65_534, 65_535, 0, 1 }, got.Select(g => g.Header.Sequence).ToArray());
        Assert.Equal(2, DatagramKit.ChannelStats(h.Server, 6).Dropped);
    }

    [Fact]
    public void Coalescing_Delivers_One_Value_Per_Key_Where_Sequenced_Delivers_Every_Newer_One()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> all = [];
        List<(ReceiveHeader Header, byte[] Payload)> latest = [];
        h.Server!.RegisterHandler(3, Handlers.Collect(all));
        h.Server.RegisterHandler(4, Handlers.Collect(latest));

        // A 60 Hz sender; the receiver polls only after ten messages per key.
        for (int tick = 1; tick <= 10; tick++)
        {
            for (ulong key = 1; key <= 3; key++)
            {
                h.Client.SendCopy(new SendHeader(3, key), [(byte)tick]);
                h.Client.SendCopy(new SendHeader(4, key), [(byte)tick]);
            }

            h.Client.Flush((uint)tick);
            h.Network.Advance(16_667);
            h.Client.Poll();
        }

        h.Server.Poll();
        Assert.Equal(30, all.Count);
        foreach (ulong key in new ulong[] { 1, 2, 3 })
        {
            Assert.Equal(Enumerable.Range(1, 10).Select(i => (byte)i).ToArray(), all.Where(m => m.Header.Key == key).Select(m => m.Payload[0]).ToArray());
        }

        Assert.Equal(3, latest.Count);
        Assert.Equal(new ulong[] { 1, 2, 3 }, latest.Select(m => m.Header.Key).OrderBy(k => k).ToArray());
        Assert.All(latest, m => Assert.Equal(10, m.Payload[0]));
        Assert.All(latest, m => Assert.Equal(10u, m.Header.SenderTick));
        ChannelStatistics stats = DatagramKit.ChannelStats(h.Server, 4);
        Assert.Equal(30, stats.Received);
        Assert.Equal(27, stats.ReceiveSuperseded);
        Assert.Equal(0, DatagramKit.Statistics(h.Server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void Unordered_Coalescing_Keeps_The_Latest_Arrival_Per_Key_For_Drain()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        foreach (byte value in new byte[] { 1, 2, 3 })
        {
            h.Client.SendCopy(new SendHeader(9, 7), [value]);
            h.Client.SendCopy(new SendHeader(9, 8), [(byte)(value + 10)]);
            h.Client.Flush();
            h.Network.Advance(1_000);
            h.Client.Poll();
        }

        h.Client.SendCopy(new SendHeader(9, 9), []);
        h.Client.Flush();
        h.Client.SendCopy(new SendHeader(9, 9), []);
        h.Client.Flush();
        h.Network.Advance(1_000);
        h.Server!.Poll();
        ReceivedMessage[] buffer = new ReceivedMessage[8];
        int count = h.Server.Drain(9, buffer);
        Assert.Equal(3, count);
        Dictionary<ulong, byte[]> byKey = buffer.Take(count).ToDictionary(m => m.Header.Key, m => m.Payload.ToArray());
        Assert.Equal(new byte[] { 3 }, byKey[7]);
        Assert.Equal(new byte[] { 13 }, byKey[8]);
        Assert.Empty(byKey[9]);
        h.Server.Release(buffer.AsSpan(0, count));
        Assert.Equal(5, DatagramKit.ChannelStats(h.Server, 9).ReceiveSuperseded);
        Assert.Equal(0, DatagramKit.Statistics(h.Server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Full_Receive_Ring_Drops_The_Newest_Messages_And_Counts_Them()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 4;
        });
        for (int i = 0; i < 10; i++)
        {
            h.Client.SendCopy(new SendHeader(2), [(byte)i]);
        }

        h.Client.Flush();
        h.Network.Advance(1_000);
        ChannelStatistics stats = DatagramKit.ChannelStats(h.Server!, 2);
        Assert.Equal(4, stats.Received);
        Assert.Equal(6, stats.RingDrops);
        Assert.Equal(6, DatagramKit.Statistics(h.Server!).ReceiveRingDrops);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        Assert.Equal(4, h.Server.Poll());
        Assert.Equal(new byte[] { 0, 1, 2, 3 }, got.Select(g => g.Payload[0]).ToArray());
        Assert.Equal(0, DatagramKit.Statistics(h.Server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void The_Receive_Budget_Bounds_What_Waits_For_Poll()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveBudgetBytes = 200;
        });
        byte[] payload = new byte[60];
        for (int i = 0; i < 10; i++)
        {
            h.Client.SendCopy(new SendHeader(2), payload);
        }

        h.Client.Flush();
        h.Network.Advance(1_000);
        ChannelStatistics stats = DatagramKit.ChannelStats(h.Server!, 2);
        Assert.Equal(3, stats.Received);
        Assert.Equal(7, stats.OutOfBuffers);
        Assert.Equal(7, DatagramKit.Statistics(h.Server!).OutOfReceiveBuffers);
        int received = 0;
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        h.Server.Poll();
        h.Client.SendCopy(new SendHeader(2), payload);
        Assert.True(h.RunUntil(() => received == 4));
    }

    [Fact]
    public void A_Full_Key_Table_Evicts_The_Least_Recently_Updated_Key()
    {
        using ServerHarness h = new(table: Table, server: DatagramKit.Quiet);
        Assert.True(h.Admit());
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(10, Handlers.Collect(got));
        void Send(ulong key, uint sequence) => h.Raw.SendDatagram(DatagramKit.Frame(Table, 10, sequence, key, [(byte)key]));

        Send(1, 10);
        Send(2, 11);
        Send(3, 12);
        Send(4, 13);  // four keys: the table is full
        Send(1, 9);   // stale: dropped; key 1 stays the least recently updated
        Send(5, 14);  // evicts key 1
        Send(1, 5);   // key 1 is new again, so any sequence is accepted (the documented replay window); evicts key 2
        Send(2, 3);   // the same for key 2; evicts key 3
        h.Run(10_000);
        Assert.Equal(new ulong[] { 1, 2, 3, 4, 5, 1, 2 }, got.Select(g => g.Header.Key).ToArray());
        Assert.Equal(new uint[] { 10, 11, 12, 13, 14, 5, 3 }, got.Select(g => g.Header.Sequence).ToArray());
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server, 10).Dropped);
        DatagramEngine engine = (DatagramEngine)h.Server.Core.GetEngine(ChannelMode.UnreliableSequenced)!;
        Assert.Equal(3, engine.KeyEvictions(h.Server.Core.ChannelIndexOf(10)));
        Assert.Equal(0, engine.KeyEvictions(h.Server.Core.ChannelIndexOf(3)));
    }

    [Fact]
    public void A_Dense_Key_Space_Refuses_Keys_Beyond_Its_Maximum_On_Both_Sides()
    {
        using ServerHarness h = new(table: Table, server: DatagramKit.Quiet);
        Assert.True(h.Admit());
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(11, Handlers.Collect(got));
        h.Raw.SendDatagram(DatagramKit.Frame(Table, 11, 1, 15, [15]));
        h.Raw.SendDatagram(DatagramKit.Frame(Table, 11, 2, 16, [16]));
        h.Raw.SendDatagram(DatagramKit.Frame(Table, 11, 3, 15, [15]));
        h.Raw.SendDatagram(DatagramKit.Frame(Table, 11, 2, 15, [15]));
        h.Run(10_000);
        Assert.Equal(new uint[] { 1, 3 }, got.Select(g => g.Header.Sequence).ToArray());
        ChannelStatistics stats = DatagramKit.ChannelStats(h.Server, 11);
        Assert.Equal(1, stats.ReceiveKeyTableFull);
        Assert.Equal(1, stats.Dropped);

        Assert.Equal(SendStatus.KeyTableFull, h.Server.SendCopy(new SendHeader(11, 16), [1]).Status);
        Assert.Equal(SendStatus.Admitted, h.Server.SendCopy(new SendHeader(11, 15), [1]).Status);
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server, 11).SendKeyTableFull);
    }

    [Fact]
    public void A_New_Epoch_On_The_Same_Engine_Restarts_Sequences_On_Both_Sides()
    {
        using ServerHarness h = new(table: Table, server: DatagramKit.Quiet);
        Assert.True(h.Admit());
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(3, Handlers.Collect(got));
        h.Raw.SendDatagram(DatagramKit.Frame(Table, 3, 10, 1, [1]));
        h.Raw.SendDatagram(DatagramKit.Frame(Table, 3, 9, 1, [2]));
        h.Run(10_000);
        Assert.Single(got);

        h.Server.SendCopy(new SendHeader(3, 1), [1]);
        h.Server.Flush();
        h.Server.SendCopy(new SendHeader(3, 1), [2]);
        h.Server.Flush();

        // The first epoch began when the session was admitted; a second one on the same engine resets its tables.
        h.Server.Core.GetEngine(ChannelMode.UnreliableSequenced)!.OnEpochReset(resumed: true);
        h.Raw.SendDatagram(DatagramKit.Frame(Table, 3, 9, 1, [3]));
        h.Server.SendCopy(new SendHeader(3, 1), [3]);
        h.Server.Flush();
        h.Run(10_000);
        Assert.Equal(new byte[] { 1, 3 }, got.Select(g => g.Payload[0]).ToArray());
        uint[] sequences = DatagramKit.ApplicationDatagrams(h.Raw.Sink).SelectMany(d => DatagramKit.Messages(d, Table)).Select(m => m.Header.Sequence).ToArray();
        Assert.Equal(new uint[] { 0, 1, 0 }, sequences);
    }
}
