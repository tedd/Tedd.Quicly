using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// UnreliableSequenced ordering on the channel's sequence clock (PROTOCOL.md §8): a key may idle for longer than half the
/// sequence space while other keys of its channel are busy, and a channel that loses half the space or more resynchronises.
/// Frames are written by hand, so every test chooses the exact sequences the receiver sees.
/// </summary>
public class SequencedIdleKeyTests
{
    private const ushort Keyed16 = 2;
    private const ushort Keyed32 = 3;
    private const ushort Dense16 = 4;
    private const ushort Coalescing16 = 5;
    private const ushort Fragmenting16 = 6;
    private const ushort Unkeyed16 = 7;
    private const ushort FewKeys16 = 8;

    /// <summary>
    /// 2 keyed 16-bit · 3 keyed 32-bit · 4 keyed 16-bit, dense keys 0 … 15 · 5 keyed 16-bit coalescing · 6 keyed 16-bit
    /// fragmenting · 7 unkeyed (16-bit) · 8 keyed 16-bit, 4 keys. Expiry is off everywhere.
    /// </summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(Keyed16, "moves16", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 16; o.ExpiryMicros = 0; })
        .Add(Keyed32, "moves32", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 32; o.ExpiryMicros = 0; })
        .Add(Dense16, "dense16", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 16; o.KeySpace = KeySpace.Dense(15); o.ExpiryMicros = 0; })
        .Add(Coalescing16, "latest16", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 16; o.CoalesceOnReceive = true; o.MaxKeys = 64; o.ExpiryMicros = 0; })
        .Add(Fragmenting16, "state16", ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.SequenceBits = 16;
            o.Fragmentation = true;
            o.MaxMessageSize = ChannelDefinition.FragmentedMaxMessageSize;
            o.ExpiryMicros = 0;
        })
        .Add(Unkeyed16, "clock", ChannelMode.UnreliableSequenced, o => o.ExpiryMicros = 0)
        .Add(FewKeys16, "few16", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 16; o.MaxKeys = 4; o.ExpiryMicros = 0; })
        .Build();

    private static ServerHarness Start(ushort channel, out List<(ReceiveHeader Header, byte[] Payload)> got)
    {
        ServerHarness h = new(table: Table, server: DatagramKit.Quiet);
        Assert.True(h.Admit());
        got = [];
        if (!Table[channel]!.CoalesceOnReceive)
        {
            h.Server!.RegisterHandler(channel, Handlers.Collect(got));
        }

        return h;
    }

    private static void Send(ServerHarness h, ushort channel, ulong key, uint sequence, byte payload = 0) =>
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(DatagramKit.Frame(Table, channel, sequence, key, [payload])));

    [Theory]
    [InlineData(Keyed16)]
    [InlineData(Dense16)]
    public void A_Key_Idle_Past_Half_The_16_Bit_Space_Accepts_Its_Next_Values(ushort channel)
    {
        using ServerHarness h = Start(channel, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, channel, 1, 0);
        Send(h, channel, 2, 30_000);
        Send(h, channel, 2, 60_000);
        for (uint sequence = 60_001; sequence <= 60_005; sequence++)
        {
            // Each is more than 32 768 past key 1's last value (0): in serial arithmetic "older".
            Send(h, channel, 1, sequence, 9);
        }

        h.Run(10_000);
        Assert.Equal(new uint[] { 0, 60_001, 60_002, 60_003, 60_004, 60_005 }, got.Where(m => m.Header.Key == 1).Select(m => m.Header.Sequence).ToArray());
        Assert.Equal(8, got.Count);
        Assert.Equal(0, DatagramKit.ChannelStats(h.Server!, channel).Dropped);
        Assert.Equal(0, h.Statistics().SequenceResyncs);
    }

    [Fact]
    public void A_Key_Idle_Past_Half_The_32_Bit_Space_Accepts_Its_Next_Value()
    {
        using ServerHarness h = Start(Keyed32, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, Keyed32, 1, 0);
        Send(h, Keyed32, 2, 0x7000_0000);
        Send(h, Keyed32, 2, 0xE000_0000);
        Send(h, Keyed32, 1, 0xE000_0001, 9);
        h.Run(10_000);
        Assert.Equal(new uint[] { 0, 0x7000_0000, 0xE000_0000, 0xE000_0001 }, got.Select(m => m.Header.Sequence).ToArray());
        Assert.Equal(0, DatagramKit.ChannelStats(h.Server!, Keyed32).Dropped);
    }

    [Fact]
    public void The_Audit_Scenario_Key_One_Survives_40000_Updates_Of_Key_Two()
    {
        // End to end, with the real sender: key 1 (a parked entity) updates once, key 2 (a busy one) 40 000 times on the same
        // 16-bit channel, then key 1 moves again. Its new values are 40 001 … 40 005 channel messages after its last one.
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(Keyed16, Handlers.Collect(got));
        h.Client.SendCopy(new SendHeader(Keyed16, 1), [1]);
        uint tick = 1;
        h.Client.Flush(tick++);
        h.Run(10_000);
        for (int batch = 0; batch < 400; batch++)
        {
            for (int i = 0; i < 100; i++)
            {
                h.Client.SendCopy(new SendHeader(Keyed16, 2), [2]);
            }

            h.Client.Flush(tick++);
            h.Run(2_000);
        }

        int before = got.Count;
        for (int i = 0; i < 5; i++)
        {
            h.Client.SendCopy(new SendHeader(Keyed16, 1), [3]);
            h.Client.Flush(tick++);
            h.Run(10_000);
        }

        h.Run(100_000);
        Assert.True(before > 30_000, $"only {before} busy-key messages arrived");
        Assert.Equal(0, DatagramKit.ChannelStats(h.Server, Keyed16).Dropped);
        Assert.Equal(5, got.Skip(before).Count(m => m.Header.Key == 1));
        Assert.Equal(0, DatagramKit.Statistics(h.Server).SequenceResyncs);
    }

    [Fact]
    public void A_Key_Idle_Across_Several_Wraps_Accepts_The_Same_Low_Bits_Again()
    {
        using ServerHarness h = Start(Keyed16, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, Keyed16, 1, 5);

        // Eleven steps of 30 000 (each under half the space): 330 000 channel messages, five wraps and 2 320 more.
        uint sequence = 5;
        for (int step = 0; step < 11; step++)
        {
            sequence = (ushort)(sequence + 30_000);
            Send(h, Keyed16, 2, sequence);
        }

        Assert.Equal(2_325u, sequence);

        // Key 1's value of exactly five wraps later carries the wire sequence it carried before.
        Send(h, Keyed16, 1, 5, 9);
        h.Run(10_000);
        Assert.Equal(13, got.Count);
        Assert.Equal(1ul, got[^1].Header.Key);
        Assert.Equal(9, got[^1].Payload[0]);
        Assert.Equal(0, DatagramKit.ChannelStats(h.Server!, Keyed16).Dropped);
    }

    [Fact]
    public void A_Coalescing_Channel_Keeps_The_New_Value_Of_An_Idle_Key()
    {
        using ServerHarness h = Start(Coalescing16, out _);
        Send(h, Coalescing16, 1, 0, 1);
        Send(h, Coalescing16, 2, 30_000, 2);
        Send(h, Coalescing16, 2, 60_000, 3);
        Send(h, Coalescing16, 1, 60_001, 9);
        h.Run(10_000);
        ReceivedMessage[] buffer = new ReceivedMessage[4];
        int count = h.Server!.Drain(Coalescing16, buffer);
        Assert.Equal(2, count);
        Dictionary<ulong, byte> latest = [];
        for (int i = 0; i < count; i++)
        {
            latest[buffer[i].Header.Key] = buffer[i].Payload[0];
        }

        h.Server.Release(buffer.AsSpan(0, count));
        Assert.Equal(9, latest[1]);
        Assert.Equal(3, latest[2]);
        ChannelStatistics stats = DatagramKit.ChannelStats(h.Server, Coalescing16);
        Assert.Equal(0, stats.Dropped);
        Assert.Equal(2, stats.ReceiveSuperseded);
    }

    [Fact]
    public void A_Reassembled_Message_For_An_Idle_Key_Is_Delivered()
    {
        using ServerHarness h = Start(Fragmenting16, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, Fragmenting16, 1, 0);
        Send(h, Fragmenting16, 2, 30_000);
        Send(h, Fragmenting16, 2, 60_000);
        byte[] message = DatagramKit.Payload(77, 600);
        foreach (byte[] fragment in FragmentKit.Split(Table[Fragmenting16]!, 60_001, 1, message, 3))
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(fragment));
        }

        h.Run(10_000);
        Assert.Equal(4, got.Count);
        Assert.Equal(1ul, got[^1].Header.Key);
        Assert.Equal(message, got[^1].Payload);
        Assert.Equal(0, DatagramKit.ChannelStats(h.Server!, Fragmenting16).Dropped);
        Assert.Equal(1, h.Statistics().FragmentedMessagesReceived);
    }

    [Fact]
    public void A_Reordered_Value_Of_A_Formerly_Idle_Key_Is_Still_Dropped()
    {
        using ServerHarness h = Start(Keyed16, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, Keyed16, 1, 100);
        Send(h, Keyed16, 2, 30_000);
        Send(h, Keyed16, 2, 60_000);
        Send(h, Keyed16, 1, 60_001, 9);
        Send(h, Keyed16, 1, 60_000, 8);   // older than what key 1 now holds
        Send(h, Keyed16, 1, 60_001, 7);   // and a duplicate of it
        h.Run(10_000);
        Assert.Equal(new uint[] { 100, 60_001 }, got.Where(m => m.Header.Key == 1).Select(m => m.Header.Sequence).ToArray());
        Assert.Equal(9, got[^1].Payload[0]);
        Assert.Equal(2, DatagramKit.ChannelStats(h.Server!, Keyed16).Dropped);
    }

    [Fact]
    public void Values_Of_Different_Keys_May_Arrive_In_Any_Order()
    {
        using ServerHarness h = Start(Keyed16, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, Keyed16, 2, 101);
        Send(h, Keyed16, 1, 100);   // behind the channel's newest, but key 1 has no value yet
        Send(h, Keyed16, 3, 99);
        Send(h, Keyed16, 1, 102);
        Send(h, Keyed16, 3, 98);    // behind key 3's own value
        h.Run(10_000);
        Assert.Equal(new uint[] { 101, 100, 99, 102 }, got.Select(m => m.Header.Sequence).ToArray());
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server!, Keyed16).Dropped);
        Assert.Equal(0, h.Statistics().SequenceResyncs);
    }

    [Theory]
    [InlineData(Unkeyed16)]
    [InlineData(Keyed16)]
    public void A_Channel_Recovers_After_Losing_More_Than_Half_The_Space(ushort channel)
    {
        using ServerHarness h = Start(channel, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, channel, 1, 10);

        // Three seconds in which 40 000 numbers were used and nothing arrived (a blackout, or sends that expired unsent).
        h.Run(3_000_000, step: 100_000);
        Send(h, channel, 1, 40_010);   // reads as 25 536 behind
        Send(h, channel, 1, 40_011);
        h.Run(10_000);
        Assert.Equal(new uint[] { 10, 40_010, 40_011 }, got.Select(m => m.Header.Sequence).ToArray());
        Assert.Equal(0, DatagramKit.ChannelStats(h.Server!, channel).Dropped);
        Assert.Equal(1, h.Statistics().SequenceResyncs);
    }

    [Theory]
    [InlineData(Unkeyed16)]
    [InlineData(Keyed16)]
    public void A_Late_Value_Inside_The_Quiet_Threshold_Is_Dropped(ushort channel)
    {
        using ServerHarness h = Start(channel, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, channel, 1, 10);
        h.Run(1_900_000, step: 100_000);
        Send(h, channel, 1, 9);
        Send(h, channel, 1, 40_010);   // a jump this soon is indistinguishable from a late message: dropped too
        h.Run(10_000);
        Assert.Single(got);
        Assert.Equal(2, DatagramKit.ChannelStats(h.Server!, channel).Dropped);
        Assert.Equal(0, h.Statistics().SequenceResyncs);
    }

    [Theory]
    [InlineData(Unkeyed16)]
    [InlineData(Keyed16)]
    public void A_Duplicate_Of_The_Newest_Is_Dropped_Even_After_A_Long_Quiet(ushort channel)
    {
        using ServerHarness h = Start(channel, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, channel, 1, 10);
        h.Run(5_000_000, step: 100_000);
        Send(h, channel, 1, 10);
        h.Run(10_000);
        Assert.Single(got);
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server!, channel).Dropped);
        Assert.Equal(0, h.Statistics().SequenceResyncs);
    }

    [Theory]
    [InlineData(Unkeyed16)]
    [InlineData(Keyed16)]
    public void A_Straggler_Inside_The_Reorder_Window_Is_Late_However_Long_The_Channel_Was_Quiet(ushort channel)
    {
        // A datagram can be seconds later than its successors without any network fault: it waited in the sender's
        // transport queue while a later one with the priority flag overtook it. It is a few numbers behind the clock, where
        // a real jump reads as that close only after almost a whole span was lost — so inside the window (1 024 on a
        // 16-bit channel) the quiet period does not make it a jump.
        using ServerHarness h = Start(channel, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, channel, 1, 2_000);
        h.Run(3_000_000, step: 100_000);
        Send(h, channel, 1, 1_999);
        Send(h, channel, 1, 2_000 - 1_024);   // the edge of the window
        h.Run(10_000);
        Assert.Single(got);
        Assert.Equal(2, DatagramKit.ChannelStats(h.Server!, channel).Dropped);
        Assert.Equal(0, h.Statistics().SequenceResyncs);

        // The clock did not move: the next genuine value is ahead of it.
        Send(h, channel, 1, 2_001);
        h.Run(10_000);
        Assert.Equal(new uint[] { 2_000, 2_001 }, got.Select(m => m.Header.Sequence).ToArray());
    }

    [Fact]
    public void A_Straggler_Inside_The_Reorder_Window_Of_A_32_Bit_Channel_Is_Late()
    {
        using ServerHarness h = Start(Keyed32, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, Keyed32, 1, 100_000);
        h.Run(3_000_000, step: 100_000);
        Send(h, Keyed32, 1, 100_000 - 65_536);   // the edge of the window: late
        h.Run(10_000);
        Assert.Single(got);
        Assert.Equal(0, h.Statistics().SequenceResyncs);

        Send(h, Keyed32, 1, 100_000 - 65_537);   // one past it: a jump of 2^32 − 65 537
        h.Run(10_000);
        Assert.Equal(new uint[] { 100_000, 100_000 - 65_537 }, got.Select(m => m.Header.Sequence).ToArray());
        Assert.Equal(1, h.Statistics().SequenceResyncs);
    }

    [Fact]
    public void After_A_Resync_The_Channel_Keeps_Delivering()
    {
        using ServerHarness h = Start(Unkeyed16, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, Unkeyed16, 0, 2_000);
        h.Run(3_000_000, step: 100_000);

        // One past the reorder window, after the quiet period: a jump of 65 536 − 1 025 numbers. (Were it a message that
        // more than 1 024 later ones overtook by three seconds, this would be the false resync that remains: delivered out
        // of order, once.) The channel goes on from it — the next sequences are ahead of it, older ones behind.
        Send(h, Unkeyed16, 0, 975);
        Send(h, Unkeyed16, 0, 977);
        Send(h, Unkeyed16, 0, 978);
        Send(h, Unkeyed16, 0, 974);
        h.Run(10_000);
        Assert.Equal(new uint[] { 2_000, 975, 977, 978 }, got.Select(m => m.Header.Sequence).ToArray());
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server!, Unkeyed16).Dropped);
        Assert.Equal(1, h.Statistics().SequenceResyncs);
    }

    [Fact]
    public void A_Stalled_Clock_Heals_After_The_Quiet_Threshold()
    {
        // A sequence far ahead (a peer's mistake) leaves the clock where no genuine value can reach it; "quiet" means the
        // clock did not advance, not that nothing arrived, so the stream of dropped values ends two seconds later.
        using ServerHarness h = Start(Unkeyed16, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, Unkeyed16, 0, 10);
        Send(h, Unkeyed16, 0, 30_000);
        for (uint sequence = 11; sequence < 31; sequence++)
        {
            Send(h, Unkeyed16, 0, sequence);
            h.Run(99_000, step: 33_000);
        }

        Assert.Equal(2, got.Count);
        h.Run(100_000, step: 50_000);
        Send(h, Unkeyed16, 0, 31);
        Send(h, Unkeyed16, 0, 32);
        h.Run(10_000);
        Assert.Equal(new uint[] { 10, 30_000, 31, 32 }, got.Select(m => m.Header.Sequence).ToArray());
        Assert.Equal(20, DatagramKit.ChannelStats(h.Server!, Unkeyed16).Dropped);
        Assert.Equal(1, h.Statistics().SequenceResyncs);
    }

    [Theory]
    [InlineData(Unkeyed16)]
    [InlineData(Keyed16)]
    public void A_New_Epoch_Forgets_The_Channel_Clock(ushort channel)
    {
        using ServerHarness h = Start(channel, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, channel, 1, 60_000);
        Send(h, channel, 1, 60_100);
        h.Run(10_000);

        // The first epoch began when the session was admitted; a second one on the same engine resets its tables.
        h.Server!.Core.GetEngine(ChannelMode.UnreliableSequenced)!.OnEpochReset(resumed: true);
        Send(h, channel, 1, 60_050);   // behind the old epoch's newest: the new epoch's first value
        Send(h, channel, 1, 60_049);   // and behind that
        Send(h, channel, 1, 60_051);
        h.Run(10_000);
        Assert.Equal(new uint[] { 60_000, 60_100, 60_050, 60_051 }, got.Select(m => m.Header.Sequence).ToArray());
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server, channel).Dropped);
        Assert.Equal(0, h.Statistics().SequenceResyncs);
    }

    [Fact]
    public void An_Evicted_Key_Still_Accepts_Any_Sequence()
    {
        using ServerHarness h = Start(FewKeys16, out List<(ReceiveHeader Header, byte[] Payload)> got);
        Send(h, FewKeys16, 1, 10);
        Send(h, FewKeys16, 2, 11);
        Send(h, FewKeys16, 3, 12);
        Send(h, FewKeys16, 4, 13);      // four keys: the table is full
        Send(h, FewKeys16, 5, 14);      // evicts key 1
        Send(h, FewKeys16, 1, 5);       // key 1 is new again: any sequence (the documented replay window); evicts key 2
        Send(h, FewKeys16, 5, 30_000);
        Send(h, FewKeys16, 5, 60_000);

        // Key 3 stayed in the table and idled past half the space: it is still ordered, on the channel's clock.
        Send(h, FewKeys16, 3, 60_001);
        Send(h, FewKeys16, 3, 60_000);  // stale
        h.Run(10_000);
        Assert.Equal(new uint[] { 10, 11, 12, 13, 14, 5, 30_000, 60_000, 60_001 }, got.Select(m => m.Header.Sequence).ToArray());
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server!, FewKeys16).Dropped);
    }
}
