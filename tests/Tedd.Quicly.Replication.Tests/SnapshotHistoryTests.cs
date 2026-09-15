namespace Tedd.Quicly.Replication.Tests;

public class SnapshotHistoryTests
{
    private static byte[] Pattern(uint tick, int length)
    {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++)
        {
            data[i] = (byte)((tick * 31) + (uint)(i * 7));
        }

        return data;
    }

    private static void AssertHolds(SnapshotHistory history, uint tick, int length)
    {
        Assert.True(history.TryGet(tick, out ReadOnlySpan<byte> snapshot), $"tick {tick} missing");
        Assert.True(snapshot.SequenceEqual(Pattern(tick, length)), $"tick {tick} corrupted");
    }

    [Fact]
    public void Constructor_Validates_And_Reports_Sizes()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SnapshotHistory(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SnapshotHistory(1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SnapshotHistory(1, 1, -1));
        SnapshotHistory defaults = new();
        Assert.Equal(SnapshotHistory.DefaultCapacity, defaults.Capacity);
        Assert.Equal(SnapshotHistory.DefaultArenaBytes, defaults.ArenaBytes);
        Assert.Equal(SnapshotHistory.DefaultMaxPeers, defaults.MaxPeers);
        Assert.Equal(0, defaults.Count);
        Assert.False(defaults.TryGetLatest(out uint tick, out ReadOnlySpan<byte> latest));
        Assert.Equal(0u, tick);
        Assert.True(latest.IsEmpty);
    }

    [Fact]
    public void Store_And_Lookup()
    {
        SnapshotHistory history = new(4, 1000, 2);
        Assert.True(history.Store(10, Pattern(10, 100)));
        AssertHolds(history, 10, 100);
        Assert.False(history.TryGet(11, out ReadOnlySpan<byte> missing));
        Assert.True(missing.IsEmpty);
        Assert.True(history.Contains(10));
        Assert.False(history.Contains(9));

        Assert.False(history.Store(10, Pattern(10, 5)));
        Assert.False(history.Store(9, Pattern(9, 5)));
        Assert.False(history.Store(11, new byte[1001]));
        Assert.Equal(1, history.Count);

        Assert.True(history.Store(11, ReadOnlySpan<byte>.Empty));
        Assert.True(history.TryGet(11, out ReadOnlySpan<byte> empty));
        Assert.True(empty.IsEmpty);
        Assert.True(history.TryGetLatest(out uint latestTick, out _));
        Assert.Equal(11u, latestTick);
        // The 1000-byte snapshot overwrites tick 10; the empty tick 11 occupies nothing and stays.
        Assert.True(history.Store(12, new byte[1000]));
        Assert.Equal(2, history.Count);
        Assert.False(history.Contains(10));
        Assert.True(history.TryGet(11, out empty));
        Assert.True(empty.IsEmpty);
        Assert.True(history.TryGet(12, out ReadOnlySpan<byte> large));
        Assert.Equal(1000, large.Length);
    }

    [Fact]
    public void Capacity_Evicts_The_Oldest()
    {
        SnapshotHistory history = new(4, 10_000, 0);
        for (uint tick = 1; tick <= 10; tick++)
        {
            Assert.True(history.Store(tick, Pattern(tick, 50)));
        }

        Assert.Equal(4, history.Count);
        for (uint tick = 1; tick <= 10; tick++)
        {
            Assert.Equal(tick >= 7, history.Contains(tick));
        }

        for (uint tick = 7; tick <= 10; tick++)
        {
            AssertHolds(history, tick, 50);
        }
    }

    [Fact]
    public void Arena_Evicts_Only_What_Is_Overwritten()
    {
        SnapshotHistory history = new(8, 100, 0);
        Assert.True(history.Store(1, Pattern(1, 40)));   // [0, 40)
        Assert.True(history.Store(2, Pattern(2, 40)));   // [40, 80)
        Assert.True(history.Store(3, Pattern(3, 40)));   // wraps to [0, 40): evicts 1
        Assert.False(history.Contains(1));
        AssertHolds(history, 2, 40);
        AssertHolds(history, 3, 40);
        Assert.True(history.Store(4, Pattern(4, 40)));   // [40, 80): evicts 2
        Assert.False(history.Contains(2));
        Assert.True(history.Store(5, Pattern(5, 20)));   // [80, 100): fits
        AssertHolds(history, 3, 40);
        AssertHolds(history, 4, 40);
        AssertHolds(history, 5, 20);
        Assert.True(history.Store(6, Pattern(6, 30)));   // wraps to [0, 30): evicts 3 only
        Assert.False(history.Contains(3));
        AssertHolds(history, 4, 40);
        AssertHolds(history, 5, 20);
        AssertHolds(history, 6, 30);
        Assert.Equal(3, history.Count);
    }

    [Fact]
    public void Wrapping_Evicts_The_Skipped_Tail_Before_Newer_Snapshots()
    {
        SnapshotHistory history = new(8, 1024, 0);
        Assert.True(history.Store(1, Pattern(1, 800)));  // A [0, 800)
        Assert.True(history.Store(2, Pattern(2, 200)));  // B [800, 1000)
        Assert.True(history.Store(3, Pattern(3, 300)));  // C wraps to [0, 300): evicts A
        Assert.False(history.Contains(1));
        AssertHolds(history, 2, 200);
        AssertHolds(history, 3, 300);

        Assert.True(history.Store(4, Pattern(4, 500)));  // E [300, 800): live region wrapped, nothing overlaps
        AssertHolds(history, 2, 200);
        AssertHolds(history, 3, 300);
        AssertHolds(history, 4, 500);

        Assert.True(history.Store(5, Pattern(5, 300)));  // wraps: B sits in the skipped tail (oldest), C is overwritten
        Assert.False(history.Contains(2));
        Assert.False(history.Contains(3));
        AssertHolds(history, 4, 500);
        AssertHolds(history, 5, 300);

        Assert.True(history.Store(6, Pattern(6, 1000))); // needs almost everything
        Assert.Equal(1, history.Count);
        AssertHolds(history, 6, 1000);
    }

    [Fact]
    public void Zero_Length_Snapshots_Occupy_Nothing_But_Keep_Fifo_Order()
    {
        SnapshotHistory history = new(8, 100, 0);
        Assert.True(history.Store(1, Pattern(1, 60)));   // [0, 60)
        Assert.True(history.Store(2, []));               // at 60, empty
        Assert.True(history.Store(3, Pattern(3, 30)));   // [60, 90)
        Assert.True(history.Store(4, Pattern(4, 50)));   // wraps to [0, 50): evicts 1; 2 survives (3 is not evicted)
        Assert.False(history.Contains(1));
        Assert.True(history.TryGet(2, out ReadOnlySpan<byte> empty) && empty.IsEmpty);
        AssertHolds(history, 3, 30);
        AssertHolds(history, 4, 50);

        Assert.True(history.Store(5, Pattern(5, 70)));   // wraps to [0, 70): 3 and 4 go, so 2 (older) goes too
        Assert.Equal(1, history.Count);
        AssertHolds(history, 5, 70);
    }

    [Fact]
    public void Random_Stores_Keep_A_Contiguous_Intact_Suffix()
    {
        Random random = new(20);
        SnapshotHistory history = new(8, 1000, 0);
        List<(uint Tick, int Length)> stored = [];
        uint tick = 0;
        for (int iteration = 0; iteration < 5000; iteration++)
        {
            tick += (uint)random.Next(1, 4);
            int length = random.Next(20) switch
            {
                0 => 1001,
                1 or 2 => random.Next(0, 3),
                _ => random.Next(0, 400),
            };
            bool ok = history.Store(tick, Pattern(tick, Math.Min(length, 1001)));
            if (length > 1000)
            {
                Assert.False(ok);
                continue;
            }

            Assert.True(ok);
            stored.Add((tick, length));

            int present = 0;
            int bytes = 0;
            for (int j = stored.Count - 1; j >= 0 && history.Contains(stored[j].Tick); j--)
            {
                AssertHolds(history, stored[j].Tick, stored[j].Length);
                present++;
                bytes += stored[j].Length;
            }

            for (int j = Math.Max(0, stored.Count - present - 20); j < stored.Count - present; j++)
            {
                Assert.False(history.Contains(stored[j].Tick));
            }

            Assert.Equal(history.Count, present);
            Assert.InRange(present, 1, 8);
            Assert.True(bytes <= 1000);
        }
    }

    [Fact]
    public void Ticks_Use_Serial_Arithmetic()
    {
        SnapshotHistory history = new(8, 1000, 1);
        uint[] ticks = [uint.MaxValue - 2, uint.MaxValue - 1, uint.MaxValue, 0, 1, 2];
        foreach (uint tick in ticks)
        {
            Assert.True(history.Store(tick, Pattern(tick, 10)));
        }

        foreach (uint tick in ticks)
        {
            AssertHolds(history, tick, 10);
        }

        Assert.False(history.Store(uint.MaxValue - 5, Pattern(0, 10)));
        Assert.True(history.AckBaseline(0, uint.MaxValue));
        Assert.True(history.AckBaseline(0, 1));
        Assert.False(history.AckBaseline(0, uint.MaxValue - 1));
    }

    [Fact]
    public void Baselines_Follow_The_Newest_Acknowledged_Snapshot()
    {
        SnapshotHistory history = new(4, 1000, 2);
        Assert.False(history.TryGetBaseline(0, out uint baselineTick, out ReadOnlySpan<byte> baseline));
        Assert.Equal(0u, baselineTick);
        Assert.True(baseline.IsEmpty);

        for (uint tick = 1; tick <= 3; tick++)
        {
            Assert.True(history.Store(tick, Pattern(tick, 20)));
        }

        Assert.False(history.AckBaseline(0, 5));
        Assert.True(history.AckBaseline(0, 2));
        Assert.True(history.TryGetBaseline(0, out baselineTick, out baseline));
        Assert.Equal(2u, baselineTick);
        Assert.True(baseline.SequenceEqual(Pattern(2, 20)));
        Assert.False(history.AckBaseline(0, 1));
        Assert.False(history.AckBaseline(0, 2));
        Assert.True(history.AckBaseline(0, 3));
        Assert.True(history.TryGetBaseline(0, out baselineTick, out _));
        Assert.Equal(3u, baselineTick);
        Assert.False(history.TryGetBaseline(1, out _, out _));

        // The baseline falls out of the ring: none until a newer acknowledgement.
        for (uint tick = 4; tick <= 8; tick++)
        {
            Assert.True(history.Store(tick, Pattern(tick, 20)));
        }

        Assert.False(history.TryGetBaseline(0, out baselineTick, out _));
        Assert.Equal(0u, baselineTick);
        Assert.True(history.AckBaseline(0, 6));
        Assert.True(history.TryGetBaseline(0, out baselineTick, out _));
        Assert.Equal(6u, baselineTick);

        history.ResetPeer(0);
        Assert.False(history.TryGetBaseline(0, out _, out _));
        Assert.True(history.AckBaseline(0, 5));

        Assert.Throws<ArgumentOutOfRangeException>(() => history.AckBaseline(2, 5));
        Assert.Throws<ArgumentOutOfRangeException>(() => history.TryGetBaseline(-1, out _, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => history.ResetPeer(2));

        history.Clear();
        Assert.Equal(0, history.Count);
        Assert.False(history.TryGetBaseline(0, out _, out _));
        Assert.True(history.Store(1, Pattern(1, 20)));
    }

    [Fact]
    public void An_Evicted_Baseline_Is_Replaced_Even_By_A_Serially_Older_Tick()
    {
        // After more than 2^31 ticks an evicted baseline can compare as newer than every stored tick.
        SnapshotHistory history = new(2, 100, 1);
        Assert.True(history.Store(10, Pattern(10, 5)));
        Assert.True(history.AckBaseline(0, 10));
        uint step = 1u << 30;
        Assert.True(history.Store(10 + step, Pattern(1, 5)));
        Assert.True(history.Store(10 + (2 * step), Pattern(2, 5)));
        uint late = 10 + (3 * step);
        Assert.True(history.Store(late, Pattern(late, 5)));
        Assert.False(history.Contains(10));
        Assert.True(history.AckBaseline(0, late));
        Assert.True(history.TryGetBaseline(0, out uint baselineTick, out _));
        Assert.Equal(late, baselineTick);
    }

    [Fact]
    public void Delta_Stream_With_Loss_And_Delayed_Acks_Keeps_The_Client_In_Sync()
    {
        Random random = new(21);
        SnapshotHistory server = new(32, 128 * 1024, 1);
        SnapshotHistory client = new(32, 128 * 1024, 0);
        byte[] world = TestData.Random(random, 2000);
        byte[] packet = new byte[DeltaCodec.GetMaxEncodedLength(4000)];
        byte[] decoded = new byte[4000];
        List<(uint Tick, uint Due)> acks = [];
        int full = 0;
        int deltas = 0;
        for (uint tick = 1; tick <= 2000; tick++)
        {
            world = TestData.Mutate(random, world, random.Next(10) == 0 ? random.Next(1500, 2500) : world.Length, 0.03);
            Assert.True(server.Store(tick, world));
            bool hasBaseline = server.TryGetBaseline(0, out uint baselineTick, out ReadOnlySpan<byte> baseline);
            int n = DeltaCodec.Encode(hasBaseline ? baseline : default, world, packet);
            Assert.True(n > 0);
            if (hasBaseline)
            {
                deltas++;
            }
            else
            {
                full++;
            }

            if (random.NextDouble() < 0.7)
            {
                ReadOnlySpan<byte> clientBaseline = default;
                if (hasBaseline)
                {
                    Assert.True(client.TryGet(baselineTick, out clientBaseline));
                }

                int m = DeltaCodec.Decode(clientBaseline, packet.AsSpan(0, n), decoded);
                Assert.Equal(world.Length, m);
                Assert.True(decoded.AsSpan(0, m).SequenceEqual(world));
                Assert.True(client.Store(tick, decoded.AsSpan(0, m)));

                // The tracked send completes Delivered some ticks later, unless that report is lost.
                if (random.NextDouble() < 0.9)
                {
                    acks.Add((tick, tick + (uint)random.Next(1, 6)));
                }
            }

            for (int k = acks.Count - 1; k >= 0; k--)
            {
                if (acks[k].Due <= tick)
                {
                    server.AckBaseline(0, acks[k].Tick);
                    acks.RemoveAt(k);
                }
            }
        }

        Assert.True(deltas > 10 * full);
    }
}
