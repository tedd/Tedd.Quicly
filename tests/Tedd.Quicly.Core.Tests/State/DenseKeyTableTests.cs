using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Tests.State;

public class DenseKeyTableTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(DenseKeyTable.MaxKeysLimit + 1)]
    public void Constructor_Rejects_Invalid_MaxKeys(int maxKeys)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new DenseKeyTable(maxKeys));
    }

    [Fact]
    public void Slot_Is_The_Key()
    {
        using var table = new DenseKeyTable(200);
        Assert.Equal(200, table.MaxKeys);
        foreach (ulong key in new ulong[] { 0, 1, 63, 64, 65, 127, 128, 199 })
        {
            Assert.False(table.TryGetSlot(key, out int missing));
            Assert.Equal(-1, missing);
            Assert.True(table.TryAdd(key, out int slot));
            Assert.Equal((int)key, slot);
            Assert.True(table.TryGetSlot(key, out slot));
            Assert.Equal((int)key, slot);
        }

        Assert.Equal(8, table.Count);
        Assert.False(table.TryAdd(64, out int again));
        Assert.Equal(64, again);
        Assert.True(table.TryGetOrAdd(64, out again));
        Assert.Equal(64, again);
        Assert.True(table.TryGetOrAdd(100, out int added));
        Assert.Equal(100, added);
        Assert.Equal(9, table.Count);
    }

    [Theory]
    [InlineData(200UL)]
    [InlineData(201UL)]
    [InlineData(1UL << 32)]
    [InlineData(ulong.MaxValue)]
    public void Keys_Outside_The_Key_Space_Are_Rejected(ulong key)
    {
        using var table = new DenseKeyTable(200);
        Assert.False(table.TryAdd(key, out int slot));
        Assert.Equal(-1, slot);
        Assert.False(table.TryGetOrAdd(key, out slot));
        Assert.Equal(-1, slot);
        Assert.False(table.TryGetSlot(key, out slot));
        Assert.Equal(-1, slot);
        Assert.False(table.Remove(key, out slot));
        Assert.Equal(-1, slot);
        Assert.Equal(0, table.Count);
    }

    [Fact]
    public void Remove_And_Clear()
    {
        using var table = new DenseKeyTable(130);
        for (ulong k = 0; k < 130; k += 3)
            table.TryAdd(k, out _);
        Assert.Equal(44, table.Count);

        Assert.True(table.Remove(63, out int slot));
        Assert.Equal(63, slot);
        Assert.False(table.Remove(63, out slot));
        Assert.Equal(-1, slot);
        Assert.False(table.Remove(64, out _));
        Assert.Equal(43, table.Count);

        table.Clear();
        Assert.Equal(0, table.Count);
        for (ulong k = 0; k < 130; k++)
            Assert.False(table.TryGetSlot(k, out _));
    }

    [Fact]
    public void Enumerates_Live_Keys_In_Increasing_Order_Across_Words()
    {
        using var table = new DenseKeyTable(300);
        foreach (KeyTableEntry _ in table)
            Assert.Fail("empty table yielded an entry");

        ulong[] keys = [299, 0, 64, 63, 128, 191, 192, 255, 256];
        foreach (ulong key in keys)
            table.TryAdd(key, out _);

        var seen = new List<ulong>();
        var enumerator = table.GetEnumerator();
        while (enumerator.MoveNext())
        {
            Assert.Equal((int)enumerator.Current.Key, enumerator.Current.Slot);
            seen.Add(enumerator.Current.Key);
        }

        Assert.False(enumerator.MoveNext());
        Array.Sort(keys);
        Assert.Equal(keys, seen);
    }

    [Theory]
    [InlineData(3, 1000)]
    [InlineData(11, 64)]
    [InlineData(17, 1)]
    public void Matches_A_Set_Model_Over_Random_Operations(int seed, int maxKeys)
    {
        var rng = new Random(seed);
        using var table = new DenseKeyTable(maxKeys);
        var model = new HashSet<ulong>();
        for (int op = 0; op < 50_000; op++)
        {
            ulong key = (ulong)rng.Next(maxKeys + maxKeys / 4 + 1);
            bool inRange = key < (ulong)maxKeys;
            bool present = model.Contains(key);
            switch (rng.Next(4))
            {
                case 0:
                    Assert.Equal(inRange && !present, table.TryAdd(key, out int s0));
                    Assert.Equal(inRange ? (int)key : -1, s0);
                    if (inRange)
                        model.Add(key);
                    break;
                case 1:
                    Assert.Equal(inRange, table.TryGetOrAdd(key, out int s1));
                    Assert.Equal(inRange ? (int)key : -1, s1);
                    if (inRange)
                        model.Add(key);
                    break;
                case 2:
                    Assert.Equal(present, table.Remove(key, out int s2));
                    Assert.Equal(present ? (int)key : -1, s2);
                    model.Remove(key);
                    break;
                default:
                    Assert.Equal(present, table.TryGetSlot(key, out int s3));
                    Assert.Equal(present ? (int)key : -1, s3);
                    break;
            }

            Assert.Equal(model.Count, table.Count);
        }

        int n = 0;
        foreach (KeyTableEntry entry in table)
        {
            Assert.Contains(entry.Key, model);
            n++;
        }

        Assert.Equal(model.Count, n);
    }

    [Fact]
    public void Dispose_Is_Idempotent_Lookups_Miss_And_Mutations_Throw()
    {
        var table = new DenseKeyTable(100);
        table.TryAdd(5, out _);
        table.Dispose();
        table.Dispose();

        Assert.Equal(0, table.Count);
        Assert.False(table.TryGetSlot(5, out int slot));
        Assert.Equal(-1, slot);
        foreach (KeyTableEntry _ in table)
            Assert.Fail("disposed table yielded an entry");
        Assert.Throws<ObjectDisposedException>(() => table.TryAdd(1, out _));
        Assert.Throws<ObjectDisposedException>(() => table.TryGetOrAdd(1, out _));
        Assert.Throws<ObjectDisposedException>(() => table.Remove(5, out _));
        Assert.Throws<ObjectDisposedException>(() => table.Clear());
    }

    [Fact]
    public void Operations_Do_Not_Allocate()
    {
        using var table = new DenseKeyTable(4096);
        Run(table, 1_000);
        long before = GC.GetAllocatedBytesForCurrentThread();
        Run(table, 100_000);
        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);

        static int Run(DenseKeyTable table, int iterations)
        {
            long sum = 0;
            for (int i = 0; i < iterations; i++)
            {
                ulong key = (ulong)(i * 7 & 4095);
                table.TryGetOrAdd(key, out int s);
                table.TryGetSlot(key, out s);
                sum += s;
                table.Remove(key, out _);
                table.TryAdd(key, out _);
                if ((i & 4095) == 0)
                {
                    foreach (KeyTableEntry entry in table)
                        sum += entry.Slot;
                }
            }

            return (int)sum;
        }
    }
}
