using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Tests.State;

public class KeyTableTests
{
    [Theory]
    [InlineData(0, 0)]
    [InlineData(-1, 0)]
    [InlineData(KeyTable.MaxKeysLimit + 1, 0)]
    [InlineData(10, -1)]
    [InlineData(10, 11)]
    public void Constructor_Rejects_Invalid_Arguments(int maxKeys, int initialKeys)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new KeyTable(maxKeys, initialKeys));
        if (initialKeys == 0)
            Assert.Throws<ArgumentOutOfRangeException>(() => new KeyTable(maxKeys));
    }

    [Theory]
    [InlineData(1, KeyTable.MinCapacity)]
    [InlineData(4, 8)]
    [InlineData(5, 16)]
    [InlineData(1000, 2048)]
    [InlineData(1024, 2048)]
    public void Fixed_Size_Table_Holds_MaxKeys_At_Load_At_Most_Half(int maxKeys, int capacity)
    {
        using var table = new KeyTable(maxKeys);
        Assert.Equal(capacity, table.Capacity);
        Assert.Equal(maxKeys, table.MaxKeys);
        for (int i = 0; i < maxKeys; i++)
            Assert.True(table.TryAdd((ulong)i * 7919, out _));
        Assert.Equal(capacity, table.Capacity);
        Assert.True(table.Count * 2 <= table.Capacity);
    }

    [Fact]
    public void Every_64_Bit_Value_Is_A_Valid_Key()
    {
        using var table = new KeyTable(16);
        ulong[] keys = [0, 1, ulong.MaxValue, ulong.MaxValue - 1, 1UL << 63, 0x8000_0000, uint.MaxValue];
        for (int i = 0; i < keys.Length; i++)
        {
            Assert.False(table.TryGetSlot(keys[i], out int missing));
            Assert.Equal(-1, missing);
            Assert.True(table.TryAdd(keys[i], out int slot));
            Assert.Equal(i, slot);
        }

        for (int i = 0; i < keys.Length; i++)
        {
            Assert.True(table.TryGetSlot(keys[i], out int slot));
            Assert.Equal(i, slot);
        }

        Assert.True(table.Remove(0, out int zeroSlot));
        Assert.Equal(0, zeroSlot);
        Assert.False(table.TryGetSlot(0, out _));
        Assert.True(table.TryGetSlot(ulong.MaxValue, out int maxSlot));
        Assert.Equal(2, maxSlot);
    }

    [Fact]
    public void TryAdd_TryGetOrAdd_And_Remove_Report_Existing_And_Missing_Keys()
    {
        using var table = new KeyTable(8);
        Assert.True(table.TryAdd(42, out int slot));
        Assert.Equal(0, slot);
        Assert.False(table.TryAdd(42, out int existing));
        Assert.Equal(0, existing);
        Assert.True(table.TryGetOrAdd(42, out existing));
        Assert.Equal(0, existing);
        Assert.True(table.TryGetOrAdd(43, out int added));
        Assert.Equal(1, added);
        Assert.Equal(2, table.Count);

        Assert.False(table.Remove(44, out int none));
        Assert.Equal(-1, none);
        Assert.True(table.Remove(42, out int removed));
        Assert.Equal(0, removed);
        Assert.False(table.Remove(42, out _));
        Assert.Equal(1, table.Count);
    }

    [Fact]
    public void MaxKeys_Bounds_The_Table_And_Removal_Makes_Room()
    {
        using var table = new KeyTable(maxKeys: 100, initialKeys: 0);
        for (int i = 0; i < 100; i++)
            Assert.True(table.TryAdd((ulong)i << 20, out _));

        Assert.False(table.TryAdd(12345, out int slot));
        Assert.Equal(-1, slot);
        Assert.False(table.TryGetOrAdd(12345, out slot));
        Assert.Equal(-1, slot);
        // Present keys still resolve at MaxKeys.
        Assert.False(table.TryAdd(5UL << 20, out slot));
        Assert.Equal(5, slot);
        Assert.True(table.TryGetOrAdd(5UL << 20, out slot));
        Assert.Equal(5, slot);
        Assert.Equal(100, table.Count);
        Assert.Equal(256, table.Capacity);

        Assert.True(table.Remove(7UL << 20, out int freed));
        Assert.True(table.TryAdd(12345, out slot));
        Assert.Equal(freed, slot);
        Assert.False(table.TryAdd(12346, out _));
    }

    [Fact]
    public void Growth_Doubles_Up_To_The_MaxKeys_Capacity_And_Keeps_Slots_Stable()
    {
        using var table = new KeyTable(maxKeys: 5000, initialKeys: 0);
        Assert.Equal(KeyTable.MinCapacity, table.Capacity);

        var expected = new Dictionary<ulong, int>();
        int previousCapacity = table.Capacity;
        for (int i = 0; i < 5000; i++)
        {
            ulong key = (ulong)i * 0x9E3779B97F4A7C15UL;
            Assert.True(table.TryAdd(key, out int slot));
            Assert.Equal(i, slot);
            expected[key] = slot;
            Assert.True(table.Count * 2 <= table.Capacity);
            Assert.True(table.Capacity == previousCapacity || table.Capacity == previousCapacity * 2);
            previousCapacity = table.Capacity;
        }

        Assert.Equal(16384, table.Capacity);
        foreach (var (key, slot) in expected)
        {
            Assert.True(table.TryGetSlot(key, out int found));
            Assert.Equal(slot, found);
        }

        Assert.False(table.TryAdd(ulong.MaxValue, out _));
        Assert.Equal(16384, table.Capacity);
    }

    [Fact]
    public void Slots_Are_Dense_Stable_And_Recycled_Most_Recent_First()
    {
        using var table = new KeyTable(64);
        for (ulong k = 0; k < 10; k++)
        {
            Assert.True(table.TryAdd(1000 + k, out int slot));
            Assert.Equal((int)k, slot);
        }

        Assert.True(table.Remove(1003, out int s3));
        Assert.True(table.Remove(1007, out int s7));
        Assert.Equal(3, s3);
        Assert.Equal(7, s7);

        // Recycled slots come back most recently freed first, before any fresh slot.
        Assert.True(table.TryAdd(2000, out int a));
        Assert.True(table.TryAdd(2001, out int b));
        Assert.True(table.TryAdd(2002, out int c));
        Assert.Equal(7, a);
        Assert.Equal(3, b);
        Assert.Equal(10, c);

        // Survivors kept their slots through the churn.
        for (ulong k = 0; k < 10; k++)
        {
            if (k is 3 or 7)
                continue;
            Assert.True(table.TryGetSlot(1000 + k, out int slot));
            Assert.Equal((int)k, slot);
        }
    }

    [Fact]
    public void Clear_Empties_The_Table_And_Restarts_Slot_Allocation()
    {
        using var table = new KeyTable(maxKeys: 1000, initialKeys: 4);
        for (ulong k = 0; k < 500; k++)
            Assert.True(table.TryAdd(k, out _));
        int capacity = table.Capacity;
        table.Remove(10, out _);

        table.Clear();
        Assert.Equal(0, table.Count);
        Assert.Equal(capacity, table.Capacity);
        for (ulong k = 0; k < 500; k++)
            Assert.False(table.TryGetSlot(k, out _));
        foreach (KeyTableEntry _ in table)
            Assert.Fail("cleared table yielded an entry");

        Assert.True(table.TryAdd(77, out int slot));
        Assert.Equal(0, slot);
        Assert.True(table.TryAdd(78, out slot));
        Assert.Equal(1, slot);
    }

    [Fact]
    public void Enumerator_Yields_Every_Live_Pair_Exactly_Once()
    {
        using var table = new KeyTable(maxKeys: 3000, initialKeys: 10);
        foreach (KeyTableEntry _ in table)
            Assert.Fail("empty table yielded an entry");

        var expected = new Dictionary<ulong, int>();
        var rng = new Random(5);
        while (expected.Count < 2000)
        {
            ulong key = (ulong)rng.NextInt64() ^ ((ulong)rng.Next() << 63);
            if (table.TryAdd(key, out int slot))
                expected.Add(key, slot);
        }

        foreach (ulong key in expected.Keys.Take(500).ToArray())
        {
            Assert.True(table.Remove(key, out _));
            expected.Remove(key);
        }

        var seen = new Dictionary<ulong, int>();
        var enumerator = table.GetEnumerator();
        Assert.Equal(default, enumerator.Current);
        while (enumerator.MoveNext())
            seen.Add(enumerator.Current.Key, enumerator.Current.Slot);
        Assert.False(enumerator.MoveNext());
        Assert.Equal(expected.Count, seen.Count);
        foreach (var (key, slot) in expected)
            Assert.Equal(slot, seen[key]);
    }

    [Theory]
    [InlineData(1, 2048, 16, 2560)]
    [InlineData(7, 2048, 2048, 2560)]
    [InlineData(42, 100, 0, 128)]
    [InlineData(99, 4, 0, 8)]
    [InlineData(1234, 4096, 1, 8192)]
    public void Matches_A_Dictionary_Model_Over_100k_Random_Operations(int seed, int maxKeys, int initialKeys, int poolSize)
    {
        var rng = new Random(seed);
        using var table = new KeyTable(maxKeys, initialKeys, (ulong)seed * 0x9E3779B97F4A7C15UL);
        var model = new Dictionary<ulong, int>();
        var slotInUse = new bool[maxKeys];
        int maxCapacity = (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)Math.Max(2 * maxKeys, KeyTable.MinCapacity));

        // Candidate keys: small integers, strided, high-bit-only, extremes and random 64-bit values.
        var poolSet = new HashSet<ulong> { 0, ulong.MaxValue, 1UL << 63 };
        while (poolSet.Count < poolSize)
        {
            ulong k = (ulong)poolSet.Count;
            poolSet.Add((rng.Next(5)) switch
            {
                0 => k,
                1 => k << 10,
                2 => k << 40,
                3 => ~k,
                _ => (ulong)rng.NextInt64() ^ ((ulong)rng.Next(2) << 63),
            });
        }

        ulong[] pool = poolSet.ToArray();
        for (int op = 0; op < 100_000; op++)
        {
            ulong key = pool[rng.Next(pool.Length)];
            // Alternate add-heavy and remove-heavy phases so the table repeatedly fills to MaxKeys and drains.
            bool addPhase = (op / 5_000 & 1) == 0;
            int roll = rng.Next(10);
            int kind = roll < 2 ? 3 : roll < 6 ? (addPhase ? 0 : 2) : roll < 8 ? 1 : (addPhase ? 2 : 0);
            bool present = model.TryGetValue(key, out int modelSlot);
            switch (kind)
            {
                case 0:
                {
                    bool added = table.TryAdd(key, out int slot);
                    if (present)
                    {
                        Assert.False(added);
                        Assert.Equal(modelSlot, slot);
                    }
                    else if (model.Count == maxKeys)
                    {
                        Assert.False(added);
                        Assert.Equal(-1, slot);
                    }
                    else
                    {
                        Assert.True(added);
                        AcceptNewSlot(key, slot);
                    }

                    break;
                }

                case 1:
                {
                    bool ok = table.TryGetOrAdd(key, out int slot);
                    if (present)
                    {
                        Assert.True(ok);
                        Assert.Equal(modelSlot, slot);
                    }
                    else if (model.Count == maxKeys)
                    {
                        Assert.False(ok);
                        Assert.Equal(-1, slot);
                    }
                    else
                    {
                        Assert.True(ok);
                        AcceptNewSlot(key, slot);
                    }

                    break;
                }

                case 2:
                {
                    bool removed = table.Remove(key, out int slot);
                    Assert.Equal(present, removed);
                    Assert.Equal(present ? modelSlot : -1, slot);
                    if (present)
                    {
                        model.Remove(key);
                        slotInUse[slot] = false;
                    }

                    break;
                }

                default:
                {
                    bool found = table.TryGetSlot(key, out int slot);
                    Assert.Equal(present, found);
                    Assert.Equal(present ? modelSlot : -1, slot);
                    break;
                }
            }

            Assert.Equal(model.Count, table.Count);
            if (op % 5_000 == 4_999)
                AssertSameContents();
        }

        AssertSameContents();
        Assert.True(table.Capacity <= maxCapacity);

        void AcceptNewSlot(ulong key, int slot)
        {
            Assert.InRange(slot, 0, maxKeys - 1);
            Assert.False(slotInUse[slot], "slot handed out twice");
            slotInUse[slot] = true;
            model.Add(key, slot);
        }

        void AssertSameContents()
        {
            int n = 0;
            foreach (KeyTableEntry entry in table)
            {
                Assert.True(model.TryGetValue(entry.Key, out int slot));
                Assert.Equal(slot, entry.Slot);
                n++;
            }

            Assert.Equal(model.Count, n);
            foreach (var (key, slot) in model)
            {
                Assert.True(table.TryGetSlot(key, out int found));
                Assert.Equal(slot, found);
            }

            Assert.True(table.Count * 2 <= table.Capacity);
        }
    }

    [Fact]
    public void Backward_Shift_Deletion_Keeps_Wrapping_Probe_Runs_Intact()
    {
        // Capacity 16 with seed 0: pick keys by home bucket so the runs are known.
        const int Mask = 15;
        using var table = new KeyTable(maxKeys: 8, initialKeys: 8, seed: 0);
        Assert.Equal(16, table.Capacity);
        ulong[] home14 = KeysWithHome(14, 3, Mask);
        ulong[] home15 = KeysWithHome(15, 1, Mask);
        ulong[] home0 = KeysWithHome(0, 1, Mask);
        ulong[] home2 = KeysWithHome(2, 1, Mask);

        // Buckets: 14:A 15:B 0:C (home 14, wrapped) 1:D (home 15) 2:E (home 0) 3:F (home 2, displaced by E).
        ulong a = home14[0], b = home14[1], c = home14[2], d = home15[0], e = home0[0], f = home2[0];
        foreach (ulong key in new[] { a, b, c, d, e, f })
            Assert.True(table.TryAdd(key, out _));
        Assert.Equal(1, table.GetProbeLength(a));
        Assert.Equal(2, table.GetProbeLength(b));
        Assert.Equal(3, table.GetProbeLength(c));
        Assert.Equal(3, table.GetProbeLength(d));
        Assert.Equal(3, table.GetProbeLength(e));
        Assert.Equal(2, table.GetProbeLength(f));
        Assert.Equal(3, table.MaxProbeLength());

        // Remove A: B, C, D and E each move back one bucket; F (home 2) moves back into its home bucket.
        Assert.True(table.Remove(a, out _));
        Assert.Equal(1, table.GetProbeLength(b));
        Assert.Equal(2, table.GetProbeLength(c));
        Assert.Equal(2, table.GetProbeLength(d));
        Assert.Equal(2, table.GetProbeLength(e));
        Assert.Equal(1, table.GetProbeLength(f));
        Assert.Equal(-1, table.GetProbeLength(a));

        // Remove D (the middle of the wrapped run): E shifts, F stays at home.
        Assert.True(table.Remove(d, out _));
        Assert.Equal(1, table.GetProbeLength(e));
        Assert.Equal(1, table.GetProbeLength(f));
        Assert.Equal(2, table.GetProbeLength(c));

        foreach (ulong key in new[] { b, c, e, f })
            Assert.True(table.TryGetSlot(key, out _));
        Assert.False(table.TryGetSlot(a, out _));
        Assert.False(table.TryGetSlot(d, out _));
        Assert.Equal(4, table.Count);

        static ulong[] KeysWithHome(int home, int count, int mask)
        {
            var keys = new ulong[count];
            int n = 0;
            for (ulong k = 1; n < count; k++)
            {
                if (((int)KeyHash.Fmix64(k) & mask) == home)
                    keys[n++] = k;
            }

            return keys;
        }
    }

    [Theory]
    [InlineData(0)] // sequential
    [InlineData(1)] // stride 1024 (an identity hash would put them all in 32 buckets)
    [InlineData(2)] // only the high 24 bits vary (an identity hash would put them all in bucket 0)
    [InlineData(3)] // 16 dense clusters of 2048 far apart
    public void Probe_Lengths_Stay_Short_For_Clustered_Keys(int pattern)
    {
        const int N = 32_768;
        for (ulong seed = 1; seed <= 3; seed++)
        {
            using var table = new KeyTable(N, N, seed);
            for (int i = 0; i < N; i++)
            {
                ulong key = pattern switch
                {
                    0 => (ulong)i,
                    1 => (ulong)i << 10,
                    2 => (ulong)i << 40,
                    _ => ((ulong)(i >> 11) << 48) | (uint)(i & 2047),
                };
                Assert.True(table.TryAdd(key, out _));
            }

            Assert.Equal(65_536, table.Capacity);
            long total = 0;
            foreach (KeyTableEntry entry in table)
                total += table.GetProbeLength(entry.Key);
            double average = (double)total / N;
            // Linear probing at load 0.5 with a good hash: ~1.5 probes per hit, longest run in the tens.
            Assert.InRange(average, 1.0, 2.0);
            Assert.InRange(table.MaxProbeLength(), 1, 100);
        }
    }

    [Fact]
    public void Tables_Get_Independent_Random_Seeds()
    {
        // With fixed-capacity tables and identical inserts, bucket order (enumeration order) reveals the seed.
        using var a = new KeyTable(1024);
        using var b = new KeyTable(1024);
        for (ulong k = 0; k < 1024; k++)
        {
            a.TryAdd(k, out _);
            b.TryAdd(k, out _);
        }

        var orderA = new List<ulong>();
        var orderB = new List<ulong>();
        foreach (KeyTableEntry entry in a)
            orderA.Add(entry.Key);
        foreach (KeyTableEntry entry in b)
            orderB.Add(entry.Key);
        Assert.NotEqual(orderA, orderB);
    }

    [Fact]
    public void Dispose_Is_Idempotent_Lookups_Miss_And_Mutations_Throw()
    {
        var table = new KeyTable(16);
        table.TryAdd(5, out _);
        table.Dispose();
        table.Dispose();

        Assert.Equal(0, table.Count);
        Assert.False(table.TryGetSlot(5, out int slot));
        Assert.Equal(-1, slot);
        Assert.False(table.TryGetSlot(ulong.MaxValue, out _));
        foreach (KeyTableEntry _ in table)
            Assert.Fail("disposed table yielded an entry");
        Assert.Throws<ObjectDisposedException>(() => table.TryAdd(1, out _));
        Assert.Throws<ObjectDisposedException>(() => table.TryGetOrAdd(1, out _));
        Assert.Throws<ObjectDisposedException>(() => table.Remove(5, out _));
        Assert.Throws<ObjectDisposedException>(() => table.Clear());
    }

    [Fact]
    public void Lookups_Churn_And_Enumeration_Do_Not_Allocate()
    {
        using var table = new KeyTable(1024);
        for (ulong k = 0; k < 512; k++)
            table.TryAdd(k * 31, out _);
        Run(table, 1_000);

        WindowedAllocation.AssertNone(() => Run(table, 20_000));

        static int Run(KeyTable table, int iterations)
        {
            int sum = 0;
            for (int i = 0; i < iterations; i++)
            {
                ulong hit = (ulong)(i & 511) * 31;
                table.TryGetSlot(hit, out int s);
                sum += s;
                table.TryGetSlot(hit + 1, out s);
                sum += s;
                table.Remove(hit, out _);
                table.TryAdd(hit, out _);
                table.TryGetOrAdd(hit, out s);
                sum += s;
                if ((i & 1023) == 0)
                {
                    foreach (KeyTableEntry entry in table)
                        sum += entry.Slot;
                }
            }

            return sum;
        }
    }

    [Fact]
    public void Both_Key_Tables_Behave_Alike_Through_The_Interface()
    {
        using var hashed = new KeyTable(64);
        using var dense = new DenseKeyTable(64);
        Exercise(hashed);
        Exercise(dense);

        static void Exercise<T>(T table) where T : IKeyTable
        {
            Assert.Equal(64, table.MaxKeys);
            Assert.True(table.TryAdd(3, out int s3));
            Assert.True(table.TryGetOrAdd(9, out int s9));
            Assert.NotEqual(s3, s9);
            Assert.Equal(2, table.Count);
            Assert.True(table.TryGetSlot(3, out int found));
            Assert.Equal(s3, found);
            Assert.True(table.Remove(3, out int removed));
            Assert.Equal(s3, removed);
            Assert.False(table.TryGetSlot(3, out _));
            table.Clear();
            Assert.Equal(0, table.Count);
            Assert.False(table.TryGetSlot(9, out _));
        }
    }
}
