using Tedd.Quicly.Core.Control;

namespace Tedd.Quicly.Core.Tests.Control;

public class ReplayCacheTests
{
    [Fact]
    public void Add_Duplicate_Contains()
    {
        ReplayCache cache = new(8);
        Assert.Equal(8, cache.Capacity);
        Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(1, 2, 100, 0));
        Assert.Equal(ReplayCacheResult.Duplicate, cache.TryAdd(1, 2, 100, 0));
        Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(1, 3, 200, 0));
        Assert.True(cache.Contains(1, 2, 50));
        Assert.False(cache.Contains(2, 2, 50));
        Assert.False(cache.Contains(1, 2, 100)); // expired entries met while probing are deleted
        Assert.Equal(1, cache.Count);
        Assert.True(cache.Contains(1, 3, 100)); // the colliding entry shifted back into the hole is still found
    }

    [Fact]
    public void Full_Until_Something_Expires()
    {
        ReplayCache cache = new(3);
        Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(10, 0, 100, 0));
        Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(20, 0, 300, 0));
        Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(30, 0, 200, 0));
        Assert.Equal(ReplayCacheResult.Full, cache.TryAdd(40, 0, 400, 50));
        Assert.Equal(ReplayCacheResult.Full, cache.TryAdd(40, 0, 400, 99));
        Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(40, 0, 400, 100));
        Assert.Equal(3, cache.Count);
        Assert.Equal(ReplayCacheResult.Duplicate, cache.TryAdd(20, 0, 300, 100));
        Assert.Equal(ReplayCacheResult.Full, cache.TryAdd(50, 0, 500, 150));
        // The sweep recomputed the earliest remaining expiry (200); from then on the next sweep frees an entry.
        Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(50, 0, 500, 250));
    }

    [Fact]
    public void A_Shorter_Lived_Entry_Lowers_The_Sweep_Deadline()
    {
        ReplayCache cache = new(3);
        Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(1, 0, 1000, 0));
        Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(2, 0, 1000, 0));
        Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(3, 0, 50, 0));
        Assert.Equal(ReplayCacheResult.Full, cache.TryAdd(4, 0, 1000, 49));
        Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(4, 0, 1000, 60));
        Assert.False(cache.Contains(3, 0, 60));
    }

    [Fact]
    public void Colliding_Keys_Wrap_And_Survive_Deletion()
    {
        // Capacity 8 => 16 slots; every lo below has home slot 14 or 15, so probe runs wrap around the table end.
        ReplayCache cache = new(8);
        ulong[] keys = [14, 30, 46, 15, 31, 47, 62, 63];
        for (int i = 0; i < keys.Length; i++)
        {
            Assert.Equal(ReplayCacheResult.Added, cache.TryAdd(keys[i], (ulong)i, i % 2 == 0 ? 10 : 1000, 0));
        }

        // Expire the even-indexed entries; lookups of the others must still succeed after backward shifts.
        for (int i = 0; i < keys.Length; i++)
        {
            Assert.Equal(i % 2 != 0, cache.Contains(keys[i], (ulong)i, 10));
        }

        Assert.Equal(4, cache.Count);
        for (int i = 1; i < keys.Length; i += 2)
        {
            Assert.Equal(ReplayCacheResult.Duplicate, cache.TryAdd(keys[i], (ulong)i, 1000, 10));
        }
    }

    [Fact]
    public void Matches_A_Reference_Model_Under_Random_Load()
    {
        Random random = new(42);
        const int capacity = 64;
        ReplayCache cache = new(capacity);
        Dictionary<(ulong, ulong), long> model = [];
        long now = 0;
        for (int step = 0; step < 50_000; step++)
        {
            now += random.Next(0, 3);
            // Few distinct keys so duplicates are common; colliding low bits so probe runs are long.
            ulong lo = (ulong)random.Next(0, 200) << random.Next(0, 3) * 7;
            ulong hi = (ulong)random.Next(0, 2);
            long expiry = now + random.Next(1, 400);

            foreach ((ulong, ulong) key in model.Where(kv => kv.Value <= now).Select(kv => kv.Key).ToList())
            {
                model.Remove(key);
            }

            if (random.Next(4) == 0)
            {
                Assert.Equal(model.ContainsKey((lo, hi)), cache.Contains(lo, hi, now));
                continue;
            }

            ReplayCacheResult expected = model.ContainsKey((lo, hi)) ? ReplayCacheResult.Duplicate
                : model.Count >= capacity ? ReplayCacheResult.Full
                : ReplayCacheResult.Added;
            Assert.Equal(expected, cache.TryAdd(lo, hi, expiry, now));
            if (expected == ReplayCacheResult.Added)
            {
                model[(lo, hi)] = expiry;
            }

            Assert.InRange(cache.Count, model.Count, capacity);
        }
    }

    [Fact]
    public void Constructor_Bounds()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplayCache(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplayCache(ReplayCache.MaxCapacity + 1));
        Assert.Equal(1, new ReplayCache(1).Capacity);
    }
}
