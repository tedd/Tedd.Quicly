using System.Diagnostics;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Memory;

namespace Tedd.Quicly.Core.Tests.Memory;

public unsafe class SlabAllocatorTests
{
    private static SlabAllocatorOptions SmallOptions(bool validate = true) => new()
    {
        SizeClasses = [new(64, 8), new(256, 4), new(1536, 2), new(4096, 1)],
        ValidateLeases = validate,
    };

    [Fact]
    public void Default_Construction_Uses_Default_Options()
    {
        using var allocator = new SlabAllocator();
        Assert.Equal(7, allocator.ClassCount);
        Assert.Equal(262144, allocator.MaxBlockSize);
        Assert.Equal(16L * 1024 * 1024, allocator.GetStatistics().TotalCapacityBytes);
        Assert.Equal(new SlabAllocatorOptions().ValidateLeases, allocator.ValidateLeases);
    }

    [Fact]
    public void Rents_From_Every_Default_Class_With_Exact_Fit_And_Rounding()
    {
        using var allocator = new SlabAllocator();
        ReadOnlySpan<SizeClassDefinition> classes = allocator.SizeClasses;
        for (int ci = 0; ci < classes.Length; ci++)
        {
            int size = classes[ci].BlockSize;

            // Exact fit lands in the class itself.
            Assert.True(allocator.TryRent(size, out BufferLease exact));
            Assert.Equal(ci, exact.ClassIndex);
            Assert.Equal(size, exact.Length);
            Assert.Equal(size, allocator.GetSpan(exact).Length);

            // One byte more rounds up to the next class (or fails on the last).
            bool rented = allocator.TryRent(size + 1, out BufferLease larger);
            if (ci + 1 < classes.Length)
            {
                Assert.True(rented);
                Assert.Equal(ci + 1, larger.ClassIndex);
                Assert.Equal(classes[ci + 1].BlockSize, larger.Length);
                allocator.Return(larger);
            }
            else
            {
                Assert.False(rented);
                Assert.True(larger.IsEmpty);
            }

            // One byte less stays in the class (or the previous class for the first).
            int smaller = size - 1;
            Assert.True(allocator.TryRent(smaller, out BufferLease less));
            Assert.Equal(ci == 0 ? 0 : (classes[ci - 1].BlockSize >= smaller ? ci - 1 : ci), less.ClassIndex);
            allocator.Return(less);
            allocator.Return(exact);
        }

        SlabStatistics stats = allocator.GetStatistics();
        Assert.Equal(0, stats.TotalRentedBytes);
        Assert.Equal(0, stats.TotalExhaustions);
    }

    [Fact]
    public void Zero_Length_Rents_Smallest_Class()
    {
        using var allocator = new SlabAllocator(SmallOptions());
        Assert.True(allocator.TryRent(0, out BufferLease lease));
        Assert.Equal(0, lease.ClassIndex);
        Assert.Equal(64, lease.Length);
        allocator.Return(lease);
    }

    [Fact]
    public void Negative_Length_Throws()
    {
        using var allocator = new SlabAllocator(SmallOptions());
        Assert.Throws<ArgumentOutOfRangeException>(() => allocator.TryRent(-1, out _));
    }

    [Fact]
    public void Too_Large_Returns_False_Without_Counting_Exhaustion()
    {
        using var allocator = new SlabAllocator(SmallOptions());
        Assert.False(allocator.TryRent(allocator.MaxBlockSize + 1, out BufferLease lease));
        Assert.True(lease.IsEmpty);
        Assert.Equal(0, allocator.GetStatistics().TotalExhaustions);
    }

    [Fact]
    public void Exhaustion_Is_Counted_And_Recovers_After_Return()
    {
        using var allocator = new SlabAllocator(SmallOptions());
        var leases = new BufferLease[4];
        for (int i = 0; i < 4; i++)
            Assert.True(allocator.TryRent(200, out leases[i]));

        Assert.False(allocator.TryRent(200, out BufferLease failed));
        Assert.True(failed.IsEmpty);
        Assert.False(allocator.TryRent(200, out _));

        SizeClassStatistics stats = allocator.GetClassStatistics(1);
        Assert.Equal(4, stats.Rented);
        Assert.Equal(4, stats.Peak);
        Assert.Equal(0, stats.Free);
        Assert.Equal(2, stats.Exhaustions);

        // Other classes unaffected.
        Assert.True(allocator.TryRent(1, out BufferLease small));
        Assert.Equal(0, allocator.GetClassStatistics(0).Exhaustions);
        allocator.Return(small);

        allocator.Return(leases[2]);
        Assert.True(allocator.TryRent(200, out BufferLease recovered));
        Assert.Equal(leases[2].BlockIndex, recovered.BlockIndex);
        Assert.Equal((ushort)(leases[2].Generation + 1), recovered.Generation);

        allocator.Return(recovered);
        allocator.Return(leases[0]);
        allocator.Return(leases[1]);
        allocator.Return(leases[3]);

        stats = allocator.GetClassStatistics(1);
        Assert.Equal(0, stats.Rented);
        Assert.Equal(4, stats.Peak);
        Assert.Equal(4, stats.Capacity);
        Assert.Equal(256, stats.BlockSize);
        Assert.Equal(2, stats.Exhaustions);
    }

    [Fact]
    public void Blocks_Are_64_Byte_Aligned_And_Distinct()
    {
        using var allocator = new SlabAllocator();
        ReadOnlySpan<SizeClassDefinition> classes = allocator.SizeClasses;
        for (int ci = 0; ci < classes.Length; ci++)
        {
            int count = Math.Min(classes[ci].BlockCount, 16);
            var leases = new BufferLease[count];
            var pointers = new HashSet<nuint>();
            for (int i = 0; i < count; i++)
            {
                Assert.True(allocator.TryRent(classes[ci].BlockSize, out leases[i]));
                byte* p = allocator.GetPointer(leases[i]);
                Assert.Equal(0u, (nuint)p % 64);
                Assert.True(pointers.Add((nuint)p));
                fixed (byte* s = allocator.GetSpan(leases[i]))
                    Assert.True(s == p);
            }

            // Adjacent blocks in one slab do not overlap: distance between any two is a multiple of block size.
            for (int i = 1; i < count; i++)
            {
                long distance = Math.Abs((long)allocator.GetPointer(leases[i]) - (long)allocator.GetPointer(leases[0]));
                Assert.Equal(0, distance % classes[ci].BlockSize);
                Assert.True(distance >= classes[ci].BlockSize);
            }

            for (int i = 0; i < count; i++)
                allocator.Return(leases[i]);
        }
    }

    [Fact]
    public void GetSpan_Covers_Exactly_The_Block_And_Data_Survives_Round_Trip()
    {
        using var allocator = new SlabAllocator(SmallOptions());
        Assert.True(allocator.TryRent(1000, out BufferLease a));
        Assert.True(allocator.TryRent(1000, out BufferLease b));
        Span<byte> spanA = allocator.GetSpan(a);
        Span<byte> spanB = allocator.GetSpan(b);
        Assert.Equal(1536, spanA.Length);
        Assert.Equal(1536, spanB.Length);
        spanA.Fill(0xAA);
        spanB.Fill(0x55);
        Assert.All(allocator.GetSpan(a).ToArray(), x => Assert.Equal(0xAA, x));
        Assert.All(allocator.GetSpan(b).ToArray(), x => Assert.Equal(0x55, x));
        Assert.True(allocator.GetPointer(a)[1535] == 0xAA);
        allocator.Return(a);
        allocator.Return(b);
    }

    [Fact]
    public void Lease_Validation_Rejects_Bounds_Violations()
    {
        using var allocator = new SlabAllocator(SmallOptions());
        Assert.True(allocator.TryRent(64, out BufferLease good));

        BufferLease empty = BufferLease.Empty;
        var ex = Assert.Throws<ArgumentException>(() => allocator.GetSpan(empty));
        Assert.Contains("empty", ex.Message, StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() => allocator.GetPointer(empty));
        Assert.Throws<ArgumentException>(() => allocator.Return(empty));
        Assert.Throws<ArgumentException>(() => allocator.ValidateBounds(empty));

        BufferLease badClass = Forge(classIndex: 9, generation: good.Generation, blockIndex: 0, offset: 0, length: 64);
        ex = Assert.Throws<ArgumentException>(() => allocator.GetSpan(badClass));
        Assert.Contains("does not address", ex.Message, StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() => allocator.Return(badClass));

        BufferLease badBlock = Forge(0, good.Generation, blockIndex: 8, offset: 8 * 64, length: 64);
        Assert.Throws<ArgumentException>(() => allocator.GetSpan(badBlock));

        BufferLease badLength = Forge(0, good.Generation, blockIndex: good.BlockIndex, offset: good.Offset, length: 256);
        Assert.Throws<ArgumentException>(() => allocator.GetSpan(badLength));

        BufferLease badOffset = Forge(0, good.Generation, blockIndex: good.BlockIndex, offset: 8 * 64, length: 64);
        Assert.Throws<ArgumentException>(() => allocator.GetPointer(badOffset));

        BufferLease negativeBlock = Forge(0, good.Generation, blockIndex: -1, offset: 0, length: 64);
        Assert.Throws<ArgumentException>(() => allocator.ValidateBounds(negativeBlock));

        BufferLease badShard = Forge(0, good.Generation, good.BlockIndex, good.Offset, 64, shard: allocator.ShardCount);
        Assert.Throws<ArgumentException>(() => allocator.Return(badShard));
        Assert.Throws<ArgumentException>(() => allocator.GetSpan(badShard));

        allocator.ValidateBounds(good);
        allocator.Return(good);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(8)]
    [InlineData(64)]
    public void Shard_Count_Is_Honoured_And_Every_Block_Is_Reachable(int shards)
    {
        var options = new SlabAllocatorOptions
        {
            SizeClasses = [new(64, 5), new(256, 100)],
            FreeListShards = shards,
            ValidateLeases = true,
        };
        using var allocator = new SlabAllocator(options);
        Assert.Equal(shards, allocator.ShardCount);

        // Rent everything: shards with fewer blocks than others (or none at all) must not hide capacity.
        for (int ci = 0; ci < 2; ci++)
        {
            int capacity = options.SizeClasses[ci].BlockCount;
            var all = new BufferLease[capacity];
            var blocks = new HashSet<int>();
            for (int i = 0; i < capacity; i++)
            {
                Assert.True(allocator.TryRent(options.SizeClasses[ci].BlockSize, out all[i]));
                Assert.True(all[i].Shard < shards);
                Assert.True(blocks.Add(all[i].BlockIndex));
            }

            Assert.False(allocator.TryRent(options.SizeClasses[ci].BlockSize, out _));
            SizeClassStatistics full = allocator.GetClassStatistics(ci);
            Assert.Equal(capacity, full.Rented);
            Assert.Equal(capacity, full.Peak);
            Assert.Equal(1, full.Exhaustions);

            for (int i = 0; i < capacity; i++)
                allocator.Return(all[i]);
            Assert.Equal(0, allocator.GetClassStatistics(ci).Rented);

            // A returned block goes back to its own shard and is re-rented with a bumped generation.
            Assert.True(allocator.TryRent(options.SizeClasses[ci].BlockSize, out BufferLease again));
            BufferLease original = Array.Find(all, l => l.BlockIndex == again.BlockIndex);
            Assert.Equal(original.Shard, again.Shard);
            Assert.Equal((ushort)(original.Generation + 1), again.Generation);
            allocator.Return(again);
        }
    }

    [Fact]
    public void Threads_Prefer_Distinct_Shards()
    {
        using var allocator = new SlabAllocator(new SlabAllocatorOptions { SizeClasses = [new(64, 64)], FreeListShards = 8 });
        var shards = new int[8];
        var threads = new Thread[8];
        var leases = new BufferLease[8];
        using var go = new ManualResetEventSlim(false);
        for (int t = 0; t < threads.Length; t++)
        {
            int id = t;
            threads[t] = new Thread(() =>
            {
                go.Wait();
                Assert.True(allocator.TryRent(1, out leases[id]));
                Interlocked.Increment(ref shards[leases[id].Shard]);
            });
            threads[t].Start();
        }
        go.Set();
        foreach (Thread thread in threads)
            thread.Join();

        // Round-robin assignment: eight fresh threads land on eight different shards, unless a thread of
        // another test class (they run in parallel) took a slot in the same instant; tolerate a couple.
        int distinct = shards.Count(count => count > 0);
        Assert.True(distinct >= 6, $"only {distinct} distinct shards: [{string.Join(", ", shards)}]");
        foreach (BufferLease lease in leases)
            allocator.Return(lease);
    }

    [Fact]
    public void Validation_Detects_Double_Return()
    {
        using var allocator = new SlabAllocator(SmallOptions(validate: true));
        Assert.True(allocator.ValidateLeases);
        Assert.True(allocator.TryRent(64, out BufferLease lease));
        allocator.Return(lease);
        var ex = Assert.Throws<InvalidOperationException>(() => allocator.Return(lease));
        Assert.Contains("returned twice", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, allocator.GetClassStatistics(0).Rented);
    }

    [Fact]
    public void Validation_Detects_Stale_Lease()
    {
        using var allocator = new SlabAllocator(SmallOptions(validate: true));
        Assert.True(allocator.TryRent(64, out BufferLease first));
        allocator.Return(first);
        Assert.True(allocator.TryRent(64, out BufferLease second));
        Assert.Equal(first.BlockIndex, second.BlockIndex);

        var ex = Assert.Throws<InvalidOperationException>(() => allocator.Return(first));
        Assert.Contains("stale", ex.Message, StringComparison.Ordinal);
        Assert.Equal(1, allocator.GetClassStatistics(0).Rented);
        allocator.Return(second);
        Assert.Equal(0, allocator.GetClassStatistics(0).Rented);
    }

    [Fact]
    public void Validation_Disabled_Skips_Ownership_Checks()
    {
        using var allocator = new SlabAllocator(SmallOptions(validate: false));
        Assert.False(allocator.ValidateLeases);
        Assert.True(allocator.TryRent(64, out BufferLease lease));
        Assert.Equal(1, lease.Generation);
        allocator.Return(lease);
        Assert.True(allocator.TryRent(64, out BufferLease again));
        Assert.Equal(2, again.Generation);
        allocator.Return(again);
        Assert.Equal(0, allocator.GetClassStatistics(0).Rented);
    }

    [Fact]
    public void Generation_Wraps_At_16_Bits()
    {
        using var allocator = new SlabAllocator(new SlabAllocatorOptions { SizeClasses = [new(64, 1)], ValidateLeases = true });
        BufferLease lease = default;
        for (int i = 0; i < 70_000; i++)
        {
            Assert.True(allocator.TryRent(1, out lease));
            allocator.Return(lease);
        }

        Assert.Equal(unchecked((ushort)70_000), lease.Generation);
    }

    [Fact]
    public void Statistics_Track_Rented_Peak_And_Exhaustions_Per_Class()
    {
        using var allocator = new SlabAllocator(SmallOptions());
        SlabStatistics before = allocator.GetStatistics();
        Assert.Equal(4, before.ClassCount);
        Assert.Equal(64L * 8 + 256L * 4 + 1536L * 2 + 4096L, before.TotalCapacityBytes);
        Assert.Equal(0, before.TotalRentedBytes);
        for (int ci = 0; ci < before.ClassCount; ci++)
        {
            Assert.Equal(0, before[ci].Rented);
            Assert.Equal(0, before[ci].Peak);
            Assert.Equal(0, before[ci].Exhaustions);
            Assert.Equal(before[ci].Capacity, before[ci].Free);
        }

        Assert.True(allocator.TryRent(64, out BufferLease a));
        Assert.True(allocator.TryRent(64, out BufferLease b));
        Assert.True(allocator.TryRent(64, out BufferLease c));
        allocator.Return(b);
        Assert.True(allocator.TryRent(4096, out BufferLease big));
        Assert.False(allocator.TryRent(4096, out _));

        SlabStatistics during = allocator.GetStatistics();
        Assert.Equal(2, during[0].Rented);
        Assert.Equal(3, during[0].Peak);
        Assert.Equal(6, during[0].Free);
        Assert.Equal(0, during[0].Exhaustions);
        Assert.Equal(1, during[3].Rented);
        Assert.Equal(1, during[3].Peak);
        Assert.Equal(1, during[3].Exhaustions);
        Assert.Equal(2 * 64L + 4096, during.TotalRentedBytes);
        Assert.Equal(1, during.TotalExhaustions);
        Assert.Equal(during[0], allocator.GetClassStatistics(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => during[4]);
        Assert.Throws<ArgumentOutOfRangeException>(() => during[-1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => allocator.GetClassStatistics(4));
        Assert.Throws<ArgumentOutOfRangeException>(() => allocator.GetClassStatistics(-1));

        allocator.Return(a);
        allocator.Return(c);
        allocator.Return(big);
        SlabStatistics after = allocator.GetStatistics();
        Assert.Equal(0, after.TotalRentedBytes);
        Assert.Equal(3, after[0].Peak);
        Assert.Equal(1, after[3].Peak);
    }

    [Fact]
    public void Dispose_Is_Idempotent_And_Later_Use_Throws()
    {
        var allocator = new SlabAllocator(SmallOptions());
        Assert.True(allocator.TryRent(64, out BufferLease lease));
        allocator.Dispose();
        allocator.Dispose();

        Assert.Throws<ObjectDisposedException>(() => allocator.TryRent(64, out _));
        Assert.Throws<ObjectDisposedException>(() => allocator.TryRent(1 << 20, out _));
        Assert.Throws<ObjectDisposedException>(() => allocator.Return(lease));
        Assert.Throws<ObjectDisposedException>(() => allocator.GetSpan(lease));
        Assert.Throws<ObjectDisposedException>(() => allocator.GetPointer(lease));
        Assert.Throws<ObjectDisposedException>(() => allocator.ValidateBounds(lease));
        Assert.Throws<ObjectDisposedException>(() => allocator.GetStatistics());
        Assert.Throws<ObjectDisposedException>(() => allocator.GetClassStatistics(0));

        // Metadata that does not touch native memory remains readable.
        Assert.Equal(4, allocator.ClassCount);
        Assert.Equal(4096, allocator.MaxBlockSize);
    }

    [Fact]
    public void Finalizer_Frees_Undisposed_Allocator()
    {
        CreateAndDrop();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void CreateAndDrop()
    {
        var allocator = new SlabAllocator(SmallOptions());
        Assert.True(allocator.TryRent(64, out _));
    }

    [Fact]
    public void Rent_Return_Loop_Does_Not_Allocate()
    {
        using var allocator = new SlabAllocator(SmallOptions(validate: true));
        for (int i = 0; i < 1_000; i++)
            RentWriteReturn(allocator);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100_000; i++)
            RentWriteReturn(allocator);
        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);

        // Statistics snapshots are allocation-free too (asserting inside the loop would allocate in xunit).
        long rentedSum = 0;
        long capacitySum = 0;
        before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 10_000; i++)
        {
            SlabStatistics stats = allocator.GetStatistics();
            rentedSum += stats[0].Rented;
            SizeClassStatistics one = allocator.GetClassStatistics(1);
            capacitySum += one.Capacity;
        }
        after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);
        Assert.Equal(0, rentedSum);
        Assert.Equal(40_000, capacitySum);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void RentWriteReturn(SlabAllocator allocator)
    {
        Assert.True(allocator.TryRent(1500, out BufferLease lease));
        Span<byte> span = allocator.GetSpan(in lease);
        span[0] = 1;
        span[^1] = 2;
        allocator.GetPointer(in lease)[1] = 3;
        allocator.Return(in lease);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public void Sixteen_Threads_Stress_For_Two_Seconds_Never_Hand_Out_A_Block_Twice(int shards)
    {
        const int threadCount = 16;
        var options = new SlabAllocatorOptions
        {
            SizeClasses = [new(64, 24), new(256, 12), new(1536, 6), new(4096, 3)],
            FreeListShards = shards,
            ValidateLeases = true,
        };
        using var allocator = new SlabAllocator(options);
        int[][] owners = new int[options.SizeClasses.Length][];
        for (int ci = 0; ci < owners.Length; ci++)
            owners[ci] = new int[options.SizeClasses[ci].BlockCount];

        int[] requestSizes = [1, 64, 65, 200, 256, 300, 1500, 1536, 2000, 4096];
        long violations = 0;
        long dataCorruptions = 0;
        long rents = 0;
        long exhaustions = 0;
        Exception? failure = null;
        var start = new ManualResetEventSlim(false);
        var stopwatch = new Stopwatch();

        var threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++)
        {
            int threadId = t + 1;
            threads[t] = new Thread(() =>
            {
                try
                {
                    var rng = new Random(threadId * 7919);
                    var held = new BufferLease[4];
                    start.Wait();
                    while (stopwatch.ElapsedMilliseconds < 2_000)
                    {
                        int inFlight = 0;
                        int batch = 1 + rng.Next(held.Length);
                        for (int i = 0; i < batch; i++)
                        {
                            int size = requestSizes[rng.Next(requestSizes.Length)];
                            if (!allocator.TryRent(size, out BufferLease lease))
                            {
                                Interlocked.Increment(ref exhaustions);
                                continue;
                            }

                            Interlocked.Increment(ref rents);
                            if (Interlocked.CompareExchange(ref owners[lease.ClassIndex][lease.BlockIndex], threadId, 0) != 0)
                                Interlocked.Increment(ref violations);

                            Span<byte> span = allocator.GetSpan(lease);
                            for (int k = 0; k < span.Length; k++)
                                span[k] = (byte)(threadId + k);
                            held[inFlight++] = lease;
                        }

                        Thread.SpinWait(rng.Next(50));

                        for (int i = 0; i < inFlight; i++)
                        {
                            BufferLease lease = held[i];
                            Span<byte> span = allocator.GetSpan(lease);
                            for (int k = 0; k < span.Length; k++)
                            {
                                if (span[k] != (byte)(threadId + k))
                                {
                                    Interlocked.Increment(ref dataCorruptions);
                                    break;
                                }
                            }

                            if (Interlocked.CompareExchange(ref owners[lease.ClassIndex][lease.BlockIndex], 0, threadId) != threadId)
                                Interlocked.Increment(ref violations);
                            allocator.Return(lease);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.CompareExchange(ref failure, ex, null);
                }
            })
            { IsBackground = true, Name = $"slab-stress-{threadId}" };
            threads[t].Start();
        }

        stopwatch.Start();
        start.Set();
        foreach (Thread thread in threads)
            Assert.True(thread.Join(TimeSpan.FromSeconds(30)));

        Assert.Null(failure);
        Assert.Equal(0, Interlocked.Read(ref violations));
        Assert.Equal(0, Interlocked.Read(ref dataCorruptions));
        Assert.True(Interlocked.Read(ref rents) > 10_000, $"only {rents} rents in 2 s");

        SlabStatistics stats = allocator.GetStatistics();
        long countedExhaustions = 0;
        for (int ci = 0; ci < stats.ClassCount; ci++)
        {
            Assert.Equal(0, stats[ci].Rented);
            Assert.True(stats[ci].Peak <= stats[ci].Capacity);
            Assert.True(stats[ci].Peak > 0);
            countedExhaustions += stats[ci].Exhaustions;
            for (int b = 0; b < owners[ci].Length; b++)
                Assert.Equal(0, owners[ci][b]);
        }
        Assert.Equal(Interlocked.Read(ref exhaustions), countedExhaustions);

        // Every block is rentable again after the storm.
        for (int ci = 0; ci < stats.ClassCount; ci++)
        {
            var all = new BufferLease[stats[ci].Capacity];
            for (int i = 0; i < all.Length; i++)
                Assert.True(allocator.TryRent(stats[ci].BlockSize, out all[i]));
            Assert.False(allocator.TryRent(stats[ci].BlockSize, out _));
            for (int i = 0; i < all.Length; i++)
                allocator.Return(all[i]);
        }
    }

    [Fact]
    public void Concurrent_Peak_Never_Exceeds_Capacity_And_Reaches_It_Under_Pressure()
    {
        using var allocator = new SlabAllocator(new SlabAllocatorOptions { SizeClasses = [new(64, 32)], ValidateLeases = false });
        Parallel.For(0, 64, new ParallelOptions { MaxDegreeOfParallelism = 16 }, _ =>
        {
            var held = new BufferLease[8];
            for (int round = 0; round < 2_000; round++)
            {
                int n = 0;
                for (int i = 0; i < held.Length; i++)
                {
                    if (allocator.TryRent(1, out held[n]))
                        n++;
                }
                for (int i = 0; i < n; i++)
                    allocator.Return(held[i]);
            }
        });

        SizeClassStatistics stats = allocator.GetClassStatistics(0);
        Assert.Equal(0, stats.Rented);
        Assert.True(stats.Peak <= 32);
        Assert.True(stats.Peak >= 8);
    }

    private static BufferLease Forge(int classIndex, ushort generation, int blockIndex, int offset, int length, int shard = 0)
    {
        // Same layout as BufferLease: class (1) + shard (1) + generation (2) + block (4) + offset (4) + length (4).
        Span<byte> bytes = stackalloc byte[16];
        bytes[0] = (byte)classIndex;
        bytes[1] = (byte)shard;
        BitConverter.TryWriteBytes(bytes[2..], generation);
        BitConverter.TryWriteBytes(bytes[4..], blockIndex);
        BitConverter.TryWriteBytes(bytes[8..], offset);
        BitConverter.TryWriteBytes(bytes[12..], length);
        return Unsafe.ReadUnaligned<BufferLease>(ref bytes[0]);
    }
}
