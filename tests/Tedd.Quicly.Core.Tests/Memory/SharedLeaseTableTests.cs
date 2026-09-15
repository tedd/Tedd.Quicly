using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Memory;

namespace Tedd.Quicly.Core.Tests.Memory;

public class SharedLeaseTableTests
{
    private static SlabAllocator NewAllocator(bool validate = true) => new(new SlabAllocatorOptions
    {
        SizeClasses = [new(64, 8), new(256, 4)],
        ValidateLeases = validate,
    });

    [Fact]
    public void Null_Allocator_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new SharedLeaseTable(null!));
    }

    [Fact]
    public void Share_Retain_Release_Returns_Exactly_Once()
    {
        using var allocator = NewAllocator();
        var table = new SharedLeaseTable(allocator);
        Assert.Same(allocator, table.Allocator);

        Assert.True(allocator.TryRent(100, out BufferLease lease));
        SharedLease shared = table.Share(lease, 2);
        Assert.True(shared.IsValid);
        Assert.Equal(lease, shared.Lease);
        Assert.Equal(2, table.GetReferenceCount(shared));

        table.Retain(shared);
        Assert.Equal(3, table.GetReferenceCount(shared));

        Assert.False(table.Release(shared));
        Assert.False(table.Release(shared));
        Assert.Equal(1, allocator.GetClassStatistics(1).Rented);
        Assert.True(table.Release(shared));
        Assert.Equal(0, allocator.GetClassStatistics(1).Rented);
        Assert.Equal(0, table.GetReferenceCount(shared));

        // Releasing or retaining an unshared block is an error and leaves the count at zero.
        Assert.Throws<InvalidOperationException>(() => table.Release(shared));
        Assert.Equal(0, table.GetReferenceCount(shared));
        Assert.Throws<InvalidOperationException>(() => table.Retain(shared));
        Assert.Equal(0, table.GetReferenceCount(shared));

        // The block can be shared again after its last release.
        Assert.True(allocator.TryRent(100, out BufferLease again));
        Assert.Equal(lease.BlockIndex, again.BlockIndex);
        SharedLease sharedAgain = table.Share(again, 1);
        Assert.True(table.Release(sharedAgain));
    }

    [Fact]
    public void Share_Validates_Arguments()
    {
        using var allocator = NewAllocator();
        var table = new SharedLeaseTable(allocator);
        Assert.True(allocator.TryRent(1, out BufferLease lease));

        Assert.Throws<ArgumentOutOfRangeException>(() => table.Share(lease, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => table.Share(lease, -5));
        Assert.Throws<ArgumentException>(() => table.Share(BufferLease.Empty, 1));

        SharedLease shared = table.Share(lease, 1);
        var ex = Assert.Throws<InvalidOperationException>(() => table.Share(lease, 1));
        Assert.Contains("already shared", ex.Message, StringComparison.Ordinal);
        Assert.True(table.Release(shared));
    }

    [Fact]
    public void Retain_Release_And_Count_Reject_Invalid_Leases()
    {
        using var allocator = NewAllocator();
        var table = new SharedLeaseTable(allocator);
        SharedLease empty = default;
        Assert.False(empty.IsValid);
        Assert.Throws<ArgumentException>(() => table.Retain(empty));
        Assert.Throws<ArgumentException>(() => table.Release(empty));
        Assert.Throws<ArgumentException>(() => table.GetReferenceCount(empty));

        // A lease from a bigger allocator addresses a class / block this table does not have.
        using var other = new SlabAllocator(new SlabAllocatorOptions { SizeClasses = [new(64, 64), new(256, 4), new(1536, 2)] });
        Assert.True(other.TryRent(1536, out BufferLease foreignClass));
        var foreign = new SharedLease(foreignClass);
        Assert.Throws<ArgumentException>(() => table.Retain(foreign));
        Assert.Throws<ArgumentException>(() => table.Release(foreign));

        BufferLease[] many = new BufferLease[64];
        for (int i = 0; i < many.Length; i++)
            Assert.True(other.TryRent(1, out many[i]));
        // Blocks come out shard by shard, starting at the shard of the calling thread: take the highest index.
        BufferLease outOfRangeBlock = many[0];
        for (int i = 1; i < many.Length; i++)
            if (many[i].BlockIndex > outOfRangeBlock.BlockIndex)
                outOfRangeBlock = many[i];
        Assert.True(outOfRangeBlock.BlockIndex >= 8);
        Assert.Throws<ArgumentException>(() => table.GetReferenceCount(new SharedLease(outOfRangeBlock)));
        for (int i = 0; i < many.Length; i++)
            other.Return(many[i]);
        other.Return(foreignClass);
    }

    [Fact]
    public void Counters_Are_Padded_To_One_Cache_Line_Per_Block()
    {
        Assert.Equal(64, SharedLeaseTable.CounterStrideBytes);

        // The table reserves one line per block up front: for 8 + 4 blocks that is at least 12 × 64 bytes.
        using var allocator = NewAllocator();
        long before = GC.GetAllocatedBytesForCurrentThread();
        var table = new SharedLeaseTable(allocator);
        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.True(after - before >= 12 * SharedLeaseTable.CounterStrideBytes, $"table allocated only {after - before} bytes");

        // Adjacent blocks have independent counters.
        Assert.True(allocator.TryRent(1, out BufferLease a));
        Assert.True(allocator.TryRent(1, out BufferLease b));
        Assert.Equal(1, Math.Abs(a.BlockIndex - b.BlockIndex));
        SharedLease sa = table.Share(a, 3);
        SharedLease sb = table.Share(b, 5);
        Assert.Equal(3, table.GetReferenceCount(sa));
        Assert.Equal(5, table.GetReferenceCount(sb));
        table.Retain(sa);
        Assert.Equal(4, table.GetReferenceCount(sa));
        Assert.Equal(5, table.GetReferenceCount(sb));
        for (int i = 0; i < 3; i++)
            Assert.False(table.Release(sa));
        Assert.True(table.Release(sa));
        Assert.Equal(5, table.GetReferenceCount(sb));
        for (int i = 0; i < 4; i++)
            Assert.False(table.Release(sb));
        Assert.True(table.Release(sb));
    }

    [Fact]
    public void Forged_Block_Index_That_Wraps_The_Counter_Slot_Is_Rejected()
    {
        using var allocator = NewAllocator();
        var table = new SharedLeaseTable(allocator);

        // 0x1000_0001 × 16 wraps to 16 in 32-bit arithmetic, i.e. block 1's counter, if the slot were checked
        // instead of the block index.
        var wrapping = new SharedLease(Forge(classIndex: 0, blockIndex: 0x1000_0001, length: 64));
        Assert.Throws<ArgumentException>(() => table.GetReferenceCount(wrapping));
        Assert.Throws<ArgumentException>(() => table.Retain(wrapping));
        Assert.Throws<ArgumentException>(() => table.Release(wrapping));

        var negative = new SharedLease(Forge(classIndex: 0, blockIndex: -1, length: 64));
        Assert.Throws<ArgumentException>(() => table.GetReferenceCount(negative));

        // Block 1 was never touched by the rejected calls.
        Assert.True(allocator.TryRent(1, out BufferLease a));
        Assert.True(allocator.TryRent(1, out BufferLease b));
        SharedLease sharedB = table.Share(b.BlockIndex == 1 ? b : a, 1);
        Assert.Equal(1, table.GetReferenceCount(sharedB));
        Assert.True(table.Release(sharedB));
        allocator.Return(b.BlockIndex == 1 ? a : b);
    }

    private static BufferLease Forge(int classIndex, int blockIndex, int length)
    {
        // Same layout as BufferLease: class (1) + shard (1) + generation (2) + block (4) + offset (4) + length (4).
        Span<byte> bytes = stackalloc byte[16];
        bytes[0] = (byte)classIndex;
        BitConverter.TryWriteBytes(bytes[4..], blockIndex);
        BitConverter.TryWriteBytes(bytes[12..], length);
        return Unsafe.ReadUnaligned<BufferLease>(ref bytes[0]);
    }

    [Fact]
    public void SharedLease_Equality_Members()
    {
        using var allocator = NewAllocator();
        var table = new SharedLeaseTable(allocator);
        Assert.True(allocator.TryRent(1, out BufferLease a));
        Assert.True(allocator.TryRent(1, out BufferLease b));
        SharedLease sa = table.Share(a, 1);
        SharedLease sb = table.Share(b, 1);
        SharedLease saCopy = sa;

        Assert.True(sa == saCopy);
        Assert.False(sa != saCopy);
        Assert.True(sa != sb);
        Assert.False(sa == sb);
        Assert.True(sa.Equals((object)saCopy));
        Assert.False(sa.Equals((object)sb));
        Assert.False(sa.Equals(null));
        Assert.Equal(sa.GetHashCode(), saCopy.GetHashCode());
        Assert.Equal(a.GetHashCode(), sa.GetHashCode());
        Assert.StartsWith("SharedBufferLease(", sa.ToString(), StringComparison.Ordinal);

        Assert.True(table.Release(sa));
        Assert.True(table.Release(sb));
    }

    [Fact]
    public void Release_Race_Returns_Exactly_Once()
    {
        using var allocator = NewAllocator(validate: true);
        var table = new SharedLeaseTable(allocator);

        for (int round = 0; round < 20; round++)
        {
            const int references = 512;
            Assert.True(allocator.TryRent(200, out BufferLease lease));
            SharedLease shared = table.Share(lease, references);
            int returned = 0;
            Parallel.For(0, references, new ParallelOptions { MaxDegreeOfParallelism = 16 }, _ =>
            {
                if (table.Release(shared))
                    Interlocked.Increment(ref returned);
            });

            Assert.Equal(1, returned);
            Assert.Equal(0, table.GetReferenceCount(shared));
            Assert.Equal(0, allocator.GetClassStatistics(1).Rented);
        }
    }

    [Fact]
    public void Retain_And_Release_Race_Ends_With_One_Return()
    {
        using var allocator = NewAllocator(validate: true);
        var table = new SharedLeaseTable(allocator);

        for (int round = 0; round < 20; round++)
        {
            const int workers = 256;
            Assert.True(allocator.TryRent(1, out BufferLease lease));
            SharedLease shared = table.Share(lease, 1);
            int returned = 0;

            // Each worker retains then releases; the original reference keeps the block alive until the end.
            Parallel.For(0, workers, new ParallelOptions { MaxDegreeOfParallelism = 16 }, _ =>
            {
                table.Retain(shared);
                table.Retain(shared);
                if (table.Release(shared))
                    Interlocked.Increment(ref returned);
                if (table.Release(shared))
                    Interlocked.Increment(ref returned);
            });

            Assert.Equal(0, returned);
            Assert.Equal(1, table.GetReferenceCount(shared));
            Assert.True(table.Release(shared));
            Assert.Equal(0, allocator.GetClassStatistics(0).Rented);
        }
    }

    [Fact]
    public void Share_Retain_Release_Loop_Does_Not_Allocate()
    {
        using var allocator = NewAllocator(validate: true);
        var table = new SharedLeaseTable(allocator);
        for (int i = 0; i < 1_000; i++)
            ShareRetainRelease(allocator, table);

        // Measured in windows like the other zero-allocation tests: a one-off runtime event on this thread (tier-up or
        // OSR compilation landing inside a window, observed on .NET 11 previews) does not repeat, while a steady-state
        // allocation shows up in every window.
        const int windows = 5;
        long[] deltas = new long[windows];
        int allocating = 0;
        for (int window = 0; window < windows; window++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 20_000; i++)
                ShareRetainRelease(allocator, table);
            deltas[window] = GC.GetAllocatedBytesForCurrentThread() - before;
            if (deltas[window] != 0)
                allocating++;
        }

        Assert.True(allocating <= 1, $"Allocated in {allocating} of {windows} windows: {string.Join(", ", deltas)} bytes per 20000 calls.");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ShareRetainRelease(SlabAllocator allocator, SharedLeaseTable table)
    {
        Assert.True(allocator.TryRent(100, out BufferLease lease));
        SharedLease shared = table.Share(in lease, 1);
        table.Retain(in shared);
        Assert.False(table.Release(in shared));
        Assert.True(table.Release(in shared));
    }
}
