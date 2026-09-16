using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Memory;

namespace Tedd.Quicly.Core.Tests.Memory;

public class LeaseExtensionsTests
{
    private static SlabAllocator NewAllocator() => new(new SlabAllocatorOptions
    {
        SizeClasses = [new(64, 2), new(256, 1)],
        ValidateLeases = true,
    });

    [Fact]
    public void TryRentCopy_Copies_Into_Smallest_Fitting_Class()
    {
        using var allocator = NewAllocator();
        byte[] source = Enumerable.Range(0, 100).Select(i => (byte)i).ToArray();

        Assert.True(allocator.TryRentCopy(source, out BufferLease lease));
        Assert.Equal(1, lease.ClassIndex);
        Assert.Equal(256, lease.Length);
        Assert.Equal(source, allocator.GetSpan(lease)[..100].ToArray());
        Assert.Equal(source, allocator.GetSpan(lease, 100).ToArray());
        allocator.Return(lease);
    }

    [Fact]
    public void TryRentCopy_Empty_Source_Rents_Smallest_Block()
    {
        using var allocator = NewAllocator();
        Assert.True(allocator.TryRentCopy(ReadOnlySpan<byte>.Empty, out BufferLease lease));
        Assert.Equal(0, lease.ClassIndex);
        Assert.Equal(0, allocator.GetSpan(lease, 0).Length);
        allocator.Return(lease);
    }

    [Fact]
    public void TryRentCopy_Fails_When_Too_Large_Or_Exhausted()
    {
        using var allocator = NewAllocator();
        Assert.False(allocator.TryRentCopy(new byte[257], out BufferLease tooLarge));
        Assert.True(tooLarge.IsEmpty);

        Assert.True(allocator.TryRentCopy(new byte[200], out BufferLease first));
        Assert.False(allocator.TryRentCopy(new byte[200], out BufferLease exhausted));
        Assert.True(exhausted.IsEmpty);
        Assert.Equal(1, allocator.GetClassStatistics(1).Exhaustions);
        allocator.Return(first);
    }

    [Fact]
    public void Null_Allocator_Throws()
    {
        SlabAllocator? allocator = null;
        Assert.Throws<ArgumentNullException>(() => allocator!.TryRentCopy(new byte[1], out _));
        Assert.Throws<ArgumentNullException>(() => allocator!.GetSpan(BufferLease.Empty, 0));
    }

    [Fact]
    public void GetSpan_With_Length_Validates_Range()
    {
        using var allocator = NewAllocator();
        Assert.True(allocator.TryRent(64, out BufferLease lease));
        Assert.Equal(64, allocator.GetSpan(lease, 64).Length);
        Assert.Equal(10, allocator.GetSpan(lease, 10).Length);
        Assert.Throws<ArgumentOutOfRangeException>(() => allocator.GetSpan(lease, 65));
        Assert.Throws<ArgumentOutOfRangeException>(() => allocator.GetSpan(lease, -1));
        Assert.Throws<ArgumentException>(() => allocator.GetSpan(BufferLease.Empty, 0));
        allocator.Return(lease);
    }

    [Fact]
    public void TryRentCopy_Loop_Does_Not_Allocate()
    {
        using var allocator = NewAllocator();
        byte[] source = new byte[200];
        for (int i = 0; i < 1_000; i++)
            RentCopyReturn(allocator, source);

        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 20_000; i++)
                RentCopyReturn(allocator, source);
        });
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void RentCopyReturn(SlabAllocator allocator, byte[] source)
    {
        Assert.True(allocator.TryRentCopy(source, out BufferLease lease));
        allocator.Return(in lease);
    }
}
