using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Memory;

namespace Tedd.Quicly.Core.Tests.Memory;

public class BufferLeaseTests
{
    private static bool IsUnmanaged<T>() where T : unmanaged => true;

    [Fact]
    public void Is_16_Bytes_And_Unmanaged()
    {
        Assert.Equal(16, Unsafe.SizeOf<BufferLease>());
        Assert.True(IsUnmanaged<BufferLease>());
        Assert.True(IsUnmanaged<SharedLease>());
        Assert.Equal(16, Unsafe.SizeOf<SharedLease>());
    }

    [Fact]
    public void Empty_Is_Default_And_Invalid()
    {
        BufferLease empty = BufferLease.Empty;
        Assert.True(empty.IsEmpty);
        Assert.False(empty.IsValid);
        Assert.Equal(0, empty.Length);
        Assert.Equal(0, empty.ClassIndex);
        Assert.Equal(0, empty.BlockIndex);
        Assert.Equal(0, empty.Offset);
        Assert.Equal(0, empty.Generation);
        Assert.Equal(default, empty);
        Assert.Equal("BufferLease.Empty", empty.ToString());
    }

    [Fact]
    public void Rented_Lease_Exposes_Fields()
    {
        using var allocator = new SlabAllocator(new SlabAllocatorOptions { SizeClasses = [new(64, 4), new(256, 4)] });
        Assert.True(allocator.TryRent(100, out BufferLease a));
        Assert.True(allocator.TryRent(100, out BufferLease b));

        Assert.True(a.IsValid);
        Assert.False(a.IsEmpty);
        Assert.Equal(1, a.ClassIndex);
        Assert.Equal(256, a.Length);
        Assert.Equal(a.BlockIndex * 256, a.Offset);
        Assert.Equal(b.BlockIndex * 256, b.Offset);
        Assert.NotEqual(a.BlockIndex, b.BlockIndex);
        Assert.Equal(1, a.Generation);
        Assert.True(a.Shard >= 0 && a.Shard < allocator.ShardCount);
        Assert.Contains("class=1", a.ToString(), StringComparison.Ordinal);
        Assert.Contains($"block={a.BlockIndex}", a.ToString(), StringComparison.Ordinal);
        Assert.Contains($"shard={a.Shard}", a.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Equality_Members()
    {
        using var allocator = new SlabAllocator(new SlabAllocatorOptions { SizeClasses = [new(64, 4)] });
        Assert.True(allocator.TryRent(1, out BufferLease a));
        Assert.True(allocator.TryRent(1, out BufferLease b));
        BufferLease aCopy = a;

        Assert.True(a.Equals(aCopy));
        Assert.True(a == aCopy);
        Assert.False(a != aCopy);
        Assert.True(a != b);
        Assert.False(a == b);
        Assert.True(a.Equals((object)aCopy));
        Assert.False(a.Equals((object)b));
        Assert.False(a.Equals(null));
        Assert.False(a.Equals("not a lease"));
        Assert.Equal(a.GetHashCode(), aCopy.GetHashCode());
        Assert.NotEqual(a.GetHashCode(), b.GetHashCode());

        // Same block, different generation: not equal.
        allocator.Return(a);
        Assert.True(allocator.TryRent(1, out BufferLease aAgain));
        Assert.Equal(a.BlockIndex, aAgain.BlockIndex);
        Assert.NotEqual(a.Generation, aAgain.Generation);
        Assert.NotEqual(a, aAgain);
    }

    [Fact]
    public void Works_As_Struct_Array_Element()
    {
        using var allocator = new SlabAllocator(new SlabAllocatorOptions { SizeClasses = [new(64, 8)] });
        var leases = new BufferLease[8];
        for (int i = 0; i < leases.Length; i++)
            Assert.True(allocator.TryRent(64, out leases[i]));
        for (int i = 0; i < leases.Length; i++)
        {
            Assert.True(leases[i].IsValid);
            allocator.Return(in leases[i]);
        }
        Assert.Equal(0, allocator.GetClassStatistics(0).Rented);
    }
}
