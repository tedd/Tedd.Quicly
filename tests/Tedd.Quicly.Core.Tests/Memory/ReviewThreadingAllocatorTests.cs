using Tedd.Quicly.Core.Memory;

namespace Tedd.Quicly.Core.Tests.Memory;

/// <summary>
/// Failing tests of the performance pass review (threading / memory lens) that target code new in the pass
/// (<see cref="SlabAllocator.ReturnMany"/>), so they pin an invariant the new code must keep rather than a 691f20d behaviour.
/// </summary>
public class ReviewThreadingAllocatorTests
{
    /// <summary>
    /// <see cref="SlabAllocator.ReturnMany"/> documents that when a lease is stale "the leases before its run were
    /// returned" — so the leases of the failing run itself were not, and stay rented. With validation on, the run is
    /// validated lease by lease and each block's state is flipped to free (Interlocked.Exchange) before the next lease is
    /// checked; the push happens only after the whole run passed. A stale lease in the middle of a run therefore leaves the
    /// blocks before it marked free but never pushed: a later <see cref="SlabAllocator.Return"/> of such a lease throws as a
    /// double return, the shard's rented count still includes it, and the block can never be rented again — the pool shrinks
    /// by one block per earlier lease of the run, permanently.
    /// </summary>
    [Fact]
    public void ReturnMany_Rejecting_A_Stale_Lease_Leaves_The_Earlier_Leases_Of_Its_Run_Rented_And_Recoverable()
    {
        using var allocator = new SlabAllocator(new SlabAllocatorOptions
        {
            FreeListShards = 1,
            SizeClasses = [new(64, 4)],
            ValidateLeases = true,
        });
        Assert.True(allocator.TryRent(10, out BufferLease a));
        Assert.True(allocator.TryRent(10, out BufferLease b));
        allocator.Return(b);
        Assert.True(allocator.TryRent(10, out BufferLease b2)); // the same block under a new generation: b is stale now
        Assert.Equal(b.BlockIndex, b2.BlockIndex);

        // One run (same class and shard): a, then the stale b.
        Assert.Throws<InvalidOperationException>(() => allocator.ReturnMany([a, b]));

        // a belongs to the rejected run, so it is still rented: returning it now must work, and every block comes back.
        allocator.Return(a);
        allocator.Return(b2);
        Assert.Equal(0, allocator.GetStatistics().TotalRentedBytes);
        var all = new List<BufferLease>();
        while (allocator.TryRent(10, out BufferLease x))
        {
            all.Add(x);
        }

        Assert.Equal(4, all.Count);
    }
}
