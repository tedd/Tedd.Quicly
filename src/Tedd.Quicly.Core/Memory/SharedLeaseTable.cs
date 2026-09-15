using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.Memory;

/// <summary>
/// Reference counting for fan-out sends: one <see cref="BufferLease"/> is serialised once, shared with N
/// senders, and returned to the <see cref="SlabAllocator"/> exactly once, when the last sender releases it.
/// </summary>
/// <remarks>
/// <para>Counts live in one pre-allocated array per size class, indexed by block, so sharing, retaining and
/// releasing never allocate and are safe to call concurrently from any threads (<see cref="Interlocked"/>
/// operations only). A block whose count is zero is not shared; sharing it again after the last release is
/// allowed.</para>
/// <para><b>Padding.</b> Every counter sits on its own 64-byte line (<see cref="CounterStrideBytes"/>): a
/// fan-out completes on many transport workers at once, each doing one <see cref="Interlocked.Decrement(ref int)"/>
/// for its peer, and with unpadded counters the completions of <i>different</i> messages in flight would bounce
/// the same line between cores (false sharing). The cost is 64 bytes per block instead of 4 — 1.5 MiB for the
/// default 16 MiB allocator — paid once at construction.</para>
/// </remarks>
public sealed class SharedLeaseTable
{
    /// <summary>Distance in bytes between the reference counts of two adjacent blocks (one cache line).</summary>
    public const int CounterStrideBytes = 64;

    private const int CounterStrideInts = CounterStrideBytes / sizeof(int);

    private readonly SlabAllocator _allocator;
    private readonly int[][] _referenceCounts;

    /// <summary>Creates a table sized to every block of <paramref name="allocator"/>.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="allocator"/> is <see langword="null"/>.</exception>
    public SharedLeaseTable(SlabAllocator allocator)
    {
        ArgumentNullException.ThrowIfNull(allocator);
        _allocator = allocator;
        ReadOnlySpan<SizeClassDefinition> classes = allocator.SizeClasses;
        _referenceCounts = new int[classes.Length][];
        for (int ci = 0; ci < classes.Length; ci++)
            _referenceCounts[ci] = new int[classes[ci].BlockCount * CounterStrideInts];
    }

    /// <summary>The allocator the leases belong to and are returned to.</summary>
    public SlabAllocator Allocator => _allocator;

    /// <summary>
    /// Starts sharing <paramref name="lease"/> with <paramref name="initialCount"/> references. Ownership of the
    /// lease moves to the table: the caller must not <see cref="SlabAllocator.Return"/> it, only
    /// <see cref="Release"/> its references.
    /// </summary>
    /// <exception cref="ArgumentException">The lease does not address a block of the allocator.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="initialCount"/> is less than one.</exception>
    /// <exception cref="InvalidOperationException">The block is already shared.</exception>
    /// <exception cref="ObjectDisposedException">The allocator has been disposed.</exception>
    public SharedLease Share(in BufferLease lease, int initialCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(initialCount, 1);
        _allocator.ValidateBounds(in lease);
        ref int count = ref _referenceCounts[lease.ClassIndex][lease.BlockIndex * CounterStrideInts];
        if (Interlocked.CompareExchange(ref count, initialCount, 0) != 0)
            ThrowAlreadyShared(in lease);
        return new SharedLease(in lease);
    }

    /// <summary>Adds one reference.</summary>
    /// <exception cref="ArgumentException">The lease does not address a block of the allocator.</exception>
    /// <exception cref="InvalidOperationException">The block is not currently shared.</exception>
    public void Retain(in SharedLease shared)
    {
        ref int count = ref Resolve(in shared);
        if (Interlocked.Increment(ref count) <= 1)
        {
            Interlocked.Decrement(ref count);
            ThrowNotShared(in shared);
        }
    }

    /// <summary>
    /// Drops one reference. When the count reaches zero the underlying lease is returned to the allocator.
    /// </summary>
    /// <returns><see langword="true"/> when this call released the last reference and returned the block.</returns>
    /// <exception cref="ArgumentException">The lease does not address a block of the allocator.</exception>
    /// <exception cref="InvalidOperationException">The block is not currently shared (released too often).</exception>
    /// <exception cref="ObjectDisposedException">The allocator has been disposed.</exception>
    public bool Release(in SharedLease shared)
    {
        ref int count = ref Resolve(in shared);
        int remaining = Interlocked.Decrement(ref count);
        if (remaining == 0)
        {
            _allocator.Return(shared.Lease);
            return true;
        }

        if (remaining < 0)
        {
            Interlocked.Increment(ref count);
            ThrowNotShared(in shared);
        }

        return false;
    }

    /// <summary>Current reference count of the block (zero when not shared). Diagnostic; racy by nature.</summary>
    /// <exception cref="ArgumentException">The lease does not address a block of the allocator.</exception>
    public int GetReferenceCount(in SharedLease shared) => Volatile.Read(ref Resolve(in shared));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref int Resolve(in SharedLease shared)
    {
        int[][] table = _referenceCounts;
        int ci = shared.Lease.ClassIndex;
        if ((uint)ci < (uint)table.Length)
        {
            int[] counts = table[ci];
            int index = shared.Lease.BlockIndex;
            // Range-check the block index itself, not the scaled slot: a forged index large enough to wrap
            // the multiplication must not alias a real counter.
            if ((uint)index < (uint)(counts.Length / CounterStrideInts) && shared.Lease.IsValid)
                return ref counts[index * CounterStrideInts];
        }

        ThrowInvalidLease(in shared);
        return ref Unsafe.NullRef<int>();
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowInvalidLease(in SharedLease shared) =>
        throw new ArgumentException($"{shared} does not address a block of the allocator.", nameof(shared));

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowAlreadyShared(in BufferLease lease) =>
        throw new InvalidOperationException($"{lease} is already shared.");

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowNotShared(in SharedLease shared) =>
        throw new InvalidOperationException($"{shared} is not shared.");
}
