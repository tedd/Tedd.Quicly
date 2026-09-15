namespace Tedd.Quicly.Core.Memory;

/// <summary>Convenience helpers over <see cref="SlabAllocator"/> and <see cref="BufferLease"/>.</summary>
public static class LeaseExtensions
{
    /// <summary>
    /// Rents the smallest block that holds <paramref name="source"/> and copies the bytes into it.
    /// </summary>
    /// <param name="allocator">The allocator to rent from.</param>
    /// <param name="source">Bytes to copy. An empty span rents a block of the smallest class.</param>
    /// <param name="lease">The rented block holding a copy of <paramref name="source"/>, or <see cref="BufferLease.Empty"/> on failure.</param>
    /// <returns><see langword="false"/> when the source is larger than the largest class or the class is exhausted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="allocator"/> is <see langword="null"/>.</exception>
    /// <exception cref="ObjectDisposedException">The allocator has been disposed.</exception>
    public static bool TryRentCopy(this SlabAllocator allocator, ReadOnlySpan<byte> source, out BufferLease lease)
    {
        ArgumentNullException.ThrowIfNull(allocator);
        if (!allocator.TryRent(source.Length, out lease))
            return false;
        source.CopyTo(allocator.GetSpan(in lease));
        return true;
    }

    /// <summary>The first <paramref name="length"/> bytes of the block as a span.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="allocator"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> is negative or exceeds <see cref="BufferLease.Length"/>.</exception>
    /// <exception cref="ArgumentException">The lease does not address a block of the allocator.</exception>
    public static Span<byte> GetSpan(this SlabAllocator allocator, in BufferLease lease, int length)
    {
        ArgumentNullException.ThrowIfNull(allocator);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(length, lease.Length);
        return allocator.GetSpan(in lease)[..length];
    }
}
