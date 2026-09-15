namespace Tedd.Quicly.Core.Memory;

/// <summary>
/// A <see cref="BufferLease"/> whose ownership is shared between several senders and tracked by a
/// <see cref="SharedLeaseTable"/>. The block is returned to the allocator when the last reference is released.
/// </summary>
public readonly struct SharedLease : IEquatable<SharedLease>
{
    internal SharedLease(in BufferLease lease)
    {
        Lease = lease;
    }

    /// <summary>The underlying block.</summary>
    public BufferLease Lease { get; }

    /// <summary><see langword="true"/> when the shared lease refers to a block.</summary>
    public bool IsValid => Lease.IsValid;

    /// <inheritdoc />
    public bool Equals(SharedLease other) => Lease.Equals(other.Lease);

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is SharedLease other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => Lease.GetHashCode();

    /// <inheritdoc />
    public override string ToString() => $"Shared{Lease}";

    /// <summary>Value equality.</summary>
    public static bool operator ==(SharedLease left, SharedLease right) => left.Equals(right);

    /// <summary>Value inequality.</summary>
    public static bool operator !=(SharedLease left, SharedLease right) => !left.Equals(right);
}
