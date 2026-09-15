using System.Runtime.InteropServices;

namespace Tedd.Quicly.Core.Memory;

/// <summary>
/// The unit of buffer ownership handed out by <see cref="SlabAllocator"/>: identifies one block of one size
/// class. A lease is a 16-byte unmanaged value, so it can live in struct arrays, be copied freely and be
/// passed by <c>in</c> reference with no GC involvement.
/// </summary>
/// <remarks>
/// <para>Layout (16 bytes, sequential): class index (1 byte), free-list shard (1 byte), generation (2 bytes),
/// block index (4 bytes), byte offset of the block inside its slab (4 bytes) and usable length (4 bytes).</para>
/// <para>The generation is a per-block counter that the allocator bumps on every rent. When lease validation
/// is enabled (<see cref="SlabAllocatorOptions.ValidateLeases"/>) a <see cref="SlabAllocator.Return"/> with a
/// generation that does not match the block's current generation is rejected as stale, and a return of a
/// block that is not currently rented is rejected as a double return.</para>
/// <para><see cref="Length"/> is the full capacity of the block (the size class), not the minimum the caller
/// asked for. A lease with <see cref="Length"/> of zero is <see cref="Empty"/>.</para>
/// </remarks>
[StructLayout(LayoutKind.Sequential, Pack = 4, Size = 16)]
public readonly struct BufferLease : IEquatable<BufferLease>
{
    private readonly byte _classIndex;
    private readonly byte _shard;
    private readonly ushort _generation;
    private readonly int _blockIndex;
    private readonly int _offset;
    private readonly int _length;

    internal BufferLease(byte classIndex, byte shard, ushort generation, int blockIndex, int offset, int length)
    {
        _classIndex = classIndex;
        _shard = shard;
        _generation = generation;
        _blockIndex = blockIndex;
        _offset = offset;
        _length = length;
    }

    /// <summary>A lease that refers to nothing. <see cref="IsEmpty"/> is <see langword="true"/>.</summary>
    public static BufferLease Empty => default;

    /// <summary>Index of the size class the block belongs to.</summary>
    public int ClassIndex => _classIndex;

    /// <summary>Free-list shard of the class the block belongs to (see <see cref="SlabAllocatorOptions.FreeListShards"/>).</summary>
    public int Shard => _shard;

    /// <summary>Index of the block inside its size class.</summary>
    public int BlockIndex => _blockIndex;

    /// <summary>Byte offset of the block from the start of its size class slab.</summary>
    public int Offset => _offset;

    /// <summary>Usable length in bytes (the block size of the class).</summary>
    public int Length => _length;

    /// <summary>Generation of the block at the time it was rented; used to detect stale leases.</summary>
    public ushort Generation => _generation;

    /// <summary><see langword="true"/> when the lease refers to no block.</summary>
    public bool IsEmpty => _length == 0;

    /// <summary><see langword="true"/> when the lease refers to a block (the inverse of <see cref="IsEmpty"/>).</summary>
    public bool IsValid => _length != 0;

    /// <inheritdoc />
    public bool Equals(BufferLease other) =>
        _classIndex == other._classIndex
        && _shard == other._shard
        && _generation == other._generation
        && _blockIndex == other._blockIndex
        && _offset == other._offset
        && _length == other._length;

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is BufferLease other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(_classIndex, _generation, _blockIndex, _length);

    /// <inheritdoc />
    public override string ToString() =>
        IsEmpty ? "BufferLease.Empty" : $"BufferLease(class={_classIndex}, block={_blockIndex}, offset={_offset}, length={_length}, gen={_generation}, shard={_shard})";

    /// <summary>Value equality.</summary>
    public static bool operator ==(BufferLease left, BufferLease right) => left.Equals(right);

    /// <summary>Value inequality.</summary>
    public static bool operator !=(BufferLease left, BufferLease right) => !left.Equals(right);
}
