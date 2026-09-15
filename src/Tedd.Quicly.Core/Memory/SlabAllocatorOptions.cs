namespace Tedd.Quicly.Core.Memory;

/// <summary>One size class of a <see cref="SlabAllocator"/>: a block size and how many blocks to reserve.</summary>
/// <param name="BlockSize">Block size in bytes; must be a positive multiple of <see cref="SlabAllocatorOptions.BlockAlignment"/>.</param>
/// <param name="BlockCount">Number of blocks reserved for the class; must be positive.</param>
public readonly record struct SizeClassDefinition(int BlockSize, int BlockCount)
{
    /// <summary>Total bytes of the class slab (<see cref="BlockSize"/> × <see cref="BlockCount"/>).</summary>
    public long SlabBytes => (long)BlockSize * BlockCount;
}

/// <summary>Configuration for <see cref="SlabAllocator"/>. Validated when the allocator is constructed.</summary>
public sealed class SlabAllocatorOptions
{
    /// <summary>Maximum number of size classes an allocator can have.</summary>
    public const int MaxSizeClasses = 16;

    /// <summary>Alignment of every slab and every block, in bytes (one cache line).</summary>
    public const int BlockAlignment = 64;

    /// <summary>Maximum value of <see cref="FreeListShards"/>.</summary>
    public const int MaxFreeListShards = 64;

    /// <summary>
    /// Number of independent free lists per size class; a power of two between 1 and
    /// <see cref="MaxFreeListShards"/>. Each thread is assigned a shard round-robin the first time it rents and
    /// pops from that shard first (stealing from the others only when it is empty); a block is always returned
    /// to the shard it came from. More shards spread contending threads over more cache lines at the cost of
    /// a slightly longer scan when the class is nearly exhausted. Default 8.
    /// </summary>
    public int FreeListShards { get; set; } = 8;

    /// <summary>
    /// Size classes in strictly increasing block size. The default set is 64 B, 256 B, 1 536 B, 4 KiB, 16 KiB,
    /// 64 KiB and 256 KiB with block counts that reserve 16 MiB in total.
    /// </summary>
    public SizeClassDefinition[] SizeClasses { get; set; } = CreateDefaultSizeClasses();

    /// <summary>
    /// When <see langword="true"/> the allocator keeps a per-block state byte and checks the lease generation
    /// on <see cref="SlabAllocator.Return"/>, so double returns and stale leases throw
    /// <see cref="InvalidOperationException"/> instead of corrupting the free list. Costs one atomic exchange
    /// per return. Defaults to <see langword="true"/> in Debug builds of the library and <see langword="false"/>
    /// in Release builds.
    /// </summary>
    public bool ValidateLeases { get; set; } =
#if DEBUG
        true;
#else
        false;
#endif

    /// <summary>Creates a fresh copy of the default size classes (16 MiB total).</summary>
    public static SizeClassDefinition[] CreateDefaultSizeClasses() =>
    [
        new(64, 16_384),        // 1 MiB
        new(256, 4_096),        // 1 MiB
        new(1_536, 2_048),      // 3 MiB
        new(4_096, 768),        // 3 MiB
        new(16_384, 192),       // 3 MiB
        new(65_536, 32),        // 2 MiB
        new(262_144, 12),       // 3 MiB
    ];

    /// <summary>Validates the options and returns a defensive copy of the size classes.</summary>
    /// <exception cref="ArgumentException">The options are invalid.</exception>
    internal SizeClassDefinition[] ValidateAndCopyClasses()
    {
        int shards = FreeListShards;
        if (shards < 1 || shards > MaxFreeListShards || !int.IsPow2(shards))
            throw new ArgumentException($"FreeListShards {shards} must be a power of two between 1 and {MaxFreeListShards}.", nameof(FreeListShards));

        SizeClassDefinition[]? classes = SizeClasses;
        if (classes is null)
            throw new ArgumentException("SizeClasses must not be null.", nameof(SizeClasses));
        if (classes.Length == 0)
            throw new ArgumentException("At least one size class is required.", nameof(SizeClasses));
        if (classes.Length > MaxSizeClasses)
            throw new ArgumentException($"At most {MaxSizeClasses} size classes are supported.", nameof(SizeClasses));

        var copy = new SizeClassDefinition[classes.Length];
        int previousSize = 0;
        for (int i = 0; i < classes.Length; i++)
        {
            SizeClassDefinition c = classes[i];
            if (c.BlockSize <= 0 || (c.BlockSize % BlockAlignment) != 0)
                throw new ArgumentException($"Size class {i}: BlockSize {c.BlockSize} must be a positive multiple of {BlockAlignment}.", nameof(SizeClasses));
            if (c.BlockSize <= previousSize)
                throw new ArgumentException($"Size class {i}: BlockSize {c.BlockSize} must be greater than the previous class ({previousSize}).", nameof(SizeClasses));
            if (c.BlockCount <= 0)
                throw new ArgumentException($"Size class {i}: BlockCount {c.BlockCount} must be positive.", nameof(SizeClasses));
            if (c.SlabBytes > int.MaxValue)
                throw new ArgumentException($"Size class {i}: BlockSize × BlockCount ({c.SlabBytes}) exceeds {int.MaxValue} bytes.", nameof(SizeClasses));
            previousSize = c.BlockSize;
            copy[i] = c;
        }

        return copy;
    }
}
