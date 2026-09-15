using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.Memory;

/// <summary>Snapshot of one size class of a <see cref="SlabAllocator"/>.</summary>
/// <param name="BlockSize">Block size of the class in bytes.</param>
/// <param name="Capacity">Number of blocks reserved for the class.</param>
/// <param name="Rented">Number of blocks currently rented at the time of the snapshot.</param>
/// <param name="Peak">Highest number of concurrently rented blocks observed since construction.</param>
/// <param name="Exhaustions">Number of <see cref="SlabAllocator.TryRent"/> calls that failed because the class was empty.</param>
public readonly record struct SizeClassStatistics(int BlockSize, int Capacity, int Rented, int Peak, long Exhaustions)
{
    /// <summary>Number of blocks available to rent at the time of the snapshot.</summary>
    public int Free => Capacity - Rented;
}

[InlineArray(SlabAllocatorOptions.MaxSizeClasses)]
internal struct SizeClassStatisticsBuffer
{
    private SizeClassStatistics _element0;
}

/// <summary>
/// Allocation-free snapshot of a <see cref="SlabAllocator"/>: per-class statistics for up to
/// <see cref="SlabAllocatorOptions.MaxSizeClasses"/> classes stored inline in the struct, plus totals.
/// </summary>
public struct SlabStatistics
{
    private SizeClassStatisticsBuffer _classes;
    private int _classCount;

    internal void Add(in SizeClassStatistics stats)
    {
        _classes[_classCount] = stats;
        _classCount++;
    }

    /// <summary>Number of size classes in the snapshot.</summary>
    public readonly int ClassCount => _classCount;

    /// <summary>Statistics for the size class at <paramref name="classIndex"/>.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="classIndex"/> is not a valid class index.</exception>
    public readonly SizeClassStatistics this[int classIndex]
    {
        get
        {
            if ((uint)classIndex >= (uint)_classCount)
                throw new ArgumentOutOfRangeException(nameof(classIndex));
            return _classes[classIndex];
        }
    }

    /// <summary>Total bytes reserved across all size classes.</summary>
    public readonly long TotalCapacityBytes
    {
        get
        {
            long total = 0;
            for (int i = 0; i < _classCount; i++)
                total += (long)_classes[i].BlockSize * _classes[i].Capacity;
            return total;
        }
    }

    /// <summary>Total bytes currently rented across all size classes.</summary>
    public readonly long TotalRentedBytes
    {
        get
        {
            long total = 0;
            for (int i = 0; i < _classCount; i++)
                total += (long)_classes[i].BlockSize * _classes[i].Rented;
            return total;
        }
    }

    /// <summary>Total number of failed rents across all size classes.</summary>
    public readonly long TotalExhaustions
    {
        get
        {
            long total = 0;
            for (int i = 0; i < _classCount; i++)
                total += _classes[i].Exhaustions;
            return total;
        }
    }
}
