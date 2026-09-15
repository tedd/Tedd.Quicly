using System.Numerics;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.Threading;

/// <summary>
/// Bounded lock-free single-producer / single-consumer ring buffer.
/// </summary>
/// <remarks>
/// <para>
/// Exactly one thread may call <see cref="TryEnqueue"/> and exactly one (possibly different) thread may call
/// <see cref="TryDequeue"/> at any time. Elements are copied by value; the ring never allocates after
/// construction.
/// </para>
/// <para>
/// The producer's index and the consumer's index live on separate cache lines, and each side keeps a private
/// snapshot of the other's index, so in steady state the producer only touches the consumer's line when its
/// snapshot says the ring is full, and the consumer only touches the producer's line when its snapshot says
/// the ring is empty.
/// </para>
/// </remarks>
/// <typeparam name="T">Element type; must be unmanaged so that the buffer is a flat array of values.</typeparam>
public sealed class SpscRing<T> where T : unmanaged
{
    private readonly T[] _buffer;
    private readonly int _mask;
    private SpscIndices _idx;

    /// <summary>Creates a ring that holds at least <paramref name="minimumCapacity"/> elements.</summary>
    /// <param name="minimumCapacity">Requested capacity; rounded up to the next power of two (minimum 2).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="minimumCapacity"/> is not positive or exceeds 2^30.</exception>
    public SpscRing(int minimumCapacity)
    {
        int capacity = RoundUpCapacity(minimumCapacity);
        _buffer = new T[capacity];
        _mask = capacity - 1;
    }

    internal static int RoundUpCapacity(int minimumCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(minimumCapacity, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(minimumCapacity, 1 << 30);
        return (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(minimumCapacity, 2));
    }

    /// <summary>Number of elements the ring can hold. Always a power of two.</summary>
    public int Capacity => _mask + 1;

    /// <summary>
    /// Approximate number of queued elements. Exact only when called by the producer or the consumer while the
    /// other side is idle.
    /// </summary>
    public int Count
    {
        get
        {
            // Head is read first, so a concurrent producer can only make the difference too large, never negative.
            long head = Volatile.Read(ref _idx.Head);
            return (int)Math.Min(Volatile.Read(ref _idx.Tail) - head, _mask + 1);
        }
    }

    /// <summary>True when no element is queued (approximate from a third thread).</summary>
    public bool IsEmpty => Volatile.Read(ref _idx.Tail) == Volatile.Read(ref _idx.Head);

    /// <summary>Appends an element. Producer thread only.</summary>
    /// <param name="item">Element to copy into the ring.</param>
    /// <returns><see langword="false"/> when the ring is full; the element is not stored.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(in T item)
    {
        long tail = _idx.Tail;
        if (tail - _idx.CachedHead > _mask)
        {
            _idx.CachedHead = Volatile.Read(ref _idx.Head);
            if (tail - _idx.CachedHead > _mask)
                return false;
        }

        _buffer[(int)(tail & _mask)] = item;
        Volatile.Write(ref _idx.Tail, tail + 1);
        return true;
    }

    /// <summary>Removes the oldest element. Consumer thread only.</summary>
    /// <param name="item">Receives the element, or <see langword="default"/> when the ring is empty.</param>
    /// <returns><see langword="false"/> when the ring is empty.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(out T item)
    {
        long head = _idx.Head;
        if (head == _idx.CachedTail)
        {
            _idx.CachedTail = Volatile.Read(ref _idx.Tail);
            if (head == _idx.CachedTail)
            {
                item = default;
                return false;
            }
        }

        item = _buffer[(int)(head & _mask)];
        Volatile.Write(ref _idx.Head, head + 1);
        return true;
    }
}
