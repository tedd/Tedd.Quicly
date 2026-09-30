using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Threading;

/// <summary>
/// Bounded lock-free single-producer / single-consumer ring buffer over native memory.
/// </summary>
/// <remarks>
/// <para>
/// Exactly one thread may call <see cref="TryEnqueue"/> and exactly one (possibly different) thread may call
/// <see cref="TryDequeue"/> at any time. Elements are copied by value; the ring never allocates after
/// construction. The elements live in a <see cref="NativeArray{T}"/> (64-byte aligned native memory, never
/// scanned or moved by the GC, never on the large-object heap: ADR 0008 invariant 12).
/// </para>
/// <para>
/// The producer's index and the consumer's index live on separate cache lines, and each side keeps a private
/// snapshot of the other's index, so in steady state the producer only touches the consumer's line when its
/// snapshot says the ring is full, and the consumer only touches the producer's line when its snapshot says
/// the ring is empty. <see cref="HasRoomFor"/> / <see cref="TryEnqueueReserving"/> / <see cref="CachedCount"/>
/// keep that property for a producer that also counts slots it has reserved elsewhere;
/// <see cref="Count"/> does not (it reads both indices).
/// </para>
/// <para>
/// <see cref="Dispose"/> frees the native memory and must not race with element access (a finalizer frees it if
/// the owner forgot). The owner disposes the ring only once neither side can touch it any more — for a peer's
/// rings, after the transport can no longer call back.
/// </para>
/// </remarks>
/// <typeparam name="T">Element type; must be unmanaged so that the buffer is a flat block of values.</typeparam>
public sealed unsafe class SpscRing<T> : IDisposable where T : unmanaged
{
    private readonly NativeArray<T> _buffer;
    private readonly T* _slots;
    private readonly int _mask;
    private SpscIndices _idx;

    /// <summary>Creates a ring that holds at least <paramref name="minimumCapacity"/> elements.</summary>
    /// <param name="minimumCapacity">Requested capacity; rounded up to the next power of two (minimum 2).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="minimumCapacity"/> is not positive or exceeds 2^30.</exception>
    public SpscRing(int minimumCapacity)
    {
        int capacity = RoundUpCapacity(minimumCapacity);
        _buffer = new NativeArray<T>(capacity);
        _slots = _buffer.Pointer;
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

    /// <summary>Size of the element buffer in bytes (native memory).</summary>
    public long ByteLength => _buffer.ByteLength;

    /// <summary>Address of the first element (64-byte aligned); for layout assertions.</summary>
    internal nint Address => (nint)_slots;

    /// <summary>
    /// Approximate number of queued elements. Exact only when called by the producer or the consumer while the
    /// other side is idle. Reads both indices, so the producer's hot path uses <see cref="CachedCount"/> instead.
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

    /// <summary>
    /// Number of queued elements as the producer's private snapshot of the consumer's index sees it (producer
    /// thread; never touches the consumer's cache line, so it may be too large until the snapshot is refreshed by
    /// <see cref="HasRoomFor"/>, <see cref="TryEnqueue"/> or <see cref="RefreshedCount"/>).
    /// </summary>
    public int CachedCount => (int)Math.Min(_idx.Tail - _idx.CachedHead, _mask + 1);

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

        _slots[(int)(tail & _mask)] = item;
        Volatile.Write(ref _idx.Tail, tail + 1);
        return true;
    }

    /// <summary>
    /// True when one more element fits while <paramref name="reserved"/> further slots count as taken (producer
    /// thread). The consumer's index is read only when the producer's private snapshot says the ring is full
    /// (ADR 0008 invariant 5), so a steady-state check costs no coherence traffic.
    /// </summary>
    /// <param name="reserved">Slots the producer has promised to fill later (0 for a plain enqueue).</param>
    /// <returns><see langword="false"/> when the ring is full counting <paramref name="reserved"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool HasRoomFor(int reserved)
    {
        long tail = _idx.Tail;
        if (tail - _idx.CachedHead + reserved > _mask)
        {
            _idx.CachedHead = Volatile.Read(ref _idx.Head);
            return tail - _idx.CachedHead + reserved <= _mask;
        }

        return true;
    }

    /// <summary>
    /// Appends an element while <paramref name="reserved"/> further slots count as taken (producer thread;
    /// <see cref="HasRoomFor"/> plus the store).
    /// </summary>
    /// <param name="item">Element to copy into the ring.</param>
    /// <param name="reserved">Slots the producer has promised to fill later.</param>
    /// <returns><see langword="false"/> when the ring is full counting <paramref name="reserved"/>; the element is not stored.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueueReserving(in T item, int reserved)
    {
        if (!HasRoomFor(reserved))
            return false;

        long tail = _idx.Tail;
        _slots[(int)(tail & _mask)] = item;
        Volatile.Write(ref _idx.Tail, tail + 1);
        return true;
    }

    /// <summary>
    /// Refreshes the producer's snapshot of the consumer's index and returns the exact number of queued elements
    /// (producer thread; one read of the consumer's cache line, for diagnostics that must not be pessimistic).
    /// </summary>
    /// <returns>The queued elements.</returns>
    public int RefreshedCount()
    {
        _idx.CachedHead = Volatile.Read(ref _idx.Head);
        return (int)Math.Min(_idx.Tail - _idx.CachedHead, _mask + 1);
    }

    /// <summary>Removes the oldest element. Consumer thread only.</summary>
    /// <param name="item">
    /// Receives the element. <b>Undefined when the call returns <see langword="false"/></b> (ADR 0008 invariant 5): the
    /// failure path does not write it at all, so it never holds a plausible value — a <see langword="default"/> there is a
    /// valid index for an index-typed element, which is how a live record once got recycled.
    /// </param>
    /// <returns><see langword="false"/> when the ring is empty.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue([MaybeNullWhen(false)] out T item)
    {
        long head = _idx.Head;
        if (head == _idx.CachedTail)
        {
            _idx.CachedTail = Volatile.Read(ref _idx.Tail);
            if (head == _idx.CachedTail)
            {
                // Deliberately left uninitialised; see the parameter's documentation.
                Unsafe.SkipInit(out item);
                return false;
            }
        }

        item = _slots[(int)(head & _mask)];
        Volatile.Write(ref _idx.Head, head + 1);
        return true;
    }

    /// <summary>Copies the oldest element without removing it. Consumer thread only.</summary>
    /// <param name="item">
    /// Receives a copy of the element. <b>Undefined when the call returns <see langword="false"/></b>, as for
    /// <see cref="TryDequeue"/>.
    /// </param>
    /// <returns><see langword="false"/> when the ring is empty.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryPeek([MaybeNullWhen(false)] out T item)
    {
        long head = _idx.Head;
        if (head == _idx.CachedTail)
        {
            _idx.CachedTail = Volatile.Read(ref _idx.Tail);
            if (head == _idx.CachedTail)
            {
                Unsafe.SkipInit(out item);
                return false;
            }
        }

        item = _slots[(int)(head & _mask)];
        return true;
    }

    /// <summary>Frees the native element buffer. Safe to call more than once; must not race with element access.</summary>
    public void Dispose() => _buffer.Dispose();
}
