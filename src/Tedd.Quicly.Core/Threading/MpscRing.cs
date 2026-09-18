using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Threading;

/// <summary>
/// Bounded lock-free multi-producer / single-consumer ring buffer (Vyukov's bounded queue) over native memory.
/// </summary>
/// <remarks>
/// <para>
/// Any number of threads may call <see cref="TryEnqueue"/> concurrently; exactly one thread may call
/// <see cref="TryDequeue"/> / <see cref="TryDequeueBatch"/> at any time. Every slot carries its own sequence
/// number, so producers never contend on anything but the enqueue position (one compare-exchange per element)
/// and the consumer never touches the enqueue position at all. Elements are dequeued in the order in which
/// producers claimed their slots.
/// </para>
/// <para>
/// The slots live in a <see cref="NativeArray{T}"/>, so the block is 64-byte aligned (ADR 0008 invariant 12) and
/// a slot whose sequence word plus value is 64 bytes — the peer's <c>ForeignSend</c> front — occupies exactly one
/// cache line. The ring never allocates after construction. <see cref="Dispose"/> frees the native memory and must
/// not race with element access; a finalizer frees it if the owner forgot.
/// </para>
/// <para>
/// An internal owner whose producers may still enqueue after it is done with the ring (the completion table's free
/// list: a wait consumed after the table was disposed returns its slot) creates it with <c>pinnedObjectHeap</c>: the
/// block is then a reference-free byte array on the pinned object heap (the other storage invariant 12 allows), still
/// 64-byte aligned, which <see cref="Dispose"/> leaves alone and the GC reclaims once nothing references the ring, so a
/// late enqueue writes into live memory instead of freed memory.
/// </para>
/// </remarks>
/// <typeparam name="T">Element type; must be unmanaged so that the buffer is a flat block of values.</typeparam>
public sealed unsafe class MpscRing<T> : IDisposable where T : unmanaged
{
    private struct Slot
    {
        public long Sequence;
        public T Value;
    }

    private readonly NativeArray<Slot>? _buffer;
    private readonly byte[]? _pinned;
    private readonly Slot* _slots;
    private readonly int _mask;
    private MpscPositions _pos;

    /// <summary>Creates a ring that holds at least <paramref name="minimumCapacity"/> elements.</summary>
    /// <param name="minimumCapacity">Requested capacity; rounded up to the next power of two (minimum 2).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="minimumCapacity"/> is not positive or exceeds 2^30.</exception>
    public MpscRing(int minimumCapacity)
        : this(minimumCapacity, pinnedObjectHeap: false)
    {
    }

    /// <summary>Creates a ring whose slots live in native memory or, with <paramref name="pinnedObjectHeap"/>, on the pinned object heap.</summary>
    /// <param name="minimumCapacity">Requested capacity; rounded up to the next power of two (minimum 2).</param>
    /// <param name="pinnedObjectHeap">
    /// Keep the slots in GC-managed memory (a reference-free byte array on the pinned object heap) whose lifetime is the
    /// ring's reachability: <see cref="Dispose"/> frees nothing, and an enqueue after it is harmless.
    /// </param>
    internal MpscRing(int minimumCapacity, bool pinnedObjectHeap)
    {
        int capacity = SpscRing<T>.RoundUpCapacity(minimumCapacity);
        if (pinnedObjectHeap)
        {
            // Pinned-heap objects never move, so the aligned start computed once stays valid for the array's lifetime,
            // and the array lives as long as this ring references it.
            _pinned = GC.AllocateUninitializedArray<byte>(capacity * sizeof(Slot) + CacheLine.Size - 1, pinned: true);
            nint start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_pinned));
            _slots = (Slot*)((start + (CacheLine.Size - 1)) & ~(nint)(CacheLine.Size - 1));
        }
        else
        {
            _buffer = new NativeArray<Slot>(capacity);
            _slots = _buffer.Pointer;
        }

        _mask = capacity - 1;
        for (int i = 0; i < capacity; i++)
            _slots[i].Sequence = i;
    }

    /// <summary>Number of elements the ring can hold. Always a power of two.</summary>
    public int Capacity => _mask + 1;

    /// <summary>Size of one slot in bytes (the sequence word plus the element).</summary>
    public static int SlotSize => sizeof(Slot);

    /// <summary>Address of the first slot (64-byte aligned); for layout assertions.</summary>
    internal nint Address => (nint)_slots;

    /// <summary>
    /// Approximate number of queued elements (claimed slots, including ones a producer has not yet finished
    /// writing).
    /// </summary>
    public int Count
    {
        get
        {
            // Dequeue is read first, so concurrent producers can only make the difference too large, never negative.
            long dequeue = Volatile.Read(ref _pos.Dequeue);
            return (int)Math.Min(Volatile.Read(ref _pos.Enqueue) - dequeue, _mask + 1);
        }
    }

    /// <summary>True when no element is queued (approximate; see <see cref="Count"/>).</summary>
    public bool IsEmpty => Volatile.Read(ref _pos.Enqueue) == Volatile.Read(ref _pos.Dequeue);

    /// <summary>Appends an element. Safe to call from any number of threads concurrently.</summary>
    /// <param name="item">Element to copy into the ring.</param>
    /// <returns><see langword="false"/> when the ring is full; the element is not stored.</returns>
    public bool TryEnqueue(in T item)
    {
        Slot* slots = _slots;
        long pos = Volatile.Read(ref _pos.Enqueue);
        while (true)
        {
            ref Slot slot = ref slots[(int)(pos & _mask)];
            long diff = Volatile.Read(ref slot.Sequence) - pos;
            if (diff == 0)
            {
                long seen = Interlocked.CompareExchange(ref _pos.Enqueue, pos + 1, pos);
                if (seen == pos)
                {
                    slot.Value = item;
                    Volatile.Write(ref slot.Sequence, pos + 1);

                    // The slot is addressed through a raw pointer, which does not keep the block alive: keep the ring (and
                    // with it a pinned-heap block) reachable until the element is published.
                    GC.KeepAlive(this);
                    return true;
                }

                pos = seen;
            }
            else if (diff < 0)
            {
                // The slot still holds an element from the previous lap: the ring is full.
                return false;
            }
            else
            {
                // Another producer claimed this slot after we read the position; reload and retry.
                pos = Volatile.Read(ref _pos.Enqueue);
            }
        }
    }

    /// <summary>Removes the oldest element. Consumer thread only.</summary>
    /// <param name="item">
    /// Receives the element. <b>Undefined when the call returns <see langword="false"/></b> (ADR 0008 invariant 5): the
    /// failure path does not write it at all, so it never holds a plausible value — a <see langword="default"/> there is a
    /// valid index for an index-typed element.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the ring is empty, or when the oldest claimed slot has not yet been written
    /// by its producer (it will be readable a few instructions later).
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue([MaybeNullWhen(false)] out T item)
    {
        long pos = _pos.Dequeue;
        ref Slot slot = ref _slots[(int)(pos & _mask)];
        if (Volatile.Read(ref slot.Sequence) != pos + 1)
        {
            // Deliberately left uninitialised; see the parameter's documentation.
            Unsafe.SkipInit(out item);
            return false;
        }

        item = slot.Value;
        Volatile.Write(ref slot.Sequence, pos + _mask + 1);
        Volatile.Write(ref _pos.Dequeue, pos + 1);
        return true;
    }

    /// <summary>Removes up to <paramref name="destination"/>.Length elements in FIFO order. Consumer thread only.</summary>
    /// <param name="destination">Receives the dequeued elements, oldest first.</param>
    /// <returns>Number of elements written to <paramref name="destination"/>.</returns>
    public int TryDequeueBatch(Span<T> destination)
    {
        Slot* slots = _slots;
        long pos = _pos.Dequeue;
        int count = 0;
        while (count < destination.Length)
        {
            ref Slot slot = ref slots[(int)(pos & _mask)];
            if (Volatile.Read(ref slot.Sequence) != pos + 1)
                break;

            destination[count++] = slot.Value;
            Volatile.Write(ref slot.Sequence, pos + _mask + 1);
            pos++;
        }

        if (count != 0)
            Volatile.Write(ref _pos.Dequeue, pos);
        return count;
    }

    /// <summary>
    /// Frees the native slot block. Safe to call more than once; must not race with element access. A ring on the pinned
    /// object heap frees nothing here (the GC reclaims it once unreachable), so for it element access after this call is safe.
    /// </summary>
    public void Dispose() => _buffer?.Dispose();
}
