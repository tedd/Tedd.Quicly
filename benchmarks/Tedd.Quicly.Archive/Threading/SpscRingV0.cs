using System.Numerics;

namespace Tedd.Quicly.Archive.Threading;

/// <summary>
/// V0 of the SPSC ring: correct, minimal. Volatile head/tail with no padding and no cached snapshot of the
/// other side's index, so every enqueue reads the consumer's line and every dequeue reads the producer's line.
/// Superseded by <c>Tedd.Quicly.Core.Threading.SpscRing&lt;T&gt;</c> (padded, cached indices); kept for the
/// benchmark comparison in docs/benchmarks/threading.md.
/// </summary>
public sealed class SpscRingV0<T> where T : unmanaged
{
    private readonly T[] _buffer;
    private readonly int _mask;
    private long _head;
    private long _tail;

    public SpscRingV0(int minimumCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(minimumCapacity, 1);
        int capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(minimumCapacity, 2));
        _buffer = new T[capacity];
        _mask = capacity - 1;
    }

    public int Capacity => _mask + 1;

    public int Count => (int)(Volatile.Read(ref _tail) - Volatile.Read(ref _head));

    public bool IsEmpty => Volatile.Read(ref _tail) == Volatile.Read(ref _head);

    public bool TryEnqueue(in T item)
    {
        long tail = Volatile.Read(ref _tail);
        if (tail - Volatile.Read(ref _head) > _mask)
            return false;
        _buffer[(int)(tail & _mask)] = item;
        Volatile.Write(ref _tail, tail + 1);
        return true;
    }

    public bool TryDequeue(out T item)
    {
        long head = Volatile.Read(ref _head);
        if (head == Volatile.Read(ref _tail))
        {
            item = default;
            return false;
        }

        item = _buffer[(int)(head & _mask)];
        Volatile.Write(ref _head, head + 1);
        return true;
    }
}
