using System.Numerics;

namespace Tedd.Quicly.Archive.Threading;

/// <summary>
/// V0 of the MPSC ring: the simple correct version, a plain ring protected by a monitor lock on both sides.
/// Superseded by <c>Tedd.Quicly.Core.Threading.MpscRing&lt;T&gt;</c> (Vyukov bounded queue: per-slot sequence
/// numbers, one compare-exchange per enqueue, lock-free consumer); kept for the benchmark comparison in
/// docs/benchmarks/threading.md.
/// </summary>
public sealed class MpscRingV0<T> where T : unmanaged
{
    private readonly T[] _buffer;
    private readonly int _mask;
    private readonly Lock _lock = new();
    private long _head;
    private long _tail;

    public MpscRingV0(int minimumCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(minimumCapacity, 1);
        int capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(minimumCapacity, 2));
        _buffer = new T[capacity];
        _mask = capacity - 1;
    }

    public int Capacity => _mask + 1;

    public int Count
    {
        get
        {
            lock (_lock)
                return (int)(_tail - _head);
        }
    }

    public bool IsEmpty => Count == 0;

    public bool TryEnqueue(in T item)
    {
        lock (_lock)
        {
            if (_tail - _head > _mask)
                return false;
            _buffer[(int)(_tail & _mask)] = item;
            _tail++;
            return true;
        }
    }

    public bool TryDequeue(out T item)
    {
        lock (_lock)
        {
            if (_head == _tail)
            {
                item = default;
                return false;
            }

            item = _buffer[(int)(_head & _mask)];
            _head++;
            return true;
        }
    }

    public int TryDequeueBatch(Span<T> destination)
    {
        lock (_lock)
        {
            int count = 0;
            while (count < destination.Length && _head != _tail)
            {
                destination[count++] = _buffer[(int)(_head & _mask)];
                _head++;
            }

            return count;
        }
    }
}
