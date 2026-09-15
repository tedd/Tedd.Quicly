using System.Diagnostics.CodeAnalysis;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.State;

/// <summary>
/// Native arena of <see cref="TransportSegment"/>s from which a stream gather takes one contiguous run per submission
/// (ADR 0008 invariant 1 as amended): send entries are not adjacent <c>QUIC_BUFFER</c>s, so a multi-entry stream send
/// copies the entries' header/payload pairs into a run that stays reserved until <c>OnStreamSendCompleted</c>.
/// </summary>
/// <remarks>
/// <para>Allocation is circular: runs are handed out in order from a tail position and wrap to the start of the arena
/// when they do not fit before its end (the unused end is booked as a pre-freed padding run). Runs may be freed in any
/// order (completions of different streams interleave); the space of a freed run is reused once every older run has
/// been freed too, which matches the mostly-FIFO completion order of stream sends. A run never moves, and its memory is
/// native, so the transport may hold a pointer to it until the completion.</para>
/// <para>Game thread only (allocation and free both happen on the owner thread: the free is driven by the completion
/// drained from the completion ring). Allocation-free after construction.</para>
/// </remarks>
public sealed unsafe class SegmentArena : IDisposable
{
    private readonly NativeArray<TransportSegment> _segments;
    private readonly NativeArray<int> _runLength;
    private readonly NativeArray<byte> _freed;
    private int _head;
    private int _tail;
    private int _used;

    /// <summary>Creates an arena of <paramref name="capacity"/> segments.</summary>
    /// <param name="capacity">Number of segments (1 … 2^24).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is out of range.</exception>
    public SegmentArena(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(capacity, 1 << 24);
        _segments = new NativeArray<TransportSegment>(capacity);
        _runLength = new NativeArray<int>(capacity);
        _freed = new NativeArray<byte>(capacity);
    }

    /// <summary>Number of segments in the arena.</summary>
    public int Capacity => _segments.Length;

    /// <summary>Segments covered by live runs (including padding booked for a wrap).</summary>
    public int Used => _used;

    /// <summary>Number of live (allocated, not yet reclaimed) segments available for a run right now, ignoring fragmentation.</summary>
    public int Available => _segments.Length - _used;

    /// <summary>
    /// Reserves <paramref name="count"/> contiguous segments. Owner thread.
    /// </summary>
    /// <param name="count">Number of segments (1 … <see cref="Capacity"/>).</param>
    /// <param name="start">Index of the first segment of the run, or -1.</param>
    /// <returns><see langword="false"/> when no contiguous run of that size is free yet.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is out of range.</exception>
    public bool TryAllocate(int count, out int start)
    {
        int capacity = _segments.Length;
        if (count < 1 || count > capacity)
        {
            ThrowCount(count);
        }

        if (_used == 0)
        {
            _head = 0;
            _tail = 0;
        }

        if (_tail + count > capacity)
        {
            // The run does not fit before the end: book the end as a pre-freed padding run and wrap. That needs `count`
            // free segments at the start, i.e. count <= head (the live region is [head, tail) and does not wrap here).
            int pad = capacity - _tail;
            if (_used + pad + count > capacity)
            {
                start = -1;
                return false;
            }

            _runLength.Pointer[_tail] = pad;
            _freed.Pointer[_tail] = 1;
            _used += pad;
            _tail = 0;
        }
        else if (_used + count > capacity)
        {
            start = -1;
            return false;
        }

        start = _tail;
        _runLength.Pointer[start] = count;
        _freed.Pointer[start] = 0;
        _used += count;
        _tail = start + count == capacity ? 0 : start + count;
        return true;
    }

    /// <summary>Pointer to the first segment of the run starting at <paramref name="start"/>. Valid until the run is freed.</summary>
    /// <param name="start">Run start returned by <see cref="TryAllocate"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="start"/> is outside the arena.</exception>
    public TransportSegment* GetPointer(int start)
    {
        if ((uint)start >= (uint)_segments.Length)
        {
            ThrowStart(start);
        }

        return _segments.Pointer + start;
    }

    /// <summary>The run starting at <paramref name="start"/> as a span of <paramref name="count"/> segments.</summary>
    /// <param name="start">Run start returned by <see cref="TryAllocate"/>.</param>
    /// <param name="count">Length of the run.</param>
    /// <exception cref="ArgumentOutOfRangeException">The range is outside the arena.</exception>
    public Span<TransportSegment> GetSpan(int start, int count) => _segments.AsSpan(start, count);

    /// <summary>
    /// Releases the run starting at <paramref name="start"/>. Owner thread. Space is reclaimed once every older run has
    /// been released as well.
    /// </summary>
    /// <param name="start">Run start returned by <see cref="TryAllocate"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="start"/> is outside the arena.</exception>
    /// <exception cref="InvalidOperationException">No live run starts at <paramref name="start"/> (double free).</exception>
    public void Free(int start)
    {
        if ((uint)start >= (uint)_segments.Length)
        {
            ThrowStart(start);
        }

        if (_runLength.Pointer[start] == 0 || _freed.Pointer[start] != 0)
        {
            throw new InvalidOperationException($"No live segment run starts at {start}.");
        }

        _freed.Pointer[start] = 1;
        int capacity = _segments.Length;
        while (_used > 0 && _freed.Pointer[_head] != 0)
        {
            int length = _runLength.Pointer[_head];
            _runLength.Pointer[_head] = 0;
            _freed.Pointer[_head] = 0;
            _used -= length;
            _head += length;
            if (_head == capacity)
            {
                _head = 0;
            }
        }

        if (_used == 0)
        {
            _head = 0;
            _tail = 0;
        }
    }

    /// <summary>Frees the native memory. Idempotent.</summary>
    public void Dispose()
    {
        _segments.Dispose();
        _runLength.Dispose();
        _freed.Dispose();
        _used = 0;
    }

    [DoesNotReturn]
    private static void ThrowCount(int count) =>
        throw new ArgumentOutOfRangeException(nameof(count), count, "A run holds 1 … Capacity segments.");

    [DoesNotReturn]
    private static void ThrowStart(int start) =>
        throw new ArgumentOutOfRangeException(nameof(start), start, "Start is outside the segment arena.");
}
