namespace Tedd.Quicly.Core.Session;

/// <summary>
/// A small set of disjoint <c>[start, end)</c> byte ranges of one bulk object, kept sorted and merged: the ranges a
/// receiving object has verified, so a second copy of one is refused and "complete" — the verified ranges exactly tiling
/// the object — is sound.
/// </summary>
/// <remarks>
/// The capacity is the point. Each live transfer contributes one contiguous run and the sending driver issues runs in
/// ascending order, so runs merge as fast as they appear and the set collapses to a single range — a 10 GB object in
/// 640 transfers costs one entry, not 640. A handful of entries absorbs the interleaving of the transfers a peer may
/// have in flight; beyond that the caller decides what a too-fragmented object means, because that is a peer sending
/// something no driver of ours produces.
/// </remarks>
/// <param name="capacity">Ranges the set may hold before <see cref="TryAdd"/> starts failing.</param>
internal sealed class BulkRangeSet(int capacity)
{
    private readonly long[] _start = new long[capacity];
    private readonly long[] _end = new long[capacity];

    /// <summary>Ranges held right now.</summary>
    public int Count { get; private set; }

    /// <summary>First byte of range <paramref name="index"/>.</summary>
    public long StartAt(int index) => _start[index];

    /// <summary>Byte after the last of range <paramref name="index"/>.</summary>
    public long EndAt(int index) => _end[index];

    /// <summary>Empties the set.</summary>
    public void Clear() => Count = 0;

    /// <summary>Drops the lowest range.</summary>
    public void RemoveFirst()
    {
        Count--;
        Array.Copy(_start, 1, _start, 0, Count);
        Array.Copy(_end, 1, _end, 0, Count);
    }

    /// <summary>Whether any held range shares a byte with <c>[start, end)</c>.</summary>
    /// <param name="start">First byte.</param>
    /// <param name="end">Byte after the last.</param>
    public bool Intersects(long start, long end)
    {
        for (int i = 0; i < Count; i++)
        {
            if (_start[i] < end && start < _end[i])
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Adds <c>[start, end)</c>, merging it with every range it touches or bridges. False when the set is full and the
    /// new range stands alone, which leaves the set unchanged.
    /// </summary>
    /// <param name="start">First byte.</param>
    /// <param name="end">Byte after the last.</param>
    public bool TryAdd(long start, long end)
    {
        if (end <= start)
        {
            return true;
        }

        int at = 0;
        while (at < Count && _end[at] < start)
        {
            at++;
        }

        // The ranges are sorted and disjoint, so everything the new one touches is contiguous from here.
        int last = at;
        while (last < Count && _start[last] <= end)
        {
            start = Math.Min(start, _start[last]);
            end = Math.Max(end, _end[last]);
            last++;
        }

        int absorbed = last - at;
        if (absorbed == 0)
        {
            if (Count == _start.Length)
            {
                return false;
            }

            int tail = Count - at;
            Array.Copy(_start, at, _start, at + 1, tail);
            Array.Copy(_end, at, _end, at + 1, tail);
            Count++;
        }
        else if (absorbed > 1)
        {
            int tail = Count - last;
            Array.Copy(_start, last, _start, at + 1, tail);
            Array.Copy(_end, last, _end, at + 1, tail);
            Count -= absorbed - 1;
        }

        _start[at] = start;
        _end[at] = end;
        return true;
    }
}
