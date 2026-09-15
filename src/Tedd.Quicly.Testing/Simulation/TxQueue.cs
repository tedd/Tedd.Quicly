namespace Tedd.Quicly.Testing.Simulation;

/// <summary>A packet waiting for (or occupying) a direction's serializer.</summary>
internal struct TxPacket
{
    public long Seq;
    public int Priority;
    public bool IsDatagram;
    public bool Fin;
    public int Bytes;
    /// <summary>Datagram: record index. Stream: send record index.</summary>
    public int Record;
    public uint RecordGeneration;
    public int BufferOffset;
    public long StreamOffset;
}

/// <summary>Max-heap of packets by priority, FIFO among equal priorities.</summary>
internal sealed class TxQueue
{
    private TxPacket[] _items = new TxPacket[64];
    private int _count;
    private long _nextSeq;

    public int Count => _count;

    public long QueuedBytes { get; private set; }

    public void Push(TxPacket packet)
    {
        packet.Seq = _nextSeq++;
        Insert(packet);
    }

    private void Insert(TxPacket packet)
    {
        if (_count == _items.Length)
            Array.Resize(ref _items, _items.Length * 2);
        int i = _count++;
        while (i > 0)
        {
            int parent = (i - 1) >> 1;
            if (!Before(ref packet, ref _items[parent]))
                break;
            _items[i] = _items[parent];
            i = parent;
        }
        _items[i] = packet;
        QueuedBytes += packet.Bytes;
    }

    public TxPacket Pop()
    {
        TxPacket top = _items[0];
        QueuedBytes -= top.Bytes;
        int last = --_count;
        if (last > 0)
        {
            TxPacket item = _items[last];
            int i = 0;
            while (true)
            {
                int child = (i << 1) + 1;
                if (child >= last)
                    break;
                if (child + 1 < last && Before(ref _items[child + 1], ref _items[child]))
                    child++;
                if (!Before(ref _items[child], ref item))
                    break;
                _items[i] = _items[child];
                i = child;
            }
            _items[i] = item;
        }
        _items[last] = default;
        return top;
    }

    /// <summary>Removes every datagram larger than <paramref name="maxPayload"/>, appending them to <paramref name="removed"/> in send order.</summary>
    public void RemoveOversizedDatagrams(int maxPayload, List<TxPacket> removed)
    {
        int start = removed.Count;
        TxPacket[] snapshot = _items[.._count];
        Array.Clear(_items, 0, _count);
        _count = 0;
        QueuedBytes = 0;
        foreach (TxPacket packet in snapshot)
        {
            if (packet.IsDatagram && packet.Bytes > maxPayload)
                removed.Add(packet);
            else
                Insert(packet);
        }
        removed.Sort(start, removed.Count - start, SeqComparer.Instance);
    }

    /// <summary>Removes every packet, appending them to <paramref name="removed"/> in send order.</summary>
    public void Clear(List<TxPacket> removed)
    {
        int start = removed.Count;
        for (int i = 0; i < _count; i++)
            removed.Add(_items[i]);
        removed.Sort(start, removed.Count - start, SeqComparer.Instance);
        Array.Clear(_items, 0, _count);
        _count = 0;
        QueuedBytes = 0;
    }

    private static bool Before(ref TxPacket a, ref TxPacket b) => a.Priority > b.Priority || (a.Priority == b.Priority && a.Seq < b.Seq);

    private sealed class SeqComparer : IComparer<TxPacket>
    {
        public static readonly SeqComparer Instance = new();

        public int Compare(TxPacket x, TxPacket y) => x.Seq.CompareTo(y.Seq);
    }
}
