namespace Tedd.Quicly.Testing.Simulation;

/// <summary>What a scheduled event does when it becomes due.</summary>
internal enum SimEventKind : byte
{
    CapabilityChanged,
    Connected,
    DatagramSent,
    DatagramArrive,
    DatagramFinal,
    TxDone,
    StreamStarted,
    PeerStreamStarted,
    StreamChunkArrive,
    StreamSendComplete,
    StreamDeliver,
    StreamShutdownComplete,
    StreamReset,
    StreamStopSending,
    StreamsAvailable,
    StreamCreditReturn,
    MtuChange,
    Disconnect,
    CloseLocal,
    ClosePeer,
    ConnectAttempt,
    ConnectFailed,
}

/// <summary>One scheduled event. A flat struct so the queue is a single array (no per-event allocation).</summary>
internal struct SimEvent
{
    public long Due;
    public long Seq;
    public SimEventKind Kind;
    public byte B0;
    public SimulatedTransport? Target;
    public object? Obj;
    public int I0;
    public int I1;
    public uint G0;
    public uint G1;
    public long L0;
    public long L1;
    public ulong U0;
}

/// <summary>Binary min-heap ordered by (due time, insertion sequence): earliest first, FIFO among equal times.</summary>
internal sealed class EventQueue
{
    private SimEvent[] _items = new SimEvent[256];
    private int _count;
    private long _nextSeq;

    public int Count => _count;

    public long PeekDue => _items[0].Due;

    public void Push(ref SimEvent item)
    {
        item.Seq = _nextSeq++;
        if (_count == _items.Length)
            Array.Resize(ref _items, _items.Length * 2);
        int i = _count++;
        while (i > 0)
        {
            int parent = (i - 1) >> 1;
            if (!Less(ref item, ref _items[parent]))
                break;
            _items[i] = _items[parent];
            i = parent;
        }
        _items[i] = item;
    }

    public SimEvent Pop()
    {
        SimEvent top = _items[0];
        int last = --_count;
        if (last > 0)
        {
            SimEvent item = _items[last];
            int i = 0;
            while (true)
            {
                int child = (i << 1) + 1;
                if (child >= last)
                    break;
                if (child + 1 < last && Less(ref _items[child + 1], ref _items[child]))
                    child++;
                if (!Less(ref _items[child], ref item))
                    break;
                _items[i] = _items[child];
                i = child;
            }
            _items[i] = item;
        }
        _items[last] = default;
        return top;
    }

    public void Clear()
    {
        Array.Clear(_items, 0, _count);
        _count = 0;
    }

    private static bool Less(ref SimEvent a, ref SimEvent b) => a.Due < b.Due || (a.Due == b.Due && a.Seq < b.Seq);
}
