using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Testing.Simulation;

/// <summary>
/// State of one stream at one endpoint. Instances live in the transport's slot table and are reused after
/// <see cref="ITransport.CloseStream"/> (the slot's generation is bumped).
/// </summary>
internal sealed class SimStream
{
    public uint Generation;
    public bool InUse;
    public bool Local;
    public StreamKind Kind;
    public ulong OpenContext;
    public ushort Priority;
    public bool Started;
    public bool Announced;
    public long QuicId = -1;
    public int PeerSlot = -1;
    public uint PeerGeneration;
    public bool AppClosed;
    public bool ShutdownScheduled;
    public bool ShutdownDelivered;
    public bool CountsTowardLimit;

    /// <summary>The start was refused for the peer's stream limit: the stream never starts (it is dead until closed).</summary>
    public bool StartRefused;

    // Send direction.
    public bool CanSend;
    public bool FinQueued;
    public bool SendDone;
    public long SendOffset;
    private int[] _pending = new int[8];
    private int _pendingHead;
    public int PendingCount;

    // Receive direction.
    public bool CanReceive;
    public bool RecvDone;
    public bool DiscardIncoming;
    public byte[]? RecvBuffer;
    public long BaseOffset;
    public long Head;
    public long Frontier;
    public long HighWater;
    public long FinalSize = -1;
    public bool Pending;
    public bool FinIndicated;

    // Flow control (LinkOptions.StreamReceiveWindowBytes > 0).
    /// <summary>Sender: the highest stream offset the peer lets this end send up to.</summary>
    public long SendLimit = long.MaxValue;
    /// <summary>Receiver: the limit this end last advertised to the sender.</summary>
    public long AdvertisedLimit;
    private TxPacket[] _blocked = new TxPacket[8];
    private int _blockedHead;
    /// <summary>Sender: packets held back by the peer's limit, in stream order.</summary>
    public int BlockedCount;
    private long[] _boundaries = new long[16];
    private int _boundaryHead;
    private int _boundaryCount;
    private readonly List<(long Start, long End)> _held = new();

    public TransportStreamId Id(int slot) => new(slot, Generation);

    public void Reset()
    {
        InUse = false;
        Local = false;
        Kind = default;
        OpenContext = 0;
        Priority = 0;
        Started = false;
        Announced = false;
        QuicId = -1;
        PeerSlot = -1;
        PeerGeneration = 0;
        AppClosed = false;
        ShutdownScheduled = false;
        ShutdownDelivered = false;
        CountsTowardLimit = false;
        StartRefused = false;
        CanSend = false;
        FinQueued = false;
        SendDone = false;
        SendOffset = 0;
        _pendingHead = 0;
        PendingCount = 0;
        CanReceive = false;
        RecvDone = false;
        DiscardIncoming = false;
        BaseOffset = 0;
        Head = 0;
        Frontier = 0;
        HighWater = 0;
        FinalSize = -1;
        Pending = false;
        FinIndicated = false;
        _boundaryHead = 0;
        _boundaryCount = 0;
        _held.Clear();
        SendLimit = long.MaxValue;
        AdvertisedLimit = 0;
        Array.Clear(_blocked);
        _blockedHead = 0;
        BlockedCount = 0;
    }

    // ---- pending send FIFO ----

    public void EnqueueSend(int record)
    {
        if (PendingCount == _pending.Length)
        {
            int[] grown = new int[_pending.Length * 2];
            for (int i = 0; i < PendingCount; i++)
                grown[i] = _pending[(_pendingHead + i) & (_pending.Length - 1)];
            _pending = grown;
            _pendingHead = 0;
        }
        _pending[(_pendingHead + PendingCount) & (_pending.Length - 1)] = record;
        PendingCount++;
    }

    public int PendingAt(int index) => _pending[(_pendingHead + index) & (_pending.Length - 1)];

    public void RemoveSend(int record)
    {
        int i = 0;
        while (i < PendingCount && PendingAt(i) != record)
            i++;
        if (i == PendingCount)
            return;
        for (; i < PendingCount - 1; i++)
            _pending[(_pendingHead + i) & (_pending.Length - 1)] = PendingAt(i + 1);
        PendingCount--;
    }

    public int DequeueSend()
    {
        int record = _pending[_pendingHead];
        _pendingHead = (_pendingHead + 1) & (_pending.Length - 1);
        PendingCount--;
        return record;
    }

    // ---- flow-control hold queue (sender) ----

    public void Block(in TxPacket packet)
    {
        if (BlockedCount == _blocked.Length)
        {
            TxPacket[] grown = new TxPacket[_blocked.Length * 2];
            for (int i = 0; i < BlockedCount; i++)
                grown[i] = _blocked[(_blockedHead + i) & (_blocked.Length - 1)];
            _blocked = grown;
            _blockedHead = 0;
        }
        _blocked[(_blockedHead + BlockedCount) & (_blocked.Length - 1)] = packet;
        BlockedCount++;
    }

    public ref TxPacket PeekBlocked() => ref _blocked[_blockedHead];

    public void DropBlocked()
    {
        _blocked[_blockedHead] = default;
        _blockedHead = (_blockedHead + 1) & (_blocked.Length - 1);
        BlockedCount--;
    }

    // ---- receive reassembly ----

    /// <summary>True when every byte of the stream (and the FIN) has arrived contiguously.</summary>
    public bool FinArrived => FinalSize >= 0 && Frontier == FinalSize;

    /// <summary>Stores a chunk at its absolute offset; returns true when the contiguous frontier advanced (or the FIN arrived in order).</summary>
    /// <remarks>
    /// A chunk that carries no bytes and no FIN (a zero-length send) holds nothing and is ignored: holding it could park
    /// an entry behind the frontier that the merge would never reach. Held entries at or behind the frontier are
    /// dropped by the merge rather than stopping it.
    /// </remarks>
    public bool WriteChunk(SimBufferPool pool, ReadOnlySpan<byte> data, long offset, bool fin)
    {
        if (data.Length == 0 && !fin)
            return false;
        long end = offset + data.Length;
        if (fin)
            FinalSize = end;
        EnsureCapacity(pool, end);
        if (data.Length > 0)
            data.CopyTo(RecvBuffer.AsSpan((int)(offset - BaseOffset)));
        if (end > HighWater)
            HighWater = end;

        if (offset > Frontier)
        {
            int i = 0;
            while (i < _held.Count && _held[i].Start <= offset)
                i++;
            _held.Insert(i, (offset, end));
            return false;
        }

        long before = Frontier;
        if (end > Frontier)
            Advance(end);
        while (_held.Count > 0 && _held[0].Start <= Frontier)
        {
            if (_held[0].End > Frontier)
                Advance(_held[0].End);
            _held.RemoveAt(0);
        }
        return Frontier != before || fin;
    }

    /// <summary>Moves the frontier forward to <paramref name="end"/> (callers guarantee <c>end &gt; Frontier</c>) and records the chunk boundary.</summary>
    private void Advance(long end)
    {
        Frontier = end;
        if (_boundaryCount == _boundaries.Length)
        {
            long[] grown = new long[_boundaries.Length * 2];
            for (int i = 0; i < _boundaryCount; i++)
                grown[i] = _boundaries[(_boundaryHead + i) & (_boundaries.Length - 1)];
            _boundaries = grown;
            _boundaryHead = 0;
        }
        _boundaries[(_boundaryHead + _boundaryCount) & (_boundaries.Length - 1)] = end;
        _boundaryCount++;
    }

    private void EnsureCapacity(SimBufferPool pool, long end)
    {
        long needed = Math.Max(end, HighWater) - BaseOffset;
        if (RecvBuffer is not null && needed <= RecvBuffer.Length)
            return;
        // Compact: drop consumed bytes in front of Head.
        long live = Math.Max(Math.Max(end, HighWater), Head) - Head;
        byte[] target = RecvBuffer is not null && live <= RecvBuffer.Length ? RecvBuffer : pool.Rent(checked((int)Math.Max(live, 1024)));
        if (RecvBuffer is not null)
        {
            int keep = (int)(Math.Max(HighWater, Head) - Head);
            RecvBuffer.AsSpan((int)(Head - BaseOffset), keep).CopyTo(target);
            if (!ReferenceEquals(target, RecvBuffer))
                pool.Return(RecvBuffer);
        }
        RecvBuffer = target;
        BaseOffset = Head;
    }

    /// <summary>Fills <paramref name="segments"/> with the contiguous unconsumed bytes split at chunk boundaries.</summary>
    public unsafe int BuildSegments(ref TransportSegment[] segments)
    {
        while (_boundaryCount > 0 && _boundaries[_boundaryHead] <= Head)
        {
            _boundaryHead = (_boundaryHead + 1) & (_boundaries.Length - 1);
            _boundaryCount--;
        }
        if (Frontier == Head)
            return 0;
        byte* basePtr = SimBufferPool.AddressOf(RecvBuffer!);
        int count = 0;
        long start = Head;
        for (int i = 0; i < _boundaryCount; i++)
        {
            long end = _boundaries[(_boundaryHead + i) & (_boundaries.Length - 1)];
            if (count == segments.Length)
                Array.Resize(ref segments, segments.Length * 2);
            segments[count++] = new TransportSegment(basePtr + (start - BaseOffset), (int)(end - start));
            start = end;
        }
        return count;
    }

    /// <summary>Releases the receive buffer and discards every buffered byte.</summary>
    public void DiscardReceive(SimBufferPool pool)
    {
        pool.Return(RecvBuffer);
        RecvBuffer = null;
        BaseOffset = Head = Frontier = HighWater = 0;
        _boundaryHead = 0;
        _boundaryCount = 0;
        _held.Clear();
        Pending = false;
    }
}
