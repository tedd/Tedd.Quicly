using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Http3;

namespace Tedd.Quicly.Transport.MsQuic.WebTransport;

public sealed unsafe partial class WebTransportTransport
{
    private const int SlotFree = 0;
    private const int SlotInUse = 1;

    /// <summary>
    /// One datagram send in flight. The gather array is given back as soon as the transport reports the payload
    /// released; the record itself lives until the send reaches a final <see cref="DatagramSendState"/>, because Core's
    /// context is still needed for the states in between.
    /// </summary>
    private struct DatagramRecord
    {
        public ulong CallerContext;
        public uint Generation;
        public int Scratch;
    }

    private DatagramRecord[] _datagramRecords = [];
    private int[] _datagramRecordState = [];
    private int _datagramRecordHint;

    /// <summary>Gather arrays of unsent datagrams: <see cref="WebTransportOptions.MaxDatagramSegments"/> + 1 segments each.</summary>
    private NativeArray<TransportSegment> _datagramScratch = null!;

    /// <summary>The RFC 9297 prefix of each unsent datagram.</summary>
    private NativeArray<byte> _datagramPrefixes = null!;

    private int[] _datagramScratchState = [];
    private int _datagramScratchHint;
    private int _scratchStride;

    private long _datagramsDroppedForSession;

    private void InitialiseDatagrams(WebTransportOptions options)
    {
        _datagramRecords = new DatagramRecord[options.MaxPendingDatagrams];
        _datagramRecordState = new int[options.MaxPendingDatagrams];
        _scratchStride = options.MaxDatagramSegments + 1;
        _datagramScratch = new NativeArray<TransportSegment>(options.MaxUnsentDatagrams * _scratchStride);
        _datagramPrefixes = new NativeArray<byte>(options.MaxUnsentDatagrams * DatagramPrefixCapacity);
        _datagramScratchState = new int[options.MaxUnsentDatagrams];
    }

    private void DisposeDatagrams()
    {
        _datagramPrefixes.Dispose();
        _datagramScratch.Dispose();
    }

    /// <summary>Datagrams dropped because they named a session this carrier does not have.</summary>
    public long DatagramsDroppedForOtherSession => Volatile.Read(ref _datagramsDroppedForSession);

    /// <inheritdoc/>
    /// <remarks>
    /// The RFC 9297 quarter-stream-id prefix is prepended as one extra gather segment, so the caller's buffers are
    /// never copied. A send with more than <see cref="WebTransportOptions.MaxDatagramSegments"/> segments, or made while
    /// the carrier has no free send slot, is refused and no completion follows.
    /// </remarks>
    public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        ITransport? inner = ConnectedInner();
        if (inner is null || !_sessionIdKnown) return TransportStatus.InvalidState;
        if (count < 0 || count > _scratchStride - 1) return TransportStatus.TooLarge;

        int scratch = RentSlot(_datagramScratchState, ref _datagramScratchHint);
        if (scratch < 0) return TransportStatus.OutOfMemory;

        int record = RentSlot(_datagramRecordState, ref _datagramRecordHint);
        if (record < 0)
        {
            ReturnSlot(_datagramScratchState, scratch);
            return TransportStatus.OutOfMemory;
        }

        byte* prefix = _datagramPrefixes.Pointer + ((long)scratch * DatagramPrefixCapacity);
        int prefixLength = HttpDatagram.WritePrefix(new Span<byte>(prefix, DatagramPrefixCapacity), _sessionId);
        if (prefixLength < 0)
        {
            ReturnSlot(_datagramRecordState, record);
            ReturnSlot(_datagramScratchState, scratch);
            return TransportStatus.Failed;
        }

        TransportSegment* gather = _datagramScratch.Pointer + ((long)scratch * _scratchStride);
        gather[0] = new TransportSegment(prefix, prefixLength);
        for (int i = 0; i < count; i++) gather[i + 1] = segments[i];

        // The completion can arrive inline, so the record is complete before the send is made.
        ref DatagramRecord slot = ref _datagramRecords[record];
        slot.CallerContext = context;
        slot.Scratch = scratch;
        uint generation = slot.Generation == 0 || slot.Generation == uint.MaxValue ? 1u : slot.Generation + 1u;
        Volatile.Write(ref slot.Generation, generation);

        TransportStatus status = inner.SendDatagram(gather, count + 1, ((ulong)generation << 32) | (uint)record, flags);
        if (status != TransportStatus.Success)
        {
            // No completion follows a refused send, so both slots come back here.
            if (Interlocked.Exchange(ref _datagramRecords[record].Scratch, -1) == scratch) ReturnSlot(_datagramScratchState, scratch);
            ReturnSlot(_datagramRecordState, record);
        }

        return status;
    }

    /// <inheritdoc/>
    void ITransportSink.OnDatagramSendStateChanged(ulong context, DatagramSendState state)
    {
        int index = (int)(uint)context;
        uint generation = (uint)(context >> 32);
        if ((uint)index >= (uint)_datagramRecords.Length) return;
        if (Volatile.Read(ref _datagramRecords[index].Generation) != generation) return;
        if (Volatile.Read(ref _datagramRecordState[index]) != SlotInUse) return;

        if (state.ReleasesPayload())
        {
            int scratch = Interlocked.Exchange(ref _datagramRecords[index].Scratch, -1);
            if (scratch >= 0) ReturnSlot(_datagramScratchState, scratch);
        }

        ulong callerContext = _datagramRecords[index].CallerContext;
        if (state.IsFinal())
        {
            Volatile.Write(ref _datagramRecords[index].Generation, generation == uint.MaxValue ? 1u : generation);
            ReturnSlot(_datagramRecordState, index);
        }

        _sink?.OnDatagramSendStateChanged(callerContext, state);
    }

    /// <inheritdoc/>
    void ITransportSink.OnDatagramReceived(ReadOnlySpan<byte> payload)
    {
        if (!HttpDatagram.TryRead(payload, out ulong streamId, out ReadOnlySpan<byte> inner))
        {
            Interlocked.Increment(ref _datagramsDroppedForSession);
            return;
        }

        // A datagram for a session we do not have is dropped, never buffered (PROTOCOL.md §5). A session whose id is
        // known but which is not established yet is one we do not have: the id is read off the CONNECT request, well
        // before the peer's SETTINGS have arrived, and Core has not been told the transport is connected. Unlike a
        // stream there is nothing to hold a datagram in — they are droppable by definition — so it is dropped.
        if (!_sessionIdKnown || streamId != _sessionId || !IsSessionEstablished)
        {
            Interlocked.Increment(ref _datagramsDroppedForSession);
            return;
        }

        _sink?.OnDatagramReceived(inner);
    }

    /// <summary>Releases the datagram pools once the connection is closed and no completion can follow.</summary>
    private void FailPendingDatagrams()
    {
        for (int i = 0; i < _datagramRecordState.Length; i++)
        {
            if (Interlocked.Exchange(ref _datagramRecordState[i], SlotFree) != SlotInUse) continue;
            int scratch = Interlocked.Exchange(ref _datagramRecords[i].Scratch, -1);
            if (scratch >= 0) ReturnSlot(_datagramScratchState, scratch);
        }
    }

    /// <summary>Takes a free slot without locking, starting at <paramref name="hint"/>. Returns -1 when all are in use.</summary>
    private static int RentSlot(int[] state, ref int hint)
    {
        int length = state.Length;
        if (length == 0) return -1;
        int start = Volatile.Read(ref hint);
        for (int i = 0; i < length; i++)
        {
            int index = start + i;
            if (index >= length) index -= length;
            if (Interlocked.CompareExchange(ref state[index], SlotInUse, SlotFree) == SlotFree)
            {
                int next = index + 1;
                Volatile.Write(ref hint, next >= length ? 0 : next);
                return index;
            }
        }

        return -1;
    }

    private static void ReturnSlot(int[] state, int index) => Volatile.Write(ref state[index], SlotFree);
}
