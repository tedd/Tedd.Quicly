using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Testing.Simulation;

public sealed unsafe partial class SimulatedTransport
{
    /// <summary>Serializer priority of datagrams: above every stream (streams use 0..65535, plus 65536 with the Priority flag).</summary>
    private const int DatagramPriority = 2 * 65536;
    private const int PriorityFlagBoost = 65536;

    private struct DatagramRecord
    {
        public uint Generation;
        public bool InUse;
        public bool SentReported;
        public bool InFlight;
        /// <summary>Completed (canceled) by this end's close but kept alive so an arrival already on the wire still reaches the peer.</summary>
        public bool Orphaned;
        public ulong Context;
        public byte[]? Buffer;
        public int Length;
    }

    private DatagramRecord[] _datagrams = new DatagramRecord[64];
    private int[] _freeDatagrams = new int[64];
    private int _freeDatagramCount;
    private int _datagramHighWater;
    private readonly TxQueue _tx = new();
    private bool _txBusy;
    private TxPacket _txCurrent;
    private readonly List<TxPacket> _scratch = new();

    /// <inheritdoc/>
    /// <remarks>
    /// Returns <see cref="TransportStatus.InvalidState"/> unless connected with datagrams enabled, and
    /// <see cref="TransportStatus.TooLarge"/> when the gathered payload exceeds the current maximum datagram payload.
    /// </remarks>
    public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        long total = SumSegments(segments, count);
        lock (_network.Gate)
        {
            if (_state != TransportState.Connected || _network.IsDisposed)
                return TransportStatus.InvalidState;
            LinkOptions o = Link.Options;
            if (!o.DatagramsEnabled)
                return TransportStatus.InvalidState;
            if (total > Link.MaxPayload)
                return TransportStatus.TooLarge;

            int index = AllocDatagram(out uint generation);
            ref DatagramRecord record = ref _datagrams[index];
            record.Context = context;
            record.Length = (int)total;
            record.Buffer = _network.Pool.Rent((int)total);
            CopySegments(segments, count, record.Buffer);
            LinkStats.DatagramsSent++;
            LinkStats.DatagramBytesSent += total;
            long now = _network.NowMicros;

            if (o.BandwidthBitsPerSecond == 0)
            {
                Post(SimEventKind.DatagramSent, now, this, index, generation);
                DepartDatagram(index, generation, now);
                return TransportStatus.Success;
            }

            bool cancelOnBlocked = (flags & TransportSendFlags.CancelOnBlocked) != 0;
            if (_tx.QueuedBytes + total > o.MaxQueueBytes)
            {
                LinkStats.DatagramsDropped++;
                if (cancelOnBlocked)
                {
                    LinkStats.DatagramsCanceled++;
                    Post(SimEventKind.DatagramFinal, now, this, index, generation, b0: (byte)DatagramSendState.Canceled);
                }
                else
                {
                    Post(SimEventKind.DatagramSent, now, this, index, generation);
                    Post(SimEventKind.DatagramFinal, now + 2 * o.DelayMicros, this, index, generation, b0: (byte)DatagramSendState.LostDiscarded);
                }
                return TransportStatus.Success;
            }
            if (cancelOnBlocked && (_txBusy || _tx.Count > 0))
            {
                // Cannot go out immediately: CancelOnBlocked drops instead of queueing.
                LinkStats.DatagramsCanceled++;
                Post(SimEventKind.DatagramFinal, now, this, index, generation, b0: (byte)DatagramSendState.Canceled);
                return TransportStatus.Success;
            }

            _tx.Push(new TxPacket
            {
                IsDatagram = true,
                Priority = DatagramPriority + ((flags & TransportSendFlags.Priority) != 0 ? 1 : 0),
                Bytes = (int)total,
                Record = index,
                RecordGeneration = generation,
            });
            NoteQueueDepth();
            if (!_txBusy)
                StartNextTx();
            return TransportStatus.Success;
        }
    }

    private int AllocDatagram(out uint generation)
    {
        int index;
        if (_freeDatagramCount > 0)
        {
            index = _freeDatagrams[--_freeDatagramCount];
        }
        else
        {
            if (_datagramHighWater == _datagrams.Length)
            {
                Array.Resize(ref _datagrams, _datagrams.Length * 2);
                Array.Resize(ref _freeDatagrams, _freeDatagrams.Length * 2);
            }
            index = _datagramHighWater++;
        }
        ref DatagramRecord record = ref _datagrams[index];
        record.InUse = true;
        record.SentReported = false;
        record.InFlight = false;
        record.Orphaned = false;
        record.Generation = record.Generation % uint.MaxValue + 1;
        generation = record.Generation;
        return index;
    }

    private void FreeDatagram(int index)
    {
        ref DatagramRecord record = ref _datagrams[index];
        if (record.InFlight)
            _bytesInFlight -= record.Length;
        _network.Pool.Return(record.Buffer);
        record.Buffer = null;
        record.InUse = false;
        _freeDatagrams[_freeDatagramCount++] = index;
    }

    private bool IsLiveDatagram(int index, uint generation) =>
        (uint)index < (uint)_datagramHighWater && _datagrams[index].InUse && _datagrams[index].Generation == generation;

    private void NoteQueueDepth()
    {
        if (_tx.QueuedBytes > LinkStats.MaxQueuedBytes)
            LinkStats.MaxQueuedBytes = _tx.QueuedBytes;
    }

    private void StartNextTx()
    {
        TxPacket packet = _tx.Pop();
        _txBusy = true;
        _txCurrent = packet;
        long now = _network.NowMicros;
        if (packet.IsDatagram)
            Post(SimEventKind.DatagramSent, now, this, packet.Record, packet.RecordGeneration);
        long bandwidth = Link.Options.BandwidthBitsPerSecond;
        long micros = (packet.Bytes * 8L * 1_000_000 + bandwidth - 1) / bandwidth;
        Post(SimEventKind.TxDone, now + micros, this);
    }

    private void OnTxDoneEvent()
    {
        _txBusy = false;
        if (_state != TransportState.Connected)
            return;
        TxPacket packet = _txCurrent;
        _txCurrent = default;
        long now = _network.NowMicros;
        if (packet.IsDatagram)
        {
            if (IsLiveDatagram(packet.Record, packet.RecordGeneration))
                DepartDatagram(packet.Record, packet.RecordGeneration, now);
        }
        else
        {
            DepartChunk(packet.Record, packet.RecordGeneration, packet.BufferOffset, packet.Bytes, packet.StreamOffset, packet.Fin, now);
        }
        if (_tx.Count > 0)
            StartNextTx();
    }

    private void DepartDatagram(int index, uint generation, long now)
    {
        ref DatagramRecord record = ref _datagrams[index];
        LinkOptions o = Link.Options;
        record.InFlight = true;
        _bytesInFlight += record.Length;
        NoteBytesInFlight();
        _sendPackets++;
        _sendBytes += (ulong)record.Length;
        ref DeterministicRandom random = ref _network.Random;
        bool lost;
        if (_forcedDatagramLosses > 0)
        {
            _forcedDatagramLosses--;
            lost = true;
        }
        else
        {
            lost = random.Chance(o.LossPercent);
        }
        if (lost)
        {
            LinkStats.DatagramsLost++;
            _suspectedLost++;
            Post(SimEventKind.DatagramFinal, now + 2 * o.DelayMicros, this, index, generation, b0: (byte)DatagramSendState.LostDiscarded);
            return;
        }
        long arrival = now + o.DelayMicros + random.NextInt64(o.JitterMicros + 1);
        if (random.Chance(o.ReorderPercent))
        {
            LinkStats.DatagramsReordered++;
            arrival += 1 + random.NextInt64(Math.Max(o.DelayMicros, 1000));
        }
        Post(SimEventKind.DatagramArrive, arrival, Peer!, index, generation, obj: this);
    }

    /// <remarks>
    /// A record's single Sent event is posted before any event that can free it (its final state, or the close that
    /// cancels it, which also stops event delivery), so the record is always live here.
    /// </remarks>
    private void OnDatagramSentEvent(ref SimEvent e)
    {
        if (!_network.Invariant(IsLiveDatagram(e.I0, e.G0) && !_datagrams[e.I0].SentReported))
            return;
        ref DatagramRecord record = ref _datagrams[e.I0];
        record.SentReported = true;
        Sink!.OnDatagramSendStateChanged(record.Context, DatagramSendState.Sent);
    }

    /// <summary>Runs on the receiver; the record lives on the sender (<see cref="SimEvent.Obj"/>).</summary>
    /// <remarks>
    /// The record is live: its final state is posted only after this arrival (or instead of it, on loss), and a sender
    /// close keeps an in-flight record (orphaned) until this end has closed, after which no event reaches this end.
    /// </remarks>
    private void OnDatagramArriveEvent(ref SimEvent e)
    {
        SimulatedTransport sender = (SimulatedTransport)e.Obj!;
        if (!_network.Invariant(sender.IsLiveDatagram(e.I0, e.G0)))
            return;
        ref DatagramRecord record = ref sender._datagrams[e.I0];
        long now = _network.NowMicros;
        long delay = Link.Options.DelayMicros;
        if (_state != TransportState.Connected)
        {
            sender.LinkStats.DatagramsLost++;
            Post(SimEventKind.DatagramFinal, now + delay, sender, e.I0, e.G0, b0: (byte)DatagramSendState.LostDiscarded);
            return;
        }
        sender.LinkStats.DatagramsDelivered++;
        sender.LinkStats.DatagramBytesDelivered += record.Length;
        _recvPackets++;
        _recvBytes += (ulong)record.Length;
        Sink!.OnDatagramReceived(new ReadOnlySpan<byte>(record.Buffer, 0, record.Length));
        Post(SimEventKind.DatagramFinal, now + delay, sender, e.I0, e.G0, b0: (byte)DatagramSendState.Acknowledged);
    }

    /// <remarks>
    /// Exactly one final event is posted per record and nothing else frees a record while it is pending, except the
    /// connection close, after which no event is delivered.
    /// </remarks>
    private void OnDatagramFinalEvent(ref SimEvent e)
    {
        if (!_network.Invariant(IsLiveDatagram(e.I0, e.G0)))
            return;
        ref DatagramRecord record = ref _datagrams[e.I0];
        DatagramSendState state = (DatagramSendState)e.B0;
        ulong context = record.Context;
        bool report = Link.Options.DatagramSendStateReporting || (state == DatagramSendState.Canceled && !record.SentReported);
        FreeDatagram(e.I0);
        if (report)
            Sink!.OnDatagramSendStateChanged(context, state);
    }

    /// <summary>
    /// Completes a datagram as canceled right now (inside a dispatch). With <paramref name="keepForPeer"/> a datagram
    /// already on the wire keeps its payload (orphaned) so it can still arrive; <see cref="ReleaseOrphans"/> frees it.
    /// </summary>
    private void CancelDatagram(int index, uint generation, bool keepForPeer = false)
    {
        if (!_network.Invariant(IsLiveDatagram(index, generation)))
            return;
        ref DatagramRecord record = ref _datagrams[index];
        ulong context = record.Context;
        bool report = Link.Options.DatagramSendStateReporting || !record.SentReported;
        if (keepForPeer && record.InFlight)
            record.Orphaned = true;
        else
            FreeDatagram(index);
        LinkStats.DatagramsCanceled++;
        if (report)
            Sink!.OnDatagramSendStateChanged(context, DatagramSendState.Canceled);
    }

    private void CancelAllDatagrams(bool keepForPeer)
    {
        for (int i = 0; i < _datagramHighWater; i++)
        {
            if (_datagrams[i].InUse)
                CancelDatagram(i, _datagrams[i].Generation, keepForPeer);
        }
    }

    /// <summary>The link's MTU changed: report the capability, then cancel queued datagrams that no longer fit.</summary>
    internal void OnMtuChanged()
    {
        if (_closedDelivered || Sink is null || _state is TransportState.Closing or TransportState.Closed)
            return;
        Sink.OnDatagramCapabilityChanged(Link.Options.DatagramsEnabled, Link.MaxPayload);
        _scratch.Clear();
        _tx.RemoveOversizedDatagrams(Link.MaxPayload, _scratch);
        for (int i = 0; i < _scratch.Count; i++)
            CancelDatagram(_scratch[i].Record, _scratch[i].RecordGeneration);
        _scratch.Clear();
    }
}
