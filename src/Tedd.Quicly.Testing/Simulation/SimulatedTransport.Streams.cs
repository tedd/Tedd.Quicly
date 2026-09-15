using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Testing.Simulation;

public sealed unsafe partial class SimulatedTransport
{
    private const ushort DefaultStreamPriority = 32767;
    private const int MaxLossesPerPacket = 32;

    /// <summary>Largest payload one <see cref="SendStream"/> call accepts (1 GiB); larger sends return <see cref="TransportStatus.TooLarge"/>.</summary>
    public const int MaxStreamSendBytes = SimBufferPool.MaxLength;

    private struct SendRecord
    {
        public uint Generation;
        public bool InUse;
        public bool Fin;
        public bool CompletionPosted;
        public bool Canceled;
        /// <summary>Completed (canceled) by this end's close but kept alive so chunks already on the wire still reach the peer.</summary>
        public bool Orphaned;
        public int Stream;
        public uint StreamGeneration;
        public ulong Context;
        public byte[]? Buffer;
        public int Length;
        public long StreamOffset;
        public int ChunksRemaining;
    }

    private SimStream?[] _streams = new SimStream?[16];
    private int[] _freeStreams = new int[16];
    private int _freeStreamCount;
    private int _streamHighWater;
    private SendRecord[] _sends = new SendRecord[32];
    private int[] _freeSends = new int[32];
    private int _freeSendCount;
    private int _sendHighWater;
    private TransportSegment[] _segments = new TransportSegment[16];
    internal ushort AllowPeerBidi;
    internal ushort AllowPeerUni;
    private int _peerAllowsBidi;
    private int _peerAllowsUni;
    private int _localOpenBidi;
    private int _localOpenUni;
    private long _nextBidiIndex;
    private long _nextUniIndex;

    /// <inheritdoc/>
    /// <remarks>Allowed while connecting or connected. The stream consumes peer credit only when it starts.</remarks>
    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
    {
        id = TransportStreamId.None;
        lock (_network.Gate)
        {
            if (_state is not (TransportState.Connecting or TransportState.Connected) || _network.IsDisposed)
                return TransportStatus.InvalidState;
            if (kind is not (StreamKind.Unidirectional or StreamKind.Bidirectional))
                return TransportStatus.NotSupported;
            int slot = AllocStream(out SimStream s);
            s.Local = true;
            s.Announced = true;
            s.Kind = kind;
            s.OpenContext = context;
            s.Priority = priority;
            s.CanSend = true;
            s.CanReceive = kind == StreamKind.Bidirectional;
            s.RecvDone = !s.CanReceive;
            id = s.Id(slot);
            return TransportStatus.Success;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// <see cref="TransportStatus.StreamLimitReached"/> when the peer's concurrent-stream allowance is used up (the stream
    /// stays open and may be started after <see cref="ITransportSink.OnStreamsAvailable"/>).
    /// </remarks>
    public TransportStatus StartStream(TransportStreamId id)
    {
        lock (_network.Gate)
        {
            if (_state != TransportState.Connected || !TryGetStream(id, out SimStream s) || !s.Local || s.Started)
                return TransportStatus.InvalidState;
            return TryStart(id.Slot, s);
        }
    }

    /// <inheritdoc/>
    public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        long total = SumSegments(segments, count);
        lock (_network.Gate)
        {
            if (_state != TransportState.Connected || _network.IsDisposed || !TryGetStream(id, out SimStream s))
                return TransportStatus.InvalidState;
            if (!s.CanSend || s.FinQueued || s.SendDone)
                return TransportStatus.InvalidState;
            if (total > MaxStreamSendBytes)
                return TransportStatus.TooLarge;
            if (!s.Started)
            {
                if (!s.Local || (flags & TransportSendFlags.Start) == 0)
                    return TransportStatus.InvalidState;
                TransportStatus started = TryStart(id.Slot, s);
                if (started != TransportStatus.Success)
                    return started;
            }

            bool fin = (flags & TransportSendFlags.Fin) != 0;
            int record = AllocSend(out uint generation);
            ref SendRecord r = ref _sends[record];
            r.Stream = id.Slot;
            r.StreamGeneration = s.Generation;
            r.Context = context;
            r.Length = (int)total;
            r.Buffer = _network.Pool.Rent(r.Length);
            r.StreamOffset = s.SendOffset;
            r.Fin = fin;
            CopySegments(segments, count, r.Buffer);
            s.SendOffset += total;
            s.FinQueued = fin;
            s.EnqueueSend(record);
            _bytesInFlight += total;
            LinkStats.StreamBytesSent += total;

            int chunkSize = Link.MaxPayload;
            int chunks = total == 0 ? 1 : (int)((total + chunkSize - 1) / chunkSize);
            r.ChunksRemaining = chunks;
            long streamOffset = r.StreamOffset;
            int priority = s.Priority + ((flags & TransportSendFlags.Priority) != 0 ? PriorityFlagBoost : 0);
            bool unlimited = Link.Options.BandwidthBitsPerSecond == 0;
            long now = _network.NowMicros;
            for (int i = 0; i < chunks; i++)
            {
                int offset = i * chunkSize;
                int length = (int)Math.Min(chunkSize, total - offset);
                bool chunkFin = fin && i == chunks - 1;
                if (unlimited)
                {
                    DepartChunk(record, generation, offset, length, streamOffset + offset, chunkFin, now);
                }
                else
                {
                    _tx.Push(new TxPacket
                    {
                        Priority = priority, Bytes = length, Record = record, RecordGeneration = generation,
                        BufferOffset = offset, StreamOffset = streamOffset + offset, Fin = chunkFin,
                    });
                }
            }
            if (!unlimited)
            {
                NoteQueueDepth();
                if (!_txBusy)
                    StartNextTx();
            }
            return TransportStatus.Success;
        }
    }

    /// <inheritdoc/>
    public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
        lock (_network.Gate)
        {
            if (_state is TransportState.Closing or TransportState.Closed || !TryGetStream(id, out SimStream s))
                return;
            if (s.Local && !s.Started)
            {
                // Never started: nothing to abort on the wire, so it is released like CloseStream (ITransport.AbortStream contract).
                s.AppClosed = true;
                AbortCore(id.Slot, s, 0, StreamAbortDirection.Both);
                return;
            }
            AbortCore(id.Slot, s, errorCode, direction);
        }
    }

    /// <inheritdoc/>
    /// <remarks>Stored; under a bandwidth limit the serializer sends higher-priority stream data first.</remarks>
    public void SetStreamPriority(TransportStreamId id, ushort priority)
    {
        lock (_network.Gate)
        {
            if (TryGetStream(id, out SimStream s))
                s.Priority = priority;
        }
    }

    /// <inheritdoc/>
    public long GetQuicStreamId(TransportStreamId id)
    {
        lock (_network.Gate)
            return TryGetStream(id, out SimStream s, allowAppClosed: true) && s.Started ? s.QuicId : -1;
    }

    /// <inheritdoc/>
    /// <remarks>Ignored unless a receive on the stream is pending. The remaining bytes are delivered at the next advance step.</remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bytesConsumed"/> is negative or more than the held-back bytes.</exception>
    public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed)
    {
        lock (_network.Gate)
        {
            if (!TryGetStream(id, out SimStream s) || !s.Pending)
                return;
            ArgumentOutOfRangeException.ThrowIfNegative(bytesConsumed);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(bytesConsumed, s.Frontier - s.Head);
            s.Head += bytesConsumed;
            s.Pending = false;
            Post(SimEventKind.StreamDeliver, _network.NowMicros, this, id.Slot, id.Generation);
        }
    }

    /// <inheritdoc/>
    public void CloseStream(TransportStreamId id)
    {
        lock (_network.Gate)
        {
            if (!TryGetStream(id, out SimStream s))
                return;
            if (s.ShutdownDelivered)
            {
                FreeStream(id.Slot);
                return;
            }
            s.AppClosed = true;
            AbortCore(id.Slot, s, 0, StreamAbortDirection.Both);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A peer that has not connected yet reads the new limits when it connects (they travel with the handshake). A
    /// peer that is already connected sees them one one-way delay later through
    /// <see cref="ITransportSink.OnStreamsAvailable"/>, whether or not this end has finished connecting. A peer that
    /// connects while the update is in flight reads it at connect and then also gets the (redundant) callback.
    /// </remarks>
    public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional)
    {
        lock (_network.Gate)
        {
            if (_state is TransportState.Closing or TransportState.Closed)
                return;
            AllowPeerBidi = bidirectional;
            AllowPeerUni = unidirectional;
            SimulatedTransport? peer = Peer;
            if (peer is not null && (_state == TransportState.Connected || peer._state == TransportState.Connected))
                Post(SimEventKind.StreamsAvailable, _network.NowMicros + Link.Options.DelayMicros, peer, bidirectional, 0, unidirectional);
        }
    }

    // ------------------------------------------------------------------ slots

    private int AllocStream(out SimStream stream)
    {
        int slot;
        if (_freeStreamCount > 0)
        {
            slot = _freeStreams[--_freeStreamCount];
        }
        else
        {
            if (_streamHighWater == _streams.Length)
            {
                Array.Resize(ref _streams, _streams.Length * 2);
                Array.Resize(ref _freeStreams, _freeStreams.Length * 2);
            }
            slot = _streamHighWater++;
        }
        stream = _streams[slot] ??= new SimStream { Generation = 1 };
        stream.InUse = true;
        return slot;
    }

    private void FreeStream(int slot)
    {
        SimStream s = _streams[slot]!;
        s.DiscardReceive(_network.Pool);
        s.Reset();
        s.Generation = s.Generation % uint.MaxValue + 1;
        _freeStreams[_freeStreamCount++] = slot;
    }

    private bool TryGetStream(TransportStreamId id, out SimStream stream, bool allowAppClosed = false)
    {
        stream = null!;
        if ((uint)id.Slot >= (uint)_streamHighWater)
            return false;
        SimStream? candidate = _streams[id.Slot];
        if (candidate is null || !candidate.InUse || candidate.Generation != id.Generation || (candidate.AppClosed && !allowAppClosed))
            return false;
        stream = candidate;
        return true;
    }

    private int AllocSend(out uint generation)
    {
        int index;
        if (_freeSendCount > 0)
        {
            index = _freeSends[--_freeSendCount];
        }
        else
        {
            if (_sendHighWater == _sends.Length)
            {
                Array.Resize(ref _sends, _sends.Length * 2);
                Array.Resize(ref _freeSends, _freeSends.Length * 2);
            }
            index = _sendHighWater++;
        }
        ref SendRecord r = ref _sends[index];
        r.InUse = true;
        r.CompletionPosted = false;
        r.Canceled = false;
        r.Orphaned = false;
        r.Generation = r.Generation % uint.MaxValue + 1;
        generation = r.Generation;
        return index;
    }

    private void FreeSend(int index)
    {
        ref SendRecord r = ref _sends[index];
        _bytesInFlight -= r.Length;
        _network.Pool.Return(r.Buffer);
        r.Buffer = null;
        r.InUse = false;
        _freeSends[_freeSendCount++] = index;
    }

    private bool IsLiveSend(int index, uint generation) =>
        (uint)index < (uint)_sendHighWater && _sends[index].InUse && _sends[index].Generation == generation;

    // ------------------------------------------------------------------ start / send / receive

    private TransportStatus TryStart(int slot, SimStream s)
    {
        bool bidi = s.Kind == StreamKind.Bidirectional;
        if (bidi ? _localOpenBidi >= _peerAllowsBidi : _localOpenUni >= _peerAllowsUni)
            return TransportStatus.StreamLimitReached;
        if (bidi)
            _localOpenBidi++;
        else
            _localOpenUni++;
        SimulatedTransport peer = Peer!;
        s.Started = true;
        s.CountsTowardLimit = true;
        long index = bidi ? _nextBidiIndex++ : _nextUniIndex++;
        s.QuicId = index * 4 + (bidi ? 0 : 2) + (IsClient ? 0 : 1);

        int peerSlot = peer.AllocStream(out SimStream ps);
        ps.Local = false;
        ps.Kind = s.Kind;
        ps.Started = true;
        ps.QuicId = s.QuicId;
        ps.PeerSlot = slot;
        ps.PeerGeneration = s.Generation;
        ps.Priority = DefaultStreamPriority;
        ps.CanSend = bidi;
        ps.SendDone = !bidi;
        ps.CanReceive = true;
        s.PeerSlot = peerSlot;
        s.PeerGeneration = ps.Generation;

        long now = _network.NowMicros;
        Post(SimEventKind.StreamStarted, now, this, slot, s.Generation);
        Post(SimEventKind.PeerStreamStarted, now + Link.Options.DelayMicros, peer, peerSlot, ps.Generation);
        return TransportStatus.Success;
    }

    private void DepartChunk(int record, uint generation, int bufferOffset, int length, long streamOffset, bool fin, long now)
    {
        if (!IsLiveSend(record, generation) || _sends[record].Canceled)
            return;
        // A stream is never released while it has sends pending: shutdown waits for them and a connection close
        // cancels them first (and stops event delivery).
        SimStream s = _streams[_sends[record].Stream]!;
        LinkOptions o = Link.Options;
        ref DeterministicRandom random = ref _network.Random;
        LinkStats.StreamPacketsSent++;
        _sendPackets++;
        _sendBytes += (ulong)length;
        long arrival = now + o.DelayMicros + random.NextInt64(o.JitterMicros + 1);
        int losses = 0;
        if (_forcedStreamLosses > 0)
        {
            _forcedStreamLosses--;
            losses = 1;
        }
        while (losses < MaxLossesPerPacket && random.Chance(o.StreamLossPercent))
            losses++;
        if (losses > 0)
        {
            LinkStats.StreamRetransmissions += losses;
            _suspectedLost += (ulong)losses;
            arrival += losses * o.RetransmitDelayMicros;
        }
        Post(SimEventKind.StreamChunkArrive, arrival, Peer!, s.PeerSlot, s.PeerGeneration, record, generation,
            l0: streamOffset, l1: ((long)bufferOffset << 32) | (uint)length, b0: fin ? (byte)1 : (byte)0, obj: this);
    }

    /// <summary>Runs on the receiver; the send record lives on the sender (<see cref="SimEvent.Obj"/>).</summary>
    private void OnStreamChunkArriveEvent(ref SimEvent e)
    {
        SimulatedTransport sender = (SimulatedTransport)e.Obj!;
        if (!sender.IsLiveSend(e.I1, e.G1) || sender._sends[e.I1].Canceled || _state != TransportState.Connected)
            return;
        if (!TryGetStream(new TransportStreamId(e.I0, e.G0), out SimStream s, allowAppClosed: true) || s.DiscardIncoming || s.RecvDone)
            return;
        ref SendRecord r = ref sender._sends[e.I1];
        int bufferOffset = (int)(e.L1 >> 32);
        int length = (int)(uint)e.L1;
        sender.LinkStats.StreamBytesDelivered += length;
        _recvPackets++;
        _recvBytes += (ulong)length;
        bool advanced = s.WriteChunk(_network.Pool, r.Buffer.AsSpan(bufferOffset, length), e.L0, e.B0 != 0);
        r.ChunksRemaining--;
        if (!sender._closedDelivered) // an orphaned send of a closed sender has already completed canceled
            sender.PostEligibleCompletions(r.Stream, s.Frontier);
        if (advanced)
            TryDeliver(e.I0, s);
    }

    /// <summary>Posts, in order, the completions of every send whose bytes the peer now holds contiguously.</summary>
    private void PostEligibleCompletions(int slot, long frontier)
    {
        SimStream s = _streams[slot]!; // live: it has the send whose chunk just arrived
        long due = _network.NowMicros + Link.Options.DelayMicros;
        for (int i = 0; i < s.PendingCount; i++)
        {
            int record = s.PendingAt(i);
            ref SendRecord r = ref _sends[record];
            if (r.CompletionPosted)
                continue;
            if (r.ChunksRemaining > 0 || frontier < r.StreamOffset + r.Length)
                break;
            r.CompletionPosted = true;
            Post(SimEventKind.StreamSendComplete, due, this, record, r.Generation);
        }
    }

    private void OnStreamSendCompleteEvent(ref SimEvent e)
    {
        if (!IsLiveSend(e.I0, e.G0))
            return;
        ref SendRecord r = ref _sends[e.I0];
        bool canceled = e.B0 != 0;
        if (r.Canceled != canceled)
            return;
        int slot = r.Stream;
        uint streamGeneration = r.StreamGeneration;
        ulong context = r.Context;
        bool fin = r.Fin;
        bool haveStream = TryGetStream(new TransportStreamId(slot, streamGeneration), out SimStream s, allowAppClosed: true);
        if (haveStream)
        {
            s.RemoveSend(e.I0);
            if (fin && !canceled)
                s.SendDone = true;
        }
        FreeSend(e.I0);
        Sink!.OnStreamSendCompleted(new TransportStreamId(slot, streamGeneration), context, canceled);
        if (haveStream)
            CheckShutdown(slot, s);
    }

    /// <summary>
    /// Cancels every send of the stream not yet reported: inline (inside a dispatch) or at the next advance step. With
    /// <paramref name="keepForPeer"/> (connection close) a send with chunks still to arrive keeps its payload (orphaned)
    /// so what is already on the wire still reaches the peer; <see cref="ReleaseOrphans"/> frees it.
    /// </summary>
    private void CancelPendingSends(int slot, SimStream s, bool inline, bool keepForPeer = false)
    {
        if (inline)
        {
            TransportStreamId id = s.Id(slot);
            while (s.PendingCount > 0)
            {
                int record = s.DequeueSend();
                ulong context = _sends[record].Context;
                if (keepForPeer && !_sends[record].Canceled && _sends[record].ChunksRemaining > 0)
                    _sends[record].Orphaned = true;
                else
                    FreeSend(record);
                Sink!.OnStreamSendCompleted(id, context, true);
            }
            return;
        }
        long now = _network.NowMicros;
        for (int i = 0; i < s.PendingCount; i++)
        {
            int record = s.PendingAt(i);
            ref SendRecord r = ref _sends[record];
            if (r.Canceled)
                continue;
            r.Canceled = true;
            r.CompletionPosted = true;
            Post(SimEventKind.StreamSendComplete, now, this, record, r.Generation, b0: 1);
        }
    }

    private void TryDeliver(int slot, SimStream s)
    {
        if (s.Pending || s.RecvDone || s.DiscardIncoming || !s.Announced || s.AppClosed || _state != TransportState.Connected)
            return;
        bool fin = s.FinArrived;
        int count = s.BuildSegments(ref _segments);
        if (count == 0 && !fin)
            return;
        long available = s.Frontier - s.Head;
        uint generation = s.Generation;
        ReceiveResult result = Sink!.OnStreamReceived(s.Id(slot), new ReadOnlySpan<TransportSegment>(_segments, 0, count), (ulong)s.Head, fin);
        if (!s.InUse || s.Generation != generation || s.RecvDone || s.DiscardIncoming || s.AppClosed)
            return;
        if (result.BytesConsumed < 0 || result.BytesConsumed > available)
            throw new InvalidOperationException($"OnStreamReceived consumed {result.BytesConsumed} bytes of {available} delivered.");
        s.Head += result.BytesConsumed;
        if (result.Pending || (result.BytesConsumed == 0 && available > 0))
        {
            // Back-pressure. Consuming nothing of a non-empty indication counts as PendingAfter(0) (ReceiveResult contract).
            s.Pending = true;
            s.FinIndicated = fin;
            return;
        }
        if (s.Head < s.Frontier)
        {
            // Partial consumption: indicate the rest again at the next step, with whatever arrives meanwhile (ReceiveResult contract).
            Post(SimEventKind.StreamDeliver, _network.NowMicros, this, slot, generation);
            return;
        }
        if (fin)
            CompleteReceive(slot, s);
    }

    private void CompleteReceive(int slot, SimStream s)
    {
        s.RecvDone = true;
        s.FinIndicated = false;
        Sink!.OnStreamPeerSendShutdown(s.Id(slot));
        CheckShutdown(slot, s);
    }

    private void AbortCore(int slot, SimStream s, ulong errorCode, StreamAbortDirection direction)
    {
        long due = _network.NowMicros + Link.Options.DelayMicros;
        SimulatedTransport? peer = Peer;
        SimStream? ps = null;
        if (s.Started && peer is not null && peer.TryGetStream(new TransportStreamId(s.PeerSlot, s.PeerGeneration), out SimStream found, allowAppClosed: true))
            ps = found;
        if ((direction & StreamAbortDirection.Send) != 0 && s.CanSend && !s.SendDone)
        {
            s.SendDone = true;
            s.FinQueued = true;
            // The peer keeps delivering what it already holds until the reset arrives; chunks of the canceled sends
            // still in flight are dropped on arrival (their records are marked canceled).
            CancelPendingSends(slot, s, inline: false);
            if (ps is not null)
                Post(SimEventKind.StreamReset, due, peer!, s.PeerSlot, s.PeerGeneration, u0: errorCode);
        }
        if ((direction & StreamAbortDirection.Receive) != 0 && s.CanReceive && !s.RecvDone)
        {
            s.RecvDone = true;
            s.DiscardIncoming = true;
            s.Pending = false;
            if (ps is not null)
                Post(SimEventKind.StreamStopSending, due, peer!, s.PeerSlot, s.PeerGeneration, u0: errorCode);
        }
        CheckShutdown(slot, s);
    }

    private void CheckShutdown(int slot, SimStream s)
    {
        if (!s.InUse || s.ShutdownScheduled || !s.SendDone || !s.RecvDone || s.PendingCount > 0)
            return;
        s.ShutdownScheduled = true;
        Post(SimEventKind.StreamShutdownComplete, _network.NowMicros, this, slot, s.Generation);
    }

    private void RaiseStreamsAvailable() =>
        Sink!.OnStreamsAvailable((ushort)Math.Max(0, _peerAllowsBidi - _localOpenBidi), (ushort)Math.Max(0, _peerAllowsUni - _localOpenUni));

    // ------------------------------------------------------------------ close helpers

    private void CancelAllStreamSends(bool keepForPeer)
    {
        for (int slot = 0; slot < _streamHighWater; slot++)
        {
            SimStream? s = _streams[slot];
            if (s is not null && s.InUse && s.PendingCount > 0)
                CancelPendingSends(slot, s, inline: true, keepForPeer);
        }
    }

    /// <summary>Frees the sends and datagrams kept alive by this end's close; called when the peer can no longer receive them.</summary>
    internal void ReleaseOrphans()
    {
        for (int i = 0; i < _sendHighWater; i++)
        {
            if (_sends[i].InUse && _sends[i].Orphaned)
                FreeSend(i);
        }
        for (int i = 0; i < _datagramHighWater; i++)
        {
            if (_datagrams[i].InUse && _datagrams[i].Orphaned)
                FreeDatagram(i);
        }
    }

    private void ShutdownAllStreams()
    {
        for (int slot = 0; slot < _streamHighWater; slot++)
        {
            SimStream? s = _streams[slot];
            if (s is null || !s.InUse || s.ShutdownDelivered)
                continue;
            s.ShutdownDelivered = true;
            s.SendDone = true;
            s.RecvDone = true;
            if (s.AppClosed)
                FreeStream(slot);
            else
                Sink!.OnStreamShutdownComplete(s.Id(slot));
        }
    }

    // ------------------------------------------------------------------ dispatch

    private void DispatchStreamEvent(ref SimEvent e)
    {
        switch (e.Kind)
        {
            case SimEventKind.StreamChunkArrive:
                OnStreamChunkArriveEvent(ref e);
                return;
            case SimEventKind.StreamSendComplete:
                OnStreamSendCompleteEvent(ref e);
                return;
            case SimEventKind.StreamsAvailable:
                if (_state == TransportState.Connected)
                {
                    _peerAllowsBidi = e.I0;
                    _peerAllowsUni = e.I1;
                    RaiseStreamsAvailable();
                }
                return;
            case SimEventKind.StreamCreditReturn:
                if (_state == TransportState.Connected)
                {
                    if ((StreamKind)e.B0 == StreamKind.Bidirectional)
                        _localOpenBidi--;
                    else
                        _localOpenUni--;
                    RaiseStreamsAvailable();
                }
                return;
        }

        if (!TryGetStream(new TransportStreamId(e.I0, e.G0), out SimStream s, allowAppClosed: true))
            return;
        TransportStreamId id = s.Id(e.I0);
        switch (e.Kind)
        {
            case SimEventKind.StreamStarted:
                if (!s.AppClosed)
                    Sink!.OnStreamStarted(id, s.OpenContext, TransportStatus.Success);
                break;
            case SimEventKind.PeerStreamStarted:
                s.Announced = true;
                Sink!.OnPeerStreamStarted(id, s.Kind);
                TryDeliver(e.I0, s);
                break;
            case SimEventKind.StreamDeliver:
                if (s.Pending || s.RecvDone || s.AppClosed)
                    break;
                if (s.FinIndicated && s.Head == s.Frontier)
                    CompleteReceive(e.I0, s);
                else
                    TryDeliver(e.I0, s);
                break;
            case SimEventKind.StreamShutdownComplete:
                if (s.ShutdownDelivered)
                    break;
                s.ShutdownDelivered = true;
                if (!s.Local && Peer is not null)
                    Post(SimEventKind.StreamCreditReturn, _network.NowMicros + Link.Options.DelayMicros, Peer, b0: (byte)s.Kind);
                if (s.AppClosed)
                {
                    FreeStream(e.I0);
                }
                else
                {
                    s.DiscardReceive(_network.Pool);
                    Sink!.OnStreamShutdownComplete(id);
                }
                break;
            case SimEventKind.StreamReset:
                if (s.RecvDone)
                    break;
                s.RecvDone = true;
                s.DiscardIncoming = true;
                s.Pending = false;
                Sink!.OnStreamAborted(id, e.U0, StreamAbortDirection.Send);
                CheckShutdown(e.I0, s);
                break;
            case SimEventKind.StreamStopSending:
                if (s.SendDone && s.PendingCount == 0)
                    break;
                if (!s.AppClosed)
                    Sink!.OnStreamAborted(id, e.U0, StreamAbortDirection.Receive);
                s.SendDone = true;
                s.FinQueued = true;
                CancelPendingSends(e.I0, s, inline: true);
                CheckShutdown(e.I0, s);
                break;
        }
    }
}
