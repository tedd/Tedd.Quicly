using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

public sealed unsafe partial class MsQuicTransport
{
    private const ushort DefaultStreamPriority = 0x7FFF;
    private const int InitialStreamSlots = 16;
    private const int GuardClosing = 1 << 30;
    private const int AppClosedFlag = 1;
    private const int NativeShutdownFlag = 2;
    private const int ReceiveIdle = 0;
    private const int ReceiveInCallback = 1;
    private const int ReceivePending = 2;
    private const int ReceiveEarlyResume = 3;
    private const QUIC_STREAM_START_FLAGS StartFlags = QUIC_STREAM_START_FLAGS.FAIL_BLOCKED | QUIC_STREAM_START_FLAGS.SHUTDOWN_ON_FAIL;

    /// <summary>
    /// One stream table entry. Created on demand (up to <see cref="MaxStreams"/>) and reused after the native close, with
    /// the generation bumped so stale <see cref="TransportStreamId"/>s are rejected.
    /// </summary>
    /// <remarks>
    /// <see cref="Guard"/> counts API calls in progress on the stream (low bits) plus <see cref="GuardClosing"/> while the
    /// native handle is being closed: a closer waits for the count to drain, so a racing call never touches a closed
    /// handle. <see cref="CloseFlags"/> is the hand-off between <see cref="CloseStream"/> and MsQuic's SHUTDOWN_COMPLETE:
    /// whichever side sets its flag second closes the handle. <see cref="ReceiveState"/> tracks a pending receive and a
    /// resume that arrives while the receive callback is still running.
    /// </remarks>
    private sealed class StreamSlot(int index)
    {
        public readonly int Index = index;
        public uint Generation = 1;
        public MsQuicStream? Stream;
        public int Guard;
        public bool Local;
        public StreamKind Kind;
        public ulong OpenContext;
        public bool CanSend;
        public bool CanReceive;
        public volatile bool StartRequested;
        public volatile bool SendClosed;
        public int CloseFlags;
        public int CloseQueued;
        public int DeferredNext = -1;
        public int ReceiveState;
        public long PendingConsumed;
        public long PendingTotal;
        public long EarlyResumeBytes;

        public TransportStreamId Id => new(Index, Generation);

        public bool AppClosed => (Volatile.Read(ref CloseFlags) & AppClosedFlag) != 0;
    }

    private readonly Lock _tableLock = new();
    private readonly Lock _cleanupLock = new();
    private StreamSlot?[] _slots = [];
    private int[] _freeSlots = [];
    private int _freeSlotCount;
    private int _slotHighWater;
    private int _maxStreams;
    private int _deferredHead = -1;

    /// <summary>Capacity of the stream table (<see cref="MsQuicTransportOptions.MaxStreams"/>).</summary>
    public int MaxStreams => _maxStreams;

    /// <summary>Stream slots in use: open streams plus streams whose native close is still pending.</summary>
    public int OpenStreamCount
    {
        get
        {
            lock (_tableLock)
                return _slotHighWater - _freeSlotCount;
        }
    }

    private void InitializeStreams(int maxStreams)
    {
        _maxStreams = maxStreams;
        int initial = Math.Min(InitialStreamSlots, maxStreams);
        _slots = new StreamSlot?[initial];
        _freeSlots = new int[initial];
    }

    // ------------------------------------------------------------------ ITransport stream API (any thread)

    /// <inheritdoc/>
    /// <remarks>
    /// Allowed while connecting or connected. Allocates a table slot and opens the MsQuic stream (<c>StreamOpen</c>, one
    /// <see cref="MsQuicStream"/> wrapper per stream) without starting it; the stream consumes peer credit only when it
    /// starts. <see cref="TransportStatus.OutOfMemory"/> when the table is full.
    /// </remarks>
    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
    {
        id = TransportStreamId.None;
        int state = Volatile.Read(ref _state);
        if (state is not (StateConnecting or StateConnected) || _connection.IsClosed) return TransportStatus.InvalidState;
        if (kind is not (StreamKind.Unidirectional or StreamKind.Bidirectional)) return TransportStatus.NotSupported;
        if (HasDeferredCloses && !MsQuicCallbackScope.IsInsideCallback) DrainDeferredCloses();
        StreamSlot? slot = AllocateSlot();
        if (slot is null) return TransportStatus.OutOfMemory;
        slot.Local = true;
        slot.Kind = kind;
        slot.OpenContext = context;
        slot.CanSend = true;
        slot.CanReceive = kind == StreamKind.Bidirectional;
        QUIC_STREAM_OPEN_FLAGS flags = kind == StreamKind.Unidirectional ? QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL : QUIC_STREAM_OPEN_FLAGS.NONE;
        int status = _connection.OpenStream(flags, this, out MsQuicStream? stream);
        if (MsQuicStatus.Failed(status) || stream is null)
        {
            FreeSlot(slot);
            return MapStatus(status, datagramSend: false);
        }
        stream.Tag = slot;
        if (priority != DefaultStreamPriority) stream.SetPriority(priority);
        Volatile.Write(ref slot.Stream, stream);
        id = slot.Id;
        return TransportStatus.Success;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// <c>StreamStart(FAIL_BLOCKED | SHUTDOWN_ON_FAIL)</c>. MsQuic queues the start (the call returns before it runs), so a
    /// stream-limit failure normally arrives as <see cref="ITransportSink.OnStreamStarted"/> with
    /// <see cref="TransportStatus.StreamLimitReached"/>, followed by <see cref="ITransportSink.OnStreamShutdownComplete"/>.
    /// <see cref="TransportStatus.InvalidState"/> unless connected and the stream is a local stream not started yet.
    /// </remarks>
    public TransportStatus StartStream(TransportStreamId id)
    {
        if (Volatile.Read(ref _state) != StateConnected) return TransportStatus.InvalidState;
        StreamSlot? slot = Enter(id);
        if (slot is null) return TransportStatus.InvalidState;
        try
        {
            return !slot.Local || slot.StartRequested ? TransportStatus.InvalidState : StartCore(slot);
        }
        finally
        {
            Exit(slot);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Passes <paramref name="segments"/> to <c>StreamSend</c> as <c>QUIC_BUFFER*</c> and <paramref name="context"/> as the
    /// client context. <see cref="TransportSendFlags.Fin"/> → <c>FIN</c>, <see cref="TransportSendFlags.DelaySend"/> →
    /// <c>DELAY_SEND</c>, <see cref="TransportSendFlags.Priority"/> → <c>PRIORITY_WORK</c>,
    /// <see cref="TransportSendFlags.CancelOnLoss"/> → <c>CANCEL_ON_LOSS</c>; <see cref="TransportSendFlags.Start"/> starts an
    /// unstarted local stream first (as <see cref="StartStream"/>). <see cref="TransportStatus.InvalidState"/> unless connected,
    /// for a stream that cannot send (a peer unidirectional stream), after a <c>Fin</c> or an abort of the send direction,
    /// and for an unstarted stream without <c>Start</c>.
    /// </remarks>
    public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count > 0 && segments == null) throw new ArgumentNullException(nameof(segments));
        if (Volatile.Read(ref _state) != StateConnected) return TransportStatus.InvalidState;
        StreamSlot? slot = Enter(id);
        if (slot is null) return TransportStatus.InvalidState;
        try
        {
            if (!slot.CanSend || slot.SendClosed) return TransportStatus.InvalidState;
            if (!slot.StartRequested)
            {
                if (!slot.Local || (flags & TransportSendFlags.Start) == 0) return TransportStatus.InvalidState;
                TransportStatus started = StartCore(slot);
                if (started != TransportStatus.Success) return started;
            }
            bool fin = (flags & TransportSendFlags.Fin) != 0;
            if (fin) slot.SendClosed = true;
            QUIC_SEND_FLAGS sendFlags = s_streamFlagMap[(int)flags & 63] & _supportedSendFlags;
            int status = slot.Stream!.Send((QUIC_BUFFER*)segments, (uint)count, sendFlags, (void*)context);
            if (MsQuicStatus.Succeeded(status)) return TransportStatus.Success;
            if (fin) slot.SendClosed = false;
            return MapStatus(status, datagramSend: false);
        }
        finally
        {
            Exit(slot);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// <see cref="StreamAbortDirection.Send"/> → <c>ABORT_SEND</c> (RESET_STREAM), <see cref="StreamAbortDirection.Receive"/> →
    /// <c>ABORT_RECEIVE</c> (STOP_SENDING); directions the stream does not have are ignored. Aborting a stream that was never
    /// started releases it like <see cref="CloseStream"/> (MsQuic raises no events for such a stream).
    /// </remarks>
    public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
        StreamSlot? slot = Enter(id);
        if (slot is null) return;
        bool neverStarted = false;
        try
        {
            if (!slot.StartRequested)
            {
                neverStarted = true;
            }
            else
            {
                QUIC_STREAM_SHUTDOWN_FLAGS flags = QUIC_STREAM_SHUTDOWN_FLAGS.NONE;
                if ((direction & StreamAbortDirection.Send) != 0 && slot.CanSend)
                {
                    flags |= QUIC_STREAM_SHUTDOWN_FLAGS.ABORT_SEND;
                    slot.SendClosed = true;
                }
                if ((direction & StreamAbortDirection.Receive) != 0 && slot.CanReceive)
                {
                    flags |= QUIC_STREAM_SHUTDOWN_FLAGS.ABORT_RECEIVE;
                    Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
                }
                if (flags != QUIC_STREAM_SHUTDOWN_FLAGS.NONE) slot.Stream!.Shutdown(flags, errorCode);
            }
        }
        finally
        {
            Exit(slot);
        }
        if (neverStarted) CloseStream(id);
    }

    /// <inheritdoc/>
    /// <remarks><c>QUIC_PARAM_STREAM_PRIORITY</c> (0 lowest, 65535 highest, 32767 default).</remarks>
    public void SetStreamPriority(TransportStreamId id, ushort priority)
    {
        StreamSlot? slot = Enter(id);
        if (slot is null) return;
        try
        {
            slot.Stream!.SetPriority(priority);
        }
        finally
        {
            Exit(slot);
        }
    }

    /// <inheritdoc/>
    public long GetQuicStreamId(TransportStreamId id)
    {
        StreamSlot? slot = Enter(id, allowAppClosed: true);
        if (slot is null) return -1;
        try
        {
            ulong quicId = slot.Stream!.Id;
            return quicId == ulong.MaxValue ? -1 : (long)quicId;
        }
        finally
        {
            Exit(slot);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Ignored unless a receive on the stream is pending (or its callback is still running and then returns
    /// <c>Pending</c>: the resume is applied when it returns). Calls <c>StreamReceiveComplete</c> with every byte of the
    /// held indication consumed so far and, when bytes remain, <c>StreamReceiveSetEnabled(TRUE)</c> so MsQuic indicates
    /// them again at once (msquic 2.5.10 pauses a stream after any partial completion).
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bytesConsumed"/> is negative or more than the held-back bytes.</exception>
    public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed)
    {
        StreamSlot? slot = Enter(id);
        if (slot is null) return;
        try
        {
            while (true)
            {
                int state = Volatile.Read(ref slot.ReceiveState);
                if (state == ReceivePending)
                {
                    long consumed = slot.PendingConsumed;
                    long total = slot.PendingTotal;
                    ArgumentOutOfRangeException.ThrowIfNegative(bytesConsumed);
                    ArgumentOutOfRangeException.ThrowIfGreaterThan(bytesConsumed, total - consumed);
                    if (Interlocked.CompareExchange(ref slot.ReceiveState, ReceiveIdle, ReceivePending) != ReceivePending) continue;
                    long complete = consumed + bytesConsumed;
                    slot.Stream!.ReceiveComplete((ulong)complete);
                    if (complete < total) slot.Stream.ReceiveSetEnabled(true);
                    return;
                }
                if (state == ReceiveInCallback)
                {
                    ArgumentOutOfRangeException.ThrowIfNegative(bytesConsumed);
                    Volatile.Write(ref slot.EarlyResumeBytes, bytesConsumed);
                    if (Interlocked.CompareExchange(ref slot.ReceiveState, ReceiveEarlyResume, ReceiveInCallback) == ReceiveInCallback) return;
                    continue;
                }
                return;
            }
        }
        finally
        {
            Exit(slot);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// After <see cref="ITransportSink.OnStreamShutdownComplete"/> the handle is closed (<c>StreamClose</c>) and the slot
    /// freed: at once, or on a thread-pool thread when called from inside a callback. Before that the stream is aborted in
    /// both directions with code 0 and closed when MsQuic reports its shutdown; pending send completions are still
    /// reported (canceled), the shutdown callback is not. Unknown or stale ids are ignored.
    /// </remarks>
    public void CloseStream(TransportStreamId id)
    {
        StreamSlot? slot = Enter(id);
        if (slot is null) return;
        bool closeNow = false;
        try
        {
            int old = Interlocked.Or(ref slot.CloseFlags, AppClosedFlag);
            if ((old & AppClosedFlag) != 0) return;
            Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
            if ((old & NativeShutdownFlag) != 0 || !slot.StartRequested) closeNow = true;
            else slot.Stream!.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0);
        }
        finally
        {
            Exit(slot);
        }
        if (closeNow) CloseOrDefer(slot);
    }

    private TransportStatus StartCore(StreamSlot slot)
    {
        slot.StartRequested = true;
        int status = slot.Stream!.Start(StartFlags);
        if (MsQuicStatus.Succeeded(status)) return TransportStatus.Success;
        slot.StartRequested = false;
        return MapStatus(status, datagramSend: false);
    }

    // ------------------------------------------------------------------ slot table

    private StreamSlot? AllocateSlot()
    {
        lock (_tableLock)
        {
            if (_freeSlotCount > 0) return _slots[_freeSlots[--_freeSlotCount]];
            if (_slotHighWater == _maxStreams) return null;
            if (_slotHighWater == _slots.Length)
            {
                var grown = new StreamSlot?[Math.Min(_maxStreams, _slots.Length * 2)];
                Array.Copy(_slots, grown, _slotHighWater);
                Array.Resize(ref _freeSlots, grown.Length);
                Volatile.Write(ref _slots, grown);
            }
            var slot = new StreamSlot(_slotHighWater);
            _slots[_slotHighWater++] = slot;
            return slot;
        }
    }

    /// <summary>Resets a slot whose native stream is closed (or was never opened), bumps its generation and returns it to the free list.</summary>
    private void FreeSlot(StreamSlot slot)
    {
        slot.Stream = null;
        slot.Local = false;
        slot.Kind = default;
        slot.OpenContext = 0;
        slot.CanSend = false;
        slot.CanReceive = false;
        slot.StartRequested = false;
        slot.SendClosed = false;
        slot.DeferredNext = -1;
        slot.PendingConsumed = 0;
        slot.PendingTotal = 0;
        slot.EarlyResumeBytes = 0;
        Volatile.Write(ref slot.ReceiveState, ReceiveIdle);
        Volatile.Write(ref slot.CloseQueued, 0);
        Volatile.Write(ref slot.CloseFlags, 0);
        slot.Generation = slot.Generation == uint.MaxValue ? 1 : slot.Generation + 1;
        Interlocked.And(ref slot.Guard, ~GuardClosing);
        lock (_tableLock)
            _freeSlots[_freeSlotCount++] = slot.Index;
    }

    /// <summary>Pins a live slot for an API call (see <see cref="StreamSlot.Guard"/>); null for unknown, stale, closed or closing ids.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private StreamSlot? Enter(TransportStreamId id, bool allowAppClosed = false)
    {
        StreamSlot?[] slots = Volatile.Read(ref _slots);
        if ((uint)id.Slot >= (uint)slots.Length) return null;
        StreamSlot? slot = slots[id.Slot];
        if (slot is null) return null;
        int guard = Interlocked.Increment(ref slot.Guard);
        if ((guard & GuardClosing) == 0 && slot.Generation == id.Generation && Volatile.Read(ref slot.Stream) is not null
            && (allowAppClosed || (Volatile.Read(ref slot.CloseFlags) & AppClosedFlag) == 0))
        {
            return slot;
        }
        Interlocked.Decrement(ref slot.Guard);
        return null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Exit(StreamSlot slot) => Interlocked.Decrement(ref slot.Guard);

    /// <summary>Closes the native stream and frees the slot. Never on an MsQuic callback thread; callers hold <see cref="_cleanupLock"/>.</summary>
    private void CloseSlotNative(StreamSlot slot)
    {
        if ((Interlocked.Or(ref slot.Guard, GuardClosing) & GuardClosing) != 0) return;
        SpinWait spin = default;
        while ((Volatile.Read(ref slot.Guard) & ~GuardClosing) != 0) spin.SpinOnce();
        slot.Stream?.Close();
        FreeSlot(slot);
    }

    private void CloseOrDefer(StreamSlot slot)
    {
        if (MsQuicCallbackScope.IsInsideCallback)
        {
            EnqueueDeferredClose(slot);
            return;
        }
        lock (_cleanupLock)
            CloseSlotNative(slot);
    }

    /// <summary>Pushes the slot on the lock-free deferred-close stack (intrusive, no allocation) and schedules the cleanup work item.</summary>
    private void EnqueueDeferredClose(StreamSlot slot)
    {
        if (Interlocked.Exchange(ref slot.CloseQueued, 1) != 0) return;
        int head;
        do
        {
            head = Volatile.Read(ref _deferredHead);
            slot.DeferredNext = head;
        }
        while (Interlocked.CompareExchange(ref _deferredHead, slot.Index, head) != head);
        ScheduleCleanup();
    }

    private bool HasDeferredCloses => Volatile.Read(ref _deferredHead) >= 0;

    /// <summary>Closes every stream on the deferred-close stack. Never on an MsQuic callback thread.</summary>
    private void DrainDeferredCloses()
    {
        lock (_cleanupLock)
        {
            int index = Interlocked.Exchange(ref _deferredHead, -1);
            StreamSlot?[] slots = Volatile.Read(ref _slots);
            while (index >= 0)
            {
                StreamSlot slot = slots[index]!;
                int next = slot.DeferredNext;
                CloseSlotNative(slot);
                index = next;
            }
        }
    }

    /// <summary>Closes every stream handle still open (connection teardown, after SHUTDOWN_COMPLETE). Never on a callback thread.</summary>
    private void CloseAllStreamHandles()
    {
        lock (_cleanupLock)
        {
            StreamSlot?[] slots = Volatile.Read(ref _slots);
            int high;
            lock (_tableLock)
                high = _slotHighWater;
            for (int i = 0; i < high; i++)
            {
                StreamSlot? slot = slots[i];
                if (slot is not null && Volatile.Read(ref slot.Stream) is not null) CloseSlotNative(slot);
            }
        }
    }

    /// <summary>
    /// Connection SHUTDOWN_COMPLETE (worker thread): reports <see cref="ITransportSink.OnStreamShutdownComplete"/> for every
    /// stream MsQuic did not report (streams never started are not tracked by MsQuic), so it precedes <c>OnClosed</c>.
    /// </summary>
    private void ShutdownRemainingStreams(ITransportSink sink)
    {
        StreamSlot?[] slots = Volatile.Read(ref _slots);
        int high;
        lock (_tableLock)
            high = _slotHighWater;
        for (int i = 0; i < high; i++)
        {
            StreamSlot? slot = slots[i];
            if (slot is null || Volatile.Read(ref slot.Stream) is null) continue;
            Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
            int old = Interlocked.Or(ref slot.CloseFlags, NativeShutdownFlag);
            if ((old & NativeShutdownFlag) != 0) continue;
            if ((old & AppClosedFlag) != 0)
            {
                EnqueueDeferredClose(slot);
                continue;
            }
            try
            {
                sink.OnStreamShutdownComplete(slot.Id);
            }
            catch (Exception ex)
            {
                Volatile.Write(ref _lastSinkException, ex);
                Interlocked.Increment(ref _sinkExceptions);
                MsQuicCallbackScope.OnEscapedException(ex);
                Diagnose(TransportDiagnosticLevel.Error, "ITransportSink.OnStreamShutdownComplete threw during connection shutdown.", ex);
            }
        }
    }

    // ------------------------------------------------------------------ connection / stream events (MsQuic worker thread)

    bool IMsQuicConnectionEvents.PeerStreamStarted(MsQuicConnection connection, MsQuicStream stream, QUIC_STREAM_OPEN_FLAGS flags)
    {
        if (Volatile.Read(ref _closedDelivered) != 0) return false;
        StreamSlot? slot = AllocateSlot();
        if (slot is null)
        {
            Interlocked.Increment(ref _refusedPeerStreams);
            Diagnose(TransportDiagnosticLevel.Warning, "A peer stream was refused because the stream table is full.", null);
            return false;
        }
        bool unidirectional = (flags & QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL) != 0;
        slot.Local = false;
        slot.Kind = unidirectional ? StreamKind.Unidirectional : StreamKind.Bidirectional;
        slot.CanSend = !unidirectional;
        slot.CanReceive = true;
        slot.StartRequested = true;
        stream.Events = this;
        stream.Tag = slot;
        Volatile.Write(ref slot.Stream, stream);
        // The wrapper attaches the stream's callback only after this handler returns; attach it now so that API calls
        // the sink makes on the new stream from inside OnPeerStreamStarted see their events.
        stream.AttachCallback();
        ITransportSink? sink = LiveSink;
        if (sink is null) return true;
        try
        {
            sink.OnPeerStreamStarted(slot.Id, slot.Kind);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
        return true;
    }

    void IMsQuicStreamEvents.StartComplete(MsQuicStream stream, int status, ulong id, bool peerAccepted)
    {
        var slot = (StreamSlot)stream.Tag!;
        TransportStatus mapped = MsQuicStatus.Succeeded(status) ? TransportStatus.Success : MapStatus(status, datagramSend: false);
        if (mapped != TransportStatus.Success) slot.SendClosed = true;
        if (slot.AppClosed) return;
        ITransportSink? sink = LiveSink;
        if (sink is null) return;
        try
        {
            sink.OnStreamStarted(slot.Id, slot.OpenContext, mapped);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
    }

    MsQuicReceiveResult IMsQuicStreamEvents.Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
    {
        var slot = (StreamSlot)stream.Tag!;
        if (slot.AppClosed) return MsQuicReceiveResult.Consumed(totalLength);
        ITransportSink? sink = LiveSink;
        if (sink is null) return MsQuicReceiveResult.Consumed(totalLength);
        slot.PendingTotal = (long)totalLength;
        Volatile.Write(ref slot.ReceiveState, ReceiveInCallback);
        ReceiveResult result;
        try
        {
            result = sink.OnStreamReceived(slot.Id, new ReadOnlySpan<TransportSegment>(buffers, (int)bufferCount), absoluteOffset, (flags & QUIC_RECEIVE_FLAGS.FIN) != 0);
        }
        catch (Exception ex)
        {
            Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
            OnSinkException(ex);
            return MsQuicReceiveResult.Consumed(totalLength);
        }
        long consumed = result.BytesConsumed;
        if (consumed < 0 || (ulong)consumed > totalLength)
        {
            Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
            OnSinkException(new InvalidOperationException($"OnStreamReceived consumed {consumed} bytes but {totalLength} were indicated."));
            return MsQuicReceiveResult.Consumed(totalLength);
        }
        if (result.Pending || (consumed == 0 && totalLength != 0))
        {
            // Back-pressure (Consumed(0) of a non-empty indication counts as PendingAfter(0)): hold MsQuic's buffers until
            // ResumeStreamReceive. Publish the consumed count before the state so a resume on another thread reads it.
            slot.PendingConsumed = consumed;
            int previous = Interlocked.CompareExchange(ref slot.ReceiveState, ReceivePending, ReceiveInCallback);
            if (previous == ReceiveInCallback) return MsQuicReceiveResult.Pending;
            if (previous != ReceiveEarlyResume) return MsQuicReceiveResult.Consumed(totalLength); // aborted or closed meanwhile
            consumed = Math.Min(consumed + Volatile.Read(ref slot.EarlyResumeBytes), (long)totalLength);
            Volatile.Write(ref slot.ReceiveState, ReceiveIdle);
        }
        else if (Interlocked.CompareExchange(ref slot.ReceiveState, ReceiveIdle, ReceiveInCallback) != ReceiveInCallback)
        {
            // An early resume without a pending receive is meaningless; an abort or close during the call discards the rest.
            if (Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle) != ReceiveEarlyResume) return MsQuicReceiveResult.Consumed(totalLength);
        }
        if ((ulong)consumed == totalLength) return MsQuicReceiveResult.Consumed(totalLength);
        // Partial consumption: MsQuic would pause the stream until receives are re-enabled; re-enable them inline so the
        // remainder is indicated again right away (together with anything that arrived since).
        stream.ReceiveSetEnabled(true);
        return MsQuicReceiveResult.Consumed((ulong)consumed);
    }

    void IMsQuicStreamEvents.SendComplete(MsQuicStream stream, void* clientContext, bool canceled)
    {
        var slot = (StreamSlot)stream.Tag!;
        ITransportSink? sink = LiveSink;
        if (sink is null) return;
        try
        {
            sink.OnStreamSendCompleted(slot.Id, (ulong)clientContext, canceled);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
    }

    void IMsQuicStreamEvents.PeerSendShutdown(MsQuicStream stream)
    {
        var slot = (StreamSlot)stream.Tag!;
        if (slot.AppClosed) return;
        ITransportSink? sink = LiveSink;
        if (sink is null) return;
        try
        {
            sink.OnStreamPeerSendShutdown(slot.Id);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
    }

    void IMsQuicStreamEvents.PeerSendAborted(MsQuicStream stream, ulong errorCode)
    {
        var slot = (StreamSlot)stream.Tag!;
        Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
        if (slot.AppClosed) return;
        ITransportSink? sink = LiveSink;
        if (sink is null) return;
        try
        {
            sink.OnStreamAborted(slot.Id, errorCode, StreamAbortDirection.Send);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
    }

    void IMsQuicStreamEvents.PeerReceiveAborted(MsQuicStream stream, ulong errorCode)
    {
        var slot = (StreamSlot)stream.Tag!;
        slot.SendClosed = true;
        if (slot.AppClosed) return;
        ITransportSink? sink = LiveSink;
        if (sink is null) return;
        try
        {
            sink.OnStreamAborted(slot.Id, errorCode, StreamAbortDirection.Receive);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
    }

    void IMsQuicStreamEvents.ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info)
    {
        var slot = (StreamSlot)stream.Tag!;
        Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
        int old = Interlocked.Or(ref slot.CloseFlags, NativeShutdownFlag);
        if ((old & NativeShutdownFlag) != 0) return;
        if ((old & AppClosedFlag) != 0)
        {
            EnqueueDeferredClose(slot);
            return;
        }
        ITransportSink? sink = LiveSink;
        if (sink is null) return;
        try
        {
            sink.OnStreamShutdownComplete(slot.Id);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
    }

    void IMsQuicStreamEvents.IdealSendBufferSize(MsQuicStream stream, ulong byteCount)
    {
        var slot = (StreamSlot)stream.Tag!;
        if (slot.AppClosed) return;
        ITransportSink? sink = LiveSink;
        if (sink is null) return;
        try
        {
            sink.OnIdealSendBufferSize(slot.Id, byteCount);
        }
        catch (Exception ex)
        {
            OnSinkException(ex);
        }
    }
}
