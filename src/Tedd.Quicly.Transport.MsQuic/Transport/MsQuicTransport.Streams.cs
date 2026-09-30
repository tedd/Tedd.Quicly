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
    /// <para><see cref="Guard"/> counts API calls in progress on the stream (low bits) plus <see cref="GuardClosing"/> while
    /// the native handle is being closed: the closer waits for the count to drain, so a racing call never touches a closed
    /// handle.</para>
    /// <para><see cref="CloseFlags"/> is the hand-off between <see cref="CloseStream"/> (<see cref="AppClosedFlag"/>) and the
    /// stream's shutdown (<see cref="NativeShutdownFlag"/>: MsQuic's SHUTDOWN_COMPLETE, or the connection's shutdown sweep
    /// for streams MsQuic never started): whichever side sets its flag second queues the native close. Closing a stream
    /// MsQuic never started sets both flags at once, so exactly one party ever decides to close a slot. The close is queued
    /// at most once per incarnation (<see cref="CloseQueued"/>) and run by the cleanup work item under
    /// <see cref="_cleanupLock"/>, which checks the generation the close was decided for.</para>
    /// <para><see cref="ReceiveState"/> tracks a held stream (<see cref="ReceivePending"/>: the sink asked for the rest to be
    /// held back, and MsQuic has paused the stream) and a resume that arrives while the receive callback is still
    /// running.</para>
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

        /// <summary>The priority for the stream's start (START_COMPLETE applies it on the worker, where the parameter call runs inline).</summary>
        public ushort Priority = DefaultStreamPriority;
        public volatile bool PriorityPending;
        public volatile bool StartRequested;

        /// <summary>StreamStart failed synchronously: the stream never starts, and MsQuic raises no events for it.</summary>
        public volatile bool StartRefused;

        /// <summary>START_COMPLETE was indicated (possibly inline, inside StreamStart).</summary>
        public volatile bool StartReported;

        /// <summary>START_COMPLETE reported that the peer's stream limit refused the start (set with <see cref="StartReported"/>).</summary>
        public volatile bool StartRefusedByLimit;
        public volatile bool SendClosed;
        public int CloseFlags;
        public int CloseQueued;
        public int DeferredNext = -1;
        public uint DeferredGeneration;
        public int ReceiveState;

        /// <summary>Bytes of the held indication the sink consumed, and the bytes it was given (what a resume may still credit is the difference).</summary>
        public long PendingConsumed;
        public long PendingTotal;
        public long EarlyResumeBytes;

        /// <summary>
        /// Bytes at the head of the stream's next indication that the sink already has: MsQuic was told less than the sink
        /// consumed (a resume credited them after the stream was paused, or the sink consumed a whole indication and held
        /// the stream, where one byte is kept back so that MsQuic pauses it). The receive callback skips them.
        /// </summary>
        public long SkipBytes;

        /// <summary>
        /// The indication the sink held carried the FIN (worker thread only). The sink has seen it, so it is not shown an
        /// indication again that adds nothing to it: when the bytes before the FIN are all the sink's, the stream is over.
        /// </summary>
        public bool FinHeld;

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

    // Under _tableLock: the size the options and the grants call for (MaxStreams, StreamTableFor). _maxStreams grows past it
    // only for slots whose streams wait for the cleanup work item, and at most to twice it (AllocateSlot).
    private int _sizedStreams;
    private int _streamTableGrown;
    private int _streamTableGrowthReported;

    // Under _tableLock: the most streams the peer may have open in each direction (the largest grant made so far; QUIC
    // never takes granted credit back), and the slots this end's own streams hold.
    private int _grantedPeerUnidi;
    private int _grantedPeerBidi;
    private int _localSlots;
    private int _deferredHead = -1;

    /// <summary>
    /// Set under <see cref="_tableLock"/> when the connection's shutdown sweep takes its snapshot of the table (and when the
    /// transport is refused or released): no stream is published afterwards, so none can miss its
    /// <see cref="ITransportSink.OnStreamShutdownComplete"/>.
    /// </summary>
    private bool _tableClosed;

    /// <summary>
    /// Capacity of the stream table: <see cref="MsQuicTransportOptions.MaxStreams"/>, or more when the streams the peer was
    /// granted need it (<see cref="MakeRoomForPeerStreams"/>).
    /// </summary>
    public int MaxStreams
    {
        get
        {
            lock (_tableLock)
                return _maxStreams;
        }
    }

    /// <summary>
    /// Sizes the stream table for what the peer may open: <paramref name="bidirectional"/> and
    /// <paramref name="unidirectional"/> streams at once (the connection's settings at first, then every
    /// <see cref="UpdatePeerStreamLimits"/>). MsQuic admits a stream the peer has credit for, and a stream that found no
    /// slot could only be refused below the session, with whatever it carried lost — so the table always has room for the
    /// grants, next to a quarter of it for this end's own streams (<see cref="MsQuicTransportOptions.StreamTableFor"/>).
    /// <see cref="MsQuicTransportOptions.MaxStreams"/> is the size the table has when that is enough. Says so once,
    /// through the diagnostic sink, when it is not.
    /// </summary>
    private void MakeRoomForPeerStreams(int bidirectional, int unidirectional)
    {
        int peer;
        int before;
        int after;
        lock (_tableLock)
        {
            _grantedPeerBidi = Math.Max(_grantedPeerBidi, bidirectional);
            _grantedPeerUnidi = Math.Max(_grantedPeerUnidi, unidirectional);
            peer = _grantedPeerBidi + _grantedPeerUnidi;
            before = _sizedStreams;
            after = Math.Max(before, MsQuicTransportOptions.StreamTableFor(peer));
            _sizedStreams = after;
            _maxStreams = Math.Max(_maxStreams, after);
        }

        if (after != before && Interlocked.Exchange(ref _streamTableRaised, 1) == 0)
        {
            Diagnose(TransportDiagnosticLevel.Information,
                $"The peer may have {peer} streams open, more than a stream table of {before} slots (MsQuicTransportOptions.MaxStreams) has room for next to the local streams: the table was raised to {after} slots.", null);
        }
    }

    /// <summary>Stream slots in use: open streams plus released streams whose native close the cleanup work item has not run yet.</summary>
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
        _sizedStreams = maxStreams;
        int initial = Math.Min(InitialStreamSlots, maxStreams);
        _slots = new StreamSlot?[initial];
        _freeSlots = new int[initial];
    }

    // ------------------------------------------------------------------ ITransport stream API (any thread)

    /// <inheritdoc/>
    /// <remarks>
    /// Allowed while connecting or connected. Allocates a table slot and opens the MsQuic stream (<c>StreamOpen</c>, one
    /// <see cref="MsQuicStream"/> wrapper per stream) without starting it; the stream consumes peer credit only when it
    /// starts. A <paramref name="priority"/> other than the default is applied when the stream starts (from its
    /// START_COMPLETE, on the worker), so the call never waits for the MsQuic worker. <see cref="TransportStatus.OutOfMemory"/>
    /// when the table is full (a slot released by <see cref="CloseStream"/> is back once the cleanup work item has run);
    /// <see cref="TransportStatus.InvalidState"/> once the connection has shut down.
    /// </remarks>
    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
    {
        id = TransportStreamId.None;
        int state = Volatile.Read(ref _state);
        if (state is not (StateConnecting or StateConnected) || _connection.IsClosed) return TransportStatus.InvalidState;
        if (kind is not (StreamKind.Unidirectional or StreamKind.Bidirectional)) return TransportStatus.NotSupported;
        StreamSlot? slot = AllocateSlot(local: true, out TransportStatus refused);
        if (slot is null) return refused;
        slot.Local = true;
        slot.Kind = kind;
        slot.OpenContext = context;
        slot.CanSend = true;
        slot.CanReceive = kind == StreamKind.Bidirectional;
        slot.Priority = priority;
        slot.PriorityPending = priority != DefaultStreamPriority;
        QUIC_STREAM_OPEN_FLAGS flags = kind == StreamKind.Unidirectional ? QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL : QUIC_STREAM_OPEN_FLAGS.NONE;
        int status = _connection.OpenStream(flags, this, out MsQuicStream? stream);
        if (MsQuicStatus.Failed(status) || stream is null)
        {
            FreeSlot(slot);
            return MapStatus(status, datagramSend: false);
        }
        stream.Tag = slot;
        if (!TryPublish(slot, stream)) return TransportStatus.InvalidState;
        id = slot.Id;
        return TransportStatus.Success;
    }

    /// <summary>
    /// Makes a new local stream visible to the API and to the connection's shutdown sweep, unless that sweep has already
    /// taken its snapshot: then nobody would ever report the stream, so its native close is queued instead and the open fails.
    /// </summary>
    private bool TryPublish(StreamSlot slot, MsQuicStream stream)
    {
        lock (_tableLock)
        {
            if (!_tableClosed)
            {
                Volatile.Write(ref slot.Stream, stream);
                return true;
            }
            // Both halves of the close hand-off: nobody reports this stream and nobody else closes it.
            Volatile.Write(ref slot.CloseFlags, AppClosedFlag | NativeShutdownFlag);
            Volatile.Write(ref slot.Stream, stream);
        }
        EnqueueDeferredClose(slot, slot.Generation);
        return false;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// <c>StreamStart(FAIL_BLOCKED | SHUTDOWN_ON_FAIL)</c>. MsQuic queues the start (the call returns before it runs), so a
    /// start the peer's stream limit refuses is reported asynchronously: <see cref="ITransportSink.OnStreamStarted"/> with
    /// <see cref="TransportStatus.StreamLimitReached"/>, then <see cref="ITransportSink.OnStreamShutdownComplete"/>. A refused
    /// stream never starts: a later <see cref="StartStream"/> returns <see cref="TransportStatus.InvalidState"/>; release it
    /// with <see cref="CloseStream"/> and open a new stream to retry. A synchronous failure is returned directly (nothing
    /// follows for the stream, and it never starts either). <see cref="TransportStatus.InvalidState"/> unless connected and
    /// the stream is a local stream whose start was never requested.
    /// </remarks>
    public TransportStatus StartStream(TransportStreamId id)
    {
        if (Volatile.Read(ref _state) != StateConnected) return TransportStatus.InvalidState;
        StreamSlot? slot = Enter(id);
        if (slot is null) return TransportStatus.InvalidState;
        try
        {
            return !slot.Local || slot.StartRequested || slot.StartRefused ? TransportStatus.InvalidState : StartCore(slot);
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
    /// unstarted local stream first (as <see cref="StartStream"/>; a send accepted with a start the peer's stream limit
    /// refuses completes canceled). <see cref="TransportStatus.InvalidState"/> unless connected, for a stream that cannot
    /// send (a peer unidirectional stream), after a <c>Fin</c> or an abort of the send direction, after a refused start, and
    /// for an unstarted stream without <c>Start</c>.
    /// <para>
    /// The start and the send are two MsQuic calls, and the worker can run the start between them. When the peer's stream
    /// limit refuses it there (START_COMPLETE reports <c>STREAM_LIMIT_REACHED</c> and the stream shuts down), MsQuic rejects
    /// the send with <c>INVALID_STATE</c>: the payload was never accepted, so no completion follows. The call then returns
    /// <see cref="TransportStatus.StreamLimitReached"/> — a synchronous refusal — although
    /// <see cref="ITransportSink.OnStreamStarted"/> and <see cref="ITransportSink.OnStreamShutdownComplete"/> may already
    /// have been delivered for the stream (see <see cref="ITransport.SendStream"/>). Any other <c>INVALID_STATE</c> of the
    /// send is <see cref="TransportStatus.InvalidState"/>.
    /// </para>
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
            bool startedHere = false;
            if (!slot.StartRequested)
            {
                if (!slot.Local || (flags & TransportSendFlags.Start) == 0 || slot.StartRefused) return TransportStatus.InvalidState;
                TransportStatus started = StartCore(slot);
                if (started != TransportStatus.Success) return started;
                startedHere = true;
                if (_delaySendUntilStartReported)
                {
                    // Counted only under the seam: a locked increment per started stream is not free (ADR 0008).
                    Interlocked.Increment(ref _startsWithSend);
                    WaitForStartReported(slot, 10_000);

                    // MsQuic shuts a refused start down right after it indicates START_COMPLETE (SHUTDOWN_ON_FAIL); wait for that
                    // too, so that the send always finds the stream shut down.
                    if (slot.StartRefusedByLimit) WaitForNativeShutdown(slot, 10_000);
                }
            }
            bool fin = (flags & TransportSendFlags.Fin) != 0;
            if (fin) slot.SendClosed = true;
            QUIC_SEND_FLAGS sendFlags = s_streamFlagMap[(int)flags & 63] & _supportedSendFlags;
            int status = slot.Stream!.Send((QUIC_BUFFER*)segments, (uint)count, sendFlags, (void*)context);
            if (MsQuicStatus.Succeeded(status)) return TransportStatus.Success;
            if (startedHere && status == MsQuicStatus.QUIC_STATUS_INVALID_STATE && !slot.StartReported)
            {
                // A queued start always ends in START_COMPLETE, but the worker may shut the stream down a moment before it
                // indicates it: wait (bounded) for the indication, which says whether the peer's stream limit refused it.
                WaitForStartReported(slot, 1_000);
            }
            if (startedHere && status == MsQuicStatus.QUIC_STATUS_INVALID_STATE && slot.StartRefusedByLimit)
            {
                // The worker ran the start between StartCore and Send, the peer's stream limit refused it, and the stream
                // shut down: the send was never accepted (no completion follows), and the stream is refused. The refusal
                // callbacks may have been delivered already; SendClosed stays set, as the failed start left it.
                Interlocked.Increment(ref _startRefusalRaces);
                return TransportStatus.StreamLimitReached;
            }
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
    /// <remarks>
    /// Before the stream starts the priority is only stored and applied when it starts (no MsQuic call). On a started
    /// stream it sets <c>QUIC_PARAM_STREAM_PRIORITY</c> (0 lowest, 65535 highest, 32767 default), a parameter call that
    /// waits for the MsQuic worker when made off it (see the class remarks).
    /// </remarks>
    public void SetStreamPriority(TransportStreamId id, ushort priority)
    {
        StreamSlot? slot = Enter(id);
        if (slot is null) return;
        try
        {
            if (!slot.StartRequested)
            {
                slot.Priority = priority;
                slot.PriorityPending = true;
                // A start requested meanwhile on another thread may already have applied the previous value.
                if (!slot.StartRequested) return;
            }
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

    /// <summary>Reads <c>QUIC_PARAM_STREAM_PRIORITY</c> of a stream (for tests; waits for the worker). -1 for an unknown id or a failed query.</summary>
    internal int QueryStreamPriority(TransportStreamId id)
    {
        StreamSlot? slot = Enter(id, allowAppClosed: true);
        if (slot is null) return -1;
        try
        {
            return MsQuicStatus.Succeeded(slot.Stream!.GetParam(MsQuicParam.QUIC_PARAM_STREAM_PRIORITY, out ushort priority)) ? priority : -1;
        }
        finally
        {
            Exit(slot);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Ignored unless the stream is held (or its receive callback is still running and then holds it: the resume is
    /// applied when it returns). A held stream is one MsQuic has paused — the receive callback answered the hold as a
    /// partial consumption (the class remarks, "Receive") — so the resume is one call,
    /// <c>StreamReceiveSetEnabled(TRUE)</c>, which MsQuic always queues to the connection's worker: it cannot be lost
    /// however it is timed against the callback that held the stream. The bytes credited here are still in MsQuic's
    /// buffer; they lead the next indication and the receive callback skips them. The call does not wait for the worker.
    /// If MsQuic cannot queue the re-enable (it allocates an operation for it), the stream is left held as it was and the
    /// failure is reported through the diagnostic sink.
    /// <para>
    /// The transport does not complete a pending receive from this thread (<c>StreamReceiveComplete</c>): a completion of
    /// zero bytes that reaches MsQuic before the receive callback has returned to it is dropped (MsQuic only adds the
    /// length to the stream's completion counter while a receive call is active, and completes the receive after a
    /// <c>PENDING</c> return only if that counter is not zero), the re-enable that follows finds a read still pending and
    /// queues nothing, and the stream is never indicated again.
    /// </para>
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
                    ArgumentOutOfRangeException.ThrowIfNegative(bytesConsumed);
                    ArgumentOutOfRangeException.ThrowIfGreaterThan(bytesConsumed, slot.PendingTotal - slot.PendingConsumed);
                    if (Interlocked.CompareExchange(ref slot.ReceiveState, ReceiveIdle, ReceivePending) != ReceivePending) continue;
                    // No receive callback can run for the stream between the exchange above and the re-enable below (MsQuic
                    // has paused it), so the worker reads the new skip count only after it was written.
                    if (bytesConsumed != 0) Volatile.Write(ref slot.SkipBytes, Volatile.Read(ref slot.SkipBytes) + bytesConsumed);
                    int status = slot.Stream!.ReceiveSetEnabled(true);
                    if (MsQuicStatus.Failed(status))
                    {
                        // MsQuic did not queue the re-enable (it allocates an operation for it), so the stream is still
                        // paused. Everything goes back to where it was — a later resume finds the stream held — and the
                        // failure is reported: nothing else would show that the stream stopped.
                        if (bytesConsumed != 0) Volatile.Write(ref slot.SkipBytes, Volatile.Read(ref slot.SkipBytes) - bytesConsumed);
                        Interlocked.CompareExchange(ref slot.ReceiveState, ReceivePending, ReceiveIdle);
                        Diagnose(TransportDiagnosticLevel.Error,
                            $"A held stream could not be resumed (StreamReceiveSetEnabled: {MsQuicStatus.GetName(status)}); it stays held until it is resumed again.", null);
                    }

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
    /// The native handle is closed (<c>StreamClose</c>) and the slot freed by the cleanup work item on a thread-pool thread,
    /// never on the calling thread (<c>StreamClose</c> of a stream MsQuic never started waits for the connection's worker):
    /// the call returns at once and the slot is back in the table a moment later. After
    /// <see cref="ITransportSink.OnStreamShutdownComplete"/>, or for a stream that was never started, that is all. Before
    /// that the stream is aborted in both directions with code 0 and closed once MsQuic reports its shutdown: pending send
    /// completions are still reported (canceled), the shutdown callback is not. Unknown or stale ids are ignored.
    /// </remarks>
    public void CloseStream(TransportStreamId id)
    {
        StreamSlot? slot = Enter(id);
        if (slot is null) return;
        bool close = false;
        try
        {
            bool started = slot.StartRequested;
            // MsQuic raises no events for a stream it never started: take both halves of the hand-off at once, so the
            // connection's shutdown sweep neither reports this stream nor decides to close it too.
            int old = Interlocked.Or(ref slot.CloseFlags, started ? AppClosedFlag : AppClosedFlag | NativeShutdownFlag);
            if ((old & AppClosedFlag) != 0) return;
            Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
            if (!started || (old & NativeShutdownFlag) != 0) close = true;
            else slot.Stream!.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0);
        }
        finally
        {
            Exit(slot);
        }
        if (close) EnqueueDeferredClose(slot, id.Generation);
    }

    // Test seams (internal, for Tedd.Quicly.Transport.MsQuic.Tests): SendStream waits until START_COMPLETE was indicated
    // before it queues the send, which makes the race that the remarks of SendStream describe deterministic; the counters
    // say how many starts a SendStream made and how many of them lost the race.
    private volatile bool _delaySendUntilStartReported;
    private long _startsWithSend;
    private long _startRefusalRaces;

    /// <summary>Test seam: <see cref="SendStream"/> waits (at most 10 s each) for START_COMPLETE, and for the native shutdown of a stream whose start was refused, before it queues a send that started the stream.</summary>
    internal bool DelaySendUntilStartReported
    {
        get => _delaySendUntilStartReported;
        set => _delaySendUntilStartReported = value;
    }

    /// <summary>
    /// Test seam: starts made by <see cref="SendStream"/> (a send carrying <see cref="TransportSendFlags.Start"/> on an
    /// unstarted stream) while <see cref="DelaySendUntilStartReported"/> is set; nothing is counted otherwise.
    /// </summary>
    internal long StartsWithSend => Interlocked.Read(ref _startsWithSend);

    /// <summary>Test seam: sends that returned <see cref="TransportStatus.StreamLimitReached"/> after the worker had already refused their start.</summary>
    internal long StartRefusalRaces => Interlocked.Read(ref _startRefusalRaces);

    /// <summary>
    /// Waits (bounded) for START_COMPLETE of a start this thread queued: a busy wait that yields but never sleeps — a sleep is
    /// a whole timer tick on Windows, and the worker answers in microseconds.
    /// </summary>
    private static void WaitForStartReported(StreamSlot slot, int milliseconds)
    {
        if (slot.StartReported) return;
        long deadline = Environment.TickCount64 + milliseconds;
        SpinWait spin = default;
        while (!slot.StartReported && Environment.TickCount64 < deadline) spin.SpinOnce(sleep1Threshold: -1);
    }

    /// <summary>Waits (bounded, like <see cref="WaitForStartReported"/>) for the native SHUTDOWN_COMPLETE of the slot's stream.</summary>
    private static void WaitForNativeShutdown(StreamSlot slot, int milliseconds)
    {
        long deadline = Environment.TickCount64 + milliseconds;
        SpinWait spin = default;
        while ((Volatile.Read(ref slot.CloseFlags) & NativeShutdownFlag) == 0 && Environment.TickCount64 < deadline) spin.SpinOnce(sleep1Threshold: -1);
    }

    private TransportStatus StartCore(StreamSlot slot)
    {
        slot.StartRequested = true;
        int status = slot.Stream!.Start(StartFlags);
        if (MsQuicStatus.Succeeded(status)) return TransportStatus.Success;
        // From a callback of this connection MsQuic runs the start inline and may already have reported the failure through
        // START_COMPLETE (OnStreamStarted): that stays the only report, and the stream's shutdown follows as for a queued start.
        if (slot.StartReported) return TransportStatus.Success;
        slot.StartRequested = false;
        slot.StartRefused = true;
        return MapStatus(status, datagramSend: false);
    }

    // ------------------------------------------------------------------ slot table

    /// <summary>
    /// Takes a slot for a stream. A local stream is refused (<see cref="TransportStatus.OutOfMemory"/>) once this end's
    /// streams hold their whole share of the table — what the peer was granted is not theirs to take. A peer stream is
    /// within the grants the table was sized for (MsQuic admitted it); when its slot is still held by streams whose native
    /// close has not run yet, the table grows, up to twice the size it was sized for. Past that — a peer that resets
    /// streams faster than a starved thread pool closes them, or an honest peer's churning group streams while the pool is
    /// starved for seconds — the stream is refused (<see cref="RefusedPeerStreamCount"/>) and what it carried can be lost,
    /// so a peer's churn cannot grow the table without bound. The pending closes cannot run here instead: this runs on
    /// the MsQuic worker, and a native close waits for callbacks that may be running on it (<see cref="CloseSlotNative"/>).
    /// </summary>
    private StreamSlot? AllocateSlot(bool local, out TransportStatus failure)
    {
        lock (_tableLock)
        {
            failure = TransportStatus.Success;
            if (_tableClosed)
            {
                failure = TransportStatus.InvalidState;
                return null;
            }
            if (local)
            {
                if (_localSlots >= _sizedStreams - (_grantedPeerBidi + _grantedPeerUnidi))
                {
                    failure = TransportStatus.OutOfMemory;
                    return null;
                }
                _localSlots++;
            }
            if (_freeSlotCount > 0) return _slots[_freeSlots[--_freeSlotCount]];
            if (_slotHighWater == _maxStreams)
            {
                // Every slot is in use although neither side is over what it may hold: slots of closed streams are given
                // back by the cleanup work item, a moment after MsQuic returned their credit to the peer.
                if ((long)_maxStreams >= 2L * _sizedStreams)
                {
                    if (local) _localSlots--;
                    failure = TransportStatus.OutOfMemory;
                    return null;
                }
                _maxStreams = (int)Math.Min(2L * _sizedStreams, _maxStreams + Math.Max(16, _maxStreams / 8));
                _streamTableGrown = 1;
            }
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
        bool local = slot.Local;
        slot.Stream = null;
        slot.Local = false;
        slot.Kind = default;
        slot.OpenContext = 0;
        slot.CanSend = false;
        slot.CanReceive = false;
        slot.Priority = DefaultStreamPriority;
        slot.PriorityPending = false;
        slot.StartRequested = false;
        slot.StartRefused = false;
        slot.StartReported = false;
        slot.StartRefusedByLimit = false;
        slot.SendClosed = false;
        slot.PendingConsumed = 0;
        slot.PendingTotal = 0;
        slot.EarlyResumeBytes = 0;
        slot.SkipBytes = 0;
        slot.FinHeld = false;
        Volatile.Write(ref slot.ReceiveState, ReceiveIdle);
        Volatile.Write(ref slot.CloseFlags, 0);
        // DeferredNext is left alone: only a push writes it, and a drain has always unlinked the slot before freeing it.
        Volatile.Write(ref slot.CloseQueued, 0);
        slot.Generation = slot.Generation == uint.MaxValue ? 1 : slot.Generation + 1;
        Interlocked.And(ref slot.Guard, ~GuardClosing);
        lock (_tableLock)
        {
            if (local) _localSlots--;
            _freeSlots[_freeSlotCount++] = slot.Index;
        }
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

    /// <summary>
    /// Closes the native stream and frees the slot, provided it still holds the incarnation the close was decided for.
    /// Callers hold <see cref="_cleanupLock"/>, so closers never overlap, and never run on an MsQuic callback thread.
    /// </summary>
    private void CloseSlotNative(StreamSlot slot, uint generation)
    {
        if ((Interlocked.Or(ref slot.Guard, GuardClosing) & GuardClosing) != 0) return;
        if (slot.Generation != generation || Volatile.Read(ref slot.Stream) is null)
        {
            // A stale decision: that incarnation is gone already. No other closer can be waiting on the bit (all run under the lock).
            Interlocked.And(ref slot.Guard, ~GuardClosing);
            return;
        }
        SpinWait spin = default;
        while ((Volatile.Read(ref slot.Guard) & ~GuardClosing) != 0) spin.SpinOnce();
        slot.Stream!.Close();
        FreeSlot(slot);
    }

    /// <summary>
    /// Queues the native close of <paramref name="slot"/>'s incarnation <paramref name="generation"/> on the lock-free
    /// deferred-close stack (intrusive, no allocation; at most once per incarnation) and schedules the cleanup work item.
    /// </summary>
    private void EnqueueDeferredClose(StreamSlot slot, uint generation)
    {
        if (Interlocked.Exchange(ref slot.CloseQueued, 1) != 0) return;
        slot.DeferredGeneration = generation;
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
            DrainDeferredClosesLocked();
    }

    private void DrainDeferredClosesLocked()
    {
        int index = Interlocked.Exchange(ref _deferredHead, -1);
        if (index < 0) return;
        // Every queued slot was allocated before it was pushed, so the current array holds it.
        StreamSlot?[] slots = Volatile.Read(ref _slots);
        while (index >= 0)
        {
            StreamSlot slot = slots[index]!;
            int next = slot.DeferredNext;
            CloseSlotNative(slot, slot.DeferredGeneration);
            index = next;
        }
    }

    /// <summary>Closes every stream handle still open (connection teardown, after SHUTDOWN_COMPLETE). Under <see cref="_cleanupLock"/>.</summary>
    private void CloseAllStreamHandlesLocked()
    {
        StreamSlot?[] slots;
        int high;
        lock (_tableLock)
        {
            _tableClosed = true;
            slots = _slots;
            high = Math.Min(_slotHighWater, slots.Length);
        }
        for (int i = 0; i < high; i++)
        {
            StreamSlot? slot = slots[i];
            // A slot with a queued close is closed by the drain that follows (freeing it here would leave it linked).
            if (slot is not null && Volatile.Read(ref slot.Stream) is not null && Volatile.Read(ref slot.CloseQueued) == 0) CloseSlotNative(slot, slot.Generation);
        }
    }

    /// <summary>
    /// Connection SHUTDOWN_COMPLETE (worker thread): reports <see cref="ITransportSink.OnStreamShutdownComplete"/> to
    /// <paramref name="sink"/> for every stream MsQuic did not report (streams never started are not tracked by MsQuic), so
    /// it precedes <c>OnClosed</c>, and closes the table to new streams.
    /// </summary>
    private void ShutdownRemainingStreams(ITransportSink? sink)
    {
        StreamSlot?[] slots;
        int high;
        // One snapshot under the table lock: the array and its high-water mark must belong together (a concurrent OpenStream
        // can grow the table), and a stream published after this point is refused instead of never being reported.
        lock (_tableLock)
        {
            _tableClosed = true;
            slots = _slots;
            high = Math.Min(_slotHighWater, slots.Length);
        }
        for (int i = 0; i < high; i++)
        {
            StreamSlot? slot = slots[i];
            // Pin the slot: the cleanup work item may be closing and freeing it right now (then it is skipped), and it must
            // not free it while this loop reads its flags or the sink runs.
            if (slot is null || !TryPin(slot)) continue;
            try
            {
                Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
                int old = Interlocked.Or(ref slot.CloseFlags, NativeShutdownFlag);
                if ((old & NativeShutdownFlag) != 0) continue;
                if ((old & AppClosedFlag) != 0)
                {
                    EnqueueDeferredClose(slot, slot.Generation);
                    continue;
                }
                sink?.OnStreamShutdownComplete(slot.Id);
            }
            catch (Exception ex)
            {
                RecordCallbackFailure(ex, "ITransportSink.OnStreamShutdownComplete threw during connection shutdown.");
            }
            finally
            {
                Exit(slot);
            }
        }
    }

    /// <summary>True while <see cref="CloseSlotNative"/> closes the slot's stream (MsQuic may indicate its SHUTDOWN_COMPLETE meanwhile).</summary>
    private static bool IsNativeCloseInProgress(StreamSlot slot) => (Volatile.Read(ref slot.Guard) & GuardClosing) != 0;

    /// <summary>Pins a slot that holds a stream and is not being closed (see <see cref="StreamSlot.Guard"/>); false otherwise.</summary>
    private static bool TryPin(StreamSlot slot)
    {
        int guard = Interlocked.Increment(ref slot.Guard);
        if ((guard & GuardClosing) == 0 && Volatile.Read(ref slot.Stream) is not null) return true;
        Interlocked.Decrement(ref slot.Guard);
        return false;
    }

    // ------------------------------------------------------------------ connection / stream events (MsQuic worker thread)

    bool IMsQuicConnectionEvents.PeerStreamStarted(MsQuicConnection connection, MsQuicStream stream, QUIC_STREAM_OPEN_FLAGS flags)
    {
        if (Volatile.Read(ref _closedDelivered) != 0) return false;
        StreamSlot? slot = AllocateSlot(local: false, out TransportStatus failure);
        if (slot is null)
        {
            // The table is closed (the connection is shutting down), or it has grown to twice its size for slots whose
            // streams wait for the cleanup work item and the peer keeps opening new ones (see AllocateSlot).
            if (failure == TransportStatus.OutOfMemory)
            {
                Interlocked.Increment(ref _refusedPeerStreams);
                Diagnose(TransportDiagnosticLevel.Warning, "A peer stream was refused: the stream table grew to twice its size for streams whose close is still waiting for the thread pool.", null);
            }
            return false;
        }

        if (Volatile.Read(ref _streamTableGrown) != 0 && Interlocked.Exchange(ref _streamTableGrowthReported, 1) == 0)
        {
            Diagnose(TransportDiagnosticLevel.Information,
                "The stream table grew past its size (MsQuicTransportOptions.MaxStreams, or what the grants need): closed streams wait for the thread pool's cleanup work item.", null);
        }
        bool unidirectional = (flags & QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL) != 0;
        slot.Local = false;
        slot.Kind = unidirectional ? StreamKind.Unidirectional : StreamKind.Bidirectional;
        slot.CanSend = !unidirectional;
        slot.CanReceive = true;
        slot.StartRequested = true;
        slot.StartReported = true;
        stream.Events = this;
        stream.Tag = slot;
        // Serialised with the connection's shutdown sweep (both run on the worker), so no snapshot can miss this stream.
        Volatile.Write(ref slot.Stream, stream);
        // The wrapper attaches the stream's callback only after this handler returns; attach it now so that API calls
        // the sink makes on the new stream from inside OnPeerStreamStarted see their events.
        stream.AttachCallback();
        try
        {
            LiveSink?.OnPeerStreamStarted(slot.Id, slot.Kind);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
        return true;
    }

    void IMsQuicStreamEvents.StartComplete(MsQuicStream stream, int status, ulong id, bool peerAccepted)
    {
        var slot = (StreamSlot)stream.Tag!;
        bool started = MsQuicStatus.Succeeded(status);
        if (!started) slot.SendClosed = true;
        // Before StartReported: whoever sees the report sees why the start failed.
        if (status == MsQuicStatus.QUIC_STATUS_STREAM_LIMIT_REACHED) slot.StartRefusedByLimit = true;
        slot.StartReported = true;
        try
        {
            // The priority OpenStream or SetStreamPriority stored: set it here, on the worker, where the call runs inline.
            if (started && slot.PriorityPending)
            {
                slot.PriorityPending = false;
                stream.SetPriority(slot.Priority);
            }
            if (slot.AppClosed) return;
            LiveSink?.OnStreamStarted(slot.Id, slot.OpenContext, started ? TransportStatus.Success : MapStatus(status, datagramSend: false));
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }

    MsQuicReceiveResult IMsQuicStreamEvents.Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
    {
        var slot = (StreamSlot)stream.Tag!;
        if (slot.AppClosed) return MsQuicReceiveResult.Consumed(totalLength);
        ITransportSink? sink = LiveSink;
        if (sink is null) return MsQuicReceiveResult.Consumed(totalLength);
        try
        {
            bool fin = (flags & QUIC_RECEIVE_FLAGS.FIN) != 0;
            if (Volatile.Read(ref slot.ReceiveState) == ReceivePending)
            {
                // Indicated although the sink holds the stream (MsQuic has it paused, so this is not expected): hold this
                // indication as well, without showing it to the sink.
                return totalLength == 0 ? MsQuicReceiveResult.Pending : MsQuicReceiveResult.Consumed(0);
            }

            // Bytes the sink already has (SkipBytes): MsQuic indicates them again, the sink must not see them twice. The same
            // goes for a FIN the sink saw on the indication it held: once every byte before it is the sink's, the stream is
            // complete, and MsQuic reports the peer's send shutdown when this callback returns.
            var segments = new ReadOnlySpan<TransportSegment>(buffers, (int)bufferCount);
            long skip = Volatile.Read(ref slot.SkipBytes);
            bool finHeld = slot.FinHeld;
            if (skip != 0 || finHeld)
            {
                slot.FinHeld = false;
                skip = Math.Min(skip, (long)totalLength);
                Volatile.Write(ref slot.SkipBytes, slot.SkipBytes - skip);
                if ((ulong)skip == totalLength && (!fin || finHeld)) return MsQuicReceiveResult.Consumed(totalLength); // nothing the sink has not seen
                if (skip != 0)
                {
                    TransportSegment* trimmed = stackalloc TransportSegment[(int)Math.Min(bufferCount, MaxTrimmedSegments)];
                    segments = TrimSegments(segments, skip, bufferCount <= MaxTrimmedSegments ? new Span<TransportSegment>(trimmed, (int)bufferCount) : new TransportSegment[bufferCount]);
                }
            }

            long offered = (long)totalLength - skip;
            slot.PendingTotal = offered;
            Volatile.Write(ref slot.ReceiveState, ReceiveInCallback);
            ReceiveResult result = sink.OnStreamReceived(slot.Id, segments, absoluteOffset + (ulong)skip, fin);
            long consumed = result.BytesConsumed;
            if (consumed < 0 || consumed > offered)
            {
                throw new InvalidOperationException($"OnStreamReceived consumed {consumed} bytes but {offered} were indicated.");
            }

            // What MsQuic is told: the bytes skipped above are consumed as well.
            long done = skip + consumed;
            if (result.Pending || (consumed == 0 && offered != 0))
            {
                // Back-pressure (Consumed(0) of a non-empty indication counts as PendingAfter(0)). The hold is answered as a
                // partial consumption, which MsQuic completes itself when this callback returns and which pauses the stream
                // until ResumeStreamReceive re-enables it — not as QUIC_STATUS_PENDING, whose completion from another thread
                // can be lost (see ResumeStreamReceive). Two indications cannot be held that way:
                //  - one the sink consumed whole: one byte is kept back, so that the completion is partial, and skipped
                //    when MsQuic indicates it again;
                //  - an empty one (only the FIN): it stays pending, and the re-enable alone makes MsQuic indicate it again
                //    (no completion is involved: MsQuic has no read pending for it).
                // Publish the counts before the state, so that a resume on another thread reads them.
                slot.PendingConsumed = consumed;
                bool keptBack = (ulong)done == totalLength && totalLength != 0;
                if (keptBack) Volatile.Write(ref slot.SkipBytes, 1);
                int previous = Interlocked.CompareExchange(ref slot.ReceiveState, ReceivePending, ReceiveInCallback);
                if (previous == ReceiveInCallback)
                {
                    slot.FinHeld = fin;
                    return totalLength == 0 ? MsQuicReceiveResult.Pending : MsQuicReceiveResult.Consumed((ulong)(keptBack ? done - 1 : done));
                }

                // Not held after all: the byte that was kept back is the sink's, and nothing will indicate it again.
                if (keptBack) Volatile.Write(ref slot.SkipBytes, 0);
                if (previous != ReceiveEarlyResume) return MsQuicReceiveResult.Consumed(totalLength); // aborted or closed meanwhile
                done = Math.Min(done + Volatile.Read(ref slot.EarlyResumeBytes), (long)totalLength);
                Volatile.Write(ref slot.ReceiveState, ReceiveIdle);
            }
            else if (Interlocked.CompareExchange(ref slot.ReceiveState, ReceiveIdle, ReceiveInCallback) != ReceiveInCallback)
            {
                // An early resume without a pending receive is meaningless; an abort or close during the call discards the rest.
                if (Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle) != ReceiveEarlyResume) return MsQuicReceiveResult.Consumed(totalLength);
            }
            if ((ulong)done == totalLength) return MsQuicReceiveResult.Consumed(totalLength);
            // Partial consumption: MsQuic would pause the stream until receives are re-enabled; re-enable them inline so the
            // remainder is indicated again right away (together with anything that arrived since).
            stream.ReceiveSetEnabled(true);
            return MsQuicReceiveResult.Consumed((ulong)done);
        }
        catch (Exception ex)
        {
            Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
            OnHandlerException(ex);
            return MsQuicReceiveResult.Consumed(totalLength);
        }
    }

    /// <summary>
    /// Segments of one indication that are trimmed on the stack when bytes have to be skipped. MsQuic's default receive
    /// mode indicates at most two (its event has room for three); an indication with more than this many — which only a
    /// receive mode the binding does not enable would produce — is trimmed into a managed array instead.
    /// </summary>
    private const int MaxTrimmedSegments = 8;

    /// <summary>
    /// The segments of an indication without its first <paramref name="skip"/> bytes, written to
    /// <paramref name="destination"/> (at least as long as <paramref name="source"/>).
    /// </summary>
    private static ReadOnlySpan<TransportSegment> TrimSegments(ReadOnlySpan<TransportSegment> source, long skip, Span<TransportSegment> destination)
    {
        int count = 0;
        for (int i = 0; i < source.Length; i++)
        {
            TransportSegment segment = source[i];
            if (skip >= segment.Length)
            {
                skip -= segment.Length;
                continue;
            }

            destination[count++] = new TransportSegment(segment.Buffer + skip, (int)(segment.Length - skip));
            skip = 0;
        }

        return destination.Slice(0, count);
    }

    void IMsQuicStreamEvents.SendComplete(MsQuicStream stream, void* clientContext, bool canceled)
    {
        var slot = (StreamSlot)stream.Tag!;
        try
        {
            LiveSink?.OnStreamSendCompleted(slot.Id, (ulong)clientContext, canceled);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }

    void IMsQuicStreamEvents.PeerSendShutdown(MsQuicStream stream)
    {
        var slot = (StreamSlot)stream.Tag!;
        if (slot.AppClosed) return;
        try
        {
            LiveSink?.OnStreamPeerSendShutdown(slot.Id);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }

    void IMsQuicStreamEvents.PeerSendAborted(MsQuicStream stream, ulong errorCode)
    {
        var slot = (StreamSlot)stream.Tag!;
        Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
        if (slot.AppClosed) return;
        try
        {
            LiveSink?.OnStreamAborted(slot.Id, errorCode, StreamAbortDirection.Send);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }

    void IMsQuicStreamEvents.PeerReceiveAborted(MsQuicStream stream, ulong errorCode)
    {
        var slot = (StreamSlot)stream.Tag!;
        slot.SendClosed = true;
        if (slot.AppClosed) return;
        try
        {
            LiveSink?.OnStreamAborted(slot.Id, errorCode, StreamAbortDirection.Receive);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }

    void IMsQuicStreamEvents.ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info)
    {
        var slot = (StreamSlot)stream.Tag!;
        // The cleanup work item is closing this very stream (StreamClose of a stream MsQuic never started indicates
        // SHUTDOWN_COMPLETE): that close frees the slot, so the event must not queue another one.
        if (IsNativeCloseInProgress(slot)) return;
        Interlocked.Exchange(ref slot.ReceiveState, ReceiveIdle);
        int old = Interlocked.Or(ref slot.CloseFlags, NativeShutdownFlag);
        if ((old & NativeShutdownFlag) != 0) return;
        if ((old & AppClosedFlag) != 0)
        {
            EnqueueDeferredClose(slot, slot.Generation);
            return;
        }
        try
        {
            LiveSink?.OnStreamShutdownComplete(slot.Id);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }

    void IMsQuicStreamEvents.IdealSendBufferSize(MsQuicStream stream, ulong byteCount)
    {
        var slot = (StreamSlot)stream.Tag!;
        if (slot.AppClosed) return;
        try
        {
            LiveSink?.OnIdealSendBufferSize(slot.Id, byteCount);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }
}
