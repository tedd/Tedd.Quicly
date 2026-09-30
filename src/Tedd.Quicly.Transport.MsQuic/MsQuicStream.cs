using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// A QUIC stream. Created by <see cref="MsQuicConnection.OpenStream"/> (local) or delivered through
/// <see cref="IMsQuicConnectionEvents.PeerStreamStarted"/> (peer).
/// </summary>
/// <remarks>
/// <para><b>Lifetime.</b> The owner must call <see cref="Close"/> exactly once, normally after
/// <see cref="IMsQuicStreamEvents.ShutdownComplete"/>, and never from an MsQuic callback thread (enforced; defer
/// it to the owner's thread). Closing earlier aborts the stream silently. Never call any method after <see cref="Close"/>.</para>
/// <para><b>Buffers.</b> Buffers and the buffer array passed to <see cref="Send"/> must stay valid and unmodified
/// until the matching <see cref="IMsQuicStreamEvents.SendComplete"/> (send buffering is disabled by default); on
/// a failure status no completion follows. A completion may arrive before <see cref="Send"/> returns.</para>
/// <para><b>Threading.</b> Callbacks are serialised with the owning connection's callbacks on MsQuic worker
/// threads. <see cref="Start"/> may deliver <c>StartComplete</c> inline before it returns.</para>
/// <para><b>Status codes.</b> Calls made off the connection's worker thread are queued and return
/// <c>QUIC_STATUS_PENDING</c>; test acceptance with <see cref="MsQuicStatus.Succeeded"/>, never against
/// <c>QUIC_STATUS_SUCCESS</c>.</para>
/// <para><b>Failure.</b> An exception escaping <see cref="Events"/> is recorded in <see cref="LastCallbackException"/>
/// and poisons the owning connection (see <see cref="MsQuicConnection.Poison"/>). So does a
/// <see cref="IMsQuicStreamEvents.Receive"/> result that claims more bytes than were indicated; MsQuic is then told
/// that nothing was consumed.</para>
/// <para><b>Per-stream cost.</b> Every stream, local or peer-opened, allocates one managed <see cref="MsQuicStream"/>
/// (about 100 bytes) plus one <see cref="GCHandle"/> (not GC heap) as its native context; send, receive and event
/// dispatch then allocate nothing. For workloads that churn thousands of short-lived streams without GC
/// allocations (ARCHITECTURE §7 per-channel unidirectional streams), bypass the wrapper and call the raw table
/// with a caller-owned callback and context, e.g. a generation-tagged slot index (ADR 0008 §2):
/// <c>connection.Api.Table-&gt;StreamOpen(connection.Handle, flags, callback, context, &amp;handle)</c>.</para>
/// </remarks>
public sealed unsafe class MsQuicStream : IDisposable
{
    private readonly MsQuicApi _api;
    private QUIC_HANDLE* _handle;
    private GCHandle _gcHandle;
    private IMsQuicStreamEvents _events;

    /// <summary>The owning connection.</summary>
    public MsQuicConnection Connection { get; }

    /// <summary>The native handle (null after <see cref="Close"/>).</summary>
    public QUIC_HANDLE* Handle => _handle;

    /// <summary>True once <see cref="Close"/> has run.</summary>
    public bool IsClosed => _handle == null;

    /// <summary>Flags the stream was opened with.</summary>
    public QUIC_STREAM_OPEN_FLAGS OpenFlags { get; }

    /// <summary>True for unidirectional streams.</summary>
    public bool IsUnidirectional => (OpenFlags & QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL) != 0;

    /// <summary>True when the peer opened the stream.</summary>
    public bool IsPeerStarted { get; }

    /// <summary>
    /// The QUIC stream id; <see cref="ulong.MaxValue"/> until known. Peer streams know it immediately; local
    /// streams learn it at <c>StartComplete</c> (or earlier through <see cref="QueryId"/> once started).
    /// </summary>
    public ulong Id { get; private set; } = ulong.MaxValue;

    /// <summary>Event sink. Set before <see cref="Start"/> / before returning from PeerStreamStarted.</summary>
    public IMsQuicStreamEvents Events
    {
        get => _events;
        set => _events = value ?? NoOpStreamEvents.Instance;
    }

    /// <summary>Free slot for the owner's state; never touched by the wrapper.</summary>
    public object? Tag { get; set; }

    private Exception? _lastCallbackException;

    /// <summary>
    /// The last exception thrown by <see cref="Events"/> (callbacks never propagate; the wrapper records and poisons
    /// the connection). Written on MsQuic worker threads with a volatile write, so any thread may read it.
    /// </summary>
    public Exception? LastCallbackException => Volatile.Read(ref _lastCallbackException);

    internal MsQuicStream(MsQuicConnection connection, QUIC_HANDLE* handle, QUIC_STREAM_OPEN_FLAGS flags, bool peerStarted)
    {
        Connection = connection;
        _api = connection.Api;
        _handle = handle;
        OpenFlags = flags;
        IsPeerStarted = peerStarted;
        _events = NoOpStreamEvents.Instance;
        _gcHandle = GCHandle.Alloc(this);
        if (peerStarted && MsQuicStatus.Succeeded(_api.GetParam(handle, MsQuicParam.QUIC_PARAM_STREAM_ID, out ulong id)))
        {
            Id = id;
        }
    }

    internal static int Open(MsQuicConnection connection, QUIC_STREAM_OPEN_FLAGS flags, IMsQuicStreamEvents? events, out MsQuicStream? stream)
    {
        stream = null;
        MsQuicApi api = connection.Api;
        QUIC_HANDLE* handle = null;
        // Allocate the wrapper first so its GCHandle can be the native context from the very first callback.
        var s = new MsQuicStream(connection, null, flags, peerStarted: false) { Events = events! };
        int status = api.Table->StreamOpen(connection.Handle, flags, &NativeCallback, (void*)GCHandle.ToIntPtr(s._gcHandle), &handle);
        if (MsQuicStatus.Failed(status))
        {
            s._gcHandle.Free();
            return status;
        }
        s._handle = handle;
        stream = s;
        return status;
    }

    internal void AttachCallback()
    {
        _api.Table->SetCallbackHandler(_handle, (delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*, QUIC_STREAM_EVENT*, int>)&NativeCallback, (void*)GCHandle.ToIntPtr(_gcHandle));
    }

    /// <summary>Closes a peer stream the app declined; no callback was ever attached so no events follow.</summary>
    internal void CloseRejected()
    {
        QUIC_HANDLE* handle = _handle;
        _handle = null;
        _api.Table->StreamClose(handle);
        _gcHandle.Free();
    }

    /// <summary>
    /// Starts a locally-opened stream. Completion is reported by <see cref="IMsQuicStreamEvents.StartComplete"/>,
    /// possibly inline. Returns <c>QUIC_STATUS_PENDING</c> when the start was queued (the normal case off the worker
    /// thread) and <c>QUIC_STATUS_SUCCESS</c> when it ran inline; both mean accepted. With
    /// <see cref="QUIC_STREAM_START_FLAGS.FAIL_BLOCKED"/> a stream-limit failure is reported by StartComplete (and returned
    /// directly when the start runs inline) as <c>QUIC_STATUS_STREAM_LIMIT_REACHED</c>; add
    /// <see cref="QUIC_STREAM_START_FLAGS.SHUTDOWN_ON_FAIL"/> to have MsQuic shut the stream down on failure.
    /// Flags the loaded library does not know (<see cref="MsQuicApi.SupportedStartFlags"/>) are refused with
    /// <c>QUIC_STATUS_NOT_SUPPORTED</c> without calling MsQuic.
    /// </summary>
    public int Start(QUIC_STREAM_START_FLAGS flags)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        if ((flags & ~_api.SupportedStartFlags) != 0) return MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED;
        return _api.Table->StreamStart(_handle, flags);
    }

    /// <summary>
    /// Queues <paramref name="bufferCount"/> gathered buffers for sending. Returns <c>QUIC_STATUS_PENDING</c> (queued) or
    /// <c>QUIC_STATUS_SUCCESS</c> (inline) on acceptance; no allocation.
    /// <see cref="QUIC_SEND_FLAGS.FIN"/> closes the send direction after this data; <see cref="QUIC_SEND_FLAGS.START"/>
    /// starts the stream implicitly; <see cref="QUIC_SEND_FLAGS.DELAY_SEND"/> hints that more data follows. Flags the
    /// loaded library does not know (<see cref="MsQuicApi.SupportedSendFlags"/>) are refused with
    /// <c>QUIC_STATUS_NOT_SUPPORTED</c> without calling MsQuic (and without a completion).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Send(QUIC_BUFFER* buffers, uint bufferCount, QUIC_SEND_FLAGS flags, void* clientContext)
    {
        QUIC_HANDLE* handle = _handle;
        if (handle == null) ThrowDisposed();
        if ((flags & ~_api.SupportedSendFlags) != 0) return MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED;
        return _api.Table->StreamSend(handle, buffers, bufferCount, flags, clientContext);
    }

    /// <summary>
    /// Shuts down the stream: GRACEFUL sends FIN; ABORT_SEND / ABORT_RECEIVE / ABORT reset with <paramref name="errorCode"/>.
    /// <see cref="QUIC_STREAM_SHUTDOWN_FLAGS.INLINE"/> is refused (ADR 0008 §7).
    /// </summary>
    public int Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS flags, ulong errorCode)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        if ((flags & QUIC_STREAM_SHUTDOWN_FLAGS.INLINE) != 0) throw new ArgumentException("INLINE shutdown is never used (ADR 0008 §7).", nameof(flags));
        return _api.Table->StreamShutdown(_handle, flags, errorCode);
    }

    /// <summary>Completes a receive that returned <see cref="MsQuicReceiveResult.Pending"/>, consuming <paramref name="bufferLength"/> bytes.</summary>
    /// <remarks>
    /// Do not complete with zero bytes from another thread: if the call reaches MsQuic before the receive callback that
    /// returns <c>Pending</c> has returned to it, MsQuic (2.5) only adds the length to the stream's completion counter and
    /// completes the receive after the callback only if that counter is not zero. A zero-byte completion is then dropped,
    /// and the receive stays pending for good. Return a partial count from the callback instead (MsQuic pauses the stream)
    /// and call <see cref="ReceiveSetEnabled"/> to go on, as <c>MsQuicTransport</c> does.
    /// </remarks>
    public void ReceiveComplete(ulong bufferLength)
    {
        QUIC_HANDLE* handle = _handle;
        ObjectDisposedException.ThrowIf(handle == null, this);
        _api.Table->StreamReceiveComplete(handle, bufferLength);
    }

    /// <summary>Enables or disables RECEIVE events.</summary>
    public int ReceiveSetEnabled(bool enabled)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.Table->StreamReceiveSetEnabled(_handle, enabled ? (byte)1 : (byte)0);
    }

    /// <summary>Sets the send priority (0 low .. 0xFFFF high, default 0x7FFF).</summary>
    public int SetPriority(ushort priority)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.SetParam(_handle, MsQuicParam.QUIC_PARAM_STREAM_PRIORITY, in priority);
    }

    /// <summary>Reads the stream id through <c>QUIC_PARAM_STREAM_ID</c> (fails until the stream is started) and caches it in <see cref="Id"/>.</summary>
    public int QueryId(out ulong id)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        int status = _api.GetParam(_handle, MsQuicParam.QUIC_PARAM_STREAM_ID, out id);
        if (MsQuicStatus.Succeeded(status)) Id = id;
        return status;
    }

    /// <summary>Typed <c>SetParam</c>.</summary>
    public int SetParam<T>(uint param, in T value) where T : unmanaged
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.SetParam(_handle, param, in value);
    }

    /// <summary>Typed <c>GetParam</c>.</summary>
    public int GetParam<T>(uint param, out T value) where T : unmanaged
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.GetParam(_handle, param, out value);
    }

    /// <summary>Closes the handle (<c>StreamClose</c>) and frees the context. Idempotent. Throws when called from an MsQuic callback thread (ADR 0008 §7).</summary>
    public void Close()
    {
        QUIC_HANDLE* handle = _handle;
        if (handle == null) return;
        MsQuicCallbackScope.ThrowIfInsideCallback("StreamClose");
        _handle = null;
        _api.Table->StreamClose(handle);
        if (_gcHandle.IsAllocated) _gcHandle.Free();
    }

    /// <inheritdoc cref="Close"/>
    public void Dispose() => Close();

    private void ThrowDisposed() => throw new ObjectDisposedException(nameof(MsQuicStream));

    /// <summary>The native callback, so tests can dispatch synthetic events through the full callback path.</summary>
    internal static delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*, QUIC_STREAM_EVENT*, int> NativeCallbackPointer => &NativeCallback;

    /// <summary>The context handed to MsQuic for this object (a <see cref="GCHandle"/>).</summary>
    internal void* NativeContext => (void*)GCHandle.ToIntPtr(_gcHandle);

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static int NativeCallback(QUIC_HANDLE* handle, void* context, QUIC_STREAM_EVENT* evt)
    {
        MsQuicStream? stream = null;
        MsQuicCallbackScope.Enter();
        try
        {
            stream = MsQuicCallbackScope.ResolveContext<MsQuicStream>(context);
            if (stream is null) return MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR;
            return stream.HandleEvent(evt);
        }
        catch (Exception ex)
        {
            if (stream is not null) stream.RecordHandlerFailure(ex);
            else MsQuicCallbackScope.OnEscapedException(ex);
            return MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR;
        }
        finally
        {
            MsQuicCallbackScope.Exit();
        }
    }

    private void RecordHandlerFailure(Exception exception)
    {
        Volatile.Write(ref _lastCallbackException, exception);
        Connection.Poison(exception);
    }

    private int HandleEvent(QUIC_STREAM_EVENT* evt)
    {
        IMsQuicStreamEvents events = _events;
        switch (evt->Type)
        {
            case QUIC_STREAM_EVENT_TYPE.START_COMPLETE:
                ref var start = ref evt->START_COMPLETE;
                if (MsQuicStatus.Succeeded(start.Status)) Id = start.ID;
                events.StartComplete(this, start.Status, start.ID, start.PeerAccepted);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_STREAM_EVENT_TYPE.RECEIVE:
                ref var recv = ref evt->RECEIVE;
                ulong offered = recv.TotalBufferLength;
                MsQuicReceiveResult result = events.Receive(this, recv.Buffers, recv.BufferCount, recv.AbsoluteOffset, offered, recv.Flags);
                if (result.IsPending) return MsQuicStatus.QUIC_STATUS_PENDING;
                if (result.BytesConsumed > offered)
                {
                    // MsQuic must never be told more than it indicated (asserts in debug builds, undefined in release):
                    // report nothing consumed and treat it as a handler failure.
                    recv.TotalBufferLength = 0;
                    RecordHandlerFailure(new InvalidOperationException($"The Receive handler consumed {result.BytesConsumed} bytes but only {offered} were indicated."));
                    return MsQuicStatus.QUIC_STATUS_SUCCESS;
                }
                recv.TotalBufferLength = result.BytesConsumed;
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_STREAM_EVENT_TYPE.SEND_COMPLETE:
                events.SendComplete(this, evt->SEND_COMPLETE.ClientContext, evt->SEND_COMPLETE.Canceled != 0);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_STREAM_EVENT_TYPE.PEER_SEND_SHUTDOWN:
                events.PeerSendShutdown(this);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_STREAM_EVENT_TYPE.PEER_SEND_ABORTED:
                events.PeerSendAborted(this, evt->PEER_SEND_ABORTED.ErrorCode);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_STREAM_EVENT_TYPE.PEER_RECEIVE_ABORTED:
                events.PeerReceiveAborted(this, evt->PEER_RECEIVE_ABORTED.ErrorCode);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_STREAM_EVENT_TYPE.SEND_SHUTDOWN_COMPLETE:
                events.SendShutdownComplete(this, evt->SEND_SHUTDOWN_COMPLETE.Graceful != 0);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_STREAM_EVENT_TYPE.SHUTDOWN_COMPLETE:
                events.ShutdownComplete(this, new MsQuicStreamShutdownInfo(in evt->SHUTDOWN_COMPLETE));
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_STREAM_EVENT_TYPE.IDEAL_SEND_BUFFER_SIZE:
                events.IdealSendBufferSize(this, evt->IDEAL_SEND_BUFFER_SIZE.ByteCount);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_STREAM_EVENT_TYPE.PEER_ACCEPTED:
                events.PeerAccepted(this);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_STREAM_EVENT_TYPE.CANCEL_ON_LOSS:
                events.CancelOnLoss(this, evt->CANCEL_ON_LOSS.ErrorCode);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            default:
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
        }
    }
}
