using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>Listener event sink.</summary>
/// <remarks>Threading: invoked on MsQuic worker threads; <see cref="NewConnection"/> may run concurrently for different connections.</remarks>
public unsafe interface IMsQuicListenerEvents
{
    /// <summary>
    /// A client is connecting. Inspect <paramref name="info"/> (ALPN list, SNI, addresses; valid only during the
    /// call), set <see cref="MsQuicConnection.Events"/> on <paramref name="connection"/> and return the
    /// configuration to accept with, or null to reject. On rejection the wrapper is released and MsQuic drops
    /// the connection; the app must not close it.
    /// </summary>
    MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, ref QUIC_NEW_CONNECTION_INFO info);

    /// <summary>The listener has fully stopped (after <see cref="MsQuicListener.Stop"/> or <see cref="MsQuicListener.Close"/>).</summary>
    void StopComplete(MsQuicListener listener, bool appCloseInProgress)
    {
    }
}

/// <summary>A QUIC listener bound to a local UDP end point and a set of ALPNs.</summary>
/// <remarks>
/// Lifetime: <see cref="Close"/> stops the listener (waiting for STOP_COMPLETE) and frees the handle; call it
/// once, never from inside the listener callback. Connections accepted through it outlive it and must be
/// closed separately. The native context is one <see cref="GCHandle"/> per listener.
/// </remarks>
public sealed unsafe class MsQuicListener : IDisposable
{
    private readonly MsQuicApi _api;
    private readonly IMsQuicListenerEvents _events;
    private QUIC_HANDLE* _handle;
    private GCHandle _gcHandle;

    /// <summary>Owning registration.</summary>
    public MsQuicRegistration Registration { get; }

    /// <summary>The native handle (null after <see cref="Close"/>).</summary>
    public QUIC_HANDLE* Handle => _handle;

    /// <summary>True once <see cref="Close"/> has run.</summary>
    public bool IsClosed => _handle == null;

    /// <summary>True between <see cref="Start"/> and <see cref="Stop"/>.</summary>
    public bool IsStarted { get; private set; }

    /// <summary>Free slot for the owner's state; never touched by the wrapper.</summary>
    public object? Tag { get; set; }

    /// <summary>The last exception thrown by the event sink (callbacks must not throw; the wrapper records and swallows).</summary>
    public Exception? LastCallbackException { get; private set; }

    /// <summary>Opens a listener (not yet started).</summary>
    public MsQuicListener(MsQuicRegistration registration, IMsQuicListenerEvents events)
    {
        ArgumentNullException.ThrowIfNull(registration);
        ArgumentNullException.ThrowIfNull(events);
        ObjectDisposedException.ThrowIf(registration.IsClosed, registration);
        Registration = registration;
        _api = registration.Api;
        _events = events;
        _gcHandle = GCHandle.Alloc(this);
        QUIC_HANDLE* handle = null;
        int status = _api.Table->ListenerOpen(registration.Handle, &NativeCallback, (void*)GCHandle.ToIntPtr(_gcHandle), &handle);
        if (MsQuicStatus.Failed(status))
        {
            _gcHandle.Free();
            throw new MsQuicException(status, "ListenerOpen");
        }
        _handle = handle;
    }

    /// <summary>Starts listening on <paramref name="localEndPoint"/> (port 0 = ephemeral; see <see cref="LocalEndPoint"/>) for the given ALPNs.</summary>
    public void Start(IPEndPoint localEndPoint, ReadOnlySpan<string> alpns)
    {
        ArgumentNullException.ThrowIfNull(localEndPoint);
        ObjectDisposedException.ThrowIf(_handle == null, this);
        if (alpns.Length == 0) throw new ArgumentException("At least one ALPN is required.", nameof(alpns));
        QuicAddr addr = QuicAddr.FromIPEndPoint(localEndPoint);
        AlpnList list = AlpnList.Create(alpns);
        try
        {
            int status = _api.Table->ListenerStart(_handle, list.Buffers, list.Count, &addr);
            MsQuicException.ThrowIfFailed(status, "ListenerStart");
            IsStarted = true;
        }
        finally
        {
            list.Free();
        }
    }

    /// <summary>The bound local address (valid after <see cref="Start"/>).</summary>
    public int GetLocalAddress(out QuicAddr address)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.GetParam(_handle, MsQuicParam.QUIC_PARAM_LISTENER_LOCAL_ADDRESS, out address);
    }

    /// <summary>The bound local end point (allocates); throws when not started.</summary>
    public IPEndPoint LocalEndPoint
    {
        get
        {
            int status = GetLocalAddress(out QuicAddr addr);
            MsQuicException.ThrowIfFailed(status, "GetParam(LISTENER_LOCAL_ADDRESS)");
            return addr.ToIPEndPoint() ?? throw new InvalidOperationException("Listener is not bound.");
        }
    }

    /// <summary>Stops accepting connections; STOP_COMPLETE is delivered asynchronously. Idempotent.</summary>
    public void Stop()
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        if (!IsStarted) return;
        IsStarted = false;
        _api.Table->ListenerStop(_handle);
    }

    /// <summary>Stops (if needed), closes the handle and frees the context. Idempotent. Blocks until stopped.</summary>
    public void Close()
    {
        QUIC_HANDLE* handle = _handle;
        if (handle == null) return;
        _handle = null;
        IsStarted = false;
        _api.Table->ListenerClose(handle);
        if (_gcHandle.IsAllocated) _gcHandle.Free();
    }

    /// <inheritdoc cref="Close"/>
    public void Dispose() => Close();

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static int NativeCallback(QUIC_HANDLE* handle, void* context, QUIC_LISTENER_EVENT* evt)
    {
        var listener = (MsQuicListener)GCHandle.FromIntPtr((nint)context).Target!;
        try
        {
            return listener.HandleEvent(evt);
        }
        catch (Exception ex)
        {
            listener.LastCallbackException = ex;
            return MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR;
        }
    }

    private int HandleEvent(QUIC_LISTENER_EVENT* evt)
    {
        switch (evt->Type)
        {
            case QUIC_LISTENER_EVENT_TYPE.NEW_CONNECTION:
                return HandleNewConnection(evt->NEW_CONNECTION.Connection, evt->NEW_CONNECTION.Info);
            case QUIC_LISTENER_EVENT_TYPE.STOP_COMPLETE:
                _events.StopComplete(this, evt->STOP_COMPLETE.AppCloseInProgress);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            default:
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
        }
    }

    private int HandleNewConnection(QUIC_HANDLE* connectionHandle, QUIC_NEW_CONNECTION_INFO* info)
    {
        var connection = new MsQuicConnection(_api, connectionHandle);
        MsQuicConfiguration? configuration;
        try
        {
            configuration = _events.NewConnection(this, connection, ref *info);
        }
        catch (Exception ex)
        {
            LastCallbackException = ex;
            configuration = null;
        }
        if (configuration is null || configuration.IsClosed)
        {
            connection.Abandon();
            return MsQuicStatus.QUIC_STATUS_CONNECTION_REFUSED;
        }
        // The handler must be in place before the configuration is applied (MsQuic starts indicating events once
        // the handshake proceeds); the connection is owned by the app from here on.
        connection.AttachCallback();
        int status = connection.SetConfiguration(configuration);
        if (MsQuicStatus.Failed(status))
        {
            // MsQuic drops a connection whose NEW_CONNECTION callback fails and never indicates events for it.
            connection.Abandon();
            return status;
        }
        return MsQuicStatus.QUIC_STATUS_SUCCESS;
    }
}
