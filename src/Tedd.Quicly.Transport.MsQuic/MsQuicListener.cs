using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// Read-only view over <see cref="QUIC_NEW_CONNECTION_INFO"/> for the new-connection callback. Every span and
/// reference is valid only during the callback.
/// </summary>
public readonly unsafe ref struct MsQuicNewConnectionInfo
{
    private readonly QUIC_NEW_CONNECTION_INFO* _info;

    internal MsQuicNewConnectionInfo(QUIC_NEW_CONNECTION_INFO* info) => _info = info;

    /// <summary>The raw structure.</summary>
    public QUIC_NEW_CONNECTION_INFO* Raw => _info;

    /// <summary>Negotiated QUIC version (1 for RFC 9000).</summary>
    public uint QuicVersion => _info->QuicVersion;

    /// <summary>The peer's address.</summary>
    public ref readonly QUIC_ADDR RemoteAddress => ref *_info->RemoteAddress;

    /// <summary>The local address the connection arrived on.</summary>
    public ref readonly QUIC_ADDR LocalAddress => ref *_info->LocalAddress;

    /// <summary>The SNI the client sent (UTF-8, not null-terminated; empty when absent).</summary>
    public ReadOnlySpan<byte> ServerName => new(_info->ServerName, _info->ServerNameLength);

    /// <summary>The ALPN MsQuic negotiated (the first entry of the client's list that the listener offers).</summary>
    public ReadOnlySpan<byte> NegotiatedAlpn => new(_info->NegotiatedAlpn, _info->NegotiatedAlpnLength);

    /// <summary>The full ALPN list the client offered, wire format (one-byte length prefix per entry).</summary>
    public ReadOnlySpan<byte> ClientAlpnList => new(_info->ClientAlpnList, _info->ClientAlpnListLength);

    /// <summary>The raw ClientHello bytes.</summary>
    public ReadOnlySpan<byte> CryptoBuffer => new(_info->CryptoBuffer, (int)_info->CryptoBufferLength);

    /// <summary>True when <see cref="NegotiatedAlpn"/> equals <paramref name="alpn"/> (ASCII, ordinal).</summary>
    public bool NegotiatedAlpnIs(ReadOnlySpan<byte> alpn) => NegotiatedAlpn.SequenceEqual(alpn);
}

/// <summary>Listener event sink.</summary>
/// <remarks>Threading: invoked on MsQuic worker threads; <see cref="NewConnection"/> may run concurrently for different connections. Exceptions are recorded in <see cref="MsQuicListener.LastCallbackException"/> and reject the connection.</remarks>
public interface IMsQuicListenerEvents
{
    /// <summary>
    /// A client is connecting. Inspect <paramref name="info"/> (remote address, SNI, negotiated ALPN; valid only
    /// during the call), set <see cref="MsQuicConnection.Events"/> on <paramref name="connection"/> and return the
    /// configuration to accept with (one per ALPN when several are hosted), or null to reject. On rejection MsQuic
    /// drops the connection and the wrapper is released; the app must not use it afterwards.
    /// </summary>
    MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, in MsQuicNewConnectionInfo info);

    /// <summary>The listener has fully stopped (after <see cref="MsQuicListener.Stop"/> or <see cref="MsQuicListener.Close"/>).</summary>
    void StopComplete(MsQuicListener listener, bool appCloseInProgress)
    {
    }

    /// <summary>Denial-of-service mitigation mode toggled (only when <c>QUIC_PARAM_DOS_MODE_EVENTS</c> is enabled).</summary>
    void DosModeChanged(MsQuicListener listener, bool enabled)
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

    /// <summary>The last exception thrown by the event sink (callbacks never propagate; the wrapper records and rejects).</summary>
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
        QUIC_ADDR addr = QUIC_ADDR.FromIPEndPoint(localEndPoint);
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
    public int GetLocalAddress(out QUIC_ADDR address)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.GetParam(_handle, MsQuicParam.QUIC_PARAM_LISTENER_LOCAL_ADDRESS, out address);
    }

    /// <summary>The bound local end point (allocates); throws when not started.</summary>
    public IPEndPoint LocalEndPoint
    {
        get
        {
            int status = GetLocalAddress(out QUIC_ADDR addr);
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

    /// <summary>Stops (if needed), closes the handle and frees the context. Idempotent. Blocks until stopped. Never call it from a callback thread.</summary>
    public void Close()
    {
        QUIC_HANDLE* handle = _handle;
        if (handle == null) return;
        MsQuicCallbackScope.ThrowIfInsideCallback("ListenerClose");
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
        MsQuicCallbackScope.Enter();
        try
        {
            return listener.HandleEvent(evt);
        }
        catch (Exception ex)
        {
            listener.LastCallbackException = ex;
            MsQuicCallbackScope.OnEscapedException(ex);
            return MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR;
        }
        finally
        {
            MsQuicCallbackScope.Exit();
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
            case QUIC_LISTENER_EVENT_TYPE.DOS_MODE_CHANGED:
                _events.DosModeChanged(this, evt->DOS_MODE_CHANGED.DosModeEnabled);
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
            configuration = _events.NewConnection(this, connection, new MsQuicNewConnectionInfo(info));
        }
        catch (Exception ex)
        {
            LastCallbackException = ex;
            MsQuicCallbackScope.OnEscapedException(ex);
            configuration = null;
        }
        if (configuration is null || configuration.IsClosed)
        {
            // MsQuic drops a connection whose NEW_CONNECTION callback fails and never indicates events for it.
            connection.Abandon();
            return MsQuicStatus.QUIC_STATUS_CONNECTION_REFUSED;
        }
        // The handler must be in place before the configuration is applied (MsQuic starts indicating events once
        // the handshake proceeds); the connection is owned by the app from here on.
        connection.AttachCallback();
        int status = connection.SetConfiguration(configuration);
        if (MsQuicStatus.Failed(status))
        {
            connection.Abandon();
            return status;
        }
        return MsQuicStatus.QUIC_STATUS_SUCCESS;
    }
}
