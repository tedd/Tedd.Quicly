using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// A QUIC connection (client or server side). Events are delivered to <see cref="Events"/>.
/// </summary>
/// <remarks>
/// <para><b>Lifetime.</b> A client connection is created with the constructor and started with <see cref="Start"/>;
/// a server connection is created by <see cref="MsQuicListener"/> and handed to
/// <see cref="IMsQuicListenerEvents.NewConnection"/>. In both cases the owner must call <see cref="Close"/>
/// exactly once, normally after <see cref="IMsQuicConnectionEvents.ShutdownComplete"/>, and never from an MsQuic
/// callback thread (enforced). Close streams before the connection. Never call any method after <see cref="Close"/>.</para>
/// <para><b>Threading.</b> Callbacks arrive on MsQuic worker threads, serialised per connection (including its
/// streams); the worker may change between callbacks. API calls are thread-safe as far as MsQuic makes them so;
/// calls made from inside a callback run inline and may deliver events re-entrantly. Completions may arrive
/// before the API call that caused them returns.</para>
/// <para><b>Context.</b> The native context is one <see cref="GCHandle"/> per connection object, allocated in the
/// constructor and freed in <see cref="Close"/>; nothing is allocated per event.</para>
/// <para><b>Failure.</b> An exception escaping <see cref="Events"/> is recorded in <see cref="LastCallbackException"/>,
/// <see cref="IsPoisoned"/> becomes true, the connection is shut down with <see cref="CallbackFailureErrorCode"/>
/// and the callback returns <c>QUIC_STATUS_INTERNAL_ERROR</c>.</para>
/// </remarks>
public sealed unsafe class MsQuicConnection : IDisposable
{
    /// <summary>Application error code used when a callback handler threw (ADR 0008 §8).</summary>
    public const ulong CallbackFailureErrorCode = 0xFFFF_FFFF;

    private readonly MsQuicApi _api;
    private QUIC_HANDLE* _handle;
    private GCHandle _gcHandle;
    private IMsQuicConnectionEvents _events;
    private bool _portableCertificate;
    private bool _defersCertificateValidation;
    private Exception? _lastCallbackException;
    private int _poisoned;
    private MsQuicOwnedCredential? _credential;

    /// <summary>Longest server name <see cref="Start"/> accepts, in UTF-8 bytes (the SNI / DNS name limit).</summary>
    public const int MaxServerNameLength = 255;

    /// <summary>The native handle (null after <see cref="Close"/>).</summary>
    public QUIC_HANDLE* Handle => _handle;

    /// <summary>True once <see cref="Close"/> has run.</summary>
    public bool IsClosed => _handle == null;

    /// <summary>True for connections accepted by a listener.</summary>
    public bool IsServer { get; }

    /// <summary>The API instance.</summary>
    public MsQuicApi Api => _api;

    /// <summary>Event sink. Replace before <see cref="Start"/> / before returning from <see cref="IMsQuicListenerEvents.NewConnection"/>.</summary>
    public IMsQuicConnectionEvents Events
    {
        get => _events;
        set => _events = value ?? NoOpConnectionEvents.Instance;
    }

    /// <summary>Free slot for the owner's state; never touched by the wrapper.</summary>
    public object? Tag { get; set; }

    /// <summary>
    /// The last exception thrown by an event handler of this connection or one of its streams. Written on MsQuic
    /// worker threads and published with a volatile write, so any thread may read it.
    /// </summary>
    public Exception? LastCallbackException => Volatile.Read(ref _lastCallbackException);

    /// <summary>True once a handler exception has poisoned the connection (it is being shut down). Thread-safe.</summary>
    public bool IsPoisoned => Volatile.Read(ref _poisoned) != 0;

    /// <summary>Creates an unstarted client connection.</summary>
    public MsQuicConnection(MsQuicRegistration registration, IMsQuicConnectionEvents? events = null)
    {
        ArgumentNullException.ThrowIfNull(registration);
        ObjectDisposedException.ThrowIf(registration.IsClosed, registration);
        _api = registration.Api;
        _events = events ?? NoOpConnectionEvents.Instance;
        _gcHandle = GCHandle.Alloc(this);
        QUIC_HANDLE* handle = null;
        int status = _api.Table->ConnectionOpen(registration.Handle, &NativeCallback, (void*)GCHandle.ToIntPtr(_gcHandle), &handle);
        if (MsQuicStatus.Failed(status))
        {
            _gcHandle.Free();
            throw new MsQuicException(status, "ConnectionOpen");
        }
        _handle = handle;
    }

    /// <summary>Wraps a listener-accepted connection. The callback is attached only once the app accepts it.</summary>
    internal MsQuicConnection(MsQuicApi api, QUIC_HANDLE* handle)
    {
        _api = api;
        _handle = handle;
        _events = NoOpConnectionEvents.Instance;
        IsServer = true;
        _gcHandle = GCHandle.Alloc(this);
    }

    internal void AttachCallback()
    {
        _api.Table->SetCallbackHandler(_handle, (delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*, QUIC_CONNECTION_EVENT*, int>)&NativeCallback, (void*)GCHandle.ToIntPtr(_gcHandle));
    }

    /// <summary>Releases the wrapper for a connection MsQuic keeps ownership of (listener rejected it).</summary>
    internal void Abandon()
    {
        _handle = null;
        if (_gcHandle.IsAllocated) _gcHandle.Free();
        Interlocked.Exchange(ref _credential, null)?.Release();
    }

    /// <summary>
    /// Client: starts the handshake to <paramref name="serverName"/>:<paramref name="serverPort"/>. Returns the status
    /// (asynchronous completion via events). <paramref name="serverName"/> is limited to
    /// <see cref="MaxServerNameLength"/> UTF-8 bytes. Returns <c>QUIC_STATUS_INVALID_STATE</c> without calling MsQuic
    /// when the configuration's owned credential has already been released (the configuration was closed).
    /// </summary>
    public int Start(MsQuicConfiguration configuration, string serverName, ushort serverPort, int addressFamily = QuicAddressFamily.UNSPEC)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrEmpty(serverName);
        QUIC_HANDLE* handle = _handle;
        ObjectDisposedException.ThrowIf(handle == null, this);
        if (Encoding.UTF8.GetByteCount(serverName) > MaxServerNameLength)
        {
            throw new ArgumentException($"The server name must be at most {MaxServerNameLength} UTF-8 bytes.", nameof(serverName));
        }
        byte* name = stackalloc byte[MaxServerNameLength + 1];
        int written = Encoding.UTF8.GetBytes(serverName, new Span<byte>(name, MaxServerNameLength));
        name[written] = 0;
        if (!TryAcquireCredential(configuration, out MsQuicOwnedCredential? lease)) return MsQuicStatus.QUIC_STATUS_INVALID_STATE;
        RememberCredentialFlags(configuration);
        int status = _api.Table->ConnectionStart(handle, configuration.Handle, (ushort)addressFamily, (sbyte*)name, serverPort);
        CommitCredential(lease, status);
        return status;
    }

    /// <summary>
    /// Server: sets the configuration (normally done by the listener from the NewConnection result). Returns
    /// <c>QUIC_STATUS_INVALID_STATE</c> without calling MsQuic when the configuration's owned credential has already
    /// been released (the configuration was closed).
    /// </summary>
    public int SetConfiguration(MsQuicConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        QUIC_HANDLE* handle = _handle;
        ObjectDisposedException.ThrowIf(handle == null, this);
        if (!TryAcquireCredential(configuration, out MsQuicOwnedCredential? lease)) return MsQuicStatus.QUIC_STATUS_INVALID_STATE;
        RememberCredentialFlags(configuration);
        int status = _api.Table->ConnectionSetConfiguration(handle, configuration.Handle);
        CommitCredential(lease, status);
        return status;
    }

    /// <summary>
    /// Takes a reference on the key container the configuration persisted (if any), so it outlives
    /// <see cref="MsQuicConfiguration.Close"/> for as long as this connection exists (ADR 0009). False when the
    /// credential is already gone.
    /// </summary>
    private static bool TryAcquireCredential(MsQuicConfiguration configuration, out MsQuicOwnedCredential? lease)
    {
        lease = configuration.OwnedCredential;
        if (lease is null || lease.TryAddRef()) return true;
        lease = null;
        return false;
    }

    /// <summary>Keeps the reference when MsQuic accepted the configuration, drops it otherwise.</summary>
    private void CommitCredential(MsQuicOwnedCredential? lease, int status)
    {
        if (MsQuicStatus.Failed(status))
        {
            lease?.Release();
            return;
        }
        Interlocked.Exchange(ref _credential, lease)?.Release();
    }

    private void RememberCredentialFlags(MsQuicConfiguration configuration)
    {
        _portableCertificate = configuration.IndicatesPortableCertificate;
        _defersCertificateValidation = configuration.DefersCertificateValidation;
    }

    /// <summary>
    /// Queues a datagram made of <paramref name="bufferCount"/> gathered buffers. The buffers and the buffer array
    /// must stay valid until <see cref="IMsQuicConnectionEvents.DatagramSendStateChanged"/> reports
    /// <c>SENT</c>/<c>CANCELED</c> for <paramref name="clientContext"/>; the context is reported until a final
    /// state. Acceptance is <c>QUIC_STATUS_PENDING</c> (or <c>QUIC_STATUS_SUCCESS</c> inline); on a failure status no
    /// event follows. No allocation. Flags the loaded library does not know
    /// (<see cref="MsQuicApi.SupportedSendFlags"/>, e.g. <c>CANCEL_ON_BLOCKED</c> before 2.4) are refused with
    /// <c>QUIC_STATUS_NOT_SUPPORTED</c> without calling MsQuic.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int SendDatagram(QUIC_BUFFER* buffers, uint bufferCount, QUIC_SEND_FLAGS flags, void* clientContext)
    {
        QUIC_HANDLE* handle = _handle;
        if (handle == null) ThrowDisposed();
        if ((flags & ~_api.SupportedSendFlags) != 0) return MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED;
        return _api.Table->DatagramSend(handle, buffers, bufferCount, flags, clientContext);
    }

    /// <summary>Opens a locally-initiated stream (not started). Returns the status; <paramref name="stream"/> is set on success.</summary>
    public int OpenStream(QUIC_STREAM_OPEN_FLAGS flags, IMsQuicStreamEvents? events, out MsQuicStream? stream)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return MsQuicStream.Open(this, flags, events, out stream);
    }

    /// <summary>Starts closing the connection with an application error code. Completion is signalled by ShutdownComplete.</summary>
    public void Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS flags, ulong errorCode)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        _api.Table->ConnectionShutdown(_handle, flags, errorCode);
    }

    /// <summary>Reads <c>QUIC_PARAM_CONN_STATISTICS_V2</c> without allocating (see <see cref="MsQuicApi.StatisticsV2Size"/> for how many bytes the library fills).</summary>
    public int GetStatisticsV2(out QUIC_STATISTICS_V2 statistics) => GetStatisticsV2(out statistics, out _);

    /// <summary>Reads <c>QUIC_PARAM_CONN_STATISTICS_V2</c>; <paramref name="bytesWritten"/> says up to which field the struct is valid (compare with <see cref="QUIC_STATISTICS_V2.SIZE_4"/> etc.).</summary>
    public int GetStatisticsV2(out QUIC_STATISTICS_V2 statistics, out uint bytesWritten)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        statistics = default;
        uint length = (uint)sizeof(QUIC_STATISTICS_V2);
        int status;
        fixed (QUIC_STATISTICS_V2* p = &statistics)
        {
            status = _api.Table->GetParam(_handle, MsQuicParam.QUIC_PARAM_CONN_STATISTICS_V2, &length, p);
        }
        bytesWritten = MsQuicStatus.Succeeded(status) ? Math.Min(length, (uint)sizeof(QUIC_STATISTICS_V2)) : 0;
        return status;
    }

    /// <summary>Reads the remote address without allocating.</summary>
    public int GetRemoteAddress(out QUIC_ADDR address)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.GetParam(_handle, MsQuicParam.QUIC_PARAM_CONN_REMOTE_ADDRESS, out address);
    }

    /// <summary>Reads the local address without allocating.</summary>
    public int GetLocalAddress(out QUIC_ADDR address)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.GetParam(_handle, MsQuicParam.QUIC_PARAM_CONN_LOCAL_ADDRESS, out address);
    }

    /// <summary>The remote end point, or null when not yet known (allocates).</summary>
    public IPEndPoint? RemoteEndPoint => MsQuicStatus.Succeeded(GetRemoteAddress(out QUIC_ADDR a)) ? a.ToIPEndPoint() : null;

    /// <summary>The local end point, or null when not yet known (allocates).</summary>
    public IPEndPoint? LocalEndPoint => MsQuicStatus.Succeeded(GetLocalAddress(out QUIC_ADDR a)) ? a.ToIPEndPoint() : null;

    /// <summary>Applies <c>QUIC_PARAM_CONN_SETTINGS</c> to a live connection (only fields with their IsSet bit are changed).</summary>
    public int UpdateSettings(in QUIC_SETTINGS settings)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.SetParam(_handle, MsQuicParam.QUIC_PARAM_CONN_SETTINGS, in settings);
    }

    /// <summary>
    /// Applies a settings builder to a live connection. Start from <see cref="MsQuicSettings.Empty"/>: the default
    /// builder carries handshake-time settings (MTU bounds, resumption level, ...) that MsQuic refuses on a live
    /// connection with <c>QUIC_STATUS_INVALID_PARAMETER</c>.
    /// </summary>
    public int UpdateSettings(MsQuicSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        QUIC_SETTINGS native = settings.ToNative();
        return UpdateSettings(in native);
    }

    /// <summary>
    /// Changes how many bidirectional / unidirectional streams the peer may have open at once
    /// (<c>PeerBidiStreamCount</c> / <c>PeerUnidiStreamCount</c> through <c>QUIC_PARAM_CONN_SETTINGS</c>; the
    /// <c>QUIC_PARAM_CONN_LOCAL_*_STREAM_COUNT</c> parameters are read-only). Raising a limit sends MAX_STREAMS and
    /// the peer sees <see cref="IMsQuicConnectionEvents.StreamsAvailable"/>; QUIC never takes stream credit back,
    /// so a lower value only limits future credit. No allocation.
    /// </summary>
    public int UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        QUIC_SETTINGS settings = default;
        settings.SetPeerBidiStreamCount(bidirectional);
        settings.SetPeerUnidiStreamCount(unidirectional);
        return _api.SetParam(_handle, MsQuicParam.QUIC_PARAM_CONN_SETTINGS, in settings);
    }

    /// <summary>
    /// Finishes a certificate validation the handler deferred (<see cref="MsQuicCertificateDecision.Defer"/>).
    /// <paramref name="alert"/> is sent to the peer when <paramref name="accept"/> is false.
    /// </summary>
    public int CompleteCertificateValidation(bool accept, QUIC_TLS_ALERT_CODES alert = QUIC_TLS_ALERT_CODES.BAD_CERTIFICATE)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.Table->ConnectionCertificateValidationComplete(_handle, accept ? (byte)1 : (byte)0, alert);
    }

    /// <summary>Server: sends a resumption ticket carrying up to 65 535 bytes of application data.</summary>
    public int SendResumptionTicket(QUIC_SEND_RESUMPTION_FLAGS flags, ReadOnlySpan<byte> resumptionData)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        if (resumptionData.Length > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(resumptionData));
        fixed (byte* p = resumptionData)
        {
            return _api.Table->ConnectionSendResumptionTicket(_handle, flags, (ushort)resumptionData.Length, p);
        }
    }

    /// <summary>Raw <c>SetParam</c> on this connection.</summary>
    public int SetParam(uint param, uint bufferLength, void* buffer)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.SetParam(_handle, param, bufferLength, buffer);
    }

    /// <summary>Raw <c>GetParam</c> on this connection.</summary>
    public int GetParam(uint param, uint* bufferLength, void* buffer)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        return _api.GetParam(_handle, param, bufferLength, buffer);
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

    /// <summary>
    /// Closes the handle (<c>ConnectionClose</c>) and frees the context. Idempotent. Blocks until no callback is
    /// running, which is why it throws <see cref="InvalidOperationException"/> when called from an MsQuic callback
    /// thread (ADR 0008 §7). MsQuic keeps the native connection alive until every stream handle is closed too.
    /// Also drops this connection's reference on a key container its configuration persisted; the container is
    /// deleted once the configuration and every connection that used it are closed (ADR 0009).
    /// </summary>
    public void Close()
    {
        QUIC_HANDLE* handle = _handle;
        if (handle == null) return;
        MsQuicCallbackScope.ThrowIfInsideCallback("ConnectionClose");
        _handle = null;
        _api.Table->ConnectionClose(handle);
        if (_gcHandle.IsAllocated) _gcHandle.Free();
        Interlocked.Exchange(ref _credential, null)?.Release();
    }

    /// <inheritdoc cref="Close"/>
    public void Dispose() => Close();

    private void ThrowDisposed() => throw new ObjectDisposedException(nameof(MsQuicConnection));

    /// <summary>Records a handler exception (from this connection or one of its streams) and shuts the connection down once.</summary>
    internal void Poison(Exception exception)
    {
        Volatile.Write(ref _lastCallbackException, exception);
        MsQuicCallbackScope.OnEscapedException(exception);
        // Read the handle once: Close on the owner thread may clear the field concurrently (it then blocks in
        // ConnectionClose until this callback returns, so the local copy stays valid).
        QUIC_HANDLE* handle = _handle;
        if (handle == null || Interlocked.Exchange(ref _poisoned, 1) != 0) return;
        _api.Table->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, CallbackFailureErrorCode);
    }

    /// <summary>The native callback, so tests can dispatch synthetic events through the full callback path.</summary>
    internal static delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*, QUIC_CONNECTION_EVENT*, int> NativeCallbackPointer => &NativeCallback;

    /// <summary>The context handed to MsQuic for this object (a <see cref="GCHandle"/>).</summary>
    internal void* NativeContext => (void*)GCHandle.ToIntPtr(_gcHandle);

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static int NativeCallback(QUIC_HANDLE* handle, void* context, QUIC_CONNECTION_EVENT* evt)
    {
        MsQuicConnection? connection = null;
        MsQuicCallbackScope.Enter();
        try
        {
            connection = MsQuicCallbackScope.ResolveContext<MsQuicConnection>(context);
            if (connection is null) return MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR;
            return connection.HandleEvent(evt);
        }
        catch (Exception ex)
        {
            if (connection is not null) connection.Poison(ex);
            else MsQuicCallbackScope.OnEscapedException(ex);
            return MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR;
        }
        finally
        {
            MsQuicCallbackScope.Exit();
        }
    }

    private int HandleEvent(QUIC_CONNECTION_EVENT* evt)
    {
        IMsQuicConnectionEvents events = _events;
        switch (evt->Type)
        {
            case QUIC_CONNECTION_EVENT_TYPE.CONNECTED:
                events.Connected(this, evt->CONNECTED.NegotiatedAlpnSpan, evt->CONNECTED.SessionResumed != 0);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.SHUTDOWN_INITIATED_BY_TRANSPORT:
                events.ShutdownInitiatedByTransport(this, evt->SHUTDOWN_INITIATED_BY_TRANSPORT.Status, evt->SHUTDOWN_INITIATED_BY_TRANSPORT.ErrorCode);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.SHUTDOWN_INITIATED_BY_PEER:
                events.ShutdownInitiatedByPeer(this, evt->SHUTDOWN_INITIATED_BY_PEER.ErrorCode);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.SHUTDOWN_COMPLETE:
                ref var sc = ref evt->SHUTDOWN_COMPLETE;
                events.ShutdownComplete(this, sc.HandshakeCompleted, sc.PeerAcknowledgedShutdown, sc.AppCloseInProgress);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.LOCAL_ADDRESS_CHANGED:
                events.LocalAddressChanged(this, in *evt->LOCAL_ADDRESS_CHANGED.Address);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.PEER_ADDRESS_CHANGED:
                events.PeerAddressChanged(this, in *evt->PEER_ADDRESS_CHANGED.Address);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.PEER_STREAM_STARTED:
                return HandlePeerStreamStarted(evt->PEER_STREAM_STARTED.Stream, evt->PEER_STREAM_STARTED.Flags);
            case QUIC_CONNECTION_EVENT_TYPE.STREAMS_AVAILABLE:
                events.StreamsAvailable(this, evt->STREAMS_AVAILABLE.BidirectionalCount, evt->STREAMS_AVAILABLE.UnidirectionalCount);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.PEER_NEEDS_STREAMS:
                events.PeerNeedsStreams(this, evt->PEER_NEEDS_STREAMS.Bidirectional != 0);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.DATAGRAM_STATE_CHANGED:
                events.DatagramStateChanged(this, evt->DATAGRAM_STATE_CHANGED.SendEnabled != 0, evt->DATAGRAM_STATE_CHANGED.MaxSendLength);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.DATAGRAM_RECEIVED:
                QUIC_BUFFER* buffer = evt->DATAGRAM_RECEIVED.Buffer;
                events.DatagramReceived(this, new ReadOnlySpan<byte>(buffer->Buffer, (int)buffer->Length), evt->DATAGRAM_RECEIVED.Flags);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.DATAGRAM_SEND_STATE_CHANGED:
                events.DatagramSendStateChanged(this, evt->DATAGRAM_SEND_STATE_CHANGED.ClientContext, evt->DATAGRAM_SEND_STATE_CHANGED.State);
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.RESUMED:
                events.Resumed(this, new ReadOnlySpan<byte>(evt->RESUMED.ResumptionState, evt->RESUMED.ResumptionStateLength));
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.RESUMPTION_TICKET_RECEIVED:
                events.ResumptionTicketReceived(this, new ReadOnlySpan<byte>(evt->RESUMPTION_TICKET_RECEIVED.ResumptionTicket, (int)evt->RESUMPTION_TICKET_RECEIVED.ResumptionTicketLength));
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
            case QUIC_CONNECTION_EVENT_TYPE.PEER_CERTIFICATE_RECEIVED:
                return HandlePeerCertificate(ref evt->PEER_CERTIFICATE_RECEIVED);
            default:
                // IDEAL_PROCESSOR_CHANGED, RELIABLE_RESET_NEGOTIATED, ONE_WAY_DELAY_NEGOTIATED, NETWORK_STATISTICS: not surfaced.
                return MsQuicStatus.QUIC_STATUS_SUCCESS;
        }
    }

    private int HandlePeerStreamStarted(QUIC_HANDLE* streamHandle, QUIC_STREAM_OPEN_FLAGS flags)
    {
        var stream = new MsQuicStream(this, streamHandle, flags, peerStarted: true);
        bool accepted;
        try
        {
            accepted = _events.PeerStreamStarted(this, stream, flags);
        }
        catch (Exception ex)
        {
            Poison(ex);
            accepted = false;
        }
        if (!accepted)
        {
            // We never took ownership: close the native stream now (allowed inline in this event) and drop the wrapper.
            stream.CloseRejected();
            return MsQuicStatus.QUIC_STATUS_SUCCESS;
        }
        stream.AttachCallback();
        return MsQuicStatus.QUIC_STATUS_SUCCESS;
    }

    private int HandlePeerCertificate(ref QUIC_CONNECTION_EVENT._Anonymous_e__Union._PEER_CERTIFICATE_RECEIVED_e__Struct e)
    {
        ReadOnlySpan<byte> certificate = default;
        ReadOnlySpan<byte> chain = default;
        void* platformCertificate = null;
        void* platformChain = null;
        if (_portableCertificate)
        {
            if (e.Certificate != null)
            {
                var der = (QUIC_BUFFER*)e.Certificate;
                certificate = new ReadOnlySpan<byte>(der->Buffer, (int)der->Length);
            }
            if (e.Chain != null)
            {
                var pkcs7 = (QUIC_BUFFER*)e.Chain;
                chain = new ReadOnlySpan<byte>(pkcs7->Buffer, (int)pkcs7->Length);
            }
        }
        else
        {
            platformCertificate = e.Certificate;
            platformChain = e.Chain;
        }
        var info = new MsQuicPeerCertificateInfo(certificate, chain, platformCertificate, platformChain, e.DeferredErrorFlags, e.DeferredStatus, _portableCertificate, _defersCertificateValidation);
        MsQuicCertificateDecision decision = _events.PeerCertificateReceived(this, in info);
        if (decision == MsQuicCertificateDecision.Accept) return MsQuicStatus.QUIC_STATUS_SUCCESS;
        if (decision == MsQuicCertificateDecision.Defer && _defersCertificateValidation) return MsQuicStatus.QUIC_STATUS_PENDING;
        return MsQuicStatus.QUIC_STATUS_BAD_CERTIFICATE;
    }
}
