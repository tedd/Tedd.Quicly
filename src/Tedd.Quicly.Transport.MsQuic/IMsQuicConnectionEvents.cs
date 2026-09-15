using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>The handler's verdict on a peer certificate.</summary>
public enum MsQuicCertificateDecision
{
    /// <summary>Reject: the handshake fails with a <c>bad_certificate</c> alert.</summary>
    Reject = 0,
    /// <summary>Accept the certificate.</summary>
    Accept = 1,
    /// <summary>
    /// Decide later with <see cref="MsQuicConnection.CompleteCertificateValidation"/> (the callback returns
    /// <c>QUIC_STATUS_PENDING</c>). Only valid when the configuration was loaded with
    /// <c>DEFER_CERTIFICATE_VALIDATION</c> (<see cref="MsQuicCertificateValidation.Callback"/>); otherwise it is
    /// treated as <see cref="Reject"/>.
    /// </summary>
    Defer = 2,
}

/// <summary>
/// The peer certificate as delivered by <c>PEER_CERTIFICATE_RECEIVED</c>. Spans and pointers are valid only during
/// the callback; copy what must outlive it.
/// </summary>
public readonly unsafe ref struct MsQuicPeerCertificateInfo
{
    /// <summary>The leaf certificate in DER form (empty unless <see cref="IsPortable"/>).</summary>
    public readonly ReadOnlySpan<byte> CertificateDer;

    /// <summary>The chain as a DER-encoded PKCS#7 blob (empty unless <see cref="IsPortable"/> or when the provider sends none).</summary>
    public readonly ReadOnlySpan<byte> ChainPkcs7;

    /// <summary>The platform certificate (<c>PCCERT_CONTEXT</c> on Windows) when not portable; null otherwise.</summary>
    public readonly void* PlatformCertificate;

    /// <summary>The platform chain when not portable; null otherwise.</summary>
    public readonly void* PlatformChain;

    /// <summary>Platform validation error flags (only meaningful with deferred validation).</summary>
    public readonly uint DeferredErrorFlags;

    /// <summary>Platform validation status (only meaningful with deferred validation; <c>QUIC_STATUS_SUCCESS</c> when it passed).</summary>
    public readonly int DeferredStatus;

    /// <summary>True when the certificate arrived as DER bytes (<c>USE_PORTABLE_CERTIFICATES</c>).</summary>
    public readonly bool IsPortable;

    /// <summary>True when the handler may return <see cref="MsQuicCertificateDecision.Defer"/>.</summary>
    public readonly bool CanDefer;

    internal MsQuicPeerCertificateInfo(ReadOnlySpan<byte> certificateDer, ReadOnlySpan<byte> chainPkcs7, void* platformCertificate, void* platformChain, uint deferredErrorFlags, int deferredStatus, bool isPortable, bool canDefer)
    {
        CertificateDer = certificateDer;
        ChainPkcs7 = chainPkcs7;
        PlatformCertificate = platformCertificate;
        PlatformChain = platformChain;
        DeferredErrorFlags = deferredErrorFlags;
        DeferredStatus = deferredStatus;
        IsPortable = isPortable;
        CanDefer = canDefer;
    }
}

/// <summary>
/// Connection event sink. Every method has a no-op default so implementations override only what they need.
/// </summary>
/// <remarks>
/// <para><b>Threading.</b> MsQuic invokes these on its worker threads. All callbacks of one connection <b>and of
/// its streams</b> are serialised (never concurrent with each other), but different connections run in parallel
/// and the worker identity may change between callbacks. Pointers and spans passed in are valid only for the
/// duration of the call. Do not block in callbacks.</para>
/// <para><b>Ordering.</b> <see cref="DatagramStateChanged"/> may arrive before <see cref="Connected"/>.
/// Completions (<see cref="DatagramSendStateChanged"/>, stream <c>SendComplete</c>) may fire on a worker thread
/// before the API call that caused them returns on the calling thread: publish everything a completion needs
/// before calling into MsQuic (ADR 0008 §3). API calls made from a callback run inline and may deliver further
/// events re-entrantly (ADR 0008 §7).</para>
/// <para><b>Exceptions.</b> An exception escaping a handler is recorded in <see cref="MsQuicConnection.LastCallbackException"/>,
/// the connection is shut down with <see cref="MsQuicConnection.CallbackFailureErrorCode"/> and the callback
/// returns <c>QUIC_STATUS_INTERNAL_ERROR</c>; it never propagates into MsQuic.</para>
/// </remarks>
public unsafe interface IMsQuicConnectionEvents
{
    /// <summary>Handshake complete. <paramref name="negotiatedAlpn"/> is valid only during the call.</summary>
    void Connected(MsQuicConnection connection, ReadOnlySpan<byte> negotiatedAlpn, bool sessionResumed)
    {
    }

    /// <summary>The transport (local MsQuic or a protocol error) started closing the connection.</summary>
    void ShutdownInitiatedByTransport(MsQuicConnection connection, int status, ulong errorCode)
    {
    }

    /// <summary>The peer application closed the connection with an application error code.</summary>
    void ShutdownInitiatedByPeer(MsQuicConnection connection, ulong errorCode)
    {
    }

    /// <summary>Last event. The handle may be closed afterwards, from a thread that is not an MsQuic callback thread.</summary>
    void ShutdownComplete(MsQuicConnection connection, bool handshakeCompleted, bool peerAcknowledgedShutdown, bool appCloseInProgress)
    {
    }

    /// <summary>
    /// The peer opened a stream. Set <see cref="MsQuicStream.Events"/> and return true to accept; return false to
    /// reject (the wrapper closes the stream and no events follow). Default: reject.
    /// </summary>
    bool PeerStreamStarted(MsQuicConnection connection, MsQuicStream stream, QUIC_STREAM_OPEN_FLAGS flags) => false;

    /// <summary>The peer raised the number of streams we may open.</summary>
    void StreamsAvailable(MsQuicConnection connection, ushort bidirectionalCount, ushort unidirectionalCount)
    {
    }

    /// <summary>The peer wants more streams than we currently allow (raise with <see cref="MsQuicConnection.SetLocalUnidiStreamCount"/> / <see cref="MsQuicConnection.SetLocalBidiStreamCount"/>).</summary>
    void PeerNeedsStreams(MsQuicConnection connection, bool bidirectional)
    {
    }

    /// <summary>Datagram send capability changed (<paramref name="maxSendLength"/> is the largest payload accepted). May arrive before <see cref="Connected"/>.</summary>
    void DatagramStateChanged(MsQuicConnection connection, bool sendEnabled, ushort maxSendLength)
    {
    }

    /// <summary>A datagram arrived. <paramref name="data"/> is valid only during the call.</summary>
    void DatagramReceived(MsQuicConnection connection, ReadOnlySpan<byte> data, QUIC_RECEIVE_FLAGS flags)
    {
    }

    /// <summary>
    /// Lifecycle of a sent datagram, keyed by the client context passed to <see cref="MsQuicConnection.SendDatagram"/>.
    /// The full <see cref="QUIC_DATAGRAM_SEND_STATE"/> is reported; states at or above <c>LOST_DISCARDED</c> are
    /// final (<see cref="QuicDatagramSendState.IsFinal"/>), the buffers may be released at <c>SENT</c> or <c>CANCELED</c>.
    /// </summary>
    void DatagramSendStateChanged(MsQuicConnection connection, void* clientContext, QUIC_DATAGRAM_SEND_STATE state)
    {
    }

    /// <summary>The peer address changed (migration, NAT rebinding). Convert with <see cref="QUIC_ADDR.ToIPEndPoint"/> when an end point is needed.</summary>
    void PeerAddressChanged(MsQuicConnection connection, in QUIC_ADDR address)
    {
    }

    /// <summary>The local address changed.</summary>
    void LocalAddressChanged(MsQuicConnection connection, in QUIC_ADDR address)
    {
    }

    /// <summary>Client only: a resumption ticket for a future resumed connection. Valid only during the call.</summary>
    void ResumptionTicketReceived(MsQuicConnection connection, ReadOnlySpan<byte> ticket)
    {
    }

    /// <summary>Server only: the connection was resumed; <paramref name="resumptionState"/> is the app data from the ticket.</summary>
    void Resumed(MsQuicConnection connection, ReadOnlySpan<byte> resumptionState)
    {
    }

    /// <summary>
    /// The peer certificate arrived (only with <see cref="MsQuicCertificateValidation.Callback"/> or a raw
    /// credential carrying <c>INDICATE_CERTIFICATE_RECEIVED</c>). Default: reject.
    /// </summary>
    MsQuicCertificateDecision PeerCertificateReceived(MsQuicConnection connection, in MsQuicPeerCertificateInfo info) => MsQuicCertificateDecision.Reject;
}

/// <summary>Sink that ignores every event; used when no handler is attached.</summary>
internal sealed class NoOpConnectionEvents : IMsQuicConnectionEvents
{
    public static readonly NoOpConnectionEvents Instance = new();
}
