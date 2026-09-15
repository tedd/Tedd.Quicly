using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// Connection event sink. Every method has a no-op default so implementations override only what they need.
/// </summary>
/// <remarks>
/// Threading: MsQuic invokes these on its worker threads. All callbacks of one connection <b>and of its
/// streams</b> are serialised (never concurrent with each other), but different connections run in parallel.
/// Pointers and spans passed in are valid only for the duration of the call. Do not block in callbacks.
/// Calling into the same connection (Shutdown, SendDatagram, OpenStream, Close) from within a callback is
/// allowed.
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

    /// <summary>Last event; after it the handle may be closed (calling <see cref="MsQuicConnection.Close"/> inline is allowed).</summary>
    void ShutdownComplete(MsQuicConnection connection, bool handshakeCompleted, bool peerAcknowledgedShutdown, bool appCloseInProgress)
    {
    }

    /// <summary>
    /// The peer opened a stream. Set <see cref="MsQuicStream.Events"/> and return true to accept; return false to
    /// reject (the wrapper closes the stream). Default: reject.
    /// </summary>
    bool PeerStreamStarted(MsQuicConnection connection, MsQuicStream stream, QUIC_STREAM_OPEN_FLAGS flags) => false;

    /// <summary>The peer raised the number of streams we may open.</summary>
    void StreamsAvailable(MsQuicConnection connection, ushort bidirectionalCount, ushort unidirectionalCount)
    {
    }

    /// <summary>Datagram send capability changed (<paramref name="maxSendLength"/> is the largest payload accepted).</summary>
    void DatagramStateChanged(MsQuicConnection connection, bool sendEnabled, ushort maxSendLength)
    {
    }

    /// <summary>A datagram arrived. <paramref name="data"/> is valid only during the call.</summary>
    void DatagramReceived(MsQuicConnection connection, ReadOnlySpan<byte> data, QUIC_RECEIVE_FLAGS flags)
    {
    }

    /// <summary>Lifecycle of a datagram sent with a non-null client context. States at or above LOST_DISCARDED are final.</summary>
    void DatagramSendStateChanged(MsQuicConnection connection, void* clientContext, QUIC_DATAGRAM_SEND_STATE state)
    {
    }

    /// <summary>
    /// Only with <see cref="MsQuicCertificateValidation.Custom"/> (or a raw credential with
    /// <c>INDICATE_CERTIFICATE_RECEIVED</c>). Return true to accept the peer certificate. Default: reject.
    /// <paramref name="certificate"/> is null when it could not be decoded; the handler owns it and should dispose it.
    /// </summary>
    bool PeerCertificateReceived(MsQuicConnection connection, X509Certificate2? certificate, uint deferredErrorFlags, int deferredStatus) => false;

    /// <summary>Client only: a resumption ticket for a future 0-RTT / resumed connection.</summary>
    void ResumptionTicketReceived(MsQuicConnection connection, ReadOnlySpan<byte> ticket)
    {
    }

    /// <summary>Server only: the connection was resumed; <paramref name="resumptionState"/> is the app data from the ticket.</summary>
    void Resumed(MsQuicConnection connection, ReadOnlySpan<byte> resumptionState)
    {
    }

    /// <summary>The peer wants more streams than we currently allow.</summary>
    void PeerNeedsStreams(MsQuicConnection connection, bool bidirectional)
    {
    }

    /// <summary>The local address changed (e.g. NAT rebinding).</summary>
    void LocalAddressChanged(MsQuicConnection connection, in QuicAddr address)
    {
    }

    /// <summary>The peer address changed (migration).</summary>
    void PeerAddressChanged(MsQuicConnection connection, in QuicAddr address)
    {
    }
}

/// <summary>Sink that ignores every event; used when no handler is attached.</summary>
internal sealed class NoOpConnectionEvents : IMsQuicConnectionEvents
{
    public static readonly NoOpConnectionEvents Instance = new();
}
