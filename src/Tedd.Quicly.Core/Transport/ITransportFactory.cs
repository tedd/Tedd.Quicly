using System.Net;

namespace Tedd.Quicly.Core.Transport;

/// <summary>Creates client-side transports.</summary>
public interface ITransportConnector
{
    /// <summary>
    /// Starts connecting to <paramref name="endpoint"/>. The returned transport raises <see cref="ITransportSink.OnConnected"/>
    /// on <paramref name="sink"/> when the handshake completes, or <see cref="ITransportSink.OnClosed"/> on failure.
    /// </summary>
    /// <remarks>
    /// May throw when the attempt cannot even be started; then no transport is returned and no callback follows (the MsQuic
    /// connector throws <c>MsQuicException</c> when MsQuic refuses to start the connection). Every failure after the start
    /// (unreachable peer, refusal, handshake failure, timeout) is reported through <see cref="ITransportSink.OnClosed"/>
    /// with <see cref="TransportCloseReason.Transport"/>; the simulator reports all failures that way.
    /// </remarks>
    ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink);
}

/// <summary>Decision returned by a pre-handshake admission check.</summary>
public enum PreHandshakeDecision : byte
{
    /// <summary>Continue the handshake.</summary>
    Accept = 0,
    /// <summary>Drop the connection before any TLS work.</summary>
    Reject,
}

/// <summary>Runs before any TLS work for a new connection, on a transport thread (possibly concurrently for different connections); must not block.</summary>
public delegate PreHandshakeDecision PreHandshakeCallback(in NewConnectionInfo info);

/// <summary>Attaches a sink to an accepted transport, or returns <c>null</c> to reject it.</summary>
/// <remarks>
/// Runs on a transport thread while the listener admits the connection (MsQuic: inside its NEW_CONNECTION callback),
/// possibly concurrently for different connections, and must not block. It must not call any member of
/// <paramref name="transport"/>: keep the transport and use it after the callback returns (its callbacks start then; on
/// MsQuic a call made inside the callback can lose events). Returning <c>null</c>, or throwing, refuses the connection:
/// the transport is then <see cref="TransportState.Closed"/>, raises no callback and may be disposed.
/// </remarks>
public delegate ITransportSink? AcceptCallback(ITransport transport, in NewConnectionInfo info);

/// <summary>Accepts server-side transports.</summary>
public interface ITransportListener : IDisposable
{
    /// <summary>The bound local endpoint.</summary>
    IPEndPoint LocalEndPoint { get; }

    /// <summary>
    /// Starts listening. <paramref name="preHandshake"/> runs for every new connection before TLS work;
    /// <paramref name="accept"/> is called with the new transport and must return the sink that will receive its callbacks
    /// (or <c>null</c> to reject).
    /// </summary>
    void Start(PreHandshakeCallback preHandshake, AcceptCallback accept);

    /// <summary>Stops accepting new connections. Existing connections are unaffected.</summary>
    void Stop();
}
