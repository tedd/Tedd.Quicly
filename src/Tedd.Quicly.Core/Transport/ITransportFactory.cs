using System.Net;

namespace Tedd.Quicly.Core.Transport;

/// <summary>Creates client-side transports.</summary>
public interface ITransportConnector
{
    /// <summary>
    /// Starts connecting to <paramref name="endpoint"/>. The returned transport raises <see cref="ITransportSink.OnConnected"/>
    /// on <paramref name="sink"/> when the handshake completes, or <see cref="ITransportSink.OnClosed"/> on failure.
    /// </summary>
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

/// <summary>Runs before any TLS work for a new connection.</summary>
public delegate PreHandshakeDecision PreHandshakeCallback(in NewConnectionInfo info);

/// <summary>Attaches a sink to an accepted transport, or returns <c>null</c> to reject it.</summary>
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
