using System.Net;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Testing.Simulation;

/// <summary>
/// Server-side <see cref="ITransportListener"/> over a <see cref="SimulatedNetwork"/>. Connections made with a
/// <see cref="SimulatedConnector"/> to <see cref="LocalEndPoint"/> run the pre-handshake callback, then the accept
/// callback with the new <see cref="SimulatedTransport"/>; both run inside <see cref="SimulatedNetwork.Advance"/>.
/// </summary>
public sealed class SimulatedListener : ITransportListener
{
    private readonly SimulatedNetwork _network;
    private bool _started;
    private bool _disposed;

    /// <summary>Creates a listener at <paramref name="localEndPoint"/>, or at a synthetic address when <c>null</c>.</summary>
    /// <param name="network">The network to listen on.</param>
    /// <param name="localEndPoint">The address to listen at; a port of 0 is replaced by a free synthetic port.</param>
    public SimulatedListener(SimulatedNetwork network, IPEndPoint? localEndPoint = null)
    {
        ArgumentNullException.ThrowIfNull(network);
        _network = network;
        lock (network.Gate)
        {
            if (localEndPoint is null)
                LocalEndPoint = network.AllocateEndPoint();
            else if (localEndPoint.Port == 0)
                LocalEndPoint = new IPEndPoint(localEndPoint.Address, network.AllocateEndPoint().Port);
            else
                LocalEndPoint = localEndPoint;
        }
    }

    /// <inheritdoc/>
    public IPEndPoint LocalEndPoint { get; }

    internal PreHandshakeCallback? PreHandshake { get; private set; }

    internal AcceptCallback? Accept { get; private set; }

    /// <inheritdoc/>
    /// <exception cref="InvalidOperationException">Already started, or another listener is started at the same endpoint.</exception>
    public void Start(PreHandshakeCallback preHandshake, AcceptCallback accept)
    {
        ArgumentNullException.ThrowIfNull(preHandshake);
        ArgumentNullException.ThrowIfNull(accept);
        lock (_network.Gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_started)
                throw new InvalidOperationException("The listener is already started.");
            PreHandshake = preHandshake;
            Accept = accept;
            _network.RegisterListener(this);
            _started = true;
        }
    }

    /// <inheritdoc/>
    public void Stop()
    {
        lock (_network.Gate)
        {
            if (!_started)
                return;
            _network.UnregisterListener(this);
            _started = false;
        }
    }

    /// <summary>Stops the listener. Existing connections are unaffected.</summary>
    public void Dispose()
    {
        Stop();
        _disposed = true;
    }
}
