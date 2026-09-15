using System.Net;
using System.Text;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Testing.Simulation;

/// <summary>
/// Client-side <see cref="ITransportConnector"/> over a <see cref="SimulatedNetwork"/>. Each connection gets its own
/// link with this connector's <see cref="LinkOptions"/>, and reaches the <see cref="SimulatedListener"/> started at the
/// endpoint one one-way delay later, where the listener's pre-handshake and accept callbacks run (inside <c>Advance</c>).
/// </summary>
/// <remarks>
/// Accepted: the client raises <see cref="ITransportSink.OnDatagramCapabilityChanged"/> and <see cref="ITransportSink.OnConnected"/>
/// one round trip after <see cref="Connect"/>, the server capability change on accept and <c>OnConnected</c> one round
/// trip after accept. Rejected (no listener, pre-handshake reject, accept returned <c>null</c>): the client raises
/// <see cref="ITransportSink.OnClosed"/> with <see cref="TransportCloseReason.Transport"/> and
/// <see cref="SimulatedTransport.StatusUnreachable"/> or <see cref="SimulatedTransport.StatusConnectionRefused"/> one round trip after <c>Connect</c>.
/// Accepted but failing its handshake (<see cref="LinkOptions.FailHandshake"/>): neither end connects; the client closes one
/// round trip after <c>Connect</c> and the server half a round trip later, both with
/// <see cref="SimulatedTransport.StatusHandshakeFailed"/>. <see cref="Connect"/> never throws for a network failure: every
/// failure is reported through <see cref="ITransportSink.OnClosed"/>.
/// </remarks>
public sealed class SimulatedConnector : ITransportConnector
{
    /// <summary>ALPN reported by connections that do not choose another (<c>quicly/1</c>).</summary>
    public const string DefaultAlpn = "quicly/1";

    private readonly SimulatedNetwork _network;
    private readonly LinkOptions _options;
    private string _alpn = DefaultAlpn;

    /// <summary>Creates a connector whose connections use <paramref name="options"/> (copied now).</summary>
    /// <param name="network">The network to connect through.</param>
    /// <param name="options">Link conditions of every connection; <c>null</c> for an ideal link.</param>
    public SimulatedConnector(SimulatedNetwork network, LinkOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(network);
        _network = network;
        _options = (options ?? new LinkOptions()).Clone();
        _options.Validate();
    }

    /// <summary>ALPN offered to the listener and reported in <see cref="TransportConnectedInfo"/> (ASCII, 1 to 255 characters).</summary>
    public string Alpn
    {
        get => _alpn;
        set
        {
            ArgumentException.ThrowIfNullOrEmpty(value);
            if (value.Length > 255 || !Ascii.IsValid(value))
                throw new ArgumentException("The ALPN must be 1 to 255 ASCII characters.", nameof(value));
            _alpn = value;
        }
    }

    /// <inheritdoc/>
    /// <returns>A <see cref="SimulatedTransport"/> in <see cref="TransportState.Connecting"/>.</returns>
    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(sink);
        lock (_network.Gate)
        {
            ObjectDisposedException.ThrowIf(_network.IsDisposed, _network);
            SimulatedLink link = new(_network, _options, _alpn);
            SimulatedTransport client = new(_network, link, isClient: true, sink, _network.AllocateEndPoint()) { RemoteEndPoint = endpoint };
            link.A = client;
            SimEvent e = new()
            {
                Kind = SimEventKind.ConnectAttempt,
                Due = _network.NowMicros + _options.DelayMicros,
                Obj = new Attempt(link, client, endpoint, serverName),
            };
            _network.Schedule(ref e);
            link.ScheduleTimeline();
            return client;
        }
    }

    /// <summary>A connection attempt travelling to the listener.</summary>
    internal sealed class Attempt(SimulatedLink link, SimulatedTransport client, EndPoint endpoint, string? serverName)
    {
        public void Run(SimulatedNetwork network)
        {
            if (!client.IsConnectingState)
                return;
            long now = network.NowMicros;
            long delay = link.Options.DelayMicros;
            SimulatedListener? listener = network.FindListener(endpoint);
            if (listener is null)
            {
                client.ScheduleConnectFailed(now + delay, SimulatedTransport.StatusUnreachable);
                return;
            }

            NewConnectionInfo info = default;
            info.RemoteEndPoint = client.LocalEndPoint;
            info.ServerName = serverName;
            Span<byte> alpn = info.Alpn;
            link.Alpn.CopyTo(alpn);
            info.AlpnLength = (byte)link.Alpn.Length;

            if (listener.PreHandshake!(in info) == PreHandshakeDecision.Reject)
            {
                client.ScheduleConnectFailed(now + delay, SimulatedTransport.StatusConnectionRefused);
                return;
            }

            SimulatedTransport server = new(network, link, isClient: false, sink: null, listener.LocalEndPoint);
            SimulatedNetwork.Attach(link, client, server);
            ITransportSink? sink = listener.Accept!(server, in info);
            if (sink is null)
            {
                server.MarkRejected();
                link.B = null;
                client.Peer = null;
                client.ScheduleConnectFailed(now + delay, SimulatedTransport.StatusConnectionRefused);
                return;
            }
            server.Sink = sink;
            if (link.Options.FailHandshake)
            {
                // The handshake fails after the accept (for example the client rejects the server's certificate): neither end
                // connects, the client learns it one round trip after Connect, the server when the client's close arrives.
                client.ScheduleConnectFailed(now + delay, SimulatedTransport.StatusHandshakeFailed);
                server.ScheduleConnectFailed(now + 2 * delay, SimulatedTransport.StatusHandshakeFailed);
                return;
            }
            server.ScheduleConnect(now, now + 2 * delay);
            client.ScheduleConnect(now + delay, now + delay);
        }
    }
}
