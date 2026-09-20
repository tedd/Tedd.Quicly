using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;

/// <summary>
/// <see cref="ITransportTestHarness"/> that runs the WebTransport-over-HTTP/3 carrier over the simulated network, so
/// the shared transport conformance suite can be pointed at the carrier itself: every pair is a
/// <see cref="WebTransportConnector"/> and a <see cref="WebTransportListener"/> over
/// <see cref="SimulatedConnector"/>/<see cref="SimulatedListener"/>, and the transports the scenarios drive are the
/// carriers, not the raw QUIC transports underneath them.
/// </summary>
public sealed class WebTransportSimulatedHarness : ITransportTestHarness
{
    private const long StepMicros = 250;
    private readonly LinkOptions _link;
    private readonly WebTransportOptions _options;
    private readonly List<ITransport> _transports = [];
    private readonly List<SimulatedListener> _listeners = [];
    private readonly List<WebTransportListener> _carrierListeners = [];

    /// <summary>Creates a harness whose links use <paramref name="link"/> (default: 2 ms one-way delay, no loss).</summary>
    /// <param name="link">Link conditions of every pair; copied per connection.</param>
    /// <param name="seed">Seed of the network's random generator.</param>
    /// <param name="options">Carrier options; the defaults when null.</param>
    public WebTransportSimulatedHarness(LinkOptions? link = null, int seed = 1, WebTransportOptions? options = null)
    {
        _link = link ?? new LinkOptions { DelayMicros = 2_000 };
        _options = options ?? new WebTransportOptions();
        Network = new SimulatedNetwork(Clock, seed);
    }

    /// <summary>The virtual clock the network advances.</summary>
    public VirtualClock Clock { get; } = new();

    /// <summary>The simulated network.</summary>
    public SimulatedNetwork Network { get; }

    /// <inheritdoc/>
    public string Name => "WebTransportOverSimulatedTransport";

    /// <inheritdoc/>
    public TimeSpan DefaultTimeout => TimeSpan.FromSeconds(30);

    /// <inheritdoc/>
    public ConformancePair CreatePair(ITransportSink clientSink, ITransportSink serverSink, ConformancePairOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(clientSink);
        ArgumentNullException.ThrowIfNull(serverSink);
        options ??= new ConformancePairOptions();
        ITransport? server = null;
        ITransport client = Connect(clientSink, static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            server = transport;
            return serverSink;
        }, options);
        client.UpdatePeerStreamLimits(options.ClientPeerBidiStreams, options.ClientPeerUnidiStreams);
        if (!Pump(() => server is not null, DefaultTimeout)) throw new ConformanceException("The WebTransport listener never accepted the connection.");
        server!.UpdatePeerStreamLimits(options.ServerPeerBidiStreams, options.ServerPeerUnidiStreams);
        return new ConformancePair(client, server);
    }

    /// <inheritdoc/>
    public ITransport Connect(ITransportSink clientSink, PreHandshakeCallback preHandshake, AcceptCallback accept, ConformancePairOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(clientSink);
        ArgumentNullException.ThrowIfNull(preHandshake);
        ArgumentNullException.ThrowIfNull(accept);
        options ??= new ConformancePairOptions();
        LinkOptions link = _link.Clone();
        if (options.TransportCloseAfter is TimeSpan after) link.DisconnectAtMicros = (long)(after.TotalMilliseconds * 1000);
        if (options.FailHandshake) link.FailHandshake = true;

        var simulated = new SimulatedListener(Network, new IPEndPoint(IPAddress.Loopback, 0));
        _listeners.Add(simulated);
        var listener = new WebTransportListener(simulated, _options);
        _carrierListeners.Add(listener);
        listener.Start(preHandshake, (ITransport transport, in NewConnectionInfo info) =>
        {
            _transports.Add(transport);
            return accept(transport, in info);
        });

        var connector = new WebTransportConnector(new SimulatedConnector(Network, link), _options);
        ITransport client = connector.Connect(simulated.LocalEndPoint, "localhost", clientSink);
        _transports.Add(client);
        return client;
    }

    /// <inheritdoc/>
    public bool Pump(Func<bool> condition, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(condition);
        long deadline = Network.NowMicros + (long)(timeout.TotalMilliseconds * 1000);
        while (!condition())
        {
            long now = Network.NowMicros;
            if (now >= deadline) return false;
            Network.Advance(Math.Min(StepMicros, deadline - now));
        }

        return true;
    }

    /// <summary>Closes every carrier, lets the closes run, stops the listeners and disposes the network.</summary>
    public void Dispose()
    {
        foreach (ITransport transport in _transports.ToArray()) transport.Dispose();
        if (!Network.IsDisposed) Network.RunUntilIdle(10_000_000);
        foreach (WebTransportListener listener in _carrierListeners) listener.Dispose();
        foreach (SimulatedListener listener in _listeners) listener.Dispose();
        Network.Dispose();
    }
}
