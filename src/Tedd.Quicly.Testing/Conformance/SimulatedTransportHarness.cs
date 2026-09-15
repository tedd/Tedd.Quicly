using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Conformance;

/// <summary>
/// <see cref="ITransportTestHarness"/> over a <see cref="SimulatedNetwork"/>: pairs are made through a
/// <see cref="SimulatedConnector"/> and a <see cref="SimulatedListener"/> (so the connector/listener path is exercised too),
/// and <see cref="Pump"/> advances virtual time in small steps. <see cref="ConformancePairOptions.TransportCloseAfter"/> cuts
/// the link (<see cref="LinkOptions.DisconnectAtMicros"/>); <see cref="ConformancePairOptions.FailHandshake"/> sets
/// <see cref="LinkOptions.FailHandshake"/>.
/// </summary>
public sealed class SimulatedTransportHarness : ITransportTestHarness
{
    private const long StepMicros = 250;
    private readonly LinkOptions _link;
    private readonly List<ITransport> _transports = [];
    private readonly List<SimulatedListener> _listeners = [];

    /// <summary>Creates a harness whose links use <paramref name="link"/> (default: 2 ms one-way delay, no loss).</summary>
    /// <param name="link">Link conditions of every pair; copied per connection.</param>
    /// <param name="seed">Seed of the network's random generator.</param>
    public SimulatedTransportHarness(LinkOptions? link = null, int seed = 1)
    {
        _link = link ?? new LinkOptions { DelayMicros = 2_000 };
        Network = new SimulatedNetwork(Clock, seed);
    }

    /// <summary>The virtual clock the network advances.</summary>
    public VirtualClock Clock { get; } = new();

    /// <summary>The simulated network.</summary>
    public SimulatedNetwork Network { get; }

    /// <inheritdoc/>
    public string Name => "SimulatedTransport";

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
        if (!Pump(() => server is not null, DefaultTimeout)) throw new ConformanceException("The simulated listener never accepted the connection.");
        // After the accept callback, which must not call the new transport: the client is still connecting and reads these
        // limits when it connects (a client that connected already gets them through OnStreamsAvailable).
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
        var listener = new SimulatedListener(Network, new IPEndPoint(IPAddress.Loopback, 0));
        _listeners.Add(listener);
        listener.Start(preHandshake, (ITransport transport, in NewConnectionInfo info) =>
        {
            _transports.Add(transport);
            return accept(transport, in info);
        });
        ITransport client = new SimulatedConnector(Network, link).Connect(listener.LocalEndPoint, "localhost", clientSink);
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

    /// <summary>Closes every transport, lets the closes run, stops the listeners and disposes the network.</summary>
    public void Dispose()
    {
        foreach (ITransport transport in _transports.ToArray()) transport.Dispose();
        if (!Network.IsDisposed) Network.RunUntilIdle(10_000_000);
        foreach (SimulatedListener listener in _listeners) listener.Dispose();
        Network.Dispose();
    }
}
