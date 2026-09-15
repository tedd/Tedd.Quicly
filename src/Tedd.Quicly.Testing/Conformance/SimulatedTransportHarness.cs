using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Conformance;

/// <summary>
/// <see cref="ITransportTestHarness"/> over a <see cref="SimulatedNetwork"/>: pairs are made through a
/// <see cref="SimulatedConnector"/> and a <see cref="SimulatedListener"/> (so the connector/listener path is exercised too),
/// and <see cref="Pump"/> advances virtual time in small steps.
/// </summary>
public sealed class SimulatedTransportHarness : ITransportTestHarness
{
    private const long StepMicros = 250;
    private readonly LinkOptions _link;
    private readonly List<ITransport> _transports = [];

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
        using var listener = new SimulatedListener(Network, new IPEndPoint(IPAddress.Loopback, 0));
        ITransport? server = null;
        listener.Start(
            static (in NewConnectionInfo _) => PreHandshakeDecision.Accept,
            (ITransport transport, in NewConnectionInfo _) =>
            {
                server = transport;
                transport.UpdatePeerStreamLimits(options.ServerPeerBidiStreams, options.ServerPeerUnidiStreams);
                return serverSink;
            });
        var connector = new SimulatedConnector(Network, _link);
        ITransport client = connector.Connect(listener.LocalEndPoint, "localhost", clientSink);
        _transports.Add(client);
        client.UpdatePeerStreamLimits(options.ClientPeerBidiStreams, options.ClientPeerUnidiStreams);
        if (!Pump(() => server is not null, DefaultTimeout)) throw new ConformanceException("The simulated listener never accepted the connection.");
        _transports.Add(server!);
        return new ConformancePair(client, server!);
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

    /// <summary>Closes every transport, lets the closes run and disposes the network.</summary>
    public void Dispose()
    {
        foreach (ITransport transport in _transports) transport.Dispose();
        if (!Network.IsDisposed) Network.RunUntilIdle(10_000_000);
        Network.Dispose();
    }
}
