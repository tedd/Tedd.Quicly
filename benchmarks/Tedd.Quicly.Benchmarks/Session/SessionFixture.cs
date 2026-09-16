using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Session;

/// <summary>
/// Connected client/server peer pairs over zero-delay simulated links (benchmark fixture): pings, heartbeat and the fast
/// lock are off, so a measured cycle is only the traffic under test.
/// </summary>
internal sealed class SessionFixture : IDisposable
{
    /// <summary>2 unordered · 3 sequenced keyed · 4 ordered.</summary>
    public static ChannelTable Table { get; } = ChannelTable.Create()
        .Add(2, "unordered", ChannelMode.UnreliableUnordered)
        .Add(3, "sequenced", ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.ExpiryMicros = 0;
        })
        .Add(4, "ordered", ChannelMode.ReliableOrdered)
        .Build();

    public SessionFixture(int pairs, bool compact = false)
    {
        Network = new SimulatedNetwork(Clock, 1);
        Listener = new SimulatedListener(Network);
        Clients = new QuiclyPeer[pairs];
        Servers = new QuiclyPeer[pairs];
        PeerOptions serverOptions = Options(Clock, compact);
        PeerOptions clientOptions = Options(Clock, compact);
        QuiclyPeer? accepted = null;
        Listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo info) =>
        {
            accepted = QuiclyPeer.CreateServerPeer(transport, in info, Table, serverOptions, AcceptAll.Instance);
            return accepted.TransportSink;
        });
        for (int i = 0; i < pairs; i++)
        {
            accepted = null;
            QuiclyPeer client = QuiclyPeer.Connect(new SimulatedConnector(Network), Listener.LocalEndPoint, "bench", Table, clientOptions);
            for (int step = 0; step < 10_000 && !(client.State == PeerState.Connected && accepted?.State == PeerState.Connected); step++)
            {
                client.Poll();
                client.Flush();
                accepted?.Poll();
                accepted?.Flush();
                Network.Advance(100);
            }

            if (client.State != PeerState.Connected || accepted is null || accepted.State != PeerState.Connected)
            {
                throw new InvalidOperationException("The benchmark session did not connect.");
            }

            Clients[i] = client;
            Servers[i] = accepted;
        }
    }

    public VirtualClock Clock { get; } = new();

    public SimulatedNetwork Network { get; }

    public SimulatedListener Listener { get; }

    public QuiclyPeer[] Clients { get; }

    public QuiclyPeer[] Servers { get; }

    /// <summary>Quiet options with room for the benchmark's traffic (a smaller pool when many pairs share the process).</summary>
    public static PeerOptions Options(IClock clock, bool compact) => new()
    {
        Clock = clock,
        PingInterval = TimeSpan.FromHours(1),
        FastPingInterval = TimeSpan.FromHours(1),
        FastLockDuration = TimeSpan.Zero,
        HeartbeatTimeout = TimeSpan.Zero,
        SendBudgetBytes = compact ? 256 * 1024 : 4 * 1024 * 1024,
        ReceiveBudgetBytes = compact ? 256 * 1024 : 4 * 1024 * 1024,
        SendTableCapacity = compact ? 1024 : 4096,
        ReceiveRingCapacity = 4096,
        AllocatorOptions = compact
            ? new SlabAllocatorOptions
            {
                FreeListShards = 1,
                SizeClasses = [new(64, 4096), new(256, 256), new(1536, 64), new(4096, 16), new(65536, 2)],
            }
            : new SlabAllocatorOptions
            {
                FreeListShards = 2,
                SizeClasses = [new(64, 8192), new(256, 2048), new(1536, 512), new(4096, 512), new(16384, 64), new(65536, 8), new(262144, 1)],
            },
    };

    /// <summary>Hands everything to the transports, delivers it, dispatches it and drains the completions.</summary>
    public void Cycle()
    {
        foreach (QuiclyPeer client in Clients)
        {
            client.Flush();
        }

        Network.Advance(0);
        foreach (QuiclyPeer server in Servers)
        {
            server.Poll();
        }

        Network.Advance(0);
        foreach (QuiclyPeer client in Clients)
        {
            client.Poll();
        }
    }

    public void Dispose()
    {
        foreach (QuiclyPeer client in Clients)
        {
            client?.Dispose();
        }

        foreach (QuiclyPeer server in Servers)
        {
            server?.Dispose();
        }

        Network.RunUntilIdle(60_000_000);
        Network.Dispose();
        Listener.Dispose();
    }

    private sealed class AcceptAll : IPeerAdmission
    {
        public static readonly AcceptAll Instance = new();

        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }
}
