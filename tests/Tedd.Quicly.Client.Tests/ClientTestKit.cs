using System.Net;
using System.Text;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Server;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Client.Tests;

/// <summary>Channel tables of the client tests.</summary>
internal static class Tables
{
    public static ChannelTable Default { get; } = ChannelTable.Create()
        .Add(2, "state", ChannelMode.UnreliableUnordered)
        .Add(4, "chat", ChannelMode.ReliableOrdered)
        .Build();

    public static ChannelTable Other { get; } = ChannelTable.Create()
        .Add(3, "other", ChannelMode.UnreliableSequenced)
        .Build();
}

/// <summary>Small buffer pools (the defaults reserve 16 MiB each).</summary>
internal static class Pools
{
    public static SlabAllocatorOptions Small() => new()
    {
        FreeListShards = 1,
        SizeClasses = [new(64, 1024), new(256, 1024), new(1536, 256), new(4096, 64), new(16384, 16), new(65536, 4)],
    };
}

/// <summary>
/// A real <see cref="QuiclyServer"/> over a simulated network, and clients whose connect waits advance that network and pump
/// the server, so every test runs deterministically on the test thread.
/// </summary>
internal sealed class ClientFixture : IAsyncDisposable
{
    private readonly SlabAllocator _clientPool = new(Pools.Small());
    private readonly List<QuiclyClient> _clients = [];

    public ClientFixture(Action<ServerOptions>? server = null)
    {
        Network = new SimulatedNetwork(Clock, 1);
        Listener = new SimulatedListener(Network);
        ServerOptions options = new() { Channels = Tables.Default, ExpectedPeers = 8, MaxPeers = 32, ShutdownTimeout = TimeSpan.Zero };
        options.PeerOptions.Clock = Clock;
        options.PeerOptions.AllocatorOptions = Pools.Small();
        server?.Invoke(options); // the resume rate keeps its production defaults (MinResumeInterval 1 s, ResumeBurst 3)
        Server = new QuiclyServer(options, Listener);
        Server.PeerAdmitted += peer => ServerAdmitted.Add((peer.SessionId, peer.Epoch));
        Server.StartAsync(TestContext.Current.CancellationToken).GetAwaiter().GetResult();
        Connector = new SimulatedConnector(Network);
    }

    public VirtualClock Clock { get; } = new();

    public SimulatedNetwork Network { get; }

    public SimulatedListener Listener { get; }

    public QuiclyServer Server { get; }

    public SimulatedConnector Connector { get; }

    public EndPoint EndPoint => Listener.LocalEndPoint;

    public List<(ulong SessionId, uint Epoch)> ServerAdmitted { get; } = [];

    /// <summary>A client whose connect waits step the simulation (1 ms) instead of sleeping.</summary>
    public QuiclyClient CreateClient(ITransportConnector? connector = null)
    {
        QuiclyClient client = new(connector ?? Connector)
        {
            WaitOverride = (_, _) =>
            {
                Step(1_000);
                return ValueTask.CompletedTask;
            },
        };
        _clients.Add(client);
        return client;
    }

    public ClientOptions Options(string? auth = null, ReconnectPolicy? reconnect = null, ChannelTable? table = null) => new()
    {
        Channels = table ?? Tables.Default,
        PeerOptions = new PeerOptions
        {
            Clock = Clock,
            Allocator = _clientPool,
            SendTableCapacity = 64,
            ReceiveRingCapacity = 64,
            SegmentArenaCapacity = 64,
        },
        AuthToken = auth is null ? default : Encoding.UTF8.GetBytes(auth),
        ServerName = "game.test",
        Reconnect = reconnect,
    };

    /// <summary>A connector whose links die <paramref name="afterMicros"/> after they are created.</summary>
    public SimulatedConnector Cutting(long afterMicros) => new(Network, new LinkOptions { DisconnectAtMicros = afterMicros });

    /// <summary>A connector that connects to an address nobody listens at.</summary>
    public ITransportConnector Unreachable() => new RedirectingConnector(Connector, new IPEndPoint(IPAddress.Parse("10.255.255.1"), 9));

    /// <summary>Advances the network and pumps the server.</summary>
    public void Step(long micros)
    {
        Network.Advance(micros);
        Server.PollAll();
        Server.FlushAll();
    }

    public void Run(QuiclyClient client, long micros, long step = 1_000)
    {
        long end = Network.NowMicros + micros;
        while (Network.NowMicros < end)
        {
            Step(Math.Min(step, end - Network.NowMicros));
            client.Poll();
            client.Flush();
        }
    }

    public bool RunUntil(QuiclyClient client, Func<bool> condition, long maxMicros = 10_000_000, long step = 1_000)
    {
        long end = Network.NowMicros + maxMicros;
        client.Poll();
        client.Flush();
        while (!condition())
        {
            if (Network.NowMicros >= end)
            {
                return false;
            }

            Step(step);
            client.Poll();
            client.Flush();
        }

        return true;
    }

    /// <summary>The live server peer of an admitted client peer (matched by session id).</summary>
    public QuiclyPeer ServerPeerOf(QuiclyPeer client)
    {
        foreach (PeerSlot slot in Server.Peers)
        {
            if (slot.Peer is { } peer && peer.SessionId == client.SessionId && peer.State == PeerState.Connected)
            {
                return peer;
            }
        }

        throw new InvalidOperationException("No live server peer for session " + client.SessionId + ".");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (QuiclyClient client in _clients)
        {
            client.Dispose();
        }

        await Server.DisposeAsync();
        Network.RunUntilIdle(60_000_000);
        Network.Dispose();
        Listener.Dispose();
        _clientPool.Dispose();
    }
}

/// <summary>Uses a queue of connectors for successive connections, then a fallback; can throw once.</summary>
internal sealed class ScriptedConnector(ITransportConnector fallback, IClock clock) : ITransportConnector
{
    private readonly Queue<ITransportConnector> _next = new();

    public int Connects { get; private set; }

    public List<long> ConnectTimes { get; } = [];

    public Exception? ThrowNext { get; set; }

    public ScriptedConnector Then(ITransportConnector connector)
    {
        _next.Enqueue(connector);
        return this;
    }

    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
    {
        Connects++;
        ConnectTimes.Add(clock.NowMicros);
        if (ThrowNext is { } exception)
        {
            ThrowNext = null;
            throw exception;
        }

        return (_next.Count > 0 ? _next.Dequeue() : fallback).Connect(endpoint, serverName, sink);
    }
}

/// <summary>Connects to <paramref name="target"/> whatever endpoint it is asked for.</summary>
internal sealed class RedirectingConnector(ITransportConnector inner, EndPoint target) : ITransportConnector
{
    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) => inner.Connect(target, serverName, sink);
}
