using System.Net;
using System.Text;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

/// <summary>Channel tables of the server tests.</summary>
internal static class ServerTables
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

/// <summary>What the server reported when it admitted a peer (captured at the time: the peer is disposed after it closes).</summary>
internal readonly record struct AdmittedPeer(QuiclyPeer Peer, int Index, ulong SessionId, uint Epoch, ulong Tag);

/// <summary>
/// A <see cref="QuiclyServer"/> over a simulated network plus client peers, pumped deterministically on the test thread
/// (the simulator raises every transport callback inside <c>Advance</c>).
/// </summary>
internal sealed class ServerFixture : IAsyncDisposable
{
    private readonly List<QuiclyPeer> _clients = [];
    private readonly SlabAllocator _clientPool = new(Pools.Small());
    private bool _disposed;

    public ServerFixture(Action<ServerOptions>? configure = null, int seed = 1, bool start = true, ITransportListener? listener = null, bool observe = true)
    {
        Network = new SimulatedNetwork(Clock, seed);
        SimListener = new SimulatedListener(Network);
        Options = new ServerOptions { Channels = ServerTables.Default, ExpectedPeers = 16, MaxPeers = 64 };
        Options.PeerOptions.Clock = Clock;
        Options.PeerOptions.AllocatorOptions = Pools.Small();
        Options.Admission.MaxUnadmittedConnections = 32;
        configure?.Invoke(Options); // the resume rate keeps its production defaults (MinResumeInterval 1 s, ResumeBurst 3)
        Server = new QuiclyServer(Options, listener ?? SimListener);
        Server.DelayOverride = (_, _) =>
        {
            Step(1_000);
            return Task.CompletedTask;
        };
        if (observe)
        {
            Server.PeerAdmitted += peer => Admitted.Add(new AdmittedPeer(peer, peer.Index, peer.SessionId, peer.Epoch, peer.Tag));
            Server.PeerClosed += (peer, reason) => Closed.Add((peer, reason));
            Server.SessionEnded += info => Ended.Add(info);
            Server.AdmissionFailed += failure =>
            {
                lock (Failures)
                {
                    Failures.Add(failure);
                }
            };
        }
        Connector = new SimulatedConnector(Network);
        if (start)
        {
            Server.StartAsync(TestContext.Current.CancellationToken).GetAwaiter().GetResult();
        }
    }

    public VirtualClock Clock { get; } = new();

    public SimulatedNetwork Network { get; }

    public SimulatedListener SimListener { get; }

    public ServerOptions Options { get; }

    public QuiclyServer Server { get; }

    public SimulatedConnector Connector { get; }

    public List<AdmittedPeer> Admitted { get; } = [];

    public List<(QuiclyPeer Peer, CloseReason Reason)> Closed { get; } = [];

    public List<SessionEndInfo> Ended { get; } = [];

    public List<AdmissionFailure> Failures { get; } = [];

    public Action<PeerOptions>? ConfigureClient { get; set; }

    public IReadOnlyList<QuiclyPeer> Clients => _clients;

    public PeerOptions ClientOptions(ReadOnlyMemory<byte> sessionToken = default, uint lastEpoch = 0)
    {
        PeerOptions options = new()
        {
            Clock = Clock,
            Allocator = _clientPool,
            SendTableCapacity = 64,
            ReceiveRingCapacity = 64,
            SegmentArenaCapacity = 64,
            SessionToken = sessionToken,
            LastEpoch = lastEpoch,
        };
        ConfigureClient?.Invoke(options);
        return options;
    }

    public QuiclyPeer Connect(string? auth = null, ReadOnlyMemory<byte> sessionToken = default, uint lastEpoch = 0,
        ITransportConnector? connector = null, ChannelTable? table = null, string? serverName = "game.test")
    {
        byte[]? token = auth is null ? null : Encoding.UTF8.GetBytes(auth);
        QuiclyPeer client = QuiclyPeer.Connect(connector ?? Connector, SimListener.LocalEndPoint, serverName, table ?? ServerTables.Default,
            ClientOptions(sessionToken, lastEpoch), token);
        _clients.Add(client);
        return client;
    }

    /// <summary>Connects and runs until the client is admitted.</summary>
    public QuiclyPeer ConnectAdmitted(string? auth = null, ReadOnlyMemory<byte> sessionToken = default, uint lastEpoch = 0, ITransportConnector? connector = null)
    {
        QuiclyPeer client = Connect(auth, sessionToken, lastEpoch, connector);
        Assert.True(RunUntil(() => client.State == PeerState.Connected), "The client was not admitted: " + client.State + " " + client.HandshakeStatus + " " + client.CloseReason);
        return client;
    }

    /// <summary>Resumes the session of <paramref name="previous"/> on a new connection.</summary>
    public QuiclyPeer Resume(QuiclyPeer previous, ITransportConnector? connector = null) =>
        Connect(sessionToken: previous.SessionToken.ToArray(), lastEpoch: previous.Epoch, connector: connector);

    /// <summary>The live server peer of an admitted client (matched by session id).</summary>
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

    public void DisposeClient(QuiclyPeer client)
    {
        _clients.Remove(client);
        client.Dispose();
    }

    public void Pump()
    {
        Server.PollAll();
        Server.FlushAll();
        PumpClients();
    }

    public void PumpClients()
    {
        if (_disposed)
        {
            return;
        }

        foreach (QuiclyPeer client in _clients)
        {
            client.Poll();
            client.Flush();
        }
    }

    /// <summary>Advances the network and pumps the clients only (the server is driven by the caller).</summary>
    public void Step(long micros)
    {
        Network.Advance(micros);
        PumpClients();
    }

    public void Run(long micros, long step = 1_000)
    {
        long end = Network.NowMicros + micros;
        Pump();
        while (Network.NowMicros < end)
        {
            Network.AdvanceTo(Math.Min(end, Network.NowMicros + step));
            Pump();
        }
    }

    public bool RunUntil(Func<bool> condition, long maxMicros = 10_000_000, long step = 1_000)
    {
        long end = Network.NowMicros + maxMicros;
        Pump();
        while (!condition())
        {
            if (Network.NowMicros >= end)
            {
                return false;
            }

            Network.AdvanceTo(Math.Min(end, Network.NowMicros + step));
            Pump();
        }

        return true;
    }

    public List<AdmissionFailure> FailuresOf(AdmissionFailureReason reason)
    {
        lock (Failures)
        {
            return Failures.FindAll(f => f.Reason == reason);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        foreach (QuiclyPeer client in _clients)
        {
            client.Dispose();
        }

        _disposed = true;
        await Server.DisposeAsync();
        Network.RunUntilIdle(60_000_000);
        Network.Dispose();
        SimListener.Dispose();
        _clientPool.Dispose();
    }
}

/// <summary>A listener whose callbacks the test invokes itself (any remote address).</summary>
internal sealed class ManualListener : ITransportListener
{
    public IPEndPoint LocalEndPoint { get; } = new(IPAddress.Loopback, 4433);

    public PreHandshakeCallback? PreHandshake { get; private set; }

    public AcceptCallback? Accept { get; private set; }

    public int StopCalls { get; private set; }

    public bool Disposed { get; private set; }

    public void Start(PreHandshakeCallback preHandshake, AcceptCallback accept)
    {
        PreHandshake = preHandshake;
        Accept = accept;
    }

    public void Stop() => StopCalls++;

    public void Dispose() => Disposed = true;
}

/// <summary>Forwards a transport's callbacks to a sink chosen after creation, optionally reporting another remote address.</summary>
internal sealed class SpoofSink : ITransportSink
{
    public ITransportSink? Target { get; set; }

    public IPEndPoint? RemoteEndPoint { get; set; }

    public void OnConnected(in TransportConnectedInfo info)
    {
        TransportConnectedInfo copy = info;
        copy.RemoteEndPoint = RemoteEndPoint ?? info.RemoteEndPoint;
        Target?.OnConnected(in copy);
    }

    public void OnPeerAddressChanged(in TransportConnectedInfo info) => Target?.OnPeerAddressChanged(in info);

    public void OnDatagramReceived(ReadOnlySpan<byte> payload) => Target?.OnDatagramReceived(payload);

    public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) => Target?.OnPeerStreamStarted(id, kind);

    public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status) => Target?.OnStreamStarted(id, context, status);

    public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
    {
        if (Target is null)
        {
            int total = 0;
            foreach (TransportSegment segment in segments)
            {
                total += (int)segment.Length;
            }

            return ReceiveResult.Consumed(total);
        }

        return Target.OnStreamReceived(id, segments, absoluteOffset, fin);
    }

    public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) => Target?.OnStreamSendCompleted(id, context, canceled);

    public void OnDatagramSendStateChanged(ulong context, DatagramSendState state) => Target?.OnDatagramSendStateChanged(context, state);

    public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => Target?.OnStreamAborted(id, errorCode, direction);

    public void OnStreamPeerSendShutdown(TransportStreamId id) => Target?.OnStreamPeerSendShutdown(id);

    public void OnStreamShutdownComplete(TransportStreamId id) => Target?.OnStreamShutdownComplete(id);

    public void OnDatagramCapabilityChanged(bool enabled, int maxPayload) => Target?.OnDatagramCapabilityChanged(enabled, maxPayload);

    public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes) => Target?.OnIdealSendBufferSize(id, bytes);

    public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional) => Target?.OnStreamsAvailable(bidirectional, unidirectional);

    public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus) => Target?.OnClosed(reason, errorCode, transportStatus);
}

/// <summary>
/// Connects client peers to a <see cref="ManualListener"/> over simulated pairs, presenting <see cref="RemoteAddress"/> as the
/// client's address both before the handshake and when the server's transport connects.
/// </summary>
internal sealed class PairConnector(SimulatedNetwork network, ManualListener listener) : ITransportConnector
{
    public IPEndPoint RemoteAddress { get; set; } = new(IPAddress.Parse("192.0.2.10"), 50_000);

    public string Alpn { get; set; } = "quicly/1";

    public PreHandshakeDecision LastDecision { get; private set; }

    public bool LastAccepted { get; private set; }

    public SpoofSink? LastServerSink { get; private set; }

    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
    {
        SpoofSink spoof = new() { RemoteEndPoint = RemoteAddress };
        (SimulatedTransport client, SimulatedTransport server) = network.CreatePair(sink, spoof);
        NewConnectionInfo info = default;
        info.RemoteEndPoint = RemoteAddress;
        info.ServerName = serverName;
        Span<byte> alpn = info.Alpn;
        info.AlpnLength = (byte)Encoding.ASCII.GetBytes(Alpn, alpn);
        LastDecision = listener.PreHandshake!(in info);
        ITransportSink? accepted = LastDecision == PreHandshakeDecision.Accept ? listener.Accept!(server, in info) : null;
        LastAccepted = accepted is not null;
        if (accepted is null)
        {
            server.Close(0, default);
        }
        else
        {
            spoof.Target = accepted;
        }

        LastServerSink = spoof;
        return client;
    }
}

/// <summary>Allocation checks over warmed-up windows (a one-off runtime allocation in one window is tolerated).</summary>
internal static class AllocationAssert
{
    public static void NoAllocations(Action body, int warmup = 200, int iterations = 1_000, int windows = 5)
    {
        for (int i = 0; i < warmup; i++)
        {
            body();
        }

        long[] deltas = new long[windows];
        int allocating = 0;
        for (int window = 0; window < windows; window++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < iterations; i++)
            {
                body();
            }

            deltas[window] = GC.GetAllocatedBytesForCurrentThread() - before;
            if (deltas[window] != 0)
            {
                allocating++;
            }
        }

        Assert.True(allocating <= 1, "Allocated in " + allocating + " of " + windows + " windows: " + string.Join(", ", deltas) + " bytes per " + iterations + " calls.");
    }
}
