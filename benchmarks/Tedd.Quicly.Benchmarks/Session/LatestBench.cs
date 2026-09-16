using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Session;

/// <summary>
/// The ReliableLatest workload of a game: one value per key per tick, with the peer's coalesced acks completing them
/// (PROTOCOL.md §4.4). <c>Latest1000Keys</c> is the 1 000-key, 60 Hz workload of docs/design/session-layer.md §7.5;
/// <c>Latest64Keys</c> is the same cycle with fewer keys, so the per-key cost can be separated from the per-pass cost.
/// Reported per value: admission (key slot, version, value entry), the transmission through the packer, the transport, the
/// mailbox receive, dispatch, the ack batch and the completion.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class LatestBench
{
    private const int ManyKeys = 1000;
    private const int FewKeys = 64;

    private readonly byte[] _value = new byte[64];
    private LatestFixture _fixture = null!;
    private QuiclyPeer _client = null!;
    private QuiclyPeer _server = null!;
    private SimulatedNetwork _network = null!;
    private long _received;
    private uint _tick;

    [GlobalSetup]
    public void Setup()
    {
        _fixture = new LatestFixture();
        _client = _fixture.Client;
        _server = _fixture.Server;
        _network = _fixture.Network;
        _server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => _received++);
        for (int i = 0; i < 60; i++)
        {
            Latest1000Keys();
            Latest64Keys();
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _fixture.Dispose();

    [Benchmark(OperationsPerInvoke = ManyKeys)]
    public void Latest1000Keys() => Cycle(ManyKeys);

    [Benchmark(OperationsPerInvoke = FewKeys)]
    public void Latest64Keys() => Cycle(FewKeys);

    private void Cycle(int keys)
    {
        for (ulong key = 0; key < (ulong)keys; key++)
        {
            _client.SendCopy(new SendHeader(2, key), _value);
        }

        _client.Flush(++_tick);
        _network.Advance(1);
        _server.Poll();
        _server.Flush();
        _network.Advance(1);
        _client.Poll();
    }

    /// <summary>One connected client/server pair whose channel 2 is a ReliableLatest channel with a dense key space.</summary>
    private sealed class LatestFixture : IDisposable
    {
        public LatestFixture()
        {
            ChannelTable table = ChannelTable.Create()
                .Add(2, "state", ChannelMode.ReliableLatest, o =>
                {
                    o.KeySpace = KeySpace.Dense(ManyKeys - 1);
                    o.MaxKeys = ManyKeys;
                })
                .Build();
            Network = new SimulatedNetwork(Clock, 1);
            Listener = new SimulatedListener(Network);
            PeerOptions serverOptions = SessionFixture.Options(Clock, compact: false);
            PeerOptions clientOptions = SessionFixture.Options(Clock, compact: false);
            QuiclyPeer? accepted = null;
            Listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo info) =>
            {
                accepted = QuiclyPeer.CreateServerPeer(transport, in info, table, serverOptions, AcceptAll.Instance);
                return accepted.TransportSink;
            });
            QuiclyPeer client = QuiclyPeer.Connect(new SimulatedConnector(Network), Listener.LocalEndPoint, "bench", table, clientOptions);
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

            Client = client;
            Server = accepted;
        }

        public VirtualClock Clock { get; } = new();

        public SimulatedNetwork Network { get; }

        public SimulatedListener Listener { get; }

        public QuiclyPeer Client { get; }

        public QuiclyPeer Server { get; }

        public void Dispose()
        {
            Client.Dispose();
            Server.Dispose();
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
}
