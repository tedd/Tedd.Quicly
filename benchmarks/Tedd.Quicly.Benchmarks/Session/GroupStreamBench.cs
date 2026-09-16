using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Session;

/// <summary>
/// ReliableUnordered (group stream) delivery end to end over a zero-delay <see cref="SimulatedTransport"/> link: admission,
/// sealing the group, opening its unidirectional stream, the gathered carriers, the progressive receive, dispatch to a handler
/// and the completions — per message. <c>Group64</c> hands 100 × 64 B to one group, <c>Group4K</c> 16 × 4 KiB; every batch is
/// one group on one stream, opened and closed inside the measured cycle, so each row includes a stream's whole lifetime.
/// </summary>
/// <remarks>
/// The peers run with <see cref="PeerOptions.GroupMinInterval"/> = 0 because the benchmark's virtual clock does not advance
/// between cycles: the interval only bounds how often a channel opens a stream in wall-clock time (PROTOCOL.md §3.2), and a
/// benchmark that measures one group per batch has to seal every batch.
/// </remarks>
[Config(typeof(InProcessShortRunConfig))]
public class GroupStreamBench
{
    private const int Batch = 100;
    private const int LargeBatch = 16;

    /// <summary>2 unordered datagrams · 5 groups (MaxGroups 8, GroupMaxBytes 64 KiB).</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(5, "events", ChannelMode.ReliableUnordered)
        .Build();

    private readonly byte[] _small = new byte[64];
    private readonly byte[] _large = new byte[4096];
    private VirtualClock _clock = null!;
    private SimulatedNetwork _network = null!;
    private SimulatedListener _listener = null!;
    private QuiclyPeer _client = null!;
    private QuiclyPeer _server = null!;
    private long _received;

    [GlobalSetup]
    public void Setup()
    {
        _clock = new VirtualClock();
        _network = new SimulatedNetwork(_clock, 1);
        _listener = new SimulatedListener(_network);
        PeerOptions serverOptions = Options();
        PeerOptions clientOptions = Options();
        QuiclyPeer? accepted = null;
        _listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo info) =>
        {
            accepted = QuiclyPeer.CreateServerPeer(transport, in info, Table, serverOptions, AcceptAll.Instance);
            return accepted.TransportSink;
        });
        _client = QuiclyPeer.Connect(new SimulatedConnector(_network), _listener.LocalEndPoint, "bench", Table, clientOptions);
        for (int step = 0; step < 10_000 && !(_client.State == PeerState.Connected && accepted?.State == PeerState.Connected); step++)
        {
            _client.Poll();
            _client.Flush();
            accepted?.Poll();
            accepted?.Flush();
            _network.Advance(100);
        }

        if (_client.State != PeerState.Connected || accepted is null || accepted.State != PeerState.Connected)
        {
            throw new InvalidOperationException("The benchmark session did not connect.");
        }

        _server = accepted;
        _server.RegisterHandler(5, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => _received++);
        for (int i = 0; i < 200; i++)
        {
            Group64();
            Group4K();
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _client.Dispose();
        _server.Dispose();
        _network.RunUntilIdle(60_000_000);
        _network.Dispose();
        _listener.Dispose();
    }

    [Benchmark(OperationsPerInvoke = Batch)]
    public void Group64()
    {
        for (int i = 0; i < Batch; i++)
        {
            _client.SendCopy(new SendHeader(5), _small);
        }

        Deliver(Batch);
    }

    [Benchmark(OperationsPerInvoke = LargeBatch)]
    public void Group4K()
    {
        for (int i = 0; i < LargeBatch; i++)
        {
            _client.SendCopy(new SendHeader(5), _large);
        }

        Deliver(LargeBatch);
    }

    /// <summary>Runs passes until the batch's group has been delivered (one group needs a few: its start is confirmed first).</summary>
    private void Deliver(int expected)
    {
        long target = _received + expected;
        for (int pass = 0; pass < 64 && _received < target; pass++)
        {
            _client.Flush();
            _network.Advance(0);
            _server.Poll();
            _network.Advance(0);
            _client.Poll();
        }
    }

    private static PeerOptions Options()
    {
        PeerOptions options = SessionFixture.Options(new VirtualClock(), compact: false);
        options.GroupMinInterval = TimeSpan.Zero;
        return options;
    }

    private sealed class AcceptAll : IPeerAdmission
    {
        public static readonly AcceptAll Instance = new();

        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }
}
