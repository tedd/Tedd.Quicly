using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Session;

/// <summary>
/// Fragmented unreliable delivery end to end over a zero-delay <see cref="SimulatedTransport"/> link (PROTOCOL.md §2.1):
/// admission (one owner entry plus its fragments, each pointing into the owner's payload), the scheduler pass that hands
/// every fragment to the transport as its own datagram, the receiver's reassembly into one pooled lease, dispatch to a
/// handler and the fragments' completions — per <em>message</em>.
/// </summary>
/// <remarks>
/// <c>Fragment3</c> sends 2 400-byte messages (three fragments) and <c>Fragment8</c> 8 375-byte ones (eight, the maximum a
/// channel may use). The same payload sent whole would be one datagram in <c>SessionEndToEndBench.Unreliable64Loose</c>
/// terms, so the difference between the two rows is what each extra fragment costs.
/// </remarks>
[Config(typeof(InProcessShortRunConfig))]
public class FragmentBench
{
    private const int Batch = 20;

    /// <summary>2 unordered fragmenting (8 800 B).</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "chunks", ChannelMode.UnreliableUnordered, o =>
        {
            o.Fragmentation = true;
            o.MaxMessageSize = ChannelDefinition.FragmentedMaxMessageSize;
        })
        .Build();

    private readonly byte[] _three = new byte[2_400];
    private readonly byte[] _eight = new byte[8_375];
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
        PeerOptions serverOptions = SessionFixture.Options(_clock, compact: false);
        PeerOptions clientOptions = SessionFixture.Options(_clock, compact: false);
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
        _server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => _received++);
        for (int i = 0; i < 100; i++)
        {
            Fragment3();
            Fragment8();
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

    /// <summary>Messages of three fragments.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public void Fragment3() => Send(_three);

    /// <summary>Messages of eight fragments, the most PROTOCOL.md §2.1 allows.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public void Fragment8() => Send(_eight);

    private void Send(byte[] payload)
    {
        for (int i = 0; i < Batch; i++)
        {
            _client.SendCopy(new SendHeader(2), payload);
        }

        long target = _received + Batch;
        for (int pass = 0; pass < 16 && _received < target; pass++)
        {
            _client.Flush();
            _network.Advance(0);
            _server.Poll();
            _network.Advance(0);
            _client.Poll();
        }
    }

    private sealed class AcceptAll : IPeerAdmission
    {
        public static readonly AcceptAll Instance = new();

        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }
}
