using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Session;

/// <summary>
/// A request/response round trip on a <see cref="ChannelDefinition.RequestResponse"/> ordered channel (PROTOCOL.md §3.1,
/// §4.3) over a zero-delay <see cref="SimulatedTransport"/> link, per <em>request</em>: the request's admission with its id
/// and request-table slot, the carrier that takes it to the peer, the peer's dispatch and <see cref="QuiclyPeer.Respond"/>,
/// the response's carrier, and the matching that completes the value task on the requesting side.
/// </summary>
/// <remarks>
/// <c>RequestRoundTrip</c> keeps 16 requests in flight at once (the batch a game sends per tick); <c>RequestSingle</c> sends
/// one at a time, so the per-pass cost is not amortised. Neither awaits: the value tasks are consumed once they have
/// completed, which is the allocation-free path the unit tests assert.
/// </remarks>
[Config(typeof(InProcessShortRunConfig))]
public class RequestResponseBench
{
    private const int Batch = 16;

    /// <summary>10 ordered request/response.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(10, "rpc", ChannelMode.ReliableOrdered, o => o.RequestResponse = true)
        .Build();

    private readonly ReadOnlyMemory<byte> _payload = new byte[64];
    private readonly ValueTask<ReceiveLease>[] _pending = new ValueTask<ReceiveLease>[Batch];
    private VirtualClock _clock = null!;
    private SimulatedNetwork _network = null!;
    private SimulatedListener _listener = null!;
    private QuiclyPeer _client = null!;
    private QuiclyPeer _server = null!;
    private long _answered;

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
        _server.RegisterHandler(10, (QuiclyPeer self, in ReceiveHeader header, ReadOnlySpan<byte> payload) => self.Respond(in header, payload));
        for (int i = 0; i < 200; i++)
        {
            RequestRoundTrip();
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
        if (_answered == 0)
        {
            throw new InvalidOperationException("No request was answered.");
        }
    }

    /// <summary>16 requests in flight at once, answered by the peer's handler.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public void RequestRoundTrip() => Run(Batch);

    /// <summary>One request at a time: the per-pass cost is not shared with other requests.</summary>
    [Benchmark]
    public void RequestSingle() => Run(1);

    private void Run(int count)
    {
        for (int i = 0; i < count; i++)
        {
            _pending[i] = _client.SendRequestAsync(new SendHeader(10), _payload, TimeSpan.Zero);
        }

        for (int pass = 0; pass < 16 && !_pending[count - 1].IsCompleted; pass++)
        {
            _client.Flush();
            _network.Advance(0);
            _server.Poll();
            _server.Flush();
            _network.Advance(0);
            _client.Poll();
        }

        for (int i = 0; i < count; i++)
        {
            if (!_pending[i].IsCompleted)
            {
                throw new InvalidOperationException("A request was not answered.");
            }

            ReceiveLease response = _pending[i].GetAwaiter().GetResult();
            _answered++;
            _client.Release(in response);
        }
    }

    private sealed class AcceptAll : IPeerAdmission
    {
        public static readonly AcceptAll Instance = new();

        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }
}
