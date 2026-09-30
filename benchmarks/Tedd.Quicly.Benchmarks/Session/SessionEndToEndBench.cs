using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Session;

/// <summary>
/// End to end over a zero-delay <see cref="SimulatedTransport"/> link: admission, the scheduler pass, the transport, the
/// receive path, dispatch to a handler and the completions, per message. <c>Unreliable64Packed</c> sends 100 messages per
/// flush (packed containers), <c>Unreliable64Loose</c> one message per flush (a datagram each), <c>Sequenced64Keyed</c> 100
/// keyed sequenced messages per flush, <c>Sequenced64KeyedExpiring</c> the same with the expiry an
/// <c>UnreliableSequenced</c> channel has by default, <c>Ordered64</c> / <c>Ordered4K</c> 100 × 64 B / 16 × 4 KiB on the
/// persistent ordered stream. The simulator's own cost is in docs/benchmarks/simulation.md.
/// </summary>
/// <remarks>
/// The fixture's sequenced channel has expiry switched off, so <c>Sequenced64Keyed</c> never touches the expiry path.
/// <c>Sequenced64KeyedExpiring</c> is its pair: the difference between the two is what an expiry costs per message — the
/// mark at admission and the resolve at the start of the scheduler pass (<c>PeerCore.StampExpiry</c> /
/// <c>ResolveExpiry</c>). It passes the expiry per send rather than adding a channel, so the fixture's table — and with
/// it every other benchmark's baseline — stays as it was.
/// </remarks>
[Config(typeof(InProcessShortRunConfig))]
public class SessionEndToEndBench
{
    private const int Batch = 100;
    private const int LargeBatch = 16;
    private const long DefaultSequencedExpiryMicros = 33_332;

    private readonly byte[] _small = new byte[64];
    private readonly byte[] _large = new byte[4096];
    private SessionFixture _fixture = null!;
    private QuiclyPeer _client = null!;
    private QuiclyPeer _server = null!;
    private SimulatedNetwork _network = null!;
    private long _received;

    [GlobalSetup]
    public void Setup()
    {
        _fixture = new SessionFixture(1);
        _client = _fixture.Clients[0];
        _server = _fixture.Servers[0];
        _network = _fixture.Network;
        MessageHandler count = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => _received++;
        _server.RegisterHandler(2, count);
        _server.RegisterHandler(3, count);
        _server.RegisterHandler(4, count);
        for (int i = 0; i < 200; i++)
        {
            Unreliable64Packed();
            Unreliable64Loose();
            Sequenced64Keyed();
            Sequenced64KeyedExpiring();
            Ordered64();
            Ordered4K();
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _fixture.Dispose();

    [Benchmark(OperationsPerInvoke = Batch)]
    public void Unreliable64Packed()
    {
        for (int i = 0; i < Batch; i++)
        {
            _client.SendCopy(new SendHeader(2), _small);
        }

        Cycle();
    }

    [Benchmark(OperationsPerInvoke = Batch)]
    public void Unreliable64Loose()
    {
        for (int i = 0; i < Batch; i++)
        {
            _client.SendCopy(new SendHeader(2), _small);
            Cycle();
        }
    }

    [Benchmark(OperationsPerInvoke = Batch)]
    public void Sequenced64Keyed()
    {
        for (int i = 0; i < Batch; i++)
        {
            _client.SendCopy(new SendHeader(3, (ulong)i), _small);
        }

        Cycle();
    }

    [Benchmark(OperationsPerInvoke = Batch)]
    public void Sequenced64KeyedExpiring()
    {
        // 2 × the default FlushInterval (166 667 ticks): what ChannelDefinition.ExpiryTwiceFlushInterval resolves to. The
        // cycle never moves the clock, so nothing expires; every message pays the stamp and the resolve.
        SendOptions expiring = new() { ExpiryMicros = DefaultSequencedExpiryMicros };
        for (int i = 0; i < Batch; i++)
        {
            _client.SendCopy(new SendHeader(3, (ulong)i), _small, expiring);
        }

        Cycle();
    }

    [Benchmark(OperationsPerInvoke = Batch)]
    public void Ordered64()
    {
        for (int i = 0; i < Batch; i++)
        {
            _client.SendCopy(new SendHeader(4), _small);
        }

        Cycle();
    }

    [Benchmark(OperationsPerInvoke = LargeBatch)]
    public void Ordered4K()
    {
        for (int i = 0; i < LargeBatch; i++)
        {
            _client.SendCopy(new SendHeader(4), _large);
        }

        Cycle();
    }

    private void Cycle()
    {
        _client.Flush();
        _network.Advance(0);
        _server.Poll();
        _network.Advance(0);
        _client.Poll();
    }
}
