using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Session;

/// <summary>
/// The batch receive path end to end over a zero-delay <see cref="SimulatedTransport"/> link: the ordered channel has no
/// handler, so <see cref="QuiclyPeer.Poll"/> moves its messages to the channel's drain queue and the application takes
/// them with <see cref="QuiclyPeer.Drain"/> and <see cref="QuiclyPeer.Release(ReadOnlySpan{ReceivedMessage})"/>, once per
/// cycle. The handler path for the same traffic is <see cref="SessionEndToEndBench"/>'s <c>Ordered64</c> / <c>Ordered4K</c>.
/// </summary>
/// <remarks>
/// <c>OrderedDrain64</c> and <c>OrderedDrain4K</c> send a frame's worth per cycle (100 × 64 B and 16 × 4 KiB), far below
/// the drain queues' node pool (1 024 with the fixture's ring), so they measure what the path costs per message.
/// <c>OrderedDrain64Burst</c> lets 1 000 messages arrive before the receiver polls once — a late frame, and nearly the
/// whole pool in one Poll — and the cycle polls and drains until everything has arrived.
/// <para>
/// The channel takes receive credit (<see cref="QuiclyPeer.Poll"/>, "a reliable channel"). Every cycle drains it empty,
/// so after the first one it counts as read and the ring is its limit: no stream is held back in any of the three, and
/// what they measure of the credit is its bookkeeping — the count per message on both threads, the state check at the
/// end of each Drain and at each pass start. The burst is about twice the channel's reserved share of the pool (512
/// here), so half of it waits in nodes beyond the pool.
/// </para>
/// </remarks>
[Config(typeof(InProcessShortRunConfig))]
public class SessionDrainBench
{
    private const int Batch = 100;
    private const int LargeBatch = 16;
    private const int Burst = 1_000;
    private const ushort Ordered = 4;

    private readonly byte[] _small = new byte[64];
    private readonly byte[] _large = new byte[4096];
    private readonly ReceivedMessage[] _buffer = new ReceivedMessage[256];
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
        for (int i = 0; i < 200; i++)
        {
            OrderedDrain64();
            OrderedDrain4K();
            OrderedDrain64Burst();
        }

        // What the remarks promise: a channel that is drained every cycle is never held back for its credit.
        if (!_server.GetChannelStatistics(Ordered, out ChannelStatistics statistics) || statistics.BacklogHolds != 0)
        {
            throw new InvalidOperationException($"The drained channel was held back {statistics.BacklogHolds} times during warm-up.");
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _fixture.Dispose();

    [Benchmark(OperationsPerInvoke = Batch)]
    public void OrderedDrain64()
    {
        for (int i = 0; i < Batch; i++)
        {
            _client.SendCopy(new SendHeader(Ordered), _small);
        }

        Cycle(Batch);
    }

    [Benchmark(OperationsPerInvoke = LargeBatch)]
    public void OrderedDrain4K()
    {
        for (int i = 0; i < LargeBatch; i++)
        {
            _client.SendCopy(new SendHeader(Ordered), _large);
        }

        Cycle(LargeBatch);
    }

    [Benchmark(OperationsPerInvoke = Burst)]
    public void OrderedDrain64Burst()
    {
        // A late frame on the receiving side: two flushes of the sender arrive before the receiver polls once.
        for (int flush = 0; flush < 2; flush++)
        {
            for (int i = 0; i < Burst / 2; i++)
            {
                _client.SendCopy(new SendHeader(Ordered), _small);
            }

            _client.Flush();
            _network.Advance(0);
            _client.Poll();
        }

        Cycle(Burst);
    }

    /// <summary>Messages taken so far (keeps the drain observable).</summary>
    public long Received => _received;

    private void Cycle(int expected)
    {
        _client.Flush();
        int taken = 0;
        int rounds = 0;
        do
        {
            _network.Advance(0);
            _server.Poll();
            int n;
            while ((n = _server.Drain(Ordered, _buffer)) > 0)
            {
                _server.Release(_buffer.AsSpan(0, n));
                taken += n;
            }

            if (taken == expected)
            {
                break;
            }

            if (++rounds > 64)
            {
                throw new InvalidOperationException($"Only {taken} of {expected} messages arrived.");
            }

            // Not everything is there yet: the stream is still starting (the very first cycle), or the receiver held the
            // stream back and the transport delivers the rest now that there is room. The sender keeps running.
            _network.Advance(0);
            _client.Poll();
            _client.Flush();
        }
        while (true);

        _received += taken;
        _network.Advance(0);
        _client.Poll();
    }
}
