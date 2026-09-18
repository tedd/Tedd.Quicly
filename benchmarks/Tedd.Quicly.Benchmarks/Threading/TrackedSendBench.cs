using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Benchmarks.Session;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Threading;

/// <summary>
/// Tracked sends end to end over a zero-delay <see cref="SimulatedTransport"/> link (the <see cref="SessionEndToEndBench"/>
/// cycle with <see cref="SendOptions.Tracked"/>), so every message also allocates, completes and releases a
/// <see cref="CompletionTable"/> slot. 100 × 64 B per cycle on the ordered stream. <c>TrackedOrdered64</c> never waits: the
/// slot is released inside the client's Poll when its second stage completes. <c>AwaitedOrdered64</c> takes the
/// <see cref="QuiclyPeer.WaitAsync"/> ValueTask right after each send and consumes it once it has completed (a cycle or
/// more later: the ordered stream completes a carrier when the transport reports it, not necessarily within the same
/// cycle), so the slot is released when the result is read — the path a wait consumed after Dispose also takes.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class TrackedSendBench
{
    private const int Batch = 100;
    private const int MaxOutstanding = 2048;

    private readonly byte[] _small = new byte[64];
    private readonly ValueTask<DeliveryStatus>[] _waits = new ValueTask<DeliveryStatus>[MaxOutstanding];
    private SessionFixture _fixture = null!;
    private QuiclyPeer _client = null!;
    private QuiclyPeer _server = null!;
    private SimulatedNetwork _network = null!;
    private long _received;
    private long _admitted;
    private long _delivered;
    private int _head;
    private int _tail;

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
            TrackedOrdered64();
            AwaitedOrdered64();
        }

        if (_admitted != 2 * 200 * Batch || _tail - _head > 4 * Batch)
        {
            throw new InvalidOperationException($"Tracked sends: {_admitted} admitted of {2 * 200 * Batch}, {_tail - _head} waits outstanding.");
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _fixture.Dispose();

    [Benchmark(OperationsPerInvoke = Batch)]
    public void TrackedOrdered64()
    {
        long admitted = 0;
        for (int i = 0; i < Batch; i++)
        {
            if (_client.SendCopy(new SendHeader(4), _small, SendOptions.Tracked).IsAdmitted)
            {
                admitted++;
            }
        }

        _admitted += admitted;
        Cycle();
    }

    [Benchmark(OperationsPerInvoke = Batch)]
    public void AwaitedOrdered64()
    {
        ValueTask<DeliveryStatus>[] waits = _waits;
        long admitted = 0;
        for (int i = 0; i < Batch && _tail - _head < MaxOutstanding; i++)
        {
            SendResult result = _client.SendCopy(new SendHeader(4), _small, SendOptions.Tracked);
            if (result.IsAdmitted)
            {
                admitted++;
                waits[_tail++ & (MaxOutstanding - 1)] = _client.WaitAsync(result.Token, CompletionStage.RemoteAccepted);
            }
        }

        _admitted += admitted;
        Cycle();
        long delivered = 0;
        while (_head != _tail)
        {
            ref ValueTask<DeliveryStatus> wait = ref waits[_head & (MaxOutstanding - 1)];
            if (!wait.IsCompleted)
            {
                break;
            }

            if (wait.Result == DeliveryStatus.Delivered)
            {
                delivered++;
            }

            wait = default;
            _head++;
        }

        _delivered += delivered;
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
