using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Benchmarks.Session;

/// <summary>
/// The two halves of the send path in isolation, over four peer pairs so that one invocation is long enough to time while
/// the tables stay in cache:
/// <c>Flush100Buffered</c> is one <see cref="QuiclyPeer.Flush"/> of a peer holding 100 buffered 64-byte messages (the
/// scheduler pass: packing into containers, or gathering into stream sends), <c>SendCopy</c> the admission of one 64-byte
/// message (channel lookup, entry, lease, copy, header, queue). The iteration setup fills (or drains) the queues outside the
/// measurement, so every iteration is one invocation.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
[InvocationCount(1)]
[IterationCount(60)]
[WarmupCount(10)]
public class SessionPassBench
{
    private const int Pairs = 4;
    private const int Messages = 100;

    private readonly byte[] _payload = new byte[64];
    private SessionFixture _fixture = null!;

    /// <summary>Unreliable (datagrams, packed) or ordered (the persistent stream).</summary>
    [Params(false, true)]
    public bool Ordered { get; set; }

    private ushort Channel => Ordered ? (ushort)4 : (ushort)2;

    [GlobalSetup]
    public void Setup()
    {
        _fixture = new SessionFixture(Pairs, compact: true);
        for (int i = 0; i < 50; i++)
        {
            AdmitEverywhere();
            _fixture.Cycle();
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _fixture.Dispose();

    [IterationSetup(Target = nameof(Flush100Buffered))]
    public void AdmitEverywhere()
    {
        foreach (QuiclyPeer client in _fixture.Clients)
        {
            for (int i = 0; i < Messages; i++)
            {
                client.SendCopy(new SendHeader(Channel), _payload);
            }
        }
    }

    [Benchmark(OperationsPerInvoke = Pairs)]
    public void Flush100Buffered()
    {
        foreach (QuiclyPeer client in _fixture.Clients)
        {
            client.Flush();
        }
    }

    [IterationCleanup(Target = nameof(Flush100Buffered))]
    public void DeliverAfterFlush() => _fixture.Cycle();

    [Benchmark(OperationsPerInvoke = Pairs * Messages)]
    public void SendCopy()
    {
        foreach (QuiclyPeer client in _fixture.Clients)
        {
            for (int i = 0; i < Messages; i++)
            {
                client.SendCopy(new SendHeader(Channel), _payload);
            }
        }
    }

    [IterationCleanup(Target = nameof(SendCopy))]
    public void DeliverAfterAdmission() => _fixture.Cycle();
}
