using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Simulation;

/// <summary>
/// Cost of the simulated transport itself (so tests and session benchmarks know what the harness adds):
/// one datagram each way per 100 µs of virtual time, on an ideal and on a lossy/jittery/reordering link,
/// and a 64 KiB stream send delivered and acknowledged.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public unsafe class SimulatedTransportBench
{
    private const int Datagrams = 1000;

    private SimulatedNetwork _ideal = null!;
    private SimulatedTransport _idealA = null!;
    private SimulatedTransport _idealB = null!;
    private SimulatedNetwork _lossy = null!;
    private SimulatedTransport _lossyA = null!;
    private SimulatedTransport _lossyB = null!;
    private SimulatedNetwork _stream = null!;
    private SimulatedTransport _streamA = null!;
    private TransportStreamId _streamId;
    private readonly byte[] _datagram = new byte[600];
    private readonly byte[] _block = new byte[64 * 1024];

    [GlobalSetup]
    public void Setup()
    {
        (_ideal, _idealA, _idealB) = Create(new LinkOptions { DelayMicros = 1_000 }, 1);
        (_lossy, _lossyA, _lossyB) = Create(new LinkOptions { DelayMicros = 1_000, JitterMicros = 500, LossPercent = 5, ReorderPercent = 5 }, 2);
        (_stream, _streamA, _) = Create(new LinkOptions { DelayMicros = 1_000 }, 3);
        _streamA.OpenStream(StreamKind.Bidirectional, 0, 32767, out _streamId);
        _streamA.StartStream(_streamId);
        _stream.RunUntilIdle(1_000_000);
        for (int i = 0; i < 20; i++)
        {
            DatagramPingPong();
            DatagramPingPongLossy();
            StreamSend64KiB();
        }
    }

    private static (SimulatedNetwork, SimulatedTransport, SimulatedTransport) Create(LinkOptions options, int seed)
    {
        SimulatedNetwork network = new(new VirtualClock(), seed);
        (SimulatedTransport a, SimulatedTransport b) = network.CreatePair(new NullSink(), new NullSink(), options);
        network.RunUntilIdle(1_000_000);
        return (network, a, b);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _ideal.Dispose();
        _lossy.Dispose();
        _stream.Dispose();
    }

    [Benchmark(OperationsPerInvoke = Datagrams)]
    public void DatagramPingPong() => Run(_ideal, _idealA, _idealB);

    [Benchmark(OperationsPerInvoke = Datagrams)]
    public void DatagramPingPongLossy() => Run(_lossy, _lossyA, _lossyB);

    [Benchmark]
    public void StreamSend64KiB()
    {
        fixed (byte* p = _block)
        {
            TransportSegment segment = new(p, _block.Length);
            _streamA.SendStream(_streamId, &segment, 1, 0, TransportSendFlags.None);
        }
        _stream.RunUntilIdle(1_000_000);
    }

    private void Run(SimulatedNetwork network, SimulatedTransport a, SimulatedTransport b)
    {
        fixed (byte* p = _datagram)
        {
            TransportSegment segment = new(p, _datagram.Length);
            for (int i = 0; i < Datagrams; i++)
            {
                a.SendDatagram(&segment, 1, 1, TransportSendFlags.None);
                b.SendDatagram(&segment, 1, 2, TransportSendFlags.None);
                network.Advance(100);
            }
        }
    }

    private sealed class NullSink : ITransportSink
    {
        public void OnConnected(in TransportConnectedInfo info) { }
        public void OnDatagramReceived(ReadOnlySpan<byte> payload) { }
        public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) { }
        public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status) { }
        public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            int total = 0;
            foreach (TransportSegment segment in segments)
                total += (int)segment.Length;
            return ReceiveResult.Consumed(total);
        }
        public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) { }
        public void OnDatagramSendStateChanged(ulong context, DatagramSendState state) { }
        public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) { }
        public void OnStreamPeerSendShutdown(TransportStreamId id) { }
        public void OnStreamShutdownComplete(TransportStreamId id) { }
        public void OnDatagramCapabilityChanged(bool enabled, int maxPayload) { }
        public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes) { }
        public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional) { }
        public void OnPeerAddressChanged(in TransportConnectedInfo info) { }
        public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus) { }
    }
}
