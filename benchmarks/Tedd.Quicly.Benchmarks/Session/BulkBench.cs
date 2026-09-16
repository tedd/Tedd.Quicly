using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Session;

/// <summary>
/// Bulk throughput end to end over a zero-delay <see cref="SimulatedTransport"/> link (PROTOCOL.md §3.3): the transfer's
/// stream, the reads from the application's <see cref="IBulkSource"/> into pooled blocks, the framing, the transport, the
/// progressive write into the application's <see cref="IBulkSink"/>, the <c>BulkProgress</c> the receiver owes and the
/// completions — for a 4 MiB object, with and without chunked LZ4 compression.
/// </summary>
/// <remarks>
/// <para><c>OperationsPerInvoke</c> is the object's size in MiB, so <c>Mean</c> is <b>nanoseconds per MiB</b> and
/// throughput is <c>1.048576e9 / Mean</c> MB/s. Both peers and the simulator run on the one benchmark thread, so these are
/// single-core figures for the whole session, as everywhere in docs/benchmarks/session.md.</para>
/// <para>The virtual clock advances 4 ms per pass and the send window holds 256 KiB (four 64 KiB pieces) outstanding, which
/// keeps the receiver's <c>BulkProgress</c> traffic — one frame per 64 KiB accepted (PROTOCOL.md §2.3) — near 1 000 per
/// simulated second, comfortably inside the 2 000/s control-message default that both peers keep
/// (<see cref="PeerOptions.ControlMessagesPerSecond"/>). Raising that default would measure a configuration no host
/// should run.</para>
/// </remarks>
[Config(typeof(InProcessShortRunConfig))]
public class BulkBench
{
    private const int Mib = 1024 * 1024;
    private const int ObjectBytes = 4 * Mib;
    private const int Megabytes = ObjectBytes / Mib;

    /// <summary>2 unordered datagrams · 5 bulk at the lowest priority.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(5, "world", ChannelMode.Bulk, o => o.Priority = 0)
        .Build();

    private VirtualClock _clock = null!;
    private SimulatedNetwork _network = null!;
    private SimulatedListener _listener = null!;
    private QuiclyPeer _client = null!;
    private QuiclyPeer _server = null!;
    private BulkSource _source = null!;

    [GlobalSetup]
    public void Setup()
    {
        _clock = new VirtualClock();
        _network = new SimulatedNetwork(_clock, 1);
        _listener = new SimulatedListener(_network);
        _source = new BulkSource();
        PeerOptions serverOptions = Options(new BulkRouter());
        PeerOptions clientOptions = Options(null);
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
        for (int i = 0; i < 3; i++)
        {
            Transfer(compress: false);
            Transfer(compress: true);
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

    /// <summary>A 4 MiB object as a raw body: one transfer, one stream, 64 KiB pieces.</summary>
    [Benchmark(OperationsPerInvoke = Megabytes)]
    public void BulkRaw() => Transfer(compress: false);

    /// <summary>The same object as a chunked LZ4 body (PROTOCOL.md §3.3): every 64 KiB piece is one chunk.</summary>
    [Benchmark(OperationsPerInvoke = Megabytes)]
    public void BulkCompressed() => Transfer(compress: true);

    /// <summary>
    /// One object, start to finish: the transfer completes on the peer's <c>BulkProgress</c>, but its record is released
    /// only when the stream shuts down a pass or two later (PROTOCOL.md §7: the slot returns with the peer's credit), so
    /// the measurement runs to that point too. Stopping at completion would leave records behind and the invocation after
    /// next would be refused by the two-per-direction limit — which is also why each row includes a stream's whole
    /// lifetime rather than just its bytes.
    /// </summary>
    private void Transfer(bool compress)
    {
        BulkDescriptor descriptor = new(5, 1, 1, ObjectBytes, 0, 0, compress);
        BulkTransfer transfer = _client.BeginBulkSendAsync(descriptor, _source).GetAwaiter().GetResult();
        for (int pass = 0; pass < 20_000 && (!transfer.IsFinished || LiveTransfers() > 0); pass++)
        {
            _client.Flush();
            _network.Advance(0);
            _server.Poll();
            _server.Flush();
            _network.Advance(4_000);
            _client.Poll();
        }

        if (transfer.Status != BulkStatus.Completed)
        {
            throw new InvalidOperationException($"The transfer ended {transfer.Status}, not Completed.");
        }

        if (LiveTransfers() > 0)
        {
            throw new InvalidOperationException("The transfer's stream did not shut down.");
        }
    }

    /// <summary>Transfer records the sending engine still holds on the bulk channel.</summary>
    private long LiveTransfers() => _client.GetChannelStatistics(5, out ChannelStatistics statistics) ? statistics.QueuedMessages : 0;

    private static PeerOptions Options(IBulkRouter? router)
    {
        PeerOptions options = SessionFixture.Options(new VirtualClock(), compact: false);
        options.SendBudgetBytes = 4 * Mib;
        options.ReceiveBudgetBytes = 4 * Mib;
        options.BulkSendWindowBytes = 256 * 1024;
        options.BulkShareOfCongestionWindow = 1;
        options.BulkRouter = router;
        options.AllocatorOptions = new SlabAllocatorOptions
        {
            FreeListShards = 2,
            SizeClasses =
            [
                new(64, 4096),
                new(256, 1024),
                new(1536, 256),
                new(4096, 128),
                new(16384, 64),
                new(65536, 128),
                new(262144, 8),
            ],
        };
        return options;
    }

    /// <summary>A compressible object generated on the fly, so the benchmark holds no 4 MiB array.</summary>
    private sealed class BulkSource : IBulkSource
    {
        public int Read(long offset, Span<byte> destination)
        {
            int take = (int)Math.Min(destination.Length, ObjectBytes - offset);
            if (take <= 0)
            {
                return 0;
            }

            for (int i = 0; i < take; i++)
            {
                destination[i] = (byte)((offset + i) >> 6);
            }

            return take;
        }
    }

    private sealed class BulkRouter : IBulkRouter
    {
        private readonly BulkTarget _target = new();

        public BulkReceiveDecision SelectTarget(in BulkTransferInfo info) => BulkReceiveDecision.Accept(_target);
    }

    private sealed class BulkTarget : IBulkSink
    {
        public long Bytes;

        public void Write(long objectOffset, ReadOnlySpan<byte> data) => Bytes += data.Length;

        public void Finish(in BulkResult result)
        {
        }
    }

    private sealed class AcceptAll : IPeerAdmission
    {
        public static readonly AcceptAll Instance = new();

        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }
}
