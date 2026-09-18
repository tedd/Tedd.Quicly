using System.Diagnostics;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Benchmarks.Session;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Threading;

/// <summary>
/// The real receive path of a peer across two cores: the transport thread (producer) hands packed containers of 64-byte
/// unordered messages to the server peer's <see cref="QuiclyPeer.TransportSink"/> — container parse, engine, receive lease
/// rent and copy, receive-ring publish, work signal — and the game thread (consumer, the benchmark thread) runs
/// <see cref="QuiclyPeer.Poll"/>, which dispatches every message to a handler and gives its lease back.
/// </summary>
/// <remarks>
/// <para>
/// Unlike <see cref="CrossCoreHandoffBench"/>, which times kernels, this drives the product code end to end on the receiving
/// side. Setup connects a client and a server over the simulator, then captures the datagrams one client flush of
/// <see cref="Batch"/> messages produces (unordered messages carry no sequence state, so the same containers can be
/// replayed forever). A pass is: the producer replays the captured containers into the server's sink, then the consumer
/// polls the server once. The phases are lockstep (padded <c>go</c>/<c>done</c> words) so each side's busy time is its own;
/// <see cref="Placement"/> as in <see cref="CrossCoreHandoffBench"/> (the process affinity must include CPUs 4, 6 and 20:
/// <c>PROF_AFFINITY=100050</c>).
/// </para>
/// <para>
/// <see cref="Signal"/> gives the server a <see cref="PeerOptions.WorkSignal"/> (a counter, as a host's would be): the
/// server and client hosts always set one, so every published message runs the peer's work-signal protocol.
/// <see cref="Cleanup"/> prints each side's busy time per message after a 2.5 s warm-up.
/// </para>
/// </remarks>
[Config(typeof(ThreadingBenchmarkConfig))]
public unsafe class PeerReceiveHandoffBench
{
    /// <summary>Messages per invocation.</summary>
    public const int Messages = 1000;

    private const int ConsumerCpu = 4;
    private const int SameCcdCpu = 6;
    private const int CrossCcdCpu = 20;

    private readonly VirtualClock _clock = new();
    private SimulatedNetwork _network = null!;
    private SimulatedListener _listener = null!;
    private QuiclyPeer _client = null!;
    private QuiclyPeer _server = null!;
    private TapSink _tap = null!;
    private CountingSignal _signal = null!;
    private byte[][] _containers = [];
    private Thread? _producer;
    private long _seq;
    private PaddedLong _go;
    private PaddedLong _done;
    private PaddedLong _producerTicks;
    private long _consumerTicks;
    private long _wallTicks;
    private long _measuredMessages;
    private long _received;
    private long _sink;
    private long _setupTimestamp;
    private bool _measuring;
    private int _harnessThread;
    private PaddedLong _produced;
    private PaddedLong _consumed;
    private long _producerTicksBase;
    private long _producedBase;
    private long _receivedBase;

    /// <summary>Messages per pass (one client flush; must divide <see cref="Messages"/>).</summary>
    [Params(10, 100)]
    public int Batch { get; set; } = 100;

    /// <summary><c>Single</c>, <c>SameCcd</c> or <c>CrossCcd</c>.</summary>
    [Params("Single", "SameCcd", "CrossCcd")]
    public string Placement { get; set; } = "Single";

    /// <summary>Whether the server has a work signal (hosts always set one).</summary>
    [Params(true, false)]
    public bool Signal { get; set; } = true;

    /// <summary>
    /// Both threads run at once, as in a real peer: the producer keeps publishing (at most two batches ahead of what the
    /// consumer has taken) while the consumer polls, so lines both use are contended instead of handed over once per pass.
    /// Only with a producer thread (<see cref="Placement"/> other than <c>Single</c>).
    /// </summary>
    [Params(false, true)]
    public bool Pipelined { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        if (Messages % Batch != 0)
        {
            throw new InvalidOperationException("Batch must divide " + Messages);
        }

        PinHarness();
        _signal = new CountingSignal();
        _network = new SimulatedNetwork(_clock, 1);
        _listener = new SimulatedListener(_network);
        PeerOptions serverOptions = SessionFixture.Options(_clock, compact: false);
        serverOptions.WorkSignal = Signal ? _signal : null;
        PeerOptions clientOptions = SessionFixture.Options(_clock, compact: false);
        QuiclyPeer? accepted = null;
        _listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo info) =>
        {
            accepted = QuiclyPeer.CreateServerPeer(transport, in info, SessionFixture.Table, serverOptions, AcceptAll.Instance);
            _tap = new TapSink(accepted.TransportSink);
            return _tap;
        });
        _client = QuiclyPeer.Connect(new SimulatedConnector(_network), _listener.LocalEndPoint, "bench", SessionFixture.Table, clientOptions);
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
        _server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            _received++;
            _sink += payload[0] + payload[^1] + header.Length;
        });

        byte[] message = new byte[64];
        for (int i = 0; i < message.Length; i++)
        {
            message[i] = (byte)(i + 1);
        }

        // A few simulator cycles first, then capture one flush of Batch messages.
        for (int round = 0; round < 20; round++)
        {
            SendBatch(message);
            _client.Flush();
            _network.Advance(0);
            _server.Poll();
            _network.Advance(0);
            _client.Poll();
        }

        SendBatch(message);
        _client.Flush();
        long before = _received;
        _tap.Capture = true;
        _network.Advance(0);
        _tap.Capture = false;
        _server.Poll();
        _network.Advance(0);
        _client.Poll();
        if (_received - before != Batch)
        {
            throw new InvalidOperationException($"Captured {_received - before} messages, expected {Batch}.");
        }

        _containers = _tap.Captured.ToArray();
        _receivedBase = _received;
        if (Placement != "Single")
        {
            int cpu = Placement switch
            {
                "SameCcd" => SameCcdCpu,
                "CrossCcd" => CrossCcdCpu,
                _ => throw new ArgumentException(Placement),
            };
            _producer = Pipelined
                ? new Thread(() => PipelinedProducerMain(cpu)) { IsBackground = true, Priority = ThreadPriority.Highest }
                : new Thread(() => ProducerMain(cpu)) { IsBackground = true, Priority = ThreadPriority.Highest };
            _producer.Start();
        }

        _setupTimestamp = Stopwatch.GetTimestamp();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (_producer is not null)
        {
            Volatile.Write(ref _go.Value, -1);
            _producer.Join();
        }

        double f = 1e9 / Stopwatch.Frequency / Math.Max(1, _measuredMessages);
        if (_measuredMessages > 0)
        {
            if (Pipelined && _producer is not null)
            {
                double pf = 1e9 / Stopwatch.Frequency / Math.Max(1, _produced.Value - _producedBase);
                _server.GetStatistics(out PeerStatistics s);
                Console.WriteLine(string.Create(System.Globalization.CultureInfo.InvariantCulture,
                    $"HANDOFF PeerReceive{(Signal ? "Signal" : "")} {Placement}Pipe batch={Batch}: producer {(_producerTicks.Value - _producerTicksBase) * pf:F1} | consumer {_consumerTicks * f:F1} | busy {(_producerTicks.Value - _producerTicksBase) * pf + _consumerTicks * f:F1} | wall {_wallTicks * f:F1} ns/msg ({_measuredMessages} msgs, containers/pass {_containers.Length}, signals {_signal.Count}, drops {s.ReceiveRingDrops + s.OutOfReceiveBuffers}, outstanding {s.ReceiveBytesOutstanding})"));
                _measuredMessages = 0;
            }
        }

        if (_measuredMessages > 0)
        {
            _server.GetStatistics(out PeerStatistics statistics);
            Console.WriteLine(string.Create(System.Globalization.CultureInfo.InvariantCulture,
                $"HANDOFF PeerReceive{(Signal ? "Signal" : "")} {Placement} batch={Batch}: producer {_producerTicks.Value * f:F1} | consumer {_consumerTicks * f:F1} | busy {(_producerTicks.Value + _consumerTicks) * f:F1} | wall {_wallTicks * f:F1} ns/msg ({_measuredMessages} msgs, containers/pass {_containers.Length}, signals {_signal.Count}, drops {statistics.ReceiveRingDrops + statistics.OutOfReceiveBuffers}, outstanding {statistics.ReceiveBytesOutstanding})"));
        }

        _client.Dispose();
        _server.Dispose();
        _network.RunUntilIdle(60_000_000);
        _network.Dispose();
        _listener.Dispose();
    }

    [Benchmark(OperationsPerInvoke = Messages)]
    public void PeerReceive()
    {
        if (Environment.CurrentManagedThreadId != _harnessThread)
        {
            PinHarness();
        }

        if (!_measuring && Stopwatch.GetElapsedTime(_setupTimestamp).TotalSeconds > 2.5)
        {
            // Lockstep: the producer is idle between passes (it has published its last `done`), so its counter can be reset
            // here. Pipelined: it keeps running, so its counters are sampled instead.
            _producerTicksBase = Volatile.Read(ref _producerTicks.Value);
            _producedBase = Volatile.Read(ref _produced.Value);
            _measuring = true;
            if (!Pipelined)
            {
                _producerTicks.Value = 0;
                _producerTicksBase = 0;
            }

            _consumerTicks = 0;
            _wallTicks = 0;
            _measuredMessages = 0;
        }

        if (Pipelined && _producer is not null)
        {
            RunPipelined();
            return;
        }

        int passes = Messages / Batch;
        long consumer = 0;
        long start = Stopwatch.GetTimestamp();
        if (_producer is null)
        {
            long producer = 0;
            for (int p = 0; p < passes; p++)
            {
                long t0 = Stopwatch.GetTimestamp();
                Produce();
                long t1 = Stopwatch.GetTimestamp();
                Consume();
                long t2 = Stopwatch.GetTimestamp();
                producer += t1 - t0;
                consumer += t2 - t1;
            }

            _producerTicks.Value += producer;
        }
        else
        {
            for (int p = 0; p < passes; p++)
            {
                long seq = ++_seq;
                Volatile.Write(ref _go.Value, seq);
                while (Volatile.Read(ref _done.Value) != seq)
                {
                    Thread.SpinWait(1);
                }

                long t0 = Stopwatch.GetTimestamp();
                Consume();
                consumer += Stopwatch.GetTimestamp() - t0;
            }
        }

        _consumerTicks += consumer;
        _wallTicks += Stopwatch.GetTimestamp() - start;
        _measuredMessages += Messages;
    }

    private void RunPipelined()
    {
        long first = _received;
        long target = first + Messages;
        long consumer = 0;
        long start = Stopwatch.GetTimestamp();
        while (_received < target)
        {
            long t0 = Stopwatch.GetTimestamp();
            if (_server.Poll() > 0)
            {
                consumer += Stopwatch.GetTimestamp() - t0;
                Volatile.Write(ref _consumed.Value, _received - _receivedBase);
            }
        }

        _consumerTicks += consumer;
        _wallTicks += Stopwatch.GetTimestamp() - start;
        _measuredMessages += _received - first;
    }

    private void PipelinedProducerMain(int cpu)
    {
        Pin(cpu);
        long produced = 0;
        long ticks = 0;
        while (Volatile.Read(ref _go.Value) >= 0)
        {
            if (produced - Volatile.Read(ref _consumed.Value) > 2 * Batch)
            {
                Thread.SpinWait(1);
                continue;
            }

            long t0 = Stopwatch.GetTimestamp();
            Produce();
            ticks += Stopwatch.GetTimestamp() - t0;
            produced += Batch;
            Volatile.Write(ref _producerTicks.Value, ticks);
            Volatile.Write(ref _produced.Value, produced);
        }
    }

    private void SendBatch(byte[] message)
    {
        for (int i = 0; i < Batch; i++)
        {
            if (!_client.SendCopy(new SendHeader(2), message).IsAdmitted)
            {
                throw new InvalidOperationException("The client refused a send.");
            }
        }
    }

    private void Produce()
    {
        ITransportSink sink = _tap.Inner;
        byte[][] containers = _containers;
        for (int i = 0; i < containers.Length; i++)
        {
            sink.OnDatagramReceived(containers[i]);
        }
    }

    private void Consume()
    {
        long before = _received;
        _server.Poll();
        if (_received - before != Batch)
        {
            Fail();
        }
    }

    private void ProducerMain(int cpu)
    {
        Pin(cpu);
        long seen = 0;
        while (true)
        {
            long go;
            while ((go = Volatile.Read(ref _go.Value)) == seen)
            {
                Thread.SpinWait(1);
            }

            if (go < 0)
            {
                return;
            }

            seen = go;
            long t0 = Stopwatch.GetTimestamp();
            Produce();
            _producerTicks.Value += Stopwatch.GetTimestamp() - t0;
            Volatile.Write(ref _done.Value, go);
        }
    }

    private static void Fail() => throw new InvalidOperationException("a pass did not dispatch exactly one batch");

    private void PinHarness()
    {
        Pin(ConsumerCpu);
        _harnessThread = Environment.CurrentManagedThreadId;
    }

    private static void Pin(int cpu)
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        if (SetThreadAffinityMask(GetCurrentThread(), (nuint)1 << cpu) == 0)
        {
            throw new InvalidOperationException($"Cannot pin a thread to CPU {cpu}: the process affinity must include it (driver: PROF_AFFINITY=100050).");
        }
    }

    [DllImport("kernel32.dll")]
    private static extern nint GetCurrentThread();

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern nuint SetThreadAffinityMask(nint thread, nuint mask);

    private sealed class AcceptAll : IPeerAdmission
    {
        public static readonly AcceptAll Instance = new();

        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }

    /// <summary>A host's work signal: counts the edges (any thread).</summary>
    private sealed class CountingSignal : IPeerWorkSignal
    {
        private long _count;

        public long Count => Volatile.Read(ref _count);

        public void OnWork(QuiclyPeer peer) => Interlocked.Increment(ref _count);
    }

    /// <summary>Forwards every callback to the peer's sink and, while <see cref="Capture"/> is on, copies the datagrams.</summary>
    private sealed class TapSink(ITransportSink inner) : ITransportSink
    {
        public ITransportSink Inner { get; } = inner;

        public bool Capture { get; set; }

        public List<byte[]> Captured { get; } = [];

        public void OnConnected(in TransportConnectedInfo info) => Inner.OnConnected(in info);

        public void OnDatagramReceived(ReadOnlySpan<byte> payload)
        {
            if (Capture)
            {
                Captured.Add(payload.ToArray());
            }

            Inner.OnDatagramReceived(payload);
        }

        public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) => Inner.OnPeerStreamStarted(id, kind);

        public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status) => Inner.OnStreamStarted(id, context, status);

        public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin) =>
            Inner.OnStreamReceived(id, segments, absoluteOffset, fin);

        public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) => Inner.OnStreamSendCompleted(id, context, canceled);

        public void OnDatagramSendStateChanged(ulong context, DatagramSendState state) => Inner.OnDatagramSendStateChanged(context, state);

        public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => Inner.OnStreamAborted(id, errorCode, direction);

        public void OnStreamPeerSendShutdown(TransportStreamId id) => Inner.OnStreamPeerSendShutdown(id);

        public void OnStreamShutdownComplete(TransportStreamId id) => Inner.OnStreamShutdownComplete(id);

        public void OnDatagramCapabilityChanged(bool enabled, int maxPayload) => Inner.OnDatagramCapabilityChanged(enabled, maxPayload);

        public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes) => Inner.OnIdealSendBufferSize(id, bytes);

        public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional) => Inner.OnStreamsAvailable(bidirectional, unidirectional);

        public void OnPeerAddressChanged(in TransportConnectedInfo info) => Inner.OnPeerAddressChanged(in info);

        public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus) => Inner.OnClosed(reason, errorCode, transportStatus);
    }
}
