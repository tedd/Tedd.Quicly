using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Transport.MsQuic;

namespace Tedd.Quicly.Benchmarks.Transport;

/// <summary>
/// One <see cref="MsQuicTransport"/> pair over loopback (127.0.0.1, pinned self-signed certificate, default
/// <see cref="MsQuicTransportOptions"/>), shared by the MsQuic transport benchmarks. Waiting is a spin on counters the
/// sinks update from MsQuic worker threads, so a measurement includes MsQuic's own scheduling (worker wake-ups).
/// </summary>
public abstract unsafe class MsQuicTransportBenchBase
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private MsQuicRegistration _registration = null!;
    private X509Certificate2 _certificate = null!;
    private MsQuicTransportListener _listener = null!;
    private MsQuicTransportConnector _connector = null!;
    private MsQuicTransport? _server;

    protected MsQuicTransport Client { get; private set; } = null!;

    protected BenchSink ClientSink { get; private set; } = null!;

    protected BenchSink ServerSink { get; private set; } = null!;

    /// <summary>Native payload memory: 64 KiB of zeros.</summary>
    protected byte* Payload { get; private set; }

    /// <summary>[0] 64 B, [1] 1 KiB, [2..3] two 32 KiB halves of one 64 KiB gathered send.</summary>
    protected TransportSegment* Segments { get; private set; }

    protected TransportStreamId Stream { get; private set; }

    [GlobalSetup]
    public void Setup()
    {
        // The borrowed registration decides the worker execution profile: use the one the transport options default to
        // (LOW_LATENCY), as a connector or listener that owns its registration would.
        _registration = new MsQuicRegistration("quicly-bench", new MsQuicTransportOptions().ExecutionProfile);
        _certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        ServerSink = new BenchSink();
        ClientSink = new BenchSink();
        var accepted = new ManualResetEventSlim();
        _listener = new MsQuicTransportListener(new IPEndPoint(IPAddress.Loopback, 0), _certificate, new MsQuicTransportOptions { ServerPeerBidiStreamCount = 4, ServerPeerUnidiStreamCount = 4 }, _registration);
        _listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            _server = (MsQuicTransport)transport;
            ServerSink.Transport = transport;
            accepted.Set();
            return ServerSink;
        });
        _connector = new MsQuicTransportConnector(new MsQuicTransportOptions
        {
            ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
            PinnedSpkiSha256 = [SpkiPin.Compute(_certificate)],
        }, _registration);
        Client = _connector.Connect(_listener.LocalEndPoint, "localhost", ClientSink);
        ClientSink.Transport = Client;
        if (!accepted.Wait(Timeout)) throw new InvalidOperationException("The listener did not accept the connection.");
        if (!SpinUntil(ref ClientSink.Connected, 1) || !SpinUntil(ref ServerSink.Connected, 1)) throw new InvalidOperationException("The handshake did not complete.");
        long deadline = Environment.TickCount64 + (long)Timeout.TotalMilliseconds;
        while (!Client.Capabilities.Datagrams || !_server!.Capabilities.Datagrams)
        {
            if (Environment.TickCount64 > deadline) throw new InvalidOperationException("Datagrams were not negotiated.");
            Thread.Sleep(1);
        }

        Payload = (byte*)NativeMemory.AllocZeroed(64 * 1024);
        Segments = (TransportSegment*)NativeMemory.AllocZeroed((nuint)(4 * sizeof(TransportSegment)));
        Segments[0] = new TransportSegment(Payload, 64);
        Segments[1] = new TransportSegment(Payload, 1024);
        Segments[2] = new TransportSegment(Payload, 32 * 1024);
        Segments[3] = new TransportSegment(Payload + (32 * 1024), 32 * 1024);
        if (Client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId stream) != TransportStatus.Success) throw new InvalidOperationException("OpenStream failed.");
        Stream = stream;
        if (Client.SendStream(Stream, Segments, 1, 0, TransportSendFlags.Start) != TransportStatus.Success) throw new InvalidOperationException("The first stream send failed.");
        if (!SpinUntil(ref ClientSink.SendCompletions, 1)) throw new InvalidOperationException("The first stream send did not complete.");
        Warmup();
    }

    /// <summary>Runs each workload once before measuring.</summary>
    protected abstract void Warmup();

    [GlobalCleanup]
    public void Cleanup()
    {
        Console.WriteLine($"// {GetType().Name}: client received {ClientSink.DatagramsReceived} datagrams, server received {ServerSink.DatagramsReceived}, client final states {ClientSink.FinalStates} (lost {ClientSink.Lost}, canceled {ClientSink.Canceled}), ping timeouts {PingTimeouts}");
        Client.Dispose();
        _server?.Dispose();
        Client.WaitForHandlesClosed(Timeout);
        _server?.WaitForHandlesClosed(Timeout);
        _listener.Dispose();
        _connector.Dispose();
        var close = new Thread(_registration.Close) { IsBackground = true };
        close.Start();
        close.Join(Timeout);
        _certificate.Dispose();
        NativeMemory.Free(Segments);
        NativeMemory.Free(Payload);
    }

    /// <summary>Round trips whose echo did not arrive within a second (then the next ping is sent anyway).</summary>
    protected long PingTimeouts;

    protected static bool SpinUntil(ref long counter, long target) => SpinUntil(ref counter, target, Timeout);

    protected static bool SpinUntil(ref long counter, long target, TimeSpan timeout)
    {
        if (Volatile.Read(ref counter) >= target) return true;
        long deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
        var spin = new SpinWait();
        while (Volatile.Read(ref counter) < target)
        {
            if (Environment.TickCount64 > deadline) return false;
            spin.SpinOnce(sleep1Threshold: -1);
        }
        return true;
    }

    private static bool SpinUntil(ref int counter, int target)
    {
        long deadline = Environment.TickCount64 + (long)Timeout.TotalMilliseconds;
        while (Volatile.Read(ref counter) < target)
        {
            if (Environment.TickCount64 > deadline) return false;
            Thread.Sleep(1);
        }
        return true;
    }

    /// <summary>
    /// Waits until the server has received <paramref name="target"/> datagrams, or until nothing arrived for 200 ms
    /// (datagrams can be dropped under load; the drops are reported by <see cref="Cleanup"/>).
    /// </summary>
    protected void WaitForServerDatagrams(long target)
    {
        long last = Volatile.Read(ref ServerSink.DatagramsReceived);
        long idleSince = Environment.TickCount64;
        var spin = new SpinWait();
        while (true)
        {
            long now = Volatile.Read(ref ServerSink.DatagramsReceived);
            if (now >= target) return;
            if (now != last)
            {
                last = now;
                idleSince = Environment.TickCount64;
            }
            else if (Environment.TickCount64 - idleSince > 200)
            {
                return;
            }
            spin.SpinOnce(sleep1Threshold: -1);
        }
    }

    /// <summary>Counts callbacks; echoes datagrams from a native buffer when <see cref="Echo"/> is set. Never allocates.</summary>
    protected sealed class BenchSink : ITransportSink
    {
        private readonly byte* _echo = (byte*)NativeMemory.AllocZeroed(2048);
        private readonly TransportSegment* _echoSegment = (TransportSegment*)NativeMemory.AllocZeroed((nuint)sizeof(TransportSegment));
        public ITransport? Transport;
        public volatile bool Echo;
        public int Connected;
        public long DatagramsReceived;
        public long FinalStates;
        public long Lost;
        public long Canceled;
        public long SendCompletions;
        public long StreamBytes;

        public void OnConnected(in TransportConnectedInfo info) => Interlocked.Increment(ref Connected);

        public void OnDatagramReceived(ReadOnlySpan<byte> payload)
        {
            if (Echo)
            {
                // One ping in flight at a time: the previous echo was packetised (Sent) before its reply arrived.
                payload.CopyTo(new Span<byte>(_echo, payload.Length));
                *_echoSegment = new TransportSegment(_echo, payload.Length);
                Transport!.SendDatagram(_echoSegment, 1, 0, TransportSendFlags.Priority);
            }
            Interlocked.Increment(ref DatagramsReceived);
        }

        public void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
        {
            if (!state.IsFinal()) return;
            Interlocked.Increment(ref FinalStates);
            if (state == DatagramSendState.LostDiscarded) Interlocked.Increment(ref Lost);
            else if (state == DatagramSendState.Canceled) Interlocked.Increment(ref Canceled);
        }

        public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            int total = 0;
            for (int i = 0; i < segments.Length; i++) total += (int)segments[i].Length;
            Interlocked.Add(ref StreamBytes, total);
            return ReceiveResult.Consumed(total);
        }

        public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) => Interlocked.Increment(ref SendCompletions);

        public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) { }
        public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status) { }
        public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) { }
        public void OnStreamPeerSendShutdown(TransportStreamId id) { }
        public void OnStreamShutdownComplete(TransportStreamId id) => Transport?.CloseStream(id);
        public void OnDatagramCapabilityChanged(bool enabled, int maxPayload) { }
        public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes) { }
        public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional) { }
        public void OnPeerAddressChanged(in TransportConnectedInfo info) { }
        public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus) { }
    }
}

/// <summary>
/// Loopback datagram ping-pong latency (64 B, reported per round trip), one-way datagram throughput (64 B and 1 KiB,
/// reported per datagram: messages/s = 1 / mean) and stream throughput (64 KiB sends gathered from two 32 KiB segments,
/// reported per send, waiting for MsQuic's completion, i.e. the peer's acknowledgement).
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public unsafe class MsQuicTransportBench : MsQuicTransportBenchBase
{
    private const int RoundTrips = 200;
    private const int Datagrams = 4_000;
    private const int StreamSends = 64;

    protected override void Warmup()
    {
        DatagramPingPong64();
        DatagramThroughput64();
        DatagramThroughput1K();
        StreamThroughput64K();
    }

    [Benchmark(OperationsPerInvoke = RoundTrips)]
    public void DatagramPingPong64()
    {
        ServerSink.Echo = true;
        for (int i = 0; i < RoundTrips; i++)
        {
            long target = Volatile.Read(ref ClientSink.DatagramsReceived) + 1;
            Client.SendDatagram(Segments, 1, 1, TransportSendFlags.Priority);
            if (!SpinUntil(ref ClientSink.DatagramsReceived, target, TimeSpan.FromSeconds(1))) PingTimeouts++;
        }
        ServerSink.Echo = false;
    }

    [Benchmark(OperationsPerInvoke = Datagrams)]
    public void DatagramThroughput64() => Blast(Segments);

    [Benchmark(OperationsPerInvoke = Datagrams)]
    public void DatagramThroughput1K() => Blast(Segments + 1);

    [Benchmark(OperationsPerInvoke = StreamSends)]
    public void StreamThroughput64K()
    {
        long target = Volatile.Read(ref ClientSink.SendCompletions) + StreamSends;
        for (int i = 0; i < StreamSends; i++) Client.SendStream(Stream, Segments + 2, 2, 3, TransportSendFlags.None);
        SpinUntil(ref ClientSink.SendCompletions, target, TimeSpan.FromSeconds(30));
    }

    private void Blast(TransportSegment* segment)
    {
        long target = Volatile.Read(ref ServerSink.DatagramsReceived) + Datagrams;
        for (int i = 0; i < Datagrams; i++) Client.SendDatagram(segment, 1, 2, TransportSendFlags.None);
        WaitForServerDatagrams(target);
    }
}

/// <summary>
/// Bursts of 32 datagrams of 64 B, each burst waited for at the receiver (a game tick's worth), with and without
/// <see cref="TransportSendFlags.DelaySend"/> on all but the last datagram of the burst. Reported per datagram.
/// Hypothesis: DELAY_SEND lets MsQuic packetise a burst in fewer worker passes, so throughput rises.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public unsafe class MsQuicTransportDelaySendBench : MsQuicTransportBenchBase
{
    private const int Bursts = 50;
    private const int BurstSize = 32;

    [Params(false, true)]
    public bool DelaySend { get; set; }

    protected override void Warmup() => DatagramBurst32();

    [Benchmark(OperationsPerInvoke = Bursts * BurstSize)]
    public void DatagramBurst32()
    {
        long target = Volatile.Read(ref ServerSink.DatagramsReceived);
        for (int burst = 0; burst < Bursts; burst++)
        {
            for (int i = 0; i < BurstSize; i++)
            {
                TransportSendFlags flags = DelaySend && i < BurstSize - 1 ? TransportSendFlags.DelaySend : TransportSendFlags.None;
                Client.SendDatagram(Segments, 1, 4, flags);
            }
            target += BurstSize;
            WaitForServerDatagrams(target);
        }
    }
}
