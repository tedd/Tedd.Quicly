using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Simulation;

/// <summary>Regression tests added by review. Failing tests document open defects.</summary>
public unsafe class ReviewTests
{
    /// <summary>
    /// A zero-length (non-FIN) stream send produces a (X, X) chunk. When jitter makes it arrive after the frontier has
    /// already passed X, <c>SimStream.WriteChunk</c> inserts it at the head of the held list, where the merge loop
    /// (<c>_held[0].Start == Frontier</c>) can never remove it, so every later out-of-order chunk stays held forever:
    /// data is never delivered and the sends never complete.
    /// </summary>
    [Fact]
    public void ZeroLengthSend_WithJitter_DoesNotStallStream()
    {
        List<int> stalled = new();
        for (int seed = 1; seed <= 64; seed++)
        {
            using SimHarness h = new(new LinkOptions { DelayMicros = 1_000, JitterMicros = 10_000 }, seed);
            Assert.Equal(TransportStatus.Success, h.A.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id));
            // Start the stream with an empty send (legal: count 0), then send data.
            Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, [], 100, TransportSendFlags.Start));
            byte[] data = Sim.Pattern(12_000, seed);
            Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, data, 101));
            Assert.True(h.Network.RunUntilIdle(10_000_000));

            TransportStreamId peer = h.SinkB.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
            bool delivered = h.SinkB.GetStreamData(peer).AsSpan().SequenceEqual(data);
            bool completed = h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted).Count == 2;
            if (!delivered || !completed)
                stalled.Add(seed);
        }
        Assert.True(stalled.Count == 0, $"Stream stalled (data not delivered / sends not completed) for seeds: {string.Join(",", stalled)}");
    }

    /// <summary>
    /// Connector flow: the client is connected at 2 x delay, the server only at 3 x delay. A server-side
    /// <c>UpdatePeerStreamLimits</c> in that window is stored (state Connecting) but never reaches the client, which
    /// read the server's allowance at its own connect: the update is silently lost.
    /// </summary>
    [Fact]
    public void UpdatePeerStreamLimits_WhileServerConnectingAfterClientConnected_ReachesClient()
    {
        VirtualClock clock = new();
        using SimulatedNetwork network = new(clock, 1);
        using SimulatedListener listener = new(network);
        SimulatedTransport? server = null;
        RecordingSink serverSink = new(clock);
        listener.Start(
            static (in NewConnectionInfo _) => PreHandshakeDecision.Accept,
            (ITransport t, in NewConnectionInfo _) =>
            {
                server = (SimulatedTransport)t;
                serverSink.Transport = t;
                return serverSink;
            });
        RecordingSink clientSink = new(clock);
        SimulatedTransport client = (SimulatedTransport)new SimulatedConnector(network, new LinkOptions { DelayMicros = 1_000 })
            .Connect(listener.LocalEndPoint, null, clientSink);
        clientSink.Transport = client;

        network.AdvanceTo(2_500);
        Assert.Equal(TransportState.Connected, client.State);
        Assert.NotNull(server);
        Assert.Equal(TransportState.Connecting, server.State);

        server.UpdatePeerStreamLimits(4, 2);
        Assert.True(network.RunUntilIdle(1_000_000));
        Assert.Equal(TransportState.Connected, server.State);

        for (int i = 0; i < 4; i++)
        {
            Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, (ulong)i, 32767, out TransportStreamId id));
            Assert.Equal(TransportStatus.Success, client.StartStream(id));
        }
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 9, 32767, out TransportStreamId uni));
        Assert.Equal(TransportStatus.Success, client.StartStream(uni));
    }

    /// <summary>Probe: one unidirectional stream per message (open, send with Start|Fin, shutdown, CloseStream) must not allocate.</summary>
    [Fact]
    public void SteadyState_StreamPerMessage_DoesNotAllocate()
    {
        VirtualClock clock = new();
        using SimulatedNetwork network = new(clock, 3);
        ClosingSink a = new(), b = new();
        (SimulatedTransport ta, SimulatedTransport tb) = network.CreatePair(a, b,
            new LinkOptions { DelayMicros = 1_000, JitterMicros = 300, StreamLossPercent = 5, PeerUnidiStreams = 64 });
        a.Transport = ta;
        b.Transport = tb;
        network.RunUntilIdle(1_000_000);
        byte[] payload = Sim.Pattern(3000);
        // Warm up until the (seed-fixed) peak of concurrent streams, sends and events has been reached; tables then stop growing.
        for (int i = 0; i < 20_000; i++)
            OneMessage(network, ta, payload);

        int windows = WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 5_000; i++)
                OneMessage(network, ta, payload);
        });

        // Streams are opened as often as the stream limit allows, so the bytes through are a fraction of the messages tried.
        long tried = 20_000 + (windows * 5_000L);
        Assert.True(b.Bytes > tried * 3000L * 9 / 25, $"received {b.Bytes} bytes of {tried} messages tried");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void OneMessage(SimulatedNetwork network, SimulatedTransport a, byte[] payload)
    {
        if (a.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId id) == TransportStatus.Success)
        {
            fixed (byte* p = payload)
            {
                TransportSegment segment = new(p, payload.Length);
                if (a.SendStream(id, &segment, 1, 2, TransportSendFlags.Start | TransportSendFlags.Fin) != TransportStatus.Success)
                    a.CloseStream(id);
            }
        }
        network.Advance(200);
    }

    private sealed class ClosingSink : ITransportSink
    {
        public ITransport? Transport;
        public long Bytes;

        public void OnConnected(in TransportConnectedInfo info) { }
        public void OnDatagramReceived(ReadOnlySpan<byte> payload) { }
        public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) { }
        public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status) { }
        public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            int total = 0;
            for (int i = 0; i < segments.Length; i++)
                total += (int)segments[i].Length;
            Bytes += total;
            return ReceiveResult.Consumed(total);
        }
        public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) { }
        public void OnDatagramSendStateChanged(ulong context, DatagramSendState state) { }
        public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) { }
        public void OnStreamPeerSendShutdown(TransportStreamId id) { }
        public void OnStreamShutdownComplete(TransportStreamId id) => Transport!.CloseStream(id);
        public void OnDatagramCapabilityChanged(bool enabled, int maxPayload) { }
        public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes) { }
        public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional) { }
        public void OnPeerAddressChanged(in TransportConnectedInfo info) { }
        public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus) { }
    }
}
