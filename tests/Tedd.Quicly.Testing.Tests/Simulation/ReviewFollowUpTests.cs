using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Simulation;

/// <summary>Tests for the fixes and additions made after review (see <see cref="ReviewTests"/> for the original reproductions).</summary>
public class ReviewFollowUpTests
{
    private static int Total(ReadOnlySpan<TransportSegment> segments)
    {
        int total = 0;
        foreach (TransportSegment segment in segments)
            total += (int)segment.Length;
        return total;
    }

    private static DatagramSendState[] States(RecordingSink sink, ulong context) =>
        sink.OfKind(RecordedEventKind.DatagramSendStateChanged).Where(e => e.Context == context).Select(e => e.DatagramState).ToArray();

    // ------------------------------------------------------------------ reassembly

    [Fact]
    public void SimStream_IgnoresEmptyChunks_AndMergesAHeldEmptyFin()
    {
        SimBufferPool pool = new();
        SimStream s = new() { Generation = 1, InUse = true };
        byte[] data = Sim.Pattern(20);
        Assert.False(s.WriteChunk(pool, [], 0, fin: false)); // an empty send holds nothing
        Assert.False(s.WriteChunk(pool, data.AsSpan(10, 10), 10, fin: false));
        Assert.False(s.WriteChunk(pool, [], 20, fin: true)); // empty FIN ahead of the frontier: held
        Assert.False(s.WriteChunk(pool, [], 5, fin: false));
        Assert.True(s.WriteChunk(pool, data.AsSpan(0, 10), 0, fin: false));
        Assert.Equal(20, s.Frontier);
        Assert.True(s.FinArrived);
        Assert.False(s.WriteChunk(pool, [], 20, fin: false)); // a late empty chunk at the frontier changes nothing
        TransportSegment[] segments = new TransportSegment[4];
        Assert.Equal(2, s.BuildSegments(ref segments));
    }

    // ------------------------------------------------------------------ connect ordering and stream limits

    [Fact]
    public void CreatePair_ZeroDelay_BothCapabilitiesPrecedeBothConnects_AndALimitRaisedInTheCallbackReachesThePeer()
    {
        VirtualClock clock = new();
        using SimulatedNetwork network = new(clock, 1);
        List<string> log = new();
        OrderSink a = new("A", log), b = new("B", log);
        (SimulatedTransport ta, SimulatedTransport tb) = network.CreatePair(a, b);
        b.OnCapability = () => tb.UpdatePeerStreamLimits(3, 2);
        Assert.True(network.RunUntilIdle(1_000));
        Assert.Equal(["A:Capability", "B:Capability", "A:Connected", "B:Connected"], log);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(TransportStatus.Success, ta.OpenStream(StreamKind.Bidirectional, (ulong)i, 0, out TransportStreamId id));
            Assert.Equal(TransportStatus.Success, ta.StartStream(id));
        }
        Assert.Equal(TransportStatus.Success, ta.OpenStream(StreamKind.Unidirectional, 9, 0, out TransportStreamId uni));
        Assert.Equal(TransportStatus.Success, ta.StartStream(uni));
        Assert.Equal(0, network.InvariantViolations);
    }

    [Fact]
    public void UpdatePeerStreamLimits_FromTheConnectedClient_WhileTheServerIsConnecting_IsReadAtConnect_AndReportedAgain()
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
        client.UpdatePeerStreamLimits(3, 0);
        Assert.True(network.RunUntilIdle(1_000_000));

        // The server read the new limit when it connected (3 ms) and is told again when the update lands (3.5 ms).
        RecordedEvent available = Assert.Single(serverSink.OfKind(RecordedEventKind.StreamsAvailable));
        Assert.Equal(3_500, available.TimeMicros);
        Assert.Equal(3, available.Bidirectional);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(TransportStatus.Success, server.OpenStream(StreamKind.Bidirectional, (ulong)i, 0, out TransportStreamId id));
            Assert.Equal(TransportStatus.Success, server.StartStream(id));
        }
        Assert.Equal(TransportStatus.Success, server.OpenStream(StreamKind.Bidirectional, 9, 0, out TransportStreamId fourth));
        Assert.Equal(TransportStatus.Success, server.StartStream(fourth)); // beyond the limit: refused asynchronously, as MsQuic does
        Assert.True(network.RunUntilIdle(1_000_000));
        Assert.Equal(TransportStatus.StreamLimitReached, Assert.Single(serverSink.OfKind(RecordedEventKind.StreamStarted), e => e.StreamId == fourth).Status);
    }

    // ------------------------------------------------------------------ aborts are causal

    [Fact]
    public void AbortSend_ThePeerKeepsDeliveringHeldDataUntilTheResetArrives()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        int calls = 0;
        h.SinkB.ReceiveHandler = (_, segments, _, _) => ++calls == 1 ? ReceiveResult.PendingAfter(0) : ReceiveResult.Consumed(Total(segments));
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "held"u8, 1));
        h.Network.Advance(10_000); // B holds the bytes (pending)
        Assert.Equal(1, calls);
        TransportStreamId peer = h.SinkB.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;

        h.A.AbortStream(id, 7, StreamAbortDirection.Send); // the reset reaches B at 20 ms
        h.Network.Advance(1_000);
        h.B.ResumeStreamReceive(peer, 0);
        h.Network.Advance(0);
        Assert.Equal(2, calls); // B has not seen the reset yet
        Assert.Equal("held"u8.ToArray(), h.SinkB.OfKind(RecordedEventKind.StreamReceived)[1].Data);
        Assert.Empty(h.SinkB.OfKind(RecordedEventKind.StreamAborted));

        h.Network.RunUntilIdle(1_000_000);
        RecordedEvent aborted = Assert.Single(h.SinkB.OfKind(RecordedEventKind.StreamAborted));
        Assert.Equal(7UL, aborted.ErrorCode);
        Assert.Equal(20_000 + h.SinkA.Events[0].TimeMicros, aborted.TimeMicros);
        Assert.True(Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted)).Canceled);
    }

    // ------------------------------------------------------------------ size limits

    [Fact]
    public unsafe void SendStream_LargerThanOneGiB_IsTooLarge()
    {
        using SimHarness h = new();
        TransportStreamId id = h.OpenAndStart(h.A);
        byte b = 0;
        TransportSegment* segments = stackalloc TransportSegment[2];
        segments[0] = new TransportSegment(&b, SimulatedTransport.MaxStreamSendBytes); // never read: the size check comes first
        segments[1] = new TransportSegment(&b, 1);
        Assert.Equal(TransportStatus.TooLarge, h.A.SendStream(id, segments, 2, 0, TransportSendFlags.None));
    }

    [Fact]
    public void SimBufferPool_RentBeyondMaxLength_Throws()
    {
        SimBufferPool pool = new();
        Assert.Throws<InsufficientMemoryException>(() => pool.Rent(SimBufferPool.MaxLength + 1));
        Assert.Throws<InsufficientMemoryException>(() => pool.Rent(-1));
    }

    // ------------------------------------------------------------------ partial consumption after the FIN

    [Fact]
    public void PartialConsumeAfterFin_WithoutPending_IndicatesTheRestAgain()
    {
        using SimHarness h = new();
        h.SinkB.ReceiveHandler = (_, segments, _, _) => ReceiveResult.Consumed(Math.Min(1, Total(segments)));
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "abcd"u8, 1, TransportSendFlags.Fin));
        Assert.True(h.Network.RunUntilIdle(1_000_000));
        IReadOnlyList<RecordedEvent> receives = h.SinkB.OfKind(RecordedEventKind.StreamReceived);
        Assert.Equal([0UL, 1UL, 2UL, 3UL], receives.Select(e => e.Offset).ToArray());
        Assert.All(receives, e => Assert.True(e.Fin));
        Assert.Equal("abcd"u8.ToArray(), h.SinkB.GetStreamData(receives[0].StreamId));
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.StreamPeerSendShutdown));
    }

    [Fact]
    public void ConsumingNothingAfterFin_WithoutPending_StallsWithoutSpinning()
    {
        using SimHarness h = new();
        h.SinkB.ReceiveHandler = (_, _, _, _) => ReceiveResult.Consumed(0);
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "abc"u8, 1, TransportSendFlags.Fin));
        Assert.True(h.Network.RunUntilIdle(1_000_000));
        Assert.Single(h.SinkB.OfKind(RecordedEventKind.StreamReceived));
        Assert.Equal(0, h.SinkB.CountOf(RecordedEventKind.StreamPeerSendShutdown));
    }

    // ------------------------------------------------------------------ CancelOnBlocked

    [Fact]
    public void CancelOnBlocked_WhileTheSerializerIsBusy_CancelsInsteadOfQueueing()
    {
        using SimHarness h = new(new LinkOptions { BandwidthBitsPerSecond = 1_000_000 });
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[100], 1, TransportSendFlags.CancelOnBlocked)); // idle: goes out
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[100], 2, TransportSendFlags.CancelOnBlocked)); // busy: canceled
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[100], 3)); // busy: queued
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Acknowledged], States(h.SinkA, 1));
        Assert.Equal([DatagramSendState.Canceled], States(h.SinkA, 2));
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Acknowledged], States(h.SinkA, 3));
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(1, stats.DatagramsCanceled);
        Assert.Equal(0, stats.DatagramsDropped);
        Assert.Equal(2, h.SinkB.CountOf(RecordedEventKind.DatagramReceived));

        // Idle again: a CancelOnBlocked datagram goes out.
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[100], 4, TransportSendFlags.CancelOnBlocked));
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Acknowledged], States(h.SinkA, 4));
    }

    // ------------------------------------------------------------------ invariants and fault injection

    [Fact]
    public void Invariant_CountsFailedChecks()
    {
        using SimulatedNetwork network = new(new VirtualClock(), 1);
        Assert.True(network.Invariant(true));
        Assert.False(network.Invariant(false));
        Assert.Equal(1, network.InvariantViolations);
    }

    [Fact]
    public void CorruptedDatagramEvents_AreCountedAsInvariantViolations_NotDelivered()
    {
        using SimHarness h = new();
        // Events naming a datagram record that does not exist: the checks must catch them in every build.
        SimEvent sent = new() { Kind = SimEventKind.DatagramSent, Due = h.Clock.NowMicros, Target = h.A, I0 = 5, G0 = 1 };
        SimEvent final = new() { Kind = SimEventKind.DatagramFinal, Due = h.Clock.NowMicros, Target = h.A, I0 = 5, G0 = 1, B0 = (byte)DatagramSendState.Acknowledged };
        SimEvent arrive = new() { Kind = SimEventKind.DatagramArrive, Due = h.Clock.NowMicros, Target = h.B, I0 = 5, G0 = 1, Obj = h.A };
        h.Network.Schedule(ref sent);
        h.Network.Schedule(ref final);
        h.Network.Schedule(ref arrive);
        Assert.Equal(3, h.Network.Advance(0));
        Assert.Equal(3, h.Network.InvariantViolations);
        Assert.Equal(0, h.SinkA.Count);
        Assert.Equal(0, h.SinkB.Count);
        h.Network.InvariantViolations = 0; // expected here; the harness asserts none are left
    }

    [Fact]
    public void DropNextDatagrams_LosesExactlyTheNextDepartures()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000 });
        Assert.Throws<ArgumentOutOfRangeException>(() => h.A.DropNextDatagrams(-1));
        h.A.DropNextDatagrams(1);
        h.A.DropNextDatagrams(1); // accumulates
        for (ulong i = 0; i < 4; i++)
            Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, [(byte)i], i));
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.LostDiscarded], States(h.SinkA, 0));
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.LostDiscarded], States(h.SinkA, 1));
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Acknowledged], States(h.SinkA, 2));
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Acknowledged], States(h.SinkA, 3));
        Assert.Equal([2, 3], h.SinkB.OfKind(RecordedEventKind.DatagramReceived).Select(e => (int)e.Data[0]).ToArray());
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(2, stats.DatagramsLost);
    }

    [Fact]
    public void LoseNextStreamPackets_DelaysThosePacketsByARetransmission_DataStaysInOrder()
    {
        LinkOptions options = new() { DelayMicros = 1_000, MaxDatagramPayload = 1000 };
        using SimHarness h = new(options);
        Assert.Throws<ArgumentOutOfRangeException>(() => h.A.LoseNextStreamPackets(-1));
        TransportStreamId id = h.OpenAndStart(h.A);
        h.A.LoseNextStreamPackets(1);
        byte[] data = Sim.Pattern(3000);
        long t0 = h.Clock.NowMicros;
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, data, 1));
        h.Network.RunUntilIdle(1_000_000);
        // Packets 2 and 3 wait behind the retransmitted first one, then all three are indicated together.
        RecordedEvent receive = Assert.Single(h.SinkB.OfKind(RecordedEventKind.StreamReceived));
        Assert.Equal(t0 + 1_000 + options.RetransmitDelayMicros, receive.TimeMicros);
        Assert.Equal(3, receive.SegmentCount);
        Assert.Equal(data, h.SinkB.GetStreamData(receive.StreamId));
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(1, stats.StreamRetransmissions);
    }

    // ------------------------------------------------------------------ close delivers what is already on the wire

    [Fact]
    public void Close_DataAlreadyOnTheWire_StillReachesThePeerBeforeItsClose()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "goodbye"u8, 1, TransportSendFlags.Fin));
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, "dg"u8, 2));
        h.A.Close(9, default);
        h.Network.RunUntilIdle(1_000_000);

        // A completes both as canceled: neither was acknowledged before the close.
        Assert.True(Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted)).Canceled);
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Canceled], States(h.SinkA, 2));
        // B still received both, then the close.
        TransportStreamId peer = h.SinkB.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        Assert.Equal("goodbye"u8.ToArray(), h.SinkB.GetStreamData(peer));
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.StreamPeerSendShutdown));
        Assert.Equal("dg"u8.ToArray(), Assert.Single(h.SinkB.OfKind(RecordedEventKind.DatagramReceived)).Data);
        RecordedEvent closed = h.SinkB.Events[^1];
        Assert.Equal(RecordedEventKind.Closed, closed.Kind);
        Assert.Equal(TransportCloseReason.Peer, closed.CloseReason);
        Assert.Equal(9UL, closed.ErrorCode);

        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(7, stats.StreamBytesDelivered);
        Assert.Equal(1, stats.DatagramsDelivered);
        h.A.GetStatistics(out TransportStatistics statistics);
        Assert.Equal(0UL, statistics.BytesInFlight); // the kept payloads were released when B closed
    }

    [Fact]
    public void Close_PacketArrivingAfterTheCloseFrame_IsDropped_AndItsPayloadReleased()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        h.A.LoseNextStreamPackets(1); // arrives at 10 ms + one retransmission (31 ms), after the close frame (10 ms)
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "late"u8, 1));
        h.A.Close(0, default);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Empty(h.SinkB.OfKind(RecordedEventKind.StreamReceived));
        Assert.Equal(TransportCloseReason.Peer, h.SinkB.Events[^1].CloseReason);
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(0, stats.StreamBytesDelivered);
        h.A.GetStatistics(out TransportStatistics statistics);
        Assert.Equal(0UL, statistics.BytesInFlight);
    }

    [Fact]
    public void Close_QueuedBehindABandwidthLimit_IsLost()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000, BandwidthBitsPerSecond = 1_000_000, MaxDatagramPayload = 1000 });
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[1000], 1)); // being serialized (8 ms)
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[1000], 2)); // queued
        h.A.Close(0, default);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(0, h.SinkB.CountOf(RecordedEventKind.DatagramReceived));
        h.A.GetStatistics(out TransportStatistics statistics);
        Assert.Equal(0UL, statistics.BytesInFlight);
    }

    [Fact]
    public void Disconnect_LosesEverythingInFlight()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000, DisconnectAtMicros = 25_000 }, connect: false);
        h.Network.AdvanceTo(20_000);
        Assert.Equal(TransportState.Connected, h.A.State);
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, "x"u8, 1)); // would arrive at 30 ms
        Assert.True(h.Network.RunUntilIdle(1_000_000));
        Assert.Equal(0, h.SinkB.CountOf(RecordedEventKind.DatagramReceived));
        Assert.Equal(TransportCloseReason.Transport, h.SinkB.Events[^1].CloseReason);
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Canceled], States(h.SinkA, 1));
        h.A.GetStatistics(out TransportStatistics statistics);
        Assert.Equal(0UL, statistics.BytesInFlight);
    }

    /// <summary>Logs capability and connect callbacks into one shared list, so ordering across ends can be checked.</summary>
    private sealed class OrderSink(string name, List<string> log) : ITransportSink
    {
        public Action? OnCapability;

        public void OnConnected(in TransportConnectedInfo info) => log.Add(name + ":Connected");
        public void OnDatagramCapabilityChanged(bool enabled, int maxPayload)
        {
            log.Add(name + ":Capability");
            OnCapability?.Invoke();
        }
        public void OnDatagramReceived(ReadOnlySpan<byte> payload) { }
        public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) { }
        public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status) { }
        public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin) =>
            ReceiveResult.Consumed(Total(segments));
        public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) { }
        public void OnDatagramSendStateChanged(ulong context, DatagramSendState state) { }
        public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) { }
        public void OnStreamPeerSendShutdown(TransportStreamId id) { }
        public void OnStreamShutdownComplete(TransportStreamId id) { }
        public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes) { }
        public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional) { }
        public void OnPeerAddressChanged(in TransportConnectedInfo info) { }
        public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus) { }
    }
}
