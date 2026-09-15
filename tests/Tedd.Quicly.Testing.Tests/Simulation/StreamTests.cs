using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Simulation;

public class StreamTests
{
    private static int Total(ReadOnlySpan<TransportSegment> segments)
    {
        int total = 0;
        foreach (TransportSegment segment in segments)
            total += (int)segment.Length;
        return total;
    }

    private static TransportStreamId PeerId(RecordingSink sink, int index = 0) => sink.OfKind(RecordedEventKind.PeerStreamStarted)[index].StreamId;

    [Fact]
    public void OpenAndStart_RaisesStartedLocally_AndPeerStartedAfterOneWayDelay()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        long t0 = h.Clock.NowMicros;
        Assert.Equal(TransportStatus.Success, h.A.OpenStream(StreamKind.Bidirectional, 7, 100, out TransportStreamId id));
        Assert.True(id.IsValid);
        Assert.Equal(-1, h.A.GetQuicStreamId(id));
        Assert.Equal(TransportStatus.Success, h.A.StartStream(id));
        Assert.Equal(0, h.A.GetQuicStreamId(id));
        Assert.Equal(TransportStatus.InvalidState, h.A.StartStream(id));
        Assert.Equal(0, h.SinkA.Count);

        h.Network.Advance(0);
        RecordedEvent started = Assert.Single(h.SinkA.Events);
        Assert.Equal(RecordedEventKind.StreamStarted, started.Kind);
        Assert.Equal(id, started.StreamId);
        Assert.Equal(7UL, started.Context);
        Assert.Equal(TransportStatus.Success, started.Status);

        h.Network.Advance(9_999);
        Assert.Equal(0, h.SinkB.Count);
        h.Network.Advance(1);
        RecordedEvent peer = Assert.Single(h.SinkB.Events);
        Assert.Equal(RecordedEventKind.PeerStreamStarted, peer.Kind);
        Assert.Equal(StreamKind.Bidirectional, peer.StreamKind);
        Assert.Equal(t0 + 10_000, peer.TimeMicros);
        Assert.Equal(0, h.B.GetQuicStreamId(peer.StreamId));
        Assert.Equal(TransportStatus.InvalidState, h.B.StartStream(peer.StreamId));

        TransportStreamId serverStream = h.OpenAndStart(h.B);
        Assert.Equal(1, h.B.GetQuicStreamId(serverStream));
    }

    [Fact]
    public void PeerStreamLimits_AreEnforced_AndUpdateRaisesStreamsAvailable()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 5_000 });
        Assert.Equal(TransportStatus.Success, h.A.OpenStream(StreamKind.Bidirectional, 1, 0, out TransportStreamId s1));
        Assert.Equal(TransportStatus.Success, h.A.OpenStream(StreamKind.Bidirectional, 2, 0, out TransportStreamId s2));
        Assert.Equal(TransportStatus.Success, h.A.OpenStream(StreamKind.Unidirectional, 3, 0, out TransportStreamId u1));
        Assert.Equal(TransportStatus.Success, h.A.StartStream(s1));
        Assert.Equal(TransportStatus.StreamLimitReached, h.A.StartStream(s2));
        Assert.Equal(TransportStatus.StreamLimitReached, Sim.SendStream(h.A, s2, "x"u8, 1, TransportSendFlags.Start));
        Assert.Equal(TransportStatus.StreamLimitReached, h.A.StartStream(u1));

        h.B.UpdatePeerStreamLimits(3, 1);
        h.Network.Advance(4_999);
        Assert.Empty(h.SinkA.OfKind(RecordedEventKind.StreamsAvailable));
        h.Network.Advance(1);
        RecordedEvent available = Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamsAvailable));
        Assert.Equal(2, available.Bidirectional);
        Assert.Equal(1, available.Unidirectional);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, s2, "x"u8, 1, TransportSendFlags.Start));
        Assert.Equal(TransportStatus.Success, h.A.StartStream(u1));
    }

    [Fact]
    public void StreamCredit_ReturnsToTheOpener_WhenThePeerSideShutsDown()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000 });
        TransportStreamId s1 = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, s1, "x"u8, 1, TransportSendFlags.Fin));
        h.Network.Advance(1_000);
        TransportStreamId peer = PeerId(h.SinkB);
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.StreamPeerSendShutdown));
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.B, peer, [], 2, TransportSendFlags.Fin));
        Assert.Equal(TransportStatus.Success, h.A.OpenStream(StreamKind.Bidirectional, 9, 0, out TransportStreamId s2));
        Assert.Equal(TransportStatus.StreamLimitReached, h.A.StartStream(s2));

        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(s1, Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamShutdownComplete)).StreamId);
        Assert.Equal(peer, Assert.Single(h.SinkB.OfKind(RecordedEventKind.StreamShutdownComplete)).StreamId);
        RecordedEvent available = Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamsAvailable));
        Assert.Equal(1, available.Bidirectional);
        Assert.Equal(TransportStatus.Success, h.A.StartStream(s2));
    }

    [Fact]
    public void SendStream_DeliversInOrder_OneSegmentPerPacket_AndCompletesAfterDeliveryPlusDelay()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000, MaxDatagramPayload = 1000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        byte[] data = Sim.Pattern(2500);
        long t0 = h.Clock.NowMicros;
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, data, 5));
        h.Network.RunUntilIdle(1_000_000);

        IReadOnlyList<RecordedEvent> receives = h.SinkB.OfKind(RecordedEventKind.StreamReceived);
        Assert.Equal(3, receives.Count);
        Assert.Equal([0UL, 1000UL, 2000UL], receives.Select(e => e.Offset).ToArray());
        Assert.Equal([1000, 1000, 500], receives.Select(e => e.Data.Length).ToArray());
        Assert.All(receives, e => Assert.Equal(1, e.SegmentCount));
        Assert.All(receives, e => Assert.False(e.Fin));
        Assert.Equal(data, h.SinkB.GetStreamData(receives[0].StreamId));

        RecordedEvent completed = Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted));
        Assert.Equal(5UL, completed.Context);
        Assert.False(completed.Canceled);
        Assert.Equal(t0 + 2_000, completed.TimeMicros);

        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(2500, stats.StreamBytesSent);
        Assert.Equal(2500, stats.StreamBytesDelivered);
        Assert.Equal(3, stats.StreamPacketsSent);
        h.A.GetStatistics(out TransportStatistics transport);
        Assert.Equal(0UL, transport.BytesInFlight);
        Assert.Equal(3UL, transport.SendTotalPackets);
    }

    [Fact]
    public void SendStream_GathersSegments()
    {
        using SimHarness h = new();
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStreamParts(h.A, id, [1, 2], [3, 4, 5], 1, TransportSendFlags.Fin));
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, h.SinkB.GetStreamData(PeerId(h.SinkB)));
    }

    [Fact]
    public void SendStream_ZeroLengthWithoutFin_CompletesWithoutReceive()
    {
        using SimHarness h = new();
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, [], 1));
        h.Network.RunUntilIdle(1_000_000);
        Assert.False(Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted)).Canceled);
        Assert.Empty(h.SinkB.OfKind(RecordedEventKind.StreamReceived));
    }

    [Fact]
    public void PendingAfter_HoldsBackBytesUntilResume_WithoutLossOrDuplication()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000 });
        int calls = 0;
        h.SinkB.ReceiveHandler = (_, segments, _, _) => ++calls == 1 ? ReceiveResult.PendingAfter(100) : ReceiveResult.Consumed(Total(segments));
        TransportStreamId id = h.OpenAndStart(h.A);
        byte[] full = Sim.Pattern(5 * 3000);
        for (int i = 0; i < 5; i++)
            Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, full.AsSpan(i * 3000, 3000), (ulong)i));
        h.Network.RunUntilIdle(1_000_000);

        Assert.Equal(1, calls);
        TransportStreamId peer = PeerId(h.SinkB);
        Assert.Equal(full[..100], h.SinkB.GetStreamData(peer));
        Assert.Equal(5, h.SinkA.CountOf(RecordedEventKind.StreamSendCompleted)); // transport-level acknowledgement

        h.B.ResumeStreamReceive(peer, 0);
        Assert.Equal(1, calls); // never inline
        h.Network.Advance(0);
        Assert.Equal(2, calls);
        RecordedEvent second = h.SinkB.OfKind(RecordedEventKind.StreamReceived)[1];
        Assert.Equal(100UL, second.Offset);
        Assert.Equal(full.Length - 100, second.Data.Length);
        Assert.Equal(15, second.SegmentCount);
        Assert.Equal(full, h.SinkB.GetStreamData(peer));
    }

    [Fact]
    public void ResumeStreamReceive_CreditsBytesConsumedOutOfBand()
    {
        using SimHarness h = new();
        int calls = 0;
        h.SinkB.ReceiveHandler = (_, segments, _, _) => ++calls == 1 ? ReceiveResult.PendingAfter(100) : ReceiveResult.Consumed(Total(segments));
        TransportStreamId id = h.OpenAndStart(h.A);
        byte[] full = Sim.Pattern(1000);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, full, 1));
        h.Network.RunUntilIdle(1_000_000);
        TransportStreamId peer = PeerId(h.SinkB);

        Assert.Throws<ArgumentOutOfRangeException>(() => h.B.ResumeStreamReceive(peer, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.B.ResumeStreamReceive(peer, 901));
        h.B.ResumeStreamReceive(peer, 50);
        h.B.ResumeStreamReceive(peer, 50); // not pending any more: ignored
        h.Network.RunUntilIdle(1_000_000);
        RecordedEvent second = h.SinkB.OfKind(RecordedEventKind.StreamReceived)[1];
        Assert.Equal(150UL, second.Offset);
        Assert.Equal([.. full[..100], .. full[150..]], h.SinkB.GetStreamData(peer));
    }

    [Fact]
    public void PartialConsume_WithoutPending_RedeliversRemainderWithTheNextData()
    {
        using SimHarness h = new(new LinkOptions { MaxDatagramPayload = 1000 });
        h.SinkB.ReceiveHandler = (_, segments, _, fin) => ReceiveResult.Consumed(fin ? Total(segments) : Math.Max(0, Total(segments) - 10));
        TransportStreamId id = h.OpenAndStart(h.A);
        byte[] full = Sim.Pattern(3000);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, full, 1, TransportSendFlags.Fin));
        h.Network.RunUntilIdle(1_000_000);
        IReadOnlyList<RecordedEvent> receives = h.SinkB.OfKind(RecordedEventKind.StreamReceived);
        Assert.Equal([0UL, 990UL, 1990UL], receives.Select(e => e.Offset).ToArray());
        Assert.Equal(2, receives[1].SegmentCount);
        Assert.True(receives[2].Fin);
        Assert.Equal(full, h.SinkB.GetStreamData(receives[0].StreamId));
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.StreamPeerSendShutdown));
    }

    [Fact]
    public void PendingOnFin_ResumeCompletesTheReceive()
    {
        using SimHarness h = new();
        int calls = 0;
        h.SinkB.ReceiveHandler = (_, segments, _, _) => ++calls == 1 ? ReceiveResult.PendingAfter(Total(segments)) : throw new InvalidOperationException("no second call expected");
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "abc"u8, 1, TransportSendFlags.Fin));
        h.Network.RunUntilIdle(1_000_000);
        TransportStreamId peer = PeerId(h.SinkB);
        Assert.Equal(0, h.SinkB.CountOf(RecordedEventKind.StreamPeerSendShutdown));
        h.B.ResumeStreamReceive(peer, 0);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(1, calls);
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.StreamPeerSendShutdown));
    }

    [Fact]
    public void PendingOnFin_PartiallyConsumed_RedeliversRemainderWithFin()
    {
        using SimHarness h = new();
        int calls = 0;
        h.SinkB.ReceiveHandler = (_, segments, _, _) => ++calls == 1 ? ReceiveResult.PendingAfter(1) : ReceiveResult.Consumed(Total(segments));
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "abc"u8, 1, TransportSendFlags.Fin));
        h.Network.RunUntilIdle(1_000_000);
        TransportStreamId peer = PeerId(h.SinkB);
        h.B.ResumeStreamReceive(peer, 0);
        h.Network.RunUntilIdle(1_000_000);
        RecordedEvent second = h.SinkB.OfKind(RecordedEventKind.StreamReceived)[1];
        Assert.Equal(1UL, second.Offset);
        Assert.True(second.Fin);
        Assert.Equal("abc"u8.ToArray(), h.SinkB.GetStreamData(peer));
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.StreamPeerSendShutdown));
    }

    [Fact]
    public void OnStreamReceived_ConsumingMoreThanDelivered_Throws()
    {
        using SimHarness h = new();
        h.SinkB.ReceiveHandler = (_, segments, _, _) => ReceiveResult.Consumed(Total(segments) + 1);
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "abc"u8, 1));
        Assert.Throws<InvalidOperationException>(() => h.Network.RunUntilIdle(1_000_000));
    }

    [Fact]
    public void Fin_BothDirections_ShutdownOrderingAndTiming()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        long t0 = h.Clock.NowMicros;
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "abc"u8, 1, TransportSendFlags.Fin));
        Assert.Equal(TransportStatus.InvalidState, Sim.SendStream(h.A, id, "more"u8, 9));
        h.Network.Advance(10_000);
        TransportStreamId peer = PeerId(h.SinkB);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.B, peer, "xyz"u8, 2, TransportSendFlags.Fin));
        h.Network.RunUntilIdle(1_000_000);

        Assert.Equal(
        [
            $"{t0} StreamStarted {id} ctx=7 Success",
            $"{t0 + 20_000} StreamSendCompleted {id} ctx=1 canceled=False",
            $"{t0 + 20_000} StreamReceived {id} off=0 len=3 segs=1 fin=True -> 3",
            $"{t0 + 20_000} StreamPeerSendShutdown {id}",
            $"{t0 + 20_000} StreamShutdownComplete {id}",
            $"{t0 + 40_000} StreamsAvailable bidi=1 uni=0",
        ], Sim.Describe(h.SinkA.Events));
        Assert.Equal(
        [
            $"{t0 + 10_000} PeerStreamStarted {peer} Bidirectional",
            $"{t0 + 10_000} StreamReceived {peer} off=0 len=3 segs=1 fin=True -> 3",
            $"{t0 + 10_000} StreamPeerSendShutdown {peer}",
            $"{t0 + 30_000} StreamSendCompleted {peer} ctx=2 canceled=False",
            $"{t0 + 30_000} StreamShutdownComplete {peer}",
        ], Sim.Describe(h.SinkB.Events));

        // The sink closed the stream: the id is stale now.
        Assert.Equal(TransportStatus.InvalidState, Sim.SendStream(h.A, id, "x"u8, 3));
        Assert.Equal(-1, h.A.GetQuicStreamId(id));
    }

    [Fact]
    public void EmptyFin_IsDeliveredAsZeroLengthReceive()
    {
        using SimHarness h = new();
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "ab"u8, 1));
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, [], 2, TransportSendFlags.Fin));
        h.Network.RunUntilIdle(1_000_000);
        IReadOnlyList<RecordedEvent> receives = h.SinkB.OfKind(RecordedEventKind.StreamReceived);
        Assert.Equal(2, receives.Count);
        Assert.False(receives[0].Fin);
        Assert.Empty(receives[1].Data);
        Assert.Equal(0, receives[1].SegmentCount);
        Assert.Equal(2UL, receives[1].Offset);
        Assert.True(receives[1].Fin);
    }

    [Fact]
    public void UnidirectionalStream_OnlyTheOpenerSends()
    {
        using SimHarness h = new(new LinkOptions { PeerUnidiStreams = 1 });
        TransportStreamId u = h.OpenAndStart(h.A, StreamKind.Unidirectional);
        Assert.Equal(2, h.A.GetQuicStreamId(u));
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, u, "uni"u8, 1, TransportSendFlags.Fin));
        h.Network.Advance(0);
        RecordedEvent peer = Assert.Single(h.SinkB.OfKind(RecordedEventKind.PeerStreamStarted));
        Assert.Equal(StreamKind.Unidirectional, peer.StreamKind);
        Assert.Equal(TransportStatus.InvalidState, Sim.SendStream(h.B, peer.StreamId, "no"u8, 1));
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal("uni"u8.ToArray(), h.SinkB.GetStreamData(peer.StreamId));
        Assert.Equal(1, h.SinkA.CountOf(RecordedEventKind.StreamShutdownComplete));
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.StreamShutdownComplete));
        RecordedEvent available = h.SinkA.OfKind(RecordedEventKind.StreamsAvailable)[^1];
        Assert.Equal(1, available.Unidirectional);

        // A local stream that never started is released by any abort, like CloseStream (ITransport.AbortStream contract):
        // no callback follows and the id is stale afterwards.
        Assert.Equal(TransportStatus.Success, h.A.OpenStream(StreamKind.Unidirectional, 5, 0, out TransportStreamId idle));
        h.A.AbortStream(idle, 1, StreamAbortDirection.Receive);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(1, h.SinkA.CountOf(RecordedEventKind.StreamShutdownComplete));
        Assert.Equal(TransportStatus.InvalidState, h.A.StartStream(idle));
        h.A.AbortStream(idle, 1, StreamAbortDirection.Send);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(1, h.SinkA.CountOf(RecordedEventKind.StreamShutdownComplete));
        Assert.Equal(TransportStatus.Success, h.A.OpenStream(StreamKind.Unidirectional, 6, 0, out TransportStreamId reused));
        Assert.Equal(idle.Slot, reused.Slot);
        Assert.NotEqual(idle.Generation, reused.Generation);
    }

    [Fact]
    public void AbortSend_CancelsPendingSends_AndPeerSeesReset()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, Sim.Pattern(3000), 1));
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, Sim.Pattern(10), 2));
        h.A.AbortStream(id, 77, StreamAbortDirection.Send);
        Assert.Equal(TransportStatus.InvalidState, Sim.SendStream(h.A, id, "x"u8, 3));
        h.Network.Advance(0);
        IReadOnlyList<RecordedEvent> completions = h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted);
        Assert.Equal([1UL, 2UL], completions.Select(e => e.Context).ToArray());
        Assert.All(completions, e => Assert.True(e.Canceled));

        h.Network.Advance(10_000);
        TransportStreamId peer = PeerId(h.SinkB);
        RecordedEvent aborted = Assert.Single(h.SinkB.OfKind(RecordedEventKind.StreamAborted));
        Assert.Equal(77UL, aborted.ErrorCode);
        Assert.Equal(StreamAbortDirection.Send, aborted.Direction);
        Assert.Empty(h.SinkB.OfKind(RecordedEventKind.StreamReceived));

        h.B.AbortStream(peer, 5, StreamAbortDirection.Send);
        h.Network.RunUntilIdle(1_000_000);
        RecordedEvent back = Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamAborted));
        Assert.Equal(5UL, back.ErrorCode);
        Assert.Equal(StreamAbortDirection.Send, back.Direction);
        Assert.Equal(1, h.SinkA.CountOf(RecordedEventKind.StreamShutdownComplete));
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.StreamShutdownComplete));
    }

    [Fact]
    public void AbortReceive_StopSending_CancelsThePeersPendingSends()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        long t0 = h.Clock.NowMicros;
        TransportStreamId id = h.OpenAndStart(h.A);
        h.Network.Advance(10_000);
        TransportStreamId peer = PeerId(h.SinkB);
        h.B.AbortStream(peer, 9, StreamAbortDirection.Receive);
        h.Network.Advance(5_000);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "late"u8, 4));
        h.Network.RunUntilIdle(1_000_000);

        Assert.Equal(
        [
            $"{t0} StreamStarted {id} ctx=7 Success",
            $"{t0 + 20_000} StreamAborted {id} code=9 Receive",
            $"{t0 + 20_000} StreamSendCompleted {id} ctx=4 canceled=True",
        ], Sim.Describe(h.SinkA.Events));
        Assert.Empty(h.SinkB.OfKind(RecordedEventKind.StreamReceived));
        Assert.Equal(TransportStatus.InvalidState, Sim.SendStream(h.A, id, "x"u8, 5));

        h.A.AbortStream(id, 1, StreamAbortDirection.Receive);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(1, h.SinkA.CountOf(RecordedEventKind.StreamShutdownComplete));
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.StreamShutdownComplete));
        Assert.Equal(StreamAbortDirection.Receive, Assert.Single(h.SinkB.OfKind(RecordedEventKind.StreamAborted)).Direction);
    }

    [Fact]
    public void CrossingAborts_LateResetAndStopSendingAreIgnored()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        h.Network.Advance(10_000);
        TransportStreamId peer = PeerId(h.SinkB);
        h.B.AbortStream(peer, 1, StreamAbortDirection.Receive);
        h.A.AbortStream(id, 2, StreamAbortDirection.Send);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Empty(h.SinkA.OfKind(RecordedEventKind.StreamAborted));
        Assert.Empty(h.SinkB.OfKind(RecordedEventKind.StreamAborted));
    }

    [Fact]
    public void AbortAfterDeliveryButBeforeCompletion_ReportsOneCanceledCompletion()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "abc"u8, 1));
        h.Network.Advance(10_000); // delivered; completion due in 10 ms
        h.Clock.AdvanceMicros(20_000); // time passes without the network running
        h.A.AbortStream(id, 3, StreamAbortDirection.Send);
        h.Network.RunUntilIdle(1_000_000);
        RecordedEvent completion = Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted));
        Assert.True(completion.Canceled);
    }

    [Fact]
    public void CloseStream_BeforeShutdown_AbortsBothDirections_AndSuppressesTheShutdownCallback()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "data"u8, 3));
        h.A.CloseStream(id);
        Assert.Equal(TransportStatus.InvalidState, Sim.SendStream(h.A, id, "x"u8, 1));
        Assert.Equal(TransportStatus.InvalidState, h.A.StartStream(id));
        h.A.CloseStream(id); // already closing: ignored
        h.Network.RunUntilIdle(1_000_000);

        Assert.Equal(RecordedEventKind.StreamSendCompleted, h.SinkA.Events[0].Kind);
        Assert.True(h.SinkA.Events[0].Canceled);
        Assert.Empty(h.SinkA.OfKind(RecordedEventKind.StreamStarted));
        Assert.Empty(h.SinkA.OfKind(RecordedEventKind.StreamShutdownComplete));
        Assert.Equal(1, h.SinkA.CountOf(RecordedEventKind.StreamsAvailable));
        IReadOnlyList<RecordedEvent> aborts = h.SinkB.OfKind(RecordedEventKind.StreamAborted);
        Assert.Equal([StreamAbortDirection.Send, StreamAbortDirection.Receive], aborts.Select(e => e.Direction).ToArray());
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.StreamShutdownComplete));
        Assert.Empty(h.SinkB.OfKind(RecordedEventKind.StreamReceived));

        TransportStreamId next = h.OpenAndStart(h.A);
        Assert.Equal(id.Slot, next.Slot);
        Assert.NotEqual(id.Generation, next.Generation);

        // An opened, never started stream is released silently.
        Assert.Equal(TransportStatus.Success, h.A.OpenStream(StreamKind.Bidirectional, 1, 0, out TransportStreamId idle));
        h.A.CloseStream(idle);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Empty(h.SinkA.OfKind(RecordedEventKind.StreamShutdownComplete));
    }

    [Fact]
    public void CloseStream_AfterShutdown_ReleasesTheSlotAndBumpsTheGeneration()
    {
        using SimHarness h = new();
        h.SinkA.AutoCloseStreams = false;
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "a"u8, 1, TransportSendFlags.Fin));
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.B, PeerId(h.SinkB), "b"u8, 2, TransportSendFlags.Fin));
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(id, Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamShutdownComplete)).StreamId);
        Assert.Equal(0, h.A.GetQuicStreamId(id)); // not closed yet
        h.A.CloseStream(id);
        Assert.Equal(-1, h.A.GetQuicStreamId(id));
        Assert.Equal(TransportStatus.Success, h.A.OpenStream(StreamKind.Bidirectional, 1, 0, out TransportStreamId next));
        Assert.Equal(id.Slot, next.Slot);
        Assert.Equal(id.Generation + 1, next.Generation);
    }

    [Fact]
    public void Priority_UnderBandwidthLimit_ServesHigherPriorityFirst()
    {
        using SimHarness h = new(new LinkOptions { BandwidthBitsPerSecond = 1_000_000, PeerBidiStreams = 3, MaxDatagramPayload = 1000 });
        TransportStreamId low = h.OpenAndStart(h.A);
        TransportStreamId high = h.OpenAndStart(h.A);
        TransportStreamId flagged = h.OpenAndStart(h.A);
        h.A.SetStreamPriority(low, 100);
        h.A.SetStreamPriority(high, 60_000);
        h.A.SetStreamPriority(new TransportStreamId(999, 1), 1); // unknown: ignored
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, low, Sim.Pattern(5000), 1));
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, high, Sim.Pattern(5000, 1), 2));
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, flagged, Sim.Pattern(5000, 2), 3, TransportSendFlags.Priority));
        h.Network.RunUntilIdle(10_000_000);
        Assert.Equal([3UL, 2UL, 1UL], h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted).Select(e => e.Context).ToArray());
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(15, stats.StreamPacketsSent);
        Assert.True(stats.MaxQueuedBytes >= 14_000);
    }

    [Fact]
    public void StreamLoss_IsModelledAsRetransmissionDelay_NeverAsMissingData()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 5_000, JitterMicros = 3_000, StreamLossPercent = 30 }, seed: 99);
        TransportStreamId id = h.OpenAndStart(h.A);
        byte[] full = Sim.Pattern(600_000);
        for (int i = 0; i < 20; i++)
        {
            TransportSendFlags flags = i == 19 ? TransportSendFlags.Fin : TransportSendFlags.None;
            Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, full.AsSpan(i * 30_000, 30_000), (ulong)i, flags));
        }
        h.Network.RunUntilIdle(100_000_000);
        Assert.Equal(full, h.SinkB.GetStreamData(PeerId(h.SinkB)));
        IReadOnlyList<RecordedEvent> completions = h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted);
        Assert.Equal(Enumerable.Range(0, 20).Select(i => (ulong)i).ToArray(), completions.Select(e => e.Context).ToArray());
        Assert.All(completions, e => Assert.False(e.Canceled));
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(500, stats.StreamPacketsSent);
        Assert.Equal(600_000, stats.StreamBytesDelivered);
        Assert.InRange(stats.StreamRetransmissions, 160, 270); // mean 500 * 0.3 / 0.7 = 214
        h.A.GetStatistics(out TransportStatistics transport);
        Assert.Equal((ulong)stats.StreamRetransmissions, transport.SendSuspectedLostPackets);
    }

    [Fact]
    public void StreamLoss_OfOneHundredPercent_StillDeliversAfterBoundedRetries()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000, StreamLossPercent = 100 });
        TransportStreamId id = h.OpenAndStart(h.A);
        long t0 = h.Clock.NowMicros;
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "abc"u8, 1));
        h.Network.RunUntilIdle(100_000_000);
        RecordedEvent received = Assert.Single(h.SinkB.OfKind(RecordedEventKind.StreamReceived));
        Assert.Equal(t0 + 1_000 + 32 * new LinkOptions { DelayMicros = 1_000 }.RetransmitDelayMicros, received.TimeMicros);
    }

    [Fact]
    public void InvalidIdsAndStates_AreRejectedOrIgnored()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000 }, connect: false);
        Assert.Equal(TransportStatus.Success, h.A.OpenStream(StreamKind.Bidirectional, 1, 0, out TransportStreamId early));
        Assert.Equal(TransportStatus.InvalidState, h.A.StartStream(early));
        Assert.Equal(TransportStatus.InvalidState, Sim.SendStream(h.A, early, "x"u8, 1, TransportSendFlags.Start));
        Assert.Equal(TransportStatus.NotSupported, h.A.OpenStream((StreamKind)9, 1, 0, out TransportStreamId none));
        Assert.False(none.IsValid);
        h.Network.RunUntilIdle(1_000_000);

        Assert.Equal(TransportStatus.InvalidState, Sim.SendStream(h.A, early, "x"u8, 1)); // not started, no Start flag
        foreach (TransportStreamId bogus in new[] { TransportStreamId.None, new TransportStreamId(99, 1), new TransportStreamId(early.Slot, early.Generation + 1) })
        {
            Assert.Equal(TransportStatus.InvalidState, h.A.StartStream(bogus));
            Assert.Equal(TransportStatus.InvalidState, Sim.SendStream(h.A, bogus, "x"u8, 1, TransportSendFlags.Start));
            Assert.Equal(-1, h.A.GetQuicStreamId(bogus));
            h.A.AbortStream(bogus, 0, StreamAbortDirection.Both);
            h.A.SetStreamPriority(bogus, 1);
            h.A.ResumeStreamReceive(bogus, 0);
            h.A.CloseStream(bogus);
        }
        Assert.Throws<ArgumentOutOfRangeException>(() => SendNegative(h.A, early));

        h.A.Close(0, default);
        h.A.UpdatePeerStreamLimits(5, 5);
        h.A.AbortStream(early, 0, StreamAbortDirection.Both);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(TransportStatus.InvalidState, h.A.OpenStream(StreamKind.Bidirectional, 1, 0, out _));
        Assert.Equal(TransportStatus.InvalidState, h.A.StartStream(early));
        h.A.CloseStream(early); // released after connection close
        Assert.Empty(h.SinkB.OfKind(RecordedEventKind.StreamsAvailable));
    }

    private static unsafe bool SendNegative(SimulatedTransport transport, TransportStreamId id) =>
        transport.SendStream(id, null, -1, 0, TransportSendFlags.None) == TransportStatus.Success;

    [Fact]
    public void Close_WithAppClosedStream_ReleasesItSilently()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "x"u8, 1)); // keeps the stream from shutting down first
        h.A.CloseStream(id);
        h.A.Close(0, default);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Empty(h.SinkA.OfKind(RecordedEventKind.StreamShutdownComplete));
        Assert.Equal([RecordedEventKind.StreamSendCompleted, RecordedEventKind.Closed], h.SinkA.Events.Select(e => e.Kind).ToArray());
        Assert.True(h.SinkA.Events[0].Canceled);
    }

    [Fact]
    public unsafe void SendStream_LongerThanIntMaxValue_IsTooLarge()
    {
        using SimHarness h = new();
        TransportStreamId id = h.OpenAndStart(h.A);
        byte b = 0;
        TransportSegment* segments = stackalloc TransportSegment[2];
        segments[0] = new TransportSegment(&b, int.MaxValue); // never read: the size check comes first
        segments[1] = new TransportSegment(&b, 1);
        Assert.Equal(TransportStatus.TooLarge, h.A.SendStream(id, segments, 2, 0, TransportSendFlags.None));
    }

    [Fact]
    public void AbortSend_UnderBandwidthLimit_DropsQueuedPackets()
    {
        using SimHarness h = new(new LinkOptions { BandwidthBitsPerSecond = 1_000_000, MaxDatagramPayload = 1000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, Sim.Pattern(10_000), 1));
        h.Network.Advance(8_000); // first packet serialized (8 ms per 1000 bytes)
        h.A.AbortStream(id, 4, StreamAbortDirection.Send);
        h.Network.RunUntilIdle(1_000_000);
        Assert.True(Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted)).Canceled);
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(1, stats.StreamPacketsSent);
        Assert.Equal(4UL, Assert.Single(h.SinkB.OfKind(RecordedEventKind.StreamAborted)).ErrorCode);
    }

    [Fact]
    public void DataArrivingAfterLocalAbortReceive_IsDropped()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        h.Network.Advance(12_000); // B learned about the stream at 10 ms
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "late"u8, 1)); // arrives at 22 ms
        h.Network.Advance(3_000);
        h.B.AbortStream(PeerId(h.SinkB), 6, StreamAbortDirection.Receive); // stop-sending reaches A at 25 ms
        h.Network.RunUntilIdle(1_000_000);
        Assert.Empty(h.SinkB.OfKind(RecordedEventKind.StreamReceived));
        Assert.True(Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted)).Canceled);
    }

    [Fact]
    public void AbortAfterDelivery_CanceledCompletionWins_AndTheLaterAcknowledgementIsIgnored()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "abc"u8, 1));
        h.Network.Advance(10_000); // delivered; acknowledgement due at 20 ms
        h.A.AbortStream(id, 3, StreamAbortDirection.Send);
        h.Network.RunUntilIdle(1_000_000);
        Assert.True(Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted)).Canceled);
        Assert.Equal("abc"u8.ToArray(), h.SinkB.GetStreamData(PeerId(h.SinkB)));
    }

    [Fact]
    public void SinkAbortingInsideOnStreamReceived_StopsDelivery()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000 });
        h.SinkB.ReceiveHandler = (id, _, _, _) =>
        {
            h.B.AbortStream(id, 2, StreamAbortDirection.Receive);
            return ReceiveResult.Consumed(0);
        };
        TransportStreamId id = h.OpenAndStart(h.A);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, "abc"u8, 1));
        h.Network.RunUntilIdle(1_000_000);
        Assert.Single(h.SinkB.OfKind(RecordedEventKind.StreamReceived));
        Assert.Equal(StreamAbortDirection.Receive, Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamAborted)).Direction);
    }

    [Fact]
    public void EventsForAReleasedStream_AreIgnored()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        TransportStreamId id = h.OpenAndStart(h.A);
        h.Network.Advance(10_000);
        TransportStreamId peer = PeerId(h.SinkB);
        h.A.AbortStream(id, 1, StreamAbortDirection.Both);
        h.B.AbortStream(peer, 2, StreamAbortDirection.Both);
        h.Network.RunUntilIdle(1_000_000);
        // Each side released its stream at once; the other side's reset and stop-sending find nothing.
        Assert.Empty(h.SinkA.OfKind(RecordedEventKind.StreamAborted));
        Assert.Empty(h.SinkB.OfKind(RecordedEventKind.StreamAborted));
        Assert.Equal(1, h.SinkA.CountOf(RecordedEventKind.StreamShutdownComplete));
        Assert.Equal(1, h.SinkB.CountOf(RecordedEventKind.StreamShutdownComplete));
    }

    [Fact]
    public void ManyStreams_GrowTheSlotAndSendTables()
    {
        using SimHarness h = new(new LinkOptions { PeerBidiStreams = 100 });
        List<TransportStreamId> ids = new();
        for (int i = 0; i < 40; i++)
        {
            TransportStreamId id = h.OpenAndStart(h.A);
            ids.Add(id);
            for (int j = 0; j < 3; j++)
                Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, id, Sim.Pattern(100, i), (ulong)(i * 10 + j), j == 2 ? TransportSendFlags.Fin : TransportSendFlags.None));
        }
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(120, h.SinkA.CountOf(RecordedEventKind.StreamSendCompleted));
        Assert.Equal(40, h.SinkB.CountOf(RecordedEventKind.StreamPeerSendShutdown));
    }
}
