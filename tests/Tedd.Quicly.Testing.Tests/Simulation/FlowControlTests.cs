using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Simulation;

/// <summary>Stream flow control of the simulator (<see cref="LinkOptions.StreamReceiveWindowBytes"/>): QUIC's MAX_STREAM_DATA, per stream.</summary>
public class FlowControlTests
{
    private static int Total(ReadOnlySpan<TransportSegment> segments)
    {
        int total = 0;
        foreach (TransportSegment segment in segments)
            total += (int)segment.Length;
        return total;
    }

    [Fact]
    public void A_Receiver_That_Consumes_Nothing_Holds_The_Sender_To_One_Window()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000, StreamReceiveWindowBytes = 4_096, PeerUnidiStreams = 4 });
        bool hold = true;
        h.SinkB.ReceiveHandler = (TransportStreamId _, ReadOnlySpan<TransportSegment> segments, ulong _, bool _) =>
            hold ? ReceiveResult.PendingAfter(0) : ReceiveResult.Consumed(Total(segments));
        TransportStreamId stream = h.OpenAndStart(h.A, StreamKind.Unidirectional);
        byte[] data = Sim.Pattern(20_000);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, stream, data, 5));
        h.Network.Advance(50_000);

        // Three 1 200-byte packets fit the window; the send cannot complete while the rest is held back.
        Assert.Empty(h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted));
        h.A.GetLinkStatistics(out SimulatedLinkStatistics held);
        Assert.Equal(3_600, held.StreamBytesDelivered);

        hold = false;
        TransportStreamId peer = h.SinkB.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        h.B.ResumeStreamReceive(peer, 0);
        Assert.True(h.Network.RunUntilIdle(1_000_000));
        Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted));
        Assert.Equal(data, h.SinkB.GetStreamData(peer));
    }

    [Fact]
    public void Window_Updates_Take_A_One_Way_Delay_So_Throughput_Is_About_One_Window_Per_Round_Trip()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000, StreamReceiveWindowBytes = 12_000, PeerUnidiStreams = 4 });
        TransportStreamId stream = h.OpenAndStart(h.A, StreamKind.Unidirectional);
        byte[] data = Sim.Pattern(120_000, 3);
        long start = h.Clock.NowMicros;
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, stream, data, 9));
        Assert.True(h.Network.RunUntilIdle(10_000_000));
        long elapsed = h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted)[0].TimeMicros - start;

        // Beyond the first window every byte waits for an update from the receiver: at least nine round trips here.
        Assert.True(elapsed >= 9 * 20_000, $"{elapsed} µs");
        TransportStreamId peer = h.SinkB.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        Assert.Equal(data, h.SinkB.GetStreamData(peer));
    }

    [Fact]
    public void Held_Packets_Of_A_Reset_Stream_Never_Leave()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000, StreamReceiveWindowBytes = 2_400, PeerUnidiStreams = 4 });
        bool hold = true;
        h.SinkB.ReceiveHandler = (TransportStreamId _, ReadOnlySpan<TransportSegment> segments, ulong _, bool _) =>
            hold ? ReceiveResult.PendingAfter(0) : ReceiveResult.Consumed(Total(segments));
        TransportStreamId stream = h.OpenAndStart(h.A, StreamKind.Unidirectional);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, stream, Sim.Pattern(10_000), 5));
        h.Network.Advance(10_000);
        h.A.AbortStream(stream, 3, StreamAbortDirection.Send);
        hold = false;
        TransportStreamId peer = h.SinkB.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        h.B.ResumeStreamReceive(peer, 0);
        Assert.True(h.Network.RunUntilIdle(1_000_000));
        RecordedEvent completed = Assert.Single(h.SinkA.OfKind(RecordedEventKind.StreamSendCompleted));
        Assert.True(completed.Canceled);
        h.A.GetLinkStatistics(out SimulatedLinkStatistics statistics);
        Assert.Equal(2_400, statistics.StreamBytesDelivered);
    }
}
