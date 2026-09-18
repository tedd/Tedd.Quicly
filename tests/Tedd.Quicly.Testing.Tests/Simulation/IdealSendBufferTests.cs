using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Simulation;

/// <summary>
/// <see cref="LinkOptions.IdealSendBufferReporting"/>: the simulator tells each local stream its ideal send buffer the way
/// MsQuic does with send buffering disabled (<c>QUIC_STREAM_EVENT_IDEAL_SEND_BUFFER_SIZE</c>) — right after the stream
/// starts, and again whenever a new maximum of the bytes in flight grows the connection's ideal along 128 KiB × 1.5ⁿ.
/// Without it nothing drives a consumer's windowing the way MsQuic will.
/// </summary>
public class IdealSendBufferTests
{
    private const ulong Step0 = SimulatedTransport.DefaultIdealSendBufferBytes; // 131 072
    private const ulong Step1 = Step0 + (Step0 / 2);                           // 196 608
    private const ulong Step2 = Step1 + (Step1 / 2);                           // 294 912

    private static LinkOptions Reporting() => new() { DelayMicros = 1_000, PeerUnidiStreams = 4, IdealSendBufferReporting = true };

    [Fact]
    public void A_Started_Stream_Hears_The_Ideal_Right_After_Its_Start()
    {
        using SimHarness h = new(Reporting());
        Assert.True(h.A.Capabilities.IdealSendBufferSize);

        TransportStreamId stream = h.OpenAndStart(h.A, StreamKind.Unidirectional);
        h.Network.Advance(0);

        RecordedEvent ideal = Assert.Single(h.SinkA.OfKind(RecordedEventKind.IdealSendBufferSize));
        Assert.Equal(stream, ideal.StreamId);
        Assert.Equal(Step0, ideal.Bytes);

        // Ordered after OnStreamStarted, as MsQuic raises it once the start completed.
        IReadOnlyList<RecordedEvent> events = h.SinkA.Events;
        int started = -1;
        int told = -1;
        for (int i = 0; i < events.Count; i++)
        {
            if (events[i].Kind == RecordedEventKind.StreamStarted && started < 0)
                started = i;
            if (events[i].Kind == RecordedEventKind.IdealSendBufferSize && told < 0)
                told = i;
        }

        Assert.True(started >= 0 && told > started, string.Join(" | ", Sim.Describe(events)));
    }

    [Fact]
    public void The_Ideal_Grows_With_The_Bytes_In_Flight_And_Every_Stream_Still_Sending_Hears_It()
    {
        using SimHarness h = new(Reporting());
        TransportStreamId busy = h.OpenAndStart(h.A, StreamKind.Unidirectional, context: 1);
        TransportStreamId idle = h.OpenAndStart(h.A, StreamKind.Unidirectional, context: 2);
        TransportStreamId finished = h.OpenAndStart(h.A, StreamKind.Unidirectional, context: 3);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, finished, Sim.Pattern(100), 30, TransportSendFlags.Fin));
        Assert.True(h.Network.RunUntilIdle(1_000_000));
        h.SinkA.Clear();

        // 200 000 bytes in flight at once: the ideal steps over 131 072 and 196 608 to the first value above, 294 912.
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, busy, Sim.Pattern(200_000), 10));
        h.Network.Advance(0);

        IReadOnlyList<RecordedEvent> grown = h.SinkA.OfKind(RecordedEventKind.IdealSendBufferSize);
        Assert.Equal(2, grown.Count);
        Assert.All(grown, e => Assert.Equal(Step2, e.Bytes));
        Assert.Contains(grown, e => e.StreamId == busy);
        Assert.Contains(grown, e => e.StreamId == idle);

        // The stream that already sent its FIN is not told: it has nothing more to buffer.
        Assert.DoesNotContain(grown, e => e.StreamId == finished);

        // A stream that starts in the same step as the ideal grows again (300 000 in flight: past 294 912 to 442 368) has
        // two notices queued, its start's and the growth's. It hears the connection's current ideal once — never the
        // 128 KiB the connection started from, and never the same value twice.
        Assert.True(h.Network.RunUntilIdle(1_000_000));
        h.SinkA.Clear();
        TransportStreamId late = h.OpenAndStart(h.A, StreamKind.Unidirectional, context: 4);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, late, Sim.Pattern(300_000), 11));
        h.Network.Advance(0);
        RecordedEvent told = Assert.Single(h.SinkA.OfKind(RecordedEventKind.IdealSendBufferSize), e => e.StreamId == late);
        Assert.Equal(Step2 + (Step2 / 2), told.Bytes);
        Assert.Single(h.SinkA.OfKind(RecordedEventKind.IdealSendBufferSize), e => e.StreamId == busy);
    }

    [Fact]
    public void The_Ideal_Follows_Only_Bytes_The_Congestion_Window_Lets_Onto_The_Wire()
    {
        // 8 Mbit/s over a 2 ms round trip: a congestion window of 2 400 bytes (two packets). Queueing a megabyte behind it
        // puts that megabyte "in flight" for the simulator, but MsQuic's congestion control would have let only the window
        // onto the wire, so the ideal stays where it started.
        LinkOptions link = Reporting();
        link.BandwidthBitsPerSecond = 8_000_000;
        using SimHarness h = new(link);
        h.A.GetStatistics(out TransportStatistics statistics);
        Assert.True(statistics.CongestionWindowBytes < Step0);

        TransportStreamId stream = h.OpenAndStart(h.A, StreamKind.Unidirectional);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, stream, Sim.Pattern(1_000_000), 10));
        h.Network.Advance(5_000);

        RecordedEvent ideal = Assert.Single(h.SinkA.OfKind(RecordedEventKind.IdealSendBufferSize));
        Assert.Equal(Step0, ideal.Bytes);
    }

    [Fact]
    public void Without_Reporting_No_Ideal_Is_Raised()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 1_000, PeerUnidiStreams = 4 });
        Assert.False(h.A.Capabilities.IdealSendBufferSize);

        TransportStreamId stream = h.OpenAndStart(h.A, StreamKind.Unidirectional);
        Assert.Equal(TransportStatus.Success, Sim.SendStream(h.A, stream, Sim.Pattern(300_000), 10));
        Assert.True(h.Network.RunUntilIdle(1_000_000));

        Assert.Empty(h.SinkA.OfKind(RecordedEventKind.IdealSendBufferSize));
    }
}
