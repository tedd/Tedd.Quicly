using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Simulation;

public unsafe class RecordingSinkTests
{
    private static void RaiseEveryCallback(ITransportSink sink)
    {
        TransportConnectedInfo info = default;
        info.RemoteEndPoint = new IPEndPoint(IPAddress.Loopback, 1);
        info.LocalEndPoint = new IPEndPoint(IPAddress.Loopback, 2);
        Span<byte> alpn = info.Alpn;
        "h3"u8.CopyTo(alpn);
        info.AlpnLength = 2;
        info.Capabilities.Datagrams = true;
        info.Capabilities.MaxDatagramPayload = 1200;
        TransportStreamId id = new(3, 1);
        sink.OnConnected(in info);
        sink.OnDatagramReceived([1, 2, 3]);
        sink.OnPeerStreamStarted(id, StreamKind.Unidirectional);
        sink.OnStreamStarted(id, 5, TransportStatus.Success);
        byte[] data = [9, 8, 7, 6];
        fixed (byte* p = data)
        {
            TransportSegment* segments = stackalloc TransportSegment[2];
            segments[0] = new TransportSegment(p, 3);
            segments[1] = new TransportSegment(p + 3, 1);
            sink.OnStreamReceived(id, new ReadOnlySpan<TransportSegment>(segments, 2), 10, true);
        }
        sink.OnStreamSendCompleted(id, 6, true);
        sink.OnDatagramSendStateChanged(7, DatagramSendState.Acknowledged);
        sink.OnStreamAborted(id, 8, StreamAbortDirection.Both);
        sink.OnStreamPeerSendShutdown(id);
        sink.OnStreamShutdownComplete(id);
        sink.OnDatagramCapabilityChanged(true, 999);
        sink.OnIdealSendBufferSize(id, 4096);
        sink.OnStreamsAvailable(2, 3);
        sink.OnPeerAddressChanged(in info);
        sink.OnClosed(TransportCloseReason.Peer, 11, 12);
    }

    [Fact]
    public void RecordsEveryCallback_WithReadableDescriptions()
    {
        VirtualClock clock = new(42);
        RecordingSink sink = new(clock);
        RaiseEveryCallback(sink);
        IReadOnlyList<RecordedEvent> events = sink.Events;
        Assert.Equal(Enum.GetValues<RecordedEventKind>().Length, events.Count);
        Assert.Equal(Enum.GetValues<RecordedEventKind>(), events.Select(e => e.Kind).ToArray());
        Assert.All(events, e => Assert.Equal(42, e.TimeMicros));
        Assert.All(events, e => Assert.StartsWith($"42 {e.Kind}", e.ToString(), StringComparison.Ordinal));

        RecordedEvent connected = events[0];
        Assert.Equal("h3", connected.Alpn);
        Assert.Contains("alpn=h3 dg=True max=1200", connected.ToString(), StringComparison.Ordinal);
        Assert.Equal(new byte[] { 1, 2, 3 }, events[1].Data);
        Assert.Contains("data=010203", events[1].ToString(), StringComparison.Ordinal);
        RecordedEvent received = events[4];
        Assert.Equal(new byte[] { 9, 8, 7, 6 }, received.Data);
        Assert.Equal(2, received.SegmentCount);
        Assert.Equal(ReceiveResult.Consumed(4), received.Result);
        Assert.Contains("off=10 len=4 segs=2 fin=True -> 4", received.ToString(), StringComparison.Ordinal);
        Assert.Equal(new byte[] { 9, 8, 7, 6 }, sink.GetStreamData(new TransportStreamId(3, 1)));
        Assert.Empty(sink.GetStreamData(new TransportStreamId(4, 1)));
        Assert.Equal(4096UL, events[11].Bytes);
        Assert.Equal(IPAddress.Loopback, events[13].RemoteEndPoint!.Address);
        Assert.Contains("Peer code=11 status=12", events[14].ToString(), StringComparison.Ordinal);
        Assert.True(sink.IsClosed);
        Assert.Equal(1, sink.CountOf(RecordedEventKind.Closed));

        sink.Clear();
        Assert.Equal(0, sink.Count);
        Assert.Empty(sink.GetStreamData(new TransportStreamId(3, 1)));
        Assert.False(sink.IsClosed);
    }

    [Fact]
    public void ForwardsToInnerSink_AndRecordsItsReceiveResult()
    {
        RecordingSink inner = new() { ReceiveHandler = (_, _, _, _) => ReceiveResult.PendingAfter(1) };
        RecordingSink outer = new(inner: inner) { Transport = null };
        RaiseEveryCallback(outer);
        Assert.Equal(outer.Count, inner.Count);
        RecordedEvent received = outer.OfKind(RecordedEventKind.StreamReceived)[0];
        Assert.Equal(ReceiveResult.PendingAfter(1), received.Result);
        Assert.Contains("-> 1 pending", received.ToString(), StringComparison.Ordinal);
        Assert.Equal(new byte[] { 9 }, outer.GetStreamData(new TransportStreamId(3, 1)));
        Assert.Equal(0, outer.OfKind(RecordedEventKind.Connected)[0].TimeMicros);
    }

    [Fact]
    public void ReceiveHandler_ResultIsClampedWhenRecordingConsumedBytes()
    {
        RecordingSink sink = new() { ReceiveHandler = (_, _, _, _) => ReceiveResult.Consumed(-5) };
        RaiseEveryCallback(sink);
        Assert.Empty(sink.GetStreamData(new TransportStreamId(3, 1)));
    }

    [Fact]
    public void WaitHelpers_ObserveEventsFromAnotherThread()
    {
        VirtualClock clock = new();
        using SimulatedNetwork network = new(clock, 1);
        RecordingSink sinkA = new(clock), sinkB = new(clock);
        (SimulatedTransport a, _) = network.CreatePair(sinkA, sinkB, new LinkOptions { DelayMicros = 1_000 });
        Thread driver = new(() =>
        {
            network.RunUntilIdle(1_000_000);
            Thread.Sleep(20);
            Sim.SendDatagram(a, "x"u8, 1);
            network.RunUntilIdle(1_000_000);
        });
        driver.Start();
        Assert.True(sinkB.WaitForCount(RecordedEventKind.DatagramReceived, 1, TimeSpan.FromSeconds(10)));
        Assert.True(sinkA.WaitFor(e => e.Kind == RecordedEventKind.DatagramSendStateChanged && e.DatagramState == DatagramSendState.Acknowledged, TimeSpan.FromSeconds(10)));
        driver.Join();
        Assert.True(sinkA.WaitForCount(RecordedEventKind.Connected, 0, TimeSpan.Zero));
        Assert.False(sinkA.WaitForCount(RecordedEventKind.Closed, 1, TimeSpan.FromMilliseconds(30)));
        Assert.Throws<ArgumentNullException>(() => sinkA.WaitFor(null!, TimeSpan.Zero));
    }
}
