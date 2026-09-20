using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Simulation;

public unsafe class DatagramTests
{
    private static DatagramSendState[] States(RecordingSink sink, ulong context)
    {
        List<DatagramSendState> states = new();
        foreach (RecordedEvent e in sink.OfKind(RecordedEventKind.DatagramSendStateChanged))
        {
            if (e.Context == context)
                states.Add(e.DatagramState);
        }
        return states.ToArray();
    }

    [Fact]
    public void Delivered_ReportsSentAtNextStep_ThenAcknowledgedAtRtt()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        long t0 = h.Clock.NowMicros;
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, "hello"u8, 5));
        Assert.Equal(0, h.SinkA.Count); // never inline
        h.Network.Advance(0);
        RecordedEvent sent = Assert.Single(h.SinkA.Events);
        Assert.Equal(DatagramSendState.Sent, sent.DatagramState);
        Assert.Equal(t0, sent.TimeMicros);

        h.Network.Advance(9_999);
        Assert.Equal(0, h.SinkB.Count);
        h.Network.Advance(1);
        RecordedEvent received = Assert.Single(h.SinkB.Events);
        Assert.Equal("hello"u8.ToArray(), received.Data);
        Assert.Equal(t0 + 10_000, received.TimeMicros);

        h.Network.Advance(10_000);
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Acknowledged], States(h.SinkA, 5));
        Assert.Equal(t0 + 20_000, h.SinkA.Events[^1].TimeMicros);

        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(1, stats.DatagramsSent);
        Assert.Equal(1, stats.DatagramsDelivered);
        Assert.Equal(5, stats.DatagramBytesSent);
        Assert.Equal(5, stats.DatagramBytesDelivered);
    }

    [Fact]
    public void Lost_ReportsLostDiscardedAtRtt_AndNothingArrives()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000, LossPercent = 100 });
        long t0 = h.Clock.NowMicros;
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, "x"u8, 1));
        Assert.True(h.Network.RunUntilIdle(1_000_000));
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.LostDiscarded], States(h.SinkA, 1));
        Assert.Equal(t0 + 20_000, h.SinkA.Events[^1].TimeMicros);
        Assert.Equal(0, h.SinkB.Count);
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(1, stats.DatagramsLost);
        h.A.GetStatistics(out TransportStatistics transport);
        Assert.Equal(1UL, transport.SendSuspectedLostPackets);
        Assert.Equal(0UL, transport.BytesInFlight);
    }

    [Fact]
    public void TooLarge_IsSynchronous_AndProducesNoCallback()
    {
        using SimHarness h = new();
        Assert.Equal(TransportStatus.TooLarge, Sim.SendDatagram(h.A, new byte[1201], 1));
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[1200], 2));
        h.Network.RunUntilIdle(1_000_000);
        Assert.Empty(States(h.SinkA, 1));
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Acknowledged], States(h.SinkA, 2));
    }

    [Fact]
    public void Gather_ConcatenatesSegments_AndEmptyDatagramIsAllowed()
    {
        using SimHarness h = new();
        byte[] first = [1, 2, 3];
        byte[] second = [4, 5];
        fixed (byte* p1 = first)
        fixed (byte* p2 = second)
        {
            TransportSegment* segments = stackalloc TransportSegment[3];
            segments[0] = new TransportSegment(p1, 3);
            segments[1] = new TransportSegment(null, 0);
            segments[2] = new TransportSegment(p2, 2);
            Assert.Equal(TransportStatus.Success, h.A.SendDatagram(segments, 3, 1, TransportSendFlags.None));
            Assert.Equal(TransportStatus.Success, h.A.SendDatagram(null, 0, 2, TransportSendFlags.None));
        }
        h.Network.RunUntilIdle(1_000_000);
        IReadOnlyList<RecordedEvent> received = h.SinkB.OfKind(RecordedEventKind.DatagramReceived);
        Assert.Equal(2, received.Count);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, received[0].Data);
        Assert.Empty(received[1].Data);
    }

    [Fact]
    public void InvalidArguments_Throw()
    {
        using SimHarness h = new();
        Assert.Throws<ArgumentOutOfRangeException>(() => h.A.SendDatagram(null, -1, 0, TransportSendFlags.None));
        Assert.Throws<ArgumentNullException>(() => h.A.SendDatagram(null, 1, 0, TransportSendFlags.None));
    }

    [Fact]
    public void NotConnected_OrDisabled_IsInvalidState()
    {
        using SimHarness notConnected = new(new LinkOptions { DelayMicros = 1_000 }, connect: false);
        Assert.Equal(TransportStatus.InvalidState, Sim.SendDatagram(notConnected.A, "x"u8, 1));

        using SimHarness disabled = new(new LinkOptions { DatagramsEnabled = false }, connect: false);
        disabled.Network.RunUntilIdle(1_000_000);
        RecordedEvent capability = disabled.SinkA.OfKind(RecordedEventKind.DatagramCapabilityChanged)[0];
        Assert.False(capability.Enabled);
        Assert.False(disabled.A.Capabilities.Datagrams);
        Assert.False(disabled.A.Capabilities.DatagramSendState);
        Assert.Equal(TransportStatus.InvalidState, Sim.SendDatagram(disabled.A, "x"u8, 1));
    }

    [Fact]
    public void ReportingDisabled_ReportsOnlySent_EvenWhenLost()
    {
        using SimHarness h = new(new LinkOptions { DatagramSendStateReporting = false, LossPercent = 50, DelayMicros = 1_000 }, seed: 3);
        Assert.False(h.A.Capabilities.DatagramSendState);
        for (ulong i = 0; i < 20; i++)
            Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, "x"u8, i));
        h.Network.RunUntilIdle(1_000_000);
        for (ulong i = 0; i < 20; i++)
            Assert.Equal([DatagramSendState.Sent], States(h.SinkA, i));
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.True(stats.DatagramsLost > 0);
        Assert.Equal(20 - stats.DatagramsLost, h.SinkB.CountOf(RecordedEventKind.DatagramReceived));
    }

    [Fact]
    public void ReportingDisabled_CanceledBeforeSent_IsReported()
    {
        LinkOptions options = new() { DatagramSendStateReporting = false, BandwidthBitsPerSecond = 8_000, MaxQueueBytes = 1000 };
        using SimHarness h = new(options);
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[800], 1));
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[800], 2)); // queued
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[800], 3, TransportSendFlags.CancelOnBlocked)); // queue full
        h.A.Close(0, default);
        h.Network.RunUntilIdle(10_000_000);
        Assert.Equal([DatagramSendState.Sent], States(h.SinkA, 1));
        Assert.Equal([DatagramSendState.Canceled], States(h.SinkA, 2));
        Assert.Equal([DatagramSendState.Canceled], States(h.SinkA, 3));
    }

    [Fact]
    public void MtuDrop_ReportsCapabilityOnBothEnds_AndCancelsQueuedDatagramsThatNoLongerFit()
    {
        LinkOptions options = new() { BandwidthBitsPerSecond = 100_000 };
        options.MtuChanges.Add(new MtuChange(5_000, 500));
        using SimHarness h = new(options, connect: false);
        h.Network.Advance(0); // capability + connected (delay 0)
        Assert.Equal(TransportState.Connected, h.A.State);
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[1000], 1)); // serializing (80 ms)
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[1000], 2)); // queued
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[400], 3)); // queued, still fits
        h.Network.AdvanceTo(5_000);

        RecordedEvent[] caps = h.SinkA.OfKind(RecordedEventKind.DatagramCapabilityChanged).ToArray();
        Assert.Equal(500, caps[^1].MaxPayload);
        Assert.Equal(500, h.SinkB.OfKind(RecordedEventKind.DatagramCapabilityChanged)[^1].MaxPayload);
        Assert.Equal([DatagramSendState.Canceled], States(h.SinkA, 2));
        Assert.Equal(500, h.A.Capabilities.MaxDatagramPayload);
        h.A.GetStatistics(out TransportStatistics stats);
        Assert.Equal(500 + SimulatedTransport.PathOverheadBytes, stats.PathMtu);
        Assert.Equal(TransportStatus.TooLarge, Sim.SendDatagram(h.A, new byte[501], 4));

        h.Network.RunUntilIdle(10_000_000);
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Acknowledged], States(h.SinkA, 1));
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Acknowledged], States(h.SinkA, 3));
        Assert.Equal(2, h.SinkB.CountOf(RecordedEventKind.DatagramReceived));
        h.A.GetLinkStatistics(out SimulatedLinkStatistics link);
        Assert.Equal(1, link.DatagramsCanceled);
    }

    [Fact]
    public void MtuChange_AfterClose_IsIgnored()
    {
        LinkOptions options = new();
        options.MtuChanges.Add(new MtuChange(5_000, 500));
        using SimHarness h = new(options, connect: false);
        h.Network.Advance(0);
        h.A.Close(0, default);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Single(h.SinkA.OfKind(RecordedEventKind.DatagramCapabilityChanged)); // only the initial one
        Assert.Single(h.SinkB.OfKind(RecordedEventKind.DatagramCapabilityChanged));
    }

    [Fact]
    public void Bandwidth_SerializesAtTheConfiguredRate()
    {
        using SimHarness h = new(new LinkOptions { BandwidthBitsPerSecond = 1_000_000, DelayMicros = 1_000, MaxDatagramPayload = 1250 });
        long t0 = h.Clock.NowMicros;
        for (ulong i = 0; i < 10; i++)
            Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[1250], i));
        h.Network.RunUntilIdle(10_000_000);
        IReadOnlyList<RecordedEvent> received = h.SinkB.OfKind(RecordedEventKind.DatagramReceived);
        Assert.Equal(10, received.Count);
        for (int i = 0; i < 10; i++)
            Assert.Equal(t0 + (i + 1) * 10_000 + 1_000, received[i].TimeMicros);
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(9 * 1250, stats.MaxQueuedBytes);
        h.A.GetStatistics(out TransportStatistics transport);
        Assert.Equal(2u * 1250, transport.CongestionWindowBytes); // BDP 125 kB/s * 2 ms = 250 B < two packets
    }

    [Fact]
    public void Bandwidth_QueueOverflow_DropsDatagrams_CancelOnBlockedCancels()
    {
        using SimHarness h = new(new LinkOptions { BandwidthBitsPerSecond = 1_000_000, MaxQueueBytes = 2500, DelayMicros = 1_000, MaxDatagramPayload = 1250 });
        for (ulong i = 0; i < 4; i++)
            Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[1250], i));
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[1250], 4, TransportSendFlags.CancelOnBlocked));
        h.Network.RunUntilIdle(10_000_000);
        for (ulong i = 0; i < 3; i++)
            Assert.Equal([DatagramSendState.Sent, DatagramSendState.Acknowledged], States(h.SinkA, i));
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.LostDiscarded], States(h.SinkA, 3));
        Assert.Equal([DatagramSendState.Canceled], States(h.SinkA, 4));
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(2, stats.DatagramsDropped);
        Assert.Equal(1, stats.DatagramsCanceled);
        Assert.Equal(3, h.SinkB.CountOf(RecordedEventKind.DatagramReceived));
    }

    [Fact]
    public void Bandwidth_PriorityDatagram_OvertakesQueuedOnes()
    {
        using SimHarness h = new(new LinkOptions { BandwidthBitsPerSecond = 1_000_000 });
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, [0], 0));
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, [1], 1));
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, [2], 2));
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, [3], 3, TransportSendFlags.Priority));
        h.Network.RunUntilIdle(10_000_000);
        IReadOnlyList<RecordedEvent> received = h.SinkB.OfKind(RecordedEventKind.DatagramReceived);
        Assert.Equal([0, 3, 1, 2], received.Select(e => (int)e.Data[0]).ToArray());
    }

    [Fact]
    public void Loss_FractionWithinBounds_AndEveryDatagramFinalExactlyOnce()
    {
        const int count = 5000;
        using SimHarness h = new(new LinkOptions { DelayMicros = 2_000, LossPercent = 20 }, seed: 42);
        CountingSink counter = new();
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, "abc"u8, (ulong)i));
            h.Network.Advance(100);
        }
        h.Network.RunUntilIdle(10_000_000);
        int sent = 0, acked = 0, lost = 0;
        HashSet<ulong> finals = new();
        foreach (RecordedEvent e in h.SinkA.OfKind(RecordedEventKind.DatagramSendStateChanged))
        {
            if (e.DatagramState == DatagramSendState.Sent)
            {
                sent++;
                continue;
            }
            Assert.True(finals.Add(e.Context));
            if (e.DatagramState == DatagramSendState.Acknowledged)
                acked++;
            else if (e.DatagramState == DatagramSendState.LostDiscarded)
                lost++;
        }
        Assert.Equal(count, sent);
        Assert.Equal(count, acked + lost);
        Assert.InRange(lost, count * 17 / 100, count * 23 / 100);
        Assert.Equal(acked, h.SinkB.CountOf(RecordedEventKind.DatagramReceived));
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(lost, stats.DatagramsLost);
    }

    [Fact]
    public void Jitter_ArrivalDelayStaysWithinBounds()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000, JitterMicros = 5_000 }, seed: 7);
        byte[] payload = new byte[8];
        for (int i = 0; i < 1000; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(payload, h.Clock.NowMicros);
            Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, payload, (ulong)i));
            h.Network.Advance(1_000);
        }
        h.Network.RunUntilIdle(10_000_000);
        IReadOnlyList<RecordedEvent> received = h.SinkB.OfKind(RecordedEventKind.DatagramReceived);
        Assert.Equal(1000, received.Count);
        long sum = 0;
        foreach (RecordedEvent e in received)
        {
            long delay = e.TimeMicros - BinaryPrimitives.ReadInt64LittleEndian(e.Data);
            Assert.InRange(delay, 10_000, 15_000);
            sum += delay;
        }
        Assert.InRange(sum / received.Count, 12_000, 13_000);
    }

    [Fact]
    public void Reorder_DeliversOutOfOrder_WithoutLoss()
    {
        const int count = 2000;
        using SimHarness h = new(new LinkOptions { DelayMicros = 5_000, ReorderPercent = 10 }, seed: 11);
        byte[] payload = new byte[4];
        for (int i = 0; i < count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(payload, i);
            Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, payload, (ulong)i));
            h.Network.Advance(1_000);
        }
        h.Network.RunUntilIdle(10_000_000);
        IReadOnlyList<RecordedEvent> received = h.SinkB.OfKind(RecordedEventKind.DatagramReceived);
        Assert.Equal(count, received.Count);
        int outOfOrder = 0, previous = -1;
        HashSet<int> seen = new();
        foreach (RecordedEvent e in received)
        {
            int value = BinaryPrimitives.ReadInt32LittleEndian(e.Data);
            Assert.True(seen.Add(value));
            if (value < previous)
                outOfOrder++;
            previous = Math.Max(previous, value);
        }
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.InRange(stats.DatagramsReordered, count * 8 / 100, count * 12 / 100);
        Assert.True(outOfOrder > 0);
    }

    [Fact]
    public void Close_CancelsDatagramsAwaitingAcknowledgement()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, "x"u8, 9));
        h.A.Close(0, default);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Canceled], States(h.SinkA, 9));
        Assert.Equal(RecordedEventKind.Closed, h.SinkA.Events[^1].Kind);
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(1, stats.DatagramsCanceled);
    }

    [Fact]
    public void ArrivalAtClosingPeer_CountsAsLost()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, "x"u8, 1));
        h.Network.Advance(0);
        h.Clock.AdvanceMicros(10_000); // the arrival is due, but nothing ran yet
        h.B.Close(0, default); // B is closing (not closed) when the arrival runs
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(0, h.SinkB.CountOf(RecordedEventKind.DatagramReceived));
        h.A.GetLinkStatistics(out SimulatedLinkStatistics stats);
        Assert.Equal(1, stats.DatagramsLost);
        // B's close reaches A before the loss report does, so the datagram completes canceled.
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Canceled], States(h.SinkA, 1));
    }

    [Fact]
    public void ManyDatagramsInFlight_GrowTheRecordTable()
    {
        using SimHarness h = new(new LinkOptions { DelayMicros = 10_000 });
        for (ulong i = 0; i < 200; i++)
            Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, "x"u8, i));
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(200, h.SinkB.CountOf(RecordedEventKind.DatagramReceived));
        Assert.Equal(400, h.SinkA.CountOf(RecordedEventKind.DatagramSendStateChanged));
    }

    [Fact]
    public void SerializerFinishingWhileClosing_DropsThePacket()
    {
        using SimHarness h = new(new LinkOptions { BandwidthBitsPerSecond = 1_000_000, MaxDatagramPayload = 1250 });
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[1250], 1)); // serializer busy for 10 ms
        Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, new byte[1250], 2));
        h.Clock.AdvanceMicros(10_000); // the serializer's completion is due, but nothing ran yet
        h.A.Close(0, default);
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(0, h.SinkB.CountOf(RecordedEventKind.DatagramReceived));
        Assert.Equal([DatagramSendState.Sent, DatagramSendState.Canceled], States(h.SinkA, 1));
        Assert.Equal([DatagramSendState.Canceled], States(h.SinkA, 2));
    }

    [Fact]
    public void SteadyState_DatagramSendAndDelivery_DoesNotAllocate()
    {
        VirtualClock clock = new();
        using SimulatedNetwork network = new(clock, 5);
        CountingSink a = new(), b = new();
        (SimulatedTransport ta, SimulatedTransport tb) = network.CreatePair(a, b, new LinkOptions { DelayMicros = 2_000, JitterMicros = 500, LossPercent = 5, ReorderPercent = 5 });
        network.RunUntilIdle(1_000_000);
        byte[] payload = Sim.Pattern(600);
        // Warm up long enough for the (seed-fixed) peak of outstanding events and buffers to be reached.
        for (int i = 0; i < 40_000; i++)
            SendAndAdvance(network, ta, tb, payload);

        int windows = WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 10_000; i++)
                SendAndAdvance(network, ta, tb, payload);
        });

        int sent = 40_000 + (windows * 10_000);
        Assert.Equal(sent, a.Sent);
        Assert.True(b.Received > sent * 0.9, $"received {b.Received} of {sent}");
    }

    [Fact]
    public void SteadyState_UnderBandwidthLimit_DoesNotAllocate()
    {
        VirtualClock clock = new();
        using SimulatedNetwork network = new(clock, 6);
        CountingSink a = new(), b = new();
        (SimulatedTransport ta, SimulatedTransport tb) = network.CreatePair(a, b, new LinkOptions { DelayMicros = 2_000, BandwidthBitsPerSecond = 100_000_000 });
        network.RunUntilIdle(1_000_000);
        byte[] payload = Sim.Pattern(1000);
        for (int i = 0; i < 5_000; i++)
            SendAndAdvance(network, ta, tb, payload);

        int windows = WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 20_000; i++)
                SendAndAdvance(network, ta, tb, payload);
        });

        Assert.Equal(5_000 + (windows * 20_000), a.Sent);
        Assert.Equal(0, a.Lost);
        Assert.InRange(b.Received - a.Acknowledged, 0, 20); // acknowledgements still in flight
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void SendAndAdvance(SimulatedNetwork network, SimulatedTransport a, SimulatedTransport b, byte[] payload)
    {
        fixed (byte* p = payload)
        {
            TransportSegment segment = new(p, payload.Length);
            a.SendDatagram(&segment, 1, 1, TransportSendFlags.None);
            b.SendDatagram(&segment, 1, 2, TransportSendFlags.None);
        }
        network.Advance(250);
    }
}
