using System.Buffers.Binary;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Simulation;

public class DeterminismTests
{
    private static string[] RunScenario(int seed)
    {
        VirtualClock clock = new();
        using SimulatedNetwork network = new(clock, seed);
        RecordingSink sinkA = new(clock), sinkB = new(clock);
        LinkOptions options = new()
        {
            DelayMicros = 3_000, JitterMicros = 2_000, LossPercent = 10, ReorderPercent = 10, StreamLossPercent = 10,
            BandwidthBitsPerSecond = 20_000_000, PeerBidiStreams = 2,
        };
        options.MtuChanges.Add(new MtuChange(100_000, 1000));
        (SimulatedTransport a, SimulatedTransport b) = network.CreatePair(sinkA, sinkB, options);
        sinkA.Transport = a;
        sinkB.Transport = b;
        network.RunUntilIdle(1_000_000);
        Assert.Equal(TransportStatus.Success, a.OpenStream(StreamKind.Bidirectional, 1, 0, out TransportStreamId stream));
        byte[] payload = new byte[64];
        for (int i = 0; i < 200; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(payload, i);
            Sim.SendDatagram(a, payload, (ulong)i);
            Sim.SendDatagram(b, payload, (ulong)i);
            if (i % 20 == 0)
                Assert.Equal(TransportStatus.Success, Sim.SendStream(a, stream, Sim.Pattern(3000, i), (ulong)i, i == 0 ? TransportSendFlags.Start : TransportSendFlags.None));
            network.Advance(1_000);
        }
        network.RunUntilIdle(10_000_000);
        return [.. Sim.Describe(sinkA.Events), "--", .. Sim.Describe(sinkB.Events)];
    }

    [Fact]
    public void SameSeed_ProducesTheIdenticalEventSequence()
    {
        string[] first = RunScenario(123);
        string[] second = RunScenario(123);
        Assert.Equal(first, second);
        Assert.True(first.Length > 800);
        Assert.NotEqual(first, RunScenario(124));
    }

    [Fact]
    public void EqualDueTimes_AreDeliveredInSchedulingOrder()
    {
        using SimHarness h = new();
        for (int i = 0; i < 50; i++)
            Assert.Equal(TransportStatus.Success, Sim.SendDatagram(h.A, [(byte)i], (ulong)i));
        h.Network.RunUntilIdle(1_000_000);
        IReadOnlyList<RecordedEvent> received = h.SinkB.OfKind(RecordedEventKind.DatagramReceived);
        for (int i = 0; i < 50; i++)
            Assert.Equal(i, received[i].Data[0]);
    }
}
