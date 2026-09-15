using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Simulation;

/// <summary>A network with one pair of recorded transports.</summary>
internal sealed class SimHarness : IDisposable
{
    public SimHarness(LinkOptions? options = null, int seed = 1, bool connect = true)
    {
        Network = new SimulatedNetwork(Clock, seed);
        SinkA = new RecordingSink(Clock);
        SinkB = new RecordingSink(Clock);
        (A, B) = Network.CreatePair(SinkA, SinkB, options);
        SinkA.Transport = A;
        SinkB.Transport = B;
        if (connect)
        {
            Assert.True(Network.RunUntilIdle(10_000_000));
            SinkA.Clear();
            SinkB.Clear();
        }
    }

    public VirtualClock Clock { get; } = new();
    public SimulatedNetwork Network { get; }
    public RecordingSink SinkA { get; }
    public RecordingSink SinkB { get; }
    public SimulatedTransport A { get; }
    public SimulatedTransport B { get; }

    public TransportStreamId OpenAndStart(SimulatedTransport transport, StreamKind kind = StreamKind.Bidirectional, ulong context = 7)
    {
        Assert.Equal(TransportStatus.Success, transport.OpenStream(kind, context, 32767, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, transport.StartStream(id));
        return id;
    }

    public void Dispose() => Network.Dispose();
}

internal static unsafe class Sim
{
    public static TransportStatus SendDatagram(ITransport transport, ReadOnlySpan<byte> data, ulong context, TransportSendFlags flags = TransportSendFlags.None)
    {
        fixed (byte* p = data)
        {
            TransportSegment segment = new(p, data.Length);
            return transport.SendDatagram(&segment, 1, context, flags);
        }
    }

    public static TransportStatus SendStream(ITransport transport, TransportStreamId id, ReadOnlySpan<byte> data, ulong context, TransportSendFlags flags = TransportSendFlags.None)
    {
        fixed (byte* p = data)
        {
            TransportSegment segment = new(p, data.Length);
            return transport.SendStream(id, &segment, 1, context, flags);
        }
    }

    public static TransportStatus SendStreamParts(ITransport transport, TransportStreamId id, byte[] first, byte[] second, ulong context, TransportSendFlags flags = TransportSendFlags.None)
    {
        fixed (byte* p1 = first)
        fixed (byte* p2 = second)
        {
            TransportSegment* segments = stackalloc TransportSegment[2];
            segments[0] = new TransportSegment(p1, first.Length);
            segments[1] = new TransportSegment(p2, second.Length);
            return transport.SendStream(id, segments, 2, context, flags);
        }
    }

    public static byte[] Pattern(int length, int seed = 0)
    {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++)
            data[i] = (byte)(i * 31 + seed * 7 + (i >> 8));
        return data;
    }

    public static string[] Describe(IReadOnlyList<RecordedEvent> events)
    {
        string[] result = new string[events.Count];
        for (int i = 0; i < events.Count; i++)
            result[i] = events[i].ToString();
        return result;
    }
}

/// <summary>A sink that only counts, so steady-state allocation can be measured through it.</summary>
internal sealed class CountingSink : ITransportSink
{
    public long Received;
    public long ReceivedBytes;
    public long Sent;
    public long Acknowledged;
    public long Lost;
    public long Canceled;
    public long StreamBytes;
    public long SendCompleted;
    public bool Connected;
    public bool Closed;

    public void OnConnected(in TransportConnectedInfo info) => Connected = true;
    public void OnDatagramReceived(ReadOnlySpan<byte> payload)
    {
        Received++;
        ReceivedBytes += payload.Length;
    }
    public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) { }
    public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status) { }
    public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
    {
        int total = 0;
        for (int i = 0; i < segments.Length; i++)
            total += (int)segments[i].Length;
        StreamBytes += total;
        return ReceiveResult.Consumed(total);
    }
    public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) => SendCompleted++;
    public void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
    {
        switch (state)
        {
            case DatagramSendState.Sent: Sent++; break;
            case DatagramSendState.Acknowledged: Acknowledged++; break;
            case DatagramSendState.LostDiscarded: Lost++; break;
            case DatagramSendState.Canceled: Canceled++; break;
        }
    }
    public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) { }
    public void OnStreamPeerSendShutdown(TransportStreamId id) { }
    public void OnStreamShutdownComplete(TransportStreamId id) { }
    public void OnDatagramCapabilityChanged(bool enabled, int maxPayload) { }
    public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes) { }
    public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional) { }
    public void OnPeerAddressChanged(in TransportConnectedInfo info) { }
    public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus) => Closed = true;
}
