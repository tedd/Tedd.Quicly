using System.Buffers;
using System.Net;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Channel tables of the datagram-engine tests (immutable, shared across parallel tests).</summary>
internal static class DatagramTables
{
    /// <summary>
    /// 2 unordered · 3 sequenced keyed · 4 sequenced keyed coalescing · 5 unordered LZ4 (MinCompressSize 16) · 6 sequenced
    /// unkeyed 16-bit · 7 unordered priority 250 · 8 unordered priority 50 · 9 unordered keyed coalescing · 10 sequenced
    /// keyed, 4 keys · 11 sequenced keyed, dense keys 0 … 15 · 12 unordered, 100-byte queue limit · 13 unordered fragmenting
    /// · 14 sequenced keyed, 50 ms expiry · 15 unordered keyed coalescing, dense keys 0 … 3. Expiry is off unless stated.
    /// </summary>
    public static ChannelTable Main { get; } = ChannelTable.Create()
        .Add(2, "events", ChannelMode.UnreliableUnordered)
        .Add(3, "moves", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.ExpiryMicros = 0; })
        .Add(4, "latest-moves", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.CoalesceOnReceive = true; o.MaxKeys = 64; o.ExpiryMicros = 0; })
        .Add(5, "packed", ChannelMode.UnreliableUnordered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; })
        .Add(6, "clock", ChannelMode.UnreliableSequenced, o => o.ExpiryMicros = 0)
        .Add(7, "urgent", ChannelMode.UnreliableUnordered, o => o.Priority = 250)
        .Add(8, "idle", ChannelMode.UnreliableUnordered, o => o.Priority = 50)
        .Add(9, "latest-events", ChannelMode.UnreliableUnordered, o => { o.Keyed = true; o.CoalesceOnReceive = true; o.MaxKeys = 64; })
        .Add(10, "few-keys", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.MaxKeys = 4; o.ExpiryMicros = 0; })
        .Add(11, "dense", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.KeySpace = KeySpace.Dense(15); o.ExpiryMicros = 0; })
        .Add(12, "limited", ChannelMode.UnreliableUnordered, o => o.QueueLimitBytes = 100)
        .Add(13, "fragmenting", ChannelMode.UnreliableUnordered, o => { o.Fragmentation = true; o.MaxMessageSize = 4000; })
        .Add(14, "short-lived", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.ExpiryMicros = 50_000; })
        .Add(15, "latest-slots", ChannelMode.UnreliableUnordered, o => { o.Keyed = true; o.CoalesceOnReceive = true; o.KeySpace = KeySpace.Dense(3); })
        .Build();
}

/// <summary>Helpers of the datagram-engine tests.</summary>
internal static class DatagramKit
{
    /// <summary>No pings after the first one and no heartbeat, so datagram counts and send budgets are exact.</summary>
    public static void Quiet(PeerOptions options)
    {
        options.PingInterval = TimeSpan.FromHours(1);
        options.FastPingInterval = TimeSpan.FromHours(1);
        options.FastLockDuration = TimeSpan.Zero;
        options.HeartbeatTimeout = TimeSpan.Zero;
    }

    public static SimulatedTransport TransportOf(QuiclyPeer peer) => peer.Core.Transport switch
    {
        FlagRecordingTransport recording => (SimulatedTransport)recording.Inner,
        AsyncRefusalTransport refusal => (SimulatedTransport)refusal.Inner,
        ITransport transport => (SimulatedTransport)transport,
        _ => throw new InvalidOperationException("The peer has no transport."),
    };

    public static SimulatedLinkStatistics LinkStatistics(QuiclyPeer peer)
    {
        TransportOf(peer).GetLinkStatistics(out SimulatedLinkStatistics statistics);
        return statistics;
    }

    public static PeerStatistics Statistics(QuiclyPeer peer)
    {
        peer.GetStatistics(out PeerStatistics statistics);
        return statistics;
    }

    public static ChannelStatistics ChannelStats(QuiclyPeer peer, ushort channel)
    {
        Assert.True(peer.GetChannelStatistics(channel, out ChannelStatistics statistics));
        return statistics;
    }

    /// <summary>Application datagrams a raw endpoint received (control datagrams, which start with 0x00, are left out).</summary>
    public static List<byte[]> ApplicationDatagrams(RecordingSink sink) =>
        sink.OfKind(RecordedEventKind.DatagramReceived).Where(e => e.Data.Length > 0 && e.Data[0] != 0).Select(e => e.Data).ToList();

    /// <summary>The messages of one datagram (a packed container's members in order, or the datagram itself).</summary>
    public static List<(MessageHeader Header, byte[] Payload)> Messages(byte[] datagram, ChannelTable table)
    {
        List<(MessageHeader Header, byte[] Payload)> messages = [];
        if (datagram[0] == PackedContainer.ChannelId)
        {
            Assert.Equal(ParseStatus.Ok, PackedContainer.TryParse(datagram, out PackedContainerReader reader));
            foreach (ReadOnlySpan<byte> member in reader)
            {
                messages.Add(Parse(member, table));
            }
        }
        else
        {
            messages.Add(Parse(datagram, table));
        }

        return messages;
    }

    private static (MessageHeader Header, byte[] Payload) Parse(ReadOnlySpan<byte> frame, ChannelTable table)
    {
        Assert.Equal(ParseStatus.Ok, DatagramFraming.TryParse(frame, table, out MessageHeader header, out int offset));
        return (header, frame.Slice(offset).ToArray());
    }

    /// <summary>An application datagram frame written by hand (for raw endpoints).</summary>
    public static byte[] Frame(ChannelTable table, ushort channel, uint sequence, ulong key, ReadOnlySpan<byte> payload)
    {
        MessageHeader header = default;
        header.Sequence = sequence;
        header.Key = key;
        header.FragCount = 1;
        byte[] frame = new byte[DatagramFraming.MaxHeaderLength + payload.Length];
        int written = DatagramFraming.WriteHeader(frame, table[channel]!, in header);
        payload.CopyTo(frame.AsSpan(written));
        return frame.AsSpan(0, written + payload.Length).ToArray();
    }

    /// <summary>A recognisable payload: the id (little-endian) followed by bytes derived from it.</summary>
    public static byte[] Payload(int id, int length)
    {
        byte[] payload = new byte[length];
        BitConverter.TryWriteBytes(payload, id);
        for (int i = 4; i < length; i++)
        {
            payload[i] = (byte)(id + i);
        }

        return payload;
    }
}

/// <summary>An <see cref="ITransport"/> decorator that records the flags and sizes of accepted datagrams and can refuse them.</summary>
internal sealed unsafe class FlagRecordingTransport(ITransport inner) : ITransport
{
    public ITransport Inner => inner;

    public List<(TransportSendFlags Flags, int Bytes)> Datagrams { get; } = [];

    /// <summary>When set, every datagram is refused with this status (no completion follows).</summary>
    public TransportStatus? RefuseDatagrams { get; set; }

    public int Refused { get; private set; }

    /// <summary>When set, the <see cref="TransportCapabilities.CancelOnBlocked"/> this transport reports (the simulator's own is true).</summary>
    public bool? CancelOnBlocked { get; set; }

    public TransportCapabilities Capabilities
    {
        get
        {
            TransportCapabilities capabilities = inner.Capabilities;
            if (CancelOnBlocked is { } honoured)
            {
                capabilities.CancelOnBlocked = honoured;
            }

            return capabilities;
        }
    }

    public TransportState State => inner.State;

    public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        if (RefuseDatagrams is { } refusal)
        {
            Refused++;
            return refusal;
        }

        int bytes = 0;
        for (int i = 0; i < count; i++)
        {
            bytes += (int)segments[i].Length;
        }

        TransportStatus status = inner.SendDatagram(segments, count, context, flags);
        if (status == TransportStatus.Success)
        {
            Datagrams.Add((flags, bytes));
        }

        return status;
    }

    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id) => inner.OpenStream(kind, context, priority, out id);

    public TransportStatus StartStream(TransportStreamId id) => inner.StartStream(id);

    public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags) =>
        inner.SendStream(id, segments, count, context, flags);

    public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => inner.AbortStream(id, errorCode, direction);

    public void SetStreamPriority(TransportStreamId id, ushort priority) => inner.SetStreamPriority(id, priority);

    public long GetQuicStreamId(TransportStreamId id) => inner.GetQuicStreamId(id);

    public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed) => inner.ResumeStreamReceive(id, bytesConsumed);

    public void CloseStream(TransportStreamId id) => inner.CloseStream(id);

    public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional) => inner.UpdatePeerStreamLimits(bidirectional, unidirectional);

    public void Close(ulong errorCode, ReadOnlySpan<byte> reason) => inner.Close(errorCode, reason);

    public void GetStatistics(out TransportStatistics statistics) => inner.GetStatistics(out statistics);

    public void Dispose() => inner.Dispose();
}

/// <summary>
/// Wraps the transports of a connector in <see cref="FlagRecordingTransport"/>. With <paramref name="cancelOnBlocked"/> the
/// transport reports that <see cref="TransportCapabilities.CancelOnBlocked"/> instead of its own, at connect too.
/// </summary>
internal sealed class FlagRecordingConnector(ITransportConnector inner, bool? cancelOnBlocked = null) : ITransportConnector
{
    public FlagRecordingTransport? Transport { get; private set; }

    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
    {
        ITransportSink target = cancelOnBlocked is { } honoured ? new CapabilityPatchingSink(sink, honoured) : sink;
        Transport = new FlagRecordingTransport(inner.Connect(endpoint, serverName, target)) { CancelOnBlocked = cancelOnBlocked };
        return Transport;
    }
}

/// <summary>Forwards every callback, reporting a chosen <see cref="TransportCapabilities.CancelOnBlocked"/> at connect.</summary>
internal sealed class CapabilityPatchingSink(ITransportSink inner, bool cancelOnBlocked) : ITransportSink
{
    public void OnConnected(in TransportConnectedInfo info)
    {
        TransportConnectedInfo patched = info;
        patched.Capabilities.CancelOnBlocked = cancelOnBlocked;
        inner.OnConnected(in patched);
    }

    public void OnDatagramReceived(ReadOnlySpan<byte> payload) => inner.OnDatagramReceived(payload);

    public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) => inner.OnPeerStreamStarted(id, kind);

    public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status) => inner.OnStreamStarted(id, context, status);

    public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin) =>
        inner.OnStreamReceived(id, segments, absoluteOffset, fin);

    public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) => inner.OnStreamSendCompleted(id, context, canceled);

    public void OnDatagramSendStateChanged(ulong context, DatagramSendState state) => inner.OnDatagramSendStateChanged(context, state);

    public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => inner.OnStreamAborted(id, errorCode, direction);

    public void OnStreamPeerSendShutdown(TransportStreamId id) => inner.OnStreamPeerSendShutdown(id);

    public void OnStreamShutdownComplete(TransportStreamId id) => inner.OnStreamShutdownComplete(id);

    public void OnDatagramCapabilityChanged(bool enabled, int maxPayload) => inner.OnDatagramCapabilityChanged(enabled, maxPayload);

    public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes) => inner.OnIdealSendBufferSize(id, bytes);

    public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional) => inner.OnStreamsAvailable(bidirectional, unidirectional);

    public void OnPeerAddressChanged(in TransportConnectedInfo info) => inner.OnPeerAddressChanged(in info);

    public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus) => inner.OnClosed(reason, errorCode, transportStatus);
}

/// <summary>Memory that is not backed by an array (a borrowed send of it is copied, not pinned).</summary>
internal sealed unsafe class NativeMemoryManager : MemoryManager<byte>
{
    private readonly byte* _pointer;
    private readonly int _length;

    public NativeMemoryManager(int length)
    {
        _pointer = (byte*)NativeMemory.AllocZeroed((nuint)length);
        _length = length;
    }

    public override Span<byte> GetSpan() => new(_pointer, _length);

    public override MemoryHandle Pin(int elementIndex = 0) => new(_pointer + elementIndex);

    public override void Unpin()
    {
    }

    protected override void Dispose(bool disposing) => NativeMemory.Free(_pointer);
}
