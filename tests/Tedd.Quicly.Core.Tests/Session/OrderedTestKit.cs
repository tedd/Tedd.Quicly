using System.Net;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Channel tables of the ordered-engine tests (immutable, shared across parallel tests).</summary>
internal static class OrderedTables
{
    /// <summary>
    /// 2 unordered (movement) · 3 sequenced keyed · 4 ordered · 5 ordered keyed · 6 ordered LZ4 (MinCompressSize 16) ·
    /// 7 ordered, 64 KiB queue limit · 8 ordered, priority 200 · 9 ordered, MaxMessageSize 1000 · 10 ordered, request/response.
    /// </summary>
    public static ChannelTable Main { get; } = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(3, "state", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.ExpiryMicros = 0; })
        .Add(4, "chat", ChannelMode.ReliableOrdered)
        .Add(5, "keyed", ChannelMode.ReliableOrdered, o => o.Keyed = true)
        .Add(6, "packed", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; })
        .Add(7, "limited", ChannelMode.ReliableOrdered, o => o.QueueLimitBytes = 64 * 1024)
        .Add(8, "urgent", ChannelMode.ReliableOrdered, o => o.Priority = 200)
        .Add(9, "small", ChannelMode.ReliableOrdered, o => o.MaxMessageSize = 1000)
        .Add(10, "rpc", ChannelMode.ReliableOrdered, o => o.RequestResponse = true)
        .Build();
}

/// <summary>Helpers of the ordered-engine tests.</summary>
internal static class OrderedKit
{
    /// <summary>Budgets and a pool large enough for many 64 KiB messages in flight.</summary>
    public static void Roomy(PeerOptions options)
    {
        options.SendBudgetBytes = 4 * 1024 * 1024;
        options.ReceiveBudgetBytes = 4 * 1024 * 1024;
        options.SendTableCapacity = 4096;
        options.AllocatorOptions = new SlabAllocatorOptions
        {
            FreeListShards = 2,
            SizeClasses =
            [
                new(64, 8192),
                new(256, 2048),
                new(1536, 1024),
                new(4096, 512),
                new(16384, 128),
                new(65536, 64),
                new(262144, 2),
            ],
        };
    }

    public static ReliableOrderedEngine Engine(QuiclyPeer peer) => (ReliableOrderedEngine)peer.Core.GetEngine(ChannelMode.ReliableOrdered)!;

    public static ReliableOrderedEngine.StreamPhase Phase(QuiclyPeer peer, ushort channel) => Engine(peer).PhaseOf(peer.Core.ChannelIndexOf(channel));

    public static uint StreamSerial(QuiclyPeer peer, ushort channel) => Engine(peer).StreamSerialOf(peer.Core.ChannelIndexOf(channel));

    /// <summary>A recognisable payload of any length (byte i depends on the id and i).</summary>
    public static byte[] Payload(int id, int length)
    {
        byte[] payload = new byte[length];
        Fill(payload, id);
        return payload;
    }

    public static void Fill(Span<byte> payload, int id)
    {
        for (int i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)((id * 131) + (i * 7) + (i >> 8));
        }
    }

    public static bool Matches(ReadOnlySpan<byte> payload, int id, int length)
    {
        if (payload.Length != length)
        {
            return false;
        }

        for (int i = 0; i < payload.Length; i++)
        {
            if (payload[i] != (byte)((id * 131) + (i * 7) + (i >> 8)))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Bytes LZ4 cannot shrink.</summary>
    public static byte[] Noise(int length, int seed)
    {
        byte[] data = new byte[length];
        new Random(seed).NextBytes(data);
        return data;
    }

    public static ChannelStatistics Stats(QuiclyPeer peer, ushort channel) => DatagramKit.ChannelStats(peer, channel);
}

/// <summary>
/// Emulates MsQuic's asynchronous refusal of a stream start by the peer's stream limit: the send that carries Start returns
/// Success, and <see cref="Deliver"/> (called from the test's pump: the simulator's transport thread is the test thread)
/// raises OnStreamStarted(StreamLimitReached), the canceled completion of that send and OnStreamShutdownComplete, in that
/// order (the ITransport contract as clarified by the MsQuic transport wave). <see cref="GrantCredit"/> raises
/// OnStreamsAvailable. The refused stream is released on the inner transport (it never started).
/// </summary>
internal sealed unsafe class AsyncRefusalTransport(ITransport inner, ITransportSink sink) : ITransport
{
    private readonly Dictionary<TransportStreamId, ulong> _openContexts = [];
    private readonly List<(TransportStreamId Id, ulong OpenContext, ulong SendContext)> _refused = [];

    public ITransport Inner => inner;

    /// <summary>Starts of engine streams to refuse.</summary>
    public int RefuseStarts { get; set; }

    /// <summary>Refuse synchronously instead: the send returns StreamLimitReached and nothing follows for the stream.</summary>
    public bool Synchronous { get; set; }

    public int Refused { get; private set; }

    public int PendingRefusals => _refused.Count;

    public TransportCapabilities Capabilities => inner.Capabilities;

    public TransportState State => inner.State;

    public void Deliver()
    {
        foreach ((TransportStreamId id, ulong open, ulong send) in _refused)
        {
            inner.CloseStream(id);
            sink.OnStreamStarted(id, open, TransportStatus.StreamLimitReached);
            sink.OnStreamSendCompleted(id, send, canceled: true);
            sink.OnStreamShutdownComplete(id);
        }

        _refused.Clear();
    }

    public void GrantCredit() => sink.OnStreamsAvailable(0, 1);

    public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags) =>
        inner.SendDatagram(segments, count, context, flags);

    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
    {
        TransportStatus status = inner.OpenStream(kind, context, priority, out id);
        if (status == TransportStatus.Success)
        {
            _openContexts[id] = context;
        }

        return status;
    }

    public TransportStatus StartStream(TransportStreamId id) => inner.StartStream(id);

    public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        if (RefuseStarts > 0 && (flags & TransportSendFlags.Start) != 0 && _openContexts.TryGetValue(id, out ulong open)
            && PeerCore.TryDecodeEngineStreamContext(open, out _, out _, out _))
        {
            RefuseStarts--;
            Refused++;
            if (Synchronous)
            {
                return TransportStatus.StreamLimitReached;
            }

            _refused.Add((id, open, context));
            return TransportStatus.Success;
        }

        return inner.SendStream(id, segments, count, context, flags);
    }

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

/// <summary>Wraps the transport of a connector in <see cref="AsyncRefusalTransport"/>.</summary>
internal sealed class AsyncRefusalConnector(ITransportConnector inner) : ITransportConnector
{
    public AsyncRefusalTransport? Transport { get; private set; }

    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) =>
        Transport = new AsyncRefusalTransport(inner.Connect(endpoint, serverName, sink), sink);
}
