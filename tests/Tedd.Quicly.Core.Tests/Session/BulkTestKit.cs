using System.Net;
using System.Numerics;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Channel tables of the bulk tests (immutable, shared across parallel tests).</summary>
internal static class BulkTables
{
    /// <summary>
    /// 2 unordered movement datagrams · 5 bulk at priority 0 (MaxGroups 2, the PROTOCOL.md §7 default) · 6 bulk with
    /// MaxGroups 1 · 7 bulk with MaxMessageSize 4 096.
    /// </summary>
    public static ChannelTable Main { get; } = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(5, "world", ChannelMode.Bulk, o => o.Priority = 0)
        .Add(6, "one-stream", ChannelMode.Bulk, o =>
        {
            o.Priority = 0;
            o.MaxGroups = 1;
        })
        .Add(7, "tiny", ChannelMode.Bulk, o =>
        {
            o.Priority = 0;
            o.MaxMessageSize = 4096;
        })
        .Build();

    /// <summary>
    /// One bulk channel with <c>MaxGroups = 1</c> next to a datagram channel, so the peer grants exactly one
    /// unidirectional stream (<c>PeerCore.PeerUnidirectionalStreamLimit</c> = Σ max(MaxGroups, 1)).
    /// </summary>
    public static ChannelTable OneStream { get; } = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(5, "world", ChannelMode.Bulk, o =>
        {
            o.Priority = 0;
            o.MaxGroups = 1;
        })
        .Build();

    /// <summary>
    /// The same channels as <see cref="OneStream"/> with room for two streams. <c>MaxGroups</c> is not part of the table
    /// hash (PROTOCOL.md §1), so one end may hold this table while its peer holds <see cref="OneStream"/>: the sender then
    /// tries to open two bulk streams while the receiver grants one, which drives the refusal path.
    /// </summary>
    public static ChannelTable TwoStreams { get; } = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(5, "world", ChannelMode.Bulk, o =>
        {
            o.Priority = 0;
            o.MaxGroups = 2;
        })
        .Build();
}

/// <summary>Helpers of the bulk tests.</summary>
internal static class BulkKit
{
    /// <summary>Quiet options (no pings, no heartbeat) with roomy budgets for bulk traffic.</summary>
    public static void Quiet(PeerOptions options)
    {
        DatagramKit.Quiet(options);
        options.SendBudgetBytes = 1024 * 1024;
        options.ReceiveBudgetBytes = 1024 * 1024;
        options.AllocatorOptions = new SlabAllocatorOptions
        {
            FreeListShards = 2,
            SizeClasses =
            [
                new(64, 4096),
                new(256, 1024),
                new(1536, 256),
                new(4096, 128),
                new(16384, 64),
                new(65536, 32),
                new(262144, 8),
            ],
        };
    }

    /// <summary>Quiet options for a receiving peer that accepts every transfer into <paramref name="router"/>.</summary>
    public static Action<PeerOptions> Receiver(IBulkRouter router) => options =>
    {
        Quiet(options);
        options.BulkRouter = router;
    };

    /// <summary>Quiet options for a receiving peer that assembles every object into <paramref name="router"/>.</summary>
    public static Action<PeerOptions> ObjectReceiver(IBulkObjectRouter router) => options =>
    {
        Quiet(options);
        options.BulkObjectRouter = router;
    };

    /// <summary>Quiet options for a peer that serves authorised range requests from <paramref name="provider"/>.</summary>
    public static Action<PeerOptions> Server(IBulkProvider provider, IBulkAuthorizer? authorizer = null) => options =>
    {
        Quiet(options);
        options.BulkProvider = provider;
        options.BulkAuthorizer = authorizer ?? new AllowAll();
    };

    public static BulkEngine Engine(QuiclyPeer peer) => (BulkEngine)peer.Core.GetEngine(ChannelMode.Bulk)!;

    public static int SendTransfers(QuiclyPeer peer, ushort channel) => Engine(peer).SendTransfersOf(peer.Core.ChannelIndexOf(channel));

    public static int SendStreams(QuiclyPeer peer, ushort channel) => Engine(peer).SendStreamsOf(peer.Core.ChannelIndexOf(channel));

    public static int ReceiveStreams(QuiclyPeer peer, ushort channel) => Engine(peer).ReceiveStreamsOf(peer.Core.ChannelIndexOf(channel));

    public static List<BulkEngine.BulkPhase> SendPhases(QuiclyPeer peer, ushort channel) =>
        Engine(peer).SendPhasesOf(peer.Core.ChannelIndexOf(channel));

    public static ChannelStatistics Stats(QuiclyPeer peer, ushort channel) => DatagramKit.ChannelStats(peer, channel);

    /// <summary>The SHA-256 of <paramref name="bytes"/>, as a descriptor carries it.</summary>
    public static byte[] Hash(ReadOnlySpan<byte> bytes) => System.Security.Cryptography.SHA256.HashData(bytes);

    /// <summary>
    /// A bulk stream's bytes written by hand (PROTOCOL.md §3.3): the preamble, the header, the body, and — when the
    /// header's flags promise one — the checksum trailer.
    /// </summary>
    /// <param name="channel">The Bulk channel.</param>
    /// <param name="header">The header; <see cref="BulkFlags.ChecksumPresent"/> decides whether a trailer follows.</param>
    /// <param name="body">The body bytes.</param>
    /// <param name="checksum">
    /// The trailer to write, or null to write the one the body really hashes to. Passing a wrong value is how a test
    /// gets a corrupt range past a transport that cannot corrupt one.
    /// </param>
    public static byte[] BulkStream(ushort channel, in BulkHeader header, ReadOnlySpan<byte> body, ulong? checksum = null)
    {
        byte[] buffer = new byte[128 + body.Length + StreamFraming.BulkChecksumLength];
        int position = StreamFraming.WritePreamble(buffer, channel);
        position += StreamFraming.WriteBulkHeader(buffer.AsSpan(position), in header);
        body.CopyTo(buffer.AsSpan(position));
        position += body.Length;
        if (header.HasChecksum)
        {
            position += StreamFraming.WriteBulkChecksum(buffer.AsSpan(position), checksum ?? XxHash64.Hash(body));
        }

        return buffer.AsSpan(0, position).ToArray();
    }

    /// <summary>A bulk stream whose header bytes are written by hand, so a hostile header can be built field by field.</summary>
    public static byte[] RawBulkStream(ushort channel, ReadOnlySpan<ulong> varints, byte flags, ReadOnlySpan<byte> tail)
    {
        byte[] buffer = new byte[128 + tail.Length];
        int position = StreamFraming.WritePreamble(buffer, channel);
        foreach (ulong value in varints)
        {
            position += VarInt.Write(buffer.AsSpan(position), value);
        }

        buffer[position++] = flags;
        tail.CopyTo(buffer.AsSpan(position));
        return buffer.AsSpan(0, position + tail.Length).ToArray();
    }

    /// <summary>
    /// A bulk stream of one chunked body: the §3.3 header of a whole object of <paramref name="rawLength"/> bytes, then one
    /// chunk header announcing <paramref name="chunkLength"/> wire bytes and only <paramref name="bodyBytes"/> of them.
    /// </summary>
    public static byte[] ChunkedStream(ushort channel, ulong transferId, int chunkLength, int rawLength, int bodyBytes)
    {
        BulkHeader header = new()
        {
            TransferId = transferId,
            ObjectId = transferId,
            ObjectVersion = 1,
            TotalLength = (ulong)rawLength,
            Offset = 0,
            Length = (ulong)rawLength,
            Flags = BulkFlags.Chunked,
        };
        byte[] body = new byte[16 + bodyBytes];
        int position = StreamFraming.WriteBulkChunkHeader(body, chunkLength, rawLength);
        return BulkStream(channel, in header, body.AsSpan(0, position + bodyBytes));
    }

    /// <summary>A body of chunk frames (<c>ChunkLength, RawLength, bytes</c>), each stored uncompressed.</summary>
    public static byte[] RawChunks(params byte[][] chunks)
    {
        byte[] buffer = new byte[chunks.Sum(c => c.Length + 16)];
        int position = 0;
        foreach (byte[] chunk in chunks)
        {
            position += StreamFraming.WriteBulkChunkHeader(buffer.AsSpan(position), chunk.Length, 0);
            chunk.CopyTo(buffer.AsSpan(position));
            position += chunk.Length;
        }

        return buffer.AsSpan(0, position).ToArray();
    }
}

/// <summary>A bulk source over an array (random access, as the engine requires after a refused stream start).</summary>
internal sealed class MemorySource(byte[] bytes) : IBulkSource
{
    public byte[] Bytes { get; } = bytes;

    /// <summary>Reads asked for, so a test can see how often the engine went back to the source.</summary>
    public int Reads { get; private set; }

    public int Read(long offset, Span<byte> destination)
    {
        Reads++;
        if (offset >= Bytes.Length)
        {
            return 0;
        }

        int take = (int)Math.Min(destination.Length, Bytes.Length - offset);
        Bytes.AsSpan((int)offset, take).CopyTo(destination);
        return take;
    }
}

/// <summary>A source that runs dry after <paramref name="available"/> bytes (the engine then fails the transfer).</summary>
internal sealed class ShortSource(long available) : IBulkSource
{
    public int Read(long offset, Span<byte> destination)
    {
        if (offset >= available)
        {
            return 0;
        }

        int take = (int)Math.Min(destination.Length, available - offset);
        destination.Slice(0, take).Fill(0xAB);
        return take;
    }
}

/// <summary>
/// A source of an arbitrarily large object without holding it in memory: byte <c>n</c> is
/// <see cref="At"/>(<c>n</c>), so a receiver can verify every byte without a copy of the object.
/// </summary>
internal sealed class PatternSource(long total) : IBulkSource
{
    /// <summary>Bytes in which <c>offset &gt;&gt; 11</c>, the second term of <see cref="At"/>, stays the same.</summary>
    private const int Block = 2048;

    /// <summary>
    /// The first term of <see cref="At"/>, <c>(byte)(n * 131)</c>, for <c>n</c> from 0 on: it depends only on <c>n &amp; 255</c>,
    /// so the run of a whole block starts somewhere in the first 256 bytes.
    /// </summary>
    private static readonly byte[] Cycle = MakeCycle();

    public static byte At(long offset) => (byte)((offset * 131) ^ (offset >> 11));

    public long Total => total;

    public int Read(long offset, Span<byte> destination)
    {
        if (offset >= total)
        {
            return 0;
        }

        int take = (int)Math.Min(destination.Length, total - offset);
        Fill(offset, destination.Slice(0, take));
        return take;
    }

    /// <summary>Writes the pattern's bytes from <paramref name="offset"/> on: <see cref="At"/> a block at a time.</summary>
    public static void Fill(long offset, Span<byte> destination)
    {
        while (!destination.IsEmpty)
        {
            int take = Math.Min(destination.Length, Block - (int)(offset & (Block - 1)));
            ReadOnlySpan<byte> cycle = Cycle.AsSpan((int)(offset & 255), take);
            byte k = (byte)(offset >> 11);
            Vector<byte> kv = new(k);
            int i = 0;
            for (; i <= take - Vector<byte>.Count; i += Vector<byte>.Count)
            {
                (new Vector<byte>(cycle.Slice(i)) ^ kv).CopyTo(destination.Slice(i));
            }

            for (; i < take; i++)
            {
                destination[i] = (byte)(cycle[i] ^ k);
            }

            destination = destination.Slice(take);
            offset += take;
        }
    }

    /// <summary>How many of <paramref name="data"/>'s bytes differ from the pattern's from <paramref name="offset"/> on.</summary>
    public static long Mismatches(long offset, ReadOnlySpan<byte> data)
    {
        long mismatches = 0;
        while (!data.IsEmpty)
        {
            int take = Math.Min(data.Length, Block - (int)(offset & (Block - 1)));
            ReadOnlySpan<byte> cycle = Cycle.AsSpan((int)(offset & 255), take);
            byte k = (byte)(offset >> 11);
            Vector<byte> kv = new(k);
            int i = 0;
            for (; i <= take - Vector<byte>.Count; i += Vector<byte>.Count)
            {
                if (!Vector.EqualsAll(new Vector<byte>(data.Slice(i)) ^ kv, new Vector<byte>(cycle.Slice(i))))
                {
                    for (int j = i; j < i + Vector<byte>.Count; j++)
                    {
                        mismatches += data[j] != (byte)(cycle[j] ^ k) ? 1 : 0;
                    }
                }
            }

            for (; i < take; i++)
            {
                mismatches += data[i] != (byte)(cycle[i] ^ k) ? 1 : 0;
            }

            data = data.Slice(take);
            offset += take;
        }

        return mismatches;
    }

    private static byte[] MakeCycle()
    {
        byte[] cycle = new byte[256 + Block];
        for (int n = 0; n < cycle.Length; n++)
        {
            cycle[n] = (byte)(n * 131);
        }

        return cycle;
    }
}

/// <summary>A sink that keeps every byte, so a test can compare the object it received (allocation-free in <c>Write</c>).</summary>
internal sealed class MemorySink(long length) : IBulkSink
{
    public byte[] Bytes { get; } = new byte[length];

    public long BytesWritten { get; private set; }

    public long FirstOffset { get; private set; } = -1;

    public int Writes { get; private set; }

    public BulkResult? Result { get; private set; }

    public bool IsFinished => Result is not null;

    public void Write(long objectOffset, ReadOnlySpan<byte> data)
    {
        if (FirstOffset < 0)
        {
            FirstOffset = objectOffset;
        }

        data.CopyTo(Bytes.AsSpan((int)objectOffset));
        BytesWritten += data.Length;
        Writes++;
    }

    public void Finish(in BulkResult result) => Result = result;
}

/// <summary>
/// A sink that verifies every byte against <see cref="PatternSource.At"/> instead of storing it, so a 64 MiB transfer
/// costs no memory and is still checked byte for byte.
/// </summary>
internal sealed class PatternSink : IBulkSink
{
    public long BytesWritten { get; private set; }

    public long Mismatches { get; private set; }

    public BulkResult? Result { get; private set; }

    public bool IsFinished => Result is not null;

    public void Write(long objectOffset, ReadOnlySpan<byte> data)
    {
        Mismatches += PatternSource.Mismatches(objectOffset, data);
        BytesWritten += data.Length;
    }

    public void Finish(in BulkResult result) => Result = result;
}

/// <summary>A sink that counts without checking (for the allocation windows, where the check itself is the only work).</summary>
internal sealed class CountingSink : IBulkSink
{
    public long BytesWritten;

    public BulkResult? Result { get; private set; }

    public void Write(long objectOffset, ReadOnlySpan<byte> data) => BytesWritten += data.Length;

    public void Finish(in BulkResult result) => Result = result;
}

/// <summary>A receive router that accepts every transfer, handing each one the sink <paramref name="factory"/> makes.</summary>
internal sealed class AcceptRouter(Func<BulkTransferInfo, IBulkSink> factory) : IBulkRouter
{
    public List<BulkTransferInfo> Accepted { get; } = [];

    public List<IBulkSink> Sinks { get; } = [];

    public List<(BulkRangeRequest Request, QuiclyErrorCode Code)> Rejections { get; } = [];

    /// <summary>A router that keeps every byte of one object of <paramref name="length"/> bytes.</summary>
    public static AcceptRouter Memory(long length) => new(_ => new MemorySink(length));

    /// <summary>A router that verifies the pattern of <see cref="PatternSource"/>.</summary>
    public static AcceptRouter Pattern() => new(_ => new PatternSink());

    public BulkReceiveDecision SelectTarget(in BulkTransferInfo info)
    {
        Accepted.Add(info);
        IBulkSink sink = factory(info);
        Sinks.Add(sink);
        return BulkReceiveDecision.Accept(sink);
    }

    public void OnRequestRejected(in BulkRangeRequest request, QuiclyErrorCode code) => Rejections.Add((request, code));

    public T Sink<T>(int index = 0)
        where T : class, IBulkSink => (T)Sinks[index];
}

/// <summary>A receive router that refuses every transfer (which is also what no router at all does).</summary>
internal sealed class DenyRouter(QuiclyErrorCode code = QuiclyErrorCode.BulkRejected) : IBulkRouter
{
    public int Calls { get; private set; }

    public List<(BulkRangeRequest Request, QuiclyErrorCode Code)> Rejections { get; } = [];

    public BulkReceiveDecision SelectTarget(in BulkTransferInfo info)
    {
        Calls++;
        return BulkReceiveDecision.Reject(code);
    }

    public void OnRequestRejected(in BulkRangeRequest request, QuiclyErrorCode code) => Rejections.Add((request, code));
}

/// <summary>Authorises every range request.</summary>
internal sealed class AllowAll : IBulkAuthorizer
{
    public int Calls { get; private set; }

    public bool Authorize(in BulkRequestInfo request)
    {
        Calls++;
        return true;
    }
}

/// <summary>Refuses every range request (which is also what no authorizer at all does).</summary>
internal sealed class DenyAll : IBulkAuthorizer
{
    public List<BulkRequestInfo> Requests { get; } = [];

    public bool Authorize(in BulkRequestInfo request)
    {
        Requests.Add(request);
        return false;
    }
}

/// <summary>
/// Refuses a bulk stream the way a transport can refuse one, so the engine's two <em>synchronous</em> refusal paths run:
/// <see cref="RefuseOpens"/> makes <see cref="OpenStream"/> itself answer
/// <see cref="TransportStatus.StreamLimitReached"/> (the simulator only ever refuses a start asynchronously, the way
/// MsQuic does), and <see cref="FailStarts"/> fails the send that carries <see cref="TransportSendFlags.Start"/>
/// <em>after</em> the open succeeded — nothing took stream credit then, so no credit event is coming and the transfer
/// must go out on a new stream at the next pass rather than wait. It can also play a transport that measures nothing
/// (<see cref="ReportNoStatistics"/>) and one that refuses the peer's <c>BulkProgress</c> control datagrams
/// (<see cref="FailProgressDatagrams"/>).
/// </summary>
internal sealed unsafe class BulkRefusalTransport(ITransport inner, ITransportSink sink) : ITransport
{
    private readonly Dictionary<TransportStreamId, ulong> _openContexts = [];

    public ITransport Inner => inner;

    /// <summary>Opens of bulk streams still to refuse.</summary>
    public int RefuseOpens { get; set; }

    /// <summary>Start sends of bulk streams still to fail after their open succeeded.</summary>
    public int FailStarts { get; set; }

    /// <summary>What a failed Start send returns (anything but the stream limit).</summary>
    public TransportStatus StartStatus { get; set; } = TransportStatus.OutOfMemory;

    public int RefusedOpens { get; private set; }

    public int FailedStarts { get; private set; }

    /// <summary>Inject the sole credit grant before a refused start returns.</summary>
    public bool GrantCreditOnFailedStart { get; set; }

    public int InjectedCreditGrants { get; private set; }

    public int BulkStartAttempts { get; private set; }

    /// <summary>Report all-zero statistics — no congestion window, no RTT — like a transport that measures nothing.</summary>
    public bool ReportNoStatistics { get; set; }

    /// <summary>Refuse every <c>BulkProgress</c> control datagram (PROTOCOL.md §2.3: <c>0x00 0x05 ...</c>) this end sends.</summary>
    public bool FailProgressDatagrams { get; set; }

    public int FailedProgressDatagrams { get; private set; }

    public TransportCapabilities Capabilities => inner.Capabilities;

    public TransportState State => inner.State;

    /// <summary>Tells the peer it may open a stream again (what a shutdown elsewhere would have done).</summary>
    public void GrantCredit() => sink.OnStreamsAvailable(0, 2);

    public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        if (FailProgressDatagrams && count > 0 && segments[0].Length >= 2)
        {
            ReadOnlySpan<byte> head = segments[0].AsSpan();
            if (head[0] == 0x00 && head[1] == (byte)ControlType.BulkProgress)
            {
                FailedProgressDatagrams++;
                return TransportStatus.OutOfMemory;
            }
        }

        return inner.SendDatagram(segments, count, context, flags);
    }

    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
    {
        if (RefuseOpens > 0 && IsBulk(context))
        {
            RefuseOpens--;
            RefusedOpens++;
            id = TransportStreamId.None;
            return TransportStatus.StreamLimitReached;
        }

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
        if ((flags & TransportSendFlags.Start) != 0
            && _openContexts.TryGetValue(id, out ulong open) && IsBulk(open))
        {
            BulkStartAttempts++;
            if (FailStarts > 0)
            {
                FailStarts--;
                FailedStarts++;
                if (GrantCreditOnFailedStart)
                {
                    InjectedCreditGrants++;
                    GrantCredit();
                }
                return StartStatus;
            }
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

    public void GetStatistics(out TransportStatistics statistics)
    {
        if (ReportNoStatistics)
        {
            statistics = default;
            return;
        }

        inner.GetStatistics(out statistics);
    }

    public void Dispose() => inner.Dispose();

    private static bool IsBulk(ulong context) =>
        PeerCore.TryDecodeEngineStreamContext(context, out ChannelMode mode, out _, out _) && mode == ChannelMode.Bulk;
}

/// <summary>
/// Wraps a connector's transport in <see cref="BulkRefusalTransport"/>. With <paramref name="refusalAfterCompletion"/> the
/// peer's sink is wrapped in <see cref="RefusalAfterCompletionSink"/>, which plays the other order of a refused start's
/// two callbacks.
/// </summary>
internal sealed class BulkRefusalConnector(ITransportConnector inner, bool refusalAfterCompletion = false) : ITransportConnector
{
    public BulkRefusalTransport? Transport { get; private set; }

    public RefusalAfterCompletionSink? Sink { get; private set; }

    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
    {
        ITransportSink delivered = sink;
        if (refusalAfterCompletion)
        {
            delivered = Sink = new RefusalAfterCompletionSink(sink);
        }

        return Transport = new BulkRefusalTransport(inner.Connect(endpoint, serverName, delivered), sink);
    }
}

/// <summary>
/// Delivers a refused bulk start the other way round. The simulator reports <c>OnStreamStarted(StreamLimitReached)</c>
/// and <em>then</em> the canceled completion of the send that carried the start, in one burst; a transport may just as well
/// report the refusal after the completion, and late enough that the game thread has already applied the completion.
/// This sink holds the refusal back until another stream's next completion — a later step of the simulation, so a
/// scheduler pass runs in between. Every other callback passes straight through.
/// </summary>
internal sealed class RefusalAfterCompletionSink(ITransportSink inner) : ITransportSink
{
    private readonly Dictionary<TransportStreamId, ulong> _held = [];
    private readonly List<TransportStreamId> _released = [];

    /// <summary>Refusals delivered after their completion.</summary>
    public int Reordered { get; private set; }

    public void OnConnected(in TransportConnectedInfo info) => inner.OnConnected(in info);

    public void OnDatagramReceived(ReadOnlySpan<byte> payload) => inner.OnDatagramReceived(payload);

    public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) => inner.OnPeerStreamStarted(id, kind);

    public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
        if (status == TransportStatus.StreamLimitReached
            && PeerCore.TryDecodeEngineStreamContext(context, out ChannelMode mode, out _, out _) && mode == ChannelMode.Bulk)
        {
            _held[id] = context;
            return;
        }

        inner.OnStreamStarted(id, context, status);
    }

    public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin) =>
        inner.OnStreamReceived(id, segments, absoluteOffset, fin);

    public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled)
    {
        inner.OnStreamSendCompleted(id, context, canceled);
        if (_held.Count == 0 || _held.ContainsKey(id))
        {
            return;
        }

        _released.Clear();
        _released.AddRange(_held.Keys);
        foreach (TransportStreamId refused in _released)
        {
            _held.Remove(refused, out ulong start);
            Reordered++;
            inner.OnStreamStarted(refused, start, TransportStatus.StreamLimitReached);
        }
    }

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

/// <summary>Serves one in-memory object, honouring the range the peer asked for.</summary>
internal sealed class MemoryProvider(byte[] bytes, bool compress = false, bool hash = false) : IBulkProvider
{
    public List<BulkRequestInfo> Requests { get; } = [];

    public bool TryGetObject(in BulkRequestInfo request, out BulkDescriptor descriptor, out IBulkSource? source)
    {
        Requests.Add(request);
        descriptor = default;
        source = null;
        if (request.Offset >= bytes.Length || request.Length <= 0)
        {
            return false;
        }

        long length = Math.Min(request.Length, bytes.Length - request.Offset);
        descriptor = new BulkDescriptor(request.Channel, request.ObjectId, request.ObjectVersion, bytes.Length, request.Offset, length, compress)
        {
            Checksum = hash,
        };
        source = new MemorySource(bytes);
        return true;
    }
}

/// <summary>A router that accepts every object, handing each one the sink <paramref name="factory"/> makes.</summary>
internal sealed class AcceptObjectRouter(Func<BulkObjectInfo, IBulkObjectSink> factory) : IBulkObjectRouter
{
    public List<BulkObjectInfo> Accepted { get; } = [];

    public List<IBulkObjectSink> Sinks { get; } = [];

    public List<BulkObjectProgress> Progress { get; } = [];

    public List<(BulkRangeRequest Request, QuiclyErrorCode Code)> Rejections { get; } = [];

    /// <summary>Accepts one object of <paramref name="length"/> bytes and keeps every byte of it.</summary>
    public static AcceptObjectRouter Memory(long length) => new(_ => new MemoryObjectSink(length));

    /// <summary>Accepts objects and verifies the pattern of <see cref="PatternSource"/> without storing them.</summary>
    public static AcceptObjectRouter Pattern() => new(_ => new PatternObjectSink());

    /// <summary>
    /// Whether accepted objects get a progress callback. Off for the allocation windows: the callback appends to
    /// <see cref="Progress"/>, and a growing <see cref="List{T}"/> is the test's allocation, not the driver's.
    /// </summary>
    public bool ReportProgress { get; init; } = true;

    public BulkObjectReceiveDecision SelectTarget(in BulkObjectInfo info)
    {
        Accepted.Add(info);
        IBulkObjectSink sink = factory(info);
        Sinks.Add(sink);
        return BulkObjectReceiveDecision.Accept(sink, 0, ReportProgress ? (in BulkObjectProgress p) => Progress.Add(p) : null);
    }

    public void OnRequestRejected(in BulkRangeRequest request, QuiclyErrorCode code) => Rejections.Add((request, code));

    public T Sink<T>(int index = 0)
        where T : class, IBulkObjectSink => (T)Sinks[index];
}

/// <summary>An object router that refuses everything (which is also what no router at all does).</summary>
internal sealed class DenyObjectRouter(QuiclyErrorCode code = QuiclyErrorCode.BulkRejected) : IBulkObjectRouter
{
    public int Calls { get; private set; }

    public BulkObjectReceiveDecision SelectTarget(in BulkObjectInfo info)
    {
        Calls++;
        return BulkObjectReceiveDecision.Reject(code);
    }
}

/// <summary>An object sink that keeps every byte, so a test can compare the object it assembled.</summary>
internal sealed class MemoryObjectSink(long length) : IBulkObjectSink
{
    public byte[] Bytes { get; } = new byte[length];

    public long BytesWritten { get; private set; }

    public int Writes { get; private set; }

    public BulkObjectResult? Result { get; private set; }

    /// <summary>Times Finish was called; its contract is exactly once per accepted object.</summary>
    public int Finishes { get; private set; }

    public bool IsFinished => Result is not null;

    public void Write(long objectOffset, ReadOnlySpan<byte> data)
    {
        data.CopyTo(Bytes.AsSpan((int)objectOffset));
        BytesWritten += data.Length;
        Writes++;
    }

    public void Finish(in BulkObjectResult result)
    {
        Result = result;
        Finishes++;
    }
}

/// <summary>
/// An object sink that verifies every byte against <see cref="PatternSource.At"/> instead of storing it, so a multi-GiB
/// object costs no memory and is still checked byte for byte at its true object offset.
/// </summary>
internal sealed class PatternObjectSink : IBulkObjectSink
{
    public long BytesWritten { get; private set; }

    public long Mismatches { get; private set; }

    public BulkObjectResult? Result { get; private set; }

    public bool IsFinished => Result is not null;

    public void Write(long objectOffset, ReadOnlySpan<byte> data)
    {
        Mismatches += PatternSource.Mismatches(objectOffset, data);
        BytesWritten += data.Length;
    }

    public void Finish(in BulkObjectResult result) => Result = result;
}
