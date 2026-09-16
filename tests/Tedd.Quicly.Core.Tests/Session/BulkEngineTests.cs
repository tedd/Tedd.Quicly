using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The bulk engine's own checks and the branches peer input cannot reach: argument validation, the transport-thread
/// guards that exist as defence in depth, the epoch and teardown paths, and the shapes a transfer takes when a source
/// runs dry or a body does not compress. The delivery and stream suites cover what the two peers do to each other; this
/// one calls the engine directly where that is the only way in.
/// </summary>
public class BulkEngineTests
{
    [Fact]
    public void A_Transfer_Object_Finishes_Exactly_Once()
    {
        BulkTransfer transfer = new(new BulkDescriptor(5, 1, 1, 10), transferId: 1, length: 10);

        // Running is not a terminal state, so it is not an outcome and does not complete anything.
        transfer.Finish(new BulkResult(BulkStatus.Running, 5, BulkHashState.None, QuiclyErrorCode.NoError));
        Assert.Equal(BulkStatus.Running, transfer.Status);
        Assert.False(transfer.Completion.IsCompleted);

        transfer.Advance(4);
        Assert.Equal(4, transfer.BytesTransferred);
        Assert.Equal(0.4, transfer.Progress);

        transfer.Finish(new BulkResult(BulkStatus.Completed, 10, BulkHashState.Verified, QuiclyErrorCode.NoError));
        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(10, transfer.BytesTransferred);
        Assert.Equal(BulkHashState.Verified, transfer.Result.Hash);

        // A second outcome — a close path arriving after the completion — leaves the first one exactly as it was.
        transfer.Finish(new BulkResult(BulkStatus.Failed, 0, BulkHashState.Mismatch, QuiclyErrorCode.BulkCanceled));
        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(10, transfer.BytesTransferred);
        Assert.Equal(BulkHashState.Verified, transfer.Result.Hash);
        Assert.Equal(QuiclyErrorCode.NoError, transfer.Result.Code);

        // A transfer with no engine behind it still records the request, so a later Cancel is inert rather than a throw.
        transfer.Cancel();
        Assert.True(transfer.CancelRequested);
    }

    [Fact]
    public void A_Router_Need_Not_Handle_A_Rejected_Request()
    {
        // The default interface method exists so a router that only selects targets still compiles and is callable.
        IBulkRouter router = new TargetOnlyRouter();
        router.OnRequestRejected(new BulkRangeRequest(5, 1, 1, 0, 10), QuiclyErrorCode.BulkRejected);
        Assert.True(router.SelectTarget(default).Accepted);
    }

    private sealed class TargetOnlyRouter : IBulkRouter
    {
        public BulkReceiveDecision SelectTarget(in BulkTransferInfo info) => BulkReceiveDecision.Accept(new CountingSink());
    }

    [Fact]
    public void A_Corrupt_Compressed_Chunk_Resets_Its_Stream()
    {
        AcceptRouter router = AcceptRouter.Memory(4096);
        using ServerHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main, server: BulkKit.Receiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        // A chunk that claims to decode to 4 096 bytes but is not a valid LZ4 block: the staged bytes are consumed, so the
        // transfer's stream is reset rather than pended, and both its leases go back (PROTOCOL.md §6).
        BulkHeader header = new()
        {
            TransferId = 1,
            ObjectId = 1,
            ObjectVersion = 1,
            TotalLength = 4096,
            Offset = 0,
            Length = 4096,
            Flags = BulkFlags.Chunked,
        };
        byte[] garbage = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        byte[] body = new byte[16 + garbage.Length];
        int position = StreamFraming.WriteBulkChunkHeader(body, garbage.Length, 4096);
        garbage.CopyTo(body.AsSpan(position));

        long resets = h.Statistics().StreamsReset;
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.BulkStream(5, in header, body.AsSpan(0, position + garbage.Length)), out _));
        Assert.True(h.RunUntil(() => h.Statistics().StreamsReset > resets), "the corrupt chunk was not reset");

        Assert.Single(router.Accepted);
        Assert.Equal(BulkStatus.Failed, router.Sink<MemorySink>().Result!.Value.Status);
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void A_Chunk_Longer_Than_Its_Header_Promised_Resets_Its_Stream()
    {
        AcceptRouter router = AcceptRouter.Memory(4096);
        using ServerHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main, server: BulkKit.Receiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        // Two uncompressed chunks whose decoded sizes exceed the transfer's Length: the second is more than the header
        // promised, which the engine refuses even though the parser bounds it too.
        BulkHeader header = new()
        {
            TransferId = 2,
            ObjectId = 1,
            ObjectVersion = 1,
            TotalLength = 8,
            Offset = 0,
            Length = 8,
            Flags = BulkFlags.Chunked,
        };
        byte[] body = BulkKit.RawChunks(new byte[8], new byte[8]);

        long resets = h.Statistics().StreamsReset;
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.BulkStream(5, in header, body), out _));
        Assert.True(h.RunUntil(() => h.Statistics().StreamsReset > resets), "the overlong body was not reset");

        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public async Task Beginning_A_Transfer_Validates_Its_Descriptor()
    {
        using SessionHarness h = new(table: BulkTables.Main, client: BulkKit.Quiet, server: BulkKit.Quiet);

        // A channel that is not in the table at all is rejected by the peer; one of another mode reaches *that* channel's
        // engine, whose ChannelEngine default answers NotSupportedException, so the Bulk engine never sees it.
        Assert.Throws<ArgumentException>(() => h.Client.BeginBulkSendAsync(new BulkDescriptor(99, 1, 1, 10), new EmptySource()));
        Assert.Throws<ArgumentNullException>(() => h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 10), null!));
        Assert.True(h.Client.BeginBulkSendAsync(new BulkDescriptor(2, 1, 1, 10), new EmptySource()).IsFaulted);

        // The engine's own guard, which only a direct call can reach: it owns Bulk channels alone.
        BulkEngine engine = BulkKit.Engine(h.Client);
        Assert.Throws<ArgumentException>(() => engine.BeginBulkSendAsync(h.Table[2]!, new BulkDescriptor(2, 1, 1, 10), new EmptySource(), default));

        // Ranges: PROTOCOL.md §3.3 wants Length > 0 and Offset + Length <= TotalLength <= 2^62-1.
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, -1), new EmptySource()));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 10, 11), new EmptySource()));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 10, 0, 11), new EmptySource()));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 10, 0, -1), new EmptySource()));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, (long)VarInt.MaxValue + 1), new EmptySource()));

        // PROTOCOL.md §8: the channel's MaxMessageSize bounds one transfer's Length (channel 7 carries 4 096 bytes).
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.BeginBulkSendAsync(new BulkDescriptor(7, 1, 1, 5000), new EmptySource()));
        Assert.Equal(1, BulkKit.Stats(h.Client, 7).TooLarge);

        // A hash is 32 bytes or nothing.
        Assert.Throws<ArgumentException>(
            () => h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 10) { Sha256 = new byte[31] }, new EmptySource()));

        using CancellationTokenSource canceled = new();
        await canceled.CancelAsync();
        Assert.True(h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 10), new EmptySource(), canceled.Token).IsCanceled);
    }

    [Fact]
    public void A_Transfer_Is_Refused_When_The_Session_Is_Not_Connected()
    {
        using SessionHarness h = new(table: BulkTables.Main, client: BulkKit.Quiet, server: BulkKit.Quiet, connect: false);
        BulkTransfer transfer = h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 10), new EmptySource())
            .AsTask().GetAwaiter().GetResult();

        Assert.Equal(BulkStatus.Rejected, transfer.Status);
        Assert.Equal(QuiclyErrorCode.BulkRejected, transfer.Result.Code);
        Assert.Equal(0, transfer.BytesTransferred);
        Assert.True(transfer.Completion.IsCompleted);
    }

    [Fact]
    public void Requesting_A_Range_Validates_It_And_Bounds_The_Requests_In_Flight()
    {
        using SessionHarness h = new(table: BulkTables.Main, client: BulkKit.Quiet, server: BulkKit.Quiet);

        // As above: an unknown channel is the peer's check, another mode is that engine's default, and the Bulk engine's
        // own guard needs a direct call.
        Assert.Throws<ArgumentException>(() => h.Client.RequestBulk(new BulkRangeRequest(99, 1, 1, 0, 10)));
        Assert.Throws<NotSupportedException>(() => h.Client.RequestBulk(new BulkRangeRequest(2, 1, 1, 0, 10)));
        Assert.Throws<ArgumentException>(() => BulkKit.Engine(h.Client).RequestBulk(h.Table[2]!, new BulkRangeRequest(2, 1, 1, 0, 10)));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.RequestBulk(new BulkRangeRequest(5, 1, 1, -1, 10)));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.RequestBulk(new BulkRangeRequest(5, 1, 1, 0, 0)));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => h.Client.RequestBulk(new BulkRangeRequest(5, 1, 1, (long)VarInt.MaxValue, 10)));

        // PROTOCOL.md §7 bounds the requests in flight; the extra one is counted and dropped, not queued.
        BulkEngine engine = BulkKit.Engine(h.Client);
        for (int i = 0; i < 8; i++)
        {
            h.Client.RequestBulk(new BulkRangeRequest(5, (ulong)i, 1, 0, 10));
        }

        Assert.True(engine.PendingRequests > 0);
        Assert.True(BulkKit.Stats(h.Client, 5).QueueFull > 0);
    }

    [Fact]
    public void The_Transport_Thread_Guards_Refuse_Input_They_Can_Never_Get()
    {
        using SessionHarness h = new(table: BulkTables.Main, client: BulkKit.Quiet, server: BulkKit.Quiet);
        BulkEngine engine = BulkKit.Engine(h.Client);

        // A datagram on a Bulk channel: the peer rejects it before the engine, so this only counts a drop.
        MessageHeader header = default;
        header.Channel = 5;
        engine.OnDatagram(in header, default, 0);
        Assert.Equal(1, BulkKit.Stats(h.Client, 5).Dropped);

        // A stream preamble naming a channel of another mode.
        Assert.Equal(StreamAcceptAction.Reset, engine.OnStreamOpened(new TransportStreamId(1, 1), 2, 0).Action);
        Assert.Equal(QuiclyErrorCode.UnsupportedChannel, engine.OnStreamOpened(new TransportStreamId(1, 1), 2, 0).ResetCode);

        // A cookie outside the record table, an undefined phase, and body events with no header before them.
        long cookie = 999;

        // `scoped`, like the peer's own receive loop: the context's ref field may then bind a local of this method.
        scoped StreamMessageContext context = default;
        context.Cookie = ref cookie;
        context.Channel = 5;
        context.ChannelIndex = h.Client.Core.ChannelIndexOf(5);
        Assert.Equal(StreamConsumeAction.ResetStream, engine.OnStreamMessage(ref context).Action);

        cookie = 0;
        context.Phase = (StreamMessagePhase)99;
        Assert.Equal(StreamConsumeAction.ResetStream, engine.OnStreamMessage(ref context).Action);

        context.Phase = StreamMessagePhase.Start;
        Assert.Equal(StreamConsumeAction.ResetStream, engine.OnStreamMessage(ref context).Action);

        context.Phase = StreamMessagePhase.Chunk;
        Assert.Equal(StreamConsumeAction.ResetStream, engine.OnStreamMessage(ref context).Action);

        context.Phase = StreamMessagePhase.End;
        Assert.Equal(StreamConsumeAction.Continue, engine.OnStreamMessage(ref context).Action);

        // A send entry is never cancelled through a token on a Bulk channel.
        Assert.False(engine.TryCancel(0));

        // A start of a stream this engine did not open, and one that failed for a reason other than the stream limit.
        engine.OnStreamStarted(default, PeerCore.MakeEngineStreamContext(ChannelMode.ReliableOrdered, 0, 1), TransportStatus.Success);
        engine.OnStreamStarted(new TransportStreamId(3, 1), PeerCore.MakeEngineStreamContext(ChannelMode.Bulk, 0, 1), TransportStatus.Failed);

        // A close of a stream nobody owns, and a cancel on a channel of another mode.
        engine.OnStreamClosed(new TransportStreamId(9, 9), aborted: true, (ulong)QuiclyErrorCode.BulkCanceled);
        engine.OnStreamClosed(default, aborted: false, 0);
        Assert.False(engine.CancelReceive(h.Client.Core.ChannelIndexOf(2), 1, QuiclyErrorCode.BulkCanceled));
        Assert.False(engine.CancelReceive(-1, 1, QuiclyErrorCode.BulkCanceled));

        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    [Fact]
    public void Control_Messages_That_Name_Nothing_Are_Refused()
    {
        using SessionHarness h = new(table: BulkTables.Main, client: BulkKit.Quiet, server: BulkKit.Quiet);
        BulkEngine engine = BulkKit.Engine(h.Client);

        // PROTOCOL.md §3.4: 0x13-0x15 are control-stream only, and every body is validated before it is applied.
        Assert.False(engine.OnControl(ControlType.BulkCancel, [1, 0, 0, 0, 0], onStream: false, 0));
        Assert.False(engine.OnControl(ControlType.BulkReject, [1, 0, 0, 0, 0], onStream: false, 0));
        Assert.False(engine.OnControl(ControlType.BulkRequest, [1, 2, 1, 1, 0, 1], onStream: false, 0));
        Assert.False(engine.OnControl(ControlType.BulkProgress, [], onStream: false, 0));
        Assert.False(engine.OnControl(ControlType.BulkCancel, [], onStream: true, 0));
        Assert.False(engine.OnControl(ControlType.BulkReject, [], onStream: true, 0));
        Assert.False(engine.OnControl(ControlType.BulkRequest, [], onStream: true, 0));
        Assert.False(engine.OnControl(ControlType.KeyRetired, [], onStream: true, 0));

        // A request naming a channel that is not a Bulk channel of this session.
        Assert.False(engine.OnControl(ControlType.BulkRequest, Request(2, 1), onStream: true, 0));

        // A progress or cancel for a transfer that is not running is accepted and ignored.
        Assert.True(engine.OnControl(ControlType.BulkProgress, [9, 10], onStream: false, 0));
        h.Run(2_000);
        Assert.Equal(PeerState.Connected, h.Client.State);

        static byte[] Request(ushort channel, ulong objectId)
        {
            byte[] body = new byte[32];
            int position = VarInt.Write(body, 1);
            position += VarInt.Write(body.AsSpan(position), channel);
            position += VarInt.Write(body.AsSpan(position), objectId);
            position += VarInt.Write(body.AsSpan(position), 1);
            position += VarInt.Write(body.AsSpan(position), 0);
            position += VarInt.Write(body.AsSpan(position), 1);
            return body.AsSpan(0, position).ToArray();
        }
    }

    [Fact]
    public void The_Engine_Overflows_Its_Control_Ring_Without_Losing_The_Session()
    {
        using SessionHarness h = new(table: BulkTables.Main, client: BulkKit.Quiet, server: BulkKit.Quiet);
        BulkEngine engine = BulkKit.Engine(h.Client);

        // More progress frames between two passes than the hand-off ring holds: progress is cumulative, so the drops cost
        // nothing but are counted.
        for (int i = 0; i < 200; i++)
        {
            Assert.True(engine.OnControl(ControlType.BulkProgress, [1, 10], onStream: false, 0));
        }

        Assert.True(engine.ControlNoticesDropped > 0);
        h.Run(2_000);
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    [Fact]
    public async Task A_Body_That_Does_Not_Compress_Is_Stored_Uncompressed()
    {
        // PROTOCOL.md §3.3: RawLength = 0 means the chunk is stored as it is, which is what incompressible bytes produce.
        byte[] payload = new byte[200_000];
        Random random = new(7);
        random.NextBytes(payload);

        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(
            new BulkDescriptor(5, 1, 1, payload.Length, 0, 0, Compress: true), new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the transfer did not finish");

        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);

        // Nothing shrank, so the wire carried the object plus its chunk headers.
        h.Client.GetStatistics(out PeerStatistics statistics);
        Assert.True(statistics.StreamBytesSent >= payload.Length, $"the wire carried {statistics.StreamBytesSent} bytes");
    }

    [Fact]
    public async Task A_Compressed_Source_That_Runs_Dry_Fails_Its_Transfer()
    {
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(AcceptRouter.Memory(100_000)));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(
            new BulkDescriptor(5, 1, 1, 100_000, 0, 0, Compress: true), new ShortSource(0));
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the transfer did not finish");

        Assert.Equal(BulkStatus.Failed, transfer.Status);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(0, BulkKit.SendTransfers(h.Client, 5));
    }

    [Fact]
    public async Task Closing_The_Session_Finishes_A_Transfer_That_Never_Sent_A_Byte()
    {
        using SessionHarness h = new(table: BulkTables.Main, client: BulkKit.Quiet, server: BulkKit.Quiet);

        // Registered and never flushed, so the engine's own close path is what finishes it.
        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 400_000), new PatternSource(400_000));
        Assert.Equal(BulkStatus.Running, transfer.Status);
        Assert.Single(BulkKit.SendPhases(h.Client, 5));

        h.Client.Close(CloseReason.Normal);
        Assert.True(h.RunUntilClosed(), "the session did not close");

        Assert.Equal(BulkStatus.Disconnected, transfer.Status);
        Assert.Equal(0, transfer.BytesTransferred);
    }

    [Fact]
    public async Task A_Transfer_That_Finishes_Out_Of_Order_Leaves_The_Channel_List_Intact()
    {
        // Two transfers at once, the second far smaller: it finishes first and leaves the list from the middle.
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 2_000, BandwidthBitsPerSecond = 8_000_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router));

        BulkTransfer big = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 1024 * 1024), new PatternSource(1024 * 1024));
        BulkTransfer small = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 2, 1, 2048), new PatternSource(2048));
        Assert.Equal(2, BulkKit.SendTransfers(h.Client, 5));

        Assert.True(h.RunUntil(() => small.IsFinished, 60_000_000), "the small transfer did not finish");
        Assert.Equal(BulkStatus.Completed, small.Status);

        Assert.True(h.RunUntil(() => big.IsFinished, 60_000_000), "the large transfer did not finish");
        Assert.Equal(BulkStatus.Completed, big.Status);
        Assert.True(h.RunUntil(() => BulkKit.SendTransfers(h.Client, 5) == 0), "the records were not released");
        Assert.Equal(0, router.Sinks.Sum(s => ((PatternSink)s).Mismatches));
    }

    [Fact]
    public async Task A_New_Epoch_Forgets_The_Transfers_Of_The_One_That_Ended()
    {
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 4_000_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * 1024 * 1024), new PatternSource(4 * 1024 * 1024));
        Assert.True(h.RunUntil(() => router.Sinks.Count == 1 && router.Sink<PatternSink>().BytesWritten > 0), "nothing arrived");
        Assert.Equal(1, BulkKit.ReceiveStreams(h.Server!, 5));

        // PROTOCOL.md §4.1: transfer ids are scoped to the epoch, so the receive side forgets what it was holding. The
        // request is consumed at the next receive entry point, whichever it is.
        BulkEngine server = BulkKit.Engine(h.Server!);
        server.OnEpochReset(resumed: true);
        Assert.Equal(StreamAcceptAction.Reset, server.OnStreamOpened(new TransportStreamId(99, 1), 2, 0).Action);

        Assert.Equal(0, BulkKit.ReceiveStreams(h.Server!, 5));
        Assert.Equal(0, server.ReceiveTransfers);
        Assert.True(router.Sink<PatternSink>().IsFinished);
        Assert.Equal(BulkStatus.Disconnected, router.Sink<PatternSink>().Result!.Value.Status);

        transfer.Cancel();
        h.Run(10_000);
    }

    [Fact]
    public async Task An_Open_The_Stream_Limit_Refuses_Waits_For_Credit()
    {
        byte[] payload = new byte[60_000];
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        BulkRefusalConnector? connector = null;
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router),
            connector: inner => connector = new BulkRefusalConnector(inner));

        connector!.Transport!.RefuseOpens = 1;
        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        h.Run(20_000);

        // The open was refused, so nothing reached the peer and the transfer waits for the credit it was denied.
        Assert.Equal(1, connector.Transport!.RefusedOpens);
        Assert.Equal(BulkStatus.Running, transfer.Status);
        Assert.Equal(0, transfer.BytesTransferred);
        Assert.True(BulkKit.Engine(h.Client).StreamsRefused > 0);
        Assert.Equal([BulkEngine.BulkPhase.Waiting], BulkKit.SendPhases(h.Client, 5));

        connector.Transport!.GrantCredit();
        Assert.True(h.RunUntil(() => transfer.IsFinished, 30_000_000), "the transfer did not go out after credit returned");
        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);
    }

    [Fact]
    public async Task A_Start_That_Fails_For_Another_Reason_Opens_A_New_Stream_At_Once()
    {
        byte[] payload = new byte[60_000];
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        BulkRefusalConnector? connector = null;
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router),
            connector: inner => connector = new BulkRefusalConnector(inner));

        // The open succeeds and only the send fails, so no stream limit turned this transfer away: it must not park on the
        // current credit generation, because nothing took credit and no credit event is coming. No GrantCredit here.
        connector!.Transport!.FailStarts = 1;
        connector.Transport!.StartStatus = TransportStatus.OutOfMemory;
        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.IsFinished, 30_000_000), "the transfer never went out on a new stream");

        Assert.Equal(1, connector.Transport!.FailedStarts);
        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    [Fact]
    public async Task Disposing_A_Peer_Mid_Transfer_Releases_What_It_Was_Staging()
    {
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 4_000_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(
            new BulkDescriptor(5, 1, 1, 4 * 1024 * 1024, 0, 0, Compress: true), new PatternSource(4 * 1024 * 1024));
        Assert.True(h.RunUntil(() => router.Sinks.Count == 1), "the transfer did not start");

        // The server is disposed while a chunk may be half staged: its lease goes back with the engine.
        h.DisposeServer();
        h.Run(5_000);
        Assert.Equal(0, DatagramKit.Statistics(h.Client).ReceiveBytesOutstanding);

        transfer.Cancel();
        h.Run(5_000);
    }
}
