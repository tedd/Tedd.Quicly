using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The bulk stream's own rules (PROTOCOL.md §3.3, §3.4, §7): cancellation from either side, range requests and their
/// authorisation, resume of the remaining range, and the receive-side limits — a hostile header creates no state, and a
/// refused or malformed transfer resets its stream while the connection survives.
/// </summary>
public class BulkStreamTests
{
    private const int Mib = 1024 * 1024;

    [Fact]
    public async Task The_Sender_Cancels_Its_Own_Transfer_And_The_Receiver_Is_Told()
    {
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = Slow(router);

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * Mib), new PatternSource(4 * Mib));
        Assert.True(h.RunUntil(() => transfer.BytesTransferred > 0), "the transfer never started moving");

        transfer.Cancel();
        Assert.True(transfer.CancelRequested);
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the cancellation did not finish the transfer");

        Assert.Equal(BulkStatus.Canceled, transfer.Status);
        Assert.Equal(QuiclyErrorCode.BulkCanceled, transfer.Result.Code);
        Assert.True(h.RunUntil(() => router.Sink<PatternSink>().IsFinished), "the receiver was not told");
        Assert.Equal(BulkStatus.Canceled, router.Sink<PatternSink>().Result!.Value.Status);

        // A cancellation ends one transfer, not the session, and its slot is free again.
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server!.State);
        Assert.True(h.RunUntil(() => BulkKit.SendTransfers(h.Client, 5) == 0), "the transfer record was not released");
    }

    [Fact]
    public async Task The_Receiver_Cancels_A_Transfer_And_The_Sender_Stops_Reading_Its_Source()
    {
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = Slow(router);

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * Mib), new PatternSource(4 * Mib));
        Assert.True(h.RunUntil(() => router.Accepted.Count == 1 && router.Sink<PatternSink>().BytesWritten > 0), "nothing arrived");

        Assert.True(h.Server!.CancelBulk(5, router.Accepted[0].TransferId));
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the sender was not told");

        Assert.Equal(BulkStatus.Canceled, transfer.Status);
        Assert.Equal(BulkStatus.Canceled, router.Sink<PatternSink>().Result!.Value.Status);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server!.State);

        // An id that is not running is answered without touching anything.
        Assert.False(h.Server!.CancelBulk(5, 9999));
        Assert.Throws<ArgumentException>(() => h.Server!.CancelBulk(99, 1));
    }

    [Fact]
    public async Task An_Unauthorised_Range_Request_Is_Rejected()
    {
        DenyAll authorizer = new();
        AcceptRouter router = AcceptRouter.Memory(1000);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Receiver(router),
            server: o =>
            {
                BulkKit.Quiet(o);
                o.BulkAuthorizer = authorizer;
                o.BulkProvider = new MemoryProvider(new byte[1000]);
            });

        h.Client.RequestBulk(new BulkRangeRequest(5, 11, 1, 0, 1000));
        Assert.True(h.RunUntil(() => router.Rejections.Count > 0), "the request was not answered");

        Assert.Single(authorizer.Requests);
        Assert.Equal(11UL, authorizer.Requests[0].ObjectId);
        Assert.Equal(1000, authorizer.Requests[0].Length);
        Assert.Equal(QuiclyErrorCode.BulkRejected, router.Rejections[0].Code);
        Assert.Equal(11UL, router.Rejections[0].Request.ObjectId);
        Assert.Empty(router.Accepted);

        // The pending request was released, so the application may ask again.
        Assert.Equal(0, BulkKit.Engine(h.Client).PendingRequests);
    }

    [Fact]
    public async Task A_Request_With_No_Authorizer_At_All_Is_Rejected()
    {
        AcceptRouter router = AcceptRouter.Memory(1000);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Receiver(router),
            // No BulkAuthorizer and no BulkProvider: serving objects is opt-in (ADR 0009).
            server: BulkKit.Quiet);

        h.Client.RequestBulk(new BulkRangeRequest(5, 1, 1, 0, 1000));
        Assert.True(h.RunUntil(() => router.Rejections.Count > 0), "the request was not answered");
        Assert.Equal(QuiclyErrorCode.BulkRejected, router.Rejections[0].Code);
        await Task.CompletedTask;
    }

    [Fact]
    public async Task An_Authorised_Range_Request_Is_Served_From_The_Provider()
    {
        byte[] payload = new byte[120_000];
        for (int i = 0; i < payload.Length; i++)
        {
            payload[i] = PatternSource.At(i);
        }

        MemoryProvider provider = new(payload, hash: true);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Receiver(router),
            server: BulkKit.Server(provider));

        h.Client.RequestBulk(new BulkRangeRequest(5, 4, 2, 0, payload.Length));
        Assert.True(h.RunUntil(() => router.Sinks.Count > 0 && router.Sink<MemorySink>().IsFinished), "the object did not arrive");

        Assert.Single(provider.Requests);
        Assert.Equal(4UL, provider.Requests[0].ObjectId);
        Assert.Equal(2UL, provider.Requests[0].ObjectVersion);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);
        Assert.Equal(BulkStatus.Completed, router.Sink<MemorySink>().Result!.Value.Status);
        await Task.CompletedTask;
    }

    [Fact]
    public async Task A_Resumable_Request_Is_Asked_Again_After_An_Epoch_Change()
    {
        DenyAll authorizer = new();
        AcceptRouter router = AcceptRouter.Memory(1000);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Receiver(router),
            server: o =>
            {
                BulkKit.Quiet(o);
                o.BulkAuthorizer = authorizer;
            });

        h.Client.RequestBulk(new BulkRangeRequest(5, 5, 1, 0, 1000, Resumable: true));
        Assert.True(h.RunUntil(() => authorizer.Requests.Count == 1), "the first request did not arrive");

        // PROTOCOL.md §4.1: a resumed session re-requests what was not delivered. The rejection released the slot, so the
        // request is put back for the test and then the new epoch is announced to the engine.
        h.Client.RequestBulk(new BulkRangeRequest(5, 5, 1, 0, 1000, Resumable: true));
        Assert.True(h.RunUntil(() => authorizer.Requests.Count == 2), "the second request did not arrive");
        BulkKit.Engine(h.Client).OnEpochReset(resumed: true);
        Assert.True(h.RunUntil(() => authorizer.Requests.Count == 3), "the resumable request was not asked again");

        Assert.All(authorizer.Requests, r => Assert.Equal(5UL, r.ObjectId));

        // Every re-request carries a fresh request id, so a late reject of the old one cannot cancel the new one.
        Assert.Equal(3, authorizer.Requests.Select(r => r.RequestId).Distinct().Count());
    }

    [Fact]
    public async Task A_Disconnected_Object_Is_Resumed_As_A_New_Transfer_For_The_Remaining_Range()
    {
        const int total = 600_000;
        byte[] payload = new byte[total];
        for (int i = 0; i < total; i++)
        {
            payload[i] = PatternSource.At(i);
        }

        // One sink collects the object across both sessions, exactly as an application resuming a download would.
        MemorySink sink = new(total);
        AcceptRouter router = new(_ => sink);
        long delivered;
        using (SessionHarness first = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 4_000_000, DisconnectAtMicros = 400_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router)))
        {
            BulkTransfer transfer = await first.Client.BeginBulkSendAsync(new BulkDescriptor(5, 8, 1, total), new MemorySource(payload));
            Assert.True(first.RunUntil(() => transfer.IsFinished, 20_000_000), "the transfer neither finished nor was cut");
            Assert.NotEqual(BulkStatus.Completed, transfer.Status);

            delivered = sink.BytesWritten;
            Assert.True(delivered > 0, "nothing arrived before the link was cut");
            Assert.True(delivered < total, "the object arrived whole, so nothing was resumed");
        }

        // Resume: a new transfer for the remaining range, same object identity (PROTOCOL.md §3.3).
        using (SessionHarness second = new(
            link: new LinkOptions { DelayMicros = 5_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router)))
        {
            BulkDescriptor rest = new(5, 8, 1, total, delivered, total - delivered);
            BulkTransfer transfer = await second.Client.BeginBulkSendAsync(rest, new MemorySource(payload));
            Assert.True(second.RunUntil(() => transfer.IsFinished, 20_000_000), "the resumed range did not finish");
            Assert.Equal(BulkStatus.Completed, transfer.Status);
        }

        // Byte-exact across the two sessions, and the second range's offset continued where the first stopped.
        Assert.Equal(total, sink.BytesWritten);
        Assert.Equal(payload, sink.Bytes);
        Assert.Equal(2, router.Accepted.Count);
        Assert.Equal(delivered, router.Accepted[1].Offset);
        Assert.False(router.Accepted[1].IsWholeObject);
    }

    [Fact]
    public void A_Hostile_Or_Malformed_Header_Creates_No_State_And_Resets_Only_Its_Stream()
    {
        AcceptRouter router = AcceptRouter.Memory(1024);
        using ServerHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main, server: BulkKit.Receiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        // Every one of these is refused by the framing layer before the engine sees a descriptor (PROTOCOL.md §3.3, §8).
        (string Name, byte[] Stream)[] hostile =
        [
            ("length 0", BulkKit.RawBulkStream(5, [1, 1, 1, 1024, 0, 0], 0, [])),
            ("offset + length past the object", BulkKit.RawBulkStream(5, [2, 1, 1, 1024, 1000, 100], 0, [])),
            ("reserved hash algorithm", BulkKit.RawBulkStream(5, [3, 1, 1, 1024, 0, 1024], 0x04, [])),
            ("reserved flag bit", BulkKit.RawBulkStream(5, [4, 1, 1, 1024, 0, 1024], 0x80, [])),
            ("range above the channel limit", BulkKit.RawBulkStream(7, [5, 1, 1, 5000, 0, 5000], 0, [])),
            ("hostile total length", BulkKit.RawBulkStream(5, [6, 1, 1, VarInt.MaxValue, 0, VarInt.MaxValue], 0, [])),
        ];

        long resets = h.Statistics().StreamsReset;
        foreach ((string name, byte[] stream) in hostile)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(stream, out TransportStreamId id));
            Assert.True(h.RunUntil(() => h.Statistics().StreamsReset > resets), $"'{name}' was not reset");
            resets = h.Statistics().StreamsReset;

            // No descriptor reached the application, no transfer record was taken and no receive buffer was rented: a
            // hostile length never becomes an allocation.
            Assert.Empty(router.Accepted);
            Assert.Equal(0, BulkKit.Engine(h.Server!).ReceiveTransfers);
            Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
            Assert.Equal(PeerState.Connected, h.Server!.State);
        }

        // The channel still works: a well-formed transfer on it is accepted after all of that.
        byte[] body = new byte[512];
        BulkHeader header = new() { TransferId = 20, ObjectId = 1, ObjectVersion = 1, TotalLength = 512, Offset = 0, Length = 512 };
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.BulkStream(5, in header, body), out _, fin: true));
        Assert.True(h.RunUntil(() => router.Accepted.Count == 1), "a valid transfer was refused after the hostile ones");
        Assert.Equal(BulkStatus.Completed, router.Sink<MemorySink>().Result!.Value.Status);
    }

    [Fact]
    public void A_Huge_Declared_Object_Size_Never_Becomes_An_Allocation()
    {
        // A one-byte range at the end of a 2^62-1 byte object is perfectly well formed: TotalLength, Offset and Length are
        // untrusted (PROTOCOL.md §3.3), so what matters is that the receiver sizes nothing from them. The transfer is
        // accepted and costs one byte, not four exabytes.
        CountingSink sink = new();
        AcceptRouter router = new(_ => sink);
        using ServerHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main, server: BulkKit.Receiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        BulkHeader header = new()
        {
            TransferId = 1,
            ObjectId = 1,
            ObjectVersion = 1,
            TotalLength = VarInt.MaxValue,
            Offset = VarInt.MaxValue - 1,
            Length = 1,
        };
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.BulkStream(5, in header, new byte[1]), out _, fin: true));
        Assert.True(h.RunUntil(() => sink.Result is not null), "the one-byte range did not complete");

        Assert.Equal(1, sink.BytesWritten);
        Assert.Equal(BulkStatus.Completed, sink.Result!.Value.Status);
        Assert.Equal((long)VarInt.MaxValue, router.Accepted[0].TotalLength);
        Assert.False(router.Accepted[0].IsWholeObject);
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void A_Range_Whose_Bytes_Do_Not_Match_Its_Trailer_Fails_That_Range()
    {
        // The transport cannot corrupt a range — QUIC.s AEAD discards anything the wire damages — so the corrupt one is
        // written by hand. What is under test is the receiver.s reaction: this range fails, and it says why.
        byte[] payload = new byte[4096];
        new Random(7).NextBytes(payload);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using ServerHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main, server: BulkKit.Receiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        BulkHeader header = new()
        {
            TransferId = 1,
            ObjectId = 9,
            ObjectVersion = 1,
            TotalLength = (ulong)payload.Length,
            Offset = 0,
            Length = (ulong)payload.Length,
            Flags = BulkFlags.ChecksumPresent,
        };

        // A trailer that does not describe the body.
        byte[] stream = BulkKit.BulkStream(5, in header, payload, checksum: XxHash64.Hash(payload) ^ 1);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(stream, out _, fin: true));
        Assert.True(h.RunUntil(() => router.Sinks.Count > 0 && router.Sink<MemorySink>().IsFinished), "the range never finished");

        BulkResult result = router.Sink<MemorySink>().Result!.Value;
        Assert.Equal(BulkStatus.Failed, result.Status);
        Assert.Equal(QuiclyErrorCode.BulkChecksumFailed, result.Code);

        // The bytes were delivered before the trailer condemned them, which is why a sink writes at absolute offsets and a
        // retry overwrites. And the range is never reported fully accepted, so its sender cannot complete underneath.
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);
        Assert.True(result.BytesTransferred < payload.Length, "a failed range must not be reported as fully accepted");
        Assert.Equal(1, h.Statistics().BulkChecksumFailures);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void A_Duplicate_Transfer_Id_Resets_The_Second_Stream_Only()
    {
        AcceptRouter router = AcceptRouter.Memory(4096);
        using ServerHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main, server: BulkKit.Receiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        // The first stream stays open (its body is incomplete), so the id is still live when the second arrives.
        BulkHeader header = new() { TransferId = 77, ObjectId = 1, ObjectVersion = 1, TotalLength = 4096, Offset = 0, Length = 4096 };
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.BulkStream(5, in header, new byte[16]), out _));
        Assert.True(h.RunUntil(() => router.Accepted.Count == 1), "the first transfer was not accepted");

        long resets = h.Statistics().StreamsReset;
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.BulkStream(5, in header, new byte[16]), out _));
        Assert.True(h.RunUntil(() => h.Statistics().StreamsReset > resets), "the duplicate id was not reset");

        // PROTOCOL.md §3.3: a transfer id is unique per peer and direction; the first transfer is untouched.
        Assert.Single(router.Accepted);
        Assert.False(router.Sink<MemorySink>().IsFinished);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void More_Peer_Streams_Than_The_Channel_Allows_Are_Reset()
    {
        AcceptRouter router = AcceptRouter.Memory(4096);
        using ServerHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main, server: BulkKit.Receiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        // Channel 6 grants one concurrent transfer (MaxGroups 1, PROTOCOL.md §7).
        BulkHeader first = new() { TransferId = 1, ObjectId = 1, ObjectVersion = 1, TotalLength = 4096, Offset = 0, Length = 4096 };
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.BulkStream(6, in first, new byte[16]), out _));
        Assert.True(h.RunUntil(() => BulkKit.ReceiveStreams(h.Server!, 6) == 1), "the first stream was not accepted");

        long resets = h.Statistics().StreamsReset;
        BulkHeader second = new() { TransferId = 2, ObjectId = 2, ObjectVersion = 1, TotalLength = 4096, Offset = 0, Length = 4096 };
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.BulkStream(6, in second, new byte[16]), out _));
        Assert.True(h.RunUntil(() => h.Statistics().StreamsReset > resets), "the second stream was not reset");

        Assert.Equal(1, BulkKit.ReceiveStreams(h.Server!, 6));
        Assert.Single(router.Accepted);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void A_Chunk_This_Peer_Could_Never_Stage_Resets_Its_Stream()
    {
        AcceptRouter router = AcceptRouter.Memory(400_000);
        using ServerHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main, server: BulkKit.Receiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        // A chunk whose decoded size is above min(BulkMaxChunk, the pool's largest block) could never be staged, so the
        // stream is reset rather than pended for ever (the §7.7 sizing rule).
        BulkHeader header = new() { TransferId = 1, ObjectId = 1, ObjectVersion = 1, TotalLength = 400_000, Offset = 0, Length = 400_000, Flags = BulkFlags.Chunked };
        byte[] chunk = new byte[64];
        byte[] body = new byte[16 + chunk.Length];
        int position = StreamFraming.WriteBulkChunkHeader(body, chunk.Length, 400_000);
        chunk.CopyTo(body.AsSpan(position));
        byte[] stream = BulkKit.BulkStream(5, in header, body.AsSpan(0, position + chunk.Length));

        long resets = h.Statistics().StreamsReset;
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(stream, out _));
        Assert.True(h.RunUntil(() => h.Statistics().StreamsReset > resets), "the oversized chunk was not reset");

        Assert.Single(router.Accepted);
        Assert.Equal(BulkStatus.Failed, router.Sink<MemorySink>().Result!.Value.Status);
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public async Task Closing_The_Session_Finishes_A_Running_Transfer_On_Both_Sides()
    {
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = Slow(router);

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * Mib), new PatternSource(4 * Mib));
        Assert.True(h.RunUntil(() => router.Sinks.Count == 1 && router.Sink<PatternSink>().BytesWritten > 0), "nothing arrived");

        h.Client.Close(CloseReason.Normal);
        Assert.True(h.RunUntilClosed(), "the session did not close");

        Assert.Equal(BulkStatus.Disconnected, transfer.Status);
        Assert.True(router.Sink<PatternSink>().IsFinished);
        Assert.Equal(BulkStatus.Disconnected, router.Sink<PatternSink>().Result!.Value.Status);
    }

    /// <summary>A link slow enough that a multi-megabyte transfer is still running when a test acts on it.</summary>
    private static SessionHarness Slow(IBulkRouter router) => new(
        link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 4_000_000 },
        table: BulkTables.Main,
        client: BulkKit.Quiet,
        server: BulkKit.Receiver(router));
}
