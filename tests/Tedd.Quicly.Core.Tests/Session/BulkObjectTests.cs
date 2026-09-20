using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The bulk <em>object</em> driver: objects larger than one transfer's <c>MaxMessageSize</c> (PROTOCOL.md §8) crossing as
/// one object — one begin, one sink, one completion, and a checksum on every range it took.
/// </summary>
public class BulkObjectTests
{
    private const int Mib = 1024 * 1024;

    /// <summary>A Bulk channel whose transfers are capped well below the objects these tests send, so every object is
    /// several ranges without moving 16 MiB of test data.</summary>
    private static ChannelTable SmallRanges { get; } = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(5, "objects", ChannelMode.Bulk, o =>
        {
            o.Priority = 0;
            o.MaxMessageSize = 64 * 1024;
            o.MaxGroups = 4;
        })
        .Build();

    [Fact]
    public void An_Object_Larger_Than_A_Transfer_Arrives_As_One_Object()
    {
        // Ten ranges' worth: the channel caps one transfer at 64 KiB, so this can only arrive as a multi-range object.
        byte[] payload = Payload(640 * 1024);
        AcceptObjectRouter router = AcceptObjectRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 2_000 },
            table: SmallRanges,
            client: BulkKit.Quiet,
            server: BulkKit.ObjectReceiver(router));

        BulkObjectDescriptor descriptor = new(5, 7, 1, payload.Length);
        BulkObjectTransfer transfer = h.Client.BeginBulkObjectSend(in descriptor, new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the object did not finish");
        Assert.True(h.Server!.LastCallbackFault is null, $"the receiver faulted: {h.Server!.LastCallbackFault}");
        Assert.True(h.Client.LastCallbackFault is null, $"the sender faulted: {h.Client.LastCallbackFault}");

        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload.Length, transfer.BytesTransferred);
        Assert.Equal(1, transfer.Progress);
        Assert.Equal(10, transfer.Result.Ranges);

        // One object as far as the application is concerned: the router chose a target once, for ten transfers.
        Assert.Single(router.Accepted);
        Assert.Equal(7UL, router.Accepted[0].ObjectId);
        Assert.Equal(payload.Length, router.Accepted[0].TotalLength);
        Assert.Equal(0, router.Accepted[0].Offset);
        Assert.True(router.Accepted[0].HasChecksum);

        MemoryObjectSink sink = router.Sink<MemoryObjectSink>();
        Assert.Equal(payload, sink.Bytes);
        Assert.True(sink.IsFinished);
        BulkObjectResult result = sink.Result!.Value;
        Assert.Equal(BulkStatus.Completed, result.Status);
        Assert.Equal(payload.Length, result.BytesTransferred);
        Assert.Equal(10, result.Ranges);

        // Ten ranges, ten checksums, all of them good — and not one retry needed.
        Assert.Equal(QuiclyErrorCode.NoError, result.Code);
        Assert.Equal(0, result.Retries);
        Assert.Equal(0, h.Client.LiveBulkObjectSends);
    }

    [Fact]
    public void An_Object_That_Fits_One_Transfer_Is_Reported_The_Same_Way()
    {
        byte[] payload = Payload(20_000);
        AcceptObjectRouter router = AcceptObjectRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: SmallRanges,
            client: BulkKit.Quiet,
            server: BulkKit.ObjectReceiver(router));

        BulkObjectDescriptor descriptor = new(5, 3, 1, payload.Length);
        BulkObjectTransfer transfer = h.Client.BeginBulkObjectSend(in descriptor, new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the object did not finish");

        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(1, transfer.Result.Ranges);
        MemoryObjectSink sink = router.Sink<MemoryObjectSink>();
        Assert.Equal(payload, sink.Bytes);

        // One range that happens to be the whole object: the application cannot tell it from the ten-range case above.
        Assert.Equal(QuiclyErrorCode.NoError, sink.Result!.Value.Code);
    }

    [Fact]
    public void A_Range_That_Fails_Its_Checksum_Is_Forgotten_And_Taken_Again()
    {
        // The point of checksumming per range rather than per object: one bad range costs one range. The receiving driver
        // forgets it — so it is neither counted nor blocking — and takes it again at the same offsets when it is re-sent.
        // Hand-built streams, because a range cannot be corrupted on a wire QUIC has already authenticated.
        byte[] payload = new byte[4096];
        new Random(11).NextBytes(payload);
        AcceptObjectRouter router = AcceptObjectRouter.Memory(payload.Length);
        using ServerHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            server: BulkKit.ObjectReceiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        // First half, with a trailer that does not describe it.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(Range(1, payload, 0, 2048, good: false), out _, fin: true));
        Assert.True(
            h.RunUntil(() => h.Statistics().BulkChecksumFailures == 1),
            "the corrupt range was not detected");
        Assert.False(router.Sink<MemoryObjectSink>().IsFinished, "one bad range must not end the object");

        // The same range again, this time intact. The driver forgot the first attempt, so this is not a duplicate.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(Range(2, payload, 0, 2048, good: true), out _, fin: true));
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(Range(3, payload, 2048, 2048, good: true), out _, fin: true));
        Assert.True(h.RunUntil(() => router.Sink<MemoryObjectSink>().IsFinished), "the object never finished");

        BulkObjectResult result = router.Sink<MemoryObjectSink>().Result!.Value;
        Assert.Equal(BulkStatus.Completed, result.Status);
        Assert.Equal(payload.Length, result.BytesTransferred);
        Assert.Equal(1, result.Retries);
        Assert.Equal(payload, router.Sink<MemoryObjectSink>().Bytes);

        // One object throughout, and the session never noticed.
        Assert.Single(router.Accepted);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    /// <summary>One range of <paramref name="payload"/> as a hand-built bulk stream, with a right or wrong trailer.</summary>
    private static byte[] Range(ulong transferId, byte[] payload, int offset, int length, bool good)
    {
        BulkHeader header = new()
        {
            TransferId = transferId,
            ObjectId = 11,
            ObjectVersion = 1,
            TotalLength = (ulong)payload.Length,
            Offset = (ulong)offset,
            Length = (ulong)length,
            Flags = BulkFlags.ChecksumPresent,
        };

        ReadOnlySpan<byte> body = payload.AsSpan(offset, length);
        return BulkKit.BulkStream(5, in header, body, checksum: good ? null : XxHash64.Hash(body) ^ 1);
    }

    [Fact]
    public void An_Object_Sent_Without_A_Hash_Completes_And_Claims_Nothing()
    {
        byte[] payload = Payload(200_000);
        AcceptObjectRouter router = AcceptObjectRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: SmallRanges,
            client: BulkKit.Quiet,
            server: BulkKit.ObjectReceiver(router));

        BulkObjectTransfer transfer = h.Client.BeginBulkObjectSend(new BulkObjectDescriptor(5, 4, 1, payload.Length), new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.IsFinished && router.Sink<MemoryObjectSink>().IsFinished), "the object did not finish");

        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemoryObjectSink>().Bytes);
        Assert.Equal(QuiclyErrorCode.NoError, router.Sink<MemoryObjectSink>().Result!.Value.Code);

        // The sending end never claims a verdict: it computed nothing, and the hash is the receiver's answer.
        Assert.Equal(QuiclyErrorCode.NoError, transfer.Result.Code);
    }

    [Fact]
    public async Task SendBulkObjectAsync_Awaits_The_Whole_Object_And_Reports_Progress_On_Both_Ends()
    {
        byte[] payload = Payload(512 * 1024);
        AcceptObjectRouter router = AcceptObjectRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 2_000 },
            table: SmallRanges,
            client: options =>
            {
                BulkKit.Quiet(options);
                options.BulkObjectProgressBytes = 64 * 1024;
            },
            server: options =>
            {
                BulkKit.ObjectReceiver(router)(options);
                options.BulkObjectProgressBytes = 64 * 1024;
            });

        List<BulkObjectProgress> sent = [];
        BulkObjectDescriptor descriptor = new(5, 21, 1, payload.Length);
        Task<BulkObjectResult> completion = h.Client.SendBulkObjectAsync(descriptor, new MemorySource(payload), (in BulkObjectProgress p) => sent.Add(p));

        // Driven off the receiver's own completion, not the task's: the task resolves on the thread pool, and the harness
        // runs on virtual time, so waiting for IsCompleted here would be waiting on the pool with the clock racing ahead.
        Assert.True(h.RunUntil(() => router.Sinks.Count > 0 && router.Sink<MemoryObjectSink>().IsFinished), "the object did not finish");
        h.Run(200_000);

        BulkObjectResult result = await completion;
        Assert.Equal(BulkStatus.Completed, result.Status);
        Assert.Equal(payload.Length, result.BytesTransferred);
        Assert.Equal(QuiclyErrorCode.NoError, router.Sink<MemoryObjectSink>().Result!.Value.Code);

        // Progress on both ends: reported in object terms, never above the object, and rising.
        Assert.NotEmpty(sent);
        Assert.NotEmpty(router.Progress);
        Assert.All(sent, p => Assert.Equal(payload.Length, p.Length));
        Assert.All(sent, p => Assert.InRange(p.Fraction, 0, 1));
        Assert.All(router.Progress, p => Assert.InRange(p.BytesTransferred, 0, payload.Length));
        for (int i = 1; i < sent.Count; i++)
        {
            Assert.True(sent[i].BytesTransferred >= sent[i - 1].BytesTransferred, "the sender's progress went backwards");
        }
    }

    [Fact]
    public void Cancelling_An_Object_Stops_It_On_Both_Ends()
    {
        AcceptObjectRouter router = AcceptObjectRouter.Pattern();
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 20_000_000, MaxQueueBytes = Mib },
            table: SmallRanges,
            client: BulkKit.Quiet,
            server: options =>
            {
                BulkKit.ObjectReceiver(router)(options);

                // The backstop, in case the cancel lands on a range boundary with no stream open to reset. Short so the
                // test never depends on which of the two paths ends the object — both are correct, and both are covered.
                options.BulkObjectIdleTimeout = TimeSpan.FromSeconds(1);
            });

        const long total = 8 * Mib;
        BulkObjectTransfer transfer = h.Client.BeginBulkObjectSend(new BulkObjectDescriptor(5, 33, 1, total), new PatternSource(total));
        Assert.True(h.RunUntil(() => transfer.BytesTransferred > 128 * 1024), "the object never got going");
        transfer.Cancel();

        Assert.True(h.RunUntil(() => transfer.IsFinished), "the cancelled object did not finish");
        Assert.Equal(BulkStatus.Canceled, transfer.Status);
        Assert.True(transfer.BytesTransferred < total, "a cancelled object should not have carried everything");

        // The receiver's object ends too, rather than waiting for bytes that are never coming.
        PatternObjectSink sink = router.Sink<PatternObjectSink>();
        Assert.True(h.RunUntil(() => sink.IsFinished, maxMicros: 30_000_000), "the receiving object did not finish");
        Assert.Contains(sink.Result!.Value.Status, (BulkStatus[])[BulkStatus.Canceled, BulkStatus.Failed]);
        Assert.True(sink.BytesWritten < total, "the receiver should not have assembled the whole object");
        Assert.Equal(0, sink.Mismatches);
        Assert.Equal(0, h.Client.LiveBulkObjectSends);
    }

    [Fact]
    public void A_Sender_That_Stops_Between_Ranges_Does_Not_Strand_The_Receiving_Object()
    {
        // The case with no stream to reset: the sender abandons the object between two ranges, so the peer sees an object
        // that simply stops growing. Only the idle sweep can end it, and it must — a stranded object would hold its slot,
        // its reorder shadow and the application's wait for the life of the connection.
        AcceptObjectRouter router = AcceptObjectRouter.Pattern();
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 2_000 },
            table: SmallRanges,
            client: BulkKit.Quiet,
            server: options =>
            {
                BulkKit.ObjectReceiver(router)(options);
                options.BulkObjectIdleTimeout = TimeSpan.FromSeconds(2);
            });

        // One range of a much larger declared object, sent as a plain transfer so no driver follows it up: the receiving
        // driver sees an object that begins, takes one range and then hears nothing more, for ever.
        const long total = 4 * Mib;
        BulkTransfer range = h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 55, 1, total, 0, 64 * 1024), new PatternSource(total))
            .AsTask().GetAwaiter().GetResult();
        Assert.True(h.RunUntil(() => range.IsFinished), "the lone range did not finish");
        Assert.Equal(BulkStatus.Completed, range.Status);

        PatternObjectSink sink = router.Sink<PatternObjectSink>();
        Assert.False(sink.IsFinished, "the object finished before anything could have told it to");

        Assert.True(h.RunUntil(() => sink.IsFinished, maxMicros: 30_000_000), "the stranded object was never given up on");
        Assert.Equal(BulkStatus.Failed, sink.Result!.Value.Status);
        Assert.Equal(QuiclyErrorCode.Timeout, sink.Result!.Value.Code);
        Assert.Equal(64 * 1024, sink.BytesWritten);
        Assert.Equal(0, sink.Mismatches);
    }

    [Fact]
    public void A_Peer_Without_An_Object_Router_Refuses_The_Object()
    {
        byte[] payload = Payload(200_000);
        DenyObjectRouter router = new();
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: SmallRanges,
            client: BulkKit.Quiet,
            server: BulkKit.ObjectReceiver(router));

        BulkObjectTransfer transfer = h.Client.BeginBulkObjectSend(new BulkObjectDescriptor(5, 5, 1, payload.Length), new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the refused object did not finish");

        Assert.Equal(BulkStatus.Failed, transfer.Status);
        Assert.True(router.Calls >= 1);
        Assert.Equal(0, h.Client.LiveBulkObjectSends);
    }

    [Fact]
    public void A_Multi_Gigabyte_Object_Crosses_Byte_Exact_While_Movement_Datagrams_Keep_Flowing()
    {
        // Two GiB through a 64 KiB transfer cap: 32 768 ranges, none of which the application ever sees. Neither end holds
        // the object — the source computes its bytes and the sink checks them — so this is a real size test, not a copy.
        const long total = 2L * 1024 * Mib;
        AcceptObjectRouter router = AcceptObjectRouter.Pattern();
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000, BandwidthBitsPerSecond = 10_000_000_000, MaxQueueBytes = 4 * Mib },
            table: SmallRanges,
            client: options =>
            {
                BulkKit.Quiet(options);
                options.BulkMaxBytesPerSecond = 0;
            },
            server: BulkKit.ObjectReceiver(router));

        int moves = 0;
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => moves++);

        BulkObjectTransfer transfer = h.Client.BeginBulkObjectSend(new BulkObjectDescriptor(5, 99, 1, total), new PatternSource(total));
        int ticks = 0;
        Assert.True(
            h.RunUntil(
                () =>
                {
                    if (++ticks % 17 == 0)
                    {
                        h.Client.SendCopy(new SendHeader(2), new byte[32]);
                    }

                    return transfer.IsFinished;
                },
                maxMicros: 600_000_000,
                step: 1_000),
            $"the object stalled at {transfer.BytesTransferred} of {total} bytes ({transfer.RangesCompleted} ranges)");

        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(total, transfer.BytesTransferred);
        Assert.Equal(total / (64 * 1024), transfer.Result.Ranges);

        PatternObjectSink sink = router.Sink<PatternObjectSink>();
        Assert.True(h.RunUntil(() => sink.IsFinished), "the receiving object did not finish");
        Assert.Equal(total, sink.BytesWritten);
        Assert.Equal(0, sink.Mismatches);
        Assert.Single(router.Accepted);

        // The real-time channel kept flowing the whole way: bulk is windowed and rate-capped, so 2 GiB of stream data
        // never became a queue the datagrams had to wait behind.
        Assert.True(moves > 100, $"only {moves} movement datagrams arrived");
        Assert.True(h.Client.State == PeerState.Connected, $"the session ended early: {h.Client.State}");
    }

    [Fact]
    public void An_Object_On_The_Shipped_Defaults_Verifies_Across_Its_Ranges()
    {
        // Nothing configured: the channel allows 16 MiB transfers, and the driver's own defaults decide the range size and
        // the receiver's reorder shadow. This is the case a user gets without reading any of the options, and the hash has
        // to survive it — the two defaults are sized as a pair for exactly this.
        const long total = 8 * Mib;
        AcceptObjectRouter router = AcceptObjectRouter.Pattern();
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 1_000_000_000, MaxQueueBytes = 4 * Mib },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.ObjectReceiver(router));

        BulkObjectDescriptor descriptor = new(5, 77, 1, total);
        BulkObjectTransfer transfer = h.Client.BeginBulkObjectSend(in descriptor, new PatternSource(total));
        Assert.True(h.RunUntil(() => transfer.IsFinished, maxMicros: 120_000_000), $"the object stalled at {transfer.BytesTransferred}");

        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(8, transfer.Result.Ranges); // 8 MiB in the default 1 MiB ranges

        PatternObjectSink sink = router.Sink<PatternObjectSink>();
        Assert.True(h.RunUntil(() => sink.IsFinished), "the receiving object did not finish");
        Assert.Equal(0, sink.Mismatches);
        Assert.Equal(total, sink.BytesWritten);
        Assert.Equal(QuiclyErrorCode.NoError, sink.Result!.Value.Code);
    }

    /// <summary>The SHA-256 of <see cref="PatternSource"/>'s first <paramref name="total"/> bytes, computed streaming.</summary>
    private static byte[] PatternHash(long total)
    {
        using System.Security.Cryptography.IncrementalHash hash =
            System.Security.Cryptography.IncrementalHash.CreateHash(System.Security.Cryptography.HashAlgorithmName.SHA256);
        byte[] block = new byte[64 * 1024];
        PatternSource source = new(total);
        for (long offset = 0; offset < total; offset += block.Length)
        {
            int take = source.Read(offset, block);
            hash.AppendData(block.AsSpan(0, take));
        }

        return hash.GetHashAndReset();
    }

    private static byte[] Payload(int length)
    {
        byte[] bytes = new byte[length];
        Random random = new(length);
        random.NextBytes(bytes);
        return bytes;
    }
}
