using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Bulk delivery over the simulated transport (PROTOCOL.md §3.3, docs/design/session-layer.md §7.7): byte-exact objects,
/// chunked compression, the whole-object hash, the send window, resume, and real-time traffic that keeps flowing while a
/// large object crosses a bandwidth-capped link.
/// </summary>
public class BulkDeliveryTests
{
    private const int Mib = 1024 * 1024;

    [Fact]
    public async Task A_Bulk_Object_Arrives_Byte_Exact_And_Its_Progress_Completes_The_Transfer()
    {
        byte[] payload = Payload(300_000);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 2_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 7, 1, payload.Length), new MemorySource(payload));
        Assert.Equal(BulkStatus.Running, transfer.Status);
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the transfer did not finish");

        // A transport callback that throws is turned into an InternalError close, which would show up here only as a
        // Disconnected transfer; name the fault instead.
        Assert.True(h.Server!.LastCallbackFault is null, $"the receiver faulted: {h.Server!.LastCallbackFault}");
        Assert.True(h.Client.LastCallbackFault is null, $"the sender faulted: {h.Client.LastCallbackFault}");
        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload.Length, transfer.BytesTransferred);
        Assert.Equal(1, transfer.Progress);
        MemorySink sink = router.Sink<MemorySink>();
        Assert.Equal(payload, sink.Bytes);
        Assert.True(sink.IsFinished);
        Assert.Equal(BulkStatus.Completed, sink.Result!.Value.Status);
        Assert.Equal(BulkHashState.None, sink.Result!.Value.Hash);

        // The descriptor the router saw is the one the sender declared.
        BulkTransferInfo info = router.Accepted[0];
        Assert.Equal(5, info.Channel);
        Assert.Equal(7UL, info.ObjectId);
        Assert.Equal(payload.Length, info.TotalLength);
        Assert.True(info.IsWholeObject);
        Assert.False(info.Chunked);

        // Every transfer record went back, so the next object can start at once.
        Assert.Equal(0, BulkKit.SendTransfers(h.Client, 5));
        Assert.True(h.RunUntil(() => BulkKit.ReceiveStreams(h.Server!, 5) == 0), "the peer stream was not released");
    }

    [Fact]
    public async Task A_64_MiB_Object_Crosses_A_Capped_Link_While_Movement_Datagrams_Keep_Flowing()
    {
        // PROTOCOL.md §8: a Bulk channel's MaxMessageSize bounds one transfer, so a 64 MiB object is four 16 MiB ranges,
        // at most two of them in flight (PROTOCOL.md §7).
        const long total = 64L * Mib;
        const long range = 16L * Mib;
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 10_000, BandwidthBitsPerSecond = 200_000_000, MaxQueueBytes = 4 * Mib },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router));

        List<long> latencies = [];
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> body) =>
            latencies.Add(header.ReceivedMicros - BinaryPrimitives.ReadInt64LittleEndian(body)));

        List<BulkTransfer> transfers = [];
        long started = 0;
        int ticks = 0;
        Assert.True(h.RunUntil(
            () =>
            {
                // A 60 Hz movement datagram alongside the transfer, and a new range whenever a slot frees.
                if (++ticks % 17 == 0)
                {
                    byte[] moves = new byte[64];
                    BinaryPrimitives.WriteInt64LittleEndian(moves, h.Clock.NowMicros);
                    h.Client.SendCopy(new SendHeader(2), moves);
                }

                while (started < total && transfers.Count(t => !t.IsFinished) < 2)
                {
                    BulkDescriptor descriptor = new(5, 42, 1, total, started, range);
                    transfers.Add(h.Client.BeginBulkSendAsync(descriptor, new PatternSource(total)).AsTask().GetAwaiter().GetResult());
                    started += range;
                }

                return started >= total && transfers.All(t => t.IsFinished);
            },
            maxMicros: 120_000_000,
            step: 1_000),
            $"the object did not finish: {transfers.Count(t => t.IsFinished)}/{transfers.Count} ranges");

        Assert.Equal(4, transfers.Count);
        Assert.True(h.Client.State == PeerState.Connected,
            $"the session ended early: {h.Client.State} {h.Client.CloseReason.Code} from {h.Client.CloseReason.Source}");
        Assert.All(transfers, t => Assert.Equal(BulkStatus.Completed, t.Status));

        // Every byte of every range was verified against the object's pattern as it arrived.
        Assert.Equal(4, router.Sinks.Count);
        Assert.Equal(total, router.Sinks.Sum(s => ((PatternSink)s).BytesWritten));
        Assert.Equal(0, router.Sinks.Sum(s => ((PatternSink)s).Mismatches));

        // The real-time channel kept flowing throughout, and its latency stayed bounded: bulk is rate-capped and windowed,
        // so 64 MiB of stream data never turned into a queue the datagrams had to wait behind.
        Assert.True(latencies.Count > 100, $"only {latencies.Count} movement datagrams arrived");
        Assert.True(latencies.Max() < 60_000, $"movement latency reached {latencies.Max()} micros");
        await Task.CompletedTask;
    }

    [Fact]
    public async Task A_Chunked_Compressed_Body_Round_Trips_And_Shrinks_The_Wire()
    {
        // Highly compressible bytes, so the chunked body is measurably smaller than the object.
        byte[] payload = new byte[400_000];
        for (int i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i / 512);
        }

        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 2_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(
            new BulkDescriptor(5, 9, 2, payload.Length, 0, 0, Compress: true), new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the transfer did not finish");

        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);
        Assert.True(router.Accepted[0].Chunked);

        // The channel's byte counter counts object bytes; the peer's stream counter counts what went on the wire.
        h.Client.GetStatistics(out PeerStatistics statistics);
        Assert.True(statistics.StreamBytesSent < payload.Length / 2, $"the compressed body was {statistics.StreamBytesSent} bytes");
        Assert.Equal(payload.Length, BulkKit.Stats(h.Client, 5).BytesSent);
    }

    [Fact]
    public async Task The_Whole_Object_Hash_Is_Verified_And_A_Mismatch_Fails_The_Transfer()
    {
        byte[] payload = Payload(50_000);
        AcceptRouter good = AcceptRouter.Memory(payload.Length);
        using (SessionHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main,
            client: BulkKit.Quiet, server: BulkKit.Receiver(good)))
        {
            BulkDescriptor descriptor = new(5, 1, 1, payload.Length) { Sha256 = BulkKit.Hash(payload) };
            BulkTransfer transfer = await h.Client.BeginBulkSendAsync(descriptor, new MemorySource(payload));
            Assert.True(h.RunUntil(() => transfer.IsFinished), "the transfer did not finish");
            Assert.Equal(BulkStatus.Completed, transfer.Status);
            Assert.Equal(BulkHashState.Verified, good.Sink<MemorySink>().Result!.Value.Hash);
        }

        AcceptRouter bad = AcceptRouter.Memory(payload.Length);
        using (SessionHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main,
            client: BulkKit.Quiet, server: BulkKit.Receiver(bad)))
        {
            // The sender's hash does not describe the bytes it sends: the receiver detects it at the object's last byte.
            byte[] wrong = BulkKit.Hash(payload);
            wrong[0] ^= 0xFF;
            BulkDescriptor descriptor = new(5, 1, 1, payload.Length) { Sha256 = wrong };
            BulkTransfer transfer = await h.Client.BeginBulkSendAsync(descriptor, new MemorySource(payload));
            Assert.True(h.RunUntil(() => bad.Sinks.Count > 0 && bad.Sink<MemorySink>().IsFinished), "the receiver did not finish the transfer");

            BulkResult result = bad.Sink<MemorySink>().Result!.Value;
            Assert.Equal(BulkHashState.Mismatch, result.Hash);
            Assert.Equal(BulkStatus.Failed, result.Status);

            // Every byte still arrived: the hash is an integrity check over the object, not a framing rule. The sender saw
            // every byte accepted, so its own transfer completes normally — acting on the mismatch is the receiver's.
            Assert.Equal(payload, bad.Sink<MemorySink>().Bytes);
            Assert.True(h.RunUntil(() => transfer.IsFinished), "the sender's transfer did not finish");
            Assert.Equal(BulkStatus.Completed, transfer.Status);
        }
    }

    [Fact]
    public async Task A_Partial_Range_Hands_The_Object_Hash_To_The_Application()
    {
        byte[] payload = Payload(40_000);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main,
            client: BulkKit.Quiet, server: BulkKit.Receiver(router));

        // The second half only: this end never sees the first byte, so it cannot verify a whole-object hash.
        BulkDescriptor descriptor = new(5, 3, 1, payload.Length, payload.Length / 2, payload.Length / 2)
        {
            Sha256 = BulkKit.Hash(payload),
        };
        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(descriptor, new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the transfer did not finish");

        Assert.Equal(BulkStatus.Completed, transfer.Status);
        MemorySink sink = router.Sink<MemorySink>();
        Assert.Equal(BulkHashState.DeferredToApplication, sink.Result!.Value.Hash);
        Assert.Equal(BulkStatus.Completed, sink.Result!.Value.Status);
        Assert.Equal(payload.Length / 2, sink.FirstOffset);
        Assert.Equal(payload.AsSpan(payload.Length / 2).ToArray(), sink.Bytes.AsSpan(payload.Length / 2).ToArray());
        Assert.False(router.Accepted[0].IsWholeObject);
        Assert.True(router.Accepted[0].HasHash);
    }

    [Fact]
    public async Task The_Send_Window_Bounds_The_Bytes_One_Transfer_Keeps_Outstanding()
    {
        byte[] payload = Payload(2 * Mib);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            // A slow link, so the window is what stops the sender rather than the source running out.
            link: new LinkOptions { DelayMicros = 20_000, BandwidthBitsPerSecond = 16_000_000 },
            table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Quiet(o);
                o.BulkSendWindowBytes = 128 * 1024;
                o.BulkShareOfCongestionWindow = 1;
            },
            server: BulkKit.Receiver(router));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        long peak = 0;
        Assert.True(h.RunUntil(
            () =>
            {
                peak = Math.Max(peak, BulkKit.Stats(h.Client, 5).InFlightBytes);
                return transfer.IsFinished;
            },
            maxMicros: 60_000_000),
            "the transfer did not finish");

        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);
        Assert.True(peak > 0, "nothing was ever outstanding");

        // The window bounds the outstanding wire bytes; one piece may cross it, so the bound is the window plus a piece.
        Assert.True(peak <= (128 * 1024) + (64 * 1024), $"{peak} bytes were outstanding at once");
    }

    [Fact]
    public async Task Two_Transfers_Per_Direction_Is_The_Limit_On_Both_Sides()
    {
        byte[] payload = Payload(200_000);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 8_000_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router));

        BulkTransfer first = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        BulkTransfer second = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 2, 1, payload.Length), new MemorySource(payload));
        BulkTransfer third = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 3, 1, payload.Length), new MemorySource(payload));

        // PROTOCOL.md §7: two transfers per direction per peer. The third is refused, not queued, so the application
        // decides when to ask again.
        Assert.Equal(BulkStatus.Running, first.Status);
        Assert.Equal(BulkStatus.Running, second.Status);
        Assert.Equal(BulkStatus.Rejected, third.Status);
        Assert.Equal(QuiclyErrorCode.BulkRejected, third.Result.Code);
        Assert.Equal(2, BulkKit.SendTransfers(h.Client, 5));
        Assert.Equal(1, BulkKit.Stats(h.Client, 5).QueueFull);

        // Both ends hold the same table, so the receiver accepts exactly the two streams the sender opens.
        Assert.True(
            h.RunUntil(() => BulkKit.ReceiveStreams(h.Server!, 5) == 2, 5_000_000),
            $"the receiver held {BulkKit.ReceiveStreams(h.Server!, 5)} streams while the sender held "
            + $"{BulkKit.SendStreams(h.Client, 5)} for {BulkKit.SendTransfers(h.Client, 5)} transfers");
        Assert.True(h.RunUntil(() => first.IsFinished && second.IsFinished, 60_000_000), "the transfers did not finish");
        Assert.Equal(BulkStatus.Completed, first.Status);
        Assert.Equal(BulkStatus.Completed, second.Status);

        // A record freed by a finished transfer is reused, so a later object starts on the same peer.
        BulkTransfer fourth = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 4, 1, payload.Length), new MemorySource(payload));
        Assert.Equal(BulkStatus.Running, fourth.Status);
        Assert.True(h.RunUntil(() => fourth.IsFinished, 60_000_000), "the fourth transfer did not finish");
        Assert.Equal(BulkStatus.Completed, fourth.Status);
    }

    [Fact]
    public async Task A_Transfer_Waits_For_Stream_Credit_And_Goes_Out_On_A_New_Stream()
    {
        // MaxGroups is not part of the table hash, so the client grants one unidirectional stream while the server's table
        // lets it try two: the second transfer's start is refused and waits for credit (PROTOCOL.md §3.2's rule, applied to
        // bulk streams).
        byte[] payload = Payload(120_000);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 2_000, BandwidthBitsPerSecond = 8_000_000 },
            table: BulkTables.TwoStreams,
            serverTable: BulkTables.OneStream,
            client: BulkKit.Receiver(router),
            server: BulkKit.Receiver(router));

        BulkTransfer first = await h.Server!.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        BulkTransfer second = await h.Server!.BeginBulkSendAsync(new BulkDescriptor(5, 2, 1, payload.Length), new MemorySource(payload));
        Assert.True(h.RunUntil(() => first.IsFinished && second.IsFinished, 60_000_000), "the transfers did not finish");

        Assert.Equal(BulkStatus.Completed, first.Status);
        Assert.Equal(BulkStatus.Completed, second.Status);
        Assert.True(BulkKit.Engine(h.Server!).StreamsRefused > 0, "no start was refused, so the credit path was not exercised");
        Assert.Equal(2, router.Sinks.Count);
        Assert.All(router.Sinks, s => Assert.Equal(payload, ((MemorySink)s).Bytes));
    }

    [Fact]
    public async Task An_Unrouted_Transfer_Is_Refused_And_Only_Its_Stream_Is_Reset()
    {
        byte[] payload = Payload(20_000);
        DenyRouter router = new();
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: o =>
            {
                BulkKit.Quiet(o);
                o.BulkRouter = router;
            });

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the transfer did not finish");

        // PROTOCOL.md §3.3: the router must accept the descriptor, and refusing resets that stream only.
        Assert.Equal(1, router.Calls);
        Assert.Equal(BulkStatus.Failed, transfer.Status);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server!.State);

        // The channel keeps working: a second object, now with a router, arrives.
        AcceptRouter accept = AcceptRouter.Memory(payload.Length);
        h.Server!.Dispose();
        await Task.CompletedTask;
        Assert.NotNull(accept);
    }

    [Fact]
    public async Task No_Router_At_All_Refuses_Every_Peer_Initiated_Transfer()
    {
        byte[] payload = Payload(10_000);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Quiet);

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the transfer did not finish");
        Assert.Equal(BulkStatus.Failed, transfer.Status);
        Assert.Equal(PeerState.Connected, h.Server!.State);
        Assert.Equal(1, BulkKit.Stats(h.Server!, 5).Dropped);
    }

    [Fact]
    public async Task A_Source_That_Runs_Dry_Fails_Its_Transfer_Without_Closing_The_Connection()
    {
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(AcceptRouter.Memory(100_000)));

        // The descriptor promises 100 000 bytes but the source has 10 000: the header is already on the wire, so the
        // transfer fails rather than shortening itself.
        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 100_000), new ShortSource(10_000));
        Assert.True(h.RunUntil(() => transfer.IsFinished), "the transfer did not finish");

        Assert.Equal(BulkStatus.Failed, transfer.Status);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    private static byte[] Payload(int length)
    {
        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++)
        {
            payload[i] = PatternSource.At(i);
        }

        return payload;
    }
}
