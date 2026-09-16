using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// What bounds a bulk transfer, and what it does when a bound bites (docs/design/session-layer.md §7.7, PROTOCOL.md §7):
/// the three send gates (the pass's send cap, the per-peer rate bucket, the per-transfer send window), the send table and
/// the send budget, the receive budget on both of its rental paths, the rate rule itself — including the 16 KiB/s floor and
/// the case where there is no rate information at all — and a range request the transfer limit refuses. Plus the two
/// adverse-link cases the rest of the bulk suite never exercises: a chunked object over a lossy, reordered, jittery link,
/// and a cancellation racing that loss.
/// </summary>
public class BulkLimitTests
{
    private const int Kib = 1024;
    private const int Mib = 1024 * 1024;

    /// <summary>A link that loses, reorders and jitters, and is slow enough that a transfer is still running when a test acts.</summary>
    private static LinkOptions Hostile() => new()
    {
        DelayMicros = 5_000,
        JitterMicros = 2_000,
        LossPercent = 5,
        StreamLossPercent = 5,
        ReorderPercent = 10,
        BandwidthBitsPerSecond = 8_000_000,
    };

    /// <summary>An object whose bytes compress, so the chunked body really is a compressed one.</summary>
    private static byte[] Compressible(int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++)
        {
            bytes[i] = (byte)(i >> 6);
        }

        return bytes;
    }

    /// <summary>
    /// PROTOCOL.md §3.3: the hash covers the whole object and is verified when its last byte has arrived; §2.3:
    /// <c>BulkProgress</c> is cumulative, so losing some of it costs nothing. Every other bulk test runs on a link that
    /// only delays and rate-limits, which leaves loss, reorder, jitter and stream loss — and therefore retransmission,
    /// out-of-order datagrams and dropped progress frames — untested for the mode whose whole point is moving megabytes.
    /// </summary>
    [Fact]
    public async Task A_Chunked_Object_Crosses_A_Lossy_Reordered_Jittery_Link_With_Its_Hash_Verified()
    {
        byte[] payload = Compressible(256 * Kib);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: Hostile(),
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router));

        BulkDescriptor descriptor = new(5, 7, 3, payload.Length, 0, 0, Compress: true)
        {
            Sha256 = BulkKit.Hash(payload),
        };
        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(descriptor, new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.IsFinished, 60_000_000), "the object never crossed the hostile link");

        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload.Length, transfer.BytesTransferred);

        MemorySink sink = router.Sink<MemorySink>();
        Assert.True(h.RunUntil(() => sink.IsFinished, 10_000_000), "the receiver did not finish the transfer");
        Assert.Equal(payload, sink.Bytes);
        Assert.Equal(BulkStatus.Completed, sink.Result!.Value.Status);

        // The whole object arrived in order, so this end verified the sender's hash rather than deferring it.
        Assert.Equal(BulkHashState.Verified, sink.Result!.Value.Hash);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    /// <summary>
    /// PROTOCOL.md §3.4: a sender signals a cancellation by resetting its stream with <c>BulkCanceled</c> (§6), not with a
    /// <c>BulkCancel</c> frame, and the receiver turns that reset into a cancelled transfer through the stream's single
    /// close notice. Under loss the reset still arrives — a stream is reliable — so the race between the cancellation and
    /// the bytes still in flight must end both ends exactly once and leave the session alone.
    /// </summary>
    [Fact]
    public async Task A_Senders_Cancel_Racing_Loss_Ends_Both_Ends_And_Keeps_The_Session()
    {
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = new(
            link: Hostile(),
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * Mib), new PatternSource(4 * Mib));
        Assert.True(h.RunUntil(() => router.Sinks.Count == 1 && router.Sink<PatternSink>().BytesWritten > 0, 30_000_000), "nothing arrived");

        transfer.Cancel();
        Assert.True(h.RunUntil(() => transfer.IsFinished, 30_000_000), "the cancellation did not finish the transfer");
        Assert.Equal(BulkStatus.Canceled, transfer.Status);

        PatternSink sink = router.Sink<PatternSink>();
        Assert.True(h.RunUntil(() => sink.IsFinished, 30_000_000), "the receiver was never told");
        Assert.Equal(BulkStatus.Canceled, sink.Result!.Value.Status);
        Assert.Equal(0, sink.Mismatches);

        // The record and the channel's stream slot come back on the reset stream's close notice, exactly once.
        Assert.True(h.RunUntil(() => BulkKit.SendTransfers(h.Client, 5) == 0, 30_000_000), "the transfer record was not released");
        Assert.Equal(0, BulkKit.SendStreams(h.Client, 5));
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    /// <summary>
    /// The receiving end's counterpart under the same loss (PROTOCOL.md §3.4: <c>BulkCancel</c> is receiver-to-sender, and
    /// it travels on the control stream, so loss delays it but never drops it).
    /// </summary>
    [Fact]
    public async Task A_Receivers_Cancel_Racing_Loss_Stops_The_Sender()
    {
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = new(
            link: Hostile(),
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * Mib), new PatternSource(4 * Mib));
        Assert.True(h.RunUntil(() => router.Accepted.Count == 1 && router.Sink<PatternSink>().BytesWritten > 0, 30_000_000), "nothing arrived");

        Assert.True(h.Server!.CancelBulk(5, router.Accepted[0].TransferId));
        Assert.True(h.RunUntil(() => transfer.IsFinished, 30_000_000), "the sender was never told");

        Assert.Equal(BulkStatus.Canceled, transfer.Status);
        Assert.Equal(BulkStatus.Canceled, router.Sink<PatternSink>().Result!.Value.Status);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    /// <summary>
    /// The first gate (docs/design/session-layer.md §7.7, §7.1's budget rule): a pass hands over no more than
    /// <see cref="PeerOptions.MaxSendBytesPerSecond"/> allows, and the pass that ran out says so, which is what lowers
    /// <see cref="QuiclyPeer.NextFlushDeadlineMicros"/> to the bucket's refill time instead of leaving the transfer to be
    /// picked up by chance.
    /// </summary>
    [Fact]
    public async Task The_Passs_Send_Cap_Holds_A_Transfer_Back_And_Sets_The_Refill_Deadline()
    {
        byte[] payload = Compressible(256 * Kib);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Quiet(o);

                // Far below one 64 KiB piece per pass, so the cap is what stops the transfer rather than the window.
                o.MaxSendBytesPerSecond = 128 * Kib;
            },
            server: BulkKit.Receiver(router));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        Assert.True(
            h.RunUntil(() => h.Client.NextFlushDeadlineMicros != long.MaxValue && transfer.BytesTransferred > 0, 30_000_000),
            "no pass was ever held back by the send cap");

        long start = h.Network.NowMicros;
        Assert.True(h.RunUntil(() => transfer.IsFinished, 60_000_000), "the capped transfer never finished");
        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);

        // 256 KiB under a 128 KiB/s cap cannot have crossed in under a second of simulated time.
        Assert.True(h.Network.NowMicros - start > 500_000, $"the cap did not bind: the rest took {h.Network.NowMicros - start} µs");
    }

    /// <summary>
    /// The send table is a fixed native table (ADR 0008 invariant 1), so a transfer that wants more pieces than it has
    /// slots must simply wait for a completion rather than fail: <c>TryAllocateEntry</c> refusing is not an error.
    /// </summary>
    [Fact]
    public async Task An_Exhausted_Send_Table_Only_Delays_A_Transfer()
    {
        byte[] payload = Compressible(512 * Kib);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            // Delay only: a bandwidth limit would shrink the simulator's congestion window to a couple of pieces, and the
            // send *window* rather than the send *table* would be what holds the transfer back.
            link: new LinkOptions { DelayMicros = 5_000 },
            table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Quiet(o);

                // A window of 1 MiB in 16 KiB pieces wants 64 entries at once; the table holds its smallest allowed 16.
                o.SendTableCapacity = 16;
                o.BulkSendWindowBytes = Mib;
                o.BulkChunkBytes = 16 * Kib;
                o.BulkShareOfCongestionWindow = 1;
            },
            server: BulkKit.Receiver(router));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        Assert.True(
            h.RunUntil(() => DatagramKit.Statistics(h.Client).SendEntriesInUse >= 16, 30_000_000),
            "the send table never filled, so the refusal path never ran");

        Assert.True(h.RunUntil(() => transfer.IsFinished, 60_000_000), "the transfer never finished");
        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    /// <summary>
    /// The send budget bounds the pooled blocks a peer holds (PROTOCOL.md §7 is its receive-side counterpart), so a piece
    /// that cannot rent its block waits for one to come back: the entry it already took is given back first, or the table
    /// would leak an entry per pass.
    /// </summary>
    [Fact]
    public async Task An_Exhausted_Send_Budget_Only_Delays_A_Transfer()
    {
        byte[] payload = Compressible(256 * Kib);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 16_000_000 },
            table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Quiet(o);

                // Room for one 64 KiB piece at a time, while the window asks for four.
                o.SendBudgetBytes = 70 * Kib;
                o.BulkSendWindowBytes = 256 * Kib;
                o.BulkShareOfCongestionWindow = 1;
            },
            server: BulkKit.Receiver(router));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        Assert.True(
            h.RunUntil(() => DatagramKit.Statistics(h.Client).SendBytesOutstanding >= 64 * Kib, 30_000_000),
            "the send budget was never used up, so the refusal path never ran");

        Assert.True(h.RunUntil(() => transfer.IsFinished, 60_000_000), "the transfer never finished");
        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);
        Assert.Equal(0, DatagramKit.Statistics(h.Client).SendBytesOutstanding);
    }

    /// <summary>
    /// The receive side's first rental (docs/design/session-layer.md §7.7 sizing rule): a compressed chunk is staged whole,
    /// and a chunk that finds the receive budget used up is <see cref="StreamConsume.Pend"/>ed — back-pressure, resumed from
    /// <see cref="QuiclyPeer.Poll"/> — rather than reset. Here the budget is held by another transfer's half-arrived chunk,
    /// which is exactly the case the rule is written for.
    /// </summary>
    [Fact]
    public void A_Chunk_That_Finds_The_Receive_Budget_Used_Up_Is_Pended()
    {
        AcceptRouter router = new(_ => new CountingSink());
        using ServerHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            server: o =>
            {
                BulkKit.Receiver(router)(o);
                o.ReceiveBudgetBytes = 256 * Kib;
            });
        Assert.True(h.Admit(), "the raw client was not admitted");

        // A compressed chunk of 150 000 wire bytes whose body arrives only in part: its staging block stays rented, which
        // is what leaves no budget for the next transfer's chunk.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(Chunked(1, 150_000, 160_000, 1_000), out _));
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(h.Server!).ReceiveBytesOutstanding > 0), "the first chunk was not staged");

        long pends = DatagramKit.Statistics(h.Server!).StreamReceivePends;
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(Chunked(2, 150_000, 160_000, 150_000), out _));
        Assert.True(
            h.RunUntil(() => DatagramKit.Statistics(h.Server!).StreamReceivePends > pends),
            "the second chunk was not pended when the budget was gone");

        // Back-pressure, not a reset: both streams are still alive and so is the session.
        Assert.True(DatagramKit.Statistics(h.Server!).OutOfReceiveBuffers > 0);
        Assert.Equal(2, BulkKit.Engine(h.Server!).ReceiveTransfers);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    /// <summary>
    /// The receive side's <em>second</em> rental, and why it cannot pend: at the chunk's end the staged bytes have already
    /// been consumed from the stream, so there is nothing to un-read. The transfer's stream is reset
    /// <see cref="QuiclyErrorCode.LimitExceeded"/> instead (PROTOCOL.md §6: the transfer fails, the connection survives).
    /// </summary>
    [Fact]
    public void A_Chunk_That_Cannot_Rent_Its_Decode_Block_Resets_Its_Stream()
    {
        AcceptRouter router = new(_ => new CountingSink());
        using ServerHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            server: o =>
            {
                BulkKit.Receiver(router)(o);

                // Enough for the staged chunk, never enough for the block it decodes into as well.
                o.ReceiveBudgetBytes = 256 * Kib;
            });
        Assert.True(h.Admit(), "the raw client was not admitted");

        long resets = DatagramKit.Statistics(h.Server!).StreamsReset;
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(Chunked(1, 150_000, 160_000, 150_000), out _));
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(h.Server!).StreamsReset > resets), "the chunk's end did not reset the stream");

        Assert.Single(router.Accepted);
        Assert.Equal(BulkStatus.Failed, ((CountingSink)router.Sinks[0]).Result!.Value.Status);
        Assert.True(DatagramKit.Statistics(h.Server!).OutOfReceiveBuffers > 0);

        // Both leases are back, and the channel still works.
        Assert.Equal(0, DatagramKit.Statistics(h.Server!).ReceiveBytesOutstanding);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    /// <summary>
    /// The rate rule of docs/design/session-layer.md §7.7, asserted where it is decided rather than through a stopwatch:
    /// an explicit <see cref="PeerOptions.BulkMaxBytesPerSecond"/> is taken as it is, a rate <em>derived</em> from a
    /// bandwidth estimate is floored at 16 KiB/s so a tiny estimate still moves a transfer, and when there is no rate
    /// information at all — the transport reports no usable RTT and no send cap is set — no rate is derived and the bucket
    /// stays off, because flooring an unmetered link at 16 KiB/s would be orders of magnitude slower than the link.
    /// </summary>
    [Fact]
    public void The_Bulk_Rate_Floors_A_Derived_Estimate_And_Is_Off_Without_One()
    {
        // A one-second one-way delay over a 64 kbit/s link: the estimate (congestion window ÷ RTT, halved by the default
        // share) is a few kilobytes a second, so the floor is what the bucket runs at.
        using SessionHarness slow = new(
            link: new LinkOptions { DelayMicros = 1_000_000, BandwidthBitsPerSecond = 64_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Quiet);
        slow.Run(5_000);
        Assert.Equal(16 * Kib, BulkKit.Engine(slow.Client).RatePerSecond);

        // An explicit cap is an explicit cap, floor or no floor.
        using SessionHarness capped = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Quiet(o);
                o.BulkMaxBytesPerSecond = 4 * Kib;
            },
            server: BulkKit.Quiet);
        capped.Run(5_000);
        Assert.Equal(4 * Kib, BulkKit.Engine(capped.Client).RatePerSecond);

        // A zero-delay link reports an RTT below a microsecond, and no send cap is configured: nothing to derive from.
        using SessionHarness free = new(table: BulkTables.Main, client: BulkKit.Quiet, server: BulkKit.Quiet);
        free.Run(5_000);
        Assert.Equal(0, BulkKit.Engine(free.Client).RatePerSecond);
    }

    /// <summary>
    /// And the bucket really gates: a cap of 32 KiB/s spends simulated seconds on an object a zero-delay link would carry
    /// in one pass, and <see cref="QuiclyPeer.NextFlushDeadlineMicros"/> brings the host back when the bucket refills
    /// (<c>Tick</c>) instead of leaving the transfer stalled until other work happens.
    /// </summary>
    [Fact]
    public async Task An_Explicit_Bulk_Rate_Cap_Paces_The_Transfer()
    {
        byte[] payload = Compressible(96 * Kib);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Quiet(o);
                o.BulkMaxBytesPerSecond = 32 * Kib;
            },
            server: BulkKit.Receiver(router));

        long start = h.Network.NowMicros;
        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        Assert.True(h.RunUntil(() => transfer.BytesTransferred > 0 && h.Client.NextFlushDeadlineMicros != long.MaxValue, 30_000_000),
            "the rate bucket never held a piece back");

        Assert.True(h.RunUntil(() => transfer.IsFinished, 60_000_000), "the paced transfer never finished");
        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);

        // 96 KiB at 32 KiB/s is about three seconds; well over one, and nowhere near the link's own speed.
        long elapsed = h.Network.NowMicros - start;
        Assert.True(elapsed > 1_000_000, $"the cap did not pace the transfer: {elapsed} µs");
    }

    /// <summary>
    /// PROTOCOL.md §7: a range request the transfer limit cannot serve is answered <c>BulkReject</c> (0x15), the same
    /// answer the application gets from <see cref="QuiclyPeer.BeginBulkSendAsync"/> when it asks for one transfer too many —
    /// the requester decides when to ask again, and nothing is queued on its behalf.
    /// </summary>
    [Fact]
    public void A_Request_The_Transfer_Limit_Cannot_Serve_Is_Rejected()
    {
        byte[] payload = Compressible(512 * Kib);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 4_000_000 },
            table: BulkTables.Main,
            client: BulkKit.Receiver(router),
            server: o =>
            {
                BulkKit.Server(new MemoryProvider(payload))(o);

                // One transfer per direction: the second request finds no free record.
                o.BulkTransfersPerDirection = 1;
            });

        h.Client.RequestBulk(new BulkRangeRequest(5, 1, 1, 0, payload.Length));
        Assert.True(h.RunUntil(() => router.Accepted.Count == 1), "the first range was not served");

        h.Client.RequestBulk(new BulkRangeRequest(5, 1, 1, 0, payload.Length));
        Assert.True(h.RunUntil(() => router.Rejections.Count > 0, 30_000_000), "the second request was never answered");

        Assert.Equal(QuiclyErrorCode.BulkRejected, router.Rejections[0].Code);
        Assert.Single(router.Accepted);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    /// <summary>
    /// ADR 0009 ("every violation is a counter"): a <c>BulkProgress</c> claiming bytes this end never submitted is counted
    /// in <see cref="PeerStatistics.BulkProgressOverClaims"/> and clamped, while a claim that merely runs ahead of a
    /// completion still in flight is ordinary traffic and is not counted — the two are told apart by the bytes handed to
    /// the transport, not by the bytes whose sends have completed.
    /// </summary>
    [Fact]
    public async Task An_Over_Claimed_Progress_Frame_Is_Counted_And_An_Early_Honest_One_Is_Not()
    {
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 4_000_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(AcceptRouter.Pattern()));

        BulkEngine engine = BulkKit.Engine(h.Client);
        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * Mib), new PatternSource(4 * Mib));

        // Nothing has been read from the source yet, so every byte of this claim is invented.
        Assert.True(engine.OnControl(ControlType.BulkProgress, Progress(transfer.TransferId, 4 * Mib), onStream: false, 0));
        h.Run(20_000);

        Assert.Equal(1, engine.ProgressOverClaims);
        Assert.Equal(1, DatagramKit.Statistics(h.Client).BulkProgressOverClaims);
        Assert.NotEqual(BulkStatus.Completed, transfer.Status);
        Assert.Equal(0, transfer.BytesTransferred);

        // A claim of one byte once the transfer is moving is behind what went to the transport, so it is honest even if it
        // runs ahead of a completion: the counter must not move.
        Assert.True(h.RunUntil(() => transfer.BytesTransferred > 0, 30_000_000), "the transfer never started moving");
        Assert.True(engine.OnControl(ControlType.BulkProgress, Progress(transfer.TransferId, 1), onStream: false, 0));
        h.Run(20_000);

        Assert.Equal(1, engine.ProgressOverClaims);
        Assert.Equal(PeerState.Connected, h.Client.State);

        transfer.Cancel();
        h.Run(200_000);
    }

    /// <summary>
    /// ADR 0008 invariant 12: the bulk records are explicit-layout native structs, and a declared <c>Size</c> that cut off
    /// the last field — or misaligned an 8-byte one — would fail to load the type inside a transport callback, where it is
    /// hardest to diagnose. The engine checks the three sizes when it is built; this drives both outcomes of that check.
    /// </summary>
    [Fact]
    public void The_Bulk_Structs_Have_The_Layout_They_Declare()
    {
        // The real sizes: one cache line per channel, two per send transfer, three per receive transfer.
        BulkEngine.AssertLayout();

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => BulkEngine.AssertLayout(receiveSize: 191));
        Assert.Contains("declared layout", error.Message);
        Assert.Throws<InvalidOperationException>(() => BulkEngine.AssertLayout(stateSize: 63));
        Assert.Throws<InvalidOperationException>(() => BulkEngine.AssertLayout(sendSize: 127));
    }

    /// <summary>
    /// ADR 0008: every await the session layer hands out is released on <see cref="QuiclyPeer.Dispose"/>, not only the
    /// <see cref="QuiclyPeer.SendAsync"/> and <see cref="QuiclyPeer.FlushAsync"/> waiters. A tracked send's
    /// <see cref="QuiclyPeer.WaitAsync"/> is the third: after a dispose no completion can arrive from the transport and no
    /// <see cref="QuiclyPeer.Poll"/> will ever drain the completion ring, so a wait that was not released there would hang
    /// for good — the same defect as a bulk transfer's <see cref="BulkTransfer.Completion"/>.
    /// </summary>
    [Fact]
    public async Task Disposing_A_Peer_Releases_A_Wait_On_A_Tracked_Send()
    {
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Quiet);

        // Buffered, not flushed (PROTOCOL.md §4.5), so the send is still the engine's when the peer is disposed.
        SendToken token = h.Client.SendCopy(new SendHeader(2), [1, 2, 3], new SendOptions { Track = true }).Token;
        ValueTask<DeliveryStatus> wait = h.Client.WaitAsync(token, CompletionStage.RemoteAccepted);
        Assert.False(wait.IsCompleted);

        h.DisposeClient();

        Assert.True(wait.IsCompleted, "the wait on a tracked send outlived the peer that was disposed");
        Assert.Equal(DeliveryStatus.Disconnected, await wait);
    }

    /// <summary>A bulk stream of one chunked body: the §3.3 header, then one chunk header and <paramref name="bodyBytes"/> of it.</summary>
    private static byte[] Chunked(ulong transferId, int chunkLength, int rawLength, int bodyBytes)
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
        return BulkKit.BulkStream(5, in header, body.AsSpan(0, position + bodyBytes));
    }

    private static byte[] Progress(ulong transferId, ulong bytesAccepted)
    {
        byte[] body = new byte[16];
        int position = VarInt.Write(body, transferId);
        position += VarInt.Write(body.AsSpan(position), bytesAccepted);
        return body.AsSpan(0, position).ToArray();
    }
}
