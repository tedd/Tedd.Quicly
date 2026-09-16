using System.Net;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>A transport that records calls and never calls back by itself.</summary>
internal sealed unsafe class FakeTransport : ITransport
{
    private int _nextStream = 1;

    public TransportStatus DatagramStatus = TransportStatus.Success;
    public TransportStatus StreamStatus = TransportStatus.Success;
    public TransportStatus SendStatus = TransportStatus.Success;
    public Action<ulong>? DuringSend;
    public readonly List<(ulong Context, byte[] Data, TransportSendFlags Flags)> Datagrams = [];
    public readonly List<(TransportStreamId Id, byte[] Data, TransportSendFlags Flags)> StreamSends = [];
    public ulong? ClosedWith;
    public bool Disposed;

    public TransportCapabilities Capabilities => new() { Datagrams = true, MaxDatagramPayload = 1200 };

    public TransportState State => TransportState.Connected;

    public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        DuringSend?.Invoke(context);
        if (DatagramStatus != TransportStatus.Success)
        {
            return DatagramStatus;
        }

        Datagrams.Add((context, Gather(segments, count), flags));
        return TransportStatus.Success;
    }

    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
    {
        id = StreamStatus == TransportStatus.Success ? new TransportStreamId(_nextStream++, 1) : default;
        return StreamStatus;
    }

    public TransportStatus StartStream(TransportStreamId id) => TransportStatus.Success;

    public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        if (SendStatus != TransportStatus.Success)
        {
            return SendStatus;
        }

        StreamSends.Add((id, Gather(segments, count), flags));
        return TransportStatus.Success;
    }

    public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
    }

    public void SetStreamPriority(TransportStreamId id, ushort priority)
    {
    }

    public long GetQuicStreamId(TransportStreamId id) => -1;

    public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed)
    {
    }

    public void CloseStream(TransportStreamId id)
    {
    }

    public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional)
    {
    }

    public void Close(ulong errorCode, ReadOnlySpan<byte> reason) => ClosedWith ??= errorCode;

    public void GetStatistics(out TransportStatistics statistics) => statistics = default;

    public void Dispose() => Disposed = true;

    private static byte[] Gather(TransportSegment* segments, int count)
    {
        List<byte> bytes = [];
        for (int i = 0; i < count; i++)
        {
            bytes.AddRange(segments[i].AsSpan().ToArray());
        }

        return [.. bytes];
    }
}

internal sealed class FakeConnector(ITransport transport) : ITransportConnector
{
    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) => transport;
}

internal sealed class ThrowingConnector : ITransportConnector
{
    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) => throw new InvalidOperationException("connect failed");
}

/// <summary>The application RTT / clock-offset filter (PROTOCOL.md §4.6).</summary>
public class PingClockTests
{
    private static PongSample Sample(uint echo, uint remoteReceive, uint remoteSend, uint localReceive) =>
        new() { Echo = echo, RemoteReceive = remoteReceive, RemoteSend = remoteSend, LocalReceive = localReceive };

    [Fact]
    public void First_Sample_Sets_Rtt_Variance_And_Offset()
    {
        PingClock clock = default;
        clock.AddSample(Sample(1_000, 6_010, 6_015, 1_030));
        Assert.Equal(25, clock.LatestRtt);
        Assert.Equal(25, clock.SmoothedRtt);
        Assert.Equal(12, clock.RttVariance);
        Assert.Equal(25, clock.MinRtt);
        Assert.Equal(25, clock.MaxRtt);
        Assert.Equal(1, clock.Samples);
        Assert.Equal(4_997, clock.PublishedOffset);
        Assert.Equal(4_997, clock.TargetOffset);
        Assert.True(clock.HasOffset);
        Assert.Equal(0, clock.Jitter);
    }

    [Fact]
    public void Smoothing_Follows_Rfc_6298()
    {
        PingClock clock = default;
        clock.AddSample(Sample(0, 100, 100, 80));
        clock.AddSample(Sample(1_000, 1_100, 1_100, 1_160));
        Assert.Equal(90, clock.SmoothedRtt);
        Assert.Equal(50, clock.RttVariance);
        Assert.Equal(80, clock.MinRtt);
        Assert.Equal(160, clock.MaxRtt);
        Assert.Equal(60, clock.TargetOffset);
    }

    [Fact]
    public void Offset_Comes_From_The_Minimum_Rtt_Sample_Of_The_Last_Eight_Newest_On_Ties()
    {
        PingClock clock = default;
        clock.AddSample(Sample(0, 505, 505, 10));
        for (int i = 1; i < 8; i++)
        {
            uint t = (uint)(i * 1_000);
            clock.AddSample(Sample(t, t + 815, t + 815, t + 30));
        }

        Assert.Equal(500, clock.TargetOffset);
        clock.AddSample(Sample(8_000, 8_815, 8_815, 8_030));
        Assert.Equal(800, clock.TargetOffset);
        clock.AddSample(Sample(9_000, 9_915, 9_915, 9_030));
        Assert.Equal(900, clock.TargetOffset);
        Assert.Equal(900, clock.PublishedOffset);
    }

    [Fact]
    public void Negative_Rtt_Clamps_To_Zero_And_Timestamps_Wrap()
    {
        PingClock clock = default;
        clock.AddSample(Sample(uint.MaxValue - 5, 10, 20, 10));
        Assert.Equal(6, clock.LatestRtt);
        clock.AddSample(Sample(100, 0, 50, 110));
        Assert.Equal(0, clock.LatestRtt);
    }

    [Fact]
    public void Offset_Is_Stepped_Until_Locked_Then_Slewed()
    {
        PingClock clock = default;
        clock.Advance(0, lockReached: true);
        Assert.False(clock.Locked);
        clock.AddSample(Sample(0, 1_000, 1_000, 0));
        clock.Advance(100, lockReached: false);
        Assert.False(clock.Locked);
        clock.Advance(200, lockReached: true);
        Assert.True(clock.Locked);
        clock.AddSample(Sample(10, 2_010, 2_010, 10));
        Assert.Equal(2_000, clock.TargetOffset);
        Assert.Equal(1_000, clock.PublishedOffset);
        clock.Advance(1_800, lockReached: true);
        Assert.Equal(1_100, clock.PublishedOffset);
        clock.Advance(1_800, lockReached: true);
        Assert.Equal(1_100, clock.PublishedOffset);
        clock.Advance(1_000_000, lockReached: true);
        Assert.Equal(2_000, clock.PublishedOffset);
        clock.Advance(1_000_010, lockReached: true);
        Assert.Equal(2_000, clock.PublishedOffset);
        clock.AddSample(Sample(20, 20, 20, 20));
        Assert.Equal(0, clock.TargetOffset);
        clock.Advance(1_000_026, lockReached: true);
        Assert.Equal(1_999, clock.PublishedOffset);
    }

    [Fact]
    public void Jitter_Follows_Consecutive_Pong_Timestamps()
    {
        PingClock clock = default;
        clock.AddSample(Sample(0, 1_000, 1_000, 20));
        clock.AddSample(Sample(100_000, 101_600, 101_600, 100_020));
        Assert.Equal(37, clock.Jitter);
    }
}

/// <summary>Small session support types.</summary>
public class SessionSupportTests
{
    [Fact]
    public void Token_Bucket_Refills_At_Its_Rate_Up_To_Its_Burst()
    {
        TokenBucket bucket = default;
        bucket.Initialize(ratePerSecond: 10, burst: 2, nowMicros: 0);
        Assert.True(bucket.TryTake(0));
        Assert.True(bucket.TryTake(0));
        Assert.False(bucket.TryTake(0));
        Assert.False(bucket.TryTake(99_999));
        Assert.True(bucket.TryTake(100_000));
        Assert.False(bucket.TryTake(100_000));
        Assert.True(bucket.TryTake(10_000_000_000, 2));
        Assert.False(bucket.TryTake(10_000_000_000));
        Assert.False(bucket.TryTake(5));
    }

    [Fact]
    public void Control_Pool_Sends_From_Fixed_Buffers_And_Releases_By_Context()
    {
        FakeTransport transport = new();
        using TransportControlPool pool = new();
        Assert.True(pool.TrySend(transport, [0, 2, 1, 2, 3], TransportSendFlags.Priority));
        (ulong context, byte[] data, TransportSendFlags flags) = transport.Datagrams[0];
        Assert.True(TransportControlPool.IsPoolContext(context));
        Assert.Equal(new byte[] { 0, 2, 1, 2, 3 }, data);
        Assert.Equal(TransportSendFlags.Priority, flags);
        Assert.Equal(1, pool.InUse);
        pool.Release(context);
        Assert.Equal(0, pool.InUse);
        pool.Release(context);
        Assert.Equal(0, pool.InUse);
        Assert.False(pool.TrySend(transport, new byte[TransportControlPool.SlotBytes + 1], TransportSendFlags.None));
        for (int i = 0; i < TransportControlPool.SlotCount; i++)
        {
            Assert.True(pool.TrySend(transport, [1], TransportSendFlags.None));
        }

        Assert.False(pool.TrySend(transport, [1], TransportSendFlags.None));
        pool.Release(context);
        Assert.Equal(TransportControlPool.SlotCount, pool.InUse);
        Assert.False(TransportControlPool.IsPoolContext((3UL << 32) | 5));
        pool.Release((1UL << 32) | TransportControlPool.ContextTag | 100);
    }

    [Fact]
    public void Control_Pool_Frees_A_Refused_Send_And_Tolerates_Reentrant_Completion()
    {
        FakeTransport transport = new() { DatagramStatus = TransportStatus.InvalidState };
        using TransportControlPool pool = new();
        Assert.False(pool.TrySend(transport, [1], TransportSendFlags.None));
        Assert.Equal(0, pool.InUse);
        transport.DatagramStatus = TransportStatus.Success;
        transport.DuringSend = pool.Release;
        Assert.True(pool.TrySend(transport, [1], TransportSendFlags.None));
        Assert.Equal(0, pool.InUse);
    }

    [Fact]
    public void Stream_Table_Finds_Records_By_Slot_And_Generation()
    {
        StreamTable table = new();
        TransportStreamId a = new(3, 1);
        ref StreamRecord record = ref table.Add(a, StreamTag.Engine);
        record.Channel = 7;
        Assert.Equal(1, table.Count);
        Assert.Equal(7, table.Find(a).Channel);
        Assert.True(Unsafe.IsNullRef(ref table.Find(new TransportStreamId(3, 2))));
        Assert.True(Unsafe.IsNullRef(ref table.Find(new TransportStreamId(100, 1))));
        ref StreamRecord grown = ref table.Add(new TransportStreamId(40, 1), StreamTag.Control);
        Assert.Equal(StreamTag.Control, grown.Tag);
        Assert.Equal(2, table.Count);
        table.Add(a, StreamTag.Discard);
        Assert.Equal(2, table.Count);
        Assert.Equal(StreamTag.Discard, table.Find(a).Tag);
        table.Remove(a);
        table.Remove(a);
        Assert.Equal(1, table.Count);
        Assert.Throws<ArgumentOutOfRangeException>(() => { table.Add(new TransportStreamId(-1, 1), StreamTag.Engine); });
    }

    [Fact]
    public void Receive_Queues_Keep_Per_Channel_Order_Within_A_Bounded_Pool()
    {
        ReceiveQueues queues = new(capacity: 3, channels: 2);
        Assert.True(queues.TryAppend(0, new ReceiveEntry { Sequence = 1 }));
        Assert.True(queues.TryAppend(1, new ReceiveEntry { Sequence = 2 }));
        Assert.True(queues.TryAppend(0, new ReceiveEntry { Sequence = 3 }));
        Assert.False(queues.TryAppend(1, new ReceiveEntry { Sequence = 4 }));
        Assert.Equal(3, queues.Used);
        Assert.Equal(2, queues.Count(0));
        Assert.Equal(3, queues.Capacity);
        Assert.True(queues.TryTake(0, out ReceiveEntry entry));
        Assert.Equal(1u, entry.Sequence);
        Assert.True(queues.TryTake(0, out entry));
        Assert.Equal(3u, entry.Sequence);
        Assert.False(queues.TryTake(0, out _));
        Assert.True(queues.TryAppend(0, new ReceiveEntry { Sequence = 5 }));
        Assert.True(queues.TryTake(1, out entry));
        Assert.Equal(2u, entry.Sequence);
        Assert.Equal(1, queues.Used);
        Assert.False(new ReceiveQueues(0, 1).TryAppend(0, default));
    }

    [Fact]
    public void Receive_Mailbox_Keeps_The_Latest_Value_Per_Key()
    {
        using ReceiveMailbox box = new(channel: 8, channelIndex: 3, keySlots: 4);
        Assert.Equal((ushort)8, box.Channel);
        Assert.Equal(3, box.ChannelIndex);
        Assert.Equal(4, box.KeySlots);
        BufferLease first = new(0, 0, 1, 1, 0, 64);
        BufferLease second = new(0, 0, 1, 2, 64, 64);
        Assert.True(box.TryPost(1, new ReceiveEntry { Sequence = 1, Lease = first }, out BufferLease displaced));
        Assert.True(displaced.IsEmpty);
        Assert.True(box.TryPost(1, new ReceiveEntry { Sequence = 2, Lease = second }, out displaced));
        Assert.Equal(first, displaced);
        Span<int> keys = stackalloc int[4];
        Assert.Equal(1, box.PopDirty(keys));
        Assert.Equal(1, keys[0]);
        Assert.True(box.TryTake(1, out ReceiveEntry entry));
        Assert.Equal(2u, entry.Sequence);
        Assert.False(box.TryTake(1, out _));
        for (int i = 0; i < 20; i++)
        {
            Assert.True(box.TryPost(i % 4, new ReceiveEntry { Sequence = (uint)i }, out _));
            Assert.True(box.TryTake(i % 4, out _));
        }
    }

    [Fact]
    public void Receive_Stamps_Restore_Across_The_32_Bit_Wrap()
    {
        long received = 0x1_FFFF_FFF0;
        uint stamp = PeerCore.StampReceive(received);
        Assert.Equal(received, PeerCore.RestoreReceive(stamp, received + 100));
        Assert.Equal(received, PeerCore.RestoreReceive(stamp, received));
    }

    [Fact]
    public void Core_Maps_Transport_Outcomes_To_Delivery_Statuses()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;
        Assert.Equal(DeliveryStatus.Delivered, core.MapDatagramState(DatagramSendState.Acknowledged));
        Assert.Equal(DeliveryStatus.Delivered, core.MapDatagramState(DatagramSendState.AcknowledgedSpurious));

        // PROTOCOL.md §4.3: Sent is final only on a carrier that reports no per-datagram state, and it is not a delivery
        // claim — the session knows the datagram left the host and nothing more.
        Assert.Equal(DeliveryStatus.Sent, core.MapDatagramState(DatagramSendState.Sent));
        Assert.Equal(DeliveryStatus.Lost, core.MapDatagramState(DatagramSendState.LostDiscarded));
        Assert.Equal(DeliveryStatus.Expired, core.MapDatagramState(DatagramSendState.Canceled));
        Assert.Equal(DeliveryStatus.Delivered, core.MapStreamCompletion(false));
        Assert.Equal(DeliveryStatus.Failed, core.MapStreamCompletion(true));
        core.MarkTransportClosing();
        Assert.True(core.IsTransportClosing);
        Assert.Equal(DeliveryStatus.Disconnected, core.MapDatagramState(DatagramSendState.Canceled));
        Assert.Equal(DeliveryStatus.Disconnected, core.MapStreamCompletion(true));
    }

    [Fact]
    public void Key_Tables_Follow_The_Channel_Key_Space()
    {
        ChannelTable table = ChannelTable.Create()
            .Add(2, "dense", ChannelMode.UnreliableSequenced, o =>
            {
                o.Keyed = true;
                o.KeySpace = KeySpace.Dense(9);
            })
            .Add(3, "hashed", ChannelMode.UnreliableSequenced, o => o.Keyed = true)
            .Build();
        IKeyTable dense = PeerCore.CreateKeyTable(table[2]!);
        IKeyTable hashed = PeerCore.CreateKeyTable(table[3]!);
        Assert.IsType<DenseKeyTable>(dense);
        Assert.IsType<KeyTable>(hashed);
        Assert.Equal(10, dense.MaxKeys);
        Assert.Equal(4096, hashed.MaxKeys);
        ((IDisposable)dense).Dispose();
        ((IDisposable)hashed).Dispose();
    }

    [Fact]
    public void Core_Budgets_And_Ring_Reservations()
    {
        using SessionHarness h = new(connect: false, client: o =>
        {
            o.SendBudgetBytes = 128;
            o.ReceiveBudgetBytes = 128;
            o.ReceiveRingCapacity = 2;
        });
        PeerCore core = h.Client.Core;
        Assert.True(core.TryRentSend(64, out BufferLease a));
        Assert.True(core.TryRentSend(64, out BufferLease b));
        Assert.False(core.TryRentSend(1, out _));
        core.ReturnSend(in a);
        core.ReturnSend(in b);
        core.ReturnSend(BufferLease.Empty);
        Assert.Equal(0, core.SendBytesOutstanding);
        Assert.True(core.TryRentReceive(64, out BufferLease r1));
        Assert.True(core.TryRentReceive(64, out BufferLease r2));
        Assert.False(core.TryRentReceive(1, out _));
        Assert.False(core.TryRentReceive(1 << 30, out _));
        core.ReturnReceive(in r1);
        core.ReturnReceive(in r2);
        core.ReturnReceive(BufferLease.Empty);
        Assert.Equal(0, core.ReceiveBytesOutstanding);

        Assert.True(core.TryReserveReceive());
        Assert.True(core.TryEnqueueReceive(new ReceiveEntry { Channel = 2 }));
        Assert.False(core.TryReserveReceive());
        Assert.False(core.TryEnqueueReceive(default));
        core.PublishReserved(new ReceiveEntry { Channel = 2 });
        Assert.Equal(2, core.ReceiveRing.Count);
        while (core.ReceiveRing.TryDequeue(out _))
        {
        }

        Assert.True(core.TryReserveReceive());
        core.CancelReservation();
        Assert.Equal(1, core.Counters.ReceiveRingDrops);
        Assert.Equal(2, core.Counters.ReceiveRingHighWater);
        Assert.Equal(3, core.PeerUnidirectionalStreamLimit);
    }

    [Fact]
    public unsafe void Entries_Unwind_In_Every_State()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;
        Assert.True(core.TryAllocateEntry(2, SendEntryFlags.None, out int filling));
        Assert.True(core.TryTrack(filling, 9, out SendToken token));
        Assert.True(core.TryRentSend(10, out BufferLease lease));
        core.AttachLease(filling, in lease, 10);
        core.DiscardEntry(filling);
        Assert.Equal(SendEntryState.Free, core.Entries.GetState(filling));
        Assert.Equal(DeliveryStatus.Canceled, core.Completions.GetStatus(token));
        Assert.Equal(0, core.SendBytesOutstanding);

        Assert.True(core.TryAllocateEntry(2, SendEntryFlags.None, out int inFlight));
        core.Entries.Publish(inFlight);
        core.CompleteEntry(inFlight, DeliveryStatus.Delivered);
        Assert.Equal(SendEntryState.Free, core.Entries.GetState(inFlight));

        Assert.True(core.TryAllocateEntry(2, SendEntryFlags.None, out int cancelling));
        core.Entries.Publish(cancelling);
        Assert.True(core.Entries.TryTransition(cancelling, SendEntryState.InFlight, SendEntryState.Cancelling));
        core.CompleteEntry(cancelling, DeliveryStatus.Canceled);
        Assert.Equal(SendEntryState.Free, core.Entries.GetState(cancelling));

        Assert.True(core.TryAllocateEntry(2, SendEntryFlags.None, out int pinned));
        byte[] data = [1, 2, 3];
        System.Runtime.InteropServices.GCHandle handle = System.Runtime.InteropServices.GCHandle.Alloc(data, System.Runtime.InteropServices.GCHandleType.Pinned);
        core.Entries.PinHandles[pinned] = System.Runtime.InteropServices.GCHandle.ToIntPtr(handle);
        core.CompleteEntry(pinned, DeliveryStatus.Delivered);
        Assert.Equal((nint)0, core.Entries.PinHandles[pinned]);

        Assert.True(core.TryAllocateEntry(1, SendEntryFlags.None, out int container));
        core.OnContainerCompleted(container, new CompletionEntry { Slot = container, Final = false });
        Assert.NotEqual(SendEntryState.Free, core.Entries.GetState(container));
        core.OnContainerCompleted(container, new CompletionEntry { Slot = container, Final = true });
        Assert.Equal(SendEntryState.Free, core.Entries.GetState(container));

        // Submitting without a connected transport is refused and leaves the entry with the caller.
        Assert.True(core.TryAllocateEntry(2, SendEntryFlags.None, out int unsent));
        core.MarkTransportClosing();
        Assert.Equal(TransportStatus.InvalidState, core.SubmitDatagram(unsent, TransportSendFlags.None));
        Assert.Equal(TransportStatus.InvalidState, core.SubmitStream(new TransportStreamId(1, 1), core.Entries.GetSegments(unsent), 1, unsent, TransportSendFlags.None));
        Assert.Equal(TransportStatus.InvalidState, core.OpenStream(StreamKind.Unidirectional, 0, 0, out _));
        Assert.Equal(SendEntryState.Filling, core.Entries.GetState(unsent));
        core.DiscardEntry(unsent);
    }

    [Fact]
    public void Options_Default_To_Valid_Values_And_Convert_Times()
    {
        PeerOptions options = new();
        options.Validate();
        Assert.Equal(1_500, PeerOptions.ToMicros(TimeSpan.FromMilliseconds(1.5)));
        Assert.Same(MonotonicClock.Instance, options.Clock);
        options.AutoFlushInterval = TimeSpan.FromMilliseconds(16);
        Assert.Throws<NotSupportedException>(() => options.Validate());
    }

    public static TheoryData<string> InvalidOptionNames =>
    [
        "clock", "send-budget", "receive-budget", "table", "ring", "arena", "control-rate", "pong-rate", "pong-burst", "decode",
        "max-message", "ping", "fast-ping", "admission", "flush", "ack-delay", "fast-lock", "heartbeat", "linger", "grace",
        "bandwidth", "bulk-bandwidth", "bulk-share-low", "bulk-share-high", "token",
    ];

    [Theory]
    [MemberData(nameof(InvalidOptionNames))]
    public void Invalid_Options_Are_Rejected(string name)
    {
        PeerOptions o = new();
        TimeSpan negative = TimeSpan.FromTicks(-1);
        switch (name)
        {
            case "clock": o.Clock = null!; break;
            case "send-budget": o.SendBudgetBytes = 0; break;
            case "receive-budget": o.ReceiveBudgetBytes = 0; break;
            case "table": o.SendTableCapacity = 8; break;
            case "ring": o.ReceiveRingCapacity = 1; break;
            case "arena": o.SegmentArenaCapacity = 4; break;
            case "control-rate": o.ControlMessagesPerSecond = 0; break;
            case "pong-rate": o.PongsPerSecond = 0; break;
            case "pong-burst": o.PongBurst = 0; break;
            case "decode": o.DecodedBytesPerSecond = 0; break;
            case "max-message": o.MaxMessageSize = 0; break;
            case "ping": o.PingInterval = TimeSpan.Zero; break;
            case "fast-ping": o.FastPingInterval = TimeSpan.Zero; break;
            case "admission": o.AdmissionTimeout = TimeSpan.Zero; break;
            case "flush": o.FlushInterval = TimeSpan.Zero; break;
            case "ack-delay": o.AckDelay = negative; break;
            case "fast-lock": o.FastLockDuration = negative; break;
            case "heartbeat": o.HeartbeatTimeout = negative; break;
            case "linger": o.CloseLinger = negative; break;
            case "grace": o.SessionGrace = negative; break;
            case "bandwidth": o.MaxSendBytesPerSecond = -1; break;
            case "bulk-bandwidth": o.BulkMaxBytesPerSecond = -1; break;
            case "bulk-share-low": o.BulkShareOfEstimatedBandwidth = 0; break;
            case "bulk-share-high": o.BulkShareOfEstimatedBandwidth = 1.5; break;
            default: o.SessionToken = new byte[4097]; break;
        }

        Assert.Throws<ArgumentException>(() => o.Validate());
    }

    [Fact]
    public void Admission_Results_Validate_Their_Arguments()
    {
        AdmissionResult accept = AdmissionResult.Accept(new byte[] { 1 }, 5, 2);
        Assert.Equal(AdmissionDecision.Accept, accept.Decision);
        Assert.Equal(HelloStatus.Accepted, accept.Status);
        Assert.Equal(5ul, accept.SessionId);
        Assert.Equal(2u, accept.Epoch);
        Assert.Equal(1, accept.SessionToken.Length);
        Assert.Null(accept.Reason);
        Assert.Throws<ArgumentException>(() => AdmissionResult.Accept(new byte[4097]));
        Assert.Throws<ArgumentException>(() => AdmissionResult.Accept(epoch: 0));
        AdmissionResult reject = AdmissionResult.Reject(HelloStatus.ServerFull, "full");
        Assert.Equal(AdmissionDecision.Reject, reject.Decision);
        Assert.Equal(HelloStatus.ServerFull, reject.Status);
        Assert.Equal("full", reject.Reason);
        Assert.Throws<ArgumentException>(() => AdmissionResult.Reject(HelloStatus.Accepted));
        Assert.Throws<ArgumentException>(() => AdmissionResult.Reject(HelloStatus.Informational));
        Assert.Throws<ArgumentException>(() => AdmissionResult.Reject((HelloStatus)5));
        Assert.Throws<ArgumentException>(() => AdmissionResult.Reject(HelloStatus.Rejected, new string('x', 513)));
        Assert.Equal(AdmissionDecision.Pending, AdmissionResult.Pending.Decision);
    }

    [Fact]
    public void Public_Value_Types_Have_Sensible_Defaults()
    {
        Assert.Equal(new CloseReason(QuiclyErrorCode.NoError), CloseReason.Normal);
        Assert.Equal(CloseSource.None, default(CloseReason).Source);
        Assert.True(new SendResult(SendStatus.Admitted, default).IsAdmitted);
        Assert.False(SendResult.Rejected(SendStatus.QueueFull).IsAdmitted);
        Assert.Equal(SendMode.Immediate, SendOptions.Immediate.Mode);
        Assert.True(SendOptions.Tracked.Track);
        Assert.True(default(ReceivedMessage).Payload.IsEmpty);
        Assert.False(default(ReceiveLease).IsValid);
        Assert.True(default(ReceiveLease).Payload.IsEmpty);
        Assert.Equal(new SendHeader(3, 0), new SendHeader(3));
        BulkTransfer transfer = new(new BulkDescriptor(7, 1, 2, 3));
        Assert.Equal((ushort)7, transfer.Descriptor.Channel);
        Assert.Equal(0, new EmptySource().Read(0, new byte[1]));
    }

    [Fact]
    public void Every_Mode_Has_A_Registered_Engine()
    {
        foreach (ChannelMode mode in Enum.GetValues<ChannelMode>())
        {
            using ChannelEngine engine = ChannelEngines.Create(mode);
            Assert.Equal(mode, engine.Mode);
        }

        Assert.IsType<UnreliableUnorderedEngine>(ChannelEngines.Create(ChannelMode.UnreliableUnordered));
        Assert.IsType<UnreliableSequencedEngine>(ChannelEngines.Create(ChannelMode.UnreliableSequenced));

        Assert.IsType<ReliableOrderedEngine>(ChannelEngines.Create(ChannelMode.ReliableOrdered));
        Assert.IsType<ReliableLatestEngine>(ChannelEngines.Create(ChannelMode.ReliableLatest));

        // The remaining wave C2 modes stay placeholders until their engines land.
        Assert.IsType<PlaceholderEngine>(ChannelEngines.Create(ChannelMode.ReliableUnordered));
        Assert.IsType<PlaceholderEngine>(ChannelEngines.Create(ChannelMode.Bulk));
        Assert.Throws<ArgumentOutOfRangeException>(() => ChannelEngines.Create((ChannelMode)6));
        Assert.Equal(6, ChannelEngines.ModeCount);
    }

    [Fact]
    public void Stream_Answers_Carry_Their_Action_And_Code()
    {
        Assert.True(StreamAccept.Accept(5).Accepted);
        Assert.Equal(5, StreamAccept.Accept(5).Cookie);
        Assert.Equal(StreamAcceptAction.Reset, StreamAccept.Reject(QuiclyErrorCode.UnsupportedChannel).Action);
        Assert.False(StreamAccept.Reject(QuiclyErrorCode.UnsupportedChannel).Accepted);
        Assert.Equal(QuiclyErrorCode.LimitExceeded, StreamAccept.CloseConnection(QuiclyErrorCode.LimitExceeded).ResetCode);
        Assert.Equal(StreamConsumeAction.Continue, StreamConsume.Continue.Action);
        Assert.Equal(StreamConsumeAction.Pend, StreamConsume.Pend.Action);
        Assert.Equal(QuiclyErrorCode.ProtocolViolation, StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation).Code);
        Assert.Equal(StreamConsumeAction.CloseConnection, StreamConsume.CloseConnection(QuiclyErrorCode.LimitExceeded).Action);
    }

    [Fact]
    public void Placeholder_Engine_Refuses_Everything()
    {
        using SessionHarness h = new(table: TestTables.AllModes);
        PeerCore core = h.Client.Core;
        ChannelEngine engine = core.GetEngine(ChannelMode.Bulk)!;
        Assert.IsType<PlaceholderEngine>(engine);
        ChannelDefinition bulk = h.Table[7]!;
        SendRequest request = default;
        request.Channel = bulk;
        Assert.Equal(SendStatus.NotSupported, engine.Admit(ref request));
        Assert.False(engine.TryCancel(0));
        Assert.Equal(SendStatus.NotSupported, engine.RetireKey(bulk, 1));
        Assert.True(engine.SendRequestAsync(ref request, 1, default).IsFaulted);
        Assert.Equal(SendStatus.NotSupported, engine.Respond(new ReceiveHeader(), ref request));
        Assert.True(engine.BeginBulkSendAsync(bulk, new BulkDescriptor(7, 1, 1, 1), new EmptySource(), default).IsFaulted);
        Assert.Throws<NotSupportedException>(() => engine.RequestBulk(bulk, new BulkRangeRequest(7, 1, 1, 0, 1)));
        Assert.True(engine.OnControl(ControlType.BulkCancel, default, onStream: true, 0));
        Assert.Equal(StreamAcceptAction.Reset, engine.OnStreamOpened(new TransportStreamId(1, 1), 7, 0).Action);
        StreamMessageContext context = default;
        Assert.Equal(StreamConsumeAction.ResetStream, engine.OnStreamMessage(ref context).Action);
        FlushContext flush = default;
        engine.Flush(ref flush);
        long deadline = long.MaxValue;
        engine.Tick(0, ref deadline);
        Assert.Equal(long.MaxValue, deadline);
        engine.OnEpochReset(false);
        engine.OnPeerClosed();
        engine.OnStreamClosed(default, false, 0);
        MessageHeader header = default;
        header.Channel = 7;
        engine.OnDatagram(in header, default, 0);
        Assert.True(h.Client.GetChannelStatistics(7, out ChannelStatistics stats));
        Assert.Equal(1, stats.Dropped);
        Assert.True(core.TryAllocateEntry(7, SendEntryFlags.None, out int slot));
        engine.OnSendCompleted(slot, new CompletionEntry { Slot = slot, Final = false });
        Assert.NotEqual(SendEntryState.Free, core.Entries.GetState(slot));
        engine.OnSendCompleted(slot, new CompletionEntry { Slot = slot, Final = true });
        Assert.Equal(SendEntryState.Free, core.Entries.GetState(slot));
    }


    [Fact]
    public void Construction_Validates_Its_Arguments()
    {
        IPEndPoint endpoint = new(IPAddress.Loopback, 1);
        Assert.Throws<ArgumentNullException>(() => QuiclyPeer.Connect(null!, endpoint, null, TestTables.Default, new PeerOptions()));
        Assert.Throws<ArgumentNullException>(() => QuiclyPeer.Connect(new ThrowingConnector(), null!, null, TestTables.Default, new PeerOptions()));
        Assert.Throws<ArgumentNullException>(() => QuiclyPeer.Connect(new ThrowingConnector(), endpoint, null, null!, new PeerOptions()));
        Assert.Throws<ArgumentNullException>(() => QuiclyPeer.Connect(new ThrowingConnector(), endpoint, null, TestTables.Default, null!));
        Assert.Throws<ArgumentException>(() => QuiclyPeer.Connect(new ThrowingConnector(), endpoint, null, TestTables.Default, new PeerOptions(), new byte[4097]));
        Assert.Throws<InvalidOperationException>(() => QuiclyPeer.Connect(new ThrowingConnector(), endpoint, null, TestTables.Default, new PeerOptions()));
        PeerOptions wrongEngine = new() { EngineFactory = m => new PlaceholderEngine(m == ChannelMode.UnreliableUnordered ? ChannelMode.Bulk : m) };
        Assert.Throws<InvalidOperationException>(() => QuiclyPeer.Connect(new ThrowingConnector(), endpoint, null, TestTables.Default, wrongEngine));
        FakeTransport transport = new();
        Assert.Throws<ArgumentNullException>(() => QuiclyPeer.CreateServerPeer(null!, default, TestTables.Default, new PeerOptions(), new TestAdmission()));
        Assert.Throws<ArgumentNullException>(() => QuiclyPeer.CreateServerPeer(transport, default, TestTables.Default, new PeerOptions(), null!));
    }

    [Fact]
    public void Transport_Callbacks_Drive_A_Server_Peer_Without_A_Network()
    {
        FakeTransport transport = new();
        NewConnectionInfo info = new() { RemoteEndPoint = new IPEndPoint(IPAddress.Loopback, 5) };
        QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, TestTables.Default, new PeerOptions { Clock = new VirtualClock() }, new TestAdmission());
        ITransportSink sink = peer.TransportSink;
        Assert.Equal(PeerState.Connecting, peer.State);
        Assert.Equal(5, peer.RemoteEndPoint!.Port);
        sink.OnDatagramCapabilityChanged(true, 1100);
        TransportConnectedInfo connected = new() { Capabilities = new TransportCapabilities { Datagrams = true, MaxDatagramPayload = 1100 } };
        sink.OnConnected(in connected);
        peer.Poll();
        Assert.Equal(PeerState.Handshaking, peer.State);
        sink.OnStreamSendCompleted(default, 12345, false);
        sink.OnDatagramSendStateChanged(99, DatagramSendState.Acknowledged);
        sink.OnDatagramSendStateChanged(99, DatagramSendState.LostSuspect);
        sink.OnDatagramSendStateChanged(99, DatagramSendState.Sent);
        sink.OnIdealSendBufferSize(default, 1);
        sink.OnStreamsAvailable(1, 1);
        sink.OnStreamPeerSendShutdown(default);
        sink.OnStreamStarted(default, 1, TransportStatus.Failed);
        sink.OnStreamAborted(new TransportStreamId(9, 1), 5, StreamAbortDirection.Receive);
        sink.OnStreamShutdownComplete(new TransportStreamId(9, 1));
        sink.OnPeerAddressChanged(new TransportConnectedInfo { RemoteEndPoint = new IPEndPoint(IPAddress.Loopback, 6) });
        Assert.Equal(6, peer.RemoteEndPoint!.Port);
        Assert.Equal(1, peer.Core.StreamCreditGeneration);
        Assert.Equal(1200, peer.Capabilities.MaxDatagramPayload);
        peer.GetStatistics(out PeerStatistics stats);
        Assert.Equal(3, stats.StaleCompletions);
        Assert.Equal(1100, stats.MaxDatagramPayload);
        sink.OnClosed(TransportCloseReason.Transport, 0, 42);
        peer.Poll();
        Assert.Equal(PeerState.Closed, peer.State);
        Assert.Equal(new CloseReason(QuiclyErrorCode.NoError) { Source = CloseSource.Transport, TransportStatus = 42 }, peer.CloseReason);
        peer.Dispose();
        Assert.True(peer.IsFreed);
        Assert.True(transport.Disposed);
        Assert.Null(transport.ClosedWith);
        sink.OnDatagramReceived([1, 2]);
        Assert.Equal(ReceiveResult.Consumed(0), sink.OnStreamReceived(default, [], 0, false));
        sink.OnClosed(TransportCloseReason.Local, 0, 0);
    }

    [Fact]
    public void A_Client_Whose_Control_Stream_Cannot_Open_Closes_With_InternalError()
    {
        FakeTransport transport = new() { StreamStatus = TransportStatus.OutOfMemory };
        QuiclyPeer peer = QuiclyPeer.Connect(new FakeConnector(transport), new IPEndPoint(IPAddress.Loopback, 1), null, TestTables.Default, new PeerOptions { Clock = new VirtualClock() });
        TransportConnectedInfo connected = default;
        peer.TransportSink.OnConnected(in connected);
        peer.Poll();
        Assert.Equal(PeerState.Closing, peer.State);
        Assert.Equal(QuiclyErrorCode.InternalError, peer.CloseReason.Code);
        Assert.Equal((ulong)QuiclyErrorCode.InternalError, transport.ClosedWith);
        peer.TransportSink.OnClosed(TransportCloseReason.Local, 7, 0);
        peer.Poll();
        Assert.Equal(PeerState.Closed, peer.State);
        peer.Dispose();
        Assert.True(peer.IsFreed);
    }

    [Fact]
    public void A_Client_That_Cannot_Send_Its_Hello_Closes_With_InternalError()
    {
        FakeTransport transport = new() { SendStatus = TransportStatus.InvalidState };
        QuiclyPeer peer = QuiclyPeer.Connect(new FakeConnector(transport), new IPEndPoint(IPAddress.Loopback, 1), null, TestTables.Default, new PeerOptions { Clock = new VirtualClock() });
        TransportConnectedInfo connected = default;
        peer.TransportSink.OnConnected(in connected);
        peer.Poll();
        Assert.Equal(PeerState.Closing, peer.State);
        Assert.Equal(QuiclyErrorCode.InternalError, peer.CloseReason.Code);
        peer.GetStatistics(out PeerStatistics stats);
        Assert.Equal(1, stats.ControlSendFailures);
        peer.Dispose();
        Assert.False(peer.IsFreed);
        peer.TransportSink.OnClosed(TransportCloseReason.Local, 0, 0);
        Assert.True(peer.IsFreed);
    }

    [Fact]
    public void Large_Control_Messages_Use_A_Lease_And_The_Preamble_Is_Sent_Once()
    {
        FakeTransport transport = new();
        byte[] token = new byte[100];
        QuiclyPeer peer = QuiclyPeer.Connect(new FakeConnector(transport), new IPEndPoint(IPAddress.Loopback, 1), null, TestTables.Default,
            new PeerOptions { Clock = new VirtualClock() }, token);
        TransportConnectedInfo connected = default;
        peer.TransportSink.OnConnected(in connected);
        peer.Poll();
        Assert.Single(transport.StreamSends);
        (TransportStreamId id, byte[] hello, TransportSendFlags flags) = transport.StreamSends[0];
        Assert.Equal(0, hello[0]);
        Assert.Equal(TransportSendFlags.Start | TransportSendFlags.Priority, flags);
        Assert.True(hello.Length > 100);
        peer.Close(new CloseReason(QuiclyErrorCode.NoError, "done"));
        Assert.Equal(2, transport.StreamSends.Count);
        Assert.Equal(id, transport.StreamSends[1].Id);
        Assert.NotEqual(0, transport.StreamSends[1].Data[0]);
        Assert.Equal(TransportSendFlags.Priority, transport.StreamSends[1].Flags);
        peer.TransportSink.OnClosed(TransportCloseReason.Local, 0, 0);
        peer.Poll();
        peer.Dispose();
    }
}
