using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Support types of the scheduler and the datagram engines, tested directly.</summary>
public class SchedulerUnitTests
{
    [Fact]
    public void Token_Bucket_Overdraws_And_Repays_Its_Debt()
    {
        TokenBucket bucket = default;
        bucket.Initialize(1_000, 100, 0);
        Assert.Equal(100, bucket.Available(0));
        Assert.Equal(0, bucket.MicrosUntil(100));
        bucket.Consume(150);
        Assert.Equal(-50, bucket.Available(0));
        Assert.Equal(51_000, bucket.MicrosUntil(1));
        Assert.False(bucket.TryTake(0));
        Assert.Equal(1, bucket.Available(51_000));
        Assert.Equal(100, bucket.Available(10_000_000));
        Assert.True(bucket.TryTake(10_000_000, 100));
        Assert.False(bucket.TryTake(10_000_000));
        bucket.Consume(1);
        Assert.Equal(-1, bucket.Available(10_000_000));

        TokenBucket stopped = default;
        stopped.Initialize(0, 1, 0);
        stopped.Consume(2);
        Assert.Equal(long.MaxValue, stopped.MicrosUntil(1));
    }

    [Fact]
    public void Token_Bucket_Carries_Its_Level_Across_A_Rate_Change()
    {
        // The ReliableLatest retry budget derives its rate from the congestion window, so the rate moves on every pass: a
        // change must carry the level over instead of refilling, or the cap would hand out a full burst each time and never
        // bind (PROTOCOL.md §4.4).
        TokenBucket fresh = default;
        fresh.SetRate(1_000, 100, 0);
        Assert.Equal(100, fresh.Available(0));

        TokenBucket bucket = default;
        bucket.Initialize(1_000, 100, 0);
        bucket.Consume(90);
        Assert.Equal(10, bucket.Available(0));

        // The level survives the change; the new rate then refills it up to the new burst.
        bucket.SetRate(2_000, 200, 0);
        Assert.Equal(10, bucket.Available(0));
        Assert.Equal(200, bucket.Available(1_000_000));

        // A smaller burst clamps what is already there.
        bucket.SetRate(500, 5, 1_000_000);
        Assert.Equal(5, bucket.Available(1_000_000));

        // A debt is carried too, not forgiven.
        bucket.Consume(20);
        bucket.SetRate(1_000, 50, 1_000_000);
        Assert.Equal(-15, bucket.Available(1_000_000));
    }

    private static readonly ChannelTable KeyTables = ChannelTable.Create()
        .Add(2, "hashed", ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.MaxKeys = 3;
        })
        .Add(3, "dense", ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.KeySpace = KeySpace.Dense(9);
            o.SequenceBits = 16;
        })
        .Add(4, "many", ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.MaxKeys = 1_000;
        })
        .Build();

    /// <summary>The tracker takes sequences extended on the channel's clock: the first sequence of a 32-bit channel is seeded one span up.</summary>
    private const ulong Wide = 0x1_0000_0000UL;

    /// <summary>The seed of a 16-bit channel's clock.</summary>
    private const ulong Narrow = 0x1_0000UL;

    [Fact]
    public void Key_Tracker_Orders_By_Extended_Value()
    {
        // 40 000 channel messages after a key's last value is more than half of a 16-bit space: in serial arithmetic the
        // new value reads as older. On the extended scale it is simply greater.
        using ReceiveKeyTracker keys = new(KeyTables[3]!);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(1, Narrow, out int slot));
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(1, Narrow + 40_000, out int again));
        Assert.Equal(slot, again);
        Assert.Equal(KeyAcceptance.Stale, keys.TryAcceptSequence(1, Narrow + 40_000, out _));
        Assert.Equal(KeyAcceptance.Stale, keys.TryAcceptSequence(1, Narrow + 39_999, out _));

        // Several cycles later the same low bits are a new value.
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(1, (5 * Narrow) + 40_000, out _));
    }

    [Fact]
    public void Key_Tracker_Accepts_Newer_Values_And_Evicts_The_Least_Recently_Updated_Key()
    {
        using ReceiveKeyTracker keys = new(KeyTables[2]!);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(10, Wide + 5, out int slot10));
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(20, Wide + 6, out _));
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(30, Wide + 7, out _));
        Assert.Equal(KeyAcceptance.Stale, keys.TryAcceptSequence(10, Wide + 5, out _));
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(10, Wide + 8, out int again));
        Assert.Equal(slot10, again);

        // Least recently updated first: 20, 30, 10.
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(40, Wide + 1, out _));
        Assert.Equal(1, keys.Evictions);
        Assert.Equal(3, keys.Count);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(20, Wide + 0, out _));
        Assert.Equal(2, keys.Evictions);
        Assert.Equal(KeyAcceptance.Stale, keys.TryAcceptSequence(10, Wide + 8, out _));
        Assert.True(keys.TryTouch(30, out _));
        Assert.Equal(3, keys.Evictions);
        Assert.True(keys.TryTouch(30, out _));
        Assert.Equal(3, keys.Evictions);

        keys.Clear();
        Assert.Equal(0, keys.Count);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(10, Wide + 0, out _));
    }

    [Fact]
    public void Key_Tracker_Uses_The_Key_As_Slot_In_A_Dense_Key_Space()
    {
        using ReceiveKeyTracker keys = new(KeyTables[3]!);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(9, Narrow + 65_535, out int slot9));
        Assert.Equal(9, slot9);

        // The wire sequence wraps to 0; on the channel's clock that is the next cycle, and 65 535 is now behind it.
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(9, Narrow + 65_536, out _));
        Assert.Equal(KeyAcceptance.Stale, keys.TryAcceptSequence(9, Narrow + 65_535, out _));
        Assert.Equal(KeyAcceptance.Rejected, keys.TryAcceptSequence(10, Narrow + 1, out int none));
        Assert.Equal(-1, none);
        Assert.False(keys.TryTouch(10, out _));
        Assert.True(keys.TryTouch(3, out int slot3));
        Assert.Equal(3, slot3);
        Assert.True(keys.TryTouch(3, out _));
        Assert.Equal(2, keys.Count);
        Assert.Equal(0, keys.Evictions);
        keys.Clear();
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(9, Narrow + 65_535, out _));
    }

    [Fact]
    public void Key_Tracker_Grows_With_The_Keys_In_Use()
    {
        using ReceiveKeyTracker keys = new(KeyTables[4]!);
        for (ulong key = 0; key < 1_000; key++)
        {
            Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(key * 7_919, Wide + key, out _));
        }

        for (ulong key = 0; key < 1_000; key++)
        {
            Assert.Equal(KeyAcceptance.Stale, keys.TryAcceptSequence(key * 7_919, Wide + key, out _));
        }

        Assert.Equal(1_000, keys.Count);
        Assert.Equal(0, keys.Evictions);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(123_456_789, Wide + 1, out _));
        Assert.Equal(1, keys.Evictions);

        // Key 0 was the least recently updated one, so it was evicted and is new again.
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(0, Wide + 0, out _));
        Assert.Equal(2, keys.Evictions);
    }

    [Fact]
    public void Local_Completions_Keep_Their_Order_And_Status()
    {
        using SessionHarness h = new(table: DatagramTables.Main, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        PeerCore core = h.Client.Core;
        Assert.True(core.TryAllocateEntry(2, SendEntryFlags.None, out int a));
        Assert.True(core.TryAllocateEntry(2, SendEntryFlags.None, out int b));
        core.QueueLocalCompletion(a, DeliveryStatus.Expired);
        core.QueueLocalCompletion(b, DeliveryStatus.Canceled);
        Assert.Equal(2, core.LocalCompletionsQueued);
        Assert.True(core.TryDequeueLocalCompletion(out CompletionEntry first));
        Assert.Equal(a, first.Slot);
        Assert.Equal(core.Entries[a].Generation, first.Generation);
        Assert.Equal(CompletionKind.Local, first.Kind);
        Assert.True(first.Final);
        Assert.Equal(DeliveryStatus.Expired, core.MapCompletion(in first));
        Assert.True(core.TryDequeueLocalCompletion(out CompletionEntry second));
        Assert.Equal(DeliveryStatus.Canceled, core.MapCompletion(in second));
        Assert.False(core.TryDequeueLocalCompletion(out _));
        core.DiscardEntry(a);
        core.DiscardEntry(b);

        // The queue wraps around its end.
        for (int round = 0; round < 300; round++)
        {
            Assert.True(core.TryAllocateEntry(2, SendEntryFlags.None, out int slot));
            core.QueueLocalCompletion(slot, DeliveryStatus.Failed);
            Assert.True(core.TryDequeueLocalCompletion(out CompletionEntry item));
            Assert.Equal(slot, item.Slot);
            core.DiscardEntry(slot);
        }

        Assert.Equal(DeliveryStatus.Lost, core.MapCompletion(new CompletionEntry { Kind = CompletionKind.Datagram, DatagramState = DatagramSendState.LostDiscarded, Final = true }));
        Assert.Equal(DeliveryStatus.Delivered, core.MapCompletion(new CompletionEntry { Kind = CompletionKind.Stream, Final = true }));
        Assert.Equal(DeliveryStatus.Failed, core.MapSubmitFailure(TransportStatus.TooLarge));
        Assert.Equal(DeliveryStatus.Failed, core.MapSubmitFailure(TransportStatus.InvalidState));
        h.Client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.Equal(DeliveryStatus.Disconnected, core.MapSubmitFailure(TransportStatus.InvalidState));
    }

    [Fact]
    public void The_Packer_Is_Idle_Between_Passes()
    {
        using SessionHarness h = new(table: DatagramTables.Main, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        Assert.True(h.Client.Core.Packer.IsIdle);
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.SendCopy(new SendHeader(2), [2]);
        h.Client.SendCopy(new SendHeader(3, 1), [3]);
        h.Client.Flush();
        Assert.True(h.Client.Core.Packer.IsIdle);
        Assert.Equal(0, h.Client.Core.LocalCompletionsQueued);
    }
}
