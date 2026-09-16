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

    [Fact]
    public void Key_Tracker_Accepts_Newer_Values_And_Evicts_The_Least_Recently_Updated_Key()
    {
        using ReceiveKeyTracker keys = new(KeyTables[2]!);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(10, 5, false, out int slot10));
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(20, 6, false, out _));
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(30, 7, false, out _));
        Assert.Equal(KeyAcceptance.Stale, keys.TryAcceptSequence(10, 5, false, out _));
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(10, 8, false, out int again));
        Assert.Equal(slot10, again);

        // Least recently updated first: 20, 30, 10.
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(40, 1, false, out _));
        Assert.Equal(1, keys.Evictions);
        Assert.Equal(3, keys.Count);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(20, 0, false, out _));
        Assert.Equal(2, keys.Evictions);
        Assert.Equal(KeyAcceptance.Stale, keys.TryAcceptSequence(10, 8, false, out _));
        Assert.True(keys.TryTouch(30, out _));
        Assert.Equal(3, keys.Evictions);
        Assert.True(keys.TryTouch(30, out _));
        Assert.Equal(3, keys.Evictions);

        keys.Clear();
        Assert.Equal(0, keys.Count);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(10, 0, false, out _));
    }

    [Fact]
    public void Key_Tracker_Uses_The_Key_As_Slot_In_A_Dense_Key_Space()
    {
        using ReceiveKeyTracker keys = new(KeyTables[3]!);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(9, 65_535, true, out int slot9));
        Assert.Equal(9, slot9);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(9, 0, true, out _));
        Assert.Equal(KeyAcceptance.Stale, keys.TryAcceptSequence(9, 65_535, true, out _));
        Assert.Equal(KeyAcceptance.Rejected, keys.TryAcceptSequence(10, 1, true, out int none));
        Assert.Equal(-1, none);
        Assert.False(keys.TryTouch(10, out _));
        Assert.True(keys.TryTouch(3, out int slot3));
        Assert.Equal(3, slot3);
        Assert.True(keys.TryTouch(3, out _));
        Assert.Equal(2, keys.Count);
        Assert.Equal(0, keys.Evictions);
        keys.Clear();
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(9, 65_535, true, out _));
    }

    [Fact]
    public void Key_Tracker_Grows_With_The_Keys_In_Use()
    {
        using ReceiveKeyTracker keys = new(KeyTables[4]!);
        for (ulong key = 0; key < 1_000; key++)
        {
            Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(key * 7_919, (uint)key, false, out _));
        }

        for (ulong key = 0; key < 1_000; key++)
        {
            Assert.Equal(KeyAcceptance.Stale, keys.TryAcceptSequence(key * 7_919, (uint)key, false, out _));
        }

        Assert.Equal(1_000, keys.Count);
        Assert.Equal(0, keys.Evictions);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(123_456_789, 1, false, out _));
        Assert.Equal(1, keys.Evictions);

        // Key 0 was the least recently updated one, so it was evicted and is new again.
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(0, 0, false, out _));
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
