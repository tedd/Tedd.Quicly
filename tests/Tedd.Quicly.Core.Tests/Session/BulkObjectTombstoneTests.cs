using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The receiver holds an ended object's identity for a while so a range still arriving for it cannot start a second
/// object. Driven directly rather than through a peer, because what matters is the exact timeline.
/// </summary>
public class BulkObjectTombstoneTests
{
    private sealed class Router : IBulkObjectRouter
    {
        public List<MemoryObjectSink> Sinks { get; } = [];

        public BulkObjectReceiveDecision SelectTarget(in BulkObjectInfo info)
        {
            MemoryObjectSink sink = new(info.TotalLength);
            Sinks.Add(sink);
            return BulkObjectReceiveDecision.Accept(sink);
        }
    }

    private const long Total = 4096;

    private static BulkTransferInfo Range(ulong transferId, long offset, long length) => new()
    {
        Channel = 5,
        TransferId = transferId,
        ObjectId = 11,
        ObjectVersion = 1,
        TotalLength = Total,
        Offset = offset,
        Length = length,
        HasChecksum = true,
    };

    private static BulkObjectReceiver Receiver(Router router, VirtualClock clock) =>
        new(router, clock, objects: 2, shims: 4, progressBytes: 1 << 20, _ => { });

    /// <summary>Begins an object and ends it the way a cancelled range would.</summary>
    private static void BeginAndEnd(BulkObjectReceiver receiver, Router router, ulong transferId)
    {
        BulkReceiveDecision decision = receiver.SelectTarget(Range(transferId, 0, 2048));
        Assert.True(decision.Accepted, "the object's first range was refused");
        decision.Sink!.Finish(new BulkResult(BulkStatus.Canceled, 0, QuiclyErrorCode.BulkCanceled));
        Assert.True(router.Sinks[^1].IsFinished, "the object did not end when its only range did");
    }

    /// <summary>A range of an object that already ended is refused rather than taken as the start of a new one.</summary>
    [Fact]
    public void A_Range_Of_An_Ended_Object_Is_Refused()
    {
        VirtualClock clock = new(1_000_000);
        Router router = new();
        BulkObjectReceiver receiver = Receiver(router, clock);
        BeginAndEnd(receiver, router, 1);

        clock.AdvanceMicros(1_000);
        BulkReceiveDecision late = receiver.SelectTarget(Range(2, 2048, 2048));

        Assert.False(late.Accepted, "a range arriving after the object ended started a second object");
        Assert.Equal(QuiclyErrorCode.BulkRejected, late.RejectCode);
        Assert.Single(router.Sinks);
    }

    /// <summary>
    /// The hold is a fixed window from when the object ended, so an object can always be sent again under the same
    /// identity afterwards. A hold that were extended by each stray could be kept alive by a peer's own retries and
    /// would strand that identity for good.
    /// </summary>
    [Fact]
    public void An_Identity_Comes_Back_However_Many_Strays_Arrive_While_It_Is_Held()
    {
        VirtualClock clock = new(1_000_000);
        Router router = new();
        BulkObjectReceiver receiver = Receiver(router, clock);
        BeginAndEnd(receiver, router, 1);

        // Strays land throughout the hold, none of them further apart than the hold is long.
        for (int i = 0; i < 4; i++)
        {
            clock.AdvanceMicros(400_000);
            Assert.False(receiver.SelectTarget(Range((ulong)(10 + i), 2048, 2048)).Accepted, $"stray {i} started a second object");
        }

        Assert.Single(router.Sinks);

        // Past the hold, the identity is free again.
        clock.AdvanceMicros(800_000);
        Assert.True(
            receiver.SelectTarget(Range(20, 0, 2048)).Accepted,
            "the object could not be sent again under the same identity after the hold");
        Assert.Equal(2, router.Sinks.Count);
    }
}
