using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Failing tests of the wave C1 performance / memory / threading review. Each test pins one defect against the
/// normative documents (ADR 0008, ARCHITECTURE.md §3/§4/§9); none of them fixes production code.
/// </summary>
public class ReviewPerfTests
{
    /// <summary>
    /// ARCHITECTURE.md §9 publishes the per-peer receive ring as "4 096 × 32 B" — one of the numbers
    /// <c>ServerOptions.ExpectedPeers</c> scales. <see cref="ReceiveEntry"/> is 64 bytes, so the ring is twice the
    /// published size, and it is a GC-heap <c>ReceiveEntry[]</c> (256 KiB, large-object heap) rather than the native
    /// memory ADR 0008 invariant 12 and ARCHITECTURE.md §4 require for hot struct arrays.
    /// </summary>
    [Fact]
    public void Receive_Ring_Matches_The_Published_32_Byte_Entry_Sizing()
    {
        using SessionHarness h = new(table: OrderedTables.Main);
        PeerCore core = h.Client.Core;
        int receiveEntry = Unsafe.SizeOf<ReceiveEntry>();
        long actual = (long)core.ReceiveRing.Capacity * receiveEntry;

        Assert.True(
            receiveEntry == 32,
            $"ARCHITECTURE.md §9 sizes the receive ring at 32 B per entry; ReceiveEntry is {receiveEntry} B, so the ring "
            + $"is {actual / 1024} KiB instead of the published {core.ReceiveRing.Capacity * 32 / 1024} KiB "
            + "(and it is a GC-heap array, not native memory: ADR 0008 invariant 12).");
    }

    /// <summary>
    /// ADR 0008 invariant 5 sizes the completion ring so that it "cannot overflow"; as built the peer needs at most two
    /// entries per send slot (one early <c>Sent</c> notice plus one final completion). <c>PeerCore</c> asks for
    /// <c>(2 × capacity) + 1</c>, and because <c>SpscRing</c> rounds up to a power of two while <c>capacity</c> already is
    /// one, that single extra slot doubles the ring to <b>4 × the send table</b>. Asking for <c>2 × capacity</c> holds the
    /// same two-per-entry guarantee exactly and halves the memory.
    /// </summary>
    [Fact]
    public void Completion_Ring_Holds_At_Most_Two_Completions_Per_Send_Entry()
    {
        using SessionHarness h = new(table: OrderedTables.Main);
        PeerCore core = h.Client.Core;
        int sendTable = core.Entries.Capacity;
        int completionSlots = core.CompletionRing.Capacity;
        long completionBytes = (long)completionSlots * Unsafe.SizeOf<CompletionEntry>();

        Assert.True(
            completionSlots <= 2 * sendTable,
            $"Two completions per send entry need {2 * sendTable} slots for a send table of {sendTable}, but PeerCore asks "
            + $"for (2 × {sendTable}) + 1 and SpscRing rounds that up to {completionSlots} slots "
            + $"({completionBytes / 1024} KiB, {Unsafe.SizeOf<CompletionEntry>()} B per entry). "
            + "Pass 2 × capacity instead of (2 × capacity) + 1.");
    }

    /// <summary>
    /// ADR 0008 invariant 12 ("struct arrays are reference-free, 64-byte aligned, in native memory or the pinned object
    /// heap") and ARCHITECTURE.md §3 ("nothing in the hot path allocates on the GC heap after warm-up"). The per-channel
    /// drain queues are built lazily inside <see cref="QuiclyPeer.Poll"/>, the first time a message arrives for a channel
    /// that has no handler: <c>ReceiveQueues</c> allocates <c>ReceiveEntry[ReceiveRingCapacity]</c> (256 KiB with the
    /// defaults — a large-object-heap allocation) plus an <c>int[]</c> of the same length, on the game thread, at an
    /// unpredictable moment in the middle of a session.
    /// </summary>
    [Fact]
    public void Receiving_On_A_Channel_Without_A_Handler_Does_Not_Allocate_In_Poll()
    {
        using SessionHarness h = new(table: OrderedTables.Main);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long handled = 0;

        // Channel 4 has a handler; channel 5 deliberately has none, so its messages go to the lazy drain queues.
        server.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => handled++);
        byte[] payload = new byte[64];

        // Warm up every path this test then measures, using only the channel that has a handler.
        for (int i = 0; i < 400; i++)
        {
            client.SendCopy(new SendHeader(4), payload);
            client.Flush();
            network.Advance(1_000);
            server.Poll();
            client.Poll();
        }

        Assert.True(handled > 0, "the warm-up delivered nothing");

        // One message on the handler-less channel, then the Poll that must queue it.
        client.SendCopy(new SendHeader(5), payload);
        client.Flush();
        network.Advance(1_000);

        long before = GC.GetAllocatedBytesForCurrentThread();
        server.Poll();
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(
            allocated < 64 * 1024,
            $"Poll allocated {allocated} bytes on the game thread when the first message reached a channel without a "
            + "handler: ReceiveQueues is built lazily there as GC-heap ReceiveEntry[] + int[] sized from "
            + "ReceiveRingCapacity (ADR 0008 invariant 12; ARCHITECTURE.md §3). Allocate it up front, from native memory.");
    }
}
