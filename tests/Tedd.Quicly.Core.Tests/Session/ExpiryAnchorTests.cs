using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Where a message's expiry starts (PROTOCOL.md §4.5; docs/design/session-layer.md §7.1): at the first scheduler pass after
/// its admission, never at the clock stamp of whatever pass happened to run last before the send. A message that was never
/// held back is never expired — not after a long frame, not on a peer nobody polled, not inside its own Immediate send —
/// and a message that <em>is</em> held back still expires once it has waited its expiry across passes.
/// </summary>
/// <remarks>
/// A "stall" here is <c>Network.Advance</c> with no call on the peer: the peers share the network's virtual clock, so the
/// clock moves while the peer's last pass stamp stays where it was.
/// </remarks>
public class ExpiryAnchorTests
{
    private const long Stall = 250_000;

    private static readonly ChannelTable Table = DatagramTables.Main;

    /// <summary>A quiet client whose send cap (1 000 B/s, a 33-byte burst) one 900-byte datagram overdraws for most of a second.</summary>
    private static void Capped(PeerOptions options)
    {
        DatagramKit.Quiet(options);
        options.MaxSendBytesPerSecond = 1_000;
    }

    /// <summary>
    /// Sends and flushes the filler that overdraws the send cap: the packer accepts it while the balance is positive and the
    /// debt then blocks every datagram for about 870 ms.
    /// </summary>
    private static void OverdrawTheCap(SessionHarness h)
    {
        QuiclyPeer client = h.Client;
        long sent = DatagramKit.ChannelStats(client, 2).Sent;
        Assert.True(client.SendCopy(new SendHeader(2), new byte[900]).IsAdmitted);
        client.Flush();
        Assert.Equal(sent + 1, DatagramKit.ChannelStats(client, 2).Sent);
    }

    // ------------------------------------------------------------------ regressions: never held back, never expired

    [Fact]
    public void A_Send_After_A_Stall_Longer_Than_Its_Expiry_Is_Delivered()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(14, Handlers.Collect(got));
        h.Pump();

        // Five expiries pass with no Poll and no Flush: the peer's last pass stamp is that old when the message is sent.
        h.Network.Advance(Stall);
        SendResult result = client.SendCopy(new SendHeader(14, 1), [7], SendOptions.Tracked);
        Assert.True(result.IsAdmitted);
        client.Flush();
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(result.Token) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(result.Token));
        Assert.Equal(new byte[] { 7 }, Assert.Single(got).Payload);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 14).Expired);
    }

    [Fact]
    public void A_Send_On_A_Default_Expiry_Channel_Survives_A_Stall()
    {
        // Channel 3 of the default table is UnreliableSequenced with the default expiry, 2 × FlushInterval.
        using SessionHarness h = new(table: TestTables.Default, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        long expiry = client.Channels[3]!.ResolveExpiryMicros(client.Core.FlushIntervalMicros);
        Assert.True(expiry > 0, "the channel has no default expiry");
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(3, Handlers.Collect(got));
        h.Pump();

        h.Network.Advance(5 * expiry);
        SendResult result = client.SendCopy(new SendHeader(3, 1), [7], SendOptions.Tracked);
        Assert.True(result.IsAdmitted);
        client.Flush();
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(result.Token) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(result.Token));
        Assert.Single(got);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 3).Expired);
    }

    [Fact]
    public void A_Long_Frame_Between_Send_And_Flush_Does_Not_Expire()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(14, Handlers.Collect(got));

        // Poll, send, a simulation step longer than the expiry, Flush: the Flush is the message's first pass.
        client.Poll();
        SendResult result = client.SendCopy(new SendHeader(14, 1), [7], SendOptions.Tracked);
        Assert.True(result.IsAdmitted);
        h.Network.Advance(Stall);
        client.Flush();
        Assert.Equal(1, DatagramKit.ChannelStats(client, 14).Sent);
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(result.Token) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(result.Token));
        Assert.Single(got);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 14).Expired);
    }

    [Fact]
    public void An_Immediate_Send_After_A_Stall_Goes_Out()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(14, Handlers.Collect(got));
        client.Poll();
        h.Network.Advance(Stall);
        Assert.True(client.SendCopy(new SendHeader(14, 1), [7], SendOptions.Immediate).IsAdmitted);

        // The send ran its own pass, which is the message's first: it is with the transport before any Flush.
        ChannelStatistics statistics = DatagramKit.ChannelStats(client, 14);
        Assert.Equal(1, statistics.Sent);
        Assert.Equal(0, statistics.Expired);
        Assert.False(client.Core.HasPendingExpiry);
        Assert.True(h.RunUntil(() => got.Count == 1));
    }

    [Fact]
    public void Expiry_Counts_From_The_First_Pass()
    {
        using SessionHarness h = new(table: Table, client: Capped, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        OverdrawTheCap(h); // t0: the last pass before the send

        SendResult result = client.SendCopy(new SendHeader(14, 1), [7], SendOptions.Tracked);
        Assert.True(result.IsAdmitted);
        h.Network.Advance(40_000);
        client.Flush(); // t0 + 40 ms: the first pass, blocked by the cap — the 50 ms start here
        Assert.Equal(DeliveryStatus.Pending, client.GetDeliveryStatus(result.Token));

        h.Network.Advance(40_000);
        client.Flush(); // t0 + 80 ms: 40 ms held back
        Assert.Equal(DeliveryStatus.Pending, client.GetDeliveryStatus(result.Token));
        Assert.Equal(0, DatagramKit.ChannelStats(client, 14).Expired);

        h.Network.Advance(11_000);
        client.Flush(); // t0 + 91 ms: 51 ms held back
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(result.Token));
        Assert.Equal(1, DatagramKit.ChannelStats(client, 14).Expired);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 14).Sent);
    }

    [Fact]
    public void A_Fragmented_Message_Sent_After_A_Stall_Is_Delivered_Whole()
    {
        using SessionHarness h = new(table: FragmentTables.Main, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        h.Pump();
        h.Network.Advance(20_000);
        byte[] payload = DatagramKit.Payload(1, 4_000);
        SendResult result = client.SendCopy(new SendHeader(2), payload, new SendOptions { Track = true, ExpiryMicros = 1_000 });
        Assert.True(result.IsAdmitted);
        client.Flush();
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(result.Token) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(result.Token));
        Assert.Equal(payload, Assert.Single(got).Payload);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 2).Expired);
    }

    [Fact]
    public void An_Expiring_Ordered_Message_Sent_After_A_Stall_Is_Delivered()
    {
        using SessionHarness h = new(table: OrderedTables.Main, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));

        // The channel's stream is open, so nothing holds the next message back.
        Assert.True(client.SendCopy(new SendHeader(4), [0]).IsAdmitted);
        Assert.True(h.RunUntil(() => got.Count == 1));

        h.Network.Advance(5_000);
        SendResult result = client.SendCopy(new SendHeader(4), [7], new SendOptions { Track = true, ExpiryMicros = 1_000 });
        Assert.True(result.IsAdmitted);
        client.Flush();
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(result.Token) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(result.Token));
        Assert.Equal(2, got.Count);
        Assert.Equal(new byte[] { 7 }, got[1].Payload);
        Assert.Equal(0, OrderedKit.Stats(client, 4).Expired);
    }

    [Fact]
    public void An_Expiring_Group_Message_Sent_After_A_Stall_Is_Delivered()
    {
        using SessionHarness h = new(table: GroupTables.Main, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        h.Pump();
        h.Network.Advance(5_000);
        SendResult result = client.SendCopy(new SendHeader(5), [7], new SendOptions { Track = true, ExpiryMicros = 1_000 });
        Assert.True(result.IsAdmitted);
        client.Flush();
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(result.Token) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(result.Token));
        Assert.Equal(new byte[] { 7 }, Assert.Single(got).Payload);
        Assert.Equal(0, GroupKit.Stats(client, 5).Expired);
    }

    [Fact]
    public void A_Thread_Safe_Send_Admitted_By_Poll_Survives_A_Late_Flush()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.ThreadSafeSend = true;
        }, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(14, Handlers.Collect(got));
        client.Poll();

        SendStatus status = SendStatus.QueueFull;
        Thread sender = new(() => status = client.SendCopy(new SendHeader(14, 1), [7]).Status) { IsBackground = true };
        sender.Start();
        sender.Join();
        Assert.Equal(SendStatus.Admitted, status);

        // The Poll admits the message from the front; the Flush, a long frame later, is its first pass.
        client.Poll();
        Assert.Equal(1, DatagramKit.ChannelStats(client, 14).QueuedMessages);
        h.Network.Advance(Stall);
        client.Flush();
        Assert.True(h.RunUntil(() => got.Count == 1, 1_000_000));
        Assert.Equal(0, DatagramKit.ChannelStats(client, 14).Expired);
    }

    // ------------------------------------------------------------------ guards: held back past its expiry, it still expires

    [Fact]
    public void A_Message_Held_Back_Past_Its_Expiry_Still_Expires()
    {
        using SessionHarness h = new(table: Table, client: Capped, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        OverdrawTheCap(h);
        SendResult result = client.SendCopy(new SendHeader(14, 1), [7], SendOptions.Tracked);
        Assert.True(result.IsAdmitted);
        client.Flush(); // the first pass: blocked by the cap
        Assert.Equal(DeliveryStatus.Pending, client.GetDeliveryStatus(result.Token));
        Assert.False(client.Core.HasPendingExpiry);

        // Exactly the expiry later the message is still eligible (the comparison is strict) …
        h.Network.Advance(50_000);
        client.Flush();
        Assert.Equal(DeliveryStatus.Pending, client.GetDeliveryStatus(result.Token));

        // … and one microsecond after that it is not.
        h.Network.Advance(1);
        client.Flush();
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(result.Token));
        Assert.Equal(1, DatagramKit.ChannelStats(client, 14).Expired);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 14).Sent);
    }

    [Theory]
    [InlineData(1L)]
    [InlineData(50_000L)]
    [InlineData(PeerCore.MaxRelativeExpiryMicros - 1)]
    public unsafe void A_Stamped_Deadline_Resolves_To_The_Pass_Clock_Plus_The_Expiry(long expiry)
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        PeerCore core = h.Client.Core;
        Assert.False(core.HasPendingExpiry);
        Assert.True(core.TryAllocateEntry(14, SendEntryFlags.None, out int slot));
        core.StampExpiry(slot, expiry);
        Assert.True(core.HasPendingExpiry);
        long unresolved = core.Entries.Deadlines[slot];
        Assert.Equal(PeerCore.UnresolvedDeadline + expiry, unresolved);

        // Before its first pass the entry can not be expired by any clock the peer could see.
        Assert.False(PeerCore.MaxRelativeExpiryMicros > unresolved);

        const long Now = 123_456_789;
        core.ResolveExpiry(Now);
        Assert.False(core.HasPendingExpiry);
        Assert.Equal(Now + expiry, core.Entries.Deadlines[slot]);

        // A second resolve finds nothing to do, whatever its clock.
        core.ResolveExpiry(Now + 1_000);
        Assert.Equal(Now + expiry, core.Entries.Deadlines[slot]);
        core.DiscardEntry(slot);
    }

    [Theory]
    [InlineData(PeerCore.MaxRelativeExpiryMicros)]
    [InlineData(long.MaxValue)]
    public void An_Expiry_Beyond_Any_Clock_Means_Never(long expiry)
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        PeerCore core = h.Client.Core;
        Assert.True(core.TryAllocateEntry(14, SendEntryFlags.None, out int slot));
        core.StampExpiry(slot, expiry);
        Assert.False(core.HasPendingExpiry);
        Assert.Equal(0, core.Entries.Deadlines[slot]);
        core.ResolveExpiry(123_456_789);
        Assert.Equal(0, core.Entries.Deadlines[slot]);
        core.DiscardEntry(slot);
    }

    [Fact]
    public void Every_Slot_Of_The_Table_Resolves_Including_The_Word_Boundaries()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        PeerCore core = h.Client.Core;
        int capacity = core.Entries.Capacity;
        List<int> slots = [];
        while (core.TryAllocateEntry(14, SendEntryFlags.None, out int slot))
        {
            slots.Add(slot);
        }

        Assert.True(slots.Count >= capacity - 8, "the idle peer holds more entries than expected");
        int[] stamped = [63, 64, 65, 127, 128, capacity - 1];
        Assert.All(stamped, slot => Assert.Contains(slot, slots));
        foreach (int slot in stamped)
        {
            core.StampExpiry(slot, 1_000 + slot);
        }

        const long Now = 5_000_000;
        core.ResolveExpiry(Now);
        Assert.False(core.HasPendingExpiry);
        foreach (int slot in slots)
        {
            long expected = Array.IndexOf(stamped, slot) >= 0 ? Now + 1_000 + slot : 0;
            Assert.Equal(expected, core.Entries.Deadlines[slot]);
        }

        // A lower word stamped after a higher one was resolved is still found.
        int low = slots.First(slot => slot < 63);
        core.Entries.Deadlines[low] = 0;
        core.StampExpiry(low, 10);
        core.ResolveExpiry(Now + 5);
        Assert.Equal(Now + 15, core.Entries.Deadlines[low]);
        foreach (int slot in slots)
        {
            core.DiscardEntry(slot);
        }
    }

    [Fact]
    public void A_Mark_Left_On_A_Recycled_Slot_Changes_Nothing()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        PeerCore core = h.Client.Core;

        // Stamped, then freed before any pass (a cancel): the slot's next occupant has no expiry.
        Assert.True(core.TryAllocateEntry(14, SendEntryFlags.None, out int slot));
        core.StampExpiry(slot, 1_000);
        core.DiscardEntry(slot);
        Assert.True(core.TryAllocateEntry(14, SendEntryFlags.None, out int again));
        Assert.Equal(slot, again);
        Assert.Equal(0, core.Entries.Deadlines[slot]);
        Assert.True(core.HasPendingExpiry);
        core.ResolveExpiry(9_000_000);
        Assert.Equal(0, core.Entries.Deadlines[slot]);
        Assert.False(core.HasPendingExpiry);

        // The same with an occupant that keeps an absolute deadline of its own (a ReliableLatest version budget).
        core.StampExpiry(slot, 1_000);
        core.DiscardEntry(slot);
        Assert.True(core.TryAllocateEntry(14, SendEntryFlags.None, out again));
        Assert.Equal(slot, again);
        core.Entries.Deadlines[slot] = 30_000_000;
        core.ResolveExpiry(9_000_000);
        Assert.Equal(30_000_000, core.Entries.Deadlines[slot]);
        core.DiscardEntry(slot);
    }
}
