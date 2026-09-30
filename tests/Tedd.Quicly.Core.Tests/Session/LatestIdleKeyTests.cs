using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// ReliableLatest with a key that idles while its channel's version counter runs on (PROTOCOL.md §8). The receiver orders
/// versions on the channel's version clock, so the key's next value is accepted however far the counter went; the sender
/// completes a value only on an ack that names exactly the version it last transmitted for the key, so an ack of the key's
/// previous value can never report a value <see cref="DeliveryStatus.Delivered"/> that did not arrive.
/// </summary>
public class LatestIdleKeyTests
{
    private const ushort Channel = 2;
    private const ulong A = 11;
    private const ulong B = 22;

    private static readonly ChannelTable Table = LatestTables.Main;

    private static SessionHarness Pair()
    {
        SessionHarness harness = new(link: new LinkOptions { DelayMicros = 2_000 }, table: Table, client: LatestKit.Quiet, server: LatestKit.Quiet);
        harness.Run(50_000);
        return harness;
    }

    private static ServerHarness Receiver(out List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received)
    {
        ServerHarness harness = new(table: Table, server: o =>
        {
            LatestKit.Quiet(o);
            LatestKit.Roomy(o);
        });
        Assert.True(harness.Admit(), "the raw client was not admitted");
        received = [];
        harness.Server!.RegisterHandler(Channel, LatestKit.Collect(received));
        return harness;
    }

    /// <summary>Sends one datagram value and lets the server dispatch it (a mailbox would coalesce two values of one key).</summary>
    private static void Value(ServerHarness h, ulong key, uint version, int id)
    {
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(LatestKit.Frame(Table, Channel, version, key, LatestKit.Payload(id, 16))));
        h.Run(20_000);
    }

    /// <summary>Sends a value of <paramref name="key"/> from the client and runs until it is no longer pending.</summary>
    private static DeliveryStatus Deliver(SessionHarness h, ulong key, int id, long maxMicros = 5_000_000)
    {
        SendResult result = h.Client.SendCopy(new SendHeader(Channel, key), LatestKit.Payload(id, 16), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) != DeliveryStatus.Pending, maxMicros);
        return h.Client.GetDeliveryStatus(result.Token);
    }

    private static bool Inject(QuiclyPeer peer, ulong key, uint version) =>
        LatestKit.Engine(peer).OnControl(ControlType.LatestAck, LatestKit.AckBody(Channel, key, version), onStream: false, 0);

    [Fact]
    public void A_Key_Idle_For_More_Than_Two_To_The_31_Versions_Is_Received_And_Delivered()
    {
        using SessionHarness h = Pair();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(Channel, LatestKit.Collect(received));
        Assert.Equal(DeliveryStatus.Delivered, Deliver(h, A, 1));

        // Key B keeps the channel's counter moving, in two steps of less than half the range, while key A idles.
        LatestKit.SetNextVersion(h.Client, Channel, 0x7000_0000);
        Assert.Equal(DeliveryStatus.Delivered, Deliver(h, B, 2));
        LatestKit.SetNextVersion(h.Client, Channel, 0xE000_0000);
        Assert.Equal(DeliveryStatus.Delivered, Deliver(h, B, 3));

        // Key A's next value is version 0xE000_0001: 2^31 + 0x6000_0000 past its last one, "older" in serial arithmetic.
        DeliveryStatus status = Deliver(h, A, 4);
        h.Run(50_000);
        Assert.Equal(4, received.Count);
        Assert.Equal((A, 0xE000_0001u), (received[^1].Key, received[^1].Version));
        Assert.True(LatestKit.Matches(received[^1].Payload, 4, 16));
        Assert.Equal(0, DatagramKit.ChannelStats(h.Server, Channel).Dropped);
        Assert.Equal(DeliveryStatus.Delivered, status);
        Assert.Equal(0xE000_0001u, LatestKit.AckedVersion(h.Client, Channel, A));
    }

    [Fact]
    public void A_Key_Idle_For_Exactly_Two_To_The_31_Versions_Is_Delivered()
    {
        using SessionHarness h = Pair();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(Channel, LatestKit.Collect(received));
        Assert.Equal(DeliveryStatus.Delivered, Deliver(h, A, 1));
        LatestKit.SetNextVersion(h.Client, Channel, 0x4000_0000);
        Assert.Equal(DeliveryStatus.Delivered, Deliver(h, B, 2));

        // Exactly half the range after key A's version 1: the one distance serial arithmetic calls older in both directions.
        LatestKit.SetNextVersion(h.Client, Channel, 0x8000_0001);
        DeliveryStatus status = Deliver(h, A, 3, maxMicros: 40_000_000);
        h.Run(50_000);
        Assert.Equal(DeliveryStatus.Delivered, status);
        Assert.Equal((A, 0x8000_0001u), (received[^1].Key, received[^1].Version));
        Assert.Equal(0, DatagramKit.ChannelStats(h.Server, Channel).Dropped);
    }

    [Fact]
    public void The_Receiver_Accepts_And_Acks_An_Idle_Keys_Value()
    {
        // The receiver alone (what a sender without this fix gains from a receiver with it).
        using ServerHarness h = Receiver(out List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received);
        Value(h, A, 1, 1);
        Value(h, B, 0x7000_0000, 2);
        Value(h, B, 0xE000_0000, 3);
        Value(h, A, 0xE000_0001, 4);
        h.Run(50_000);
        Assert.Equal(new uint[] { 1, 0x7000_0000, 0xE000_0000, 0xE000_0001 }, received.Select(r => r.Version).ToArray());
        Assert.True(LatestKit.Matches(received[^1].Payload, 4, 16));
        Assert.Equal(0, DatagramKit.ChannelStats(h.Server!, Channel).Dropped);
        LatestAckEntry last = LatestKit.AckEntries(h.Raw.Sink).Last(entry => entry.Key == A);
        Assert.Equal(0xE000_0001u, last.Version);
    }

    [Fact]
    public void An_Ack_Of_An_Older_Version_Never_Completes_The_Current_One()
    {
        // The sender alone, against what a receiver without this fix answers: it drops the idle key's new value as stale and
        // re-acks the version it holds. That ack must not complete the new value.
        using SessionHarness h = Pair();
        Assert.Equal(DeliveryStatus.Delivered, Deliver(h, A, 1));
        LatestKit.SetNextVersion(h.Client, Channel, 0xE000_0001);
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(64);
        SendResult result = h.Client.SendCopy(new SendHeader(Channel, A), LatestKit.Payload(2, 16), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        h.Client.Flush();
        Assert.True(Inject(h.Client, A, 1));
        h.Client.Poll();
        h.Client.Flush();
        Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(result.Token));
        Assert.Equal(1, LatestKit.LiveKeys(h.Client, Channel));

        // Honest about it: the value that never arrives ends Failed once its budget is spent (PROTOCOL.md §4.4).
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) != DeliveryStatus.Pending, 31_000_000, step: 10_000));
        Assert.Equal(DeliveryStatus.Failed, h.Client.GetDeliveryStatus(result.Token));
    }

    [Fact]
    public void An_Ack_Of_The_Previous_Value_Before_The_New_One_Is_Transmitted_Completes_Nothing()
    {
        using SessionHarness h = Pair();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(Channel, LatestKit.Collect(received));
        Assert.Equal(DeliveryStatus.Delivered, Deliver(h, A, 1));
        LatestKit.SetNextVersion(h.Client, Channel, 0x7000_0000);
        Assert.Equal(DeliveryStatus.Delivered, Deliver(h, B, 2));
        LatestKit.SetNextVersion(h.Client, Channel, 0xE000_0000);

        // Admitted, not flushed: nothing of the new value has left this host when a late duplicate of the old ack arrives.
        SendResult result = h.Client.SendCopy(new SendHeader(Channel, A), LatestKit.Payload(3, 16), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        Assert.True(Inject(h.Client, A, 1));

        // Admission drains the notices the transport thread handed over, so the ack has been applied when this returns.
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Channel, 33), LatestKit.Payload(4, 16)).Status);
        Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(result.Token));
        Assert.Equal(2, LatestKit.LiveKeys(h.Client, Channel));

        // And the value still goes out and completes on its own ack.
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {h.Client.GetDeliveryStatus(result.Token)}");
        Assert.Contains(received, r => r.Key == A && r.Version == 0xE000_0000 && LatestKit.Matches(r.Payload, 3, 16));
    }

    [Fact]
    public void An_Ack_Of_A_Superseded_Version_Completes_Nothing()
    {
        using SessionHarness h = Pair();
        LatestKit.SetNextVersion(h.Client, Channel, 5);
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(64);
        SendResult first = h.Client.SendCopy(new SendHeader(Channel, A), LatestKit.Payload(5, 16), SendOptions.Tracked);
        h.Client.Flush();
        SendResult second = h.Client.SendCopy(new SendHeader(Channel, A), LatestKit.Payload(6, 16), SendOptions.Tracked);
        h.Client.Flush();

        // Version 5 was on the wire, but it is not the version last transmitted for the key any more.
        Assert.True(Inject(h.Client, A, 5));
        h.Client.Poll();
        h.Client.Flush();
        Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(second.Token));
        Assert.Equal(0u, LatestKit.AckedVersion(h.Client, Channel, A));

        // A version that was never handed out proves nothing either.
        Assert.True(Inject(h.Client, A, 7));
        h.Client.Poll();
        h.Client.Flush();
        Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(second.Token));

        Assert.Equal(1, LatestKit.LiveKeys(h.Client, Channel));

        // The ack of the version that is both current and the last one transmitted completes it (once the transmission the
        // transport still holds has completed: the value's bytes are its payload).
        Assert.True(Inject(h.Client, A, 6));
        h.Client.Poll();
        h.Client.Flush();
        Assert.Equal(0, LatestKit.LiveKeys(h.Client, Channel));
        Assert.Equal(6u, LatestKit.AckedVersion(h.Client, Channel, A));
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(second.Token) != DeliveryStatus.Pending, 5_000_000));
        Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(second.Token));
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(first.Token) != DeliveryStatus.Pending, 5_000_000));
        Assert.Equal(DeliveryStatus.Superseded, h.Client.GetDeliveryStatus(first.Token));
    }

    [Fact]
    public void A_Large_Value_For_An_Idle_Key_Is_Accepted()
    {
        using ServerHarness h = Receiver(out List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received);
        Value(h, A, 1, 1);
        Value(h, B, 0x7000_0000, 2);
        Value(h, B, 0xE000_0000, 3);
        byte[] large = LatestKit.Payload(4, 1_500);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(LatestKit.GroupStream(Table, Channel, 0xE000_0001, A, large), out _, fin: true));
        h.Run(50_000);
        Assert.Equal(4, received.Count);
        Assert.Equal((A, 0xE000_0001u), (received[^1].Key, received[^1].Version));
        Assert.True(LatestKit.Matches(received[^1].Payload, 4, 1_500));
        Assert.Equal(0, DatagramKit.ChannelStats(h.Server!, Channel).Dropped);
        Assert.Equal(0xE000_0001u, LatestKit.AckEntries(h.Raw.Sink).Last(entry => entry.Key == A).Version);
    }

    [Fact]
    public void A_Stream_Value_Overtaken_By_A_Newer_Datagram_Is_Dropped_At_Its_End()
    {
        using ServerHarness h = Receiver(out List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received);
        byte[] stream = LatestKit.GroupStream(Table, Channel, 5, A, LatestKit.Payload(5, 1_500));

        // Version 5 starts on its stream (newer than anything the key has: accepted so far) and stalls before its end.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(stream.AsSpan(0, stream.Length - 200), out TransportStreamId id));
        h.Run(20_000);
        Assert.Empty(received);

        // Version 6 overtakes it as a datagram.
        Value(h, A, 6, 6);
        Assert.Single(received);
        long outstanding = h.Statistics().ReceiveBytesOutstanding;

        // The stream's end must not put version 5 back over version 6.
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.Raw.Transport, id, stream.AsSpan(stream.Length - 200), TransportSendFlags.Fin));
        h.Run(50_000);
        Assert.Single(received);
        Assert.Equal(6u, received[0].Version);
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server!, Channel).Dropped);
        Assert.Equal(6u, LatestKit.AckEntries(h.Raw.Sink).Last(entry => entry.Key == A).Version);

        // The staged value's buffer went back to the pool, once.
        Assert.True(h.Statistics().ReceiveBytesOutstanding < outstanding, "the dropped value's buffer was not returned");
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);

        // And the key goes on: a newer version is accepted, the dropped one's number is still stale.
        Value(h, A, 5, 5);
        Value(h, A, 7, 7);
        Assert.Equal(new uint[] { 6, 7 }, received.Select(r => r.Version).ToArray());
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void A_Retransmission_After_A_Long_Quiet_Is_Still_A_Duplicate()
    {
        // Unlike an UnreliableSequenced channel, the version clock never resynchronises on time: a retransmission arrives
        // behind the newest version after any length of quiet, legitimately.
        using ServerHarness h = Receiver(out List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received);
        Value(h, A, 10, 1);
        Value(h, B, 11, 2);
        h.Run(5_000_000, step: 100_000);
        int acks = LatestKit.AckEntries(h.Raw.Sink).Count(entry => entry.Key == A);
        Value(h, A, 10, 1);
        Value(h, A, 9, 3);
        h.Run(50_000);
        Assert.Equal(2, received.Count);
        Assert.Equal(2, DatagramKit.ChannelStats(h.Server!, Channel).Dropped);
        List<LatestAckEntry> entries = LatestKit.AckEntries(h.Raw.Sink).Where(entry => entry.Key == A).ToList();
        Assert.True(entries.Count > acks, "the duplicate was not re-acked");
        Assert.All(entries, entry => Assert.Equal(10u, entry.Version));
        Assert.Equal(0, h.Statistics().SequenceResyncs);
    }

    [Fact]
    public void A_New_Epoch_Forgets_The_Version_Clock()
    {
        using ServerHarness h = Receiver(out List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received);
        Value(h, A, 0x9000_0000, 1);
        Value(h, B, 0x9000_0001, 2);

        // The first epoch began when the session was admitted; a second one restarts the sender's counter.
        LatestKit.Engine(h.Server!).OnEpochReset(resumed: true);
        Value(h, A, 1, 3);
        Value(h, B, 2, 4);
        Value(h, A, 3, 5);
        Value(h, A, 1, 3);   // a duplicate within the new epoch
        Assert.Equal(new uint[] { 0x9000_0000, 0x9000_0001, 1, 2, 3 }, received.Select(r => r.Version).ToArray());
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server!, Channel).Dropped);
    }

    [Fact]
    public void A_Retired_Key_Reused_Accepts_Any_Version()
    {
        using ServerHarness h = Receiver(out List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received);
        Value(h, A, 50, 1);
        h.Raw.SendControl(Frames.KeyRetiredFrame(Channel, A));
        h.Run(20_000);
        Assert.Equal(2, received.Count);
        Assert.True((received[1].Flags & ReceiveFlags.KeyRetired) != 0);

        // The key's new holder starts over: a version behind the retired holder's last one is its first value.
        Value(h, A, 3, 2);
        Value(h, A, 2, 3);   // and behind that: stale
        Assert.Equal(3, received.Count);
        Assert.Equal(3u, received[2].Version);
        Assert.True(LatestKit.Matches(received[2].Payload, 2, 16));
        Assert.Equal(1, DatagramKit.ChannelStats(h.Server!, Channel).Dropped);
    }
}
