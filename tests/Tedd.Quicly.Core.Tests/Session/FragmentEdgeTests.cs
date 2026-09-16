using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The limits and failure paths of fragmentation (PROTOCOL.md §2.1, §7; docs/design/session-layer.md §7.8): hostile
/// fragment fields, the reassembly cap and its expiry, the fragmented-size bound, every send path, and what a fragmented
/// message does when it is canceled, expires or meets a closing session.
/// </summary>
public unsafe class FragmentEdgeTests
{
    private static readonly ChannelTable Table = FragmentTables.Main;

    [Fact]
    public void Hostile_Fragment_Fields_Are_Dropped_Without_Closing_The_Connection()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(2, Handlers.Collect(got));
        server.RegisterHandler(6, Handlers.Collect(got));
        byte[] body = DatagramKit.Payload(1, 600);

        // Rejected by the framing layer before the session sees them (PROTOCOL.md §2.1).
        byte[][] malformed =
        [
            FragmentKit.RawFragment(2, 2, 1, null, fragCount: 0, fragIndex: 0, rawLength: null, body),
            FragmentKit.RawFragment(2, 2, 2, null, fragCount: 9, fragIndex: 0, rawLength: null, body),
            FragmentKit.RawFragment(2, 2, 3, null, fragCount: 3, fragIndex: 3, rawLength: null, body),
            FragmentKit.RawFragment(2, 2, 4, null, fragCount: 3, fragIndex: 9, rawLength: null, body),
            FragmentKit.RawFragment(2, 2, 5, null, fragCount: 2, fragIndex: 0, rawLength: null, default),
            // A non-last fragment of 1 195 bytes with FragCount 8 implies at least 8 366 bytes on a 2 400-byte channel.
            FragmentKit.RawFragment(6, 2, 6, null, fragCount: 8, fragIndex: 0, rawLength: null, DatagramKit.Payload(2, 1_195)),
        ];
        foreach (byte[] frame in malformed)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(frame));
        }

        h.Run(20_000);
        Assert.Empty(got);
        PeerStatistics statistics = h.Statistics();
        Assert.Equal(malformed.Length, statistics.MalformedDatagrams);
        Assert.Equal(0, statistics.FragmentsReceived);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 2));
        Assert.Equal(0, FragmentKit.Reassemblies(server, 6));
        Assert.Equal(PeerState.Connected, server.State);
    }

    [Fact]
    public void Fragments_That_Disagree_About_Their_Message_Are_Dropped_With_Their_Partial()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(2, Handlers.Collect(got));
        byte[] first = DatagramKit.Payload(1, 500);

        // FragCount must be the same in every fragment of one (channel, key, sequence).
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.RawFragment(2, 2, 1, null, 3, 0, null, first)));
        h.Run(10_000);
        Assert.Equal(1, FragmentKit.Reassemblies(server, 2));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.RawFragment(2, 2, 1, null, 4, 1, null, first)));
        h.Run(10_000);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 2));
        Assert.Equal(1, h.Statistics().FragmentsDropped);

        // The last fragment is never larger than fragment 0 (PROTOCOL.md §8).
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.RawFragment(2, 2, 2, null, 3, 0, null, first)));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.RawFragment(2, 2, 2, null, 3, 2, null, DatagramKit.Payload(2, 900))));
        h.Run(10_000);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 2));
        Assert.Equal(2, h.Statistics().FragmentsDropped);

        // A tail that turns out to be larger than fragment 0 once fragment 0 arrives is caught the same way.
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.RawFragment(2, 2, 3, null, 3, 2, null, DatagramKit.Payload(3, 900))));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.RawFragment(2, 2, 3, null, 3, 0, null, first)));
        h.Run(10_000);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 2));
        Assert.Equal(3, h.Statistics().FragmentsDropped);
        Assert.Empty(got);
        Assert.Equal(PeerState.Connected, server.State);
    }

    [Fact]
    public void A_Fragment_Larger_Than_The_Receive_Budget_Could_Ever_Hold_Is_Dropped()
    {
        // The channel allows 8 800 bytes, but this peer could never buffer them: the engine bounds the message from the
        // first fragment, before it chooses a buffer (ADR 0009).
        using ServerHarness h = new(table: Table, server: o => o.ReceiveBudgetBytes = 4_096);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.RawFragment(2, 2, 1, null, 8, 0, null, DatagramKit.Payload(1, 1_195))));
        h.Run(20_000);

        Assert.Equal(0, FragmentKit.Reassemblies(server, 2));
        Assert.Equal(1, h.Statistics().FragmentsDropped);
        Assert.Equal(1, DatagramKit.ChannelStats(server, 2).ReceiveTooLarge);
        Assert.Equal(PeerState.Connected, server.State);
    }

    [Fact]
    public void The_Reassembly_Cap_Evicts_The_Oldest_Partial()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(5, Handlers.Collect(got));
        ChannelDefinition channel = Table[5]!;
        List<byte[]>[] messages =
        [
            FragmentKit.Split(channel, 1, 1, DatagramKit.Payload(1, 2_000), 2),
            FragmentKit.Split(channel, 1, 2, DatagramKit.Payload(2, 2_000), 2),
            FragmentKit.Split(channel, 1, 3, DatagramKit.Payload(3, 2_000), 2),
        ];

        // MaxReassemblies is 2 on this channel, so the third key evicts the oldest partial (PROTOCOL.md §7).
        for (int i = 0; i < messages.Length; i++)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(messages[i][0]));
            h.Run(5_000);
        }

        Assert.Equal(2, FragmentKit.Reassemblies(server, 5));
        Assert.Equal(1, h.Statistics().ReassembliesAbandoned);

        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(messages[1][1]));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(messages[2][1]));
        Assert.True(h.RunUntil(() => got.Count == 2), $"{got.Count} of 2 messages arrived");
        Assert.Equal(2UL, got[0].Header.Key);
        Assert.Equal(3UL, got[1].Header.Key);

        // The evicted message cannot complete: its tail starts a partial of its own.
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(messages[0][1]));
        h.Run(20_000);
        Assert.Equal(2, got.Count);
        Assert.Equal(PeerState.Connected, server.State);
    }

    [Fact]
    public void A_Partial_That_Stops_Arriving_Expires()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(2, Handlers.Collect(got));
        ChannelDefinition channel = Table[2]!;
        // No RTT sample on this connection, so the window is the 100 ms grace of PROTOCOL.md §7.
        Assert.Equal(100_000, FragmentKit.Window(server));

        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.Split(channel, 1, 0, DatagramKit.Payload(1, 2_000), 2)[0]));
        h.Run(10_000);
        Assert.Equal(1, FragmentKit.Reassemblies(server, 2));

        h.Run(200_000);
        Assert.Equal(0, h.Statistics().ReassembliesExpired);

        // The sweep runs when the channel next hears a fragment (the transport thread owns the table).
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.Split(channel, 2, 0, DatagramKit.Payload(2, 2_000), 2)[0]));
        h.Run(10_000);
        Assert.Equal(1, h.Statistics().ReassembliesExpired);
        Assert.Equal(0, h.Statistics().ReassembliesAbandoned);
        Assert.Equal(1, FragmentKit.Reassemblies(server, 2));
        Assert.Empty(got);
    }

    [Fact]
    public void A_Message_That_Needs_More_Than_Eight_Fragments_Is_Refused()
    {
        // PROTOCOL.md §7 "fragmented message size": with a 300-byte path a fragment carries 295 bytes, so 8 fragments
        // cover 2 360 bytes and nothing larger can be sent.
        using SessionHarness h = new(
            link: new LinkOptions { MaxDatagramPayload = 300 },
            table: Table,
            client: DatagramKit.Quiet,
            server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));

        Assert.Equal(SendStatus.TooLarge, client.SendCopy(new SendHeader(2), DatagramKit.Payload(1, 3_000)).Status);
        Assert.Equal(1, DatagramKit.ChannelStats(client, 2).TooLarge);
        Assert.Equal(0, DatagramKit.Statistics(client).FragmentedMessagesSent);

        byte[] fits = DatagramKit.Payload(2, 2_000);
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(2), fits).Status);
        Assert.True(h.RunUntil(() => got.Count == 1), "the 2 000-byte message did not arrive");
        Assert.Equal(fits, got[0].Payload);
        Assert.Equal(7, DatagramKit.Statistics(client).FragmentsSent);
    }

    [Fact]
    public void Every_Send_Path_Can_Fragment()
    {
        // A shared block is resolved through the peer's own allocator (ARCHITECTURE.md §4.1), so the host's pool is the
        // peer's pool: that is what `SendShared` means for a server that serialises once and sends to many peers.
        using SharedPool pool = new();
        using SessionHarness h = new(
            table: Table,
            client: o =>
            {
                DatagramKit.Quiet(o);
                o.Allocator = pool.Allocator;
            },
            server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        const int Length = 3_000;
        byte[] copy = DatagramKit.Payload(1, Length);
        byte[] pinned = DatagramKit.Payload(2, Length);
        byte[] borrowed = DatagramKit.Payload(3, Length);

        BufferLease owned = client.RentBuffer(Length);
        DatagramKit.Payload(4, Length).CopyTo(client.GetBufferSpan(in owned));
        BufferLease left = client.RentBuffer(Length / 2);
        BufferLease right = client.RentBuffer(Length / 2);
        byte[] gathered = DatagramKit.Payload(5, Length);
        gathered.AsSpan(0, left.Length).CopyTo(client.GetBufferSpan(in left));
        gathered.AsSpan(left.Length, gathered.Length - left.Length).CopyTo(client.GetBufferSpan(in right).Slice(0, gathered.Length - left.Length));
        SharedLease shared = pool.Share(Length, seed: 9);
        byte[] expectedShared = new byte[Length];
        for (int i = 0; i < Length; i++)
        {
            expectedShared[i] = (byte)(9 + i);
        }

        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(2), copy).Status);
        fixed (byte* p = pinned)
        {
            Assert.Equal(SendStatus.Admitted, client.SendPinned(new SendHeader(2), p, Length).Status);
        }

        Assert.Equal(SendStatus.Admitted, client.SendBorrowed(new SendHeader(2), borrowed).Status);
        Assert.Equal(SendStatus.Admitted, client.SendOwned(new SendHeader(2), owned, Length).Status);
        Assert.Equal(SendStatus.Admitted, client.SendGather(new SendHeader(2), [left, right]).Status);
        Assert.Equal(1, pool.Count(in shared));
        Assert.Equal(SendStatus.Admitted, client.SendShared(new SendHeader(2), pool.Table, in shared, Length).Status);
        Assert.Equal(2, pool.Count(in shared));

        Assert.True(h.RunUntil(() => got.Count == 6), $"{got.Count} of 6 messages arrived");
        Assert.Equal(copy, got[0].Payload);
        Assert.Equal(pinned, got[1].Payload);
        Assert.Equal(borrowed, got[2].Payload);
        Assert.Equal(DatagramKit.Payload(4, Length), got[3].Payload);
        // A gather sends each page's whole block, so the message is as long as the two leases and starts with the pattern.
        Assert.Equal(left.Length + right.Length, got[4].Payload.Length);
        Assert.Equal(gathered, got[4].Payload.AsSpan(0, gathered.Length).ToArray());
        Assert.Equal(expectedShared, got[5].Payload);
        // The shared block's reference is taken once by the message and given back exactly once (session-layer.md §4.1).
        Assert.Equal(1, pool.Count(in shared));
        pool.Table.Release(in shared);
        Assert.Equal(6, DatagramKit.Statistics(client).FragmentedMessagesSent);
    }

    [Fact]
    public void A_Tracked_Fragmented_Message_Completes_Delivered()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        byte[] payload = DatagramKit.Payload(1, 4_000);

        SendResult result = client.SendCopy(new SendHeader(2), payload, SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered),
            $"the message ended as {client.GetDeliveryStatus(result.Token)}");
        Assert.Single(got);
        Assert.Equal(payload, got[0].Payload);
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
    }

    [Fact]
    public void A_Fragmented_Message_Whose_Fragment_Is_Lost_Completes_Lost()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        byte[] payload = DatagramKit.Payload(1, 4_000);

        DatagramKit.TransportOf(client).DropNextDatagrams(1);
        SendResult result = client.SendCopy(new SendHeader(2), payload, SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        // The worst of the fragments decides the message: one lost datagram makes the whole message Lost.
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(result.Token) == DeliveryStatus.Lost),
            $"the message ended as {client.GetDeliveryStatus(result.Token)}");
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
    }

    [Fact]
    public void A_Queued_Fragmented_Message_Can_Be_Canceled()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        SendResult result = client.SendCopy(new SendHeader(2), DatagramKit.Payload(1, 4_000), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        Assert.Equal(4, DatagramKit.ChannelStats(client, 2).QueuedMessages);

        Assert.True(client.TryCancel(result.Token));
        client.Poll();
        Assert.Equal(DeliveryStatus.Canceled, client.GetDeliveryStatus(result.Token));
        Assert.Equal(0, DatagramKit.ChannelStats(client, 2).QueuedMessages);
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);

        // A message the packer has already taken cannot be recalled.
        SendResult sent = client.SendCopy(new SendHeader(2), DatagramKit.Payload(2, 4_000), SendOptions.Tracked);
        client.Flush();
        Assert.False(client.TryCancel(sent.Token));
    }

    [Fact]
    public void A_Fragmented_Message_Expires_At_Scheduling_Time()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        SendResult result = client.SendCopy(
            new SendHeader(2),
            DatagramKit.Payload(1, 4_000),
            new SendOptions { Track = true, ExpiryMicros = 1_000 });
        Assert.Equal(SendStatus.Admitted, result.Status);

        // PROTOCOL.md §4.5: expiry is evaluated when the scheduler reaches the entry, so every fragment is dropped.
        h.Network.Advance(20_000);
        client.Flush();
        client.Poll();
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(result.Token));
        Assert.Equal(4, DatagramKit.ChannelStats(client, 2).Expired);
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
    }

    [Fact]
    public void A_Queued_Fragmented_Message_Completes_Disconnected_When_The_Session_Closes()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        SendResult result = client.SendCopy(new SendHeader(2), DatagramKit.Payload(1, 4_000), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);

        client.Close(CloseReason.Normal);
        Assert.True(h.RunUntilClosed(), "the session did not close");
        Assert.Equal(DeliveryStatus.Disconnected, client.GetDeliveryStatus(result.Token));
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
    }

    [Fact]
    public void A_New_Epoch_Gives_Up_The_Partials_Of_The_Closed_One()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        ChannelDefinition channel = Table[2]!;
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.Split(channel, 1, 0, DatagramKit.Payload(1, 2_000), 2)[0]));
        h.Run(10_000);
        Assert.Equal(1, FragmentKit.Reassemblies(server, 2));
        long held = DatagramKit.Statistics(server).ReceiveBytesOutstanding;
        Assert.True(held > 0, "the partial holds a receive lease");

        // The engine's own reset (a second epoch, PROTOCOL.md §4.1) gives the partial's buffer back.
        FragmentKit.Engine(server, ChannelMode.UnreliableUnordered).OnEpochReset(resumed: true);
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.Split(channel, 9, 0, DatagramKit.Payload(2, 2_000), 2)[0]));
        h.Run(10_000);
        Assert.Equal(1, FragmentKit.Reassemblies(server, 2));
        Assert.Equal(held, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }
}
