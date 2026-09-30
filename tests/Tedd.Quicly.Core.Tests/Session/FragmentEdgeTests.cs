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

        // SendPinned keeps the pointer (zero copy) and its fragments read it at the Flush inside RunUntil, long after the
        // fixed block below has ended; the contract is "valid and unchanged until BufferReleased". A heap array the GC may
        // move would hand the transport stale bytes, so the payload lives on the pinned object heap.
        byte[] pinned = GC.AllocateArray<byte>(Length, pinned: true);
        DatagramKit.Payload(2, Length).CopyTo(pinned, 0);
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
        // A send cap of 1 000 B/s (a 33-byte burst) that one 900-byte datagram overdraws: every datagram after it is held
        // back for most of a second.
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.MaxSendBytesPerSecond = 1_000;
        }, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        Assert.True(client.SendCopy(new SendHeader(7), new byte[900]).IsAdmitted);
        client.Flush();
        Assert.Equal(1, DatagramKit.ChannelStats(client, 7).Sent);

        SendResult result = client.SendCopy(
            new SendHeader(2),
            DatagramKit.Payload(1, 4_000),
            new SendOptions { Track = true, ExpiryMicros = 1_000 });
        Assert.Equal(SendStatus.Admitted, result.Status);

        // The first pass starts the expiry of all four fragments and the cap holds them back.
        client.Flush();
        Assert.Equal(DeliveryStatus.Pending, client.GetDeliveryStatus(result.Token));
        Assert.Equal(0, DatagramKit.ChannelStats(client, 2).Expired);

        // PROTOCOL.md §4.5: expiry is evaluated when the scheduler reaches the entry, so every fragment is dropped.
        h.Network.Advance(20_000);
        client.Flush();
        client.Poll();
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(result.Token));
        Assert.Equal(4, DatagramKit.ChannelStats(client, 2).Expired);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 2).Sent);
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
    public void A_Fragmented_Message_Held_Back_By_The_Send_Cap_Still_Arrives_Whole()
    {
        // PROTOCOL.md §4.5: the send cap is a token bucket whose burst is two flush intervals' worth — 4 000 bytes at
        // 120 kB/s — so a six-fragment message does not fit one pass. The fragments the pass cannot take stay queued in
        // order and go out from later passes, and the receiver reassembles a message whose fragments never shared a pass.
        using SessionHarness h = new(
            table: Table,
            client: o =>
            {
                DatagramKit.Quiet(o);
                o.MaxSendBytesPerSecond = 120_000;
            },
            server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        byte[] payload = DatagramKit.Payload(1, 6_000);

        SendResult result = client.SendCopy(new SendHeader(2), payload, SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        Assert.Equal(6, DatagramKit.ChannelStats(client, 2).QueuedMessages);

        client.Flush();
        long queued = DatagramKit.ChannelStats(client, 2).QueuedMessages;
        Assert.InRange(queued, 1, 5);
        Assert.True(DatagramKit.ChannelStats(client, 2).Sent > 0, "the pass handed over the fragments the cap allowed");

        Assert.True(h.RunUntil(() => got.Count == 1, 2_000_000), "the capped message never arrived");
        Assert.Equal(payload, got[0].Payload);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 2).QueuedMessages);
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered),
            $"the message ended as {client.GetDeliveryStatus(result.Token)}");
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
    }

    [Fact]
    public void A_Fragmented_Message_On_A_Carrier_Without_Send_States_Completes_Sent()
    {
        // PROTOCOL.md §4.3: a carrier that reports no per-datagram send state completes every datagram `Sent`, which is final.
        // The worst outcome of a message's fragments decides the message, and `Sent` outranks `Delivered`, so such a message
        // is never reported delivered however well it travelled — and it still arrives whole.
        using SessionHarness h = new(
            link: new LinkOptions { DatagramSendStateReporting = false },
            table: Table,
            client: DatagramKit.Quiet,
            server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        byte[] payload = DatagramKit.Payload(1, 4_000);

        SendResult result = client.SendCopy(new SendHeader(2), payload, SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(result.Token) == DeliveryStatus.Sent),
            $"the message ended as {client.GetDeliveryStatus(result.Token)}");
        Assert.Single(got);
        Assert.Equal(payload, got[0].Payload);
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
    }

    [Fact]
    public void A_Resumed_Session_Fragments_Again_After_The_Owner_Map_Was_Reset()
    {
        // PROTOCOL.md §4.1: a resume starts a new epoch. The lost connection's fragment bookkeeping is forgotten
        // (`OnReconnecting`) and the send counters restart, so the resumed session's fragments own their entries again and its
        // first message carries sequence 0.
        using SessionHarness h = new(connect: false, table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
            AdmissionResult.Accept(new byte[] { 5, 6, 7 }, 4242, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);
        Assert.True(h.RunUntilConnected(), "the session did not connect");
        QuiclyPeer client = h.Client;
        QuiclyPeer oldServer = h.Server!;

        SendResult lost = client.SendCopy(new SendHeader(2), DatagramKit.Payload(1, 4_000), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, lost.Status);
        oldServer.Core.Transport!.Close(0, default);
        Assert.True(h.RunUntil(() => client.State == PeerState.Closed), "the client did not see the loss");
        Assert.Equal(DeliveryStatus.Disconnected, client.GetDeliveryStatus(lost.Token));
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);

        client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.True(
            h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null
                && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected),
            "the session did not resume");
        oldServer.Dispose();
        Assert.Equal(2u, client.Epoch);

        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        byte[] payload = DatagramKit.Payload(2, 4_000);
        SendResult resumed = client.SendCopy(new SendHeader(2), payload, SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, resumed.Status);
        Assert.True(h.RunUntil(() => got.Count == 1), "the resumed session delivered no fragmented message");
        Assert.Equal(payload, got[0].Payload);
        Assert.Equal(0u, got[0].Header.Sequence);
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(resumed.Token) == DeliveryStatus.Delivered),
            $"the resumed message ended as {client.GetDeliveryStatus(resumed.Token)}");
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
        Assert.Equal(0, FragmentKit.Reassemblies(h.Server, 2));
    }

    [Fact]
    public void A_Fragmented_Message_Needs_Its_Owner_And_Every_Fragment_In_The_Send_Table()
    {
        // The reserve is checked for the worst case (an owner plus eight fragments), so a nearly full table refuses a
        // fragmented message while it would still take a single-datagram one.
        using SessionHarness h = new(
            table: Table,
            client: o =>
            {
                DatagramKit.Quiet(o);
                o.SendTableCapacity = 16;
            },
            server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        int admitted = 0;
        SendStatus last = SendStatus.Admitted;
        while (admitted < 8 && last == SendStatus.Admitted)
        {
            last = client.SendCopy(new SendHeader(2), DatagramKit.Payload(admitted, 3_000)).Status;
            admitted += last == SendStatus.Admitted ? 1 : 0;
        }

        Assert.Equal(SendStatus.QueueFull, last);
        Assert.InRange(admitted, 1, 3);
        Assert.Equal(1, DatagramKit.ChannelStats(client, 2).QueueFull);
        // A message that fits one datagram still gets in.
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(7), DatagramKit.Payload(9, 64)).Status);
    }

    [Fact]
    public void A_Fragmented_Message_Refused_For_Its_Token_Leaves_No_Owner_Marks()
    {
        // AdmitFragmented takes the owner entry and every fragment entry *before* the tracking token, so a refusal there gives
        // back entries it never filled. Each of them carries the empty owner marker from its allocation, because a slot that
        // kept a stale mark would have a later message's completion folded into a message that no longer exists
        // (docs/design/session-layer.md §7.8). The refusal is provoked without touching the send table: a tracked send whose
        // wait nobody consumes keeps its completion slot after its entry has gone back, so the completion table runs out while
        // the send table is empty.
        using SessionHarness h = new(
            table: Table,
            client: o =>
            {
                DatagramKit.Quiet(o);
                o.SendTableCapacity = 16;
            },
            server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));

        List<ValueTask<DeliveryStatus>> waits = [];
        for (int i = 0; i < 24; i++)
        {
            SendResult small = client.SendCopy(new SendHeader(7), DatagramKit.Payload(i, 64), SendOptions.Tracked);
            if (small.Status != SendStatus.Admitted)
            {
                break;
            }

            waits.Add(client.WaitAsync(small.Token, CompletionStage.RemoteAccepted));
            Assert.True(
                h.RunUntil(() => client.GetDeliveryStatus(small.Token) != DeliveryStatus.Pending),
                $"tracked send {i} did not complete");
        }

        Assert.True(waits.Count >= 8, $"only {waits.Count} tracked sends were admitted, so the completion table was never filled");
        SendResult refused = client.SendCopy(new SendHeader(2), DatagramKit.Payload(99, 4_000), SendOptions.Tracked);
        Assert.Equal(SendStatus.QueueFull, refused.Status);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 2).QueuedMessages);
        Assert.Equal(0, DatagramKit.Statistics(client).FragmentedMessagesSent);

        // The very slots that refusal gave back now carry another fragmented message, which must arrive whole and complete.
        byte[] payload = DatagramKit.Payload(98, 4_000);
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(2), payload).Status);
        Assert.True(h.RunUntil(() => got.Count == 1), "the message that reused the refused entries did not arrive");
        Assert.Equal(payload, got[0].Payload);
        Assert.Equal(1, DatagramKit.Statistics(client).FragmentedMessagesSent);
        Assert.Equal(1, DatagramKit.ChannelStats(client, 2).QueueFull);
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
    }

    [Fact]
    public void A_Fragmented_Message_Needs_A_Payload_Buffer()
    {
        using SessionHarness h = new(
            table: Table,
            client: o =>
            {
                DatagramKit.Quiet(o);
                o.SendBudgetBytes = 2_048;
            },
            server: DatagramKit.Quiet);
        Assert.Equal(SendStatus.OutOfBuffers, h.Client.SendCopy(new SendHeader(2), DatagramKit.Payload(1, 3_000)).Status);
        Assert.Equal(0, DatagramKit.Statistics(h.Client).FragmentedMessagesSent);
    }

    [Fact]
    public void An_Immediate_Fragmented_Message_Goes_Out_Without_A_Flush()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        byte[] payload = DatagramKit.Payload(1, 3_000);

        // PROTOCOL.md §4.5: an Immediate send runs a scheduler pass before it returns, fragments included.
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(2), payload, SendOptions.Immediate).Status);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 2).QueuedMessages);
        h.Network.Advance(50_000);
        h.Server.Poll();
        Assert.Single(got);
        Assert.Equal(payload, got[0].Payload);
    }

    [Fact]
    public void A_Fragmented_Message_The_Transport_Refuses_Fails_And_Is_Not_Counted_As_Sent()
    {
        FlagRecordingConnector? recorder = null;
        using SessionHarness h = new(
            table: Table,
            client: DatagramKit.Quiet,
            server: DatagramKit.Quiet,
            connector: inner => recorder = new FlagRecordingConnector(inner));
        QuiclyPeer client = h.Client;
        SendResult result = client.SendCopy(new SendHeader(2), DatagramKit.Payload(1, 3_000), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);

        recorder!.Transport!.RefuseDatagrams = TransportStatus.OutOfMemory;
        client.Flush();
        client.Poll();

        Assert.Equal(DeliveryStatus.Failed, client.GetDeliveryStatus(result.Token));
        // The packer had counted the fragments; a refused submission takes that back.
        Assert.Equal(0, DatagramKit.ChannelStats(client, 2).Sent);
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
    }

    [Fact]
    public void Cancelling_A_Fragmented_Message_Leaves_Its_Neighbours_In_The_Queue()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        byte[] before = DatagramKit.Payload(1, 64);
        byte[] after = DatagramKit.Payload(2, 64);

        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(2), before).Status);
        SendResult big = client.SendCopy(new SendHeader(2), DatagramKit.Payload(3, 4_000), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, big.Status);
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(2), after).Status);

        // The fragments are unlinked from the middle of the channel's FIFO; the plain messages keep their order.
        Assert.True(client.TryCancel(big.Token));
        client.Poll();
        Assert.Equal(DeliveryStatus.Canceled, client.GetDeliveryStatus(big.Token));
        Assert.Equal(2, DatagramKit.ChannelStats(client, 2).QueuedMessages);
        Assert.True(h.RunUntil(() => got.Count == 2), $"{got.Count} of 2 messages arrived");
        Assert.Equal(before, got[0].Payload);
        Assert.Equal(after, got[1].Payload);
    }

    [Fact]
    public void A_Fragment_Of_An_Older_Message_Does_Not_Disturb_The_Partial_Of_A_Newer_One()
    {
        // Channel 3 is UnreliableSequenced, so its sequence orders the channel's messages and the §7 rules about older and
        // newer sequences of one key apply: the older message is obsolete by definition. On an unordered channel the sequence
        // is only a reassembly id (PROTOCOL.md §2.1), and both messages are reassembled instead — that is
        // ReviewFragmentRequestTests.Two_Fragmented_Messages_Whose_Fragments_Interleave_Are_Both_Reassembled.
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        ChannelDefinition channel = Table[3]!;
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.Split(channel, 5, 7, DatagramKit.Payload(1, 2_000), 2)[0]));
        h.Run(10_000);
        Assert.Equal(1, FragmentKit.Reassemblies(server, 3));

        // A fragment of a message the key has already moved past is dropped without touching the live partial.
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.Split(channel, 4, 7, DatagramKit.Payload(2, 2_000), 2)[0]));
        h.Run(10_000);
        Assert.Equal(1, FragmentKit.Reassemblies(server, 3));
        Assert.Equal(1, h.Statistics().FragmentsDropped);
        Assert.Equal(0, h.Statistics().ReassembliesAbandoned);
    }

    [Fact]
    public void A_Partial_That_Finds_No_Receive_Buffer_Is_Dropped_And_Counted()
    {
        // The bound fits the budget, but another partial already holds most of it.
        using ServerHarness h = new(table: Table, server: o => o.ReceiveBudgetBytes = 4_096);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        // 1 000-byte fragments, so each datagram fits the link; the first partial's buffer takes the whole 4 KiB budget.
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.Split(Table[3]!, 1, 1, DatagramKit.Payload(1, 2_000), 2)[0]));
        h.Run(10_000);
        Assert.Equal(1, FragmentKit.Reassemblies(server, 3));

        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.Split(Table[2]!, 1, 0, DatagramKit.Payload(2, 2_000), 2)[0]));
        h.Run(10_000);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 2));
        Assert.Equal(1, DatagramKit.ChannelStats(server, 2).OutOfBuffers);
        Assert.True(h.Statistics().OutOfReceiveBuffers > 0, "the peer counted the exhausted budget");
        Assert.Equal(PeerState.Connected, server.State);
    }

    [Fact]
    public void A_Reassembled_Message_That_Is_Stale_Is_Dropped_Like_Any_Other()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(3, Handlers.Collect(got));
        ChannelDefinition channel = Table[3]!;

        foreach (byte[] fragment in FragmentKit.Split(channel, 10, 4, DatagramKit.Payload(1, 2_000), 2))
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(fragment));
        }

        Assert.True(h.RunUntil(() => got.Count == 1), "the first message did not arrive");

        // A whole message of an older sequence reassembles and is then dropped by the mode's own acceptance.
        foreach (byte[] fragment in FragmentKit.Split(channel, 5, 4, DatagramKit.Payload(2, 2_000), 2))
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(fragment));
        }

        h.Run(20_000);
        Assert.Single(got);
        Assert.Equal(1, DatagramKit.ChannelStats(server, 3).Dropped);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 3));
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Reassembled_Message_Is_Dropped_When_The_Receive_Ring_Is_Full()
    {
        using ServerHarness h = new(table: Table, server: o => o.ReceiveRingCapacity = 2);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        ChannelDefinition channel = Table[2]!;

        // Nothing is polled, so the ring fills with whole messages and the reassembled one finds no slot.
        for (int i = 0; i < 2; i++)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(DatagramKit.Frame(Table, 2, (uint)i, 0, [(byte)i])));
            h.Network.Advance(5_000);
        }

        foreach (byte[] fragment in FragmentKit.Split(channel, 7, 0, DatagramKit.Payload(3, 2_000), 2))
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(fragment));
            h.Network.Advance(5_000);
        }

        Assert.True(DatagramKit.ChannelStats(server, 2).RingDrops > 0, "the reassembled message was not counted as a ring drop");
        Assert.Equal(0, FragmentKit.Reassemblies(server, 2));
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

    [Fact]
    public void A_Quiet_Engine_Keeps_The_Closed_Epochs_Partial_Until_It_Hears_A_Datagram()
    {
        // Decided and documented (docs/design/session-layer.md §7.8, "Epoch reset"): the reset is a request the transport
        // thread consumes inside the engine's next datagram, because the reassembly table is that thread's (ADR 0008
        // invariant 4). Until then the old epoch's partial keeps its buffer — bounded by MaxReassemblies × MaxMessageSize and
        // already counted in the receive budget — and nothing of the new epoch can mix into it. Any datagram of the engine
        // consumes the request, not only a fragment of the channel that holds the partial.
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(7, Handlers.Collect(got));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.Split(Table[2]!, 1, 0, DatagramKit.Payload(1, 2_000), 2)[0]));
        h.Run(10_000);
        long before = DatagramKit.Statistics(server).ReceiveBytesOutstanding;
        Assert.True(before > 0, "the partial holds a receive lease");

        FragmentKit.Engine(server, ChannelMode.UnreliableUnordered).OnEpochReset(resumed: true);
        h.Run(500_000);
        Assert.Equal(1, FragmentKit.Reassemblies(server, 2));
        Assert.Equal(before, DatagramKit.Statistics(server).ReceiveBytesOutstanding);

        // A plain datagram of another channel of the same engine (7 does not fragment) is enough.
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(DatagramKit.Frame(Table, 7, 0, 0, [42])));
        Assert.True(h.RunUntil(() => got.Count == 1), "the plain datagram did not arrive");
        Assert.Equal(0, FragmentKit.Reassemblies(server, 2));
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
        Assert.Equal(0, h.Statistics().ReassembliesAbandoned);
    }

    [Fact]
    public void A_Fragment_Whose_Count_Implies_More_Than_The_Limit_Rents_Only_The_Limit()
    {
        // PROTOCOL.md §8: a non-last fragment of size s bounds its message from below, by s × (FragCount − 1) + 1, so a fragment
        // can pass that bound while s × FragCount is far above the limit. This peer can buffer 4 096 bytes, and 1 195-byte
        // fragments of a 4-fragment message imply at least 3 586 bytes (legal) but at most 4 780 (not). Renting 4 780 bytes for
        // the partial fails on a budget that has room for every legal message: it would drop a message that fits, and count
        // OutOfReceiveBuffers and the channel's OutOfBuffers for pressure that does not exist. The rent is clamped to the
        // limit, and the exact total is checked once the last fragment tells it.
        using ServerHarness h = new(table: Table, server: o => o.ReceiveBudgetBytes = 4_096);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(2, Handlers.Collect(got));
        byte[] message = DatagramKit.Payload(1, (3 * 1_195) + 500);

        byte[] Fragment(uint sequence, int index, int length) => FragmentKit.RawFragment(
            2, 2, sequence, null, fragCount: 4, fragIndex: (byte)index, rawLength: null, message.AsSpan(index * 1_195, length));

        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(Fragment(1, 0, 1_195)));
        h.Run(10_000);
        PeerStatistics statistics = h.Statistics();
        Assert.Equal(1, FragmentKit.Reassemblies(server, 2));
        Assert.Equal(0, statistics.OutOfReceiveBuffers);
        Assert.Equal(0, DatagramKit.ChannelStats(server, 2).OutOfBuffers);
        Assert.Equal(0, statistics.FragmentsDropped);
        Assert.InRange(statistics.ReceiveBytesOutstanding, 1, 4_096);

        // A 4 085-byte message: it fits the limit, so it is delivered whole.
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(Fragment(1, 1, 1_195)));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(Fragment(1, 3, 500)));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(Fragment(1, 2, 1_195)));
        Assert.True(h.RunUntil(() => got.Count == 1), "the message that fits the limit was not delivered");
        Assert.Equal(message, got[0].Payload);

        // The same shape with a last fragment that takes the total past the limit is dropped with its partial.
        byte[] oversized = DatagramKit.Payload(2, 1_000);
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(Fragment(2, 0, 1_195)));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(FragmentKit.RawFragment(2, 2, 2, null, 4, 3, null, oversized)));
        h.Run(10_000);
        Assert.Single(got);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 2));
        statistics = h.Statistics();
        Assert.Equal(1, statistics.FragmentsDropped);
        Assert.Equal(0, statistics.OutOfReceiveBuffers);
        Assert.Equal(0, DatagramKit.ChannelStats(server, 2).OutOfBuffers);
        Assert.Equal(0, statistics.ReceiveBytesOutstanding);
        Assert.Equal(PeerState.Connected, server.State);
    }

    [Fact]
    public void A_Fragment_Packed_Beside_A_Tracked_Message_Gives_Its_Payload_Back_Exactly_Once()
    {
        // docs/design/session-layer.md §7.8 "Completions": a fragment is never tracked itself, but a container forwards its
        // Sent notice to every member as soon as any member is tracked. A fragment that shares a container with a tracked
        // message therefore hears twice — the container's Sent notice and its own final completion — and must give its
        // reference on the message's payload back exactly once. A shared block makes that visible: its reference count must
        // come back to exactly the caller's one, not zero (released twice) and not two (never released).
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
        // Two fragments of ~600 bytes: never two in one datagram, but either one leaves room for a small message beside it.
        int length = FragmentKit.LengthFor(Table[2]!, 2);
        SharedLease shared = pool.Share(length, seed: 5);
        byte[] expected = new byte[length];
        for (int i = 0; i < length; i++)
        {
            expected[i] = (byte)(5 + i);
        }

        SendResult fragmented = client.SendShared(new SendHeader(2), pool.Table, in shared, length, SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, fragmented.Status);
        SendResult small = client.SendCopy(new SendHeader(2), [1, 2, 3], SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, small.Status);
        Assert.Equal(2, pool.Count(in shared));
        ValueTask<DeliveryStatus> released = client.WaitAsync(fragmented.Token, CompletionStage.BufferReleased);

        PeerStatistics before = DatagramKit.Statistics(client);
        client.Flush();
        PeerStatistics sent = DatagramKit.Statistics(client);
        // The first fragment goes alone; the second one and the tracked message share one container.
        Assert.Equal(1, sent.ContainersSent - before.ContainersSent);
        Assert.Equal(2, sent.MessagesPacked - before.MessagesPacked);

        Assert.True(h.RunUntil(() => got.Count == 2), $"{got.Count} of 2 messages arrived");
        Assert.Equal(expected, got[0].Payload);
        Assert.Equal(new byte[] { 1, 2, 3 }, got[1].Payload);
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(fragmented.Token) != DeliveryStatus.Pending), "the message never completed");
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(fragmented.Token));
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(small.Token));
        Assert.True(released.IsCompleted, "BufferReleased never completed");
        _ = released.Result;
        Assert.Equal(1, pool.Count(in shared));
        pool.Table.Release(in shared);
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
    }
}
