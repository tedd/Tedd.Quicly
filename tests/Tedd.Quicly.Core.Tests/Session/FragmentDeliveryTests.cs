using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Fragmentation of the unreliable modes end to end over the simulator (PROTOCOL.md §2.1, §7;
/// docs/design/session-layer.md §7.8): every fragment count, loss, reordering, duplicates, the abandon and cap rules, and
/// compression on top of fragmentation.
/// </summary>
public class FragmentDeliveryTests
{
    private static readonly ChannelTable Table = FragmentTables.Main;

    private static int LengthFor(ChannelDefinition channel, int count) => FragmentKit.LengthFor(channel, count);

    [Fact]
    public void Messages_Of_Every_Fragment_Count_Round_Trip_Byte_Exact()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(2, Handlers.Collect(got));
        ChannelDefinition channel = Table[2]!;

        List<byte[]> sent = [];
        for (int count = 2; count <= 8; count++)
        {
            byte[] payload = DatagramKit.Payload(count, LengthFor(channel, count));
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(2), payload).Status);
            sent.Add(payload);
            Assert.True(h.RunUntil(() => got.Count == sent.Count), $"{got.Count} of {sent.Count} messages arrived (count {count}).");
            Assert.Equal(count, FragmentKit.Layout(channel, 1200, payload.Length).Count);
        }

        for (int i = 0; i < sent.Count; i++)
        {
            Assert.Equal(sent[i], got[i].Payload);
            Assert.True(got[i].Header.Flags.HasFlag(ReceiveFlags.Fragmented), "the message is reported as fragmented");
            Assert.Equal(0, got[i].Header.RawLength);
        }

        PeerStatistics cs = DatagramKit.Statistics(client);
        PeerStatistics ss = DatagramKit.Statistics(server);
        Assert.Equal(7, cs.FragmentedMessagesSent);
        Assert.Equal(2 + 3 + 4 + 5 + 6 + 7 + 8, cs.FragmentsSent);
        Assert.Equal(7, ss.FragmentedMessagesReceived);
        Assert.Equal(cs.FragmentsSent, ss.FragmentsReceived);
        Assert.Equal(0, ss.FragmentsDropped);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 2));
        // Every fragment is a datagram of its own, so the channel counts one Sent per fragment.
        Assert.Equal(cs.FragmentsSent, DatagramKit.ChannelStats(client, 2).Sent);
        Assert.Equal(7, DatagramKit.ChannelStats(server, 2).Received);
    }

    [Fact]
    public void A_Lost_Fragment_Drops_Only_Its_Own_Message()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(3, Handlers.Collect(got));
        ChannelDefinition channel = Table[3]!;
        byte[] lost = DatagramKit.Payload(1, LengthFor(channel, 3));
        byte[] kept = DatagramKit.Payload(2, LengthFor(channel, 3));

        // The first fragment of the first message never leaves the link; the second message is untouched.
        DatagramKit.TransportOf(client).DropNextDatagrams(1);
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(3, 7), lost).Status);
        client.Flush();
        h.Run(50_000);
        Assert.Empty(got);
        Assert.Equal(1, FragmentKit.Reassemblies(server, 3));

        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(3, 7), kept).Status);
        Assert.True(h.RunUntil(() => got.Count == 1), "the second message did not arrive");
        Assert.Equal(kept, got[0].Payload);
        // PROTOCOL.md §7: the newer sequence of the same key abandons the incomplete partial.
        Assert.Equal(1, DatagramKit.Statistics(server).ReassembliesAbandoned);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 3));
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void Fragments_Arriving_Out_Of_Order_Or_Duplicated_Are_Reassembled_Once()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(2, Handlers.Collect(got));
        ChannelDefinition channel = Table[2]!;
        byte[] message = DatagramKit.Payload(9, 3_000);
        List<byte[]> fragments = FragmentKit.Split(channel, sequence: 4, key: 0, message, count: 4);

        // Reverse order, with two duplicates in the middle.
        foreach (int index in new[] { 3, 1, 1, 2, 3, 0 })
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(fragments[index]));
        }

        Assert.True(h.RunUntil(() => got.Count == 1), "the reassembled message did not arrive");
        Assert.Equal(message, got[0].Payload);
        Assert.Equal(4u, got[0].Header.Sequence);
        Assert.True(got[0].Header.Flags.HasFlag(ReceiveFlags.Fragmented));
        PeerStatistics statistics = h.Statistics();
        Assert.Equal(6, statistics.FragmentsReceived);
        Assert.Equal(2, statistics.FragmentsDropped);
        Assert.Equal(1, statistics.FragmentedMessagesReceived);
        Assert.Equal(0, statistics.ReassembliesAbandoned);

        // A duplicate of a message that is already delivered starts a fresh partial and is not delivered twice.
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(fragments[0]));
        h.Run(20_000);
        Assert.Single(got);
        Assert.Equal(PeerState.Connected, server.State);
    }

    [Fact]
    public void The_Last_Fragment_Arriving_First_Is_Reassembled()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        ChannelDefinition channel = Table[2]!;
        // A last fragment that is shorter than fragment 0, so its bytes have to move once the size is known.
        byte[] message = DatagramKit.Payload(11, 2_500);
        List<byte[]> fragments = FragmentKit.Split(channel, sequence: 1, key: 0, message, count: 3);
        Assert.True(fragments[2].Length < fragments[0].Length, "the last fragment is the short one");

        foreach (int index in new[] { 2, 1, 0 })
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(fragments[index]));
        }

        Assert.True(h.RunUntil(() => got.Count == 1), "the reassembled message did not arrive");
        Assert.Equal(message, got[0].Payload);
    }

    [Fact]
    public void Partials_Of_Different_Keys_Are_Independent()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(3, Handlers.Collect(got));
        ChannelDefinition channel = Table[3]!;
        byte[] first = DatagramKit.Payload(1, 2_400);
        byte[] second = DatagramKit.Payload(2, 2_400);
        List<byte[]> a = FragmentKit.Split(channel, sequence: 1, key: 10, first, count: 3);
        List<byte[]> b = FragmentKit.Split(channel, sequence: 2, key: 20, second, count: 3);

        // Interleaved: both partials live at the same time, and each key completes on its own.
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(a[0]));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(b[2]));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(a[2]));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(b[0]));
        h.Run(20_000);
        Assert.Empty(got);
        Assert.Equal(2, FragmentKit.Reassemblies(server, 3));

        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(b[1]));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(a[1]));
        Assert.True(h.RunUntil(() => got.Count == 2), $"{got.Count} of 2 messages arrived");
        Assert.Equal(second, got[0].Payload);
        Assert.Equal(first, got[1].Payload);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 3));
        Assert.Equal(0, h.Statistics().ReassembliesAbandoned);
    }

    [Fact]
    public void A_Newer_Sequence_For_The_Same_Key_Abandons_The_Older_Partial()
    {
        using ServerHarness h = new(table: Table);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(3, Handlers.Collect(got));
        ChannelDefinition channel = Table[3]!;
        byte[] older = DatagramKit.Payload(1, 2_400);
        byte[] newer = DatagramKit.Payload(2, 2_400);
        List<byte[]> a = FragmentKit.Split(channel, sequence: 5, key: 3, older, count: 3);
        List<byte[]> b = FragmentKit.Split(channel, sequence: 6, key: 3, newer, count: 3);

        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(a[0]));
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(a[1]));
        h.Run(20_000);
        Assert.Equal(1, FragmentKit.Reassemblies(server, 3));

        foreach (byte[] fragment in b)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(fragment));
        }

        Assert.True(h.RunUntil(() => got.Count == 1), "the newer message did not arrive");
        Assert.Equal(newer, got[0].Payload);
        Assert.Equal(1, h.Statistics().ReassembliesAbandoned);

        // The older message's last fragment now starts a partial of its own and is never delivered (its sequence is stale).
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(a[2]));
        h.Run(20_000);
        Assert.Single(got);
        Assert.Equal(PeerState.Connected, server.State);
    }

    [Fact]
    public void A_Fragmented_Message_Can_Be_Compressed()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));

        // Half of the payload repeats the other half, so LZ4 halves it and the block still needs several datagrams.
        byte[] half = FragmentKit.Noise(4_000, 7);
        byte[] message = [.. half, .. half];
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(4), message).Status);
        Assert.True(h.RunUntil(() => got.Count == 1), "the compressed fragmented message did not arrive");
        Assert.Equal(message, got[0].Payload);
        Assert.Equal(message.Length, got[0].Header.RawLength);
        Assert.True(got[0].Header.Flags.HasFlag(ReceiveFlags.Fragmented));
        Assert.True(got[0].Header.Flags.HasFlag(ReceiveFlags.Compressed));
        PeerStatistics statistics = DatagramKit.Statistics(client);
        Assert.Equal(1, statistics.FragmentedMessagesSent);
        Assert.InRange(statistics.FragmentsSent, 2, 8);
        Assert.Equal(0, DatagramKit.Statistics(h.Server).FragmentsDropped);
    }

    [Fact]
    public void A_Reassembled_Message_Reaches_A_Coalescing_Mailbox()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(8, Handlers.Collect(got));
        ChannelDefinition channel = Table[8]!;
        byte[] first = DatagramKit.Payload(1, LengthFor(channel, 2));
        byte[] second = DatagramKit.Payload(2, LengthFor(channel, 2));

        // Two messages of one key inside one flush: the mailbox keeps the newest, so only one reaches the handler.
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(8, 1), first).Status);
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(8, 1), second).Status);
        client.Flush();
        // Both messages are delivered before the game thread claims the key, so the newer one displaces the older.
        h.Network.Advance(50_000);
        server.Poll();
        Assert.Single(got);
        Assert.Equal(second, got[0].Payload);
        Assert.Equal(1UL, got[0].Header.Key);
        Assert.Equal(1, DatagramKit.ChannelStats(server, 8).ReceiveSuperseded);
    }

    [Fact]
    public void Fragmented_Traffic_Survives_Loss_Reordering_And_Jitter()
    {
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, JitterMicros = 2_000, LossPercent = 2, ReorderPercent = 5 },
            table: Table,
            client: DatagramKit.Quiet,
            server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int received = 0;
        string? failure = null;
        server.RegisterHandler(3, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            int id = (int)header.Sequence;
            if (failure is null && !payload.SequenceEqual(DatagramKit.Payload(id, payload.Length)))
            {
                failure = $"message {id} arrived damaged";
            }

            received++;
        });

        int sent = 0;
        for (int i = 0; i < 200; i++)
        {
            byte[] payload = DatagramKit.Payload(i, 2_000 + (i % 7 * 400));
            if (client.SendCopy(new SendHeader(3, (ulong)(i % 4)), payload).IsAdmitted)
            {
                sent++;
            }

            client.Flush();
            h.Network.Advance(16_667);
            server.Poll();
            client.Poll();
        }

        h.Run(500_000);
        Assert.Null(failure);
        Assert.Equal(200, sent);
        // Every fragment must arrive for its message to, so a 2 % loss costs several of them: the rest is byte-exact.
        Assert.InRange(received, 100, 200);
        Assert.Equal(PeerState.Connected, client.State);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 3));
    }
}
