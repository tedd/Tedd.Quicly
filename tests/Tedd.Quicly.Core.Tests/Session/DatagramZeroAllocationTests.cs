using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Steady-state send, flush, packing and poll of the datagram engines must not allocate (ADR 0008). The simulator raises the
/// transport callbacks on the test thread, so the transport-thread receive paths are measured too.
/// </summary>
public class DatagramZeroAllocationTests
{
    private static readonly ChannelTable Table = DatagramTables.Main;

    [Fact]
    public void Sixty_Hertz_Traffic_Of_64_Byte_Messages_Does_Not_Allocate()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000, JitterMicros = 2_000 }, table: Table);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        MessageHandler count = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++;
        server.RegisterHandler(2, count);
        server.RegisterHandler(3, count);
        server.RegisterHandler(4, count);
        server.RegisterHandler(5, count);
        byte[] payload = new byte[64];
        byte[] compressible = new byte[200];
        uint tick = 0;
        void Tick()
        {
            for (int i = 0; i < 8; i++)
            {
                client.SendCopy(new SendHeader(2), payload);
                client.SendCopy(new SendHeader(3, (ulong)i), payload);
                client.SendCopy(new SendHeader(4, (ulong)i), payload);
            }

            client.SendCopy(new SendHeader(5), compressible);
            client.SendCopy(new SendHeader(2), payload, SendOptions.Tracked);
            client.Flush(++tick);
            network.Advance(16_667);
            server.Poll();
            server.Flush();
            client.Poll();
        }

        for (int i = 0; i < 600; i++)
        {
            Tick();
        }

        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 120; i++)
            {
                Tick();
            }
        });
        Assert.True(received > 600 * 20, $"{received} messages");
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void Expiring_Traffic_Does_Not_Allocate()
    {
        // The channels above switch expiry off; these keep it, so every send marks its entry and every Flush resolves the
        // marks against its clock (PeerCore.StampExpiry / ResolveExpiry). 2 has the UnreliableSequenced default.
        ChannelTable table = ChannelTable.Create()
            .Add(2, "moves", ChannelMode.UnreliableSequenced, o => o.Keyed = true)
            .Add(3, "events", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 50_000)
            .Build();
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000, JitterMicros = 2_000 }, table: table);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        MessageHandler count = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++;
        server.RegisterHandler(2, count);
        server.RegisterHandler(3, count);
        byte[] payload = new byte[64];
        uint tick = 0;
        void Tick()
        {
            for (int i = 0; i < 16; i++)
            {
                client.SendCopy(new SendHeader(2, (ulong)i), payload);
                client.SendCopy(new SendHeader(3), payload);
            }

            client.SendCopy(new SendHeader(3), payload, new SendOptions { ExpiryMicros = 5_000 });
            client.Flush(++tick);
            network.Advance(16_667);
            server.Poll();
            server.Flush();
            client.Poll();
        }

        for (int i = 0; i < 600; i++)
        {
            Tick();
        }

        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 120; i++)
            {
                Tick();
            }
        });
        Assert.True(received > 600 * 20, $"{received} messages");
        Assert.False(client.Core.HasPendingExpiry);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 2).Expired);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 3).Expired);
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void Sequenced_Receive_Across_The_16_Bit_Wrap_Does_Not_Allocate()
    {
        // Every window sends more than 65 536 messages on each channel, so the receiver's sequence clock (PROTOCOL.md §8)
        // crosses a wrap in every one of them; keys 8 … 15 stay idle for the whole window and are updated at its end, which
        // takes the stale-free idle-key path too.
        ChannelTable table = ChannelTable.Create()
            .Add(2, "moves16", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 16; o.ExpiryMicros = 0; })
            .Add(3, "clock", ChannelMode.UnreliableSequenced, o => o.ExpiryMicros = 0)
            .Build();
        using SessionHarness h = new(table: table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        MessageHandler count = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++;
        server.RegisterHandler(2, count);
        server.RegisterHandler(3, count);
        byte[] payload = new byte[8];
        uint tick = 0;
        void Tick(ulong firstKey)
        {
            for (int i = 0; i < 128; i++)
            {
                client.SendCopy(new SendHeader(2, firstKey + (ulong)(i & 7)), payload);
                client.SendCopy(new SendHeader(3), payload);
            }

            client.Flush(++tick);
            network.Advance(2_000);
            server.Poll();
            server.Flush();
            client.Poll();
        }

        void Window()
        {
            for (int i = 0; i < 520; i++)
            {
                Tick(0);
            }

            Tick(8);
        }

        Window();
        int windows = WindowedAllocation.AssertNone(Window);
        Assert.Equal((windows + 1) * 521L * 256, received);
        Assert.Equal(0, DatagramKit.ChannelStats(server, 2).Dropped);
        Assert.Equal(0, DatagramKit.ChannelStats(server, 3).Dropped);
        Assert.Equal(0, DatagramKit.Statistics(server).SequenceResyncs);
    }

    [Fact]
    public void Transport_Cancels_Do_Not_Allocate()
    {
        // A link that is busy for 8 ms per 1 000-byte datagram: everything sent in the same instant as the first datagram of
        // a tick is dropped by the transport (CancelOnBlocked) and counted — a loose message by its engine, the members of a
        // packed container by the packer's fan-out, fragments by the fragment path.
        using SessionHarness h = new(link: new LinkOptions { BandwidthBitsPerSecond = 1_000_000 }, table: Table,
            client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        MessageHandler count = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++;
        server.RegisterHandler(2, count);
        server.RegisterHandler(3, count);
        server.RegisterHandler(13, count);
        byte[] large = new byte[1_000];
        byte[] small = new byte[8];
        byte[] fragmented = new byte[FragmentKit.LengthFor(Table[13]!, 2)];
        uint tick = 0;
        void Tick()
        {
            client.SendCopy(new SendHeader(2), large);
            client.Flush(++tick);
            client.SendCopy(new SendHeader(2), large);
            client.Flush(tick);
            client.SendCopy(new SendHeader(2), small);
            client.SendCopy(new SendHeader(3, tick & 7), small);
            client.Flush(tick);
            client.SendCopy(new SendHeader(13), fragmented);
            client.Flush(tick);
            network.Advance(16_667);
            server.Poll();
            server.Flush();
            client.Poll();
        }

        for (int i = 0; i < 600; i++)
        {
            Tick();
        }

        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 120; i++)
            {
                Tick();
            }
        });
        Assert.True(received > 600, $"{received} messages");
        Assert.True(DatagramKit.ChannelStats(client, 2).TransportCanceled > 1_200, $"{DatagramKit.ChannelStats(client, 2).TransportCanceled} cancelled on channel 2");
        Assert.True(DatagramKit.ChannelStats(client, 3).TransportCanceled > 600, $"{DatagramKit.ChannelStats(client, 3).TransportCanceled} cancelled on channel 3");
        Assert.True(DatagramKit.ChannelStats(client, 13).TransportCanceled > 1_200, $"{DatagramKit.ChannelStats(client, 13).TransportCanceled} cancelled on channel 13");
        Assert.True(DatagramKit.Statistics(client).DatagramsCanceled > 2_400, $"{DatagramKit.Statistics(client).DatagramsCanceled} datagrams cancelled");
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void A_Capped_Scheduler_With_Expiring_Messages_Does_Not_Allocate()
    {
        using SessionHarness h = new(table: Table, client: o => o.MaxSendBytesPerSecond = 30_000);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        server.RegisterHandler(14, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        byte[] payload = new byte[64];
        uint tick = 0;
        void Tick()
        {
            for (int i = 0; i < 16; i++)
            {
                client.SendCopy(new SendHeader(14, (ulong)i), payload);
            }

            client.Flush(++tick);
            network.Advance(16_667);
            server.Poll();
            client.Poll();
        }

        for (int i = 0; i < 600; i++)
        {
            Tick();
        }

        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 120; i++)
            {
                Tick();
            }
        });
        Assert.True(received > 1_000, $"{received} messages");
        Assert.True(DatagramKit.ChannelStats(client, 14).Expired > 1_000);
    }
}
