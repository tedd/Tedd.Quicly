using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Steady-state ReliableLatest traffic must not allocate (ADR 0008): admission with the per-key slots, the transmissions
/// through the packer, the coalesced acks, the mailbox receive on the transport thread (the simulator raises it on the test
/// thread) and the completions. Measured on a clean link, where the simulator itself allocates nothing (review of wave C1,
/// non-blocking performance finding 1); loss is covered by the delivery tests instead, and the retry, duplicate and
/// re-ack paths are reached here without loss, by an ack delay longer than the retry timer.
/// </summary>
public class LatestZeroAllocationTests
{
    private static readonly ChannelTable Table = LatestTables.Main;

    [Fact]
    public void A_Thousand_Keys_Updated_At_Sixty_Hertz_Does_Not_Allocate()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 }, table: Table,
            client: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            server: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            });
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        server.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        byte[] payload = new byte[64];
        uint tick = 0;
        void Tick()
        {
            for (ulong key = 0; key < 1_000; key++)
            {
                client.SendCopy(new SendHeader(4, key), payload);
            }

            client.Flush(++tick);
            network.Advance(16_667);
            server.Poll();
            server.Flush();
            client.Poll();
        }

        // Warm-up: tiered compilation, the peer's tables, the key slots and the simulator's pool reach their steady state.
        for (int i = 0; i < 240; i++)
        {
            Tick();
        }

        long delivered = received;
        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 60; i++)
            {
                Tick();
            }
        });
        Assert.True(received > delivered + (5 * 60 * 900), $"{received - delivered} values in the measured windows");
        Assert.Equal(PeerState.Connected, client.State);
        Assert.Equal(0, DatagramKit.ChannelStats(client, 4).Retries);

        // At 60 Hz the application replaces most keys before their ack arrives, so the values complete Superseded rather
        // than Delivered (PROTOCOL.md §4.3) — every one of them still reached the peer. Once the traffic stops, the last
        // acks arrive and no key is live any more.
        Assert.True(DatagramKit.ChannelStats(client, 4).SendSuperseded > 0);
        h.Run(200_000);
        Assert.Equal(0, LatestKit.LiveKeys(client, 4));
    }

    [Fact]
    public void Duplicates_Of_An_Accepted_Version_Are_Dropped_And_Re_Acked_Without_Allocating()
    {
        // The receiver holds its acks back for longer than the sender's retry timer, so every value is retransmitted on a
        // clean link: the duplicates take the stale path (extended on the version clock, dropped, re-acked), and the acks
        // that do not name the version last transmitted take the sender's "proves nothing" path.
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: Table,
            client: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            server: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
                o.AckDelay = TimeSpan.FromMilliseconds(120);
            });
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        byte[] payload = new byte[64];
        uint tick = 0;
        void Tick()
        {
            if ((tick & 3) == 0)
            {
                for (ulong key = 0; key < 16; key++)
                {
                    client.SendCopy(new SendHeader(2, key), payload);
                }
            }

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

        long droppedBefore = DatagramKit.ChannelStats(server, 2).Dropped;
        long retriesBefore = DatagramKit.ChannelStats(client, 2).Retries;
        int windows = WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 120; i++)
            {
                Tick();
            }
        });
        long dropped = DatagramKit.ChannelStats(server, 2).Dropped - droppedBefore;
        long retries = DatagramKit.ChannelStats(client, 2).Retries - retriesBefore;
        Assert.True(dropped > windows * 100L, $"{dropped} duplicates dropped in {windows} windows");
        Assert.True(retries > windows * 100L, $"{retries} retransmissions in {windows} windows");
        Assert.True(received > 600 * 4, $"{received} values");
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void Superseding_And_Retiring_Keys_Does_Not_Allocate()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 }, table: Table,
            client: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            server: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            });
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        byte[] payload = new byte[64];
        void Tick()
        {
            // Two values per key in the same tick (the first is superseded before it is submitted), then the keys are retired
            // and used again next tick, so the key tables and their slots stay at a steady size.
            for (ulong key = 0; key < 32; key++)
            {
                client.SendCopy(new SendHeader(2, key), payload);
                client.SendCopy(new SendHeader(2, key), payload);
            }

            client.Flush();
            network.Advance(16_667);
            server.Poll();
            server.Flush();
            client.Poll();
            for (ulong key = 0; key < 32; key++)
            {
                client.RetireKey(2, key);
            }

            client.Flush();
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
            for (int i = 0; i < 50; i++)
            {
                Tick();
            }
        });
        Assert.True(received > 600 * 32, $"{received} values");
        Assert.Equal(PeerState.Connected, client.State);
    }
}
