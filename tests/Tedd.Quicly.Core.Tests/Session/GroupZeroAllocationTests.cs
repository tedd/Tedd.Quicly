using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Steady-state group traffic must not allocate (ADR 0008), even though every group opens and closes a stream of its own:
/// admission, sealing, the gathered carriers, the group records, the stream notices, the progressive receive on the transport
/// thread (the simulator raises it on the test thread), dispatch and the completions.
/// </summary>
public class GroupZeroAllocationTests
{
    private static readonly ChannelTable Table = GroupTables.Main;

    /// <summary>
    /// Measured on a clean link (delay only), where the simulator itself allocates nothing: on a lossy or jittery link its
    /// buffer pool rents a pinned array whenever more sends are in flight than ever before, so a window could straddle one of
    /// those events and fail for a reason outside the session layer (review of wave C1, non-blocking performance finding 1).
    /// Loss and jitter are covered by <see cref="GroupDeliveryTests"/> instead.
    /// </summary>
    [Fact]
    public void Steady_Group_Traffic_Of_64_Byte_Messages_Does_Not_Allocate()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000 }, table: Table);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        MessageHandler count = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++;
        server.RegisterHandler(5, count);
        server.RegisterHandler(6, count);
        server.RegisterHandler(7, count);
        client.RegisterHandler(5, count);
        byte[] payload = new byte[64];
        byte[] compressible = new byte[300];
        uint tick = 0;
        void Tick()
        {
            // One group per channel per tick: 16 plain messages, a keyed one, a compressed one, a tracked one, and one the
            // other way round.
            for (int i = 0; i < 16; i++)
            {
                client.SendCopy(new SendHeader(5), payload);
            }

            client.SendCopy(new SendHeader(6, tick), payload);
            client.SendCopy(new SendHeader(7), compressible);
            client.SendCopy(new SendHeader(5), payload, SendOptions.Tracked);
            server.SendCopy(new SendHeader(5), payload);
            client.Flush(++tick);
            network.Advance(16_667);
            server.Poll();
            server.Flush();
            client.Poll();
        }

        // Warm-up: tiered compilation, the peer's tables, the group records and the simulator's stream slots all reach the
        // run's peak (a group stream is opened and closed every tick, so the peak of concurrent streams matters here).
        for (int i = 0; i < 1_200; i++)
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
        Assert.True(received > 1_200 * 18, $"{received} messages");
        Assert.Equal(PeerState.Connected, client.State);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
        Assert.Equal(0, DatagramKit.Statistics(client).CallbackFaults);
    }
}
