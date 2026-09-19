using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Steady-state fragmentation and request/response must not allocate (ADR 0008): splitting a message and reassembling it,
/// and a request round trip whose value task is backed by the engine's pooled source. The simulator raises the transport
/// callbacks on the test thread, so the receive paths are measured too.
/// </summary>
public class FragmentRequestZeroAllocationTests
{
    /// <summary>2 unordered fragmenting · 3 sequenced keyed fragmenting · 10 ordered request/response.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "chunks", ChannelMode.UnreliableUnordered, o => { o.Fragmentation = true; o.MaxMessageSize = ChannelDefinition.FragmentedMaxMessageSize; })
        .Add(3, "state", ChannelMode.UnreliableSequenced, o =>
        {
            o.Fragmentation = true;
            o.Keyed = true;
            o.MaxMessageSize = ChannelDefinition.FragmentedMaxMessageSize;
            o.ExpiryMicros = 0;
        })
        .Add(10, "rpc", ChannelMode.ReliableOrdered, o => o.RequestResponse = true)
        .Build();

    [Fact]
    public void Steady_Fragmented_Traffic_Does_Not_Allocate()
    {
        // A clean link (delay only), like the other steady-state tests: on a lossy or jittery one the simulator's own pool
        // grows at seed-dependent ticks and a window could straddle one of those events.
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 10_000 },
            table: Table,
            client: DatagramKit.Quiet,
            server: DatagramKit.Quiet);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        MessageHandler count = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++;
        server.RegisterHandler(2, count);
        server.RegisterHandler(3, count);
        byte[] three = new byte[2_400];
        byte[] eight = new byte[8_000];
        uint tick = 0;
        void Tick()
        {
            client.SendCopy(new SendHeader(2), three);
            client.SendCopy(new SendHeader(3, tick % 4), three);
            client.SendCopy(new SendHeader(2), eight);
            client.SendCopy(new SendHeader(3, tick % 4), eight, SendOptions.Tracked);
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

        long before = received;
        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 120; i++)
            {
                Tick();
            }
        });

        Assert.True(received - before >= 120 * 4 * 0.9, $"{received - before} messages arrived in the measured windows");
        Assert.Equal(0, DatagramKit.Statistics(server).FragmentsDropped);
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void Steady_Request_Response_Traffic_Does_Not_Allocate()
    {
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 10_000 },
            table: Table,
            client: DatagramKit.Quiet,
            server: DatagramKit.Quiet);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        server.RegisterHandler(10, (QuiclyPeer self, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
            self.Respond(in header, payload));

        const int Batch = 4;
        ReadOnlyMemory<byte> request = new byte[64];
        ValueTask<ReceiveLease>[] pending = new ValueTask<ReceiveLease>[Batch];
        long answered = 0;
        long failures = 0;
        void Cycle()
        {
            for (int i = 0; i < Batch; i++)
            {
                // No cancellation token and no await: the library's own path allocates nothing per request.
                pending[i] = client.SendRequestAsync(new SendHeader(10), request, TimeSpan.Zero);
            }

            for (int pass = 0; pass < 8 && !pending[Batch - 1].IsCompleted; pass++)
            {
                client.Flush();
                network.Advance(10_000);
                server.Poll();
                server.Flush();
                network.Advance(10_000);
                client.Poll();
            }

            for (int i = 0; i < Batch; i++)
            {
                if (!pending[i].IsCompleted)
                {
                    failures++;
                    continue;
                }

                ReceiveLease response = pending[i].GetAwaiter().GetResult();
                answered++;
                client.Release(in response);
            }
        }

        for (int i = 0; i < 400; i++)
        {
            Cycle();
        }

        long before = answered;
        int windows = WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 50; i++)
            {
                Cycle();
            }
        });

        Assert.Equal(0, failures);
        Assert.Equal(50 * Batch * windows, answered - before);
        Assert.Equal(0, DatagramKit.Statistics(client).ResponsesUnmatched);
        Assert.Equal(0, DatagramKit.Statistics(client).RequestsTimedOut);
    }
}
