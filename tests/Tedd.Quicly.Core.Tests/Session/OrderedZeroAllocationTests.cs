using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Steady-state ordered traffic must not allocate (ADR 0008): admission, gathered stream sends, carrier completions, the
/// progressive receive on the transport thread (the simulator raises it on the test thread), dispatch, the synchronous paths
/// of SendAsync and FlushAsync, and the game thread's admission of sends queued by other threads.
/// </summary>
public class OrderedZeroAllocationTests
{
    private static readonly ChannelTable Table = OrderedTables.Main;

    /// <summary>
    /// Measured on a clean link (delay only), where the simulator itself allocates nothing at all: on a lossy or jittery
    /// link its buffer pool rents a pinned array whenever more sends are in flight than ever before, at ticks that depend
    /// on the seed, so a window could straddle one of those events and fail for a reason outside the session layer
    /// (review of wave C1, non-blocking performance finding 1). Loss, jitter and retransmission are covered by
    /// <see cref="Steady_Ordered_Traffic_Under_Loss_And_Jitter_Delivers_Every_Message_In_Order"/>.
    /// </summary>
    [Fact]
    public void Steady_Ordered_Traffic_Of_64_Byte_Messages_Does_Not_Allocate()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000 }, table: Table);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        MessageHandler count = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++;
        server.RegisterHandler(4, count);
        server.RegisterHandler(5, count);
        server.RegisterHandler(6, count);
        client.RegisterHandler(4, count);
        byte[] payload = new byte[64];
        byte[] compressible = new byte[300];
        uint tick = 0;
        void Tick()
        {
            for (int i = 0; i < 16; i++)
            {
                client.SendCopy(new SendHeader(4), payload);
            }

            client.SendCopy(new SendHeader(5, tick), payload);
            client.SendCopy(new SendHeader(6), compressible);
            client.SendCopy(new SendHeader(4), payload, SendOptions.Tracked);
            server.SendCopy(new SendHeader(4), payload);
            client.Flush(++tick);
            network.Advance(16_667);
            server.Poll();
            server.Flush();
            client.Poll();
        }

        // Warm-up: tiered compilation, the peer's tables and the simulator's pool all reach their steady state.
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
        Assert.True(received > 1_200 * 20, $"{received} messages");
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void Steady_Ordered_Traffic_Under_Loss_And_Jitter_Delivers_Every_Message_In_Order()
    {
        // The workload of the allocation test above on a lossy, jittery link: every admitted message arrives, in order and
        // byte-exact, and the session stays connected. Allocation is not measured here (the simulator's buffer pool grows
        // at seed-dependent ticks); it is measured on the clean link above.
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000, JitterMicros = 2_000, StreamLossPercent = 2 }, table: Table);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int received = 0;
        string? failure = null;
        server.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            if (failure is null && !OrderedKit.Matches(payload, received, 64))
            {
                failure = $"message {received} arrived out of order or damaged";
            }

            received++;
        });

        int sent = 0;
        for (uint tick = 0; tick < 600; tick++)
        {
            for (int i = 0; i < 16; i++)
            {
                if (client.SendCopy(new SendHeader(4), OrderedKit.Payload(sent, 64)).IsAdmitted)
                {
                    sent++;
                }
            }

            client.Flush(tick);
            network.Advance(16_667);
            server.Poll();
            server.Flush();
            client.Poll();
        }

        Assert.True(h.RunUntil(() => received == sent), $"{received} of {sent} messages arrived");
        Assert.Null(failure);
        Assert.True(sent > 9_000, $"{sent} messages admitted");
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void The_Synchronous_Paths_Of_SendAsync_And_FlushAsync_Do_Not_Allocate()
    {
        using SessionHarness h = new(table: Table);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        server.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        ReadOnlyMemory<byte> payload = new byte[64];
        long failures = 0;
        void Tick()
        {
            for (int i = 0; i < 8; i++)
            {
                ValueTask<SendResult> send = client.SendAsync(new SendHeader(4), payload);
                if (!send.IsCompletedSuccessfully || send.Result.Status != SendStatus.Admitted)
                {
                    failures++;
                }
            }

            ValueTask flush = client.FlushAsync();
            if (!flush.IsCompletedSuccessfully)
            {
                failures++;
            }

            network.Advance(1_000);
            server.Poll();
            client.Poll();
        }

        for (int i = 0; i < 1_000; i++)
        {
            Tick();
        }

        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 200; i++)
            {
                Tick();
            }
        });

        // The first flush waits for the stream's start to be confirmed; every later one completes at once.
        Assert.True(failures <= 1, $"{failures} sends or flushes did not complete synchronously");
        Assert.True(received > 1_000 * 8);
    }

    /// <remarks>
    /// The game thread and the producer take turns through <see cref="HandOff"/>, which allocates nothing: each spins for a
    /// while and then blocks until the other hands over, so that on a machine with no core to spare a turn costs a thread
    /// wake-up rather than the scheduler quanta a semaphore's yielding spin loses (which made the run take a minute).
    /// </remarks>
    [Fact]
    public void Admitting_Sends_Queued_By_Another_Thread_Does_Not_Allocate_On_The_Game_Thread()
    {
        using SessionHarness h = new(table: Table, client: o => o.ThreadSafeSend = true);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        server.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        client.Poll();
        const long Go = 1;
        const long Stop = 2;
        using HandOff go = new();
        using HandOff done = new();
        Exception? failure = null;
        Thread producer = new(() =>
        {
            byte[] payload = new byte[64];
            try
            {
                while (go.Take() != Stop)
                {
                    for (int i = 0; i < 10; i++)
                    {
                        client.SendCopy(new SendHeader(4), payload);
                    }

                    done.Put(1);
                }
            }
            catch (Exception e)
            {
                failure = e;
                done.Put(1);
            }
        })
        {
            IsBackground = true,
        };
        producer.Start();
        void Tick()
        {
            go.Put(Go);
            done.Take();
            if (failure is not null)
            {
                Assert.Fail($"The producer failed: {failure}");
            }

            client.Flush();
            network.Advance(1_000);
            server.Poll();
            client.Poll();
        }

        for (int i = 0; i < 1_000; i++)
        {
            Tick();
        }

        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 200; i++)
            {
                Tick();
            }
        });
        go.Put(Stop);
        Assert.True(producer.Join(TimeSpan.FromSeconds(10)));
        Assert.True(received > 1_000 * 10);
    }
}
