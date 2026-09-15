using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
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

    [Fact]
    public void Steady_Ordered_Traffic_Of_64_Byte_Messages_Does_Not_Allocate()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000, JitterMicros = 2_000, StreamLossPercent = 2 }, table: Table);
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

        // Warm up past the harness's own growth: on this lossy link the simulator's buffer pool allocates a pinned buffer (on
        // this thread) whenever more sends are in flight at once than ever before. With seed 1 that happens at ticks 1 382 and
        // 1 428, then not before tick 5 422.
        for (int i = 0; i < 3_000; i++)
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
        using SemaphoreSlim go = new(0);
        using SemaphoreSlim done = new(0);
        bool stop = false;
        Thread producer = new(() =>
        {
            byte[] payload = new byte[64];
            while (true)
            {
                go.Wait();
                if (Volatile.Read(ref stop))
                {
                    return;
                }

                for (int i = 0; i < 10; i++)
                {
                    client.SendCopy(new SendHeader(4), payload);
                }

                done.Release();
            }
        })
        {
            IsBackground = true,
        };
        producer.Start();
        void Tick()
        {
            go.Release();
            done.Wait();
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
        Volatile.Write(ref stop, true);
        go.Release();
        Assert.True(producer.Join(TimeSpan.FromSeconds(10)));
        Assert.True(received > 1_000 * 10);
    }
}
