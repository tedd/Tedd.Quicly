using System.Diagnostics;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

public class PollAllTests
{
    [Fact]
    public async Task Events_Come_From_PollAll_And_Released_Slots_Get_A_New_Generation()
    {
        await using ServerFixture f = new();
        List<string> order = [];
        f.Server.PeerAdmitted += _ => order.Add("admitted");
        f.Server.PeerClosed += (_, _) => order.Add("closed");
        QuiclyPeer client = f.ConnectAdmitted();
        AdmittedPeer admitted = Assert.Single(f.Admitted);
        uint generation = f.Server.Peers[admitted.Index].Generation;

        client.Close(new CloseReason(QuiclyErrorCode.NoError, "bye"));
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        Assert.Equal(["admitted", "closed"], order);
        Assert.Same(admitted.Peer, f.Closed[0].Peer);
        Assert.Equal("bye", f.Closed[0].Reason.Reason);
        Assert.Equal(CloseSource.Peer, f.Closed[0].Reason.Source);
        Assert.Null(f.Server.GetPeer(admitted.Index));
        Assert.Equal(0, f.Server.Peers.Length);

        f.ConnectAdmitted();
        AdmittedPeer next = f.Admitted[1];
        Assert.Equal(admitted.Index, next.Index);
        Assert.Equal(generation + 1, f.Server.Peers[next.Index].Generation);
        Assert.Null(f.Server.GetPeer(next.Index, generation));
    }

    [Fact]
    public async Task Idle_Peers_Are_Not_Polled()
    {
        await using ServerFixture f = new();
        f.ConnectAdmitted();
        f.ConnectAdmitted();
        f.Run(4_000_000, step: 10_000);
        f.Server.PollAll();
        f.Server.GetStatistics(out ServerStatistics before);
        for (int i = 0; i < 100; i++)
        {
            f.Server.PollAll();
        }

        f.Server.GetStatistics(out ServerStatistics after);
        Assert.Equal(before.PeersPolled, after.PeersPolled);
        Assert.Equal(before.PollAllCalls + 100, after.PollAllCalls);
    }

    [Fact]
    public async Task A_Zero_Message_Budget_Still_Runs_The_Control_Protocol_And_Keeps_Peers_Marked()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.Connect();
        for (int i = 0; i < 100 && client.State != PeerState.Connected; i++)
        {
            f.Network.Advance(1_000);
            f.Server.PollAll(0);
            f.Server.FlushAll();
            f.PumpClients();
        }

        Assert.Equal(PeerState.Connected, client.State);
        f.Server.GetStatistics(out ServerStatistics before);
        f.Server.PollAll(0);
        f.Server.PollAll(0);
        f.Server.GetStatistics(out ServerStatistics after);
        Assert.Equal(before.PeersPolled + 2, after.PeersPolled); // a peer whose budget ran out stays marked
        Assert.Throws<ArgumentOutOfRangeException>(() => f.Server.PollAll(-1));
    }

    [Fact]
    public async Task PollAll_From_An_Event_Returns_Zero_And_A_Throwing_Handler_Keeps_The_Work()
    {
        await using ServerFixture f = new();
        int nested = -1;
        bool throwOnce = true;
        f.Server.PeerAdmitted += _ =>
        {
            nested = f.Server.PollAll();
            if (throwOnce)
            {
                throwOnce = false;
                throw new InvalidOperationException("handler");
            }
        };
        QuiclyPeer client = f.Connect();
        Exception? caught = null;
        for (int i = 0; i < 100 && (caught is null || client.State != PeerState.Connected); i++)
        {
            f.Network.Advance(1_000);
            try
            {
                f.Server.PollAll();
            }
            catch (InvalidOperationException exception)
            {
                caught = exception;
            }

            f.Server.FlushAll();
            f.PumpClients();
        }

        Assert.NotNull(caught);
        Assert.Equal(0, nested);
        Assert.Equal(PeerState.Connected, client.State);
        f.Run(10_000);
        Assert.Single(f.Admitted);
        Assert.Equal(1, f.Server.AdmittedCount);
    }

    [Fact]
    public async Task Auto_Flush_Runs_From_PollAll()
    {
        await using ServerFixture f = new(o => o.PeerOptions.AutoFlushInterval = TimeSpan.FromMilliseconds(10));
        QuiclyPeer client = f.Connect();
        for (int i = 0; i < 200 && client.State != PeerState.Connected; i++)
        {
            f.Network.Advance(1_000);
            f.Server.PollAll();
            f.PumpClients();
        }

        Assert.Equal(PeerState.Connected, client.State);
        f.Run(100_000);
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public async Task FlushAll_Activates_New_Connections()
    {
        await using ServerFixture f = new();
        f.Connect();
        f.Network.Advance(1_000);
        Assert.Equal(1, f.Server.PeerCount);
        Assert.Equal(0, f.Server.Peers.Length);
        f.Server.FlushAll();
        Assert.Equal(1, f.Server.Peers.Length);
        Assert.Equal(PeerSlotState.Handshaking, f.Server.Peers[0].State);
    }

    [Fact]
    public async Task PollAll_Does_Not_Allocate_With_Idle_Or_Pinging_Peers()
    {
        await using ServerFixture f = new(o =>
        {
            o.PeerOptions.FastLockDuration = TimeSpan.Zero;
            o.PeerOptions.PingInterval = TimeSpan.FromMilliseconds(20);
            o.PeerOptions.PongsPerSecond = 100;
        });
        f.ConfigureClient = c =>
        {
            c.FastLockDuration = TimeSpan.Zero;
            c.PingInterval = TimeSpan.FromMilliseconds(20);
            c.PongsPerSecond = 100;
        };
        for (int i = 0; i < 32; i++)
        {
            f.Connect();
        }

        Assert.True(f.RunUntil(() => f.Admitted.Count == 32));
        f.Run(200_000);
        QuiclyServer server = f.Server;
        SimulatedNetwork network = f.Network;
        AllocationAssert.NoAllocations(() => server.PollAll(), iterations: 5_000);

        server.GetStatistics(out ServerStatistics before);
        AllocationAssert.NoAllocations(() =>
        {
            network.Advance(1_000);
            server.PollAll();
            server.FlushAll();
            f.PumpClients();
        }, warmup: 500, iterations: 1_000);
        server.GetStatistics(out ServerStatistics after);
        Assert.True(after.PeersPolled - before.PeersPolled > 1_000, (after.PeersPolled - before.PeersPolled) + " peer polls");
        Assert.Equal(32, server.AdmittedCount);
    }
}

[CollectionDefinition(nameof(MeasurementCollection), DisableParallelization = true)]
public sealed class MeasurementCollection;

/// <summary>The 1 000-peer run and the PollAll cost measurement (not parallelised, so other tests do not skew the timing).</summary>
[Collection(nameof(MeasurementCollection))]
public class ThousandPeerTests(ITestOutputHelper output)
{
    [Fact]
    public async Task A_Thousand_Peers_Connect_And_PollAll_Polls_Only_The_Peers_With_Work()
    {
        const int Peers = 1000;
        await using ServerFixture f = new(o =>
        {
            o.ExpectedPeers = Peers;
            o.MaxPeers = Peers;
            o.Admission.MaxUnadmittedConnections = Peers;
            o.PeerOptions.AllocatorOptions = null; // the server's own pool, scaled for 1 000 expected peers
            o.PeerOptions.FastLockDuration = TimeSpan.Zero;
        });
        f.ConfigureClient = c => c.FastLockDuration = TimeSpan.Zero;
        Assert.Equal(256, f.Server.Sizing.SendTableCapacity);
        Assert.Equal(128L << 20, f.Server.Sizing.SharedPoolBytes);

        QuiclyPeer[] clients = new QuiclyPeer[Peers];
        for (int i = 0; i < Peers; i++)
        {
            clients[i] = f.Connect();
        }

        // The server admits in one step and the HelloAcks reach the clients in the next: wait for both sides.
        Assert.True(f.RunUntil(() => f.Admitted.Count == Peers && Array.TrueForAll(clients, c => c.State == PeerState.Connected), maxMicros: 5_000_000));

        // Every connection exchanged the Hello/HelloAck; now let each side's first ping get its pong.
        f.Run(20_000);
        foreach (QuiclyPeer client in clients)
        {
            client.GetStatistics(out PeerStatistics statistics);
            Assert.True(statistics.RttSamples >= 1 && statistics.PongsReceived >= 1 && statistics.PongsSent >= 1);
        }

        foreach (PeerSlot slot in f.Server.Peers)
        {
            slot.Peer!.GetStatistics(out PeerStatistics statistics);
            Assert.True(statistics.RttSamples >= 1 && statistics.PongsSent >= 1);
        }

        // Idle: no transport callbacks and no deadline due, so PollAll polls no peer at all. Each phase is warmed up for half a
        // second first so the timed loop runs optimised (tier-1) code rather than the JIT's first tier.
        QuiclyServer server = f.Server;
        server.PollAll();
        server.GetStatistics(out ServerStatistics beforeIdle);
        WarmUp(() => server.PollAll());
        const int IdleCalls = 200_000;
        long started = Stopwatch.GetTimestamp();
        for (int i = 0; i < IdleCalls; i++)
        {
            server.PollAll();
        }

        double idleMicros = Stopwatch.GetElapsedTime(started).TotalMicroseconds / IdleCalls;
        server.GetStatistics(out ServerStatistics afterIdle);
        Assert.Equal(beforeIdle.PeersPolled, afterIdle.PeersPolled);

        // The naive loop for comparison: every live peer's Poll, work or not.
        WarmUp(() => server.PollEveryPeer());
        const int NaiveCalls = 500;
        started = Stopwatch.GetTimestamp();
        for (int i = 0; i < NaiveCalls; i++)
        {
            server.PollEveryPeer();
        }

        double naiveMicros = Stopwatch.GetElapsedTime(started).TotalMicroseconds / NaiveCalls;

        // Steady state: 1 ms ticks with 1 Hz pings each way (a few peers get work each tick); 1 000 warm-up ticks first.
        for (int i = 0; i < 1_000; i++)
        {
            f.Network.Advance(1_000);
            server.PollAll();
            f.PumpClients();
        }

        const int Ticks = 2_000;
        double polling = 0;
        f.Server.GetStatistics(out ServerStatistics beforeTicks);
        for (int i = 0; i < Ticks; i++)
        {
            f.Network.Advance(1_000);
            long tick = Stopwatch.GetTimestamp();
            f.Server.PollAll();
            polling += Stopwatch.GetElapsedTime(tick).TotalMicroseconds;
            f.PumpClients();
        }

        f.Server.GetStatistics(out ServerStatistics afterTicks);
        double tickMicros = polling / Ticks;
        double peersPerTick = (double)(afterTicks.PeersPolled - beforeTicks.PeersPolled) / Ticks;
        output.WriteLine("PollAll, 1 000 idle peers: " + idleMicros.ToString("F3") + " us per call");
        output.WriteLine("Poll of every peer (naive loop), 1 000 peers: " + naiveMicros.ToString("F1") + " us per call");
        output.WriteLine("PollAll at 1 ms ticks with 1 Hz pings: " + tickMicros.ToString("F2") + " us per call, " + peersPerTick.ToString("F2") + " peers polled per call");
        Console.WriteLine("MEASURE idle=" + idleMicros.ToString("F3") + "us naive=" + naiveMicros.ToString("F1") + "us tick=" + tickMicros.ToString("F2") + "us peersPerTick=" + peersPerTick.ToString("F2"));
        // What PollAll does, not how fast this machine is (the timings above are output only): idle calls poll no peer and
        // allocate nothing, and a tick polls only the few peers whose ping or pong is due, not all 1 000.
        Assert.True(peersPerTick < 10, peersPerTick.ToString("F2") + " peers polled per tick");
        AllocationAssert.NoAllocations(() => server.PollAll(), iterations: 10_000);
        Assert.All(clients, c => Assert.Equal(PeerState.Connected, c.State));
    }

    /// <summary>Runs <paramref name="action"/> for half a second so the JIT has installed optimised code before timing.</summary>
    private static void WarmUp(Action action)
    {
        long until = Stopwatch.GetTimestamp() + (Stopwatch.Frequency / 2);
        while (Stopwatch.GetTimestamp() < until)
        {
            action();
        }
    }
}
