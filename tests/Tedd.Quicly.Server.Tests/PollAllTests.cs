using System.Diagnostics;
using System.Reflection;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

public class PollAllTests
{
    /// <summary>Whether the server still holds a work bit for <paramref name="slot"/>.</summary>
    private static bool IsMarked(QuiclyServer server, int slot)
    {
        long[] words = (long[])typeof(QuiclyServer).GetField("_workBits", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(server)!;
        return (Volatile.Read(ref words[slot >> 6]) & (1L << (slot & 63))) != 0;
    }

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
        List<byte[]> received = [];
        f.Server.PeerAdmitted += peer => peer.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => received.Add(payload.ToArray()));
        QuiclyPeer client = f.Connect();
        for (int i = 0; i < 100 && client.State != PeerState.Connected; i++)
        {
            f.Network.Advance(1_000);
            f.Server.PollAll(0);
            f.Server.FlushAll();
            f.PumpClients();
        }

        Assert.Equal(PeerState.Connected, client.State);
        int slot = f.ServerPeerOf(client).Index;
        Assert.True(client.SendCopy(new SendHeader(2), [1]).IsAdmitted);
        client.Flush();
        f.Network.Advance(1_000);

        f.Server.GetStatistics(out ServerStatistics before);
        f.Server.PollAll(0);
        f.Server.GetStatistics(out ServerStatistics after);
        Assert.Equal(before.PeersPolled + 1, after.PeersPolled); // polled, but no message dispatched
        Assert.Empty(received);
        Assert.True(IsMarked(f.Server, slot), "a peer whose budget ran out stays marked");

        Assert.Equal(1, f.Server.PollAll(1)); // with a budget the message reaches its handler
        Assert.Single(received);
        Assert.Throws<ArgumentOutOfRangeException>(() => f.Server.PollAll(-1));
    }

    /// <summary>
    /// The work signal is an edge and <see cref="QuiclyPeer.HasPendingWork"/> the level behind it: a slot marked for a peer
    /// with nothing waiting is not polled, and it keeps its bit — only a <see cref="QuiclyPeer.Poll"/> re-arms the peer's
    /// signal, so clearing the bit without polling could miss the next publication.
    /// </summary>
    [Fact]
    public async Task A_Marked_Peer_With_Nothing_Pending_Is_Not_Polled_But_Stays_Marked()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.ConnectAdmitted();
        int slot = f.ServerPeerOf(client).Index;
        f.Run(4_000_000, step: 10_000); // let the fast-lock pings settle
        f.Server.PollAll();
        Assert.False(f.ServerPeerOf(client).HasPendingWork);

        f.Server.MarkWork(slot);
        f.Server.GetStatistics(out ServerStatistics before);
        f.Server.PollAll();
        f.Server.PollAll();
        f.Server.GetStatistics(out ServerStatistics skipped);
        Assert.Equal(before.PeersPolled, skipped.PeersPolled);
        Assert.True(IsMarked(f.Server, slot));

        // Work arrives, and the very next PollAll polls it (nothing else marked the slot).
        Assert.True(client.SendCopy(new SendHeader(2), [7]).IsAdmitted);
        client.Flush();
        f.Network.Advance(1_000);
        f.Server.PollAll();
        f.Server.GetStatistics(out ServerStatistics polled);
        Assert.True(polled.PeersPolled > skipped.PeersPolled);
    }

    /// <summary>
    /// The split deadlines: a host sleeps its polling loop on <see cref="QuiclyServer.NextPollDeadlineMicros"/> (the peers'
    /// own timers), and engine work that only a scheduler pass can serve brings a flush forward — a send the peer's bandwidth
    /// cap held back still leaves although the host calls nothing but <see cref="QuiclyServer.PollAll"/> after its one tick.
    /// </summary>
    [Fact]
    public async Task The_Poll_Deadline_Is_Separate_And_PollAll_Brings_A_Held_Back_Flush_Forward()
    {
        await using ServerFixture f = new(o => o.PeerOptions.MaxSendBytesPerSecond = 4_000);
        QuiclyPeer client = f.ConnectAdmitted();
        QuiclyPeer serverPeer = f.ServerPeerOf(client);
        List<int> received = [];
        client.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => received.Add(payload.Length));
        Assert.True(f.Server.NextPollDeadlineMicros > f.Clock.NowMicros);
        Assert.True(f.Server.NextPollDeadlineMicros < long.MaxValue);

        for (int i = 0; i < 6; i++)
        {
            Assert.True(serverPeer.SendCopy(new SendHeader(2), new byte[1000]).IsAdmitted);
        }

        f.Server.FlushAll(); // the host's one and only tick
        Assert.True(f.Server.NextFlushDeadlineMicros < long.MaxValue, "the bandwidth cap held a pass back, so a flush is due");
        Assert.True(f.Server.NextFlushDeadlineMicros > f.Clock.NowMicros);

        for (int i = 0; i < 4_000 && received.Count < 6; i++)
        {
            f.Network.Advance(1_000);
            f.Server.PollAll(); // no FlushAll and no AutoFlushInterval: only the flush PollAll brings forward
            f.PumpClients();
        }

        Assert.Equal(6, received.Count);
        Assert.All(received, length => Assert.Equal(1000, length));
    }

    /// <summary>
    /// The naive measurement loop (<see cref="QuiclyServer.PollEveryPeer"/>) polls every live peer and, unlike
    /// <see cref="QuiclyServer.PollAll"/>, raises nothing and releases no slot: a peer that closes there is marked, so the next
    /// PollAll finds it closed and finalizes it without polling it again.
    /// </summary>
    [Fact]
    public async Task PollEveryPeer_Marks_A_Peer_That_Closed_For_The_Next_PollAll()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.ConnectAdmitted();
        QuiclyPeer serverPeer = f.ServerPeerOf(client);
        int slot = serverPeer.Index;
        client.Close();
        for (int i = 0; i < 200 && serverPeer.State != PeerState.Closed; i++)
        {
            f.Network.Advance(1_000);
            f.Server.PollEveryPeer();
            f.Server.FlushAll();
            f.PumpClients();
        }

        Assert.Equal(PeerState.Closed, serverPeer.State);
        Assert.Empty(f.Closed);
        Assert.Equal(1, f.Server.PeerCount);
        Assert.True(IsMarked(f.Server, slot));

        f.Server.PollAll();
        Assert.Single(f.Closed);
        Assert.Equal(0, f.Server.PeerCount);
        Assert.Null(f.Server.GetPeer(slot));
    }

    /// <summary>A peer the application disposed itself is skipped, not polled (no <see cref="ObjectDisposedException"/>), and its slot goes back.</summary>
    [Fact]
    public async Task A_Peer_The_Application_Disposed_Releases_Its_Slot()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.ConnectAdmitted();
        QuiclyPeer serverPeer = f.ServerPeerOf(client);
        int slot = serverPeer.Index;
        serverPeer.Dispose();

        f.Server.MarkWork(slot);
        f.Server.PollAll();
        f.Server.FlushAll();

        Assert.True(serverPeer.IsDisposed);
        Assert.Single(f.Closed);
        Assert.Null(f.Server.GetPeer(slot));
        Assert.Equal(0, f.Server.PeerCount);
        Assert.Single(f.Ended);
    }

    /// <summary>
    /// A <see cref="QuiclyServer.PollAll"/> from inside one of its own events returns 0, and a throwing
    /// <see cref="QuiclyServer.PeerAdmitted"/> handler no longer escapes it: that event is raised inside the peer's own state
    /// change, so the peer records the exception as a callback fault and the admission stands (a
    /// <see cref="QuiclyServer.PeerClosed"/> handler, which PollAll raises itself, still propagates — see the shutdown tests).
    /// </summary>
    [Fact]
    public async Task PollAll_From_An_Event_Returns_Zero_And_A_Throwing_Admitted_Handler_Is_A_Callback_Fault()
    {
        await using ServerFixture f = new();
        int nested = -1;
        f.Server.PeerAdmitted += _ =>
        {
            nested = f.Server.PollAll();
            throw new InvalidOperationException("handler");
        };

        QuiclyPeer client = f.ConnectAdmitted();

        Assert.Equal(0, nested);
        QuiclyPeer serverPeer = f.ServerPeerOf(client);
        Assert.IsType<InvalidOperationException>(serverPeer.LastCallbackFault);
        serverPeer.GetStatistics(out PeerStatistics statistics);
        Assert.Equal(1, statistics.CallbackFaults);
        Assert.Single(f.Admitted);
        Assert.Equal(1, f.Server.AdmittedCount);
        f.Run(10_000);
        Assert.Equal(PeerState.Connected, client.State);
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
