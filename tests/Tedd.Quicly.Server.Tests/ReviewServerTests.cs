using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

/// <summary>
/// Defects found in the review of the server layer (branch quicly/c3-server-client). Each test states the expected
/// behaviour and fails until the finding is fixed.
/// </summary>
public class ReviewServerTests
{
    /// <summary>
    /// A pending validation that completes after its connection already closed (here: the admission timeout) must be
    /// ignored, as <see cref="QuiclyServer.CompleteAdmission(QuiclyPeer, bool, string?)"/> documents. Today the default
    /// policy still commits the resume: it consumes the token, bumps the epoch, attaches the session to the dead
    /// connection and closes the session's live connection with SessionReplaced, so the real player is kicked for good.
    /// </summary>
    [Fact]
    public async Task A_Pending_Resume_Completed_After_Its_Connection_Closed_Does_Not_Take_Over_The_Session()
    {
        QuiclyPeer? pending = null;
        await using ServerFixture f = new(o =>
        {
            o.PeerOptions.AdmissionTimeout = TimeSpan.FromMilliseconds(300);
            o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
            {
                if (!c.IsResume)
                {
                    return AuthTokenDecision.Accept;
                }

                pending = c.Peer;
                return AuthTokenDecision.Pending;
            };
        });
        QuiclyPeer live = f.ConnectAdmitted();
        f.Resume(live);
        Assert.True(f.RunUntil(() => pending is not null));

        // The validation outlives the resume connection's admission timeout: the server closes that connection.
        Assert.True(f.RunUntil(() => pending!.State != PeerState.Handshaking, maxMicros: 2_000_000));
        Assert.Equal(PeerState.Closing, pending!.State);

        // The validation service answers now, for a connection that is gone.
        f.Server.CompleteAdmission(pending, accepted: true);
        f.Run(200_000);

        Assert.Equal(PeerState.Connected, live.State);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(0, statistics.SessionsResumed);
        Assert.Equal(0, statistics.SessionsReplaced);
    }

    /// <summary>
    /// The fresh-session variant of the finding above: a late accept for a connection that timed out creates a session
    /// that was never admitted (no <see cref="QuiclyServer.PeerAdmitted"/>), keeps it for the grace period and then
    /// raises <see cref="QuiclyServer.SessionEnded"/> for it.
    /// </summary>
    [Fact]
    public async Task A_Pending_Admission_Completed_After_Its_Connection_Closed_Creates_No_Session()
    {
        QuiclyPeer? pending = null;
        await using ServerFixture f = new(o =>
        {
            o.PeerOptions.AdmissionTimeout = TimeSpan.FromMilliseconds(300);
            o.Sessions.Grace = TimeSpan.FromSeconds(1);
            o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
            {
                pending = c.Peer;
                return AuthTokenDecision.Pending;
            };
        });
        f.Connect();
        Assert.True(f.RunUntil(() => pending is not null));
        Assert.True(f.RunUntil(() => pending!.State != PeerState.Handshaking, maxMicros: 2_000_000));
        Assert.Equal(PeerState.Closing, pending!.State);

        f.Server.CompleteAdmission(pending, accepted: true);
        f.Run(3_000_000, step: 10_000); // past the grace period

        Assert.Empty(f.Admitted);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(0, statistics.SessionsCreated);
        Assert.Equal(0, statistics.Sessions);
        Assert.Empty(f.Ended);
    }

    /// <summary>
    /// A server kick is a deliberate close and ends the session (the implementer's rule). While the kicked connection
    /// lingers (its Close frame is not yet delivered, up to <see cref="PeerOptions.CloseLinger"/>), a resume with the
    /// session's token is accepted: it "replaces" the closing connection, detaches the session from it and the session
    /// lives on at the next epoch, so a kicked player evades the kick by reconnecting at once.
    /// </summary>
    [Fact]
    public async Task A_Kicked_Session_Cannot_Be_Resumed_While_The_Kick_Lingers()
    {
        await using ServerFixture f = new();
        SimulatedConnector slow = new(f.Network, new LinkOptions { DelayMicros = 100_000 });
        QuiclyPeer player = f.ConnectAdmitted(connector: slow);
        QuiclyPeer kicked = f.ServerPeerOf(player);
        ulong session = player.SessionId;

        kicked.Close(new CloseReason(QuiclyErrorCode.NoError, "kicked"));
        QuiclyPeer evading = f.Resume(player); // a second connection, sent before the Close frame even arrived
        Assert.True(f.RunUntil(() => evading.State is PeerState.Connected or PeerState.Closed, maxMicros: 150_000));

        Assert.Equal(PeerState.Closed, evading.State);
        Assert.Equal(HelloStatus.Rejected, evading.HandshakeStatus);
        Assert.True(f.RunUntil(() => f.Ended.Count == 1, maxMicros: 3_000_000));
        Assert.Equal(session, f.Ended[0].SessionId);
        Assert.False(f.Ended[0].Expired);
    }

    /// <summary>
    /// The session's tag must follow a resume (AuthTokenContext and the resumed peer, as documented). When the resume
    /// replaces a connection that is still live (the client noticed the loss before the server did, the common case),
    /// the tag is taken from the session record, which still holds the value from session creation (0), and the live
    /// connection's tag is never copied back because ReplaceConnection detaches the session before that connection ends.
    /// </summary>
    [Fact]
    public async Task A_Resume_That_Replaces_A_Live_Connection_Keeps_The_Session_Tag()
    {
        ulong validatorTag = 0;
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
        {
            if (c.IsResume)
            {
                validatorTag = c.SessionTag;
            }

            return AuthTokenDecision.Accept;
        });
        f.Server.PeerAdmitted += peer =>
        {
            if (peer.Epoch == 1)
            {
                peer.Tag = 42;
            }
        };
        QuiclyPeer first = f.ConnectAdmitted();
        Assert.Equal(42UL, f.ServerPeerOf(first).Tag);

        QuiclyPeer second = f.Resume(first);
        Assert.True(f.RunUntil(() => second.State == PeerState.Connected && first.State == PeerState.Closed));

        Assert.Equal(42UL, validatorTag);
        AdmittedPeer resumed = f.Admitted[^1];
        Assert.Equal(2u, resumed.Epoch);
        Assert.Equal(42UL, resumed.Tag);
    }

    /// <summary>
    /// DisposeAsync must stop the HTTP side endpoint (and the certificate provisioning) whatever happens during the
    /// shutdown. When an event handler throws inside the StopAsync that DisposeAsync runs, StopAsync leaves before
    /// StopSideServicesAsync and DisposeAsync's finally never calls it: the TCP port stays bound after disposal.
    /// </summary>
    [Fact]
    public async Task Dispose_Stops_The_Http_Side_Endpoint_Even_When_A_Close_Handler_Throws()
    {
        await using ServerFixture f = new(o => o.Http = new ServerHttpOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, 0), EnableHealthEndpoint = true });
        IPEndPoint http = Assert.IsType<IPEndPoint>(f.Server.HttpEndPoint);
        f.ConnectAdmitted();
        f.Server.PeerClosed += (_, _) => throw new InvalidOperationException("close handler");

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await f.Server.DisposeAsync());

        using TcpClient probe = new();
        await Assert.ThrowsAnyAsync<SocketException>(async () => await probe.ConnectAsync(http.Address, http.Port, TestContext.Current.CancellationToken));
        Assert.Null(f.Server.HttpEndPoint);
    }

    /// <summary>
    /// ADR 0008 invariant 1: nothing the transport was given a pointer to may be reused before the matching completion.
    /// When StopAsync (or DisposeAsync) force-closes a peer that did not close in time, the server drops the peer's
    /// shared-lease reference before it has even closed the transport, so the block can return to the pool (and be
    /// rented and overwritten) while the transport may still read it. The reference must be held until the transport
    /// reported its close (the simulator delivers that at the next step).
    /// </summary>
    [Fact]
    public async Task A_Forced_Close_Keeps_The_Shared_Reference_Until_The_Transport_Reported_Its_Close()
    {
        await using ServerFixture f = new(o => o.ShutdownTimeout = TimeSpan.Zero);
        QuiclyPeer serverPeer = f.ServerPeerOf(f.ConnectAdmitted());
        f.Server.SharedPort = new AdmittingPort();
        PeerSet set = f.Server.CreateSet();
        set.Add(serverPeer);
        Assert.True(f.Server.Allocator.TryRent(64, out BufferLease block));
        SharedLease lease = f.Server.SharedLeases.Share(in block, 1);
        Assert.Equal(1, f.Server.SendShared(set, new SendHeader(2), lease, 64).AdmittedCount);
        Assert.Equal(2, f.Server.SharedLeases.GetReferenceCount(in lease));

        await f.Server.StopAsync(TestContext.Current.CancellationToken); // the peer is disposed, its transport not yet closed
        Assert.Equal(2, f.Server.SharedLeases.GetReferenceCount(in lease));

        f.Network.RunUntilIdle(1_000_000); // the transport reports its close
        f.Server.PollAll();
        Assert.Equal(1, f.Server.SharedLeases.GetReferenceCount(in lease));
        f.Server.SharedLeases.Release(in lease);
    }

    /// <summary>
    /// The work bit protocol has a lost wake-up on x86/x64 (TSO allows a store followed by a load of another location to
    /// be reordered). WorkSignalSink publishes the peer's work with a release store (SpscRing.TryEnqueue for a
    /// completion), then MarkWork reads the word with a plain load and skips the Interlocked.Or when the bit looks set.
    /// That load can be satisfied before the publishing store is visible; PollMarked's Interlocked.Exchange then clears
    /// the bit and its poll misses the work, and nothing marks the slot again: the work waits for the peer's next
    /// deadline (up to a ping interval) or its next transport callback. This test replays exactly that producer and
    /// PollMarked's consumer side (Volatile.Read check, Interlocked.Exchange, then a read of the published data) on two
    /// threads with the real <see cref="QuiclyServer.MarkWork"/> and work-bit array. Fix: make MarkWork's fast path
    /// fence (Interlocked.MemoryBarrier before the check) or always Interlocked.Or.
    /// </summary>
    [Fact]
    public async Task Work_Published_Before_MarkWork_Is_Never_Lost_To_A_Concurrent_Poll()
    {
        await using ServerFixture f = new();
        QuiclyServer server = f.Server;
        long[] words = (long[])typeof(QuiclyServer).GetField("_workBits", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(server)!;
        const int EntryLines = 4;
        long[] entry = new long[EntryLines * 16]; // the ring entry's cache lines (one long per 128 bytes)
        long[] published = new long[32]; // index 16 only: separate cache lines for the shared words
        long[] go = new long[32];
        long[] done = new long[32];
        long rounds = 0;
        long lost = 0;
        long firstLostRound = 0;

        Thread producer = new(() =>
        {
            uint random = 0x9E3779B9;
            for (long r = 1; ; r++)
            {
                long g;
                while ((g = Volatile.Read(ref go[16])) != r)
                {
                    if (g < 0)
                    {
                        return;
                    }

                    Thread.SpinWait(1);
                }

                Spin(ref random);

                // The transport thread writes the ring entry (lines the game thread read when it took the previous one),
                // then publishes the tail with a release store (SpscRing.TryEnqueue), then WorkSignalSink.Signal marks.
                for (int i = 0; i < EntryLines; i++)
                {
                    entry[i * 16] = r;
                }

                Volatile.Write(ref published[16], r);
                server.MarkWork(0);
                Volatile.Write(ref done[16], r);
            }
        }) { IsBackground = true, Name = "review producer" };

        Thread consumer = new(() =>
        {
            uint random = 12345;
            long deadline = Stopwatch.GetTimestamp() + Stopwatch.Frequency; // detects within ~0.2 s today; bounded cost once fixed
            for (long r = 1; r <= 5_000_000 && lost == 0 && Stopwatch.GetTimestamp() < deadline; r++)
            {
                Volatile.Write(ref words[0], 1); // an earlier signal of slot 0 is still waiting for PollAll
                Volatile.Write(ref go[16], r);
                Spin(ref random);
                long seen = 0;
                if (Volatile.Read(ref words[0]) != 0) // PollMarked
                {
                    long pending = Interlocked.Exchange(ref words[0], 0);
                    if ((pending & 1) != 0)
                    {
                        seen = Volatile.Read(ref published[16]); // PollSlot -> peer.Poll -> ring.TryDequeue reads the tail
                    }
                }

                while (Volatile.Read(ref done[16]) != r)
                {
                    Thread.SpinWait(1);
                }

                for (int i = 0; i < EntryLines; i++)
                {
                    _ = Volatile.Read(ref entry[i * 16]); // the poll reads the entry it dequeues
                }

                // Either the poll saw this round's work, or the bit is set again for the next PollAll.
                if (seen != r && (Volatile.Read(ref words[0]) & 1) == 0)
                {
                    lost++;
                    firstLostRound = r;
                }

                rounds = r;
            }

            Volatile.Write(ref go[16], -1);
        }) { IsBackground = true, Name = "review consumer" };

        producer.Start();
        consumer.Start();
        consumer.Join();
        producer.Join();
        words[0] = 0;
        Assert.True(lost == 0, "A wake-up was lost in round " + firstLostRound + " of " + rounds + ": the work was published before MarkWork, the poll did not see it and the slot's bit stayed clear.");

        static void Spin(ref uint state)
        {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            int spins = (int)(state % 16);
            for (int i = 0; i < spins; i++)
            {
                Thread.SpinWait(1);
            }
        }
    }

    /// <summary>A port whose peers admit every shared send (the step-1 placeholder engines refuse them).</summary>
    private sealed unsafe class AdmittingPort : ISharedSendPort
    {
        private int _sends;

        public SendResult Send(QuiclyPeer peer, in SendHeader header, byte* payload, int length, SendOptions options) =>
            new(SendStatus.Admitted, new SendToken(++_sends & 0xFFFF, 1));

        public bool TryRelease(QuiclyPeer peer, ref SharedEntry entry) => false;

        public void Abandon(QuiclyPeer peer, ref SharedEntry entry)
        {
        }
    }
}
