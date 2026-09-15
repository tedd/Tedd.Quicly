using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

public class SessionTests
{
    /// <summary>A connector whose links die <paramref name="afterMicros"/> after they are created (a lost connection).</summary>
    private static SimulatedConnector Cutting(ServerFixture f, long afterMicros) => new(f.Network, new LinkOptions { DisconnectAtMicros = afterMicros });

    [Fact]
    public async Task Resume_Within_Grace_Keeps_The_Session_And_Increments_The_Epoch()
    {
        bool seenResume = false;
        ulong seenSession = 0;
        ulong seenTag = 0;
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
        {
            if (c.IsResume)
            {
                seenResume = true;
                seenSession = c.SessionId;
                seenTag = c.SessionTag;
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
        QuiclyPeer first = f.ConnectAdmitted(connector: Cutting(f, 100_000));
        Assert.True(f.RunUntil(() => first.State == PeerState.Closed));
        Assert.Equal(CloseSource.Transport, first.CloseReason.Source);
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        Assert.Empty(f.Ended);

        QuiclyPeer second = f.Resume(first);
        Assert.True(f.RunUntil(() => second.State == PeerState.Connected));
        Assert.Equal(first.SessionId, second.SessionId);
        Assert.Equal(2u, second.Epoch);
        Assert.False(second.SessionToken.Span.SequenceEqual(first.SessionToken.Span));
        Assert.True(seenResume);
        Assert.Equal(first.SessionId, seenSession);
        Assert.Equal(42UL, seenTag);
        AdmittedPeer resumed = f.Admitted[^1];
        Assert.Equal(2u, resumed.Epoch);
        Assert.Equal(42UL, resumed.Tag);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(1, statistics.SessionsResumed);
        Assert.Equal(1, statistics.SessionsCreated);
        Assert.Equal(1, statistics.Sessions);
    }

    [Fact]
    public async Task Resume_Of_A_Live_Session_Replaces_Its_Connection()
    {
        await using ServerFixture f = new();
        QuiclyPeer first = f.ConnectAdmitted();
        QuiclyPeer second = f.Resume(first);
        Assert.True(f.RunUntil(() => second.State == PeerState.Connected && first.State == PeerState.Closed));
        Assert.Equal(QuiclyErrorCode.SessionReplaced, first.CloseReason.Code);
        Assert.Equal(CloseSource.Peer, first.CloseReason.Source);
        Assert.Equal(first.SessionId, second.SessionId);
        Assert.Equal(2u, second.Epoch);
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        Assert.Equal(QuiclyErrorCode.SessionReplaced, f.Closed[0].Reason.Code);
        Assert.Empty(f.Ended);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(1, statistics.SessionsReplaced);
        Assert.Equal(1, statistics.AdmittedPeers);
        Assert.Equal(1, statistics.Sessions);
    }

    [Fact]
    public async Task Resume_After_The_Grace_Period_Is_Rejected()
    {
        await using ServerFixture f = new(o => o.Sessions.Grace = TimeSpan.FromSeconds(1));
        QuiclyPeer first = f.ConnectAdmitted(connector: Cutting(f, 50_000));
        Assert.True(f.RunUntil(() => first.State == PeerState.Closed));
        Assert.True(f.RunUntil(() => f.Ended.Count == 1, maxMicros: 3_000_000));
        Assert.True(f.Ended[0].Expired);
        Assert.Equal(first.SessionId, f.Ended[0].SessionId);

        QuiclyPeer late = f.Resume(first);
        Assert.True(f.RunUntil(() => late.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, late.HandshakeStatus);
        Assert.Single(f.FailuresOf(AdmissionFailureReason.SessionUnknown)); // the registry's decision: the token is still valid
        Assert.Empty(f.FailuresOf(AdmissionFailureReason.SessionTokenExpired));
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(1, statistics.SessionsExpired);
        Assert.Equal(0, statistics.Sessions);
    }

    [Fact]
    public async Task Resume_Of_An_Ended_Session_With_A_Live_Token_Is_Unknown()
    {
        await using ServerFixture f = new(o =>
        {
            o.Sessions.Grace = TimeSpan.FromMilliseconds(300);
            o.Sessions.TokenLifetime = TimeSpan.FromHours(1);
        });
        QuiclyPeer first = f.ConnectAdmitted(connector: Cutting(f, 50_000));
        Assert.True(f.RunUntil(() => f.Ended.Count == 1, maxMicros: 2_000_000));
        QuiclyPeer late = f.Resume(first);
        Assert.True(f.RunUntil(() => late.State == PeerState.Closed));
        Assert.Single(f.FailuresOf(AdmissionFailureReason.SessionUnknown));
    }

    [Fact]
    public async Task Resume_Judged_After_The_Grace_Ended_But_Before_The_Sweep_Is_Rejected()
    {
        await using ServerFixture f = new(o =>
        {
            o.Sessions.Grace = TimeSpan.FromMilliseconds(500);
            o.Sessions.TokenLifetime = TimeSpan.FromHours(1);
        });
        QuiclyPeer first = f.ConnectAdmitted(connector: Cutting(f, 50_000));
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));

        // Let the grace run out without polling the server, so the registry has not swept the session yet.
        f.Network.AdvanceTo(f.Network.NowMicros + 600_000);
        QuiclyPeer late = f.Resume(first);
        for (int i = 0; i < 20; i++)
        {
            f.Step(1_000);
        }

        f.Server.PollAll(); // judges the Hello, then sweeps
        Assert.Single(f.FailuresOf(AdmissionFailureReason.SessionExpired));
        Assert.True(f.RunUntil(() => late.State == PeerState.Closed));
        Assert.Single(f.Ended);
    }

    [Fact]
    public async Task A_Consumed_Token_Cannot_Be_Replayed()
    {
        await using ServerFixture f = new();
        QuiclyPeer first = f.ConnectAdmitted();
        byte[] token = first.SessionToken.ToArray();
        QuiclyPeer second = f.Resume(first);
        Assert.True(f.RunUntil(() => second.State == PeerState.Connected));
        QuiclyPeer replay = f.Connect(sessionToken: token, lastEpoch: 1);
        Assert.True(f.RunUntil(() => replay.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, replay.HandshakeStatus);
        Assert.Single(f.FailuresOf(AdmissionFailureReason.SessionTokenReplayed));
        Assert.Equal(PeerState.Connected, second.State);
    }

    [Fact]
    public async Task Malformed_Forged_Unknown_And_Superseded_Tokens_Are_Rejected()
    {
        await using ServerFixture f = new();
        QuiclyPeer live = f.ConnectAdmitted();
        byte[] forged = live.SessionToken.ToArray();
        forged[^1] ^= 0xFF;
        byte[] unknown = f.Server.MintToken(0x1234_5678, 1, f.Clock.NowMicros);
        byte[] superseded = f.Server.MintToken(live.SessionId, 7, f.Clock.NowMicros);
        (byte[] Token, AdmissionFailureReason Reason)[] cases =
        [
            (new byte[10], AdmissionFailureReason.SessionTokenMalformed),
            (forged, AdmissionFailureReason.SessionTokenBadSignature),
            (unknown, AdmissionFailureReason.SessionUnknown),
            (superseded, AdmissionFailureReason.SessionTokenSuperseded),
        ];
        foreach ((byte[] token, AdmissionFailureReason reason) in cases)
        {
            QuiclyPeer client = f.Connect(sessionToken: token, lastEpoch: 1);
            Assert.True(f.RunUntil(() => client.State == PeerState.Closed));
            Assert.Equal(HelloStatus.Rejected, client.HandshakeStatus);
            AdmissionFailure failure = Assert.Single(f.FailuresOf(reason));
            Assert.True(failure.IsTokenFailure);
        }

        Assert.Equal(PeerState.Connected, live.State);
    }

    [Fact]
    public async Task Resumes_Beyond_The_Burst_Wait_For_The_Minimum_Interval_Uncharged_And_Keep_The_Token()
    {
        // The production resume rate (MinResumeInterval 1 s, ResumeBurst 3); a single charged failure would block the address.
        await using ServerFixture f = new(o => o.Admission.AuthFailureBurst = 1);
        Assert.Equal(TimeSpan.FromSeconds(1), f.Options.Admission.MinResumeInterval);
        Assert.Equal(3, f.Options.Admission.ResumeBurst);
        QuiclyPeer current = f.ConnectAdmitted();
        for (int i = 0; i < 3; i++)
        {
            QuiclyPeer next = f.Resume(current); // a connection that flaps right after its admission
            Assert.True(f.RunUntil(() => next.State == PeerState.Connected));
            current = next;
        }

        QuiclyPeer tooSoon = f.Resume(current);
        Assert.True(f.RunUntil(() => tooSoon.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, tooSoon.HandshakeStatus);
        Assert.False(Assert.Single(f.FailuresOf(AdmissionFailureReason.ResumeTooSoon)).IsTokenFailure);
        Assert.Equal(PeerState.Connected, current.State);

        f.Run(1_000_000, step: 10_000);
        QuiclyPeer later = f.Resume(current); // the refused resume did not spend the token
        Assert.True(f.RunUntil(() => later.State == PeerState.Connected));
        Assert.Equal(current.SessionId, later.SessionId);
        Assert.Equal(5u, later.Epoch);
        Assert.Empty(f.FailuresOf(AdmissionFailureReason.AddressRateLimited));
    }

    [Fact]
    public async Task A_Goodbye_Or_A_Kick_Ends_The_Session()
    {
        await using ServerFixture f = new();
        QuiclyPeer leaving = f.ConnectAdmitted();
        leaving.Close();
        Assert.True(f.RunUntil(() => f.Ended.Count == 1));
        Assert.False(f.Ended[0].Expired);
        QuiclyPeer back = f.Resume(leaving);
        Assert.True(f.RunUntil(() => back.State == PeerState.Closed));
        Assert.Single(f.FailuresOf(AdmissionFailureReason.SessionUnknown));

        QuiclyPeer kicked = f.ConnectAdmitted();
        f.ServerPeerOf(kicked).Close(new CloseReason(QuiclyErrorCode.NoError, "kicked"));
        Assert.True(f.RunUntil(() => f.Ended.Count == 2 && kicked.State == PeerState.Closed));
        Assert.Equal("kicked", kicked.CloseReason.Reason);
    }

    [Fact]
    public async Task Zero_Grace_Ends_Sessions_With_Their_Connection()
    {
        await using ServerFixture f = new(o => o.Sessions.Grace = TimeSpan.Zero);
        QuiclyPeer first = f.ConnectAdmitted(connector: Cutting(f, 50_000));
        Assert.True(f.RunUntil(() => f.Ended.Count == 1));
        Assert.False(f.Ended[0].Expired);
        Assert.Equal(first.SessionId, f.Ended[0].SessionId);
    }

    [Fact]
    public async Task A_Heartbeat_Timeout_Keeps_The_Session_For_Its_Grace()
    {
        await using ServerFixture f = new(o => o.PeerOptions.HeartbeatTimeout = TimeSpan.FromMilliseconds(300));
        SimulatedConnector silent = new(f.Network, new LinkOptions { LossPercent = 100 });
        QuiclyPeer first = f.ConnectAdmitted(connector: silent);
        Assert.True(f.RunUntil(() => f.Closed.Count == 1, maxMicros: 3_000_000));
        Assert.Equal(QuiclyErrorCode.Timeout, f.Closed[0].Reason.Code);
        Assert.Empty(f.Ended);
        QuiclyPeer resumed = f.Resume(first);
        Assert.True(f.RunUntil(() => resumed.State == PeerState.Connected));
        Assert.Equal(first.SessionId, resumed.SessionId);
    }

    [Fact]
    public async Task A_Resume_Refused_For_Capacity_Keeps_Its_Token_Usable()
    {
        await using ServerFixture f = new(o =>
        {
            o.MaxPeers = 1;
            o.Admission.MaxUnadmittedConnections = 4;
        });
        QuiclyPeer first = f.ConnectAdmitted(connector: Cutting(f, 50_000));
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        QuiclyPeer other = f.ConnectAdmitted();
        QuiclyPeer refused = f.Resume(first);
        Assert.True(f.RunUntil(() => refused.State == PeerState.Closed));
        Assert.Equal(HelloStatus.ServerFull, refused.HandshakeStatus);
        Assert.Single(f.FailuresOf(AdmissionFailureReason.ServerFull));

        other.Close();
        Assert.True(f.RunUntil(() => f.Server.AdmittedCount == 0 && f.Server.PeerCount == 0));
        QuiclyPeer resumed = f.Resume(first);
        Assert.True(f.RunUntil(() => resumed.State == PeerState.Connected));
        Assert.Equal(first.SessionId, resumed.SessionId);
    }

    [Fact]
    public async Task A_Resume_That_Replaces_A_Connection_May_Exceed_Capacity()
    {
        await using ServerFixture f = new(o =>
        {
            o.MaxPeers = 1;
            o.Admission.MaxUnadmittedConnections = 4;
        });
        QuiclyPeer first = f.ConnectAdmitted();
        QuiclyPeer second = f.Resume(first);
        Assert.True(f.RunUntil(() => second.State == PeerState.Connected));
        QuiclyPeer third = f.Connect();
        Assert.True(f.RunUntil(() => third.State == PeerState.Closed));
        Assert.Equal(HelloStatus.ServerFull, third.HandshakeStatus);
    }

    [Fact]
    public async Task A_Full_Replay_Cache_Fails_Closed()
    {
        await using ServerFixture f = new(o => o.Sessions.ReplayCacheCapacity = 1);
        QuiclyPeer a = f.ConnectAdmitted();
        QuiclyPeer b = f.ConnectAdmitted();
        QuiclyPeer resumedA = f.Resume(a);
        Assert.True(f.RunUntil(() => resumedA.State == PeerState.Connected));
        QuiclyPeer resumedB = f.Resume(b);
        Assert.True(f.RunUntil(() => resumedB.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, resumedB.HandshakeStatus);
        Assert.Single(f.FailuresOf(AdmissionFailureReason.SessionTokenReplayCacheFull));
        Assert.Equal(PeerState.Connected, b.State);
    }

    [Fact]
    public async Task Rotating_The_Key_Keeps_Issued_Tokens_Valid()
    {
        byte[] key = new byte[32];
        for (int i = 0; i < key.Length; i++)
        {
            key[i] = (byte)(i + 1);
        }

        await using ServerFixture f = new(o => o.Sessions.Key = key);
        QuiclyPeer first = f.ConnectAdmitted(connector: Cutting(f, 50_000));
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        f.Server.RotateSessionKey(new byte[32]);
        QuiclyPeer resumed = f.Resume(first);
        Assert.True(f.RunUntil(() => resumed.State == PeerState.Connected));
        Assert.Throws<ArgumentException>(() => f.Server.RotateSessionKey(new byte[5]));
    }

    [Fact]
    public async Task Pending_Validation_Of_A_Resume_Commits_When_Accepted()
    {
        QuiclyPeer? pending = null;
        await using ServerFixture f = new(o => o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
        {
            if (!c.IsResume)
            {
                return AuthTokenDecision.Accept;
            }

            pending = c.Peer;
            return AuthTokenDecision.Pending;
        });
        QuiclyPeer first = f.ConnectAdmitted();
        QuiclyPeer second = f.Resume(first);
        Assert.True(f.RunUntil(() => pending is not null));
        f.Server.CompleteAdmission(pending!, accepted: true);
        Assert.True(f.RunUntil(() => second.State == PeerState.Connected && first.State == PeerState.Closed));
        Assert.Equal(2u, second.Epoch);
    }

    [Fact]
    public async Task Pending_Resume_Whose_Session_Ended_Meanwhile_Is_Rejected()
    {
        QuiclyPeer? pending = null;
        await using ServerFixture f = new(o =>
        {
            o.Sessions.Grace = TimeSpan.FromMilliseconds(300);
            o.Sessions.TokenLifetime = TimeSpan.FromHours(1);
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
        QuiclyPeer first = f.ConnectAdmitted(connector: Cutting(f, 50_000));
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        QuiclyPeer second = f.Resume(first);
        Assert.True(f.RunUntil(() => pending is not null));
        Assert.True(f.RunUntil(() => f.Ended.Count == 1, maxMicros: 2_000_000));
        f.Server.CompleteAdmission(pending!, accepted: true);
        Assert.True(f.RunUntil(() => second.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, second.HandshakeStatus);
        Assert.Single(f.FailuresOf(AdmissionFailureReason.SessionUnknown));
    }
}
