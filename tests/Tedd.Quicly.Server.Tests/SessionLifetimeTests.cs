using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

/// <summary>
/// Session-token expiry is a registry decision (PROTOCOL.md §4.1): a token carries a long maximum age
/// (<see cref="ServerSessionOptions.TokenLifetime"/>), and the session registry enforces the grace period from the moment the
/// session's connection was lost. Plus the end of the sessions still waiting when the server stops.
/// </summary>
public class SessionLifetimeTests
{
    private static SimulatedConnector Cutting(ServerFixture f, long afterMicros) => new(f.Network, new LinkOptions { DisconnectAtMicros = afterMicros });

    [Fact]
    public void Tokens_Live_A_Day_By_Default()
    {
        ServerSessionOptions options = new();
        Assert.Equal(TimeSpan.FromHours(24), options.TokenLifetime);
        Assert.Equal(TimeSpan.FromSeconds(30), options.Grace);
        ServerAdmissionOptions admission = new();
        Assert.Equal(TimeSpan.FromSeconds(1), admission.MinResumeInterval);
        Assert.Equal(3, admission.ResumeBurst);
    }

    [Fact]
    public async Task A_Live_Session_Resumes_Long_After_Its_Grace_Period()
    {
        await using ServerFixture f = new(o => o.Sessions.Grace = TimeSpan.FromSeconds(1));
        QuiclyPeer first = f.ConnectAdmitted();
        f.Run(5_000_000, step: 10_000); // five grace periods on a live connection
        QuiclyPeer second = f.Resume(first);
        Assert.True(f.RunUntil(() => second.State == PeerState.Connected && first.State == PeerState.Closed));
        Assert.Equal(first.SessionId, second.SessionId);
        Assert.Equal(2u, second.Epoch);
    }

    [Fact]
    public async Task The_Grace_Period_Runs_From_The_Loss_Of_The_Connection()
    {
        await using ServerFixture f = new(o => o.Sessions.Grace = TimeSpan.FromSeconds(1));
        QuiclyPeer first = f.ConnectAdmitted(connector: Cutting(f, 3_000_000)); // lost three grace periods after its admission
        Assert.True(f.RunUntil(() => f.Closed.Count == 1, maxMicros: 5_000_000));
        f.Run(500_000, step: 10_000);
        QuiclyPeer second = f.Resume(first, connector: Cutting(f, 100_000)); // half a grace period after the loss
        Assert.True(f.RunUntil(() => second.State == PeerState.Connected));
        Assert.Equal(first.SessionId, second.SessionId);

        Assert.True(f.RunUntil(() => f.Closed.Count == 2)); // lost again
        Assert.True(f.RunUntil(() => f.Ended.Count == 1, maxMicros: 2_000_000)); // one grace period after that loss it ends
        Assert.True(f.Ended[0].Expired);
        QuiclyPeer late = f.Resume(second);
        Assert.True(f.RunUntil(() => late.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, late.HandshakeStatus);
        Assert.Single(f.FailuresOf(AdmissionFailureReason.SessionUnknown));
    }

    [Fact]
    public async Task A_Token_Older_Than_Its_Lifetime_Cannot_Resume_Even_A_Live_Session()
    {
        await using ServerFixture f = new(o =>
        {
            o.Sessions.Grace = TimeSpan.FromSeconds(1);
            o.Sessions.TokenLifetime = TimeSpan.FromSeconds(2);
        });
        QuiclyPeer first = f.ConnectAdmitted();
        f.Run(2_500_000, step: 10_000);
        QuiclyPeer late = f.Resume(first);
        Assert.True(f.RunUntil(() => late.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, late.HandshakeStatus);
        Assert.Single(f.FailuresOf(AdmissionFailureReason.SessionTokenExpired));
        Assert.Equal(PeerState.Connected, first.State);
    }

    [Fact]
    public async Task Sessions_Still_Waiting_For_A_Resume_End_When_The_Server_Stops()
    {
        await using ServerFixture f = new(o => o.ShutdownTimeout = TimeSpan.Zero);
        QuiclyPeer lost = f.ConnectAdmitted(connector: Cutting(f, 50_000));
        QuiclyPeer live = f.ConnectAdmitted();
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        Assert.Empty(f.Ended);

        await f.Server.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, f.Ended.Count);
        Assert.True(f.Ended.Single(e => e.SessionId == lost.SessionId).Expired); // it waited for a resume that never came
        Assert.False(f.Ended.Single(e => e.SessionId == live.SessionId).Expired); // closed by the shutdown
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(0, statistics.Sessions);
        Assert.Equal(0, statistics.SessionsExpired);
    }

    [Fact]
    public async Task A_Throwing_SessionEnded_Handler_Does_Not_Drop_The_Other_Ends()
    {
        await using ServerFixture f = new(o => o.Sessions.Grace = TimeSpan.FromMilliseconds(200));
        List<ulong> raised = [];
        bool throwOnce = true;
        f.Server.SessionEnded += info =>
        {
            raised.Add(info.SessionId);
            if (throwOnce)
            {
                throwOnce = false;
                throw new InvalidOperationException("handler");
            }
        };
        f.ConnectAdmitted(connector: Cutting(f, 50_000));
        f.ConnectAdmitted(connector: Cutting(f, 50_000));
        Assert.True(f.RunUntil(() => f.Closed.Count == 2));

        f.Network.Advance(300_000); // both grace periods end before the next PollAll: one sweep ends both sessions
        Assert.Throws<InvalidOperationException>(() => f.Server.PollAll());
        Assert.Single(raised);
        f.Server.PollAll(); // the other end stayed queued for this call
        Assert.Equal(2, raised.Count);
        Assert.NotEqual(raised[0], raised[1]);
        Assert.Equal(2, f.Ended.Count);
        f.Server.PollAll();
        Assert.Equal(2, raised.Count);
    }

    [Fact]
    public async Task A_Throwing_SessionEnded_Handler_At_Stop_Still_Ends_Every_Session()
    {
        await using ServerFixture f = new(o => o.ShutdownTimeout = TimeSpan.Zero);
        f.ConnectAdmitted(connector: Cutting(f, 50_000));
        f.ConnectAdmitted(connector: Cutting(f, 50_000));
        Assert.True(f.RunUntil(() => f.Closed.Count == 2));
        f.Server.SessionEnded += _ => throw new InvalidOperationException("handler");

        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Server.StopAsync(TestContext.Current.CancellationToken));
        Assert.Equal(2, f.Ended.Count);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(0, statistics.Sessions);
        Assert.False(f.Server.IsRunning);
    }
}
