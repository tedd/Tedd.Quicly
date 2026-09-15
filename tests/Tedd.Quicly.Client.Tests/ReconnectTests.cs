using System.Net.Sockets;
using System.Reflection;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Server;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Client.Tests;

public class ReconnectTests
{
    private static ReconnectPolicy Policy(int attempts = 3, int initialMillis = 100) => new()
    {
        MaxAttempts = attempts,
        InitialDelay = TimeSpan.FromMilliseconds(initialMillis),
        MaxDelay = TimeSpan.FromSeconds(5),
        Jitter = 0,
    };

    [Fact]
    public async Task A_Lost_Connection_Is_Resumed_With_The_Same_Session()
    {
        await using ClientFixture f = new();
        ScriptedConnector connector = new ScriptedConnector(f.Connector, f.Clock).Then(f.Cutting(200_000));
        QuiclyClient client = f.CreateClient(connector);
        List<ReconnectingInfo> reconnecting = [];
        List<ReconnectedInfo> reconnected = [];
        List<(PeerState From, PeerState To)> states = [];
        client.Reconnecting += (_, info) => reconnecting.Add(info);
        client.Reconnected += (_, info) => reconnected.Add(info);
        client.StateChanged += (_, from, to) => states.Add((from, to));
        List<QuiclyPeer> created = [];
        ClientOptions options = f.Options("alice", Policy());
        options.PeerCreated = created.Add;
        QuiclyPeer first = await client.ConnectAsync(f.EndPoint, options, TestContext.Current.CancellationToken);

        Assert.True(f.RunUntil(client, () => client.State == PeerState.Reconnecting));
        long lostAt = f.Clock.NowMicros;
        ReconnectingInfo scheduled = Assert.Single(reconnecting);
        Assert.Equal(1, scheduled.Attempt);
        Assert.Equal(TimeSpan.FromMilliseconds(100), scheduled.Delay);
        Assert.Equal(CloseSource.Transport, scheduled.LastReason.Source);
        Assert.Same(first, client.Peer);
        Assert.Equal(1, client.ReconnectAttempt);

        Assert.True(f.RunUntil(client, () => client.State == PeerState.Connected));
        Assert.True(connector.ConnectTimes[1] - lostAt >= 99_000, "The attempt did not wait for the back-off.");
        ReconnectedInfo info = Assert.Single(reconnected);
        Assert.Same(first, info.PreviousPeer);
        Assert.Same(client.Peer, info.Peer);
        Assert.True(info.Resumed);
        Assert.Equal(1, info.Attempts);
        Assert.Equal(first.SessionId, info.Peer.SessionId);
        Assert.Equal(2u, info.Peer.Epoch);
        Assert.Equal(0, client.ReconnectAttempt);
        Assert.Equal(2, created.Count);
        Assert.Equal((first.SessionId, 2u), f.ServerAdmitted[^1]);
        Assert.Contains((PeerState.Connected, PeerState.Reconnecting), states);
        Assert.Equal((PeerState.Reconnecting, PeerState.Connected), states[^1]);
        Assert.Throws<ObjectDisposedException>(() => first.Poll());
    }

    [Fact]
    public async Task Back_Off_Grows_And_The_Attempts_Are_Bounded()
    {
        await using ClientFixture f = new();
        ScriptedConnector connector = new ScriptedConnector(f.Unreachable(), f.Clock).Then(f.Connector);
        QuiclyClient client = f.CreateClient(connector);
        List<ReconnectingInfo> reconnecting = [];
        List<CloseReason> disconnected = [];
        client.Reconnecting += (_, info) => reconnecting.Add(info);
        client.Disconnected += (_, reason) => disconnected.Add(reason);
        QuiclyPeer peer = await client.ConnectAsync(f.EndPoint, f.Options(reconnect: Policy()), TestContext.Current.CancellationToken);

        f.ServerPeerOf(peer).Close(new CloseReason(QuiclyErrorCode.Timeout, "lost you")); // a timeout is reconnectable
        Assert.True(f.RunUntil(client, () => client.State == PeerState.Closed, maxMicros: 5_000_000));
        Assert.Equal(new[] { 100, 200, 400 }, reconnecting.Select(r => (int)r.Delay.TotalMilliseconds).ToArray());
        Assert.Equal(new[] { 1, 2, 3 }, reconnecting.Select(r => r.Attempt).ToArray());
        CloseReason last = Assert.Single(disconnected);
        Assert.Equal(SimulatedTransport.StatusUnreachable, last.TransportStatus);
        Assert.Equal(0, client.ReconnectAttempt);
        Assert.Equal(4, connector.Connects);
        Assert.Same(peer, client.Peer);
    }

    [Fact]
    public async Task A_Refused_Resume_Falls_Back_To_A_New_Session()
    {
        await using ClientFixture f = new(o => o.Sessions.Grace = TimeSpan.FromMilliseconds(50));
        ScriptedConnector connector = new ScriptedConnector(f.Connector, f.Clock).Then(f.Cutting(100_000));
        QuiclyClient client = f.CreateClient(connector);
        List<ReconnectedInfo> reconnected = [];
        client.Reconnected += (_, info) => reconnected.Add(info);
        QuiclyPeer first = await client.ConnectAsync(f.EndPoint, f.Options(reconnect: Policy(initialMillis: 300)), TestContext.Current.CancellationToken);
        Assert.True(f.RunUntil(client, () => reconnected.Count == 1, maxMicros: 5_000_000));
        ReconnectedInfo info = reconnected[0];
        Assert.False(info.Resumed);
        Assert.NotEqual(first.SessionId, info.Peer.SessionId);
        Assert.Equal(1u, info.Peer.Epoch);
        Assert.Equal(2, info.Attempts);
    }

    [Fact]
    public async Task A_Refused_Resume_Without_Fallback_Gives_Up()
    {
        await using ClientFixture f = new(o => o.Sessions.Grace = TimeSpan.FromMilliseconds(50));
        ScriptedConnector connector = new ScriptedConnector(f.Connector, f.Clock).Then(f.Cutting(100_000));
        QuiclyClient client = f.CreateClient(connector);
        List<CloseReason> disconnected = [];
        client.Disconnected += (_, reason) => disconnected.Add(reason);
        ReconnectPolicy policy = Policy(initialMillis: 300);
        policy.FallBackToNewSession = false;
        await client.ConnectAsync(f.EndPoint, f.Options(reconnect: policy), TestContext.Current.CancellationToken);
        Assert.True(f.RunUntil(client, () => client.State == PeerState.Closed, maxMicros: 5_000_000));
        Assert.Equal(QuiclyErrorCode.AdmissionRejected, Assert.Single(disconnected).Code);
    }

    [Fact]
    public async Task Without_Resume_A_Reconnect_Starts_A_Fresh_Session()
    {
        await using ClientFixture f = new();
        ScriptedConnector connector = new ScriptedConnector(f.Connector, f.Clock).Then(f.Cutting(100_000));
        QuiclyClient client = f.CreateClient(connector);
        List<ReconnectedInfo> reconnected = [];
        client.Reconnected += (_, info) => reconnected.Add(info);
        ReconnectPolicy policy = Policy();
        policy.ResumeSession = false;
        QuiclyPeer first = await client.ConnectAsync(f.EndPoint, f.Options(reconnect: policy), TestContext.Current.CancellationToken);
        Assert.True(f.RunUntil(client, () => reconnected.Count == 1, maxMicros: 5_000_000));
        Assert.False(reconnected[0].Resumed);
        Assert.NotEqual(first.SessionId, reconnected[0].Peer.SessionId);
    }

    [Fact]
    public async Task A_Server_That_Refuses_Everyone_Makes_The_Client_Give_Up()
    {
        bool refuse = false;
        await using ClientFixture f = new(o => o.Admission.AuthTokenValidator = (in AuthTokenContext _) => refuse ? AuthTokenDecision.Reject : AuthTokenDecision.Accept);
        ScriptedConnector connector = new ScriptedConnector(f.Connector, f.Clock).Then(f.Cutting(100_000));
        QuiclyClient client = f.CreateClient(connector);
        List<CloseReason> disconnected = [];
        client.Disconnected += (_, reason) => disconnected.Add(reason);
        await client.ConnectAsync(f.EndPoint, f.Options(reconnect: Policy(attempts: 5)), TestContext.Current.CancellationToken);
        refuse = true;
        Assert.True(f.RunUntil(client, () => client.State == PeerState.Closed, maxMicros: 5_000_000));
        Assert.Single(disconnected);
        Assert.Equal(3, connector.Connects); // the resume is refused, then the fresh session too: no further attempts
    }

    [Fact]
    public async Task Deliberate_Closes_Are_Not_Reconnected()
    {
        await using ClientFixture f = new();
        QuiclyClient kicked = f.CreateClient();
        List<ReconnectingInfo> reconnecting = [];
        List<CloseReason> disconnected = [];
        kicked.Reconnecting += (_, info) => reconnecting.Add(info);
        kicked.Disconnected += (_, reason) => disconnected.Add(reason);
        QuiclyPeer peer = await kicked.ConnectAsync(f.EndPoint, f.Options(reconnect: Policy()), TestContext.Current.CancellationToken);
        f.ServerPeerOf(peer).Close(new CloseReason(QuiclyErrorCode.NoError, "kicked"));
        Assert.True(f.RunUntil(kicked, () => kicked.State == PeerState.Closed));
        Assert.Empty(reconnecting);
        Assert.Equal("kicked", Assert.Single(disconnected).Reason);

        // Another connection resumed the session: the replaced client does not fight back.
        QuiclyClient replaced = f.CreateClient();
        QuiclyPeer original = await replaced.ConnectAsync(f.EndPoint, f.Options(reconnect: Policy()), TestContext.Current.CancellationToken);
        QuiclyClient thief = f.CreateClient();
        ClientOptions stolen = f.Options();
        stolen.PeerOptions.SessionToken = original.SessionToken.ToArray();
        stolen.PeerOptions.LastEpoch = original.Epoch;
        await thief.ConnectAsync(f.EndPoint, stolen, TestContext.Current.CancellationToken);
        Assert.True(f.RunUntil(replaced, () => replaced.State == PeerState.Closed));
        Assert.Equal(QuiclyErrorCode.SessionReplaced, replaced.Peer!.CloseReason.Code);
    }

    [Fact]
    public async Task A_Custom_Rule_Can_Reconnect_After_Any_Close()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient();
        ReconnectPolicy policy = Policy();
        policy.ShouldReconnect = _ => true;
        policy.Random = new Random(7);
        policy.Jitter = 0.5;
        List<ReconnectingInfo> reconnecting = [];
        client.Reconnecting += (_, info) => reconnecting.Add(info);
        QuiclyPeer peer = await client.ConnectAsync(f.EndPoint, f.Options(reconnect: policy), TestContext.Current.CancellationToken);
        f.ServerPeerOf(peer).Close(new CloseReason(QuiclyErrorCode.NoError, "kicked"));
        Assert.True(f.RunUntil(client, () => client.State == PeerState.Reconnecting));
        TimeSpan delay = Assert.Single(reconnecting).Delay;
        Assert.InRange(delay, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(150));
        Assert.True(f.RunUntil(client, () => client.State == PeerState.Connected));
    }

    [Fact]
    public async Task Close_Stops_A_Reconnect_In_Progress()
    {
        await using ClientFixture f = new();
        ScriptedConnector connector = new ScriptedConnector(f.Unreachable(), f.Clock).Then(f.Connector);
        QuiclyClient client = f.CreateClient(connector);
        List<CloseReason> disconnected = [];
        client.Disconnected += (_, reason) => disconnected.Add(reason);
        QuiclyPeer peer = await client.ConnectAsync(f.EndPoint, f.Options(reconnect: Policy(attempts: 10)), TestContext.Current.CancellationToken);
        f.ServerPeerOf(peer).Close(new CloseReason(QuiclyErrorCode.Timeout));
        Assert.True(f.RunUntil(client, () => connector.Connects == 2)); // the first attempt is under way
        client.Flush(); // flushes the attempt
        client.Close(new CloseReason(QuiclyErrorCode.NoError, "quit"));
        Assert.Equal(PeerState.Closed, client.State);
        Assert.Equal("quit", Assert.Single(disconnected).Reason);
        f.Run(client, 2_000_000, step: 10_000);
        Assert.Equal(2, connector.Connects);
    }

    [Fact]
    public async Task A_Connector_That_Throws_Counts_As_A_Failed_Attempt()
    {
        await using ClientFixture f = new();
        ScriptedConnector connector = new ScriptedConnector(f.Connector, f.Clock).Then(f.Cutting(100_000));
        QuiclyClient client = f.CreateClient(connector);
        List<ReconnectingInfo> reconnecting = [];
        client.Reconnecting += (_, info) => reconnecting.Add(info);
        await client.ConnectAsync(f.EndPoint, f.Options(reconnect: Policy()), TestContext.Current.CancellationToken);
        connector.ThrowNext = new SocketException((int)SocketError.HostNotFound);
        Assert.True(f.RunUntil(client, () => reconnecting.Count == 2, maxMicros: 5_000_000));
        Assert.Equal(QuiclyErrorCode.InternalError, reconnecting[1].LastReason.Code);
        Assert.True(f.RunUntil(client, () => client.State == PeerState.Connected, maxMicros: 5_000_000));
    }

    [Fact]
    public async Task Later_Attempts_Present_The_Updated_Auth_Token()
    {
        List<string> tokens = [];
        await using ClientFixture f = new(o => o.Admission.AuthTokenValidator = (in AuthTokenContext c) =>
        {
            tokens.Add(System.Text.Encoding.UTF8.GetString(c.AuthToken));
            return AuthTokenDecision.Accept;
        });
        ScriptedConnector connector = new ScriptedConnector(f.Connector, f.Clock).Then(f.Cutting(100_000));
        QuiclyClient client = f.CreateClient(connector);
        client.Reconnecting += (c, _) => c.AuthToken = "renewed"u8.ToArray();
        await client.ConnectAsync(f.EndPoint, f.Options("first", Policy()), TestContext.Current.CancellationToken);
        Assert.True(f.RunUntil(client, () => client.State == PeerState.Reconnecting));
        Assert.True(f.RunUntil(client, () => client.State == PeerState.Connected));
        Assert.Equal(new[] { "first", "renewed" }, tokens);
    }

    [Fact]
    public async Task Zero_Attempts_Or_No_Policy_Never_Reconnect()
    {
        await using ClientFixture f = new();
        foreach (ReconnectPolicy? policy in new[] { null, Policy(attempts: 0) })
        {
            QuiclyClient client = f.CreateClient(new ScriptedConnector(f.Connector, f.Clock).Then(f.Cutting(100_000)));
            List<CloseReason> disconnected = [];
            client.Disconnected += (_, reason) => disconnected.Add(reason);
            await client.ConnectAsync(f.EndPoint, f.Options(reconnect: policy), TestContext.Current.CancellationToken);
            Assert.True(f.RunUntil(client, () => client.State == PeerState.Closed));
            Assert.Equal(CloseSource.Transport, Assert.Single(disconnected).Source);
        }
    }

    [Fact]
    public void Delays_Follow_The_Formula()
    {
        ReconnectPolicy policy = new() { InitialDelay = TimeSpan.FromMilliseconds(100), MaxDelay = TimeSpan.FromSeconds(1), Multiplier = 3, Jitter = 0.5 };
        Assert.Equal(TimeSpan.FromMilliseconds(100), policy.GetDelay(1, 0.5));
        Assert.Equal(TimeSpan.FromMilliseconds(300), policy.GetDelay(2, 0.5));
        Assert.Equal(TimeSpan.FromMilliseconds(900), policy.GetDelay(3, 0.5));
        Assert.Equal(TimeSpan.FromSeconds(1), policy.GetDelay(4, 0.5));
        Assert.Equal(TimeSpan.FromSeconds(1), policy.GetDelay(1000, 0.5));
        Assert.Equal(TimeSpan.FromMilliseconds(50), policy.GetDelay(1, 0));
        Assert.Equal(TimeSpan.FromMilliseconds(150), policy.GetDelay(1, 1));
        Assert.Equal(TimeSpan.FromSeconds(1), policy.GetDelay(4, 1)); // jitter never exceeds the cap
        Assert.Throws<ArgumentOutOfRangeException>(() => policy.GetDelay(0, 0.5));
        Assert.Throws<ArgumentOutOfRangeException>(() => policy.GetDelay(1, 1.5));
        Assert.Throws<ArgumentOutOfRangeException>(() => policy.GetDelay(1, double.NaN));
        Assert.True(ReconnectPolicy.IsReconnectable(new CloseReason(QuiclyErrorCode.NoError) { Source = CloseSource.Transport }));
        Assert.True(ReconnectPolicy.IsReconnectable(new CloseReason(QuiclyErrorCode.Timeout) { Source = CloseSource.Peer }));
        Assert.False(ReconnectPolicy.IsReconnectable(new CloseReason(QuiclyErrorCode.SessionReplaced) { Source = CloseSource.Peer }));
        Assert.False(ReconnectPolicy.IsReconnectable(new CloseReason(QuiclyErrorCode.NoError) { Source = CloseSource.Local }));
    }

    [Theory]
    [InlineData("attempts")]
    [InlineData("initial")]
    [InlineData("max")]
    [InlineData("multiplier")]
    [InlineData("multiplier-nan")]
    [InlineData("jitter")]
    public async Task Invalid_Policies_Are_Refused(string which)
    {
        await using ClientFixture f = new();
        ReconnectPolicy policy = Policy();
        switch (which)
        {
            case "attempts": policy.MaxAttempts = -1; break;
            case "initial": policy.InitialDelay = TimeSpan.FromSeconds(-1); break;
            case "max": policy.MaxDelay = TimeSpan.FromMilliseconds(1); break;
            case "multiplier": policy.Multiplier = 0.5; break;
            case "multiplier-nan": policy.Multiplier = double.PositiveInfinity; break;
            case "jitter": policy.Jitter = 2; break;
        }

        QuiclyClient client = f.CreateClient();
        await Assert.ThrowsAsync<ArgumentException>(async () => await client.ConnectAsync(f.EndPoint, f.Options(reconnect: policy), TestContext.Current.CancellationToken));
    }

    [Fact]
    public void Peer_Options_Copier_Copies_Every_Public_Property()
    {
        string[] settable = typeof(PeerOptions).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && p.SetMethod!.IsPublic)
            .Select(p => p.Name)
            .Order()
            .ToArray();
        Assert.Equal(settable, PeerOptionsCopier.CopiedProperties.Order().ToArray());
        PeerOptions source = new() { SessionToken = new byte[] { 9 }, LastEpoch = 4, PingInterval = TimeSpan.FromMilliseconds(7), SendTableCapacity = 32 };
        PeerOptions copy = PeerOptionsCopier.Copy(source);
        Assert.Equal(4u, copy.LastEpoch);
        Assert.Equal(TimeSpan.FromMilliseconds(7), copy.PingInterval);
        Assert.Equal(32, copy.SendTableCapacity);
        Assert.Equal(9, copy.SessionToken.Span[0]);
    }

    [Fact]
    public void Exception_Constructors()
    {
        Assert.Equal("The connection failed.", new QuiclyConnectException().Message);
        Assert.Equal("m", new QuiclyConnectException("m").Message);
        InvalidOperationException inner = new("inner");
        Assert.Same(inner, new QuiclyConnectException("m", inner).InnerException);
        QuiclyConnectException full = new("m", new CloseReason(QuiclyErrorCode.Timeout), HelloStatus.ServerFull);
        Assert.Equal(QuiclyErrorCode.Timeout, full.CloseReason.Code);
        Assert.Equal(HelloStatus.ServerFull, full.HandshakeStatus);
    }
}
