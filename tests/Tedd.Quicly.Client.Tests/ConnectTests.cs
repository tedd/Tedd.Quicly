using System.Net;
using System.Text;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Server;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Client.Tests;

public class ConnectTests
{
    [Fact]
    public async Task Connect_Returns_A_Connected_Peer()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient();
        List<(PeerState From, PeerState To)> states = [];
        client.StateChanged += (_, from, to) => states.Add((from, to));
        List<QuiclyPeer> created = [];
        ClientOptions options = f.Options("alice");
        options.PeerCreated = created.Add;
        Assert.Equal(PeerState.Closed, client.State);
        Assert.Null(client.Peer);

        QuiclyPeer peer = await client.ConnectAsync(f.EndPoint, options, TestContext.Current.CancellationToken);
        Assert.Equal(PeerState.Connected, peer.State);
        Assert.Same(peer, client.Peer);
        Assert.Same(peer, Assert.Single(created));
        Assert.Equal(PeerState.Connected, client.State);
        Assert.Equal(new[] { (PeerState.Closed, PeerState.Connecting), (PeerState.Connecting, PeerState.Connected) }, states);
        Assert.Equal(f.EndPoint, client.RemoteEndPoint);
        Assert.Same(f.Connector, client.Connector);
        Assert.Equal(1u, peer.Epoch);
        Assert.Equal(SessionTokenAuthority.TokenLength, peer.SessionToken.Length);
        Assert.Equal("alice", Encoding.UTF8.GetString(client.AuthToken.Span));
        Assert.Equal(0, client.ReconnectAttempt);
        Assert.Single(f.ServerAdmitted);
        f.Run(client, 100_000);
        Assert.Equal(PeerState.Connected, client.State);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Connect_To_Nowhere_Throws_And_The_Client_Can_Try_Again()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient();
        QuiclyConnectException failure = await Assert.ThrowsAsync<QuiclyConnectException>(
            async () => await client.ConnectAsync(new IPEndPoint(IPAddress.Parse("10.255.255.1"), 9), f.Options(), TestContext.Current.CancellationToken));
        Assert.Equal(CloseSource.Transport, failure.CloseReason.Source);
        Assert.Equal(SimulatedTransport.StatusUnreachable, failure.CloseReason.TransportStatus);
        Assert.Equal(HelloStatus.Accepted, failure.HandshakeStatus);
        Assert.Contains("before the session was admitted", failure.Message);
        Assert.Equal(PeerState.Closed, client.State);
        Assert.Null(client.Peer);

        QuiclyPeer peer = await client.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken);
        Assert.Equal(PeerState.Connected, peer.State);
    }

    [Fact]
    public async Task A_Refused_Session_Throws_With_The_Status()
    {
        await using ClientFixture f = new(o => o.Admission.AuthTokenValidator = static (in AuthTokenContext _) => AuthTokenDecision.Reject);
        QuiclyClient client = f.CreateClient();
        QuiclyConnectException failure = await Assert.ThrowsAsync<QuiclyConnectException>(
            async () => await client.ConnectAsync(f.EndPoint, f.Options("bad"), TestContext.Current.CancellationToken));
        Assert.Equal(HelloStatus.Rejected, failure.HandshakeStatus);
        Assert.Equal(QuiclyErrorCode.AdmissionRejected, failure.CloseReason.Code);
        Assert.Contains("refused", failure.Message);
    }

    [Fact]
    public async Task A_Refusal_With_A_Reason_Carries_It()
    {
        await using ClientFixture f = new(o => o.MaxPeers = 1);
        QuiclyClient first = f.CreateClient();
        await first.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken);
        f.Server.AdmissionFailed += _ => { };
        QuiclyClient second = f.CreateClient();
        QuiclyConnectException failure = await Assert.ThrowsAsync<QuiclyConnectException>(
            async () => await second.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken));
        Assert.Equal(HelloStatus.ServerFull, failure.HandshakeStatus);
    }

    [Fact]
    public async Task A_Different_Channel_Table_Is_Refused()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient();
        QuiclyConnectException failure = await Assert.ThrowsAsync<QuiclyConnectException>(
            async () => await client.ConnectAsync(f.EndPoint, f.Options(table: Tables.Other), TestContext.Current.CancellationToken));
        Assert.Equal(HelloStatus.ChannelTableMismatch, failure.HandshakeStatus);
        Assert.Contains("channel table mismatch", failure.Message);
    }

    [Fact]
    public async Task A_Stalled_Admission_Times_Out()
    {
        await using ClientFixture f = new(o =>
        {
            o.PeerOptions.AdmissionTimeout = TimeSpan.FromMilliseconds(200);
            o.Admission.AuthTokenValidator = static (in AuthTokenContext _) => AuthTokenDecision.Pending;
        });
        QuiclyClient client = f.CreateClient();
        QuiclyConnectException failure = await Assert.ThrowsAsync<QuiclyConnectException>(
            async () => await client.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken));
        Assert.Equal(QuiclyErrorCode.Timeout, failure.CloseReason.Code);
    }

    [Fact]
    public async Task Cancellation_Before_And_During_The_Connect()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await client.ConnectAsync(f.EndPoint, f.Options(), new CancellationToken(true)));
        Assert.Equal(PeerState.Closed, client.State);

        using CancellationTokenSource cancel = new();
        int waits = 0;
        client.WaitOverride = (_, token) =>
        {
            Assert.Equal(0, client.Poll()); // the connect drives the peer itself
            client.Flush();
            Assert.Throws<InvalidOperationException>(() => client.Close());
            if (++waits == 2)
            {
                cancel.Cancel();
            }

            f.Step(1_000);
            return ValueTask.CompletedTask;
        };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await client.ConnectAsync(f.EndPoint, f.Options(), cancel.Token));
        Assert.Equal(PeerState.Closed, client.State);
        Assert.Null(client.Peer);
    }

    [Fact]
    public async Task Connect_Without_A_Wait_Override_Waits_For_Transport_Callbacks()
    {
        await using ClientFixture f = new();
        QuiclyClient client = new(f.Connector);
        try
        {
            using CancellationTokenSource stop = new();
            Task pump = Task.Run(() =>
            {
                while (!stop.IsCancellationRequested)
                {
                    f.Step(1_000);
                    Thread.Sleep(1);
                }
            }, TestContext.Current.CancellationToken);
            QuiclyPeer peer = await client.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken);
            stop.Cancel();
            await pump;
            Assert.Equal(PeerState.Connected, peer.State);
        }
        finally
        {
            client.Dispose();
        }

        // A callback reaching a disposed client's signal is ignored.
        f.Network.RunUntilIdle(1_000_000);
    }

    [Fact]
    public async Task A_Throwing_Peer_Callback_Fails_The_Connect()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient();
        ClientOptions options = f.Options();
        options.PeerCreated = _ => throw new InvalidOperationException("created");
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.ConnectAsync(f.EndPoint, options, TestContext.Current.CancellationToken));
        Assert.Equal(PeerState.Closed, client.State);
    }

    [Fact]
    public async Task Invalid_Arguments_Are_Refused()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient();
        CancellationToken token = TestContext.Current.CancellationToken;
        Assert.Throws<ArgumentNullException>(() => new QuiclyClient(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ConnectAsync(null!, f.Options(), token));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ConnectAsync(f.EndPoint, null!, token));

        ClientOptions noTable = f.Options();
        noTable.Channels = null;
        ClientOptions noPeerOptions = f.Options();
        noPeerOptions.PeerOptions = null!;
        ClientOptions longToken = f.Options();
        longToken.AuthToken = new byte[4097];
        ClientOptions badPeer = f.Options();
        badPeer.PeerOptions.PingInterval = TimeSpan.Zero;
        foreach (ClientOptions options in new[] { noTable, noPeerOptions, longToken })
        {
            await Assert.ThrowsAsync<ArgumentException>(async () => await client.ConnectAsync(f.EndPoint, options, token));
        }

        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await client.ConnectAsync(f.EndPoint, badPeer, token));
        Assert.Equal(PeerState.Closed, client.State);
        Assert.Throws<ArgumentException>(() => client.AuthToken = new byte[4097]);
        client.AuthToken = new byte[] { 1, 2 };
        Assert.Equal(2, client.AuthToken.Length);
    }

    [Fact]
    public async Task Resuming_A_Session_From_An_Earlier_Run()
    {
        await using ClientFixture f = new();
        QuiclyClient first = f.CreateClient();
        QuiclyPeer original = await first.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken);
        byte[] token = original.SessionToken.ToArray();
        ulong session = original.SessionId;

        // An application restart: a new client presents the saved token.
        QuiclyClient second = f.CreateClient();
        ClientOptions options = f.Options();
        options.PeerOptions.SessionToken = token;
        options.PeerOptions.LastEpoch = 1;
        QuiclyPeer resumed = await second.ConnectAsync(f.EndPoint, options, TestContext.Current.CancellationToken);
        Assert.Equal(session, resumed.SessionId);
        Assert.Equal(2u, resumed.Epoch);
    }

    [Fact]
    public async Task Close_Poll_Flush_And_Dispose_Outside_A_Connection()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient();
        Assert.Equal(0, client.Poll());
        client.Flush();
        client.Close(); // nothing to close
        Assert.Equal(PeerState.Closed, client.State);

        QuiclyPeer peer = await client.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken);
        List<CloseReason> disconnected = [];
        client.Disconnected += (_, reason) => disconnected.Add(reason);
        client.Close(new CloseReason(QuiclyErrorCode.NoError, "bye"));
        Assert.Equal(PeerState.Closing, client.State);
        client.Close(); // already closing
        Assert.True(f.RunUntil(client, () => client.State == PeerState.Closed));
        Assert.Equal("bye", Assert.Single(disconnected).Reason);
        Assert.Same(peer, client.Peer);

        client.Dispose();
        client.Dispose();
        Assert.Throws<ObjectDisposedException>(() => client.Poll());
        Assert.Throws<ObjectDisposedException>(() => client.Flush());
        Assert.Throws<ObjectDisposedException>(() => client.Close());
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await client.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task The_Server_Closing_The_Connection_Moves_The_Client_To_Closing_Then_Closed()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient();
        QuiclyPeer peer = await client.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken);
        List<PeerState> states = [];
        client.StateChanged += (_, _, to) => states.Add(to);
        f.ServerPeerOf(peer).Close(new CloseReason(QuiclyErrorCode.NoError, "kick"));
        Assert.True(f.RunUntil(client, () => client.State == PeerState.Closed));
        Assert.Equal(PeerState.Closed, states[^1]);
    }

    [Fact]
    public async Task Auto_Flush_Is_Implemented_By_Poll()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient();
        ClientOptions options = f.Options();
        options.PeerOptions.AutoFlushInterval = TimeSpan.FromMilliseconds(10);
        await client.ConnectAsync(f.EndPoint, options, TestContext.Current.CancellationToken);
        for (int i = 0; i < 100; i++)
        {
            f.Step(1_000);
            client.Poll();
        }

        Assert.Equal(PeerState.Connected, client.State);
    }

    /// <summary>
    /// Dispose cancels the wait a connect is sitting in (its own token, linked into the wait), and the connect then ends with
    /// <see cref="ObjectDisposedException"/> and disposes the peer it drove. The wait is stubbed out here so the test never
    /// depends on real timers or on a free thread-pool thread; the work signal's own wait is covered by
    /// <see cref="Connect_Without_A_Wait_Override_Waits_For_Transport_Callbacks"/>.
    /// </summary>
    [Fact]
    public async Task Disposing_The_Client_Cancels_The_Wait_A_Connect_Sits_In()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient(f.Unreachable());
        TaskCompletionSource waiting = new(TaskCreationOptions.RunContinuationsAsynchronously);
        client.WaitOverride = async (_, token) =>
        {
            waiting.TrySetResult(); // the connect is in its wait now
            await Task.Delay(Timeout.InfiniteTimeSpan, token); // only Dispose (or the caller's token) ends it
        };

        Task<QuiclyPeer> connecting = client.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken).AsTask();
        await waiting.Task;
        Assert.False(connecting.IsCompleted);
        client.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => connecting);
        Assert.Equal(PeerState.Closed, client.State);
        Assert.Null(client.Peer);
        for (int i = 0; i < 200 && f.Server.PeerCount != 0; i++)
        {
            f.Step(1_000); // the peer the connect drove was disposed: the server sees its connection close
        }

        Assert.Equal(0, f.Server.PeerCount);
    }
}
