using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

public class ShutdownTests
{
    [Fact]
    public async Task BeginShutdown_Closes_Every_Peer_And_Stops_Accepting()
    {
        await using ServerFixture f = new();
        QuiclyPeer a = f.ConnectAdmitted();
        QuiclyPeer b = f.ConnectAdmitted();
        using ITransport silent = f.Connector.Connect(f.SimListener.LocalEndPoint, "game.test", new RecordingSink());
        f.Run(2_000);
        Assert.Equal(3, f.Server.PeerCount);

        f.Server.BeginShutdown();
        Assert.False(f.Server.IsRunning);
        Assert.False(f.Server.IsShutdownComplete);
        Assert.True(f.RunUntil(() => f.Server.IsShutdownComplete));
        Assert.Equal(QuiclyErrorCode.NoError, a.CloseReason.Code);
        Assert.Equal("server shutting down", a.CloseReason.Reason);
        Assert.Equal(PeerState.Closed, b.State);
        Assert.Equal(2, f.Closed.Count);
        Assert.Equal(2, f.Ended.Count);

        QuiclyPeer late = f.Connect();
        Assert.True(f.RunUntil(() => late.State == PeerState.Closed));
        Assert.Equal(SimulatedTransport.StatusUnreachable, late.CloseReason.TransportStatus);
        f.Server.BeginShutdown();
    }

    [Fact]
    public async Task BeginShutdown_With_A_Custom_Reason()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.ConnectAdmitted();
        Assert.ThrowsAny<ArgumentException>(() => f.Server.BeginShutdown(new CloseReason(QuiclyErrorCode.NoError, new string('x', 600))));
        f.Server.BeginShutdown(new CloseReason(QuiclyErrorCode.InternalError, "maintenance"));
        Assert.True(f.RunUntil(() => client.State == PeerState.Closed));
        Assert.Equal(QuiclyErrorCode.InternalError, client.CloseReason.Code);
        Assert.Equal("maintenance", client.CloseReason.Reason);
    }

    [Fact]
    public async Task StopAsync_Drives_The_Shutdown()
    {
        await using ServerFixture f = new();
        QuiclyPeer client = f.ConnectAdmitted();
        await f.Server.StopAsync(TestContext.Current.CancellationToken);
        Assert.True(f.Server.IsShutdownComplete);
        Assert.Single(f.Closed);
        Assert.Equal(PeerState.Closed, client.State);
        await f.Server.StopAsync(TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Server.StartAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task StopAsync_Disposes_Peers_That_Do_Not_Close_In_Time()
    {
        await using ServerFixture f = new(o => o.ShutdownTimeout = TimeSpan.Zero);
        f.ConnectAdmitted();
        await f.Server.StopAsync(TestContext.Current.CancellationToken);
        Assert.Single(f.Closed);
        Assert.Equal(0, f.Server.PeerCount);
        Assert.Equal(CloseSource.Local, f.Closed[0].Reason.Source);
    }

    [Fact]
    public async Task StopAsync_Cancellation_Cuts_The_Wait_Short()
    {
        await using ServerFixture f = new();
        f.ConnectAdmitted();
        using CancellationTokenSource cancel = new();
        f.Server.DelayOverride = (_, token) =>
        {
            cancel.Cancel();
            return Task.Delay(Timeout.Infinite, token);
        };
        await f.Server.StopAsync(cancel.Token);
        Assert.Single(f.Closed);
        Assert.Equal(0, f.Server.PeerCount);
    }

    [Fact]
    public async Task StopAsync_Waits_In_Real_Time_Without_A_Delay_Override()
    {
        await using ServerFixture f = new(o => o.ShutdownTimeout = TimeSpan.FromMilliseconds(50));
        f.ConnectAdmitted();
        f.Server.DelayOverride = null;
        await f.Server.StopAsync(TestContext.Current.CancellationToken); // the network is not advanced: the timeout disposes the peer
        Assert.Equal(0, f.Server.PeerCount);
    }

    [Fact]
    public async Task A_Throwing_Close_Handler_Does_Not_Keep_Peers_Alive()
    {
        await using ServerFixture f = new(o => o.ShutdownTimeout = TimeSpan.Zero);
        f.ConnectAdmitted();
        f.ConnectAdmitted();
        f.Server.PeerClosed += (_, _) => throw new InvalidOperationException("close handler");
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Server.StopAsync(TestContext.Current.CancellationToken));
        Assert.Equal(0, f.Server.PeerCount);
        Assert.Equal(2, f.Closed.Count);
    }

    [Fact]
    public async Task Stopping_From_Inside_An_Event_Is_Safe()
    {
        await using ServerFixture f = new(o => o.ShutdownTimeout = TimeSpan.Zero);
        f.Server.PeerAdmitted += _ => f.Server.StopAsync(TestContext.Current.CancellationToken).GetAwaiter().GetResult();
        QuiclyPeer client = f.Connect();
        f.Run(20_000);
        Assert.Equal(0, f.Server.PeerCount);
        Assert.False(f.Server.IsRunning);
        Assert.Single(f.Closed);
    }

    [Fact]
    public async Task Never_Started_And_Disposed_Servers()
    {
        ManualListener listener = new();
        await using ServerFixture f = new(start: false, listener: listener);
        await f.Server.StopAsync(TestContext.Current.CancellationToken);
        Assert.False(f.Server.IsRunning);
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Server.StartAsync(TestContext.Current.CancellationToken));
        await f.Server.DisposeAsync();
        await f.Server.DisposeAsync();
        Assert.True(listener.Disposed);
        Assert.Throws<ObjectDisposedException>(() => f.Server.PollAll());
        Assert.Throws<ObjectDisposedException>(() => f.Server.FlushAll());
        Assert.Throws<ObjectDisposedException>(() => f.Server.CreateSet());
        Assert.Throws<ObjectDisposedException>(() => f.Server.BeginShutdown());
        Assert.Throws<ObjectDisposedException>(() => f.Server.RotateSessionKey(new byte[32]));
        Assert.Throws<ObjectDisposedException>(() => f.Server.SendShared(new PeerSet(1), new SendHeader(2), default, 0));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => f.Server.StopAsync(TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => f.Server.StartAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Disposing_A_Never_Started_Server_Frees_Its_Pool()
    {
        await using ServerFixture f = new(start: false);
        await f.Server.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => f.Server.Allocator.TryRent(64, out _));
    }

    [Fact]
    public async Task The_Pool_Is_Freed_Once_Every_Peer_Let_Go()
    {
        await using ServerFixture f = new();
        f.ConnectAdmitted();
        f.ConnectAdmitted();
        await f.Server.DisposeAsync();
        f.Network.RunUntilIdle(10_000_000);
        Assert.Throws<ObjectDisposedException>(() => f.Server.Allocator.TryRent(64, out _));
    }
}
