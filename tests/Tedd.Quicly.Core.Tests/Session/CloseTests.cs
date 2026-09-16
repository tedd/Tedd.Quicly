using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Close handling and error codes in both directions (PROTOCOL.md §6).</summary>
public class CloseTests
{
    private static (PeerState, PeerState)[] ConnectedThenClosed() =>
    [
        .. HandshakeTests.ConnectedEvents,
        (PeerState.Connected, PeerState.Closing),
        (PeerState.Closing, PeerState.Closed),
    ];

    [Fact]
    public void Client_Close_Reaches_The_Server_With_Code_And_Reason()
    {
        using SessionHarness h = new();
        h.Client.Close(new CloseReason(QuiclyErrorCode.NoError, "bye"));
        Assert.Equal(PeerState.Closing, h.Client.State);
        Assert.True(h.RunUntilClosed());
        Assert.Equal(new CloseReason(QuiclyErrorCode.NoError, "bye") { Source = CloseSource.Local }, h.Client.CloseReason);
        Assert.Equal(new CloseReason(QuiclyErrorCode.NoError, "bye") { Source = CloseSource.Peer }, h.Server!.CloseReason);
        Assert.Equal(ConnectedThenClosed(), h.ClientEvents);
        Assert.Equal(ConnectedThenClosed(), h.ServerEvents);
    }

    [Fact]
    public void Server_Close_Reaches_The_Client_With_Code_And_Reason()
    {
        using SessionHarness h = new();
        h.Server!.Close(new CloseReason((QuiclyErrorCode)0x2A, "kicked"));
        Assert.True(h.RunUntilClosed());
        Assert.Equal(new CloseReason((QuiclyErrorCode)0x2A, "kicked") { Source = CloseSource.Peer }, h.Client.CloseReason);
        Assert.Equal(new CloseReason((QuiclyErrorCode)0x2A, "kicked") { Source = CloseSource.Local }, h.Server.CloseReason);
        Assert.Equal(ConnectedThenClosed(), h.ClientEvents);
    }

    [Fact]
    public void Simultaneous_Close_Keeps_Each_Sides_Own_Reason()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 });
        h.Client.Close(new CloseReason((QuiclyErrorCode)11, "client"));
        h.Server!.Close(new CloseReason((QuiclyErrorCode)12, "server"));
        Assert.True(h.RunUntilClosed());
        Assert.Equal(new CloseReason((QuiclyErrorCode)11, "client") { Source = CloseSource.Local }, h.Client.CloseReason);
        Assert.Equal(new CloseReason((QuiclyErrorCode)12, "server") { Source = CloseSource.Local }, h.Server.CloseReason);
    }

    [Fact]
    public void Transport_Close_Without_A_Close_Message_Is_Reported_As_A_Peer_Close()
    {
        using SessionHarness h = new();
        h.Server!.Core.Transport!.Close((ulong)QuiclyErrorCode.InternalError, default);
        Assert.True(h.RunUntilClosed());
        Assert.Equal(new CloseReason(QuiclyErrorCode.InternalError) { Source = CloseSource.Peer }, h.Client.CloseReason);
        Assert.Equal(new CloseReason(QuiclyErrorCode.InternalError) { Source = CloseSource.Local }, h.Server.CloseReason);
        (PeerState, PeerState)[] expected = [.. HandshakeTests.ConnectedEvents, (PeerState.Connected, PeerState.Closed)];
        Assert.Equal(expected, h.ClientEvents);
    }

    [Fact]
    public void Link_Loss_Is_Reported_As_A_Transport_Close()
    {
        using SessionHarness h = new(link: new LinkOptions { DisconnectAtMicros = 500_000 });
        Assert.True(h.RunUntilClosed());
        Assert.Equal(CloseSource.Transport, h.Client.CloseReason.Source);
        Assert.Equal(SimulatedTransport.StatusDisconnected, h.Client.CloseReason.TransportStatus);
        Assert.Equal(CloseSource.Transport, h.Server!.CloseReason.Source);
    }

    [Fact]
    public void Messages_Received_Before_The_Close_Are_Still_Dispatched()
    {
        using SessionHarness h = TestEngines.Create(out _, out _);
        int handled = 0;
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => handled++);
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.Equal(1, handled);
    }

    [Fact]
    public void Nothing_Runs_After_Closed()
    {
        using SessionHarness h = TestEngines.Create(out _, out TestEngine server);
        int handled = 0;
        int events = 0;
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => handled++);
        h.Server.StateChanged += (_, _, _) => events++;
        h.Client.SendCopy(new SendHeader(2), [1]);
        Assert.True(h.RunUntil(() => handled == 1));
        h.Server.Close();
        Assert.True(h.RunUntilClosed());
        Assert.Equal(2, events);
        Assert.Equal(1, server.PeerClosedCalls);

        for (int i = 0; i < 100; i++)
        {
            h.Network.Advance(10_000);
            Assert.Equal(0, h.Server.Poll());
            h.Server.Flush();
        }

        Assert.Equal(2, events);
        Assert.Equal(1, handled);
        Assert.Equal(1, server.PeerClosedCalls);
        Assert.Equal(PeerState.Closed, h.Server.State);
        Assert.Equal(SendStatus.NotConnected, h.Server.SendCopy(new SendHeader(2), [1]).Status);
        Assert.Equal(0, h.Server.Drain(2, new ReceivedMessage[4]));
        Assert.Equal(Timeout.InfiniteTimeSpan, h.Server.NextDeadline);
        Assert.Equal(long.MaxValue, h.Server.NextDeadlineMicros);
    }

    [Fact]
    public void Close_Validates_Its_Arguments()
    {
        using SessionHarness h = new();
        Assert.Throws<ArgumentException>(() => h.Client.Close(new CloseReason(QuiclyErrorCode.NoError, new string('x', 513))));
        Assert.Throws<ArgumentException>(() => h.Client.Close(new CloseReason((QuiclyErrorCode)0x1_0000_0000UL)));
        Assert.Equal(PeerState.Connected, h.Client.State);
        h.Client.Close(new CloseReason(QuiclyErrorCode.NoError, new string('é', 256)));
        Assert.Equal(PeerState.Closing, h.Client.State);
        Assert.True(h.RunUntilClosed());
        Assert.Equal(new string('é', 256), h.Server!.CloseReason.Reason);
    }

    [Fact]
    public void A_Second_Close_Is_Ignored()
    {
        using SessionHarness h = new();
        h.Client.Close(new CloseReason((QuiclyErrorCode)7, "first"));
        h.Client.Close(new CloseReason((QuiclyErrorCode)8, "second"));
        Assert.True(h.RunUntilClosed());
        Assert.Equal((QuiclyErrorCode)7, h.Client.CloseReason.Code);
        Assert.Equal("first", h.Server!.CloseReason.Reason);
    }

    [Fact]
    public void Close_Before_The_Transport_Connects()
    {
        using SessionHarness h = new(connect: false);
        h.Client.Close();
        Assert.Equal(PeerState.Closing, h.Client.State);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.Equal(CloseSource.Local, h.Client.CloseReason.Source);
        (PeerState, PeerState)[] expected = [(PeerState.Connecting, PeerState.Closing), (PeerState.Closing, PeerState.Closed)];
        Assert.Equal(expected, h.ClientEvents);
    }

    [Fact]
    public void Dispose_Closes_The_Session_And_Later_Calls_Throw()
    {
        using SessionHarness h = new();
        h.DisposeClient();
        Assert.Throws<ObjectDisposedException>(() => h.Client.Poll());
        Assert.Throws<ObjectDisposedException>(() => h.Client.Flush());
        Assert.Throws<ObjectDisposedException>(() => h.Client.SendCopy(new SendHeader(2), [1]));
        Assert.Throws<ObjectDisposedException>(() => h.Client.Close());
        h.Client.Dispose();
        Assert.True(h.RunUntil(() => h.Server!.State == PeerState.Closed));
        Assert.Equal(new CloseReason(QuiclyErrorCode.NoError) { Source = CloseSource.Peer }, h.Server!.CloseReason);
        Assert.True(h.Client.IsFreed);
        h.Client.GetStatistics(out PeerStatistics stats);
        Assert.Equal(0, stats.PingsSent);
        Assert.False(h.Client.GetChannelStatistics(2, out _));
        Assert.Equal(default, h.Client.Capabilities);
    }

    [Fact]
    public void Dispose_Inside_A_Handler_Is_Safe()
    {
        using SessionHarness h = TestEngines.Create(out _, out _);
        bool disposed = false;
        h.Server!.RegisterHandler(2, (QuiclyPeer peer, in ReceiveHeader _, ReadOnlySpan<byte> _) =>
        {
            peer.Dispose();
            disposed = true;
        });
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.StopPumpingServer();
        QuiclyPeer server = h.Server;
        Assert.True(h.RunUntil(() =>
        {
            if (!disposed)
            {
                server.Poll();
            }

            return disposed;
        }));
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.True(h.RunUntil(() => server.IsFreed));
    }

    [Fact]
    public void Close_Linger_Expires_Before_The_Close_Is_Acknowledged()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 50_000 }, client: o => o.CloseLinger = TimeSpan.FromMilliseconds(10));
        long closeAt = h.Network.NowMicros;
        h.Client.Close(new CloseReason((QuiclyErrorCode)9, "linger"));
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.InRange(h.Network.NowMicros - closeAt, 10_000, 20_000);
        Assert.True(h.RunUntil(() => h.Server!.State == PeerState.Closed));
        Assert.Equal(new CloseReason((QuiclyErrorCode)9, "linger") { Source = CloseSource.Peer }, h.Server!.CloseReason);
    }

    [Fact]
    public void Zero_Linger_Closes_The_Transport_At_Once()
    {
        using SessionHarness h = new(client: o => o.CloseLinger = TimeSpan.Zero);
        h.Client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.Equal(CloseSource.Local, h.Client.CloseReason.Source);
        Assert.Equal(QuiclyErrorCode.NoError, h.Server!.CloseReason.Code);
    }
}
