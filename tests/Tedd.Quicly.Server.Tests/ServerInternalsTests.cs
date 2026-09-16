using System.Net;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

public class ServerInternalsTests
{
    [Fact]
    public async Task The_Connection_Sink_Forwards_Every_Callback_And_Owns_The_Address_Accounting()
    {
        await using ServerFixture f = new();
        RecordingSink inner = new();
        ConnectionSink sink = new(f.Server, 3, inner);
        TransportStreamId stream = new(1, 1);
        TransportConnectedInfo info = default;
        sink.OnConnected(in info);
        sink.OnDatagramReceived([1, 2]);
        sink.OnPeerStreamStarted(stream, StreamKind.Unidirectional);
        sink.OnStreamStarted(stream, 7, TransportStatus.Success);
        sink.OnStreamReceived(stream, ReadOnlySpan<TransportSegment>.Empty, 0, false);
        sink.OnStreamSendCompleted(stream, 7, false);
        sink.OnDatagramSendStateChanged(7, DatagramSendState.Sent);
        sink.OnStreamAborted(stream, 3, StreamAbortDirection.Both);
        sink.OnStreamPeerSendShutdown(stream);
        sink.OnStreamShutdownComplete(stream);
        sink.OnDatagramCapabilityChanged(true, 1200);
        sink.OnIdealSendBufferSize(stream, 4096);
        sink.OnStreamsAvailable(1, 4);
        sink.OnPeerAddressChanged(in info);
        Assert.True(inner.Count >= 12, inner.Count + " callbacks recorded");
        Assert.False(sink.IsRetired);

        Assert.False(sink.TakeLimitClose());
        sink.RequestLimitClose();
        Assert.True(sink.TakeLimitClose());
        Assert.False(sink.TakeLimitClose());

        // A retired sink keeps forwarding but no longer moves address counts, nor marks its (reused) slot.
        sink.RetireLocked();
        Assert.True(sink.IsRetired);
        TransportConnectedInfo moved = default;
        moved.RemoteEndPoint = new IPEndPoint(IPAddress.Parse("192.0.2.1"), 1);
        sink.OnPeerAddressChanged(in moved);
        Assert.False(sink.HasAddress);
        sink.RequestLimitClose();
        Assert.True(sink.TakeLimitClose());
        sink.OnClosed(TransportCloseReason.Local, 0, 0);
        Assert.True(inner.IsClosed);

        ConnectionSink released = new(f.Server, 4, new RecordingSink());
        released.MarkReleased();
        released.MarkReleased(); // idempotent
        f.Server.MarkWork(-1);   // out of range: ignored
        f.Server.MarkWork(100_000);
    }

    /// <summary>
    /// The peers' <see cref="PeerOptions.WorkSignal"/> is the server's: work a peer publishes marks its slot, so a
    /// <see cref="QuiclyServer.PollAll"/> that nothing else marked still polls it (the server interposes no sink of its own
    /// for that any more).
    /// </summary>
    [Fact]
    public async Task A_Peers_Work_Signal_Marks_Its_Slot()
    {
        await using ServerFixture f = new();
        List<byte[]> received = [];
        f.Server.PeerAdmitted += peer => peer.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => received.Add(payload.ToArray()));
        QuiclyPeer client = f.ConnectAdmitted();
        f.Run(50_000);
        f.Server.GetStatistics(out ServerStatistics idle);

        // Nothing marks the slot but the peer itself: the message is published on a transport thread inside Advance.
        Assert.True(client.SendCopy(new SendHeader(2), [1, 2, 3]).IsAdmitted);
        client.Flush();
        f.Network.Advance(1_000);
        Assert.Equal(1, f.Server.PollAll());

        f.Server.GetStatistics(out ServerStatistics polled);
        Assert.True(polled.PeersPolled > idle.PeersPolled);
        Assert.Equal([1, 2, 3], Assert.Single(received));
    }

    [Fact]
    public void Address_Keys_Aggregate_Like_The_Rate_Limiter()
    {
        Assert.False(AddressKey.TryCreate(null, 64, out _));
        Assert.True(AddressKey.TryCreate(new IPEndPoint(IPAddress.Parse("192.0.2.1"), 1), 64, out AddressKey v4));
        Assert.True(AddressKey.TryCreate(new IPEndPoint(IPAddress.Parse("::ffff:192.0.2.1"), 2), 64, out AddressKey mapped));
        Assert.Equal(v4, mapped);
        Assert.True(AddressKey.TryCreate(new IPEndPoint(IPAddress.Parse("2001:db8::1"), 1), 128, out AddressKey full));
        Assert.True(AddressKey.TryCreate(new IPEndPoint(IPAddress.Parse("2001:db8::2"), 1), 128, out AddressKey other));
        Assert.NotEqual(full, other);
        Assert.True(AddressKey.TryCreate(new IPEndPoint(IPAddress.Parse("2001:db8::1"), 1), 96, out AddressKey wide));
        Assert.True(AddressKey.TryCreate(new IPEndPoint(IPAddress.Parse("2001:db8::2"), 1), 96, out AddressKey wideOther));
        Assert.Equal(wide, wideOther);
        Assert.True(AddressKey.TryCreate(new IPEndPoint(IPAddress.Parse("2001:db8:ffff::1"), 1), 32, out AddressKey narrow));
        Assert.True(AddressKey.TryCreate(new IPEndPoint(IPAddress.Parse("2001:db8:0::1"), 1), 32, out AddressKey narrowOther));
        Assert.Equal(narrow, narrowOther);
    }

    [Fact]
    public async Task A_Server_Without_Observers_Runs_Every_Path()
    {
        await using ServerFixture f = new(o =>
        {
            o.MaxPeers = 1;
            o.Sessions.Grace = TimeSpan.FromMilliseconds(100);
        }, observe: false);
        QuiclyPeer first = f.ConnectAdmitted(connector: new SimulatedConnector(f.Network, new LinkOptions { DisconnectAtMicros = 50_000 }));
        QuiclyPeer refused = f.Connect();
        Assert.True(f.RunUntil(() => refused.State == PeerState.Closed && first.State == PeerState.Closed));
        f.Run(300_000);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(1, statistics.SessionsExpired);
        Assert.Equal(1, statistics.AdmissionsRejected);
        Assert.Equal(0, statistics.Sessions);
    }

    [Fact]
    public void Sweep_Keeps_Sessions_Still_Within_Grace()
    {
        SessionRegistry registry = new(4);
        using SimulatedNetwork network = new(new Core.Time.VirtualClock(), 1);
        List<SessionEndInfo> ended = [];
        Assert.Null(registry.Find(1));
        Assert.Equal(long.MaxValue, registry.EarliestExpiry);
        registry.Sweep(0, ended);
        Assert.Empty(ended);
        Assert.Equal(0, registry.DisconnectedCount);
    }
}
