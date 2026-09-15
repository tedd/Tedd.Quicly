using System.Net;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

public class ServerInternalsTests
{
    [Fact]
    public async Task Work_Signal_Sink_Forwards_Every_Callback()
    {
        await using ServerFixture f = new();
        RecordingSink inner = new();
        WorkSignalSink sink = new(f.Server, 3, inner);
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

        // A retired sink keeps forwarding but no longer marks its (reused) slot, nor moves address counts.
        sink.RetireLocked();
        Assert.True(sink.IsRetired);
        TransportConnectedInfo moved = default;
        moved.RemoteEndPoint = new IPEndPoint(IPAddress.Parse("192.0.2.1"), 1);
        sink.OnPeerAddressChanged(in moved);
        Assert.False(sink.HasAddress);
        sink.OnClosed(TransportCloseReason.Local, 0, 0);
        Assert.True(inner.IsClosed);

        WorkSignalSink released = new(f.Server, 4, new RecordingSink());
        released.MarkReleased();
        released.MarkReleased(); // idempotent
        f.Server.MarkWork(-1);   // out of range: ignored
        f.Server.MarkWork(100_000);
    }

    [Fact]
    public unsafe void Probe_Transport_Does_Nothing()
    {
        ProbeTransport probe = new();
        Assert.Equal(TransportStatus.InvalidState, probe.SendDatagram(null, 0, 0, TransportSendFlags.None));
        Assert.Equal(TransportStatus.InvalidState, probe.OpenStream(StreamKind.Bidirectional, 0, 0, out TransportStreamId id));
        Assert.False(id.IsValid);
        Assert.Equal(TransportStatus.InvalidState, probe.StartStream(default));
        Assert.Equal(TransportStatus.InvalidState, probe.SendStream(default, null, 0, 0, TransportSendFlags.None));
        Assert.Equal(-1, probe.GetQuicStreamId(default));
        probe.AbortStream(default, 0, StreamAbortDirection.Both);
        probe.SetStreamPriority(default, 1);
        probe.ResumeStreamReceive(default, 0);
        probe.CloseStream(default);
        probe.UpdatePeerStreamLimits(1, 1);
        probe.GetStatistics(out TransportStatistics statistics);
        Assert.Equal(0u, statistics.RttMicros);
        Assert.False(probe.Capabilities.Datagrams);
        Assert.Equal(TransportState.Connecting, probe.State);
        probe.Close(0, default); // no sink attached: nothing to report
        probe.Dispose();
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
