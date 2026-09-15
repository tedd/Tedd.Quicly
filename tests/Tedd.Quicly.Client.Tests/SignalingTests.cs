using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Client.Tests;

public class SignalingTests
{
    [Fact]
    public void Signaling_Sink_Forwards_Every_Callback()
    {
        using WorkSignal signal = new();
        RecordingSink inner = new();
        SignalingSink sink = new(inner, signal);
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
        sink.OnClosed(TransportCloseReason.Local, 0, 0);
        Assert.True(inner.Count >= 12, inner.Count + " callbacks recorded");
        Assert.True(inner.IsClosed);
    }

    [Fact]
    public async Task Work_Signal_Wakes_A_Waiter_And_Tolerates_Disposal()
    {
        WorkSignal signal = new();
        ValueTask waiting = signal.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        signal.Set();
        await waiting;
        signal.Set();
        signal.Set(); // already signalled: no-op
        await signal.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await signal.WaitAsync(TimeSpan.FromMilliseconds(5), TestContext.Current.CancellationToken); // times out quietly
        signal.Dispose();
        signal.Set(); // a late transport callback after the client was disposed
    }

    [Fact]
    public void Signaling_Connector_Wraps_The_Peer_Sink()
    {
        using SimulatedNetwork network = new(new VirtualClock(), 1);
        CapturingConnector inner = new(new SimulatedConnector(network));
        using WorkSignal signal = new();
        SignalingConnector connector = new(inner, signal);
        RecordingSink peerSink = new();
        using ITransport transport = connector.Connect(new IPEndPoint(IPAddress.Loopback, 1), "game.test", peerSink);
        Assert.IsType<SignalingSink>(inner.LastSink);
        Assert.Equal("game.test", inner.LastServerName);
    }

    private sealed class CapturingConnector(ITransportConnector inner) : ITransportConnector
    {
        public ITransportSink? LastSink { get; private set; }

        public string? LastServerName { get; private set; }

        public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
        {
            LastSink = sink;
            LastServerName = serverName;
            return inner.Connect(endpoint, serverName, sink);
        }
    }
}
