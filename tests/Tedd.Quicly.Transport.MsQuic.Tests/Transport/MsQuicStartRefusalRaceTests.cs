using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>
/// A send that carries <see cref="TransportSendFlags.Start"/> is two MsQuic calls (the start, queued to the worker, and the
/// send), and the worker can run the start between them. When the peer's stream limit refuses it there, the stream shuts
/// down and MsQuic rejects the send with <c>INVALID_STATE</c>. <see cref="MsQuicTransport.DelaySendUntilStartReported"/>
/// makes the send wait until START_COMPLETE was indicated, so that the worker always wins the race.
/// </summary>
[Collection(MsQuicCollection.Name)]
public unsafe class MsQuicStartRefusalRaceTests
{
    [Fact]
    public void A_Send_Whose_Start_The_Worker_Refused_First_Returns_StreamLimitReached()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using MsQuicTransportHarness harness = new();
        TimeSpan timeout = harness.DefaultTimeout;
        RecordingSink clientSink = new() { AutoCloseStreams = false };
        RecordingSink serverSink = new();
        ConformancePair pair = harness.CreatePair(clientSink, serverSink, new ConformancePairOptions { ServerPeerUnidiStreams = 0 });
        MsQuicTransport client = (MsQuicTransport)pair.Client;
        clientSink.Transport = pair.Client;
        serverSink.Transport = pair.Server;
        Assert.True(clientSink.WaitForCount(RecordedEventKind.Connected, 1, timeout), "the client never connected");
        client.DelaySendUntilStartReported = true;

        byte[] data = [1, 2, 3, 4, 5, 6, 7, 8];
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 0x21, 32767, out TransportStreamId refused));
        TransportStatus sent;
        fixed (byte* pointer = data)
        {
            TransportSegment segment = new(pointer, data.Length);
            sent = client.SendStream(refused, &segment, 1, 0x31, TransportSendFlags.Start | TransportSendFlags.Fin);
        }

        // The worker refused the start before the send was queued; the call answers the refusal, not InvalidState.
        Assert.Equal(TransportStatus.StreamLimitReached, sent);
        Assert.Equal(1, client.StartsWithSend);
        Assert.Equal(1, client.StartRefusalRaces);

        // The refusal callbacks had been delivered (or follow): once each, and no completion for a send that was never accepted.
        Assert.True(clientSink.WaitForCount(RecordedEventKind.StreamShutdownComplete, 1, timeout), "OnStreamShutdownComplete of the refused stream");
        Thread.Sleep(200);
        List<string> seen = [];
        foreach (RecordedEvent e in clientSink.Events)
        {
            if (e.StreamId == refused && e.Kind is RecordedEventKind.StreamStarted or RecordedEventKind.StreamSendCompleted or RecordedEventKind.StreamShutdownComplete)
            {
                seen.Add(e.Kind == RecordedEventKind.StreamStarted ? $"StreamStarted {e.Status}" : e.Kind.ToString());
            }
        }

        Assert.Equal([$"StreamStarted {TransportStatus.StreamLimitReached}", nameof(RecordedEventKind.StreamShutdownComplete)], seen);
        Assert.Equal(0, clientSink.CountOf(RecordedEventKind.StreamSendCompleted));
        Assert.Equal(0, serverSink.CountOf(RecordedEventKind.PeerStreamStarted));

        // The refused stream never starts, whatever the credit does later.
        int before = clientSink.Count;
        pair.Server.UpdatePeerStreamLimits(16, 4);
        Assert.True(clientSink.WaitFor(e => e.Kind == RecordedEventKind.StreamsAvailable && e.Unidirectional > 0, timeout), "OnStreamsAvailable with unidirectional credit");
        Assert.Equal(TransportStatus.InvalidState, client.StartStream(refused));
        TransportStatus again;
        fixed (byte* pointer = data)
        {
            TransportSegment segment = new(pointer, data.Length);
            again = client.SendStream(refused, &segment, 1, 0x32, TransportSendFlags.Start);
        }

        Assert.Equal(TransportStatus.InvalidState, again);
        Assert.Equal(-1, client.GetQuicStreamId(refused));
        client.CloseStream(refused);
        Assert.True(clientSink.Count >= before);

        // A new stream delivers its data.
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 0x23, 32767, out TransportStreamId fresh));
        TransportStatus retried;
        fixed (byte* pointer = data)
        {
            TransportSegment segment = new(pointer, data.Length);
            retried = client.SendStream(fresh, &segment, 1, 0x33, TransportSendFlags.Start | TransportSendFlags.Fin);
        }

        Assert.Equal(TransportStatus.Success, retried);
        Assert.True(serverSink.WaitForCount(RecordedEventKind.StreamPeerSendShutdown, 1, timeout), "the new stream's data at the server");
        RecordedEvent peer = Assert.Single(serverSink.OfKind(RecordedEventKind.PeerStreamStarted));
        Assert.Equal(data, serverSink.GetStreamData(peer.StreamId));
        Assert.Equal(1, client.StartRefusalRaces);
        Assert.Equal(2, client.StartsWithSend);
        Assert.DoesNotContain(clientSink.OfKind(RecordedEventKind.StreamSendCompleted), e => e.Context is 0x31 or 0x32);
    }
}
