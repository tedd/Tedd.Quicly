using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Findings of the wave C2b review of the <see cref="Core.Channels.ChannelMode.ReliableUnordered"/> group-stream engine
/// (PROTOCOL.md is normative). Each test names the rule or invariant it checks.
/// </summary>
public class ReviewGroupTests
{
    /// <summary>
    /// ADR 0008 (resources released exactly once on every path) and PROTOCOL.md §3.2 (groups are independent).
    /// <para>
    /// A stream this end opened produces <em>two</em> engine notifications when the peer stops it: the peer's
    /// <c>STOP_SENDING</c> arrives as <c>OnStreamClosed(aborted: true)</c> and the shutdown that follows it as
    /// <c>OnStreamClosed(aborted: false)</c> — MsQuic always completes a started stream with SHUTDOWN_COMPLETE, and the
    /// simulator does the same (a locally opened unidirectional stream has <c>RecvDone</c> set at the open, so the
    /// stop satisfies <c>CheckShutdown</c> at once). Unlike a peer stream, which the peer's stream record de-duplicates,
    /// both reach <see cref="Core.Session.Engines.ChannelEngine.OnStreamClosed"/>, so the engine posts
    /// <c>Stopped</c> and then <c>ShutDown</c> for the same group.
    /// </para>
    /// <para>
    /// <c>GroupStreamEngine.ReleaseIfDone</c> does not bump the group record's serial when it returns the record to the
    /// free list, so the second notice still matches the freed record and releases it again: the channel's group count
    /// underflows and the record is pushed onto the free list twice, which makes its <c>Next</c> point at itself and hands
    /// every later group of every channel of the engine the same record.
    /// </para>
    /// <para>
    /// The existing tests miss it because they only stop a group that still has a carrier in flight
    /// (<c>GroupStreamTests.A_Group_The_Peer_Stops_Fails_Without_Closing_The_Channel</c>,
    /// <c>A_Message_The_Receiver_Could_Never_Buffer_Resets_Only_Its_Group</c>): an outstanding carrier makes both notices
    /// defer the release, and the carrier's own completion then frees the record exactly once. Here the send cap leaves the
    /// group holding its stream with nothing in flight, which is the ordinary state of a group between two passes.
    /// </para>
    /// </summary>
    [Fact]
    public void A_Group_Stream_The_Peer_Stops_Releases_Its_Group_Record_Exactly_Once()
    {
        LinkOptions link = new() { PeerUnidiStreams = 8 };
        using ClientHarness h = new(link: link, table: GroupTables.Main, client: o =>
        {
            DatagramKit.Quiet(o);
            o.GroupMinInterval = TimeSpan.Zero;

            // The cap hands one message of the group to the transport per pass and then holds the rest back, so the group
            // keeps its stream while no carrier is in flight (PROTOCOL.md §4.5).
            o.MaxSendBytesPerSecond = 20_000;
        });
        Assert.True(h.Accept());
        for (int i = 0; i < 8; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 1_000)).IsAdmitted);
        }

        // One group, its stream open, its first carrier already completed, the rest of its messages still queued.
        Assert.True(
            h.RunUntil(() => h.Sink.CountOf(RecordedEventKind.PeerStreamStarted) >= 2
                && GroupKit.Stats(h.Client, 5).InFlightMessages == 0
                && GroupKit.Stats(h.Client, 5).QueuedMessages > 0),
            "the group never held its stream with nothing in flight");
        Assert.Equal(1, GroupKit.Groups(h.Client, 5));

        // The peer stops the group's stream; the abort and the shutdown that follows both reach the engine.
        TransportStreamId stream = h.Sink.OfKind(RecordedEventKind.PeerStreamStarted)[1].StreamId;
        h.ServerTransport!.AbortStream(stream, 77, StreamAbortDirection.Receive);
        Assert.True(h.RunUntil(() => GroupKit.Stats(h.Client, 5).QueuedMessages == 0), "the stopped group never finished");

        // The group's record went back to the free list exactly once, so the channel holds no live group any more.
        Assert.Equal(0, GroupKit.Groups(h.Client, 5));

        // And the free list is still a list: two later groups must each get a record of their own and arrive.
        List<SendToken> tokens = [];
        for (int i = 0; i < 2; i++)
        {
            SendResult result = h.Client.SendCopy(new SendHeader(5), DatagramKit.Payload(i, 10), SendOptions.Tracked);
            Assert.True(result.IsAdmitted);
            tokens.Add(result.Token);
            h.Client.Flush();
        }

        Assert.True(
            h.RunUntil(() => tokens.TrueForAll(token => h.Client.GetDeliveryStatus(token) == DeliveryStatus.Delivered)),
            "a group admitted after the stopped one never went out");
        Assert.Equal(PeerState.Connected, h.Client.State);
    }
}
