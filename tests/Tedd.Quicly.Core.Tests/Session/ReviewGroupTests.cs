using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
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

    /// <summary>
    /// The peer side of the same finding (docs/design/session-layer.md §4.3): an engine hears a stream's close <em>once</em>,
    /// locally opened streams included. The stop and the shutdown that always follows a started stream are two transport events;
    /// the peer collapses them with the same <c>Discard</c> record it uses for peer streams, so every engine — group, bulk, large
    /// ReliableLatest — may release a stream's resources on the first notice.
    /// </summary>
    [Fact]
    public void A_Locally_Opened_Stream_The_Peer_Stops_Is_Reported_To_The_Engines_Once()
    {
        using SessionHarness h = TestEngines.Create(out TestEngine client, out _, ChannelMode.ReliableOrdered);
        h.Client.SendCopy(new SendHeader(10), [1]);
        h.Run(5_000);

        // The engine's own stream, as the server sees it.
        QuiclyPeer server = h.Server!;
        TransportStreamId peerStream = default;
        Assert.True(h.RunUntil(() => server.Core.Streams.Count > 0));
        for (int slot = 0; slot < 64 && !peerStream.IsValid; slot++)
        {
            for (uint generation = 1; generation < 4 && !peerStream.IsValid; generation++)
            {
                TransportStreamId candidate = new(slot, generation);
                if (!Unsafe.IsNullRef(ref server.Core.Streams.Find(candidate)) && server.Core.Streams.Find(candidate).Tag == StreamTag.Engine)
                {
                    peerStream = candidate;
                }
            }
        }

        Assert.True(peerStream.IsValid);
        server.Core.Transport!.AbortStream(peerStream, 0x44, StreamAbortDirection.Receive);
        Assert.True(h.RunUntil(() => client.LocalStreamEvents > 0), "the stop never reached the engine");

        // The shutdown follows the stop for every started stream; it must not be a second notice for the same stream.
        h.Run(200_000);
        Assert.Equal(1, client.LocalStreamEvents);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(0, DatagramKit.Statistics(h.Client).CallbackFaults);
    }

    /// <summary>
    /// A shared lease (<see cref="QuiclyPeer.SendShared"/>, ARCHITECTURE.md §4.1) on a group channel: the group engine commits
    /// every payload through <c>EnginePayload.Commit</c>, which is what takes the reference (<c>PeerCore.AttachShared</c>) and
    /// records a borrowed pin handle, so one serialisation carried on group streams is retained exactly once per peer and
    /// released exactly once when that peer's carrier is acknowledged. A hand-rolled commit would skip the retain silently —
    /// this test is the guard against that.
    /// </summary>
    [Fact]
    public void A_Shared_Lease_On_A_Group_Channel_Is_Retained_And_Released_Once_Per_Peer()
    {
        using SharedPool pool = new();
        SharedLease shared = pool.Share(64, seed: 7);
        Assert.Equal(1, pool.Count(in shared));

        using SessionHarness first = NewGroupPair(pool);
        using SessionHarness second = NewGroupPair(pool);
        List<(ReceiveHeader Header, byte[] Payload)> here = [];
        List<(ReceiveHeader Header, byte[] Payload)> there = [];
        first.Server!.RegisterHandler(5, Handlers.Collect(here));
        second.Server!.RegisterHandler(5, Handlers.Collect(there));

        // One serialisation, two peers: one reference each on top of the host's own.
        SendResult a = first.Client.SendShared(new SendHeader(5), pool.Table, in shared, 64, SendOptions.Tracked);
        SendResult b = second.Client.SendShared(new SendHeader(5), pool.Table, in shared, 64, SendOptions.Tracked);
        Assert.True(a.IsAdmitted);
        Assert.True(b.IsAdmitted);
        Assert.Equal(3, pool.Count(in shared));

        Assert.True(first.RunUntil(() => here.Count == 1 && first.Client.GetDeliveryStatus(a.Token) == DeliveryStatus.Delivered));
        Assert.True(second.RunUntil(() => there.Count == 1 && second.Client.GetDeliveryStatus(b.Token) == DeliveryStatus.Delivered));

        // The bytes travelled uncompressed and unchanged on each peer's own group stream (SendShared is never compressed).
        Assert.Equal(64, here[0].Payload.Length);
        Assert.Equal(7, here[0].Payload[0]);
        Assert.Equal(0, here[0].Header.RawLength);
        Assert.Equal(here[0].Payload, there[0].Payload);

        // Exactly one release per peer: the host's own reference is all that is left, and it returns the block.
        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }

    private static SessionHarness NewGroupPair(SharedPool pool) =>
        new(table: GroupTables.Main,
            client: o =>
            {
                GroupKit.Prompt(o);
                o.Allocator = pool.Allocator;
            },
            server: GroupKit.Prompt);
}
