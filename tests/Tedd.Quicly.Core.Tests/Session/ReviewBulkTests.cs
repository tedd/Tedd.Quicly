using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Findings of the wave C2c review of the <see cref="Core.Channels.ChannelMode.Bulk"/> engine (PROTOCOL.md is
/// normative). Each test names the rule or invariant it checks.
/// </summary>
public class ReviewBulkTests
{
    private const int Mib = 1024 * 1024;

    /// <summary>
    /// PROTOCOL.md §4.3 (Bulk's <c>Delivered</c> is "transfer complete (<c>BulkProgress</c> = Length)"), §2.3
    /// (<c>BulkProgress</c> carries <c>bytesAccepted</c>) and ADR 0009 ("every parser is bounded before it allocates";
    /// "both endpoints enforce the same limits — a client parses hostile servers too").
    /// <para>
    /// <c>BulkEngine.ApplyProgress</c> clamps the peer's claim to the range's own <c>Length</c> and to nothing else:
    /// <c>send.BytesAcked = Math.Min(bytesAccepted, send.Length)</c>. It never compares the claim with the bytes this end
    /// actually handed to the transport (<c>send.BytesRead</c>) or with the bytes whose sends completed
    /// (<c>send.BytesCompleted</c>), so a peer may claim to have accepted the whole range before a single body byte has
    /// left this host. The transfer then completes <see cref="BulkStatus.Completed"/> with
    /// <c>BytesTransferred == Length</c>, which is the mode's delivery guarantee reported for an object the peer cannot
    /// have: the application is told the object landed while its <see cref="IBulkSource"/> was never even read.
    /// </para>
    /// <para>
    /// It costs resources as well as truth. The completion path runs <c>ReleaseStreamSlot</c> + <c>ReleaseIfDone</c>
    /// without aborting the stream, and no FIN was ever sent, so the transfer's transport stream stays open for the life
    /// of the connection while the engine's own slot accounting says the channel is free again. Two such claims and the
    /// engine will open more streams on the channel than the table's <c>max(MaxGroups, 1)</c> allows, which is exactly
    /// the case docs/design/session-layer.md §7.7 warns of: "opening more than <c>max(MaxGroups, 1)</c> streams on a Bulk
    /// channel would make the <em>receiver</em> reset a live transfer of ours".
    /// </para>
    /// <para>
    /// The existing tests miss it because every <c>BulkProgress</c> they feed the engine is either honest (the delivery
    /// suite's real receiver) or names a transfer that does not exist
    /// (<c>BulkEngineTests.Control_Messages_That_Name_Nothing_Are_Refused</c> uses transfer id 9, and
    /// <c>The_Engine_Overflows_Its_Control_Ring_Without_Losing_The_Session</c> uses id 1 with no transfer registered), so
    /// the over-claim branch is never reached with a live record.
    /// </para>
    /// <para>
    /// Fix: clamp the accepted count to what this end has actually put on the wire —
    /// <c>Math.Min(bytesAccepted, send.BytesCompleted)</c> (completed sends are the bytes the peer could have seen) — and
    /// count a claim above that as a protocol violation of the peer (a counter, as §7 requires of every violation), so a
    /// transfer only ever completes on bytes this end really sent.
    /// </para>
    /// </summary>
    [Fact]
    public async Task A_BulkProgress_Claiming_More_Bytes_Than_This_End_Sent_Must_Not_Complete_The_Transfer()
    {
        const long length = 4 * Mib;
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 4_000_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(AcceptRouter.Pattern()));

        MemorySource source = new(new byte[1]);
        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, length), source);
        Assert.Equal(BulkStatus.Running, transfer.Status);
        Assert.Equal(0, source.Reads);

        // The peer claims the whole range before this end has read one byte of the object or opened the transfer's
        // stream. The engine applies the peer's control messages at the start of a pass, before it submits any piece, so
        // nothing of this transfer has been sent when the claim is applied.
        BulkEngine engine = BulkKit.Engine(h.Client);
        Assert.True(engine.OnControl(ControlType.BulkProgress, Progress(transfer.TransferId, (ulong)length), onStream: false, 0));
        h.Run(20_000);

        Assert.NotEqual(BulkStatus.Completed, transfer.Status);
        Assert.True(
            transfer.BytesTransferred < length,
            $"the peer's claim of {length} bytes was accepted as delivered after {source.Reads} source reads");
        Assert.Equal(PeerState.Connected, h.Client.State);

        static byte[] Progress(ulong transferId, ulong bytesAccepted)
        {
            byte[] body = new byte[16];
            int position = VarInt.Write(body, transferId);
            position += VarInt.Write(body.AsSpan(position), bytesAccepted);
            return body.AsSpan(0, position).ToArray();
        }
    }

    /// <summary>
    /// PROTOCOL.md §3.3 ("<c>TransferId</c> unique per (peer, direction)") and §3.4 (<c>BulkCancel</c> travels in
    /// <em>both</em> directions), with §4.3's terminal states.
    /// <para>
    /// A transfer id identifies a transfer only together with its direction, so id 1 exists twice on one session: once
    /// for what this end sends and once for what the peer sends. <c>BulkEngine.ApplyCancel</c> resolves an incoming
    /// <c>BulkCancel</c> with <c>FindSend(transferId)</c> alone, which searches this end's <em>send</em> records only and
    /// ignores the direction the frame was about. Both ends emit <c>BulkCancel</c> with their own id
    /// (docs/design/session-layer.md §7.7: the sender's <c>Cancel()</c> "resets the stream, sends <c>BulkCancel</c>",
    /// and the receiver's <c>CancelBulk</c> "sends the same <c>BulkCancel</c>"), so a <em>sender</em>-initiated cancel of
    /// the transfer it is pushing arrives at a peer that reads it as "cancel the transfer you are sending with that id"
    /// and kills an unrelated, healthy transfer running the other way.
    /// </para>
    /// <para>
    /// The receiving direction needs no such frame to end: the sender resets its stream at the same time, and
    /// <c>OnStreamClosed</c> already finishes that receive transfer <see cref="BulkStatus.Canceled"/>. So the collateral
    /// cancellation is pure damage, and it is trivially reachable — any session with bulk traffic in both directions
    /// numbers both first transfers 1.
    /// </para>
    /// <para>
    /// The existing tests miss it because bulk only ever flows one way in them:
    /// <c>BulkStreamTests.The_Sender_Cancels_Its_Own_Transfer_And_The_Receiver_Is_Told</c> and
    /// <c>The_Receiver_Cancels_A_Transfer_And_The_Sender_Stops_Reading_Its_Source</c> both give the router to the server
    /// only, so the peer that receives the <c>BulkCancel</c> has no outbound transfer for the id to collide with.
    /// </para>
    /// <para>
    /// Fix: make the frame say which direction it means (a sender-initiated cancel is already carried by its stream
    /// reset, so it need not be applied to the peer's send side at all), or resolve an incoming <c>BulkCancel</c> against
    /// the receive records first and apply it to a send record only when no receive transfer of that id is live.
    /// </para>
    /// </summary>
    [Fact]
    public async Task A_Senders_BulkCancel_Must_Not_Cancel_The_Peers_Own_Outbound_Transfer()
    {
        AcceptRouter atClient = AcceptRouter.Pattern();
        AcceptRouter atServer = AcceptRouter.Pattern();
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 4_000_000 },
            table: BulkTables.Main,
            client: BulkKit.Receiver(atClient),
            server: BulkKit.Receiver(atServer));

        BulkTransfer fromClient = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * Mib), new PatternSource(4 * Mib));
        BulkTransfer fromServer = await h.Server!.BeginBulkSendAsync(new BulkDescriptor(5, 2, 1, 4 * Mib), new PatternSource(4 * Mib));

        // Ids are scoped to (peer, direction), so the first transfer of each direction is id 1.
        Assert.Equal(fromClient.TransferId, fromServer.TransferId);
        Assert.True(
            h.RunUntil(() => fromClient.BytesTransferred > 0 && fromServer.BytesTransferred > 0),
            "bulk did not start moving in both directions");

        fromClient.Cancel();
        Assert.True(h.RunUntil(() => fromClient.IsFinished), "the cancellation did not finish the client's own transfer");
        h.Run(200_000);

        Assert.Equal(BulkStatus.Canceled, fromClient.Status);

        // The server's transfer shares the id but not the direction, and nobody asked for it to stop.
        Assert.NotEqual(BulkStatus.Canceled, fromServer.Status);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    /// <summary>
    /// ADR 0008 ("everything disposable disposed"; resources released exactly once on <em>every</em> path) and
    /// PROTOCOL.md §4.3, which gives a bulk transfer terminal states — <see cref="BulkStatus.Disconnected"/> among them —
    /// so that a caller awaiting it is always released.
    /// <para>
    /// The engine finishes its live transfers from <c>OnPeerClosed</c>, but that is dispatched only by
    /// <c>QuiclyPeer.FinishClosed</c> inside <see cref="QuiclyPeer.Poll"/> (and by the reconnect path).
    /// <see cref="QuiclyPeer.Dispose"/> takes neither route: it calls <c>FailWaitersOnDispose</c>, which covers the send
    /// and flush waiters only, and then frees the peer's memory. <c>BulkEngine.Dispose</c> disposes the native tables and
    /// the rings but never touches <c>_transfers</c>, so every outstanding <see cref="BulkTransfer"/> keeps its
    /// <c>TaskCompletionSource</c> unresolved for good: <see cref="BulkTransfer.Status"/> stays
    /// <see cref="BulkStatus.Running"/> and anyone who wrote <c>await transfer.Completion</c> — the documented way to wait
    /// for an object — hangs forever on a peer that was disposed rather than closed.
    /// </para>
    /// <para>
    /// Disposing without closing first is ordinary teardown (a host shutting down, a <c>using</c> block leaving on an
    /// exception), and every other await on the peer is released on that path, so bulk is the one promise that escapes it.
    /// </para>
    /// <para>
    /// The existing tests miss it because no bulk test ever awaits <c>Completion</c>: the suites poll
    /// <c>IsFinished</c>/<c>Status</c> instead, and
    /// <c>BulkEngineTests.Disposing_A_Peer_Mid_Transfer_Releases_What_It_Was_Staging</c> disposes the <em>receiving</em>
    /// peer and then cancels the sender's transfer by hand, which hides exactly this.
    /// </para>
    /// <para>
    /// Fix: finish the engine's live transfers <see cref="BulkStatus.Disconnected"/> from <c>BulkEngine.Dispose</c> (or
    /// dispatch <c>OnPeerClosed</c> from <c>QuiclyPeer.Dispose</c> before freeing), so the terminal state is reported on
    /// the dispose path as it is on the close path.
    /// </para>
    /// </summary>
    [Fact]
    public async Task Disposing_A_Peer_Mid_Transfer_Must_Finish_The_Transfer_It_Was_Sending()
    {
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 4_000_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(AcceptRouter.Pattern()));

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * Mib), new PatternSource(4 * Mib));
        Assert.True(h.RunUntil(() => transfer.BytesTransferred > 0), "the transfer never started moving");
        Assert.False(transfer.IsFinished);

        // Ordinary teardown of a peer that is still sending: no Close, so no Poll ever runs FinishClosed.
        h.DisposeClient();
        h.Run(20_000);

        Assert.True(transfer.IsFinished, $"the sending peer was disposed and the transfer stayed {transfer.Status}");
        Assert.True(
            transfer.Completion.IsCompleted,
            "BulkTransfer.Completion never completed, so a caller awaiting the object hangs for good");
    }
}
