using System.Buffers.Binary;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The rules the wave C2c review tightened (PROTOCOL.md §3.3, §3.4, §4.1, §7, §8; docs/design/session-layer.md §7.7): what
/// a peer's progress may claim and what happens when it claims the impossible, which direction <c>BulkCancel</c> travels
/// and what a sender says instead, what a disposed peer still owes a receiving application, how long a transfer id stays
/// taken, and how a range request leaves the request table.
/// </summary>
public class BulkRulesTests
{
    private const int Mib = 1024 * 1024;
    private static readonly byte[] SessionToken = [9, 8, 7, 6];

    private static LinkOptions Slow() => new() { DelayMicros = 5_000, BandwidthBitsPerSecond = 4_000_000 };

    /// <summary>
    /// B1, the control-stream half (PROTOCOL.md §3.4 control-message bounds: a field no honest peer can produce is a
    /// malformed frame — <c>ProtocolViolation</c> on the control stream, drop + count for a control datagram). A
    /// <c>BulkProgress</c> may travel on either carrier (§2.3's fallback), and a claim above the bytes this end ever handed
    /// to the transport is exactly such a field, so on the stream it closes the connection like a <c>LatestAck</c> or a
    /// <c>BulkRequest</c> naming a channel of the wrong mode does. The datagram half is
    /// <see cref="BulkLimitTests.An_Over_Claimed_Progress_Frame_Is_Counted_And_An_Early_Honest_One_Is_Not"/>.
    /// </summary>
    [Fact]
    public async Task An_Over_Claim_On_The_Control_Stream_Closes_The_Connection_As_A_Protocol_Violation()
    {
        using SessionHarness h = new(link: Slow(), table: BulkTables.Main, client: BulkKit.Quiet, server: BulkKit.Receiver(AcceptRouter.Pattern()));
        BulkEngine engine = BulkKit.Engine(h.Client);
        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * Mib), new PatternSource(4 * Mib));

        // Applied at the start of the next pass, before this end has read one byte of the object.
        Assert.True(engine.OnControl(ControlType.BulkProgress, Progress(transfer.TransferId, 4 * Mib), onStream: true, 0));
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed), "the violation did not close the connection");

        Assert.Equal(QuiclyErrorCode.ProtocolViolation, h.Client.CloseReason.Code);
        Assert.Equal(CloseSource.Local, h.Client.CloseReason.Source);
        Assert.Equal(1, engine.ProgressOverClaims);
        Assert.Equal(1, DatagramKit.Statistics(h.Client).BulkProgressOverClaims);

        // The claim delivered nothing: the connection's end finished the transfer, not the forged confirmation.
        Assert.Equal(BulkStatus.Disconnected, transfer.Status);
        Assert.Equal(0, transfer.BytesTransferred);
    }

    /// <summary>
    /// B2 (PROTOCOL.md §3.4): <c>BulkCancel</c> is receiver-to-sender, so the recipient resolves it against its own
    /// <em>send</em> records, and one that names nothing there is ignored and counted. The id it carries here is the id of
    /// the transfer this end is <em>receiving</em> — ids are scoped per direction, so that transfer must not be touched.
    /// </summary>
    [Fact]
    public async Task A_BulkCancel_That_Names_No_Transfer_This_End_Sends_Is_Ignored_And_Counted()
    {
        AcceptRouter atClient = AcceptRouter.Pattern();
        using SessionHarness h = new(link: Slow(), table: BulkTables.Main, client: BulkKit.Receiver(atClient), server: BulkKit.Quiet);

        BulkTransfer fromServer = await h.Server!.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * Mib), new PatternSource(4 * Mib));
        Assert.True(h.RunUntil(() => atClient.Accepted.Count == 1 && atClient.Sink<PatternSink>().BytesWritten > 0), "nothing arrived");

        BulkEngine engine = BulkKit.Engine(h.Client);
        Assert.True(engine.OnControl(ControlType.BulkCancel, Cancel(fromServer.TransferId), onStream: true, 0));
        h.Run(50_000);

        Assert.Equal(1, DatagramKit.Statistics(h.Client).BulkCancelsIgnored);
        Assert.False(atClient.Sink<PatternSink>().IsFinished);
        Assert.False(fromServer.IsFinished);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server!.State);

        fromServer.Cancel();
        h.Run(100_000);
    }

    /// <summary>
    /// B2, the sender's other signal (PROTOCOL.md §3.4): a sender that abandons a transfer it serves for the peer's
    /// <c>BulkRequest</c> before any stream of it exists has no stream to reset, so it answers <c>BulkReject</c>. Here the
    /// provider's source runs dry on the very first read, which is before the transfer's stream is opened — without the
    /// reject the requester would wait for an answer for the life of the connection, holding a request slot.
    /// </summary>
    [Fact]
    public void A_Served_Range_Whose_Source_Runs_Dry_Before_Its_Stream_Exists_Is_Answered_BulkReject()
    {
        AcceptRouter router = AcceptRouter.Memory(1000);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Receiver(router),
            server: BulkKit.Server(new DryProvider()));

        h.Client.RequestBulk(new BulkRangeRequest(5, 3, 1, 0, 1000));
        Assert.True(h.RunUntil(() => router.Rejections.Count > 0), "the abandoned request was never answered");

        Assert.Equal(QuiclyErrorCode.InternalError, router.Rejections[0].Code);
        Assert.Equal(3UL, router.Rejections[0].Request.ObjectId);
        Assert.Empty(router.Accepted);
        Assert.Equal(0, BulkKit.Engine(h.Client).PendingRequests);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    /// <summary>
    /// B3, the receiving half (ADR 0008; <see cref="IBulkSink.Finish"/> is called exactly once per accepted transfer). The
    /// dispose hook runs while the transport may still call back into the receive records, so it leaves them alone; the
    /// peer finishes what was half received — and returns the staging lease of a half-arrived compressed chunk — when it
    /// frees its memory, which is once the transport has reported its close.
    /// </summary>
    [Fact]
    public void Disposing_A_Receiving_Peer_Finishes_Its_Sink_Once_Its_Transport_Has_Closed()
    {
        FinishCountingSink sink = new();
        AcceptRouter router = new(_ => sink);
        using ServerHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main, server: BulkKit.Receiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        // A compressed chunk of which only a part arrives: its staging lease is held when the peer is disposed.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.ChunkedStream(5, 1, 150_000, 160_000, 1_000), out _));
        Assert.True(h.RunUntil(() => h.Statistics().ReceiveBytesOutstanding > 0), "the chunk was not staged");

        h.DisposeServer();
        Assert.Equal(0, sink.Finishes);

        h.Run(20_000);
        Assert.Equal(1, sink.Finishes);
        Assert.Equal(BulkStatus.Disconnected, sink.Result!.Value.Status);
        Assert.Equal(0, sink.Result!.Value.BytesTransferred);
    }

    /// <summary>
    /// N6 (PROTOCOL.md §3.3, §8): a transfer id is unique per peer and direction for the epoch. The receiver enforces it
    /// over every transfer it still holds any trace of — a finished transfer keeps its id until its record has travelled
    /// back through the retire/recycle rings — and documents the tolerance beyond that: once the record is recycled the id
    /// may be used again, because remembering every id an epoch has seen would be unbounded state a peer controls.
    /// </summary>
    [Fact]
    public void A_Transfer_Id_Stays_Taken_Until_Its_Record_Is_Recycled()
    {
        AcceptRouter router = new(_ => new CountingSink());
        using ServerHarness h = new(link: new LinkOptions { DelayMicros = 1_000 }, table: BulkTables.Main, server: BulkKit.Receiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");
        BulkEngine engine = BulkKit.Engine(h.Server!);
        BulkHeader header = new() { TransferId = 77, ObjectId = 1, ObjectVersion = 1, TotalLength = 64, Offset = 0, Length = 64 };

        // The first transfer arrives whole and its stream closes, all on the transport thread: the network runs without the
        // server's game thread, which is what would report it and recycle its record.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.BulkStream(5, in header, new byte[64]), out _, fin: true));
        h.Network.Advance(10_000);
        Assert.Single(router.Accepted);
        Assert.Equal(BulkStatus.Completed, ((CountingSink)router.Sinks[0]).Result!.Value.Status);
        Assert.Equal(0, engine.ReceiveTransfers);

        // Finished, closed, not yet recycled: the id is still taken, so a second transfer with it is reset.
        long resets = h.Statistics().StreamsReset;
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.BulkStream(5, in header, new byte[64]), out _, fin: true));
        h.Network.Advance(10_000);
        Assert.Single(router.Accepted);
        Assert.True(h.Statistics().StreamsReset > resets, "a reused id was accepted while the transfer that had it was still held");

        // The game thread sends the final progress and recycles the record; from then on the id may be used again.
        h.Run(20_000);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(BulkKit.BulkStream(5, in header, new byte[64]), out _, fin: true));
        Assert.True(h.RunUntil(() => router.Accepted.Count == 2), "the id was not free once its record was recycled");
        Assert.Equal(BulkStatus.Completed, ((CountingSink)router.Sinks[1]).Result!.Value.Status);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    /// <summary>
    /// N5 (PROTOCOL.md §7): the outbound range-request table holds <c>BulkTransfersPerDirection + 1</c> requests, so one more
    /// range can be asked for while every transfer runs; a request beyond it is counted and dropped, never queued.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    public void The_Outbound_Request_Table_Holds_One_More_Than_The_Transfer_Limit(int transfers)
    {
        using SessionHarness h = new(
            table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Quiet(o);
                o.BulkTransfersPerDirection = transfers;
            },
            server: BulkKit.Quiet);

        for (int i = 0; i < 8; i++)
        {
            h.Client.RequestBulk(new BulkRangeRequest(5, (ulong)i, 1, 0, 10));
        }

        Assert.Equal(transfers + 1, BulkKit.Engine(h.Client).PendingRequests);
        Assert.Equal(8 - (transfers + 1), BulkKit.Stats(h.Client, 5).QueueFull);
    }

    /// <summary>
    /// A request the peer serves leaves the table when the transfer that answers it finishes (docs/design/session-layer.md
    /// §7.7). Before this, only a <c>BulkReject</c> or a closed connection released an entry, so the table filled with
    /// answered requests and every <see cref="QuiclyPeer.RequestBulk"/> after the third served one was silently refused.
    /// </summary>
    [Fact]
    public void A_Served_Request_Leaves_The_Request_Table()
    {
        byte[] payload = new byte[4_000];
        for (int i = 0; i < payload.Length; i++)
        {
            payload[i] = PatternSource.At(i);
        }

        MemoryProvider provider = new(payload);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Receiver(router),
            server: BulkKit.Server(provider));
        BulkEngine engine = BulkKit.Engine(h.Client);

        // Twice the table's three entries, one after the other.
        for (int i = 0; i < 6; i++)
        {
            h.Client.RequestBulk(new BulkRangeRequest(5, (ulong)(10 + i), 1, 0, payload.Length));
            Assert.Equal(1, engine.PendingRequests);
            int served = i + 1;
            Assert.True(
                h.RunUntil(() => router.Sinks.Count == served && ((MemorySink)router.Sinks[served - 1]).IsFinished && engine.PendingRequests == 0),
                $"request {i} was not served and settled");
            Assert.Equal(BulkStatus.Completed, router.Sink<MemorySink>(i).Result!.Value.Status);
            Assert.Equal(payload, router.Sink<MemorySink>(i).Bytes);
        }

        Assert.Equal(0, BulkKit.Stats(h.Client, 5).QueueFull);
        Assert.Equal(6, provider.Requests.Count);
    }

    /// <summary>
    /// PROTOCOL.md §4.1: "Bulk transfers marked <c>Resumable</c> are re-requested by the library for the remaining range."
    /// A resumable request whose answer the connection cut keeps the part still missing, and the resumed session asks for
    /// exactly that — not the whole range again — so the object arrives byte-exact across the two connections.
    /// </summary>
    [Fact]
    public void A_Resumable_Request_Cut_By_A_Disconnect_Is_Asked_Again_For_The_Missing_Part_Only()
    {
        const int total = 600_000;
        byte[] payload = new byte[total];
        for (int i = 0; i < total; i++)
        {
            payload[i] = PatternSource.At(i);
        }

        MemoryProvider provider = new(payload);
        MemorySink sink = new(total);
        AcceptRouter router = new(_ => sink);
        using SessionHarness h = new(connect: false, link: Slow(), table: BulkTables.Main, client: BulkKit.Receiver(router), server: BulkKit.Server(provider));
        h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
            AdmissionResult.Accept(SessionToken, 4242, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer oldServer = h.Server!;
        BulkEngine engine = BulkKit.Engine(h.Client);

        h.Client.RequestBulk(new BulkRangeRequest(5, 8, 1, 0, total, Resumable: true));
        Assert.True(h.RunUntil(() => sink.BytesWritten > 100_000, 30_000_000), "the range never started arriving");

        // The link is lost the way a real one is: the server's transport closes without a QUICLY Close.
        oldServer.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed), "the client never saw the connection go");
        long delivered = sink.BytesWritten;
        Assert.True(delivered > 0 && delivered < total, $"{delivered} of {total} bytes arrived before the cut");
        Assert.Equal(BulkStatus.Disconnected, sink.Result!.Value.Status);
        Assert.Equal(1, engine.PendingRequests);

        h.Client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected), "the session did not resume");
        oldServer.Dispose();

        Assert.True(h.RunUntil(() => router.Accepted.Count == 2 && sink.Result is { Status: BulkStatus.Completed }, 30_000_000),
            "the missing part never arrived");

        Assert.Equal(2, provider.Requests.Count);
        Assert.Equal(delivered, provider.Requests[1].Offset);
        Assert.Equal(total - delivered, provider.Requests[1].Length);
        Assert.Equal(delivered, router.Accepted[1].Offset);
        Assert.Equal(payload, sink.Bytes);
        Assert.Equal(0, engine.PendingRequests);
    }

    private static byte[] Progress(ulong transferId, ulong bytesAccepted)
    {
        byte[] body = new byte[16];
        int position = VarInt.Write(body, transferId);
        position += VarInt.Write(body.AsSpan(position), bytesAccepted);
        return body.AsSpan(0, position).ToArray();
    }

    private static byte[] Cancel(ulong transferId)
    {
        byte[] body = new byte[16];
        int position = VarInt.Write(body, transferId);
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(position), (uint)QuiclyErrorCode.BulkCanceled);
        return body.AsSpan(0, position + 4).ToArray();
    }

    /// <summary>A provider whose object turns out to be empty at the first read: the transfer fails before it has a stream.</summary>
    private sealed class DryProvider : IBulkProvider
    {
        public bool TryGetObject(in BulkRequestInfo request, out BulkDescriptor descriptor, out IBulkSource? source)
        {
            descriptor = new BulkDescriptor(request.Channel, request.ObjectId, request.ObjectVersion, request.Length, 0, request.Length);
            source = new ShortSource(0);
            return true;
        }
    }

    /// <summary>A sink that counts its <see cref="IBulkSink.Finish"/> calls.</summary>
    private sealed class FinishCountingSink : IBulkSink
    {
        public int Finishes { get; private set; }

        public BulkResult? Result { get; private set; }

        public void Write(long objectOffset, ReadOnlySpan<byte> data)
        {
        }

        public void Finish(in BulkResult result)
        {
            Finishes++;
            Result = result;
        }
    }
}
