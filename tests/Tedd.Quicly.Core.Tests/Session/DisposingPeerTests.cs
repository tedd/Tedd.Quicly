using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Disposing a peer that was never closed releases every await it handed out, once, in one sequence (ADR 0008;
/// docs/design/session-layer.md §7.7 and §7.8): the <c>SendAsync</c>/<c>FlushAsync</c> waiters, then each engine's
/// <c>ChannelEngine.OnDisposing</c> in mode order — the ordered engine's pending requests, then the bulk engine's outgoing
/// transfers — then every tracked send's wait from the completion table. Waves C2c (bulk) and C2d (request/response) each
/// tested their own half of that sequence; this test has both halves in flight at once.
/// </summary>
public class DisposingPeerTests
{
    private const int Mib = 1024 * 1024;

    /// <summary>2 unordered datagrams · 5 bulk at priority 0 · 10 ordered request/response.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(5, "world", ChannelMode.Bulk, o => o.Priority = 0)
        .Add(10, "rpc", ChannelMode.ReliableOrdered, o => o.RequestResponse = true)
        .Build();

    [Fact]
    public async Task Disposing_An_Unclosed_Peer_Releases_A_Pending_Transfer_Request_And_Tracked_Send_Exactly_Once()
    {
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, BandwidthBitsPerSecond = 4_000_000 },
            table: Table,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(AcceptRouter.Pattern()));
        QuiclyPeer client = h.Client;

        // The server takes the request and never answers it, so it stays pending for as long as the client lives.
        int requestsSeen = 0;
        h.Server!.RegisterHandler(10, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => requestsSeen++);

        BulkTransfer transfer = await client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 4 * Mib), new PatternSource(4 * Mib));
        ValueTask<ReceiveLease> request = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        Assert.True(
            h.RunUntil(() => transfer.BytesTransferred > 0 && requestsSeen == 1),
            "the transfer never started moving, or the request never reached the server");
        Assert.False(transfer.IsFinished);
        Assert.False(request.IsCompleted);
        Assert.Equal(1, OrderedKit.Engine(client).OutstandingRequests);

        // Buffered, not flushed (PROTOCOL.md §4.5), so the tracked send is still the engine's when the peer is disposed.
        SendToken token = client.SendCopy(new SendHeader(2), [1, 2, 3], new SendOptions { Track = true }).Token;
        ValueTask<DeliveryStatus> tracked = client.WaitAsync(token, CompletionStage.RemoteAccepted);
        Assert.False(tracked.IsCompleted);

        // With CompletionMode.PollOnly the request's continuation runs inline, inside Dispose, so it sees the peer exactly as
        // the dispose sequence left it at that step.
        int requestCompletions = 0;
        bool? disposedSeen = null;
        bool? transferFinishedSeen = null;
        bool? trackedCompletedSeen = null;
        Exception? reentry = null;

        // Not an await of the test method: ConfigureAwait(false) only keeps the registration from capturing the test
        // framework's scheduling context, which would post the continuation instead of running it inline.
#pragma warning disable xUnit1030
        request.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
#pragma warning restore xUnit1030
        {
            requestCompletions++;
            disposedSeen = client.IsDisposed;
            transferFinishedSeen = transfer.IsFinished;
            trackedCompletedSeen = tracked.IsCompleted;
            reentry = Record.Exception(() => { _ = client.SendRequestAsync(new SendHeader(10), new byte[] { 2 }, TimeSpan.Zero); });
        });

        // Ordinary teardown of a peer that is still sending and still waiting: no Close, so no Poll ever runs FinishClosed.
        h.DisposeClient();

        // Every await is released by Dispose itself, before a single network step.
        Assert.Equal(1, requestCompletions);
        Assert.True(request.IsCompleted, "a pending request outlived the Dispose of its peer");
        Assert.Throws<ObjectDisposedException>(() => request.GetAwaiter().GetResult());
        Assert.True(transfer.Completion.IsCompleted, "a pending bulk transfer outlived the Dispose of its peer");
        Assert.Equal(BulkStatus.Disconnected, transfer.Status);
        Assert.True(tracked.IsCompleted, "the wait on a tracked send outlived the Dispose of its peer");
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);

        // The order: the peer was already disposed when the first await was released, so a continuation cannot start new work
        // on it; and the engines ran in mode order, the ordered engine's requests before the bulk engine's transfers, both
        // before the completion table.
        Assert.True(disposedSeen, "an await was released before the peer was marked disposed");
        Assert.IsType<ObjectDisposedException>(reentry);
        Assert.False(transferFinishedSeen, "the bulk transfer finished before the request failed");
        Assert.False(trackedCompletedSeen, "the tracked send completed before the request failed");

        // The transport's close callback frees the peer's memory, and the engines' Dispose runs then. It must find nothing left
        // to complete: a second completion of the request's pooled source would throw on the transport thread, and a second
        // Finish must not replace the transfer's outcome.
        h.Network.RunUntilIdle(1_000_000);
        Assert.True(client.IsFreed, "the transport's close callback never freed the disposed peer");
        Assert.Equal(1, requestCompletions);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);
        Assert.Equal(BulkStatus.Disconnected, transfer.Status);
        Assert.Equal(BulkStatus.Disconnected, (await transfer.Completion).Status);
        Assert.Equal(DeliveryStatus.Disconnected, await tracked);
    }
}
