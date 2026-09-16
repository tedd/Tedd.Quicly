using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The asynchronous send surface on ordered channels (docs/design/session-layer.md §7.3): SendAsync waiting for admission,
/// the FlushAsync watermark, WaitAsync / Wait / GetDeliveryStatus of tracked sends and TryCancel.
/// </summary>
public class AsyncApiTests
{
    private static readonly ChannelTable Table = OrderedTables.Main;

    private static int FillLimitedChannel(QuiclyPeer peer, int size)
    {
        int id = 0;
        while (peer.SendCopy(new SendHeader(7), OrderedKit.Payload(id, size)).IsAdmitted)
        {
            id++;
        }

        return id;
    }

    [Fact]
    public async Task SendAsync_Completes_At_Once_When_The_Channel_Has_Room()
    {
        using SessionHarness h = new(table: Table);
        ValueTask<SendResult> send = h.Client.SendAsync(new SendHeader(4), new byte[] { 1 });
        Assert.True(send.IsCompletedSuccessfully);
        Assert.Equal(SendStatus.Admitted, (await send).Status);
    }

    [Fact]
    public async Task SendAsync_Waits_For_Room_On_A_Full_Channel_And_Completes_In_Call_Order()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 20_000 }, table: Table, client: OrderedKit.Roomy, server: OrderedKit.Roomy);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(7, Handlers.Collect(got));
        const int Size = 16 * 1024;
        int queued = FillLimitedChannel(h.Client, Size);
        Assert.Equal(4, queued); // 64 KiB queue limit, counting bytes in flight
        ValueTask<SendResult>[] waits = new ValueTask<SendResult>[3];
        for (int i = 0; i < waits.Length; i++)
        {
            waits[i] = h.Client.SendAsync(new SendHeader(7), OrderedKit.Payload(queued + i, Size));
        }

        Assert.All(waits, wait => Assert.False(wait.IsCompleted));

        // While sends wait, a synchronous send on the channel does not overtake them.
        Assert.Equal(SendStatus.QueueFull, h.Client.SendCopy(new SendHeader(7), [1]).Status);
        Assert.True(h.RunUntil(() => Array.TrueForAll(waits, wait => wait.IsCompleted)));
        foreach (ValueTask<SendResult> wait in waits)
        {
            Assert.Equal(SendStatus.Admitted, (await wait).Status);
        }

        Assert.True(h.RunUntil(() => got.Count == queued + waits.Length));
        for (int i = 0; i < got.Count; i++)
        {
            Assert.Equal(OrderedKit.Payload(i, Size), got[i].Payload);
        }
    }

    [Fact]
    public async Task A_Canceled_SendAsync_Is_Never_Admitted_And_Releases_The_Channel()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 20_000 }, table: Table, client: OrderedKit.Roomy, server: OrderedKit.Roomy);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(7, Handlers.Collect(got));
        int queued = FillLimitedChannel(h.Client, 16 * 1024);
        using CancellationTokenSource cancel = new();
        ValueTask<SendResult> wait = h.Client.SendAsync(new SendHeader(7), new byte[] { 99 }, cancellationToken: cancel.Token);
        Assert.False(wait.IsCompleted);
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await wait);
        Assert.True(h.RunUntil(() => got.Count == queued));
        h.Run(50_000);
        Assert.Equal(queued, got.Count);

        // No stale waiter holds the channel.
        Assert.True(h.Client.SendCopy(new SendHeader(7), [1]).IsAdmitted);
        ValueTask<SendResult> canceledAtOnce = h.Client.SendAsync(new SendHeader(7), new byte[] { 2 }, cancellationToken: cancel.Token);
        Assert.True(canceledAtOnce.IsCanceled);
    }

    [Fact]
    public async Task SendAsync_On_An_Unreliable_Channel_Answers_At_Once()
    {
        using SessionHarness h = new(table: Table, client: o => o.SendBudgetBytes = 64);
        ValueTask<SendResult> send = h.Client.SendAsync(new SendHeader(2), new byte[500]);
        Assert.True(send.IsCompletedSuccessfully);
        Assert.Equal(SendStatus.OutOfBuffers, (await send).Status);
    }

    [Fact]
    public async Task A_Waiting_SendAsync_Completes_NotConnected_When_The_Session_Closes()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 20_000 }, table: Table, client: OrderedKit.Roomy, server: OrderedKit.Roomy);
        FillLimitedChannel(h.Client, 16 * 1024);
        ValueTask<SendResult> wait = h.Client.SendAsync(new SendHeader(7), new byte[] { 1 });
        Assert.False(wait.IsCompleted);
        h.Client.Close();
        Assert.True(h.RunUntil(() => wait.IsCompleted));
        Assert.Equal(SendStatus.NotConnected, (await wait).Status);
    }

    [Fact]
    public async Task Disposing_The_Peer_Fails_Waiting_Sends_And_Flushes()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 20_000 }, table: Table, client: o =>
        {
            OrderedKit.Roomy(o);
            o.MaxSendBytesPerSecond = 10_000;
        }, server: OrderedKit.Roomy);
        FillLimitedChannel(h.Client, 16 * 1024);
        ValueTask<SendResult> wait = h.Client.SendAsync(new SendHeader(7), new byte[] { 1 });
        ValueTask flush = h.Client.FlushAsync();
        Assert.False(wait.IsCompleted);
        Assert.False(flush.IsCompleted);
        h.DisposeClient();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await wait);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await flush);
    }

    [Fact]
    public void FlushAsync_Is_Complete_At_Once_When_Everything_Was_Handed_Over()
    {
        using SessionHarness h = new(table: Table);
        Assert.True(h.Client.SendCopy(new SendHeader(4), [0]).IsAdmitted);
        Assert.True(h.RunUntil(() => OrderedKit.Phase(h.Client, 4) == ReliableOrderedEngine.StreamPhase.Open));
        Assert.True(h.Client.SendCopy(new SendHeader(4), [1]).IsAdmitted);
        Assert.True(h.Client.SendCopy(new SendHeader(2), [2]).IsAdmitted);
        ValueTask flush = h.Client.FlushAsync();
        Assert.True(flush.IsCompletedSuccessfully);
        Assert.Equal(0, OrderedKit.Stats(h.Client, 4).QueuedMessages);
    }

    [Fact]
    public async Task FlushAsync_Waits_Until_The_Send_Cap_Lets_Out_Everything_Admitted_Before_It()
    {
        using SessionHarness h = new(table: Table, client: o => o.MaxSendBytesPerSecond = 50_000);
        for (int i = 0; i < 20; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(i, 1_000)).IsAdmitted);
        }

        ValueTask flush = h.Client.FlushAsync();
        Assert.False(flush.IsCompleted);

        // Messages admitted after the call do not hold it back.
        for (int i = 20; i < 60; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(i, 1_000)).IsAdmitted);
        }

        Assert.True(h.RunUntil(() => flush.IsCompleted, 5_000_000));
        await flush;
        ChannelStatistics stats = OrderedKit.Stats(h.Client, 4);
        Assert.True(stats.Sent >= 20);
        Assert.True(stats.QueuedMessages > 0, "later messages were still queued when the flush completed");
    }

    [Fact]
    public async Task FlushAsync_Waits_For_Stream_Credit()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000 }, table: Table);
        QuiclyPeer server = h.Server!;
        Assert.True(server.SendCopy(new SendHeader(4), [1]).IsAdmitted);
        ValueTask flush = server.FlushAsync();
        Assert.False(flush.IsCompleted);
        Assert.True(h.RunUntil(() => flush.IsCompleted));
        await flush;
        Assert.Equal(ReliableOrderedEngine.StreamPhase.Open, OrderedKit.Phase(server, 4));
    }

    [Fact]
    public async Task WaitAsync_Reports_BufferReleased_And_Delivered_When_The_Peer_Acknowledged_The_Stream_Bytes()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000 }, table: Table);
        SendToken token = h.Client.SendCopy(new SendHeader(4), [1, 2, 3], SendOptions.Tracked).Token;
        ValueTask<DeliveryStatus> released = h.Client.WaitAsync(token, CompletionStage.BufferReleased);
        ValueTask<DeliveryStatus> delivered = h.Client.WaitAsync(token, CompletionStage.RemoteAccepted);
        h.Client.Flush();
        Assert.False(released.IsCompleted);
        Assert.False(delivered.IsCompleted);
        Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(token));
        Assert.True(h.RunUntil(() => released.IsCompleted));
        Assert.True(delivered.IsCompleted, "both stages complete when the stream bytes are acknowledged");
        Assert.Equal(DeliveryStatus.Delivered, await released);
        Assert.Equal(DeliveryStatus.Delivered, await delivered);
        Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(token));
        Assert.Equal(DeliveryStatus.Delivered, h.Client.Wait(token, CompletionStage.RemoteAccepted, TimeSpan.Zero));
    }

    [Fact]
    public async Task ThreadPool_Completion_Mode_Completes_Ordered_Tokens()
    {
        using SessionHarness h = new(table: Table, client: o => o.CompletionMode = CompletionMode.ThreadPool);
        SendToken token = h.Client.SendCopy(new SendHeader(4), [1], SendOptions.Tracked).Token;
        Task<DeliveryStatus> delivered = h.Client.WaitAsync(token, CompletionStage.RemoteAccepted).AsTask();

        // The stage completes on the simulated transport's thread and the task's continuation then runs on the thread pool,
        // in real time: wait for the stage in simulated time and for the task in real time.
        Assert.True(h.RunUntil(() => h.Client.Core.Completions.IsCompleted(token, CompletionStage.RemoteAccepted)));
        Assert.Equal(DeliveryStatus.Delivered, await delivered.WaitAsync(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public void TryCancel_Works_While_A_Message_Is_Queued_And_Not_After_Hand_Off()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000 }, table: Table);
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Client.RegisterHandler(4, Handlers.Collect(got));
        SendToken a = server.SendCopy(new SendHeader(4), [1], SendOptions.Tracked).Token;
        SendToken b = server.SendCopy(new SendHeader(4), [2], SendOptions.Tracked).Token;
        SendToken c = server.SendCopy(new SendHeader(4), [3], SendOptions.Tracked).Token;
        Assert.True(server.TryCancel(c)); // still queued
        server.Flush();
        Assert.False(server.TryCancel(a)); // handed to the transport with the start

        // The start is refused (no stream credit yet): the messages come back to the queue and can be canceled again.
        h.Network.Advance(0);
        server.Poll();
        Assert.Equal(ReliableOrderedEngine.StreamPhase.Blocked, OrderedKit.Phase(server, 4));
        Assert.True(server.TryCancel(b));
        Assert.False(server.TryCancel(b));
        server.Poll();
        Assert.Equal(DeliveryStatus.Canceled, server.GetDeliveryStatus(b));
        Assert.Equal(DeliveryStatus.Canceled, server.GetDeliveryStatus(c));
        Assert.True(h.RunUntil(() => got.Count == 1 && server.GetDeliveryStatus(a) == DeliveryStatus.Delivered));
        Assert.Equal(new byte[] { 1 }, got[0].Payload);
        Assert.False(server.TryCancel(a));
    }

    [Fact]
    public async Task SendAsync_Waits_For_The_Send_Budget_On_An_Ordered_Channel()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 20_000 }, table: Table, client: o => o.SendBudgetBytes = 16 * 1024);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        int queued = 0;
        SendStatus status;
        while ((status = h.Client.SendCopy(new SendHeader(4), OrderedKit.Payload(queued, 4_000)).Status) == SendStatus.Admitted)
        {
            queued++;
        }

        // Four 4 KiB leases fill the 16 KiB budget until the peer acknowledges them.
        Assert.Equal(SendStatus.OutOfBuffers, status);
        Assert.Equal(4, queued);
        ValueTask<SendResult> wait = h.Client.SendAsync(new SendHeader(4), OrderedKit.Payload(queued, 4_000));
        Assert.False(wait.IsCompleted);
        Assert.True(h.RunUntil(() => wait.IsCompleted));
        Assert.Equal(SendStatus.Admitted, (await wait).Status);
        Assert.True(h.RunUntil(() => got.Count == queued + 1));
        for (int i = 0; i <= queued; i++)
        {
            Assert.Equal(OrderedKit.Payload(i, 4_000), got[i].Payload);
        }
    }

    [Fact]
    public async Task SendAsync_From_Another_Thread_Waits_While_The_ThreadSafeSend_Front_Is_Full()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            o.ThreadSafeSend = true;
            o.SendTableCapacity = 16;
        });
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        QuiclyPeer client = h.Client;
        client.Poll();
        SendStatus first = default;
        int queued = 0;
        Task<SendResult>? waiting = null;
        Thread producer = new(() =>
        {
            ValueTask<SendResult> at = client.SendAsync(new SendHeader(4), new byte[] { 0 });
            first = at.IsCompleted ? at.Result.Status : SendStatus.QueueFull;
            queued = 1;
            while (client.SendCopy(new SendHeader(4), new byte[] { (byte)queued }).Status == SendStatus.Admitted)
            {
                queued++;
            }

            waiting = client.SendAsync(new SendHeader(4), new byte[] { (byte)queued }).AsTask();
        });
        producer.Start();
        Assert.True(producer.Join(TimeSpan.FromSeconds(10)));
        Assert.Equal(SendStatus.Admitted, first);
        Assert.NotNull(waiting);

        // The front stays full until the game thread drains it at its next Poll or Flush; the waiting send retries every
        // millisecond, in real time.
        Assert.False(waiting.IsCompleted);
        long deadline = Environment.TickCount64 + 30_000;
        while (!waiting.IsCompleted && Environment.TickCount64 < deadline)
        {
            h.Run(1_000);
            Thread.Sleep(1);
        }

        Assert.Equal(SendStatus.Admitted, (await waiting).Status);
        Assert.True(h.RunUntil(() => got.Count == queued + 1));
        for (int i = 0; i <= queued; i++)
        {
            Assert.Equal(new byte[] { (byte)i }, got[i].Payload);
        }
    }
}
