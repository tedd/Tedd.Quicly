using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// <see cref="PeerOptions.ThreadSafeSend"/>: sends from other threads go through the Vyukov multi-producer front and are
/// admitted by the game thread at the start of Poll and Flush (ARCHITECTURE.md §3).
/// </summary>
public unsafe class ThreadSafeSendTests
{
    private static readonly ChannelTable Table = OrderedTables.Main;

    [Fact]
    public void Four_Threads_Send_And_Every_Message_Arrives_In_Per_Thread_Order()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            OrderedKit.Roomy(o);
            o.ThreadSafeSend = true;
        }, server: OrderedKit.Roomy);
        const int Threads = 4;
        const int PerThread = 2_000;
        int[] next = new int[Threads];
        int received = 0;
        string? failure = null;
        h.Server!.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            int thread = payload[0];
            int sequence = BitConverter.ToInt32(payload.Slice(1));
            if (failure is null && sequence != next[thread])
            {
                failure = $"thread {thread}: got {sequence}, expected {next[thread]}";
            }

            next[thread] = sequence + 1;
            received++;
        });
        QuiclyPeer client = h.Client;
        client.Poll();
        using Barrier start = new(Threads + 1);
        Thread[] producers = new Thread[Threads];
        for (int t = 0; t < Threads; t++)
        {
            int id = t;
            producers[t] = new Thread(() =>
            {
                // The buffer is reused at once: the front copies the payload during the call.
                byte[] message = new byte[5 + (id * 10)];
                message[0] = (byte)id;
                start.SignalAndWait();
                for (int i = 0; i < PerThread; i++)
                {
                    BitConverter.TryWriteBytes(message.AsSpan(1), i);
                    SendResult result;
                    while ((result = client.SendCopy(new SendHeader(4), message)).Status == SendStatus.QueueFull)
                    {
                        Thread.Yield();
                    }

                    if (result.Status != SendStatus.Admitted)
                    {
                        failure ??= $"thread {id}: {result.Status}";
                        return;
                    }
                }
            })
            {
                IsBackground = true,
            };
            producers[t].Start();
        }

        start.SignalAndWait();
        Assert.True(h.RunUntil(() => received == Threads * PerThread || failure is not null, 120_000_000));
        foreach (Thread producer in producers)
        {
            Assert.True(producer.Join(TimeSpan.FromSeconds(30)));
        }

        Assert.Null(failure);
        Assert.Equal(Threads * PerThread, received);
        PeerStatistics stats = DatagramKit.Statistics(client);
        Assert.Equal(Threads * PerThread, stats.ThreadSafeSends);
        Assert.Equal(0, stats.ThreadSafeSendDrops);
    }

    [Fact]
    public void An_Immediate_Send_From_Another_Thread_Runs_Its_Pass_On_The_Game_Thread()
    {
        FlagRecordingConnector? recorder = null;
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.ThreadSafeSend = true;
        }, server: DatagramKit.Quiet, connector: c => recorder = new FlagRecordingConnector(c));
        FlagRecordingTransport transport = recorder!.Transport!;
        h.Run(10_000);
        transport.Datagrams.Clear();
        SendResult result = default;
        Thread producer = new(() => result = h.Client.SendCopy(new SendHeader(2), [7], SendOptions.Immediate));
        producer.Start();
        Assert.True(producer.Join(TimeSpan.FromSeconds(10)));
        Assert.Equal(SendStatus.Admitted, result.Status);
        Assert.False(result.Token.IsValid);

        // Nothing was handed to the transport on the producing thread; the game thread's Poll (no Flush) runs the pass.
        Assert.Empty(transport.Datagrams);
        h.Client.Poll();
        (TransportSendFlags flags, int _) = Assert.Single(transport.Datagrams);
        Assert.Equal(TransportSendFlags.Priority, flags & TransportSendFlags.Priority);
    }

    [Fact]
    public void Tracked_Sends_From_Other_Threads_Are_Not_Supported_And_Game_Thread_Sends_Stay_Direct()
    {
        using SessionHarness h = new(table: Table, client: o => o.ThreadSafeSend = true);
        SendResult foreign = default;
        Thread producer = new(() => foreign = h.Client.SendCopy(new SendHeader(4), [1], SendOptions.Tracked));
        producer.Start();
        Assert.True(producer.Join(TimeSpan.FromSeconds(10)));
        Assert.Equal(SendStatus.NotSupported, foreign.Status);
        SendResult direct = h.Client.SendCopy(new SendHeader(4), [2], SendOptions.Tracked);
        Assert.True(direct.IsAdmitted);
        Assert.True(direct.Token.IsValid);
    }

    [Fact]
    public void Every_Send_Path_Works_From_Another_Thread()
    {
        using SessionHarness h = new(table: Table, client: o => o.ThreadSafeSend = true);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));
        byte[] borrowed = [3, 3, 3];
        byte[]? gathered = null;
        SendStatus[] statuses = new SendStatus[5];
        Thread producer = new(() =>
        {
            statuses[0] = h.Client.SendCopy(new SendHeader(4), [1]).Status;
            BufferLease lease = h.Client.RentBuffer(2);
            h.Client.GetBufferSpan(lease).Slice(0, 2).Fill(2);
            statuses[1] = h.Client.SendOwned(new SendHeader(4), lease, 2).Status;
            statuses[2] = h.Client.SendBorrowed(new SendHeader(4), borrowed).Status;
            byte value = 4;
            statuses[3] = h.Client.SendPinned(new SendHeader(4), &value, 1).Status;
            BufferLease a = h.Client.RentBuffer(1);
            BufferLease b = h.Client.RentBuffer(1);
            h.Client.GetBufferSpan(a).Fill(5);
            h.Client.GetBufferSpan(b).Fill(6);
            gathered = [.. h.Client.GetBufferSpan(a), .. h.Client.GetBufferSpan(b)];
            statuses[4] = h.Client.SendGather(new SendHeader(4), [a, b]).Status;
        });
        producer.Start();
        Assert.True(producer.Join(TimeSpan.FromSeconds(10)));
        Assert.All(statuses, status => Assert.Equal(SendStatus.Admitted, status));
        Assert.True(h.RunUntil(() => got.Count == 5));
        Assert.Equal(new byte[] { 1 }, got[0].Payload);
        Assert.Equal(new byte[] { 2, 2 }, got[1].Payload);
        Assert.Equal(borrowed, got[2].Payload);
        Assert.Equal(new byte[] { 4 }, got[3].Payload);
        Assert.Equal(gathered, got[4].Payload);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(h.Client).SendBytesOutstanding == 0));
    }

    [Fact]
    public void Sends_The_Game_Thread_Can_No_Longer_Admit_Are_Dropped_And_Counted()
    {
        using SessionHarness h = new(table: Table, client: o => o.ThreadSafeSend = true);
        Thread producer = new(() =>
        {
            for (int i = 0; i < 10; i++)
            {
                h.Client.SendCopy(new SendHeader(4), [1]);
            }
        });
        producer.Start();
        Assert.True(producer.Join(TimeSpan.FromSeconds(10)));

        // The session closes before the game thread admits them.
        h.Client.Close();
        Assert.True(h.RunUntilClosed());
        PeerStatistics stats = DatagramKit.Statistics(h.Client);
        Assert.Equal(10, stats.ThreadSafeSends);
        Assert.Equal(10, stats.ThreadSafeSendDrops);
        Assert.Equal(0, stats.SendBytesOutstanding);
    }
}
