using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Receive ring, Poll dispatch, Drain/Release/Retain, decode and mailboxes, driven through a test engine.</summary>
public class ReceivePlumbingTests
{
    [Fact]
    public void Datagrams_Reach_The_Channel_Handler_In_Poll()
    {
        using SessionHarness h = TestEngines.Create(out TestEngine client, out TestEngine server);
        h.Server!.Index = 17;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server.RegisterHandler(2, Handlers.Collect(got));
        long sentAt = h.Network.NowMicros;
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), [1, 2, 3]).Status);
        Assert.True(h.RunUntil(() => got.Count == 1));
        Assert.Equal(new byte[] { 1, 2, 3 }, got[0].Payload);
        ReceiveHeader header = got[0].Header;
        Assert.Equal((ushort)2, header.Channel);
        Assert.Equal(3, header.Length);
        Assert.Equal(1u, header.Epoch);
        Assert.Equal(17, header.PeerIndex);
        Assert.Equal(ReceiveFlags.None, header.Flags);
        Assert.InRange(header.ReceivedMicros, sentAt, h.Network.NowMicros);
        Assert.Equal(1, server.Received);
        Assert.True(h.RunUntil(() => client.Completed == 1));
        Assert.Equal(DeliveryStatus.Delivered, client.LastStatus);
        Assert.True(h.Server.GetChannelStatistics(2, out ChannelStatistics stats));
        Assert.Equal((ushort)2, stats.Channel);
        Assert.Equal(1, stats.Received);
        h.Server.GetStatistics(out PeerStatistics peer);
        Assert.Equal(0, peer.ReceiveBytesOutstanding);
    }

    [Fact]
    public void Poll_Honours_MaxItems()
    {
        using SessionHarness h = TestEngines.Create(out _, out TestEngine server);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        for (int i = 0; i < 5; i++)
        {
            h.Client.SendCopy(new SendHeader(2), [(byte)i]);
        }

        h.Network.Advance(1_000);
        Assert.Equal(5, server.Received);
        Assert.Equal(0, h.Server.Poll(0));
        Assert.Equal(2, h.Server.Poll(2));
        Assert.Equal(3, h.Server.Poll());
        Assert.Equal(new byte[] { 0, 1, 2, 3, 4 }, got.Select(g => g.Payload[0]).ToArray());
    }

    [Fact]
    public void Drain_Returns_Messages_Of_A_Channel_Without_A_Handler_And_Release_Returns_The_Budget()
    {
        using SessionHarness h = TestEngines.Create(out _, out TestEngine server);
        for (int i = 0; i < 5; i++)
        {
            h.Client.SendCopy(new SendHeader(2), [(byte)i]);
        }

        h.Client.SendCopy(new SendHeader(3), [9]);
        h.Network.Advance(1_000);
        Assert.Equal(6, server.Received);
        Assert.Equal(0, h.Server!.Poll());
        ReceivedMessage[] buffer = new ReceivedMessage[8];
        Assert.Equal(5, h.Server.Drain(2, buffer));
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal((byte)i, buffer[i].Payload[0]);
            Assert.Equal((ushort)2, buffer[i].Header.Channel);
        }

        h.Server.GetStatistics(out PeerStatistics held);
        Assert.True(held.ReceiveBytesOutstanding > 0);
        h.Server.Release(buffer.AsSpan(0, 5));
        Assert.Equal(1, h.Server.Drain(3, buffer));
        Assert.Equal(9, buffer[0].Payload[0]);
        h.Server.Release(buffer.AsSpan(0, 1));
        Assert.Equal(0, h.Server.Drain(2, buffer));
        Assert.Equal(0, h.Server.Drain(2, Span<ReceivedMessage>.Empty));
        h.Server.GetStatistics(out PeerStatistics released);
        Assert.Equal(0, released.ReceiveBytesOutstanding);
        Assert.Throws<ArgumentException>(() => h.Server.Drain(99, new ReceivedMessage[1]));
    }

    [Fact]
    public void Drain_Queues_Messages_Of_Other_Channels_For_Their_Handler()
    {
        using SessionHarness h = TestEngines.Create(out _, out _);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(3, Handlers.Collect(got));
        h.Client.SendCopy(new SendHeader(3), [7]);
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.SendCopy(new SendHeader(3), [8]);
        h.Network.Advance(1_000);
        ReceivedMessage[] buffer = new ReceivedMessage[4];
        Assert.Equal(1, h.Server.Drain(2, buffer));
        Assert.Empty(got);
        h.Server.Release(buffer.AsSpan(0, 1));
        Assert.Equal(2, h.Server.Poll());
        Assert.Equal(new byte[] { 7, 8 }, got.Select(g => g.Payload[0]).ToArray());
    }

    [Fact]
    public void A_Handler_Registered_Later_Receives_The_Queued_Messages()
    {
        using SessionHarness h = TestEngines.Create(out _, out _);
        h.Client.SendCopy(new SendHeader(3), [1]);
        h.Client.SendCopy(new SendHeader(3), [2]);
        h.Network.Advance(1_000);
        Assert.Equal(0, h.Server!.Poll());
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server.RegisterHandler(3, Handlers.Collect(got));
        Assert.Throws<InvalidOperationException>(() => h.Server.RegisterHandler(3, Handlers.Collect(got)));
        Assert.Equal(2, h.Server.Poll());
        Assert.Equal(new byte[] { 1, 2 }, got.Select(g => g.Payload[0]).ToArray());
        Assert.True(h.Server.UnregisterHandler(3));
        Assert.False(h.Server.UnregisterHandler(3));
        h.Client.SendCopy(new SendHeader(3), [3]);
        h.Network.Advance(1_000);
        Assert.Equal(0, h.Server.Poll());
        h.Server.RegisterHandler(3, Handlers.Collect(got));
        h.Server.UnregisterHandler(3);
        ReceivedMessage[] buffer = new ReceivedMessage[2];
        Assert.Equal(1, h.Server.Drain(3, buffer));
        h.Server.Release(buffer.AsSpan(0, 1));
    }

    [Fact]
    public void Retained_Payload_Outlives_The_Handler()
    {
        using SessionHarness h = TestEngines.Create(out _, out _);
        ReceiveLease kept = default;
        bool secondRetainFailed = false;
        h.Server!.RegisterHandler(2, (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> _) =>
        {
            kept = peer.Retain(in header);
            try
            {
                peer.Retain(in header);
            }
            catch (InvalidOperationException)
            {
                secondRetainFailed = true;
            }
        });
        h.Client.SendCopy(new SendHeader(2), [5, 6, 7]);
        Assert.True(h.RunUntil(() => kept.IsValid));
        Assert.True(secondRetainFailed);
        Assert.Equal(new byte[] { 5, 6, 7 }, kept.Payload.ToArray());
        Assert.Equal(3, kept.Header.Length);
        h.Server.GetStatistics(out PeerStatistics held);
        Assert.True(held.ReceiveBytesOutstanding > 0);
        h.Server.Release(in kept);
        h.Server.GetStatistics(out PeerStatistics released);
        Assert.Equal(0, released.ReceiveBytesOutstanding);
    }

    [Fact]
    public void Retain_Is_Only_Valid_For_The_Message_Being_Handled()
    {
        using SessionHarness h = TestEngines.Create(out _, out _);
        Assert.Throws<InvalidOperationException>(() => h.Server!.Retain(new ReceiveHeader()));
        bool wrongHeaderFailed = false;
        bool copyAccepted = false;
        h.Server!.RegisterHandler(2, (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> _) =>
        {
            ReceiveHeader other = new() { Channel = header.Channel, Length = header.Length + 1 };
            try
            {
                peer.Retain(in other);
            }
            catch (InvalidOperationException)
            {
                wrongHeaderFailed = true;
            }

            ReceiveHeader copy = header;
            ReceiveLease lease = peer.Retain(in copy);
            copyAccepted = lease.IsValid;
            peer.Release(in lease);
        });
        h.Client.SendCopy(new SendHeader(2), [1]);
        Assert.True(h.RunUntil(() => copyAccepted));
        Assert.True(wrongHeaderFailed);
        h.Server.GetStatistics(out PeerStatistics stats);
        Assert.Equal(0, stats.ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Full_Receive_Ring_Drops_The_Newest_And_Counts()
    {
        using SessionHarness h = TestEngines.Create(out _, out TestEngine server, both: o => o.ReceiveRingCapacity = 4);
        for (int i = 0; i < 10; i++)
        {
            h.Client.SendCopy(new SendHeader(2), [(byte)i]);
        }

        h.Network.Advance(1_000);
        Assert.Equal(10, server.Received);
        h.Server!.GetStatistics(out PeerStatistics stats);
        Assert.Equal(6, stats.ReceiveRingDrops);
        Assert.Equal(4, stats.ReceiveRingHighWater);
        Assert.Equal(4, stats.ReceiveRingCapacity);
        Assert.True(h.Server.GetChannelStatistics(2, out ChannelStatistics channel));
        Assert.Equal(6, channel.RingDrops);
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        int n = h.Server.Drain(2, buffer);
        Assert.Equal(4, n);
        Assert.Equal(0, buffer[0].Payload[0]);
        Assert.Equal(3, buffer[3].Payload[0]);
        h.Server.Release(buffer.AsSpan(0, n));
    }

    [Fact]
    public void Drain_Queues_Are_Bounded_And_Keep_The_Order()
    {
        using SessionHarness h = TestEngines.Create(out _, out _, both: o => o.ReceiveRingCapacity = 2);
        byte next = 0;
        for (int round = 0; round < 3; round++)
        {
            h.Client.SendCopy(new SendHeader(2), [next++]);
            h.Client.SendCopy(new SendHeader(2), [next++]);
            h.Network.Advance(1_000);
            h.Server!.Poll();
        }

        ReceivedMessage[] buffer = new ReceivedMessage[16];
        int n = h.Server!.Drain(2, buffer);
        Assert.Equal(5, n);
        for (int i = 0; i < n; i++)
        {
            Assert.Equal((byte)i, buffer[i].Payload[0]);
        }

        h.Server.Release(buffer.AsSpan(0, n));
        h.Server.GetStatistics(out PeerStatistics stats);
        Assert.Equal(1, stats.ReceiveRingDrops);
        Assert.Equal(0, stats.ReceiveBytesOutstanding);
    }

    [Fact]
    public void Compressed_Messages_Are_Decoded_In_Poll()
    {
        using SessionHarness h = TestEngines.Create(out _, out _);
        byte[] payload = new byte[500];
        Array.Fill(payload, (byte)'a');
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(6, Handlers.Collect(got));
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(6), payload).Status);
        Assert.True(h.RunUntil(() => got.Count == 1));
        Assert.Equal(payload, got[0].Payload);
        Assert.Equal(500, got[0].Header.Length);
        Assert.Equal(500, got[0].Header.RawLength);
        Assert.Equal(ReceiveFlags.Compressed, got[0].Header.Flags);
        h.Server.GetStatistics(out PeerStatistics stats);
        Assert.Equal(0, stats.ReceiveBytesOutstanding);
        Assert.Equal(0, stats.DecodeFailures);

        // The same through Drain.
        h.Server.UnregisterHandler(6);
        h.Client.SendCopy(new SendHeader(6), payload);
        h.Network.Advance(1_000);
        ReceivedMessage[] buffer = new ReceivedMessage[2];
        Assert.Equal(1, h.Server.Drain(6, buffer));
        Assert.Equal(payload, buffer[0].Payload.ToArray());
        h.Server.Release(buffer.AsSpan(0, 1));
    }

    [Fact]
    public void Undecodable_Or_Over_Budget_Compressed_Messages_Are_Dropped()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, server: o => o.DecodedBytesPerSecond = 600);
        Assert.True(h.Admit());
        int handled = 0;
        h.Server!.RegisterHandler(6, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => handled++);
        h.Raw.SendDatagram([0x06, 0x40, 0x64, 0xFF, 0xFF, 0xFF]);
        h.Run(5_000);
        Assert.Equal(1, engine().Received);
        Assert.Equal(0, handled);
        Assert.Equal(1, h.Statistics().DecodeFailures);

        byte[] raw = new byte[1000];
        byte[] compressed = new byte[Lz4Block.GetMaxCompressedLength(raw.Length)];
        int length = Lz4Block.Compress(raw, compressed);
        h.Raw.SendDatagram([0x06, 0x43, 0xE8, .. compressed.AsSpan(0, length)]);
        h.Run(5_000);
        Assert.Equal(2, engine().Received);
        Assert.Equal(2, h.Statistics().DecodeFailures);
        Assert.Equal(0, handled);
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }

    [Fact]
    public void Mailboxes_Deliver_Only_The_Latest_Value_Per_Key()
    {
        using SessionHarness h = TestEngines.Create(out _, out TestEngine server, ChannelMode.UnreliableSequenced, mailbox: true);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(8, Handlers.Collect(got));
        for (byte v = 1; v <= 5; v++)
        {
            h.Client.SendCopy(new SendHeader(8, 1), [v]);
            h.Client.SendCopy(new SendHeader(8, 2), [(byte)(v + 10)]);
        }

        h.Network.Advance(1_000);
        Assert.Equal(10, server.Received);
        Assert.Equal(2, h.Server.Poll());
        Assert.Equal(2, got.Count);
        Assert.Contains(got, g => g.Header.Key == 1 && g.Payload[0] == 5);
        Assert.Contains(got, g => g.Header.Key == 2 && g.Payload[0] == 15);
        Assert.True(h.Server.GetChannelStatistics(8, out ChannelStatistics stats));
        Assert.Equal(8, stats.ReceiveSuperseded);
        h.Server.GetStatistics(out PeerStatistics peer);
        Assert.Equal(0, peer.ReceiveBytesOutstanding);
        Assert.Equal(0, h.Server.Poll(1));
    }

    [Fact]
    public void Drain_Takes_The_Latest_Values_From_Mailboxes_And_Close_Returns_The_Rest()
    {
        using SessionHarness h = TestEngines.Create(out _, out _, ChannelMode.UnreliableSequenced, mailbox: true);
        for (byte v = 1; v <= 3; v++)
        {
            h.Client.SendCopy(new SendHeader(8, 1), [v]);
            h.Client.SendCopy(new SendHeader(8, 2), [(byte)(v + 10)]);
        }

        h.Network.Advance(1_000);
        Assert.Equal(0, h.Server!.Poll());
        ReceivedMessage[] buffer = new ReceivedMessage[1];
        Assert.Equal(1, h.Server.Drain(8, buffer));
        h.Server.Release(buffer.AsSpan(0, 1));
        Assert.Equal(1, h.Server.Drain(8, buffer));
        Assert.Contains(buffer[0].Payload[0], new byte[] { 3, 13 });
        h.Server.Release(buffer.AsSpan(0, 1));
        Assert.Equal(0, h.Server.Drain(8, buffer));

        h.Client.SendCopy(new SendHeader(8, 3), [99]);
        h.Network.Advance(1_000);
        h.Server.GetStatistics(out PeerStatistics held);
        Assert.True(held.ReceiveBytesOutstanding > 0);
        h.Server.Close();
        Assert.True(h.RunUntil(() => h.Server.State == PeerState.Closed));
        h.Server.GetStatistics(out PeerStatistics closed);
        Assert.Equal(0, closed.ReceiveBytesOutstanding);
    }

    [Fact]
    public void The_Receive_Budget_Limits_What_The_Engines_Can_Hold()
    {
        using SessionHarness h = TestEngines.Create(out _, out _, both: o => o.ReceiveBudgetBytes = 64);
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.SendCopy(new SendHeader(2), [2]);
        h.Network.Advance(1_000);
        h.Server!.GetStatistics(out PeerStatistics stats);
        Assert.Equal(1, stats.OutOfReceiveBuffers);
        Assert.Equal(64, stats.ReceiveBytesOutstanding);
        Assert.True(h.Server.GetChannelStatistics(2, out ChannelStatistics channel));
        Assert.Equal(1, channel.OutOfBuffers);
    }

    [Fact]
    public void Queue_And_Mailbox_Leases_Return_To_The_Core()
    {
        using SessionHarness h = new(connect: false);
        PeerCore core = h.Client.Core;
        ReceiveQueues queues = new(4, core.ChannelCount);
        Assert.True(core.TryRentReceive(10, out BufferLease a));
        Assert.True(queues.TryAppend(0, new ReceiveEntry { Lease = a }));
        using ReceiveMailbox box = new(2, 0, 4);
        Assert.True(core.TryRentReceive(10, out BufferLease b));
        Assert.True(box.TryPost(1, new ReceiveEntry { Lease = b }, out _));
        Assert.True(core.ReceiveBytesOutstanding > 0);
        queues.ReleaseAll(core);
        box.ReleaseAll(core);
        Assert.Equal(0, core.ReceiveBytesOutstanding);
    }
}

/// <summary>Send entries, tokens, completion modes, budgets and the engine hooks.</summary>
public class SendPlumbingTests
{
    [Fact]
    public async Task Tracked_Send_Completes_In_Poll_In_PollOnly_Mode()
    {
        using SessionHarness h = TestEngines.Create(out TestEngine client, out _);
        SendResult result = h.Client.SendCopy(new SendHeader(2), [1], SendOptions.Tracked);
        Assert.True(result.IsAdmitted);
        Assert.True(result.Token.IsValid);
        ValueTask<DeliveryStatus> released = h.Client.WaitAsync(result.Token, CompletionStage.BufferReleased);
        ValueTask<DeliveryStatus> accepted = h.Client.WaitAsync(result.Token, CompletionStage.RemoteAccepted);
        h.Network.Advance(1_000);
        Assert.False(released.IsCompleted);
        Assert.False(accepted.IsCompleted);
        h.Client.Poll();
        Assert.True(released.IsCompleted);
        Assert.True(accepted.IsCompleted);
        Assert.Equal(DeliveryStatus.Pending, await released);
        Assert.Equal(DeliveryStatus.Delivered, await accepted);
        Assert.Equal(1, client.SentNotices);
        Assert.Equal(1, client.Completed);
        Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(result.Token));
    }

    [Fact]
    public void Tracked_Send_Is_Signalled_From_The_Transport_Thread_In_ThreadPool_Mode()
    {
        using SessionHarness h = TestEngines.Create(out TestEngine client, out _, both: o => o.CompletionMode = CompletionMode.ThreadPool);
        SendResult result = h.Client.SendCopy(new SendHeader(2), [1], SendOptions.Tracked);
        ValueTask<DeliveryStatus> released = h.Client.WaitAsync(result.Token, CompletionStage.BufferReleased);
        ValueTask<DeliveryStatus> accepted = h.Client.WaitAsync(result.Token, CompletionStage.RemoteAccepted);
        h.Network.Advance(1_000);
        Assert.True(released.IsCompleted);
        Assert.True(accepted.IsCompleted);
        Assert.Equal(DeliveryStatus.Pending, released.Result);
        Assert.Equal(DeliveryStatus.Delivered, accepted.Result);
        Assert.Equal(0, client.Completed);
        h.Client.Poll();
        Assert.Equal(1, client.Completed);
        Assert.Equal(1, client.SentNotices);
    }

    [Fact]
    public void Owned_Buffers_Are_Sent_Without_A_Copy_And_Returned_After_Completion()
    {
        using SessionHarness h = TestEngines.Create(out TestEngine client, out _);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        BufferLease lease = h.Client.RentBuffer(10);
        Assert.False(lease.IsEmpty);
        Span<byte> span = h.Client.GetBufferSpan(in lease);
        span[0] = 42;
        span[1] = 43;
        Assert.Equal(SendStatus.Admitted, h.Client.SendOwned(new SendHeader(2), lease, 2).Status);
        Assert.True(h.RunUntil(() => got.Count == 1 && client.Completed == 1));
        Assert.Equal(new byte[] { 42, 43 }, got[0].Payload);
        h.Client.GetStatistics(out PeerStatistics sent);
        Assert.Equal(0, sent.SendBytesOutstanding);
        BufferLease unused = h.Client.RentBuffer(100);
        h.Client.GetStatistics(out PeerStatistics rented);
        Assert.Equal(unused.Length, rented.SendBytesOutstanding);
        h.Client.ReturnBuffer(in unused);
        h.Client.GetStatistics(out PeerStatistics returned);
        Assert.Equal(0, returned.SendBytesOutstanding);
        Assert.True(h.Client.GetBufferSpan(BufferLease.Empty).IsEmpty);
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.RentBuffer(-1));
    }

    [Fact]
    public void Send_Budget_And_Table_Exhaustion_Are_Reported()
    {
        using SessionHarness h = TestEngines.Create(out _, out _, both: o =>
        {
            o.SendBudgetBytes = 256;
            o.SendTableCapacity = 16;
        });
        Assert.True(h.Client.RentBuffer(1000).IsEmpty);
        Assert.Equal(SendStatus.OutOfBuffers, h.Client.SendCopy(new SendHeader(2), new byte[500]).Status);
        SendStatus last = SendStatus.Admitted;
        for (int i = 0; i < 40 && last == SendStatus.Admitted; i++)
        {
            last = h.Client.SendCopy(new SendHeader(2), [1]).Status;
        }

        Assert.True(last is SendStatus.QueueFull or SendStatus.OutOfBuffers, last.ToString());
    }

    [Fact]
    public void The_Send_Table_Refuses_When_Full()
    {
        using SessionHarness h = TestEngines.Create(out _, out _, both: o => o.SendTableCapacity = 16);
        SendStatus last = SendStatus.Admitted;
        int admitted = 0;
        for (int i = 0; i < 40 && last == SendStatus.Admitted; i++)
        {
            last = h.Client.SendCopy(new SendHeader(2), [1]).Status;
            admitted += last == SendStatus.Admitted ? 1 : 0;
        }

        Assert.Equal(SendStatus.QueueFull, last);
        Assert.InRange(admitted, 14, 16);
        h.Run(5_000);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), [1]).Status);
    }

    [Fact]
    public async Task Modes_Without_An_Engine_Answer_NotSupported()
    {
        using SessionHarness h = new(table: TestTables.AllModes);
        byte[] data = [1];

        // Channels 2 … 6 (the unreliable modes, ReliableOrdered, ReliableUnordered and ReliableLatest) have their engines;
        // 7 (Bulk) is a placeholder until the rest of wave C2 lands.
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2, 1), data).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(3, 1), data).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(4, 1), data).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(5, 1), data).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(6, 1), data).Status);
        Assert.Equal(SendStatus.Admitted, (await h.Client.SendAsync(new SendHeader(4, 1), data)).Status);
        Assert.Equal(SendStatus.Admitted, (await h.Client.SendAsync(new SendHeader(5, 1), data)).Status);
        foreach (ushort channel in (ushort[])[7])
        {
            Assert.Equal(SendStatus.NotSupported, h.Client.SendCopy(new SendHeader(channel, 1), data).Status);
            Assert.Equal(SendStatus.NotSupported, h.Client.SendBorrowed(new SendHeader(channel, 1), data).Status);
            Assert.Equal(SendStatus.NotSupported, h.Client.SendGather(new SendHeader(channel, 1), []).Status);
            Assert.Equal(SendStatus.NotSupported, (await h.Client.SendAsync(new SendHeader(channel, 1), data)).Status);
        }

        BufferLease lease = h.Client.RentBuffer(8);
        Assert.Equal(SendStatus.NotSupported, h.Client.SendOwned(new SendHeader(7), lease, 1).Status);
        h.Client.ReturnBuffer(in lease);
        Assert.Equal(SendStatus.NotSupported, SendPinned(h.Client));
        Assert.Equal(SendStatus.Admitted, h.Client.RetireKey(6, 1));
        Assert.Equal(SendStatus.NotSupported, h.Client.Respond(new ReceiveHeader { Channel = 4, RequestId = 1 }, data).Status);
        await Assert.ThrowsAsync<NotSupportedException>(async () => await h.Client.SendRequestAsync(new SendHeader(4), data, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<NotSupportedException>(async () => await h.Client.BeginBulkSendAsync(new BulkDescriptor(7, 1, 1, 10), new EmptySource()));
        Assert.Throws<NotSupportedException>(() => h.Client.RequestBulk(new BulkRangeRequest(7, 1, 1, 0, 10)));
        Assert.Throws<ArgumentNullException>(() => h.Client.BeginBulkSendAsync(new BulkDescriptor(7, 1, 1, 10), null!));
        Assert.False(h.Client.TryCancel(default));
    }

    private static unsafe SendStatus SendPinned(QuiclyPeer peer)
    {
        byte value = 1;
        return peer.SendPinned(new SendHeader(7), &value, 1).Status;
    }

    [Fact]
    public async Task Sends_Before_Admission_Or_On_Unknown_Channels_Are_Refused()
    {
        using SessionHarness h = new(connect: false);
        Assert.Equal(SendStatus.NotConnected, h.Client.SendCopy(new SendHeader(2), [1]).Status);
        Assert.Equal(SendStatus.InvalidChannel, h.Client.SendCopy(new SendHeader(99), [1]).Status);
        Assert.Equal(SendStatus.NotConnected, h.Client.RetireKey(3, 1));
        Assert.Equal(SendStatus.InvalidChannel, h.Client.RetireKey(99, 1));
        Assert.Equal(SendStatus.NotConnected, h.Client.Respond(new ReceiveHeader { Channel = 4 }, [1]).Status);
        Assert.Equal(SendStatus.InvalidChannel, h.Client.Respond(new ReceiveHeader { Channel = 99 }, [1]).Status);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await h.Client.SendRequestAsync(new SendHeader(4), new byte[1], TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentException>(() => h.Client.SendRequestAsync(new SendHeader(99), new byte[1], TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.SendOwned(new SendHeader(2), BufferLease.Empty, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.SendOwned(new SendHeader(2), BufferLease.Empty, -1));
        Assert.Throws<ArgumentException>(() => h.Client.SendGather(new SendHeader(2), new BufferLease[9]));
        AssertPinnedValidation(h.Client);
        Assert.Throws<ArgumentException>(() => h.Client.RegisterHandler(99, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { }));
        Assert.Throws<ArgumentNullException>(() => h.Client.RegisterHandler(2, null!));
        Assert.Throws<ArgumentException>(() => h.Client.UnregisterHandler(99));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.Poll(-1));
        Assert.Throws<ArgumentException>(() => h.Client.BeginBulkSendAsync(new BulkDescriptor(99, 1, 1, 1), new EmptySource()));
        Assert.Throws<ArgumentException>(() => h.Client.RequestBulk(new BulkRangeRequest(99, 1, 1, 0, 1)));
        using CancellationTokenSource canceled = new();
        canceled.Cancel();
        Assert.True(h.Client.SendAsync(new SendHeader(2), new byte[1], default, canceled.Token).IsCanceled);
        Assert.True(h.Client.FlushAsync(canceled.Token).IsCanceled);
        Assert.True(h.Client.FlushAsync().IsCompletedSuccessfully);
        Assert.Equal(0, h.Client.Drain(2, new ReceivedMessage[1]));
    }

    private static unsafe void AssertPinnedValidation(QuiclyPeer peer)
    {
        Assert.Throws<ArgumentNullException>(() => peer.SendPinned(new SendHeader(2), null, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => peer.SendPinned(new SendHeader(2), null, -1));
        Assert.Equal(SendStatus.NotConnected, peer.SendPinned(new SendHeader(2), null, 0).Status);
    }

    [Fact]
    public void Token_Queries_Cancel_And_Wait()
    {
        using SessionHarness h = TestEngines.Create(out _, out _);
        SendResult result = h.Client.SendCopy(new SendHeader(2), [1], new SendOptions { Track = true, Context = 5 });
        Assert.False(h.Client.TryCancel(result.Token));
        Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(result.Token));
        Assert.Equal(DeliveryStatus.Pending, h.Client.Wait(result.Token, CompletionStage.RemoteAccepted, TimeSpan.Zero));
        int slot = h.Client.Core.EntryOfToken(result.Token);
        Assert.True(slot >= 0);
        Assert.Equal(5ul, h.Client.Core.GetUserContext(slot));
        Assert.Equal(result.Token, h.Client.Core.GetToken(slot));
        h.Run(5_000);
        Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(result.Token));
        Assert.Equal(DeliveryStatus.Delivered, h.Client.Wait(result.Token, CompletionStage.RemoteAccepted, TimeSpan.Zero));
        Assert.Equal(-1, h.Client.Core.EntryOfToken(result.Token));
        Assert.Equal(-1, h.Client.Core.EntryOfToken(new SendToken(int.MaxValue, 1)));
        Assert.False(h.Client.TryCancel(result.Token));
    }

    [Fact]
    public void Engine_Hooks_Run_On_The_Game_Thread_Only_While_Connected()
    {
        using SessionHarness h = TestEngines.Create(out TestEngine client, out TestEngine server);
        Assert.Equal(1, client.EpochResets);
        Assert.Equal(1, server.EpochResets);
        int ticks = client.Ticks;
        int flushes = client.Flushes;
        h.Client.Flush();
        Assert.Equal(ticks + 1, client.Ticks);
        Assert.Equal(flushes + 1, client.Flushes);
        h.Client.Poll();
        Assert.Equal(ticks + 1, client.Ticks);
        h.Client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.Equal(1, client.PeerClosedCalls);
        Assert.Equal(1, server.PeerClosedCalls);
        ticks = client.Ticks;
        h.Run(10_000);
        Assert.Equal(ticks, client.Ticks);
    }

    [Fact]
    public void Next_Deadline_Follows_The_Timers()
    {
        using SessionHarness h = new(connect: false, client: o => o.AdmissionTimeout = TimeSpan.FromSeconds(2));
        Assert.Equal(2_000_000, h.Client.NextDeadlineMicros);
        Assert.InRange(h.Client.NextDeadline, TimeSpan.FromSeconds(1.9), TimeSpan.FromSeconds(2));
        Assert.True(h.RunUntilConnected());
        Assert.True(h.Client.NextDeadline <= TimeSpan.FromMilliseconds(100));
        Assert.True(h.Client.NextDeadlineMicros >= h.Network.NowMicros);
        h.Client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.Equal(Timeout.InfiniteTimeSpan, h.Client.NextDeadline);
    }

    [Fact]
    public void Statistics_Snapshot_Reports_Transport_And_Peer_Counters()
    {
        using SessionHarness h = TestEngines.Create(out _, out _);
        h.Client.SendCopy(new SendHeader(2), [1, 2]);
        h.Run(500_000);
        h.Server!.GetStatistics(out PeerStatistics stats);
        Assert.True(stats.Transport.RecvTotalBytes > 0);
        Assert.True(stats.DatagramsReceived > 0);
        Assert.True(stats.DatagramBytesReceived > 0);
        Assert.True(stats.StreamBytesReceived > 0);
        Assert.True(stats.ControlMessagesReceived > 0);
        Assert.True(stats.PingsSent > 0);
        Assert.True(stats.PongsSent > 0);
        Assert.Equal(1200, stats.MaxDatagramPayload);
        Assert.Equal(4096, stats.ReceiveRingCapacity);
        Assert.Equal(1024, stats.SendTableCapacity);
        Assert.InRange(stats.SendEntriesInUse, 0, 4);
        Assert.Equal(0, stats.CallbackFaults);
        Assert.False(h.Server.GetChannelStatistics(99, out _));
    }
}

/// <summary>Peer-opened streams: parser driving, back-pressure, preamble rules and stream errors (PROTOCOL.md §3, §6).</summary>
public class StreamPlumbingTests
{
    private static bool Aborted(RawClient raw, TransportStreamId id, out ulong code)
    {
        foreach (RecordedEvent e in raw.Sink.OfKind(RecordedEventKind.StreamAborted))
        {
            if (e.StreamId == id)
            {
                code = e.ErrorCode;
                return true;
            }
        }

        code = 0;
        return false;
    }

    [Fact]
    public void Stream_Messages_Reach_The_Engine_And_The_Handler()
    {
        using SessionHarness h = TestEngines.Create(out TestEngine client, out TestEngine server, ChannelMode.ReliableOrdered);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(10, Handlers.Collect(got));
        byte[] big = new byte[3000];
        for (int i = 0; i < big.Length; i++)
        {
            big[i] = (byte)i;
        }

        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(10), [1, 2, 3]).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(10), big).Status);
        Assert.True(h.RunUntil(() => got.Count == 2));
        Assert.Equal(new byte[] { 1, 2, 3 }, got[0].Payload);
        Assert.Equal(big, got[1].Payload);
        Assert.Equal(1, server.Opened);
        Assert.Equal(2, server.Ends);
        Assert.True(server.Chunks >= 4);
        Assert.True(h.RunUntil(() => client.Completed == 2));
        Assert.Equal(DeliveryStatus.Delivered, client.LastStatus);
        h.Server.GetStatistics(out PeerStatistics stats);
        Assert.Equal(0, stats.ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Pended_Stream_Is_Resumed_From_Poll()
    {
        using SessionHarness h = TestEngines.Create(out _, out TestEngine server, ChannelMode.ReliableOrdered);
        server.PendStarts = 1;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(10, Handlers.Collect(got));
        h.Client.SendCopy(new SendHeader(10), [4, 5, 6]);
        Assert.True(h.RunUntil(() => got.Count == 1));
        Assert.Equal(new byte[] { 4, 5, 6 }, got[0].Payload);
        Assert.Equal(1, server.Pended);
        Assert.Equal(1, server.Starts);
    }

    [Fact]
    public void A_Full_Ring_Pends_Streams_Until_Poll_Makes_Room()
    {
        using SessionHarness h = TestEngines.Create(out _, out TestEngine server, ChannelMode.ReliableOrdered, both: o => o.ReceiveRingCapacity = 2);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(10, Handlers.Collect(got));
        for (int i = 0; i < 8; i++)
        {
            h.Client.SendCopy(new SendHeader(10), [(byte)i]);
        }

        Assert.True(h.RunUntil(() => got.Count == 8));
        for (int i = 0; i < 8; i++)
        {
            Assert.Equal((byte)i, got[i].Payload[0]);
        }

        Assert.True(server.Pended >= 1);
        h.Server.GetStatistics(out PeerStatistics stats);
        Assert.Equal(0, stats.ReceiveRingDrops);
    }

    [Fact]
    public void A_Malformed_Ordered_Stream_Closes_The_Connection()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.ReliableOrdered);
        Assert.True(h.Admit());
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni([0x0A, 0x40, 0x01, 0xAA], out _));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
        Assert.Equal(1, engine().Opened);
        Assert.Equal(1, engine().StreamsClosed);
        Assert.True(engine().LastStreamAborted);
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, engine().LastStreamCode);
    }

    [Fact]
    public void A_Malformed_Group_Stream_Is_Reset_And_The_Connection_Survives()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.ReliableUnordered);
        Assert.True(h.Admit());
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni([0x0B, 0x00, 0x40, 0x01, 0xAA], out TransportStreamId id));
        ulong code = 0;
        Assert.True(h.RunUntil(() => Aborted(h.Raw, id, out code)));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, code);
        h.Run(10_000);
        Assert.Equal(PeerState.Connected, h.Server!.State);
        Assert.Equal(1, engine().StreamsClosed);
        Assert.Equal(1, h.Statistics().StreamsReset);
    }

    [Theory]
    [InlineData(new byte[] { 0x40, 0x63, 0x01 }, QuiclyErrorCode.UnsupportedChannel)]
    [InlineData(new byte[] { 0x02, 0x01 }, QuiclyErrorCode.UnsupportedChannel)]
    [InlineData(new byte[] { 0x01, 0x01 }, QuiclyErrorCode.UnsupportedChannel)]
    [InlineData(new byte[] { 0x00, 0x01 }, QuiclyErrorCode.UnsupportedChannel)]
    [InlineData(new byte[] { 0x40, 0x0A, 0x01 }, QuiclyErrorCode.ProtocolViolation)]
    public void Bad_Preambles_Are_Reset(byte[] data, QuiclyErrorCode expected)
    {
        using ServerHarness h = TestEngines.CreateServer(out _, ChannelMode.ReliableOrdered);
        Assert.True(h.Admit());
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(data, out TransportStreamId id));
        ulong code = 0;
        Assert.True(h.RunUntil(() => Aborted(h.Raw, id, out code)));
        Assert.Equal((ulong)expected, code);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Theory]
    [InlineData((byte)5)]
    public void Streams_Of_Modes_Without_An_Engine_Are_Reset(byte channel)
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni([channel, 0x01, 0x07], out TransportStreamId id));
        ulong code = 0;
        Assert.True(h.RunUntil(() => Aborted(h.Raw, id, out code)));
        Assert.Equal((ulong)QuiclyErrorCode.UnsupportedChannel, code);
    }

    [Fact]
    public void An_Engine_Can_Close_The_Connection_When_A_Stream_Opens()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.ReliableOrdered);
        Assert.True(h.Admit());
        engine().OpenResult = StreamAccept.CloseConnection(QuiclyErrorCode.ProtocolViolation);
        h.Raw.OpenUni([0x0A, 0x01, 0x07], out _);
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }

    [Fact]
    public void An_Engine_Can_Reset_A_Stream()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.ReliableUnordered);
        Assert.True(h.Admit());
        engine().StartResult = StreamConsume.ResetStream(QuiclyErrorCode.LimitExceeded);
        h.Raw.OpenUni([0x0B, 0x00, 0x01, 0x07], out TransportStreamId id);
        ulong code = 0;
        Assert.True(h.RunUntil(() => Aborted(h.Raw, id, out code)));
        Assert.Equal((ulong)QuiclyErrorCode.LimitExceeded, code);
        Assert.Equal(1, engine().StreamsClosed);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void An_Engine_Can_Close_The_Connection_On_A_Message()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.ReliableUnordered);
        Assert.True(h.Admit());
        engine().StartResult = StreamConsume.CloseConnection(QuiclyErrorCode.LimitExceeded);
        h.Raw.OpenUni([0x0B, 0x00, 0x01, 0x07], out _);
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.LimitExceeded, h.Raw.CloseCode);
        Assert.Equal(1, engine().StreamsClosed);
    }

    [Fact]
    public void A_Truncated_Group_Stream_Is_Reset()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.ReliableUnordered);
        Assert.True(h.Admit());
        h.Raw.OpenUni([0x0B, 0x00, 0x05, 1, 2], out _, fin: true);
        Assert.True(h.RunUntil(() => engine().StreamsClosed == 1));
        Assert.True(engine().LastStreamAborted);
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, engine().LastStreamCode);
        Assert.Equal(1, h.Statistics().StreamsReset);
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Finished_Group_Stream_Ends_Cleanly()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.ReliableUnordered);
        Assert.True(h.Admit());
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(11, Handlers.Collect(got));
        h.Raw.OpenUni([0x0B, 0x00, 0x02, 1, 2, 0x01, 3], out _, fin: true);
        Assert.True(h.RunUntil(() => got.Count == 2 && engine().StreamsClosed == 1));
        Assert.False(engine().LastStreamAborted);
        Assert.Equal(new byte[] { 1, 2 }, got[0].Payload);
        Assert.Equal(new byte[] { 3 }, got[1].Payload);
    }

    [Fact]
    public void A_Peer_Reset_Mid_Message_Reaches_The_Engine()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.ReliableUnordered);
        Assert.True(h.Admit());
        h.Raw.OpenUni([0x0B, 0x00, 0x05, 1, 2], out TransportStreamId id);
        Assert.True(h.RunUntil(() => engine().Starts == 1));
        h.Raw.Transport.AbortStream(id, 0x33, StreamAbortDirection.Send);
        Assert.True(h.RunUntil(() => engine().StreamsClosed == 1));
        Assert.True(engine().LastStreamAborted);
        Assert.Equal(0x33ul, engine().LastStreamCode);
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }

    [Fact]
    public void The_Client_Resets_A_Bidirectional_Stream_From_The_Server()
    {
        using ClientHarness h = new();
        Assert.Equal(TransportStatus.Success, h.ServerTransport!.OpenStream(StreamKind.Bidirectional, 5, 1, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.ServerTransport, id, [0x00], TransportSendFlags.Start));
        Assert.True(h.RunUntil(() => h.Sink.OfKind(RecordedEventKind.StreamAborted).Any(e => e.StreamId == id)));
        Assert.Equal((ulong)QuiclyErrorCode.UnsupportedChannel, h.Sink.OfKind(RecordedEventKind.StreamAborted).First(e => e.StreamId == id).ErrorCode);
        Assert.Equal(PeerState.Handshaking, h.Client.State);
    }

    [Fact]
    public void Local_Stream_Events_Reach_Every_Engine()
    {
        using SessionHarness h = TestEngines.Create(out TestEngine client, out _, ChannelMode.ReliableOrdered);
        h.Client.SendCopy(new SendHeader(10), [1]);
        h.Run(5_000);
        // The server stops our ordered stream: the engine that opened it hears about it.
        TransportStreamId peerStream = default;
        Assert.True(h.RunUntil(() => h.Server!.Core.Streams.Count > 0));
        QuiclyPeer server = h.Server!;
        for (int slot = 0; slot < 64 && !peerStream.IsValid; slot++)
        {
            for (uint generation = 1; generation < 4 && !peerStream.IsValid; generation++)
            {
                TransportStreamId candidate = new(slot, generation);
                if (!System.Runtime.CompilerServices.Unsafe.IsNullRef(ref server.Core.Streams.Find(candidate))
                    && server.Core.Streams.Find(candidate).Tag == StreamTag.Engine)
                {
                    peerStream = candidate;
                }
            }
        }

        Assert.True(peerStream.IsValid);
        server.Core.Transport!.AbortStream(peerStream, 0x44, StreamAbortDirection.Receive);
        Assert.True(h.RunUntil(() => client.LocalStreamEvents > 0));
    }
}

/// <summary>Datagram receive: containers, malformed input, control routing and callback faults.</summary>
public class DatagramPlumbingTests
{
    private static byte[] LatestAck(ushort channel, ulong key, uint version, ControlCarrier carrier)
    {
        byte[] buffer = new byte[64];
        LatestAckBatchWriter writer = new(buffer, carrier);
        Assert.True(writer.TryAdd(new LatestAckEntry(channel, key, version)));
        int written = writer.Finish();
        return buffer.AsSpan(0, written).ToArray();
    }

    private static byte[] BulkCancelFrame()
    {
        byte[] buffer = new byte[32];
        Assert.True(ControlCodec.TryWrite(buffer, new BulkCancel(1, QuiclyErrorCode.BulkCanceled), out int written));
        return buffer.AsSpan(0, written).ToArray();
    }

    [Fact]
    public void Packed_Container_Members_Are_Dispatched_With_The_Tick()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine);
        Assert.True(h.Admit());
        byte[] ping = Frames.PingFrame(99, ControlCarrier.Datagram);
        byte[] container = [0x01, 0x01, 0x07, 0x02, 0x02, 0xAA, (byte)ping.Length, .. ping, 0x02, 0x03, 0xBB];
        h.Raw.SendDatagram(container);
        Assert.True(h.RunUntil(() => engine().Received == 2));
        Assert.Equal(7u, engine().LastSenderTick);
        Assert.True(h.RunUntil(() => h.Raw.DatagramsOfType(ControlType.Pong) == 1));
        Assert.Equal(0, h.Statistics().MalformedDatagrams);
    }

    [Fact]
    public void Malformed_Datagrams_Are_Dropped_And_Counted()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine);
        Assert.True(h.Admit());
        h.Raw.SendDatagram([0x01, 0x00, 0x02, 0x01, 0x00]);
        h.Raw.SendDatagram([0x40, 0x63, 0x01]);
        h.Raw.SendDatagram([0x0A, 0x01]);
        h.Raw.SendDatagram([0x00, 0x10]);
        h.Raw.SendDatagram([0x01, 0x00, 0x03, 0x40, 0x63, 0x01]);
        h.Run(5_000);
        Assert.Equal(5, h.Statistics().MalformedDatagrams);
        Assert.Equal(0, engine().Received);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void Control_Messages_Are_Routed_To_The_Engine_Of_Their_Mode()
    {
        using ServerHarness h = new(table: TestTables.AllModes);
        Assert.True(h.Admit());
        h.Raw.SendDatagram(LatestAck(6, 1, 5, ControlCarrier.Datagram));
        h.Raw.SendControl(LatestAck(6, 1, 5, ControlCarrier.Stream));
        h.Raw.SendControl(Frames.KeyRetiredFrame(3, 1));
        h.Raw.SendControl(BulkCancelFrame());
        h.Run(10_000);
        Assert.Equal(0, h.Statistics().MalformedDatagrams);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void Control_Messages_For_A_Mode_The_Table_Lacks_Are_Rejected()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        h.Raw.SendDatagram(LatestAck(3, 1, 5, ControlCarrier.Datagram));
        h.Run(5_000);
        Assert.Equal(1, h.Statistics().MalformedDatagrams);
        Assert.Equal(PeerState.Connected, h.Server!.State);
        h.Raw.SendControl(LatestAck(3, 1, 5, ControlCarrier.Stream));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }

    [Fact]
    public void An_Engine_Can_Reject_A_Control_Message()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.ReliableLatest, table: TestTables.AllModes);
        Assert.True(h.Admit());
        engine().RejectControl = true;
        h.Raw.SendDatagram(LatestAck(6, 1, 5, ControlCarrier.Datagram));
        h.Run(5_000);
        Assert.Equal(1, engine().ControlMessages);
        Assert.Equal(1, h.Statistics().MalformedDatagrams);
        h.Raw.SendControl(LatestAck(6, 1, 5, ControlCarrier.Stream));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }

    [Fact]
    public void KeyRetired_For_An_Unknown_Channel_Is_A_Protocol_Violation()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        h.Raw.SendControl(Frames.KeyRetiredFrame(99, 1));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }

    [Fact]
    public void The_Channel_Table_Request_Is_Answered_After_Admission()
    {
        using ServerHarness h = new();
        h.Admission.Handler = (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Pending;
        h.Raw.SendControl(Frames.HelloFrame(h.Table.Hash));
        Assert.True(h.RunUntil(() => h.Admission.Calls == 1));
        h.Raw.SendControl(Frames.TableRequestFrame());
        h.Run(5_000);
        Assert.Equal(1, h.Statistics().DroppedBeforeAdmission);
        h.Server!.CompleteAdmission(AdmissionResult.Accept());
        Assert.True(h.RunUntil(() => h.Raw.ServerFrames().Count == 1));
        h.Raw.SendControl(Frames.TableRequestFrame());
        Assert.True(h.RunUntil(() => h.Raw.ServerFrames().Count == 2));
        byte[] answer = h.Raw.ServerFrames()[1].Body;
        Assert.Equal(HelloStatus.Informational, Frames.AckStatus(answer));
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(answer, out HelloAck ack));
        Assert.True(ack.HasTable);
        Assert.Equal(ChannelTableParseStatus.Ok, ChannelTableCodec.TryParseWithNames(ack.Table, out ChannelTableDescription? table, out _));
        Assert.Equal(h.Table.Hash, table!.Hash);
    }

    [Fact]
    public void A_Throwing_Callback_Closes_With_InternalError()
    {
        using SessionHarness h = TestEngines.Create(out _, out TestEngine server);
        server.ThrowOnDatagram = true;
        h.Client.SendCopy(new SendHeader(2), [1]);
        Assert.True(h.RunUntilClosed());
        Assert.Equal(QuiclyErrorCode.InternalError, h.Server!.CloseReason.Code);
        Assert.Equal(CloseSource.Local, h.Server.CloseReason.Source);
        Assert.IsType<InvalidOperationException>(h.Server.LastCallbackFault);
        h.Server.GetStatistics(out PeerStatistics stats);
        Assert.Equal(1, stats.CallbackFaults);
        Assert.Equal(QuiclyErrorCode.InternalError, h.Client.CloseReason.Code);
        Assert.Equal(CloseSource.Peer, h.Client.CloseReason.Source);
    }
}
