using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Edge cases of the datagram engines, the packer and the scheduler.</summary>
public class DatagramEdgeTests
{
    private static readonly ChannelTable Table = DatagramTables.Main;

    [Fact]
    public void A_Full_Completion_Table_Refuses_Tracked_Sends()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.SendTableCapacity = 16;
        }, server: DatagramKit.Quiet);

        // The handshake's first Ping may still be in flight: start with every entry free.
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(h.Client).SendEntriesInUse == 0));

        // A wait that is never consumed keeps its completion slot after the send finished.
        List<ValueTask<DeliveryStatus>> waits = [];
        for (int i = 0; i < 16; i++)
        {
            SendResult result = h.Client.SendCopy(new SendHeader(2), [1], SendOptions.Tracked);
            Assert.True(result.IsAdmitted);
            waits.Add(h.Client.WaitAsync(result.Token, CompletionStage.RemoteAccepted));
            h.Client.Flush();
        }

        Assert.True(h.RunUntil(() => waits.All(w => w.IsCompleted) && DatagramKit.Statistics(h.Client).SendEntriesInUse == 0));
        Assert.Equal(SendStatus.QueueFull, h.Client.SendCopy(new SendHeader(2), [1], SendOptions.Tracked).Status);
        Assert.Equal(1, DatagramKit.ChannelStats(h.Client, 2).QueueFull);
        Assert.Equal(0, DatagramKit.Statistics(h.Client).SendEntriesInUse);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), [1]).Status);
        foreach (ValueTask<DeliveryStatus> wait in waits)
        {
            Assert.Equal(DeliveryStatus.Delivered, wait.Result);
        }

        Assert.True(h.Client.SendCopy(new SendHeader(2), [1], SendOptions.Tracked).IsAdmitted);
    }

    [Fact]
    public void Cancelling_Queued_Messages_Anywhere_In_The_Queue_Keeps_The_Rest_In_Order()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        SendResult[] sends = [.. Enumerable.Range(1, 4).Select(i => h.Client.SendCopy(new SendHeader(2), [(byte)i], SendOptions.Tracked))];
        Assert.True(h.Client.TryCancel(sends[1].Token));
        Assert.True(h.Client.TryCancel(sends[3].Token));
        SendResult last = h.Client.SendCopy(new SendHeader(2), [5], SendOptions.Tracked);
        Assert.True(h.RunUntil(() => got.Count == 3));
        Assert.Equal(new byte[] { 1, 3, 5 }, got.Select(g => g.Payload[0]).ToArray());
        Assert.Equal(DeliveryStatus.Canceled, h.Client.GetDeliveryStatus(sends[1].Token));
        Assert.Equal(DeliveryStatus.Canceled, h.Client.GetDeliveryStatus(sends[3].Token));
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(last.Token) == DeliveryStatus.Delivered));
    }

    [Fact]
    public void A_Lone_Fragment_Waits_For_Its_Message_Instead_Of_Being_Delivered()
    {
        using ServerHarness h = new(table: Table, server: DatagramKit.Quiet);
        Assert.True(h.Admit());
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(13, Handlers.Collect(got));
        MessageHeader fragment = default;
        fragment.Sequence = 1;
        fragment.FragCount = 2;
        fragment.FragIndex = 0;
        byte[] frame = new byte[64];
        int written = DatagramFraming.WriteHeader(frame, Table[13]!, in fragment);
        h.Raw.SendDatagram(frame.AsSpan(0, written + 10));
        h.Raw.SendDatagram(DatagramKit.Frame(Table, 13, 2, 0, [9]));
        h.Run(10_000);

        // Since wave C2d the fragment starts a partial message instead of being dropped (docs/design/session-layer.md §7.8);
        // only the whole message that follows it is delivered.
        Assert.Single(got);
        Assert.Equal(9, got[0].Payload[0]);
        Assert.Equal(0, DatagramKit.ChannelStats(h.Server, 13).Dropped);
        Assert.Equal(1, FragmentKit.Reassemblies(h.Server, 13));
        Assert.Equal(1, DatagramKit.Statistics(h.Server).FragmentsReceived);
    }

    [Fact]
    public unsafe void Payloads_That_Compression_Would_Not_Shrink_Travel_As_They_Are()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        QuiclyPeer client = h.Client;
        byte[] random = new byte[600];
        new Random(9).NextBytes(random);
        long baseline = DatagramKit.Statistics(client).SendBytesOutstanding;

        BufferLease owned = client.RentBuffer(600);
        random.CopyTo(client.GetBufferSpan(in owned));
        Assert.True(client.SendOwned(new SendHeader(5), owned, 600).IsAdmitted);
        client.Flush();

        // Sent alone without a copy: the only send lease held is the caller's own.
        Assert.Equal(baseline + owned.Length, DatagramKit.Statistics(client).SendBytesOutstanding);
        Assert.True(client.SendBorrowed(new SendHeader(5), random).IsAdmitted);
        client.Flush();
        fixed (byte* pointer = random)
        {
            Assert.True(client.SendPinned(new SendHeader(5), pointer, 600).IsAdmitted);
            client.Flush();
        }

        BufferLease pageA = client.RentBuffer(256);
        BufferLease pageB = client.RentBuffer(256);
        random.AsSpan(0, 256).CopyTo(client.GetBufferSpan(in pageA));
        random.AsSpan(256, 256).CopyTo(client.GetBufferSpan(in pageB));
        Assert.True(client.SendGather(new SendHeader(5), [pageA, pageB]).IsAdmitted);
        Assert.True(h.RunUntil(() => got.Count == 4));
        Assert.All(got, g => Assert.Equal(0, g.Header.RawLength));
        Assert.Equal(random, got[0].Payload);
        Assert.Equal(random, got[1].Payload);
        Assert.Equal(random, got[2].Payload);
        Assert.Equal(random.AsSpan(0, 512).ToArray(), got[3].Payload);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).SendBytesOutstanding == baseline));
    }

    [Fact]
    public void Refused_Sends_Leave_The_Caller_Its_Pages_And_Release_What_The_Peer_Took()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.SendBudgetBytes = 256;
        }, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).SendEntriesInUse == 0));
        BufferLease a = client.RentBuffer(64);
        BufferLease b = client.RentBuffer(64);

        // Gathering needs a 128-byte lease (a 256-byte block): over the budget, so the caller keeps both pages.
        Assert.Equal(SendStatus.OutOfBuffers, client.SendGather(new SendHeader(2), [a, b]).Status);
        client.ReturnBuffer(in a);
        client.ReturnBuffer(in b);
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);

        // A borrowed array is pinned while the send is prepared; a refusal after that releases the pin and the entry.
        client.TransportSink.OnDatagramCapabilityChanged(true, 500);
        Assert.Equal(SendStatus.TooLarge, client.SendBorrowed(new SendHeader(2), new byte[600]).Status);
        Assert.Equal(0, DatagramKit.Statistics(client).SendEntriesInUse);
        client.TransportSink.OnDatagramCapabilityChanged(true, 1200);
        Assert.Equal(SendStatus.Admitted, client.SendBorrowed(new SendHeader(2), new byte[600]).Status);
    }

    [Fact]
    public void Unordered_Coalescing_With_Dense_Keys_Refuses_Keys_Beyond_The_Space()
    {
        using ServerHarness h = new(table: Table, server: DatagramKit.Quiet);
        Assert.True(h.Admit());
        h.Raw.SendDatagram(DatagramKit.Frame(Table, 15, 0, 3, [1]));
        h.Raw.SendDatagram(DatagramKit.Frame(Table, 15, 0, 4, [2]));
        h.Raw.SendDatagram(DatagramKit.Frame(Table, 15, 0, 3, [3]));
        h.Run(10_000);
        ReceivedMessage[] buffer = new ReceivedMessage[4];
        int count = h.Server!.Drain(15, buffer);
        Assert.Equal(1, count);
        Assert.Equal(3, buffer[0].Payload[0]);
        h.Server.Release(buffer.AsSpan(0, count));
        ChannelStatistics stats = DatagramKit.ChannelStats(h.Server, 15);
        Assert.Equal(1, stats.ReceiveKeyTableFull);
        Assert.Equal(1, stats.ReceiveSuperseded);
    }

    [Fact]
    public void A_Key_Table_Of_One_Key_Evicts_It_For_Every_New_Key()
    {
        ChannelTable table = ChannelTable.Create()
            .Add(2, "one", ChannelMode.UnreliableSequenced, o =>
            {
                o.Keyed = true;
                o.MaxKeys = 1;
            })
            .Build();
        using ReceiveKeyTracker keys = new(table[2]!);
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(1, 1, false, out _));
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(2, 1, false, out _));
        Assert.Equal(KeyAcceptance.Accepted, keys.TryAcceptSequence(1, 0, false, out _));
        Assert.Equal(KeyAcceptance.Stale, keys.TryAcceptSequence(1, 0, false, out _));
        Assert.Equal(2, keys.Evictions);
        Assert.Equal(1, keys.Count);
    }

    [Fact]
    public void When_A_Pass_Allows_It_Unreliable_Datagrams_Carry_CancelOnBlocked()
    {
        FlagRecordingConnector? recorder = null;
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet,
            connector: c => recorder = new FlagRecordingConnector(c));
        FlagRecordingTransport transport = recorder!.Transport!;
        transport.Datagrams.Clear();
        PeerCore core = h.Client.Core;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        h.Server.RegisterHandler(7, Handlers.Collect(got));

        // One pass by hand, as the scheduler runs it once TransportCapabilities.CancelOnBlocked is on main.
        void Pass()
        {
            FlushContext flush = new()
            {
                NowMicros = h.Network.NowMicros,
                MaxDatagramPayload = core.MaxDatagramPayload,
                DatagramsEnabled = true,
                NextDeadline = long.MaxValue,
                BudgetBytes = long.MaxValue,
                CancelBlockedDatagrams = true,
            };
            core.Packer.Begin(in flush);
            foreach (int channel in core.ScheduleOrder.ToArray())
            {
                core.GetEngine(channel).FlushChannel(channel, ref flush);
            }

            core.Packer.Finish(ref flush);
            Assert.True(flush.BytesSubmitted > 0);
        }

        h.Client.SendCopy(new SendHeader(2), [1]);
        Pass();
        h.Client.SendCopy(new SendHeader(2), [2]);
        h.Client.SendCopy(new SendHeader(7), [3]);
        Pass();
        Assert.Equal(TransportSendFlags.CancelOnBlocked, transport.Datagrams[0].Flags);
        Assert.Equal(TransportSendFlags.CancelOnBlocked | TransportSendFlags.Priority, transport.Datagrams[1].Flags);
        Assert.True(h.RunUntil(() => got.Count == 3));
    }

    [Fact]
    public void A_Send_Cap_Of_Two_Gigabytes_Per_Second_Or_More_Is_No_Cap()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.MaxSendBytesPerSecond = long.MaxValue;
        }, server: DatagramKit.Quiet);
        long before = DatagramKit.Statistics(h.Client).DatagramsSent;
        for (int i = 0; i < 100; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(2), new byte[1000]).IsAdmitted);
        }

        h.Client.Flush();
        Assert.Equal(100, DatagramKit.Statistics(h.Client).DatagramsSent - before);
    }

    [Fact]
    public void An_Immediate_Send_Held_Back_By_The_Cap_Lowers_The_Next_Deadline()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.MaxSendBytesPerSecond = 1_000;
        }, server: DatagramKit.Quiet);
        long now = h.Network.NowMicros;
        Assert.True(h.Client.NextDeadlineMicros - now > 1_000_000);
        long sent = DatagramKit.Statistics(h.Client).DatagramsSent;
        Assert.True(h.Client.SendCopy(new SendHeader(2), new byte[64], SendOptions.Immediate).IsAdmitted);
        Assert.Equal(sent + 1, DatagramKit.Statistics(h.Client).DatagramsSent);
        Assert.True(h.Client.SendCopy(new SendHeader(2), new byte[64], SendOptions.Immediate).IsAdmitted);
        Assert.Equal(sent + 1, DatagramKit.Statistics(h.Client).DatagramsSent);
        DatagramEngine engine = (DatagramEngine)h.Client.Core.GetEngine(ChannelMode.UnreliableUnordered)!;
        Assert.Equal(1, engine.QueuedMessages(h.Client.Core.ChannelIndexOf(2)));
        Assert.InRange(h.Client.NextDeadlineMicros - now, 1, 200_000);
    }
}
