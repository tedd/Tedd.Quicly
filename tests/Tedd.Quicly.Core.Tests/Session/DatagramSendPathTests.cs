using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Send paths, admission, tracked completions and compression of the datagram engines (ARCHITECTURE.md §4.1, PROTOCOL.md §2.1, §4.3).</summary>
public class DatagramSendPathTests
{
    private static readonly ChannelTable Table = DatagramTables.Main;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public unsafe void Every_Send_Path_Delivers_The_Same_Bytes(bool packed)
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        QuiclyPeer client = h.Client;
        List<byte[]> expected = [];
        void Flush()
        {
            if (!packed)
            {
                client.Flush();
            }
        }

        byte[] copy = DatagramKit.Payload(1, 40);
        Assert.True(client.SendCopy(new SendHeader(2), copy).IsAdmitted);
        expected.Add(copy);
        Flush();

        BufferLease lease = client.RentBuffer(40);
        byte[] owned = DatagramKit.Payload(2, 40);
        owned.CopyTo(client.GetBufferSpan(in lease));
        Assert.True(client.SendOwned(new SendHeader(2), lease, 40).IsAdmitted);
        expected.Add(owned);
        Flush();

        byte* native = (byte*)NativeMemory.Alloc(40);
        byte[] pinned = DatagramKit.Payload(3, 40);
        pinned.CopyTo(new Span<byte>(native, 40));
        Assert.True(client.SendPinned(new SendHeader(2), native, 40).IsAdmitted);
        expected.Add(pinned);
        Flush();

        byte[] array = new byte[60];
        byte[] borrowed = DatagramKit.Payload(4, 40);
        borrowed.CopyTo(array, 10);
        Assert.True(client.SendBorrowed(new SendHeader(2), array.AsMemory(10, 40)).IsAdmitted);
        expected.Add(borrowed);
        Flush();

        using NativeMemoryManager manager = new(40);
        byte[] managed = DatagramKit.Payload(5, 40);
        managed.CopyTo(manager.GetSpan());
        Assert.True(client.SendBorrowed(new SendHeader(2), manager.Memory).IsAdmitted);
        expected.Add(managed);
        Flush();

        BufferLease first = client.RentBuffer(64);
        BufferLease second = client.RentBuffer(64);
        byte[] gathered = DatagramKit.Payload(6, 128);
        gathered.AsSpan(0, 64).CopyTo(client.GetBufferSpan(in first));
        gathered.AsSpan(64).CopyTo(client.GetBufferSpan(in second));
        Assert.Equal(64, first.Length);
        Assert.True(client.SendGather(new SendHeader(2), [first, second]).IsAdmitted);
        expected.Add(gathered);
        Flush();
        Assert.True(client.SendGather(new SendHeader(2), []).IsAdmitted);
        expected.Add([]);
        Flush();

        client.Flush();
        Assert.True(h.RunUntil(() => got.Count == expected.Count));
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i], got[i].Payload);
        }

        PeerStatistics statistics = DatagramKit.Statistics(client);
        Assert.Equal(packed ? 1 : 0, statistics.ContainersSent);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).SendEntriesInUse == 0));
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
        NativeMemory.Free(native);
    }

    [Fact]
    public void Borrowed_Memory_Stays_Pinned_Until_The_Transport_Released_It()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 }, table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        PeerCore core = h.Client.Core;
        byte[] array = new byte[100];
        SendResult alone = h.Client.SendBorrowed(new SendHeader(2), array.AsMemory(10, 50), SendOptions.Tracked);
        int slot = core.EntryOfToken(alone.Token);
        nint pin = core.Entries.PinHandles[slot];
        Assert.NotEqual(0, pin);
        Assert.True(GCHandle.FromIntPtr(pin).IsAllocated);
        Assert.NotEqual(SendEntryFlags.None, core.Entries[slot].Flags & SendEntryFlags.Pinned);

        // Sent alone, without a copy: the transport references the array until it reports Sent.
        h.Client.Flush();
        Assert.Equal(pin, core.Entries.PinHandles[slot]);
        Assert.False(core.Completions.IsCompleted(alone.Token, CompletionStage.BufferReleased));
        h.Network.Advance(0);
        h.Client.Poll();
        Assert.Equal(0, core.Entries.PinHandles[slot]);
        Assert.True(core.Completions.IsCompleted(alone.Token, CompletionStage.BufferReleased));
        Assert.False(core.Completions.IsCompleted(alone.Token, CompletionStage.RemoteAccepted));

        // Packed: the bytes are copied into the container, so the pin goes at once; BufferReleased still waits for Sent.
        SendResult a = h.Client.SendBorrowed(new SendHeader(2), array.AsMemory(0, 10), SendOptions.Tracked);
        SendResult b = h.Client.SendBorrowed(new SendHeader(2), array.AsMemory(20, 10), SendOptions.Tracked);
        int slotA = core.EntryOfToken(a.Token);
        int slotB = core.EntryOfToken(b.Token);
        h.Client.Flush();
        Assert.Equal(0, core.Entries.PinHandles[slotA]);
        Assert.Equal(0, core.Entries.PinHandles[slotB]);
        Assert.False(core.Completions.IsCompleted(a.Token, CompletionStage.BufferReleased));
        h.Network.Advance(0);
        h.Client.Poll();
        Assert.True(core.Completions.IsCompleted(a.Token, CompletionStage.BufferReleased));
        Assert.True(core.Completions.IsCompleted(b.Token, CompletionStage.BufferReleased));
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(alone.Token) == DeliveryStatus.Delivered
            && h.Client.GetDeliveryStatus(a.Token) == DeliveryStatus.Delivered && h.Client.GetDeliveryStatus(b.Token) == DeliveryStatus.Delivered));
    }

    [Fact]
    public void Owned_Leases_Return_To_The_Budget_After_Completion_And_Stay_With_The_Caller_When_Refused()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 }, table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        long baseline = DatagramKit.Statistics(h.Client).SendBytesOutstanding;
        BufferLease lease = h.Client.RentBuffer(64);
        SendResult result = h.Client.SendOwned(new SendHeader(2), lease, 64, SendOptions.Tracked);
        h.Client.Flush();
        Assert.Equal(baseline + 64, DatagramKit.Statistics(h.Client).SendBytesOutstanding);
        h.Network.Advance(0);
        h.Client.Poll();
        Assert.Equal(baseline, DatagramKit.Statistics(h.Client).SendBytesOutstanding);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered));

        // Channel 12 queues at most 100 bytes: the second lease is refused and still belongs to the caller.
        BufferLease accepted = h.Client.RentBuffer(64);
        BufferLease refused = h.Client.RentBuffer(64);
        Assert.Equal(SendStatus.Admitted, h.Client.SendOwned(new SendHeader(12), accepted, 64).Status);
        Assert.Equal(SendStatus.QueueFull, h.Client.SendOwned(new SendHeader(12), refused, 64).Status);
        Assert.Equal(1, DatagramKit.ChannelStats(h.Client, 12).QueueFull);
        h.Client.ReturnBuffer(in refused);
        h.Client.Flush();
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(12), new byte[64]).Status);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(h.Client).SendEntriesInUse == 0));
        Assert.Equal(baseline, DatagramKit.Statistics(h.Client).SendBytesOutstanding);
    }

    [Fact]
    public void Tracked_Sends_End_Delivered_Lost_Expired_Canceled_Or_Disconnected()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000 }, table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;

        SendResult delivered = client.SendCopy(new SendHeader(2), [1], SendOptions.Tracked);
        ValueTask<DeliveryStatus> released = client.WaitAsync(delivered.Token, CompletionStage.BufferReleased);
        ValueTask<DeliveryStatus> accepted = client.WaitAsync(delivered.Token, CompletionStage.RemoteAccepted);
        Assert.True(h.RunUntil(() => accepted.IsCompleted));
        Assert.Equal(DeliveryStatus.Pending, released.Result);
        Assert.Equal(DeliveryStatus.Delivered, accepted.Result);

        DatagramKit.TransportOf(client).DropNextDatagrams(1);
        SendResult lost = client.SendCopy(new SendHeader(2), [2], SendOptions.Tracked);
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(lost.Token) != DeliveryStatus.Pending));
        Assert.Equal(DeliveryStatus.Lost, client.GetDeliveryStatus(lost.Token));

        SendResult expired = client.SendCopy(new SendHeader(2), [3], new SendOptions { Track = true, ExpiryMicros = 1_000 });
        h.Network.Advance(2_000);
        client.Flush();
        Assert.Equal(DeliveryStatus.Expired, client.GetDeliveryStatus(expired.Token));
        Assert.Equal(1, DatagramKit.ChannelStats(client, 2).Expired);

        SendResult canceled = client.SendCopy(new SendHeader(2), [4], SendOptions.Tracked);
        Assert.True(client.TryCancel(canceled.Token));
        Assert.False(client.TryCancel(canceled.Token));
        Assert.Equal(DeliveryStatus.Pending, client.GetDeliveryStatus(canceled.Token));
        client.Poll();
        Assert.Equal(DeliveryStatus.Canceled, client.GetDeliveryStatus(canceled.Token));

        // Once handed to the transport (alone or packed) a message cannot be canceled.
        SendResult handed = client.SendCopy(new SendHeader(2), [5], SendOptions.Tracked);
        client.Flush();
        Assert.False(client.TryCancel(handed.Token));
        SendResult memberA = client.SendCopy(new SendHeader(2), [6], SendOptions.Tracked);
        SendResult memberB = client.SendCopy(new SendHeader(2), [7], SendOptions.Tracked);
        client.Flush();
        Assert.False(client.TryCancel(memberA.Token));
        Assert.False(client.TryCancel(memberB.Token));
        Assert.True(h.RunUntil(() => client.GetDeliveryStatus(memberB.Token) == DeliveryStatus.Delivered));
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(memberA.Token));

        // Queued when the session closes: Disconnected.
        SendResult queued = client.SendCopy(new SendHeader(3, 1), [8], SendOptions.Tracked);
        client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.Equal(DeliveryStatus.Disconnected, client.GetDeliveryStatus(queued.Token));
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
    }

    [Fact]
    public void A_Datagram_The_Transport_Cancels_While_Queued_Completes_Expired()
    {
        LinkOptions link = new() { BandwidthBitsPerSecond = 8_000 };
        link.MtuChanges.Add(new MtuChange(1_500_000, 300));
        using SessionHarness h = new(link: link, table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        Assert.True(h.Network.NowMicros < 1_000_000);
        h.Run(1_000_000 - h.Network.NowMicros);

        // Two 901-byte datagrams at 1 000 bytes per second: the first is on the wire when the limit shrinks at 1.5 s, the
        // second still waits in the link's queue and no longer fits, so the transport cancels it.
        SendResult first = h.Client.SendCopy(new SendHeader(2), new byte[900], SendOptions.Tracked);
        SendResult second = h.Client.SendCopy(new SendHeader(2), new byte[900], SendOptions.Tracked);
        h.Client.Flush();
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(first.Token) != DeliveryStatus.Pending
            && h.Client.GetDeliveryStatus(second.Token) != DeliveryStatus.Pending, maxMicros: 5_000_000));
        Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(first.Token));
        Assert.Equal(DeliveryStatus.Expired, h.Client.GetDeliveryStatus(second.Token));
    }

    [Theory]
    [InlineData(CompletionMode.PollOnly)]
    [InlineData(CompletionMode.ThreadPool)]
    public void Members_Of_A_Container_Complete_With_It(CompletionMode mode)
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 }, table: Table,
            client: o =>
            {
                DatagramKit.Quiet(o);
                o.CompletionMode = mode;
            },
            server: DatagramKit.Quiet);
        PeerCore core = h.Client.Core;
        SendResult[] results = [.. Enumerable.Range(0, 3).Select(i => h.Client.SendCopy(new SendHeader(2), [(byte)i], SendOptions.Tracked))];
        h.Client.Flush();
        Assert.Equal(1, DatagramKit.Statistics(h.Client).ContainersSent);
        h.Network.Advance(0);
        Assert.All(results, r => Assert.False(core.Completions.IsCompleted(r.Token, CompletionStage.BufferReleased)));
        h.Client.Poll();
        Assert.All(results, r => Assert.True(core.Completions.IsCompleted(r.Token, CompletionStage.BufferReleased)));
        Assert.All(results, r => Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(r.Token)));
        h.Network.Advance(20_000);
        h.Client.Poll();
        Assert.All(results, r => Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(r.Token)));
        Assert.Equal(0, DatagramKit.Statistics(h.Client).SendEntriesInUse);
    }

    [Fact]
    public void Compressible_Payloads_Travel_Compressed_And_Arrive_Decoded_On_Every_Path()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(5, Handlers.Collect(got));
        QuiclyPeer client = h.Client;
        byte[] text = new byte[600];
        for (int i = 0; i < text.Length; i++)
        {
            text[i] = (byte)"the quick brown fox "[i % 20];
        }

        byte[] random = new byte[600];
        new Random(3).NextBytes(random);
        byte[] small = [.. text.AsSpan(0, 10)];

        client.SendCopy(new SendHeader(5), text);
        client.SendCopy(new SendHeader(5), random);
        client.SendCopy(new SendHeader(5), small);
        client.SendCopy(new SendHeader(5), []);
        BufferLease owned = client.RentBuffer(600);
        text.CopyTo(client.GetBufferSpan(in owned));
        client.SendOwned(new SendHeader(5), owned, 600);
        client.SendBorrowed(new SendHeader(5), text);
        using NativeMemoryManager manager = new(600);
        text.CopyTo(manager.GetSpan());
        client.SendBorrowed(new SendHeader(5), manager.Memory);
        unsafe
        {
            fixed (byte* pointer = text)
            {
                client.SendPinned(new SendHeader(5), pointer, 600);
                client.Flush();
            }
        }

        BufferLease pageA = client.RentBuffer(256);
        BufferLease pageB = client.RentBuffer(256);
        byte[] pages = [.. text.AsSpan(0, 256), .. text.AsSpan(0, 256)];
        pages.AsSpan(0, 256).CopyTo(client.GetBufferSpan(in pageA));
        pages.AsSpan(256).CopyTo(client.GetBufferSpan(in pageB));
        client.SendGather(new SendHeader(5), [pageA, pageB]);
        Assert.True(h.RunUntil(() => got.Count == 9));

        Assert.Equal(text, got[0].Payload);
        Assert.Equal(600, got[0].Header.Length);
        Assert.Equal(600, got[0].Header.RawLength);
        Assert.Equal(ReceiveFlags.Compressed, got[0].Header.Flags & ReceiveFlags.Compressed);
        Assert.Equal(random, got[1].Payload);
        Assert.Equal(0, got[1].Header.RawLength);
        Assert.Equal(small, got[2].Payload);
        Assert.Equal(0, got[2].Header.RawLength);
        Assert.Empty(got[3].Payload);
        Assert.Equal(0, got[3].Header.RawLength);
        for (int i = 4; i <= 7; i++)
        {
            Assert.Equal(text, got[i].Payload);
            Assert.Equal(600, got[i].Header.RawLength);
        }

        Assert.Equal(pages, got[8].Payload);
        Assert.Equal(512, got[8].Header.RawLength);
        Assert.True(DatagramKit.ChannelStats(client, 5).BytesSent < 9 * 600);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).SendEntriesInUse == 0));
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
        Assert.Equal(0, DatagramKit.Statistics(h.Server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void The_Wire_Carries_RawLength_Only_When_Compression_Shrinks_The_Payload()
    {
        using ClientHarness h = new(table: Table, client: DatagramKit.Quiet);
        Assert.True(h.Accept());
        byte[] text = new byte[600];
        Array.Fill(text, (byte)'x');
        byte[] random = new byte[600];
        new Random(5).NextBytes(random);
        h.Client.SendCopy(new SendHeader(5), text);
        h.Client.Flush();
        h.Client.SendCopy(new SendHeader(5), random);
        h.Client.Flush();
        h.Client.SendCopy(new SendHeader(5), text.AsSpan(0, 15));
        h.Client.Flush();
        Assert.True(h.RunUntil(() => DatagramKit.ApplicationDatagrams(h.Sink).Count == 3));
        List<(MessageHeader Header, byte[] Payload)> messages = DatagramKit.ApplicationDatagrams(h.Sink).SelectMany(d => DatagramKit.Messages(d, Table)).ToList();
        Assert.Equal(600, messages[0].Header.RawLength);
        Assert.True(messages[0].Payload.Length < 100);
        Assert.Equal(0, messages[1].Header.RawLength);
        Assert.Equal(random, messages[1].Payload);
        Assert.Equal(0, messages[2].Header.RawLength);
        Assert.Equal(15, messages[2].Payload.Length);
    }

    [Fact]
    public void Admission_Refuses_What_Cannot_Be_Sent()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.SendTableCapacity = 16;
        }, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        Assert.Equal(SendStatus.TooLarge, client.SendCopy(new SendHeader(2), new byte[1201]).Status);
        Assert.Equal(SendStatus.TooLarge, client.SendCopy(new SendHeader(2), new byte[1200]).Status);
        Assert.Equal(2, DatagramKit.ChannelStats(client, 2).TooLarge);
        Assert.Equal(SendStatus.NotSupported, client.SendCopy(new SendHeader(13), new byte[2000]).Status);
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(13), new byte[100]).Status);
        Assert.Throws<ArgumentOutOfRangeException>(() => client.SendCopy(new SendHeader(3, 1UL << 62), [1]));
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(2, ulong.MaxValue), [1]).Status);
        client.Flush();

        SendStatus last = SendStatus.Admitted;
        int admitted = 0;
        for (int i = 0; i < 20 && last == SendStatus.Admitted; i++)
        {
            last = client.SendCopy(new SendHeader(2), [1]).Status;
            admitted += last == SendStatus.Admitted ? 1 : 0;
        }

        Assert.Equal(SendStatus.QueueFull, last);
        Assert.InRange(admitted, 10, 16);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).SendEntriesInUse == 0));
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(2), [1]).Status);
    }

    [Fact]
    public void An_Exhausted_Send_Budget_Answers_OutOfBuffers()
    {
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.SendBudgetBytes = 256;
        }, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        Assert.Equal(SendStatus.OutOfBuffers, client.SendCopy(new SendHeader(2), new byte[300]).Status);
        SendStatus last = SendStatus.Admitted;
        for (int i = 0; i < 10 && last == SendStatus.Admitted; i++)
        {
            last = client.SendCopy(new SendHeader(2), new byte[60]).Status;
        }

        Assert.Equal(SendStatus.OutOfBuffers, last);
        client.Flush();
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).SendBytesOutstanding == 0));
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(2), new byte[60]).Status);
    }
}
