using System.Net;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Second adversarial review of fix/group-stream-receive-limit (lens: the ReliableLatest half, ecf9d4c). A large value whose
/// stream start the peer's stream limit refused asynchronously is "taken back" and waits for stream credit. Each test states
/// one property of that wait. The ones named <c>Guard_</c> pass on the branch and pin behaviour the review checked; the
/// others fail where the branch does not hold the property.
/// </summary>
public class ReviewGroup2LatestTests
{
    private static readonly ChannelTable Table = LatestTables.Main;

    /// <summary>The client's transport wrapped twice: asynchronous start refusals, and datagrams that are reported lost.</summary>
    private sealed class Rig : IDisposable
    {
        private LossyConnector _connector = null!;

        public Rig(Action<PeerOptions>? client = null)
        {
            H = new SessionHarness(link: new LinkOptions { DelayMicros = 2_000 }, table: Table,
                client: o =>
                {
                    LatestKit.Quiet(o);
                    LatestKit.Roomy(o);
                    client?.Invoke(o);
                },
                server: o =>
                {
                    LatestKit.Quiet(o);
                    LatestKit.Roomy(o);
                },
                connector: inner => _connector = new LossyConnector(new AsyncRefusalConnector(inner)));
            H.Run(50_000);
            H.Server!.RegisterHandler(2, LatestKit.Collect(Received));
            H.Server!.RegisterHandler(8, LatestKit.Collect(Received));
        }

        public SessionHarness H { get; }

        public QuiclyPeer Client => H.Client;

        public AsyncRefusalTransport Refusals => _connector.Refusals!;

        public LossyTransport Lossy => _connector.Transport!;

        public List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> Received { get; } = [];

        /// <summary>One pass of both peers, the transport thread's reports, then <paramref name="micros"/> of link time.</summary>
        public void Step(long micros = 1_000)
        {
            H.Pump();
            Refusals.Deliver();
            Lossy.Deliver();
            H.Network.Advance(micros);
        }

        public void Run(long micros, long step = 1_000)
        {
            long end = H.Network.NowMicros + micros;
            while (H.Network.NowMicros < end)
            {
                Step(step);
            }

            H.Pump();
        }

        public bool RunUntil(Func<bool> condition, long maxMicros, long step = 1_000)
        {
            long end = H.Network.NowMicros + maxMicros;
            while (!condition())
            {
                if (H.Network.NowMicros >= end)
                {
                    return false;
                }

                Step(step);
            }

            return true;
        }

        /// <summary>Sets a large value and has its start refused: the channel is then blocked on stream credit.</summary>
        public SendResult BlockedLargeValue(ushort channel, ulong key, int id, int length = 8_000)
        {
            Refusals.RefuseStarts = int.MaxValue;
            int before = Refusals.Refused;
            SendResult result = Client.SendCopy(new SendHeader(channel, key), LatestKit.Payload(id, length), SendOptions.Tracked);
            Assert.True(result.IsAdmitted);
            Run(20_000);
            Assert.Equal(before + 1, Refusals.Refused);
            Assert.Equal(DeliveryStatus.Pending, Client.GetDeliveryStatus(result.Token));
            return result;
        }

        public void LetStreamsThrough()
        {
            Refusals.RefuseStarts = 0;
            Refusals.GrantCredit();
        }

        public void Dispose() => H.Dispose();
    }

    // ------------------------------------------------------------------ findings

    /// <summary>
    /// A large value is blocked on stream credit (the late receiver of the group fix holds every slot). Meanwhile the
    /// application keeps updating that key, as a ReliableLatest key is meant to be updated. The replacement is a fresh value
    /// and stays at the head of the channel's fresh queue, because <c>TrySendLarge</c> answers "blocked" and
    /// <c>DrainQueue</c> stops the channel's pass at the first value it cannot transmit. Every small value admitted after it
    /// — other keys, which fit a datagram and need no stream at all — is stuck behind it for as long as the credit is
    /// missing. PROTOCOL.md §7 says a large value "waits"; it does not say it takes the channel's datagram traffic with it.
    /// </summary>
    [Fact]
    public void A_Large_Value_Waiting_For_Stream_Credit_Does_Not_Hold_Back_The_Channels_Small_Values()
    {
        using Rig rig = new();
        rig.BlockedLargeValue(2, key: 9, id: 7);

        // The key is updated while the channel is blocked: the new large value is fresh, and blocked.
        SendResult large = rig.Client.SendCopy(new SendHeader(2, 9), LatestKit.Payload(8, 8_000), SendOptions.Tracked);
        SendResult small = rig.Client.SendCopy(new SendHeader(2, 1), LatestKit.Payload(3, 32), SendOptions.Tracked);
        Assert.True(large.IsAdmitted && small.IsAdmitted);

        bool arrived = rig.RunUntil(() => rig.Client.GetDeliveryStatus(small.Token) == DeliveryStatus.Delivered, 2_000_000);
        ChannelStatistics statistics = DatagramKit.ChannelStats(rig.Client, 2);
        Assert.True(arrived,
            $"two seconds without stream credit: the 32-byte value of another key is {rig.Client.GetDeliveryStatus(small.Token)} "
            + $"(the server received {rig.Received.Count} values; the channel reports {statistics.QueuedMessages} queued, {statistics.Sent} sent)");
    }

    /// <summary>
    /// The same head-of-line wait in the retry queue, which is where ecf9d4c parks a refused value: the canceled completion
    /// of the refused start puts it in the channel's retry queue, and there it is the head. A small value of another key
    /// whose datagram the transport reports lost is queued behind it and is not retransmitted until stream credit arrives.
    /// </summary>
    [Fact]
    public void A_Large_Value_Waiting_For_Stream_Credit_Does_Not_Hold_Back_The_Retransmission_Of_A_Lost_Small_Value()
    {
        using Rig rig = new();
        rig.BlockedLargeValue(2, key: 9, id: 7);

        rig.Lossy.LoseDatagrams = 1;
        SendResult small = rig.Client.SendCopy(new SendHeader(2, 1), LatestKit.Payload(3, 32), SendOptions.Tracked);
        Assert.True(small.IsAdmitted);
        rig.Run(10_000);
        Assert.Equal(1, rig.Lossy.Lost);

        bool arrived = rig.RunUntil(() => rig.Client.GetDeliveryStatus(small.Token) == DeliveryStatus.Delivered, 2_000_000);
        Assert.True(arrived,
            $"two seconds after its datagram was lost the 32-byte value is {rig.Client.GetDeliveryStatus(small.Token)}; "
            + $"the channel counted {DatagramKit.ChannelStats(rig.Client, 2).Retries} retransmissions");
    }

    /// <summary>
    /// A refused start put nothing on the wire, and the commit says it is "taken back". The aggregate retry budget
    /// (<see cref="PeerOptions.MaxRetryBytesPerSecond"/>, PROTOCOL.md §4.4) is not given back: the value now sits in the retry
    /// queue, so its next start is gated by the budget and charged to it in full, refused or not. With a 60 000-byte value
    /// and the 16 KiB/s floor, one refused retry costs 3.6 s during which nothing of this peer is retransmitted — and the
    /// value itself, which has never left the host, waits that long after the credit that would let it go.
    /// </summary>
    [Fact]
    public void A_Refused_Start_Is_Not_Charged_To_The_Retry_Budget()
    {
        using Rig rig = new(o => o.MaxRetryBytesPerSecond = 16 * 1024);
        SendResult large = rig.BlockedLargeValue(2, key: 9, id: 7, length: 60_000);

        // Credit arrives, another channel's stream takes it, and the value's next start is refused again.
        rig.Refusals.GrantCredit();
        rig.Run(20_000);
        Assert.Equal(2, rig.Refusals.Refused);
        Assert.Equal(0, DatagramKit.ChannelStats(rig.Client, 2).Sent);

        // Now a slot is free. Nothing of the value was ever transmitted, so nothing is owed to the retry budget.
        rig.LetStreamsThrough();
        long start = rig.H.Network.NowMicros;
        bool delivered = rig.RunUntil(() => rig.Client.GetDeliveryStatus(large.Token) == DeliveryStatus.Delivered, 1_000_000);
        if (!delivered)
        {
            rig.RunUntil(() => rig.Client.GetDeliveryStatus(large.Token) != DeliveryStatus.Pending, 40_000_000, 10_000);
        }

        Assert.True(delivered,
            $"a second after a stream slot was free the value was still waiting; it became {rig.Client.GetDeliveryStatus(large.Token)} "
            + $"{(rig.H.Network.NowMicros - start) / 1000} ms after the credit");
    }

    /// <summary>
    /// <see cref="ChannelStatistics.Sent"/> is documented as monotonic ("the count never goes back", PeerStatistics.cs). The
    /// take-back decrements it (and <c>BytesSent</c>), so a host that samples the statistics and reports the difference sees
    /// a negative rate whenever a start is refused.
    /// </summary>
    [Fact]
    public void Sent_Never_Goes_Back_As_Its_Documentation_Says()
    {
        using Rig rig = new();
        rig.Refusals.RefuseStarts = int.MaxValue;
        Assert.True(rig.Client.SendCopy(new SendHeader(2, 9), LatestKit.Payload(7, 8_000), SendOptions.Tracked).IsAdmitted);
        rig.Client.Flush();
        ChannelStatistics before = DatagramKit.ChannelStats(rig.Client, 2);
        rig.Run(20_000);
        ChannelStatistics after = DatagramKit.ChannelStats(rig.Client, 2);
        Assert.True(after.Sent >= before.Sent && after.BytesSent >= before.BytesSent,
            $"Sent went from {before.Sent} to {after.Sent}, BytesSent from {before.BytesSent} to {after.BytesSent}");
    }

    /// <summary>
    /// The take-back restores <c>Attempts</c>, <c>Sent</c>, <c>Bytes</c> and <c>Retries</c>, but not the value's in-flight
    /// accounting: <c>InFlightMessages</c> ("handed to the transport and not yet completed") stays 1 and
    /// <c>QueuedMessages</c> ("admitted and not yet handed to the transport") stays 0 for a value of which nothing was sent
    /// and nothing is outstanding. A host that watches the queue to see why a value does not arrive sees a value in flight.
    /// </summary>
    [Fact]
    public void A_Value_Waiting_For_Stream_Credit_Is_Reported_As_Queued_Not_As_In_Flight()
    {
        using Rig rig = new();
        rig.BlockedLargeValue(2, key: 9, id: 7);
        rig.Run(100_000);
        ChannelStatistics statistics = DatagramKit.ChannelStats(rig.Client, 2);
        Assert.True(statistics.InFlightMessages == 0 && statistics.QueuedMessages == 1,
            $"Sent {statistics.Sent}, QueuedMessages {statistics.QueuedMessages}, InFlightMessages {statistics.InFlightMessages} ({statistics.InFlightBytes} bytes)");
    }

    /// <summary>
    /// The author's stated weak spot: the transmission that follows a refused first start is the value's first, and is
    /// counted in <c>Retries</c>.
    /// </summary>
    [Fact]
    public void The_First_Transmission_After_A_Refused_Start_Is_Not_A_Retransmission()
    {
        using Rig rig = new();
        SendResult large = rig.BlockedLargeValue(2, key: 9, id: 7);
        rig.LetStreamsThrough();
        Assert.True(rig.RunUntil(() => rig.Client.GetDeliveryStatus(large.Token) == DeliveryStatus.Delivered, 5_000_000));
        ChannelStatistics statistics = DatagramKit.ChannelStats(rig.Client, 2);
        Assert.True(statistics.Sent == 1 && statistics.Retries == 0, $"Sent {statistics.Sent}, Retries {statistics.Retries}");
    }

    // ------------------------------------------------------------------ guards (pass on the branch)

    [Fact]
    public void Guard_A_Blocked_Value_Replaced_By_A_Large_One_Goes_Out_When_Credit_Arrives()
    {
        using Rig rig = new();
        SendResult first = rig.BlockedLargeValue(2, key: 9, id: 7);
        SendResult second = rig.Client.SendCopy(new SendHeader(2, 9), LatestKit.Payload(8, 9_000), SendOptions.Tracked);
        rig.Run(200_000);
        Assert.Equal(DeliveryStatus.Superseded, rig.Client.GetDeliveryStatus(first.Token));
        Assert.Equal(DeliveryStatus.Pending, rig.Client.GetDeliveryStatus(second.Token));
        Assert.Equal(1, rig.Refusals.Refused);

        rig.LetStreamsThrough();
        Assert.True(rig.RunUntil(() => rig.Client.GetDeliveryStatus(second.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {rig.Client.GetDeliveryStatus(second.Token)}");
        Assert.True(LatestKit.Matches(rig.Received[^1].Payload, 8, 9_000));
        Assert.Equal(0, LatestKit.LiveKeys(rig.Client, 2));
    }

    /// <summary>The replacement arrives before the refusal is drained: the notice then finds another value in the key.</summary>
    [Fact]
    public void Guard_A_Value_Replaced_Between_The_Refusal_And_Its_Notice_Leaves_The_New_Value_Whole()
    {
        using Rig rig = new();
        rig.Refusals.RefuseStarts = int.MaxValue;
        SendResult first = rig.Client.SendCopy(new SendHeader(2, 9), LatestKit.Payload(7, 8_000), SendOptions.Tracked);
        rig.Client.Flush();
        SendResult second = rig.Client.SendCopy(new SendHeader(2, 9), LatestKit.Payload(8, 9_000), SendOptions.Tracked);
        rig.Run(200_000);
        Assert.Equal(DeliveryStatus.Superseded, rig.Client.GetDeliveryStatus(first.Token));
        Assert.Equal(DeliveryStatus.Pending, rig.Client.GetDeliveryStatus(second.Token));
        ChannelStatistics waiting = DatagramKit.ChannelStats(rig.Client, 2);
        Assert.True(waiting.Sent >= 0 && waiting.BytesSent >= 0 && waiting.Retries >= 0, $"Sent {waiting.Sent}, Bytes {waiting.BytesSent}, Retries {waiting.Retries}");

        rig.LetStreamsThrough();
        Assert.True(rig.RunUntil(() => rig.Client.GetDeliveryStatus(second.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {rig.Client.GetDeliveryStatus(second.Token)}");
        Assert.True(LatestKit.Matches(rig.Received[^1].Payload, 8, 9_000));
    }

    [Fact]
    public void Guard_A_Blocked_Value_Replaced_By_A_Small_One_Needs_No_Credit()
    {
        using Rig rig = new();
        SendResult first = rig.BlockedLargeValue(2, key: 9, id: 7);
        SendResult second = rig.Client.SendCopy(new SendHeader(2, 9), LatestKit.Payload(8, 40), SendOptions.Tracked);
        Assert.True(rig.RunUntil(() => rig.Client.GetDeliveryStatus(second.Token) == DeliveryStatus.Delivered, 2_000_000),
            $"status {rig.Client.GetDeliveryStatus(second.Token)}");
        Assert.Equal(DeliveryStatus.Superseded, rig.Client.GetDeliveryStatus(first.Token));
        Assert.True(LatestKit.Matches(rig.Received[^1].Payload, 8, 40));
        Assert.Equal(1, rig.Refusals.Refused);
    }

    [Fact]
    public void Guard_A_Blocked_Value_That_Is_Canceled_Or_Retired_Leaves_The_Channel_Usable()
    {
        using Rig rig = new();
        SendResult canceled = rig.BlockedLargeValue(2, key: 9, id: 7);
        Assert.True(rig.Client.TryCancel(canceled.Token));
        rig.Run(50_000);
        Assert.Equal(DeliveryStatus.Canceled, rig.Client.GetDeliveryStatus(canceled.Token));

        SendResult retired = rig.Client.SendCopy(new SendHeader(2, 10), LatestKit.Payload(9, 8_000), SendOptions.Tracked);
        rig.Run(50_000);
        Assert.Equal(DeliveryStatus.Pending, rig.Client.GetDeliveryStatus(retired.Token));
        Assert.Equal(SendStatus.Admitted, rig.Client.RetireKey(2, 10));
        rig.Run(50_000);
        Assert.Equal(DeliveryStatus.Canceled, rig.Client.GetDeliveryStatus(retired.Token));
        Assert.Equal(1, rig.Refusals.Refused);
        Assert.Equal(0, LatestKit.LiveKeys(rig.Client, 2));

        // No credit has arrived: the next large value still waits, and goes out when it does.
        SendResult next = rig.Client.SendCopy(new SendHeader(2, 11), LatestKit.Payload(10, 8_000), SendOptions.Tracked);
        rig.Run(200_000);
        Assert.Equal(DeliveryStatus.Pending, rig.Client.GetDeliveryStatus(next.Token));
        Assert.Equal(1, rig.Refusals.Refused);
        rig.LetStreamsThrough();
        Assert.True(rig.RunUntil(() => rig.Client.GetDeliveryStatus(next.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {rig.Client.GetDeliveryStatus(next.Token)}");
        Assert.True(LatestKit.Matches(rig.Received[^1].Payload, 10, 8_000));
    }

    [Fact]
    public void Guard_Several_Keys_Refused_In_One_Pass_All_Go_Out_When_Credit_Arrives()
    {
        using Rig rig = new();
        rig.Refusals.RefuseStarts = int.MaxValue;
        List<SendResult> values = [];
        for (int key = 0; key < 3; key++)
        {
            values.Add(rig.Client.SendCopy(new SendHeader(2, (ulong)(20 + key)), LatestKit.Payload(key, 8_000 + key), SendOptions.Tracked));
        }

        rig.Run(300_000);
        Assert.Equal(3, rig.Refusals.Refused);
        Assert.All(values, value => Assert.Equal(DeliveryStatus.Pending, rig.Client.GetDeliveryStatus(value.Token)));
        ChannelStatistics waiting = DatagramKit.ChannelStats(rig.Client, 2);
        Assert.True(waiting.Sent == 0 && waiting.BytesSent == 0 && waiting.Retries == 0, $"Sent {waiting.Sent}, Bytes {waiting.BytesSent}, Retries {waiting.Retries}");

        rig.LetStreamsThrough();
        Assert.True(rig.RunUntil(() => values.TrueForAll(value => rig.Client.GetDeliveryStatus(value.Token) == DeliveryStatus.Delivered), 5_000_000),
            string.Join(", ", values.Select(value => rig.Client.GetDeliveryStatus(value.Token))));
        Assert.Equal(3, rig.Received.Count);
    }

    /// <summary>One credit, three waiting values: one goes, two are refused again, and each later credit lets one more go.</summary>
    [Fact]
    public void Guard_Credit_For_One_Stream_At_A_Time_Still_Drains_Every_Blocked_Key()
    {
        using Rig rig = new();
        rig.Refusals.RefuseStarts = int.MaxValue;
        List<SendResult> values = [];
        for (int key = 0; key < 3; key++)
        {
            values.Add(rig.Client.SendCopy(new SendHeader(2, (ulong)(20 + key)), LatestKit.Payload(key, 8_000 + key), SendOptions.Tracked));
        }

        rig.Run(100_000);
        for (int round = 0; round < 3; round++)
        {
            // Exactly one start is let through per grant; the others of the same pass are refused again.
            rig.Lossy.StartsToLetThrough = 1;
            rig.Refusals.GrantCredit();
            rig.Run(300_000);
            int delivered = values.Count(value => rig.Client.GetDeliveryStatus(value.Token) == DeliveryStatus.Delivered);
            Assert.True(delivered >= round + 1, $"round {round}: {delivered} delivered");
        }

        Assert.Equal(3, rig.Received.Count);
        Assert.Equal(0, LatestKit.LiveKeys(rig.Client, 2));
    }

    /// <summary>A refusal the call reports after one the transport reported later: both wait on the credit generation.</summary>
    [Fact]
    public void Guard_An_Asynchronous_Refusal_Followed_By_A_Synchronous_One_Still_Waits_And_Then_Goes_Out()
    {
        using Rig rig = new();
        SendResult large = rig.BlockedLargeValue(2, key: 9, id: 7);
        rig.Refusals.Synchronous = true;
        rig.Refusals.GrantCredit();
        rig.Run(300_000);
        Assert.Equal(2, rig.Refusals.Refused);
        Assert.Equal(DeliveryStatus.Pending, rig.Client.GetDeliveryStatus(large.Token));
        Assert.Equal(0, DatagramKit.ChannelStats(rig.Client, 2).Sent);

        rig.Refusals.Synchronous = false;
        rig.LetStreamsThrough();
        Assert.True(rig.RunUntil(() => rig.Client.GetDeliveryStatus(large.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {rig.Client.GetDeliveryStatus(large.Token)}");
        Assert.Equal(1, DatagramKit.ChannelStats(rig.Client, 2).Sent);
    }

    /// <summary>
    /// A host that sleeps until <see cref="QuiclyPeer.NextDeadlineMicros"/> or the work signal: stream credit raises no work
    /// signal (OnStreamsAvailable only bumps a generation), so the blocked value's own retry timer has to bring the host back.
    /// </summary>
    [Fact]
    public void Guard_A_Host_That_Sleeps_On_The_Deadline_Still_Sends_The_Value_After_Credit()
    {
        using Rig rig = new();
        SendResult large = rig.BlockedLargeValue(2, key: 9, id: 7);
        QuiclyPeer client = rig.Client;

        int passes = 0;
        void SleepyStep()
        {
            if (client.HasPendingWork || rig.H.Network.NowMicros >= client.NextDeadlineMicros)
            {
                client.Poll();
                client.Flush();
                passes++;
            }

            rig.H.Server!.Poll();
            rig.H.Server!.Flush();
            rig.Refusals.Deliver();
            rig.H.Network.Advance(1_000);
        }

        for (int step = 0; step < 500; step++)
        {
            SleepyStep();
        }

        Assert.Equal(DeliveryStatus.Pending, client.GetDeliveryStatus(large.Token));
        Assert.True(client.NextDeadlineMicros < rig.H.Network.NowMicros + 1_100_000, "a blocked value must keep a deadline within the retry interval");
        rig.LetStreamsThrough();
        long start = rig.H.Network.NowMicros;
        while (client.GetDeliveryStatus(large.Token) != DeliveryStatus.Delivered && rig.H.Network.NowMicros - start < 3_000_000)
        {
            SleepyStep();
        }

        Assert.True(client.GetDeliveryStatus(large.Token) == DeliveryStatus.Delivered,
            $"status {client.GetDeliveryStatus(large.Token)} three seconds after the credit ({passes} passes)");
    }

    /// <summary>The thirty-second version budget is the documented backstop: the value fails, and the channel is not left blocked for good.</summary>
    [Fact]
    public void Guard_A_Value_Blocked_For_Its_Whole_Budget_Fails_And_The_Next_One_Goes_Out()
    {
        using Rig rig = new();
        SendResult large = rig.BlockedLargeValue(2, key: 9, id: 7);
        rig.Run(29_000_000, 10_000);
        Assert.Equal(DeliveryStatus.Pending, rig.Client.GetDeliveryStatus(large.Token));
        rig.Run(2_000_000, 10_000);
        Assert.Equal(DeliveryStatus.Failed, rig.Client.GetDeliveryStatus(large.Token));
        Assert.Equal(1, rig.Refusals.Refused);
        Assert.Equal(0, LatestKit.LiveKeys(rig.Client, 2));
        ChannelStatistics failed = DatagramKit.ChannelStats(rig.Client, 2);
        Assert.True(failed.InFlightMessages == 0 && failed.QueuedMessages == 0, $"in flight {failed.InFlightMessages}, queued {failed.QueuedMessages}");

        SendResult next = rig.Client.SendCopy(new SendHeader(2, 9), LatestKit.Payload(8, 8_000), SendOptions.Tracked);
        rig.LetStreamsThrough();
        Assert.True(rig.RunUntil(() => rig.Client.GetDeliveryStatus(next.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {rig.Client.GetDeliveryStatus(next.Token)}");
    }

    /// <summary>A channel with one stream (MaxGroups 1): the refused stream gives its per-channel slot back, so the retry is not stuck behind its own refusal.</summary>
    [Fact]
    public void Guard_A_Refused_Stream_Gives_Back_The_Slot_Of_A_Channel_With_One_Group()
    {
        using Rig rig = new();
        SendResult large = rig.BlockedLargeValue(8, key: 9, id: 7);
        for (int round = 0; round < 20; round++)
        {
            rig.Refusals.GrantCredit();
            rig.Run(20_000);
        }

        Assert.Equal(21, rig.Refusals.Refused);
        Assert.Equal(DeliveryStatus.Pending, rig.Client.GetDeliveryStatus(large.Token));
        rig.LetStreamsThrough();
        Assert.True(rig.RunUntil(() => rig.Client.GetDeliveryStatus(large.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {rig.Client.GetDeliveryStatus(large.Token)}");
        Assert.True(LatestKit.Matches(rig.Received[^1].Payload, 7, 8_000));
    }
}

/// <summary>Reports the next datagrams as lost instead of sending them (the transport thread's report comes from <see cref="Deliver"/>).</summary>
internal sealed unsafe class LossyTransport(ITransport inner, ITransportSink sink) : ITransport
{
    private readonly List<ulong> _lost = [];

    public int LoseDatagrams { get; set; }

    public int Lost { get; private set; }

    /// <summary>When set: that many stream starts reach the wrapped transport unrefused, every later one is refused.</summary>
    public int? StartsToLetThrough { get; set; }

    public TransportCapabilities Capabilities => inner.Capabilities;

    public TransportState State => inner.State;

    public void Deliver()
    {
        foreach (ulong context in _lost)
        {
            sink.OnDatagramSendStateChanged(context, DatagramSendState.LostDiscarded);
        }

        _lost.Clear();
    }

    public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        if (LoseDatagrams > 0)
        {
            LoseDatagrams--;
            Lost++;
            _lost.Add(context);
            return TransportStatus.Success;
        }

        return inner.SendDatagram(segments, count, context, flags);
    }

    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id) => inner.OpenStream(kind, context, priority, out id);

    public TransportStatus StartStream(TransportStreamId id) => inner.StartStream(id);

    public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        if ((flags & TransportSendFlags.Start) != 0 && StartsToLetThrough is int left && inner is AsyncRefusalTransport refusals)
        {
            refusals.RefuseStarts = left > 0 ? 0 : int.MaxValue;
            StartsToLetThrough = Math.Max(0, left - 1);
        }

        return inner.SendStream(id, segments, count, context, flags);
    }

    public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => inner.AbortStream(id, errorCode, direction);

    public void SetStreamPriority(TransportStreamId id, ushort priority) => inner.SetStreamPriority(id, priority);

    public long GetQuicStreamId(TransportStreamId id) => inner.GetQuicStreamId(id);

    public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed) => inner.ResumeStreamReceive(id, bytesConsumed);

    public void CloseStream(TransportStreamId id) => inner.CloseStream(id);

    public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional) => inner.UpdatePeerStreamLimits(bidirectional, unidirectional);

    public void Close(ulong errorCode, ReadOnlySpan<byte> reason) => inner.Close(errorCode, reason);

    public void GetStatistics(out TransportStatistics statistics) => inner.GetStatistics(out statistics);

    public void Dispose() => inner.Dispose();
}

/// <summary>Wraps the transport of a connector in <see cref="LossyTransport"/> over <see cref="AsyncRefusalTransport"/>.</summary>
internal sealed class LossyConnector(AsyncRefusalConnector inner) : ITransportConnector
{
    public LossyTransport? Transport { get; private set; }

    public AsyncRefusalTransport? Refusals => inner.Transport;

    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) =>
        Transport = new LossyTransport(inner.Connect(endpoint, serverName, sink), sink);
}
