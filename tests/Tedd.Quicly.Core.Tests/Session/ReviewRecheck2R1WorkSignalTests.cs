using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Review (recheck 2, round 1) of 3a18b6d: <see cref="QuiclyPeer.HasPendingWork"/> re-arms the work signal when it finds
/// nothing. The publications are made by a real second thread here (the peer's own transport sink is called with
/// datagrams a client encoded), so the probe, the Poll and the Drain of the host race the transport thread's publish and
/// NoteWork as they do over MsQuic.
/// </summary>
public class ReviewRecheck2R1WorkSignalTests
{
    /// <summary>2 unordered with a handler; 3 keyed sequenced, coalescing (a mailbox), read with Drain.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "fx", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(3, "state", ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.CoalesceOnReceive = true;
            o.MaxKeys = 64;
            o.ExpiryMicros = 0;
        })
        .Build();

    private sealed class EventSignal : IPeerWorkSignal
    {
        public readonly ManualResetEventSlim Event = new(false);
        public long Calls;

        public void OnWork(QuiclyPeer peer)
        {
            Interlocked.Increment(ref Calls);
            Event.Set();
        }
    }

    /// <summary>Datagrams exactly as a client peer puts them on the wire: one on the handled channel, many mailbox values.</summary>
    private static (byte[] Handled, List<byte[]> Mailbox) Capture(int mailboxValues)
    {
        using ClientHarness c = new(table: Table, client: DatagramKit.Quiet);
        Assert.True(c.Accept());
        c.Run(20_000);
        int before = DatagramKit.ApplicationDatagrams(c.Sink).Count;
        Assert.Equal(SendStatus.Admitted, c.Client.SendCopy(new SendHeader(2), [1, 2, 3, 4]).Status);
        c.Run(5_000);
        List<byte[]> all = DatagramKit.ApplicationDatagrams(c.Sink);
        Assert.Equal(before + 1, all.Count);
        byte[] handled = all[before];
        byte[] payload = new byte[4];
        for (int i = 0; i < mailboxValues; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(payload, i);
            Assert.Equal(SendStatus.Admitted, c.Client.SendCopy(new SendHeader(3, (ulong)((i % 8) + 1)), payload).Status);
            c.Client.Flush();
            c.Network.Advance(200);
            c.Client.Poll();
        }

        c.Run(5_000);
        List<byte[]> mailbox = DatagramKit.ApplicationDatagrams(c.Sink).Skip(before + 1).ToList();
        Assert.Equal(mailboxValues, mailbox.Count);
        return (handled, mailbox);
    }

    private static SessionHarness Target(EventSignal signal, out QuiclyPeer server, out ITransportSink sink)
    {
        SessionHarness h = new(table: Table, client: QuietOptions.Apply, server: o =>
        {
            QuietOptions.Apply(o);
            o.WorkSignal = signal;
        });
        server = h.Server!;
        sink = server.TransportSink;
        h.Run(20_000);
        server.Poll();
        server.Flush();
        return h;
    }

    /// <summary>
    /// The storm question. A host wakes on every OnWork and asks the probe; the traffic is values for a channel it reads
    /// with Drain (the probe does not count them and answers false, which re-arms the edge). It gets one OnWork per
    /// transport callback, and none at all from probing alone: the rate is bounded by the peer's traffic, as the
    /// release notes say.
    /// </summary>
    [Fact]
    public void A_Host_That_Probes_On_Every_Wake_Gets_One_Signal_Per_Arrival_And_None_From_Probing()
    {
        (_, List<byte[]> mailbox) = Capture(40);
        EventSignal signal = new();
        using SessionHarness h = Target(signal, out QuiclyPeer server, out ITransportSink sink);
        Assert.False(server.HasPendingWork);
        long start = signal.Calls;
        for (int i = 0; i < mailbox.Count; i++)
        {
            sink.OnDatagramReceived(mailbox[i]);
            Assert.Equal(start + i + 1, signal.Calls);
            Assert.False(server.HasPendingWork, "a mailbox value of a channel without a handler is not work");
            for (int probe = 0; probe < 100; probe++)
            {
                Assert.False(server.HasPendingWork);
            }

            Assert.Equal(start + i + 1, signal.Calls);
        }

        // Without the probe in between the edge stays consumed: a burst costs one call.
        ReceivedMessage[] buffer = new ReceivedMessage[64];
        server.Release(buffer.AsSpan(0, server.Drain(3, buffer)));
        Assert.False(server.HasPendingWork);
    }

    /// <summary>
    /// The lost wake-up question, threaded. A transport thread publishes messages for a handler and values for a
    /// Drain-style mailbox channel, with pauses long enough for the host to fall asleep in between. The host sleeps on the
    /// signal and, woken, does what the documentation describes. If it ever sleeps its whole timeout while something is
    /// waiting, a wake-up was lost.
    /// </summary>
    /// <param name="style">
    /// 0: probe, Poll only when it says so, then Drain. 1: Poll on every wake, then Drain. 2: as 0, while a third thread
    /// asks the probe in a tight loop (the probe may be called from any thread, and every false answer re-arms the edge).
    /// </param>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void A_Host_That_Sleeps_On_The_Signal_Never_Sleeps_Over_Waiting_Work(int style)
    {
        const int handledMessages = 150_000;
        const int mailboxValues = 20_000;
        (byte[] handledDatagram, List<byte[]> mailbox) = Capture(mailboxValues);
        EventSignal signal = new();
        using SessionHarness h = Target(signal, out QuiclyPeer server, out ITransportSink sink);
        int handled = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => handled++);

        int published = 0;
        bool publisherDone = false;
        Thread publisher = new(() =>
        {
            Random random = new(style + 1);
            int nextValue = 0;
            for (int i = 0; i < handledMessages; i++)
            {
                if (nextValue < mailbox.Count && random.Next(7) == 0)
                {
                    sink.OnDatagramReceived(mailbox[nextValue++]);
                }

                sink.OnDatagramReceived(handledDatagram);
                Volatile.Write(ref published, i + 1);
                int pause = random.Next(64);
                Thread.SpinWait(pause == 0 ? 40_000 : pause * 4);
            }

            while (nextValue < mailbox.Count)
            {
                sink.OnDatagramReceived(mailbox[nextValue++]);
            }

            Volatile.Write(ref publisherDone, true);
        }) { IsBackground = true };

        bool stopProber = false;
        long foreignProbes = 0;
        Thread? prober = style == 2
            ? new Thread(() =>
            {
                while (!Volatile.Read(ref stopProber))
                {
                    _ = server.HasPendingWork;
                    foreignProbes++;
                }
            }) { IsBackground = true }
            : null;

        ReceivedMessage[] buffer = new ReceivedMessage[64];
        int values = 0;
        int lost = 0;
        string firstLoss = "";
        int wakes = 0;
        publisher.Start();
        prober?.Start();
        try
        {
            while (true)
            {
                bool signalled = signal.Event.Wait(400);
                bool done = Volatile.Read(ref publisherDone);
                if (!signalled)
                {
                    // Slept the whole timeout. Anything waiting now was published without a wake-up.
                    // (A publication that raced the end of the wait set the event: that one is not lost.)
                    bool raised = signal.Event.IsSet;
                    bool ring = !raised && !server.Core.ReceiveRing.IsEmpty;
                    bool level = server.HasPendingWork;
                    int n = server.Drain(3, buffer);
                    server.Release(buffer.AsSpan(0, n));
                    values += n;
                    if (ring || (n > 0 && !raised))
                    {
                        lost++;
                        if (firstLoss.Length == 0)
                        {
                            firstLoss = $"after {wakes} wakes, published {Volatile.Read(ref published)}, handled {handled}: ring {(ring ? "not empty" : "empty")}, probe {level}, mailbox values waiting {n}";
                        }

                        server.Poll();
                    }

                    if (done && server.Core.ReceiveRing.IsEmpty)
                    {
                        break;
                    }

                    continue;
                }

                wakes++;
                signal.Event.Reset();
                if (style == 1 || server.HasPendingWork)
                {
                    server.Poll();
                }

                int got;
                while ((got = server.Drain(3, buffer)) > 0)
                {
                    values += got;
                    server.Release(buffer.AsSpan(0, got));
                }
            }
        }
        finally
        {
            Volatile.Write(ref stopProber, true);
            publisher.Join();
            prober?.Join();
        }

        PeerStatistics stats = DatagramKit.Statistics(server);
        Assert.True(lost == 0, $"style {style}: the host slept its whole timeout over waiting work {lost} time(s); first: {firstLoss}; OnWork calls {signal.Calls}, foreign probes {foreignProbes}");
        Assert.True(handled + stats.ReceiveRingDrops + stats.OutOfReceiveBuffers == handledMessages,
            $"style {style}: handled {handled} of {handledMessages} (ReceiveRingDrops {stats.ReceiveRingDrops}, OutOfReceiveBuffers {stats.OutOfReceiveBuffers})");
        Assert.True(values > 0 && wakes > 1_000, $"style {style}: the run did not exercise the wake-up path (values {values}, wakes {wakes})");
    }
}
