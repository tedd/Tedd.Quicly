using System.Reflection;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Review (recheck 2, round 1), the interleaving {publish, Poll's ClearWorkSignal}. Poll re-arms the work signal with
/// <c>Volatile.Write(ref _workSignalled, 0)</c> (QuiclyPeer.Work.cs, ClearWorkSignal) and then reads what it drains. On
/// x64 a volatile write is a plain store (no fence: the JIT emits <c>mov</c>), so the reads that follow it before the
/// next interlocked operation (<c>Interlocked.Exchange(ref _signals, 0)</c> in ProcessSignals) can be satisfied while
/// the store still sits in the store buffer: the completion ring (DrainCompletionsMarking) and the thread-safe send
/// front (DrainForeignSends). A publisher that enqueues in that window and whose <c>Interlocked.Exchange</c> in
/// NoteWork wins the cache line reads the old 1 and stays silent; the Poll has already looked and found nothing. The
/// work then waits with no OnWork — until the next publication or the host's own timer.
/// The probe added in 3a18b6d does this correctly (Interlocked.Exchange, then the level is read again); Poll does not.
/// Same on main (19cb971): not a regression of the branch.
/// <para>
/// <b>Fails in a Release build only</b> (<c>dotnet test -c Release</c>; about 100 to 400 of 400 000 rounds on the review
/// machine, x64): in a Debug build the unoptimised code between the store and the read is long enough for the store to
/// become visible, and the test passes. With <c>Interlocked.Exchange(ref _workSignalled, 0)</c> in ClearWorkSignal it
/// passes in Release as well (checked on a scratch copy).
/// </para>
/// </summary>
/// <remarks>
/// Fixed since: <c>ClearWorkSignal</c> clears the word with <c>Interlocked.Exchange</c> when it is set, a full fence before
/// the first read of the Poll, so a publication is seen by that Poll or finds the edge armed. The description above is
/// what the test found before the fix; it guards the fix only in a Release build.
/// </remarks>
public class ReviewRecheck2R1PollRearmTests
{
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "fx", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Build();

    private sealed class ThreadSignal : IPeerWorkSignal
    {
        public int ForeignThread;
        public long ForeignCalls;
        public long OtherCalls;

        public void OnWork(QuiclyPeer peer)
        {
            if (Environment.CurrentManagedThreadId == ForeignThread)
            {
                ForeignCalls++;
            }
            else
            {
                OtherCalls++;
            }
        }
    }

    private static int s_dummy;

    private static void Spin(int n)
    {
        for (int k = 0; k < n; k++)
        {
            Volatile.Read(ref s_dummy);
        }
    }

    private static byte[] CaptureDatagram()
    {
        using ClientHarness c = new(table: Table, client: DatagramKit.Quiet);
        Assert.True(c.Accept());
        c.Run(20_000);
        int before = DatagramKit.ApplicationDatagrams(c.Sink).Count;
        Assert.Equal(SendStatus.Admitted, c.Client.SendCopy(new SendHeader(2), [1, 2, 3, 4]).Status);
        c.Run(5_000);
        List<byte[]> all = DatagramKit.ApplicationDatagrams(c.Sink);
        Assert.Equal(before + 1, all.Count);
        return all[before];
    }

    /// <summary>
    /// One thread plays the transport thread and another application thread: per round it delivers a datagram (the edge
    /// is consumed, as it is whenever a host polls because it was signalled) and then makes a thread-safe send. The game
    /// thread polls at about the same moment. Afterwards either the Poll admitted the send, or the send raised OnWork.
    /// A send that is still waiting in the front although nobody was told is a lost wake-up.
    /// </summary>
    [Fact]
    public void A_Send_From_Another_Thread_That_Races_The_Start_Of_A_Poll_Is_Admitted_By_It_Or_Signalled()
    {
        const int rounds = 400_000;
        byte[] datagram = CaptureDatagram();
        ThreadSignal signal = new();
        using SessionHarness h = new(table: Table, client: QuietOptions.Apply, server: o =>
        {
            QuietOptions.Apply(o);
            o.WorkSignal = signal;
            o.ThreadSafeSend = true;
        });
        QuiclyPeer server = h.Server!;
        ITransportSink sink = server.TransportSink;
        int handled = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => handled++);
        h.Client.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        h.Run(20_000);
        server.Poll();
        server.Flush();

        object front = typeof(QuiclyPeer).GetField("_front", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(server)!;
        PropertyInfo isEmpty = front.GetType().GetProperty("IsEmpty")!;

        int go = 0;
        int injected = 0;
        int sent = 0;
        bool raisedBySend = false;
        bool sendAdmitted = false;
        Thread foreign = new(() =>
        {
            signal.ForeignThread = Environment.CurrentManagedThreadId;
            Random random = new(7);
            byte[] payload = [9, 9, 9, 9];
            for (int round = 1; round <= rounds; round++)
            {
                while (Volatile.Read(ref go) != round)
                {
                }

                sink.OnDatagramReceived(datagram); // raises OnWork: the edge is consumed when the Poll starts
                int delay = random.Next(0, 160);
                Volatile.Write(ref injected, round);
                Spin(delay);
                long before = signal.ForeignCalls;
                sendAdmitted = server.SendCopy(new SendHeader(2), payload).Status == SendStatus.Admitted;
                raisedBySend = signal.ForeignCalls != before;
                Volatile.Write(ref sent, round);
            }
        }) { IsBackground = true };
        foreign.Start();

        Random gameRandom = new(11);
        int lost = 0;
        int firstLostRound = 0;
        int raced = 0;
        for (int round = 1; round <= rounds; round++)
        {
            int delay = gameRandom.Next(0, 160);
            Volatile.Write(ref go, round);
            while (Volatile.Read(ref injected) != round)
            {
            }

            Spin(delay);
            long otherBefore = signal.OtherCalls;
            server.Poll();
            while (Volatile.Read(ref sent) != round)
            {
            }

            bool waiting = !(bool)isEmpty.GetValue(front)!;
            bool signalledOnGameThread = signal.OtherCalls != otherBefore;
            raced += waiting ? 1 : 0;
            if (sendAdmitted && waiting && !raisedBySend && !signalledOnGameThread)
            {
                lost++;
                if (firstLostRound == 0)
                {
                    firstLostRound = round;
                }
            }

            server.Poll();
            server.Flush();
            if ((round & 7) == 0)
            {
                h.Network.Advance(200);
                h.Client.Poll();
                h.Client.Flush();
            }
        }

        foreign.Join();
        Assert.True(raced > 100, $"the send landed after the Poll's look at the front only {raced} time(s): the race was not exercised");
        Assert.True(lost == 0,
            $"{lost} of {rounds} thread-safe sends ({raced} landed after the Poll had looked) were left waiting in the front without an OnWork: " +
            $"the Poll had cleared the edge but its store was not visible yet when the sender's NoteWork exchanged it (first in round {firstLostRound})");
    }
}
