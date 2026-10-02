using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review of the receive credit of the reliable stream channels (branch credit-v3, <see cref="ReceiveCredit"/>
/// and its call sites) through one lens: threading and lost wake-ups. A stream held back for credit is not retried by
/// every Poll, so every test here asks the same question in a different place: can a stream stay held back while its
/// channel has credit, with nothing scheduled to look again?
/// </summary>
/// <remarks>
/// Tests whose name starts with <c>Guard_</c> pass on the branch: they pin an interleaving that was examined and found
/// sound. The others state a property the branch claims and fail where it does not hold it. The one whose name starts
/// with <c>Outside_The_Lens_</c> fails as well, for a reason that is neither the credit nor threading (the code it
/// exercises is unchanged by the branch); it was met on the way and is kept so that it is not lost.
/// <para>
/// What fixed each finding (the comments of the tests describe the code as it was reviewed): 1 — the list of held-back
/// streams grows for the entries of streams that ended while they waited, and a peer that fills even the grown list is
/// disconnected; 2 — credit that came back is work until the game thread has looked (<c>ReceiveCredit.HasWork</c>), and
/// every Poll settles the limit of a channel whose handler was registered over a backlog; outside the lens —
/// <c>Route</c> puts a message of a channel that has messages queued behind them. One guard asserted that returned credit
/// does not set the probe; it now asserts the opposite, which is the fix of finding 2.
/// </para>
/// </remarks>
public class ReviewCreditThreadingTests
{
    private const int Ring = 64;

    /// <summary>Messages the two-thread model has to deliver before its run may end (and its final assertion asks for).</summary>
    private const long EnoughMessages = 50_000;

    /// <summary>Messages a reliable channel without a handler may have waiting at <see cref="Ring"/> (two reliable channels).</summary>
    private const int Limit = 16;

    private const ushort Ordered = 10;
    private const ushort Groups = 11;

    private static readonly ChannelTable Table = TestTables.Plumbing;

    private static byte[] Payload(int index)
    {
        byte[] payload = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static int IndexOf(ReadOnlySpan<byte> payload) => BinaryPrimitives.ReadInt32LittleEndian(payload);

    private static int Waiting(QuiclyPeer peer, ushort channel) => peer.Core.Credit.Waiting(peer.Core.ChannelIndexOf(channel));

    private static List<int> DrainAll(QuiclyPeer peer, ushort channel, int batch = 16)
    {
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[batch];
        int taken;
        while ((taken = peer.Drain(channel, buffer)) > 0)
        {
            for (int i = 0; i < taken; i++)
            {
                got.Add(IndexOf(buffer[i].Payload));
            }

            peer.Release(buffer.AsSpan(0, taken));
        }

        return got;
    }

    // ------------------------------------------------------------------ 1. the list of held-back streams overflows

    /// <summary>
    /// <see cref="ReceiveCredit.NotePended"/> writes into a ring sized for "every stream the peer may have open", on the
    /// argument that "a stream can pend only once until resumed". That bounds the streams that are <em>alive</em>, not the
    /// entries: a stream that ends while it is held back leaves its entry in the ring until the game thread's next
    /// <see cref="ReceiveCredit.Resume"/>, and its end gives the peer the stream slot back on the transport thread
    /// (<c>HandleStreamShutdownComplete</c> calls <c>CloseStream</c> there). So between two looks of the game thread the
    /// transport thread can hold back, lose and replace streams without limit. Once the ring is full the next stream that
    /// is held back is not recorded anywhere (<c>CallbackFaults</c> counts it): it is alive, its receive is paused in the
    /// transport, and nothing will ever resume it — not the credit that comes back, not <see cref="ReceiveCredit.NoteGone"/>'s
    /// flush, not a handler.
    /// </summary>
    [Fact]
    public void ReceiveCredit_A_Stream_Held_Back_After_Ended_Streams_Filled_The_List_Is_Still_Resumed()
    {
        // The list holds four streams (the peer may have four open at once).
        using ReceiveCredit credit = new(channels: 1, streams: 4, countLimit: 1, byteLimit: 1_000);
        RecordingTransport transport = new();
        credit.Enable(0);
        credit.NoteTaken(0, 10, shared: true);

        // Transport thread, between two looks of the game thread: four streams are held back and end (their sender reset
        // them), each end frees its slot, and a fifth stream — alive — takes one of the slots and is held back as well.
        for (uint i = 1; i <= 4; i++)
        {
            Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
            Assert.True(credit.NotePended(new TransportStreamId((int)i, 1), 0));
            credit.NoteGone();
        }

        TransportStreamId live = new(1, 2);
        Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
        bool listed = credit.NotePended(live, 0);

        // Game thread: the application takes the message, which gives the channel its whole credit back.
        credit.Resume(transport);
        credit.NoteReturned(0, 10, shared: true);
        credit.Resume(transport);
        credit.Resume(transport);

        Assert.True(listed && transport.Resumed.Contains(live),
            $"the stream that is alive was {(listed ? "listed" : "not listed (NotePended returned false)")} and never resumed: "
            + $"waiting {credit.Waiting(0)}, parked {credit.ParkedStreams}, HasWork {credit.HasWork}, resumed [{string.Join(", ", transport.Resumed)}]");
    }

    /// <summary>
    /// The same over the session: a peer whose group streams are held back for credit resets them and opens new ones while
    /// the receiving game thread is late with its Poll (two rounds of eight fill the list of 16 that a stream limit of 9
    /// gets). The group that arrives after that is held back and forgotten; the application then drains the channel, and
    /// that group's message never arrives although the channel is empty and has its whole credit.
    /// </summary>
    [Fact]
    public void A_Group_Held_Back_After_Reset_Groups_Filled_The_List_Goes_On_When_The_Channel_Is_Drained()
    {
        using ServerHarness h = new(table: Table, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = 16;
        });
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Groups);
        int limit = server.Core.Credit.CountLimit;
        int listCapacity = SpscRing<int>.RoundUpCapacity(server.Core.PeerStreamCapacity + 2);
        int perRound = server.Core.PeerUnidirectionalStreamLimit - 1;

        // The channel is at its limit and nobody drains it.
        byte[][] messages = new byte[limit][];
        for (int i = 0; i < limit; i++)
        {
            messages[i] = Payload(i);
        }

        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, 1, messages), out _, fin: true));
        Assert.True(h.RunUntil(() => Waiting(server, Groups) == limit && GroupKit.OpenPeerGroups(server, Groups) == 0));

        // From here on the server's game thread is late: only the network (the transport thread) runs.
        ulong group = 100;
        long pends = server.Core.Credit.Pends(index);
        while (server.Core.Credit.Pends(index) - pends < listCapacity)
        {
            TransportStreamId[] round = new TransportStreamId[perRound];
            long before = server.Core.Credit.Pends(index);
            for (int i = 0; i < round.Length; i++)
            {
                Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, group++, Payload(500)), out round[i]));
            }

            AdvanceNetworkOnly(h, 20_000);
            Assert.Equal(before + round.Length, server.Core.Credit.Pends(index));
            foreach (TransportStreamId id in round)
            {
                h.Raw.Transport.AbortStream(id, 0x77, StreamAbortDirection.Send);
            }

            AdvanceNetworkOnly(h, 20_000);
            Assert.Equal(0, GroupKit.OpenPeerGroups(server, Groups));
        }

        // One more group, which its sender does not give up.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, 2, Payload(42)), out _, fin: true));
        AdvanceNetworkOnly(h, 20_000);
        Assert.Equal(1, GroupKit.OpenPeerGroups(server, Groups));
        long faults = DatagramKit.Statistics(server).CallbackFaults;

        // The game thread is back and drains the channel until it is empty, for two simulated seconds.
        List<int> got = [];
        h.RunUntil(() =>
        {
            got.AddRange(DrainAll(server, Groups));
            return got.Contains(42);
        }, 2_000_000);

        Assert.True(got.Contains(42),
            $"the group that is alive never went on: drained {got.Count} of {limit + 1} messages, {GroupKit.OpenPeerGroups(server, Groups)} group open, "
            + $"waiting {Waiting(server, Groups)}, parked {server.Core.Credit.ParkedStreams}, HasPendingWork {server.HasPendingWork}, "
            + $"CallbackFaults {faults}, state {server.Core.Credit.State(index)}");
    }

    private static void AdvanceNetworkOnly(SimFixture h, long micros)
    {
        long end = h.Network.NowMicros + micros;
        while (h.Network.NowMicros < end)
        {
            h.Network.AdvanceTo(Math.Min(end, h.Network.NowMicros + 1_000));
        }
    }

    // ------------------------------------------------------------------ 2. a handler that throws

    /// <summary>
    /// Credit that comes back marks the channel (<c>_changed</c>) and relies on the <c>ResumeCreditPended</c> at the end
    /// of the same <see cref="QuiclyPeer.Poll"/>. A handler that throws leaves Poll before that call, and the mark is not
    /// part of <see cref="QuiclyPeer.HasPendingWork"/>: the probe answers "nothing to do" while a Poll is exactly what the
    /// held-back stream needs. A host that polls while the probe is set (the documented use) does not poll again until
    /// something else gives it a reason: never on a quiet connection, and at the next ping deadline otherwise. The
    /// back-pressure of the ring does not have this hole: its streams stay in a ring that the probe reads.
    /// </summary>
    /// <param name="quiet">No pings and no heartbeat (nothing else wakes the host); otherwise the default timers.</param>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_Handler_That_Throws_Does_Not_Leave_A_Stream_Held_Back_Behind_A_Clear_Probe(bool quiet)
    {
        using SessionHarness h = quiet
            ? NewHarness()
            : new SessionHarness(table: Table, client: o => o.GroupMinInterval = TimeSpan.Zero, server: o =>
            {
                o.GroupMinInterval = TimeSpan.Zero;
                o.ReceiveRingCapacity = Ring;
            });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Groups);
        Send(h, client, Groups, 0, 40);
        Assert.Equal(Limit, Waiting(server, Groups));
        Assert.True(server.Core.Credit.ParkedStreams > 0);

        // The handler is registered over the backlog and fails on its last message.
        List<int> got = [];
        server.RegisterHandler(Groups, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            got.Add(IndexOf(payload));
            if (got.Count == Limit)
            {
                throw new InvalidOperationException("handler fault");
            }
        });
        Assert.Throws<InvalidOperationException>(() => server.Poll());
        Assert.Equal(Limit, got.Count);
        Assert.Equal(0, Waiting(server, Groups));
        int parked = server.Core.Credit.ParkedStreams;
        bool probe = server.HasPendingWork;

        // A host that polls while the probe is set, for three simulated seconds. Its sender goes on within a few
        // milliseconds when the receiver's Poll is not missing (the second assertion allows fifty).
        long start = h.Network.NowMicros;
        long end = start + 3_000_000;
        int polls = 0;
        while (got.Count < 40 && h.Network.NowMicros < end)
        {
            client.Poll();
            client.Flush();
            h.Network.AdvanceTo(h.Network.NowMicros + 1_000);
            if (server.HasPendingWork)
            {
                polls++;
                server.Poll();
            }

            server.Flush();
        }

        long waited = h.Network.NowMicros - start;
        Assert.True(got.Count == 40 && waited <= 50_000,
            $"{got.Count} of 40 messages reached the handler after {waited / 1000} ms: after the fault the probe was {probe} with {parked} stream(s) held "
            + $"back and the whole credit free; the host polled {polls} times, {server.Core.Credit.ParkedStreams} stream(s) are still held back, "
            + $"state {server.Core.Credit.State(index)}");
    }

    /// <summary>
    /// The same fault leaves the channel's state behind as well: <c>SettleCreditLimit</c> runs only after the backlog
    /// loop of <c>DispatchQueued</c>, which the exception left, and a later Poll calls <c>DispatchQueued</c> only while
    /// something is queued for a handler. So the channel has a handler, nothing queued, and the limit of a channel nobody
    /// reads — for as long as no other handled channel gets a backlog. Its streams are held back at every burst.
    /// </summary>
    [Fact]
    public void A_Handler_That_Threw_On_Its_Backlog_Still_Lifts_The_Limit_Once_The_Backlog_Is_Gone()
    {
        using SessionHarness h = NewHarness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int index = server.Core.ChannelIndexOf(Groups);
        Send(h, client, Groups, 0, 40);
        Assert.Equal(Limit, Waiting(server, Groups));

        List<int> got = [];
        server.RegisterHandler(Groups, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            got.Add(IndexOf(payload));
            if (got.Count == Limit)
            {
                throw new InvalidOperationException("handler fault");
            }
        });
        Assert.Throws<InvalidOperationException>(() => server.Poll());

        // The host polls every frame from here on; everything arrives.
        Assert.True(h.RunUntil(() => got.Count == 40, 2_000_000), $"{got.Count} of 40 messages arrived");
        long holds = server.Core.Credit.Pends(index);
        CreditState state = server.Core.Credit.State(index);

        // A late frame: more than the share of an unread channel arrives between two Polls.
        for (int i = 0; i < 200; i++)
        {
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(Groups), Payload(1_000 + i)).Status);
        }

        for (int step = 0; step < 8; step++)
        {
            client.Poll();
            client.Flush();
            h.Network.AdvanceTo(h.Network.NowMicros + 1_000);
        }

        Assert.True(h.RunUntil(() => got.Count == 240, 2_000_000), $"{got.Count} of 240 messages arrived");
        Assert.True(state == CreditState.Handled && server.Core.Credit.Pends(index) == holds,
            $"the channel has a handler and no backlog, and its state is {state} (now {server.Core.Credit.State(index)}): "
            + $"its streams were held back {server.Core.Credit.Pends(index) - holds} more times");
    }

    // ------------------------------------------------------------------ guards: the store-fence-load pair, step by step

    /// <summary>
    /// Guard. Every order in which one side's store can fall between the other side's check and its store, driven on one
    /// thread: in each of them either the game thread finds the stream, or the transport thread finds the credit and asks
    /// for another look.
    /// </summary>
    [Fact]
    public void Guard_ReceiveCredit_Credit_That_Comes_Back_While_A_Stream_Is_Being_Held_Back_Is_Seen_By_One_Side()
    {
        TransportStreamId a = new(1, 1);
        TransportStreamId b = new(2, 1);

        // (1) The message is taken and the game thread looks (nothing listed yet) before the stream is listed.
        {
            using ReceiveCredit credit = NewCredit(out RecordingTransport transport);
            credit.NoteTaken(0, 10, shared: true);
            Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
            credit.NoteReturned(0, 10, shared: true);
            credit.Resume(transport);
            Assert.True(credit.NotePended(a, 0));
            Assert.True(credit.HasWork);
            credit.Resume(transport);
            Assert.Equal([a], transport.Resumed);
            Assert.False(credit.HasWork);
        }

        // (2) The stream is listed, then the message is taken.
        {
            using ReceiveCredit credit = NewCredit(out RecordingTransport transport);
            credit.NoteTaken(0, 10, shared: true);
            Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
            Assert.True(credit.NotePended(a, 0));
            credit.NoteReturned(0, 10, shared: true);
            credit.Resume(transport);
            Assert.Equal([a], transport.Resumed);
        }

        // (3) The stream is listed and collected by a look that found nothing to do; the message is taken afterwards.
        {
            using ReceiveCredit credit = NewCredit(out RecordingTransport transport);
            credit.NoteTaken(0, 10, shared: true);
            Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
            Assert.True(credit.NotePended(a, 0));
            credit.Resume(transport);
            Assert.False(credit.HasWork);
            credit.NoteReturned(0, 10, shared: true);

            // Credit that came back is work until the game thread has looked: the Drain or Poll that took the message
            // looks before it returns, and a Poll that a throwing handler cut short leaves the probe set (finding 2).
            Assert.True(credit.HasWork);
            credit.Resume(transport);
            Assert.Equal([a], transport.Resumed);
            Assert.False(credit.HasWork);
        }

        // (4) A handler is registered and looks before the stream is listed: no limit is left, the transport thread asks.
        {
            using ReceiveCredit credit = NewCredit(out RecordingTransport transport);
            credit.NoteTaken(0, 10, shared: true);
            Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
            credit.SetState(0, CreditState.Handled);
            credit.Resume(transport);
            Assert.True(credit.NotePended(a, 0));
            Assert.True(credit.HasWork);
            credit.Resume(transport);
            Assert.Equal([a], transport.Resumed);
        }

        // (5) The handler comes, takes everything (which marks nothing: the channel is handled) and goes again, all
        // before the stream is listed. The limit is back, the channel is empty, nothing is marked: only the transport
        // thread's second look at the counters saves the stream.
        {
            using ReceiveCredit credit = NewCredit(out RecordingTransport transport);
            credit.NoteTaken(0, 10, shared: true);
            Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
            credit.SetState(0, CreditState.Handled);
            credit.Resume(transport);
            credit.NoteReturned(0, 10, shared: true);
            credit.Resume(transport);
            credit.SetState(0, CreditState.Unread);
            Assert.False(credit.HasWork);
            Assert.True(credit.NotePended(a, 0));
            Assert.True(credit.HasWork);
            credit.Resume(transport);
            Assert.Equal([a], transport.Resumed);
        }

        // (6) The same, but the handler goes before it has taken the message: the channel is full and limited again, the
        // stream waits, and the Drain that takes the message resumes it.
        {
            using ReceiveCredit credit = NewCredit(out RecordingTransport transport);
            credit.NoteTaken(0, 10, shared: true);
            Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
            credit.SetState(0, CreditState.Handled);
            credit.Resume(transport);
            credit.SetState(0, CreditState.Unread);
            Assert.True(credit.NotePended(a, 0));
            credit.Resume(transport);
            Assert.Empty(transport.Resumed);
            Assert.False(credit.HasWork);
            credit.NoteReturned(0, 10, shared: true);
            credit.Resume(transport);
            Assert.Equal([a], transport.Resumed);
        }

        // (7) A half-received message is given up on the transport thread while the look is under way: the look that
        // read the old count keeps the stream, and the transport thread's request brings the next one.
        {
            using ReceiveCredit credit = NewCredit(out RecordingTransport transport);
            credit.NoteTaken(0, 10, shared: true);
            Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
            Assert.True(credit.NotePended(b, 0));
            credit.Resume(transport);
            Assert.Equal(1, credit.ParkedStreams);
            credit.Untake(0, 10, shared: true);
            Assert.True(credit.HasWork);
            credit.Resume(transport);
            Assert.Equal([b], transport.Resumed);
        }

        // (8) The limit is lifted for a channel the application drains (the wider count is stored before the wider byte
        // limit) while the stream is being listed: the channel was blocked by bytes alone.
        {
            using ReceiveCredit credit = new(channels: 1, streams: 8, countLimit: 4, byteLimit: 100, drainedCountLimit: 64);
            RecordingTransport transport = new();
            credit.Enable(0);
            credit.NoteTaken(0, 500, shared: true);
            Assert.False(credit.TryTake(0, 0, 0) != CreditTake.Blocked);
            credit.SetState(0, CreditState.Drained);
            credit.Resume(transport);
            Assert.True(credit.NotePended(a, 0));
            Assert.True(credit.HasWork);
            credit.Resume(transport);
            Assert.Equal([a], transport.Resumed);
        }
    }

    private static ReceiveCredit NewCredit(out RecordingTransport transport)
    {
        ReceiveCredit credit = new(channels: 1, streams: 8, countLimit: 1, byteLimit: 1_000);
        credit.Enable(0);
        transport = new RecordingTransport();
        return credit;
    }

    // ------------------------------------------------------------------ guards: two real threads

    /// <summary>
    /// Guard (x64 only proves x64). A transport thread and a game thread drive one <see cref="ReceiveCredit"/> the way the
    /// session does: twelve streams on three channels take messages, are held back, are resumed; streams end in the
    /// middle of a message (<see cref="ReceiveCredit.Untake"/>) and while they are held back
    /// (<see cref="ReceiveCredit.NoteGone"/>); the game thread hands messages on in batches, changes each channel's
    /// state at random and looks only where the session looks. A stream that makes no progress for three seconds while
    /// its channel is being read is a lost wake-up.
    /// </summary>
    /// <param name="onlyOnWork">
    /// The game thread calls <see cref="ReceiveCredit.Resume"/> only after it handed messages on, after it lifted a limit,
    /// or while <see cref="ReceiveCredit.HasWork"/> is set (a host that polls on the probe); otherwise on every turn (a
    /// host that polls every frame).
    /// </param>
    /// <param name="seed">Seed of both threads' choices.</param>
    [Theory]
    [InlineData(false, 1)]
    [InlineData(true, 2)]
    [InlineData(true, 3)]
    public void Guard_ReceiveCredit_Two_Threads_Leave_No_Stream_Held_Back(bool onlyOnWork, int seed)
    {
        const int channels = 3;
        const int streams = 12;
        const long tail = 200;
        using ReceiveCredit credit = new(channels, streams: 256, countLimit: 4, byteLimit: 1_000, drainedCountLimit: 32);
        for (int channel = 0; channel < channels; channel++)
        {
            credit.Enable(channel);
        }

        ThreadSafeTransport transport = new();
        using SpscRing<ModelMessage> ring = new(1 << 10);
        long[] delivered = new long[streams];
        bool[] paused = new bool[streams];
        long[] left = new long[streams];
        Array.Fill(left, long.MaxValue);
        int finishing = 0;
        int abort = 0;
        int transportDone = 0;
        int faults = 0;
        long untakes = 0;
        long gone = 0;
        long returned = 0;
        int grown = 0;
        long probes = 0;
        Exception? failure = null;

        // HasWork is any thread's: a host thread probes while the other two run.
        Thread prober = new(() =>
        {
            try
            {
                while (Volatile.Read(ref abort) == 0 && Volatile.Read(ref transportDone) == 0)
                {
                    _ = credit.HasWork;
                    probes++;
                    Thread.SpinWait(20);
                }
            }
            catch (Exception exception)
            {
                failure = exception;
            }
        })
        { IsBackground = true, Name = "model host" };

        Thread transportThread = new(() =>
        {
            try
            {
                Random random = new(seed);
                TransportStreamId[] ids = new TransportStreamId[streams];
                uint generation = 0;
                for (int s = 0; s < streams; s++)
                {
                    ids[s] = new TransportStreamId(s, ++generation);
                }

                bool tailSet = false;
                int capacity = 256;
                while (Volatile.Read(ref abort) == 0)
                {
                    if (!tailSet && Volatile.Read(ref finishing) != 0)
                    {
                        Array.Fill(left, tail);
                        tailSet = true;
                    }

                    if (!tailSet && capacity < 1 << 14 && random.Next(1 << 17) == 0)
                    {
                        // The list the transport thread writes is replaced while streams are in it and the game thread
                        // may be reading it (no in-tree transport does this; the class says nothing rests on that).
                        credit.SetStreamCapacity(capacity *= 2);
                        Volatile.Write(ref grown, grown + 1);
                    }

                    while (transport.Resumed.TryDequeue(out TransportStreamId resumed))
                    {
                        // A stale id (the stream ended) is ignored, as a transport ignores it.
                        if (ids[resumed.Slot] == resumed)
                        {
                            Volatile.Write(ref paused[resumed.Slot], false);
                        }
                    }

                    bool done = tailSet;
                    for (int s = 0; s < streams; s++)
                    {
                        if (left[s] == 0)
                        {
                            continue;
                        }

                        done = false;
                        int channel = s % channels;
                        if (paused[s])
                        {
                            if (!tailSet && random.Next(1 << 14) == 0)
                            {
                                // The stream ends while it is held back; another one takes its slot.
                                credit.NoteGone();
                                ids[s] = new TransportStreamId(s, ++generation);
                                Volatile.Write(ref paused[s], false);
                                gone++;
                            }

                            continue;
                        }

                        CreditTake take = credit.TryTake(channel, 0, 0);
                        if (take == CreditTake.Blocked)
                        {
                            if (!credit.NotePended(ids[s], channel))
                            {
                                faults++;
                            }

                            Volatile.Write(ref paused[s], true);
                            continue;
                        }

                        if (!ring.HasRoomFor(0))
                        {
                            continue; // the ring's back-pressure: retried
                        }

                        int bytes = random.Next(0, 600);
                        bool shared = take == CreditTake.Limited;
                        credit.NoteTaken(channel, bytes, shared);
                        if (!tailSet && random.Next(1 << 10) == 0)
                        {
                            // The stream ends in the middle of the message; another one takes its slot.
                            credit.Untake(channel, bytes, shared);
                            ids[s] = new TransportStreamId(s, ++generation);
                            untakes++;
                            continue;
                        }

                        ModelMessage message = new() { Channel = channel, Bytes = bytes, Shared = shared };
                        ring.TryEnqueue(in message);
                        left[s]--;
                        Volatile.Write(ref delivered[s], delivered[s] + 1);
                    }

                    if (done)
                    {
                        break;
                    }
                }
            }
            catch (Exception exception)
            {
                failure = exception;
            }
            finally
            {
                Volatile.Write(ref transportDone, 1);
            }
        })
        { IsBackground = true, Name = "model transport" };

        Thread gameThread = new(() =>
        {
            try
            {
                Random random = new(seed * 7919);
                int turn = 0;
                while (Volatile.Read(ref abort) == 0 && (Volatile.Read(ref transportDone) == 0 || !ring.IsEmpty))
                {
                    turn++;
                    bool look = !onlyOnWork;
                    if ((turn & 0xFF) == 0)
                    {
                        // RegisterHandler and Drain look after they changed the state; UnregisterHandler and the demotion
                        // of a channel that is no longer drained only narrow the limit and do not.
                        CreditState state = (CreditState)random.Next(3);
                        credit.SetState(random.Next(channels), state);
                        look |= state != CreditState.Unread;
                    }

                    int batch = random.Next(0, 12);
                    for (int i = 0; i < batch && ring.TryDequeue(out ModelMessage message); i++)
                    {
                        credit.NoteReturned(message.Channel, message.Bytes, message.Shared);
                        returned++;
                        look = true;
                    }

                    if (look || credit.HasWork)
                    {
                        credit.Resume(transport);
                    }
                    else if ((turn & 0xF) == 0)
                    {
                        Thread.Yield();
                    }
                }
            }
            catch (Exception exception)
            {
                failure = exception;
            }
        })
        { IsBackground = true, Name = "model game" };

        transportThread.Start();
        gameThread.Start();
        prober.Start();
        // The run lasts until the model has done what the final assertion asks of it (and at least 1.5 s), not for a fixed
        // time: how much two threads get done in a second is up to the machine. A run of the whole test project on a busy
        // machine once reached only 9 000 of the 10 000 messages and failed on that, although nothing was wrong.
        string? stall = WatchForAStall(delivered, left, runMillis: 1_500, stallMillis: 10_000, () => Volatile.Read(ref transportDone) != 0, () => Volatile.Write(ref finishing, 1),
            enough: () =>
            {
                long sum = 0;
                for (int s = 0; s < delivered.Length; s++)
                {
                    sum += Volatile.Read(ref delivered[s]);
                }

                return sum >= EnoughMessages && Volatile.Read(ref untakes) > 0 && Volatile.Read(ref gone) > 0 && Volatile.Read(ref grown) > 0 && Volatile.Read(ref probes) > 0;
            },
            maxMillis: 120_000);
        if (stall is not null)
        {
            StringBuilder report = new(stall);
            report.Append($"; HasWork {credit.HasWork}, parked {credit.ParkedStreams}, resumes queued {transport.Resumed.Count}, ring {ring.Count}");
            for (int channel = 0; channel < channels; channel++)
            {
                report.Append($"; channel {channel}: {credit.State(channel)}, waiting {credit.Waiting(channel)} ({credit.WaitingBytes(channel)} bytes)");
            }

            report.Append("; paused:");
            for (int s = 0; s < streams; s++)
            {
                report.Append(paused[s] ? $" {s}" : string.Empty);
            }

            Volatile.Write(ref abort, 1);
            transportThread.Join(5_000);
            gameThread.Join(5_000);
            prober.Join(5_000);
            Assert.Fail(report.ToString());
        }

        Assert.True(transportThread.Join(10_000), "the transport thread did not finish");
        Assert.True(gameThread.Join(10_000), "the game thread did not finish");
        Assert.True(prober.Join(10_000), "the probing thread did not finish");
        Assert.Null(failure);
        Assert.Equal(0, faults);
        for (int channel = 0; channel < channels; channel++)
        {
            Assert.Equal(0, credit.Waiting(channel));
            Assert.Equal(0, credit.WaitingBytes(channel));
        }

        long total = 0;
        foreach (long count in delivered)
        {
            total += count;
        }

        Assert.Equal(total, returned);
        Assert.True(untakes > 0 && gone > 0 && grown > 0 && probes > 0 && total >= EnoughMessages,
            $"the model did too little: {total} messages, {untakes} given up, {gone} streams ended while held back, the list grew {grown} times, {probes} probes");
    }

    /// <summary>
    /// Watches per-stream progress: runs the open-ended phase for <paramref name="runMillis"/> (and until
    /// <paramref name="enough"/> says the model did its share of work, but no longer than <paramref name="maxMillis"/>), then asks the transport
    /// thread to finish and waits for it. Returns a description when a stream with messages left made no progress for
    /// <paramref name="stallMillis"/>.
    /// </summary>
    private static string? WatchForAStall(long[] delivered, long[] left, int runMillis, int stallMillis, Func<bool> finished, Action finish, Func<bool>? enough = null, int maxMillis = int.MaxValue)
    {
        Stopwatch clock = Stopwatch.StartNew();
        long[] seen = new long[delivered.Length];
        long[] since = new long[delivered.Length];
        bool asked = false;
        while (!finished())
        {
            Thread.Sleep(20);
            long now = clock.ElapsedMilliseconds;
            if (!asked && ((now >= runMillis && (enough is null || enough())) || now >= maxMillis))
            {
                finish();
                asked = true;
            }

            for (int s = 0; s < delivered.Length; s++)
            {
                long count = Volatile.Read(ref delivered[s]);
                if (count != seen[s])
                {
                    seen[s] = count;
                    since[s] = now;
                }
                else if (Volatile.Read(ref left[s]) != 0 && now - since[s] > stallMillis)
                {
                    return $"stream {s} made no progress for {now - since[s]} ms after {count} messages ({(asked ? "finishing" : "running")})";
                }
            }
        }

        return null;
    }

    private struct ModelMessage
    {
        public int Channel;
        public int Bytes;
        public bool Shared;
    }

    // ------------------------------------------------------------------ guards: the ways an application can read

    /// <summary>
    /// Guard. A receiver that changes how it reads two reliable channels at random — Poll, Poll with a small limit, a
    /// Drain that empties the channel, a Drain that does not, a handler that comes and goes — while a sender keeps both
    /// busy, and then settles on one way of reading each. Whatever came before, every message arrives (in order on the
    /// ordered channel) and no stream stays held back: there is no sequence of calls after which a channel is stuck.
    /// </summary>
    /// <param name="seed">Seed of the receiver's choices.</param>
    /// <param name="onProbe">The receiver ends as a host that polls only while <see cref="QuiclyPeer.HasPendingWork"/> is set.</param>
    /// <param name="nestedCalls">Handlers also drain, register and unregister from inside <see cref="QuiclyPeer.Poll"/>.</param>
    [Theory]
    [InlineData(1, false, false)]
    [InlineData(2, false, false)]
    [InlineData(3, false, false)]
    [InlineData(4, false, false)]
    [InlineData(5, true, false)]
    [InlineData(6, true, false)]
    [InlineData(7, true, false)]
    [InlineData(8, true, false)]
    [InlineData(9, true, false)]
    [InlineData(10, false, false)]
    [InlineData(11, true, false)]
    [InlineData(12, false, false)]
    [InlineData(1, false, true)]
    [InlineData(2, true, true)]
    [InlineData(3, false, true)]
    [InlineData(4, true, true)]
    [InlineData(5, true, true)]
    [InlineData(6, true, true)]
    [InlineData(7, false, true)]
    [InlineData(8, true, true)]
    public void Guard_A_Receiver_That_Changes_How_It_Reads_Gets_Every_Message(int seed, bool onProbe, bool nestedCalls)
    {
        const int count = 700;
        using SessionHarness h = NewHarness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        Random random = new(seed);
        List<int> ordered = [];
        List<int> groups = [];
        bool orderedHandled = false;
        bool groupsHandled = false;
        bool settled = false;
        int nested = 0;
        int sentOrdered = 0;
        int sentGroups = 0;
        ReceivedMessage[] small = new ReceivedMessage[3];
        MessageHandler orderedHandler = null!;
        MessageHandler groupsHandler = null!;

        // Until the receiver settles, a handler now and then does what an application may do inside one: removes itself,
        // drains its own channel (what is queued follows the message it was given) or the other one, or gives the other
        // channel a handler or takes it away.
        orderedHandler = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            ordered.Add(IndexOf(payload));
            if (nestedCalls && !settled)
            {
                switch (random.Next(40))
                {
                    case 0:
                        nested++;
                        SetOrdered(false);
                        break;
                    case 1:
                        nested++;
                        DrainSome(Groups, groups);
                        break;
                    case 2:
                        nested++;
                        ordered.AddRange(DrainAll(server, Ordered));
                        break;
                    case 3:
                        nested++;
                        SetGroups(!groupsHandled);
                        break;
                    default:
                        break;
                }
            }
        };
        groupsHandler = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            groups.Add(IndexOf(payload));
            if (nestedCalls && !settled)
            {
                switch (random.Next(40))
                {
                    case 0:
                        nested++;
                        SetGroups(false);
                        break;
                    case 1:
                        nested++;
                        DrainSome(Ordered, ordered);
                        break;
                    case 2:
                        nested++;
                        groups.AddRange(DrainAll(server, Groups));
                        break;
                    case 3:
                        nested++;
                        SetOrdered(!orderedHandled);
                        break;
                    default:
                        break;
                }
            }
        };

        void SendSome()
        {
            for (int burst = random.Next(0, 14); burst > 0 && sentOrdered < count; burst--)
            {
                if (client.SendCopy(new SendHeader(Ordered), Payload(sentOrdered)).Status != SendStatus.Admitted)
                {
                    break;
                }

                sentOrdered++;
            }

            for (int burst = random.Next(0, 14); burst > 0 && sentGroups < count; burst--)
            {
                if (client.SendCopy(new SendHeader(Groups), Payload(sentGroups)).Status != SendStatus.Admitted)
                {
                    break;
                }

                sentGroups++;
            }

            client.Poll();
            client.Flush();
            h.Network.AdvanceTo(h.Network.NowMicros + 1_000);
        }

        void DrainSome(ushort channel, List<int> into)
        {
            int taken = server.Drain(channel, small.AsSpan(0, random.Next(1, small.Length + 1)));
            for (int i = 0; i < taken; i++)
            {
                into.Add(IndexOf(small[i].Payload));
            }

            server.Release(small.AsSpan(0, taken));
        }

        void SetOrdered(bool handled)
        {
            if (handled != orderedHandled)
            {
                if (handled)
                {
                    server.RegisterHandler(Ordered, orderedHandler);
                }
                else
                {
                    server.UnregisterHandler(Ordered);
                }

                orderedHandled = handled;
            }
        }

        void SetGroups(bool handled)
        {
            if (handled != groupsHandled)
            {
                if (handled)
                {
                    server.RegisterHandler(Groups, groupsHandler);
                }
                else
                {
                    server.UnregisterHandler(Groups);
                }

                groupsHandled = handled;
            }
        }

        for (int step = 0; step < 600; step++)
        {
            SendSome();
            switch (random.Next(12))
            {
                case 0:
                case 1:
                case 2:
                    server.Poll();
                    break;
                case 3:
                    server.Poll(random.Next(0, 5));
                    break;
                case 4:
                    DrainSome(Ordered, ordered);
                    break;
                case 5:
                    DrainSome(Groups, groups);
                    break;
                case 6:
                    ordered.AddRange(DrainAll(server, Ordered));
                    break;
                case 7:
                    groups.AddRange(DrainAll(server, Groups));
                    break;
                case 8:
                    SetOrdered(!orderedHandled);
                    break;
                case 9:
                    SetGroups(!groupsHandled);
                    break;
                default:
                    break; // a late frame
            }

            server.Flush();
        }

        // The receiver settles.
        settled = true;
        SetOrdered(random.Next(2) == 0);
        SetGroups(random.Next(2) == 0);
        long end = h.Network.NowMicros + 20_000_000;
        while ((ordered.Count < count || groups.Count < count) && h.Network.NowMicros < end)
        {
            SendSome();
            if (!onProbe || server.HasPendingWork)
            {
                server.Poll();
            }

            if (!orderedHandled)
            {
                ordered.AddRange(DrainAll(server, Ordered));
            }

            if (!groupsHandled)
            {
                groups.AddRange(DrainAll(server, Groups));
            }

            server.Flush();
        }

        string where = $"seed {seed}: ordered {ordered.Count}/{count} (sent {sentOrdered}, handler {orderedHandled}, state {server.Core.Credit.State(server.Core.ChannelIndexOf(Ordered))}, "
            + $"waiting {Waiting(server, Ordered)}), groups {groups.Count}/{count} (sent {sentGroups}, handler {groupsHandled}, "
            + $"state {server.Core.Credit.State(server.Core.ChannelIndexOf(Groups))}, waiting {Waiting(server, Groups)}), parked {server.Core.Credit.ParkedStreams}, "
            + $"HasPendingWork {server.HasPendingWork}, open groups {GroupKit.OpenPeerGroups(server, Groups)}";
        Assert.True(ordered.Count == count && groups.Count == count, where);
        // With calls from inside handlers only the set is checked: a Drain or a RegisterHandler from inside a handler can
        // put older messages of a handled channel behind newer ones, with or without the credit (see the last test).
        Assert.True(Enumerable.Range(0, count).SequenceEqual(nestedCalls ? ordered.Order() : ordered), "the ordered channel is out of order; " + where);
        Assert.True(Enumerable.Range(0, count).SequenceEqual(groups.Order()), "the group channel lost or repeated a message; " + where);
        Assert.Equal(0, server.Core.Credit.ParkedStreams);
        Assert.Equal(0, DatagramKit.Statistics(server).CallbackFaults);
        Assert.True((nested > 0 || !nestedCalls) && server.Core.Credit.Pends(server.Core.ChannelIndexOf(Groups)) + server.Core.Credit.Pends(server.Core.ChannelIndexOf(Ordered)) > 0,
            $"the run exercised too little: {nested} calls from inside a handler; " + where);
    }

    /// <summary>
    /// Guard (x64 only proves x64). The same receiver over the whole session with real threads: one thread advances the
    /// simulated network in real time and so runs every transport callback of both peers, one is the sender's game
    /// thread, and the test thread is the receiver's game thread. The simulator serialises transport API calls with
    /// its callbacks, but everything the session shares between its two threads — the receive ring, the credit counters,
    /// the lists of held-back streams, <c>WaitsForCredit</c> — is driven concurrently, as it is over MsQuic. A receiver
    /// that reads both channels and gets nothing for four seconds has lost a wake-up. Every run starts by leaving the group
    /// channel unread until one of its streams is held back, so that every run has a hold to lift.
    /// </summary>
    /// <param name="seed">Seed of the receiver's and the sender's choices.</param>
    /// <param name="onProbe">The receiver ends as a host that polls only while <see cref="QuiclyPeer.HasPendingWork"/> is set.</param>
    /// <param name="manyStreams">The link grants 1 024 streams by itself, so hundreds of groups can be held back at once.</param>
    [Theory]
    [InlineData(21, false, false)]
    [InlineData(22, true, false)]
    [InlineData(23, true, true)]
    [InlineData(24, false, true)]
    public void Guard_A_Receiver_On_Its_Own_Thread_Gets_Every_Message(int seed, bool onProbe, bool manyStreams)
    {
        const int count = 20_000;
        using SessionHarness h = new(link: manyStreams ? new LinkOptions { PeerUnidiStreams = 1024 } : null, table: Table, client: GroupKit.Prompt, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = Ring;
        });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int sentOrdered = 0;
        int sentGroups = 0;
        bool stop = false;
        Exception? failure = null;

        Thread network = new(() =>
        {
            try
            {
                Stopwatch clock = Stopwatch.StartNew();
                long start = h.Network.NowMicros;
                while (!Volatile.Read(ref stop))
                {
                    long target = start + (clock.ElapsedTicks * 1_000_000 / Stopwatch.Frequency);
                    h.Network.AdvanceTo(Math.Max(target, h.Network.NowMicros));
                    Thread.SpinWait(30);
                }
            }
            catch (Exception exception)
            {
                failure = exception;
            }
        })
        { IsBackground = true, Name = "network (transport thread)" };

        Thread sender = new(() =>
        {
            try
            {
                Random random = new(seed + 1_000);
                while (!Volatile.Read(ref stop))
                {
                    for (int burst = random.Next(0, 14); burst > 0 && sentOrdered < count; burst--)
                    {
                        if (client.SendCopy(new SendHeader(Ordered), Payload(sentOrdered)).Status != SendStatus.Admitted)
                        {
                            break;
                        }

                        Volatile.Write(ref sentOrdered, sentOrdered + 1);
                    }

                    for (int burst = random.Next(0, 14); burst > 0 && sentGroups < count; burst--)
                    {
                        if (client.SendCopy(new SendHeader(Groups), Payload(sentGroups)).Status != SendStatus.Admitted)
                        {
                            break;
                        }

                        Volatile.Write(ref sentGroups, sentGroups + 1);
                    }

                    client.Poll();
                    client.Flush();
                    Thread.SpinWait(random.Next(1, 400));
                }
            }
            catch (Exception exception)
            {
                failure = exception;
            }
        })
        { IsBackground = true, Name = "sender game thread" };

        Random choice = new(seed);
        List<int> ordered = [];
        List<int> groups = [];
        MessageHandler orderedHandler = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => ordered.Add(IndexOf(payload));
        MessageHandler groupsHandler = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => groups.Add(IndexOf(payload));
        bool orderedHandled = false;
        bool groupsHandled = false;
        ReceivedMessage[] small = new ReceivedMessage[3];

        void DrainSome(ushort channel, List<int> into)
        {
            int taken = server.Drain(channel, small.AsSpan(0, choice.Next(1, small.Length + 1)));
            for (int i = 0; i < taken; i++)
            {
                into.Add(IndexOf(small[i].Payload));
            }

            server.Release(small.AsSpan(0, taken));
        }

        void SetHandler(ushort channel, MessageHandler handler, ref bool handled, bool wanted)
        {
            if (wanted != handled)
            {
                if (wanted)
                {
                    server.RegisterHandler(channel, handler);
                }
                else
                {
                    server.UnregisterHandler(channel);
                }

                handled = wanted;
            }
        }

        string? stall = null;
        StringBuilder gaps = new();
        bool gapNoted = false;
        network.Start();
        sender.Start();
        try
        {
            // First a hold the run is sure to have. Whether the choices below leave the group channel unread long enough for
            // its streams to be held back depends on how the scheduler interleaves the three threads, and some runs had none
            // (seeds 22 and 24 on four busy cores). So the group channel is left unread here, without a handler or a Drain,
            // while the ordered channel is read, until the transport thread has held a group stream back for credit; the
            // choices below then lift that hold, with the threads running as before.
            Stopwatch holding = Stopwatch.StartNew();
            while (server.Core.Credit.Pends(server.Core.ChannelIndexOf(Groups)) == 0 && failure is null)
            {
                if (holding.ElapsedMilliseconds > 10_000)
                {
                    stall = $"the group channel was left unread for {holding.ElapsedMilliseconds} ms and no group stream was held back for credit";
                    break;
                }

                server.Poll();
                ordered.AddRange(DrainAll(server, Ordered));
                server.Flush();
            }

            TestContext.Current.TestOutputHelper?.WriteLine(
                $"seed {seed}: a group stream was held back after {holding.ElapsedMilliseconds} ms, with {groups.Count} group and {ordered.Count} ordered messages read");
            Stopwatch wall = Stopwatch.StartNew();
            long lastProgress = 0;
            int lastTotal = -1;
            bool settled = false;
            while (stall is null && (ordered.Count < count || groups.Count < count))
            {
                if (failure is not null)
                {
                    break;
                }

                if (!settled && (wall.ElapsedMilliseconds > 1_200 || ordered.Count + groups.Count > count + (count / 2)))
                {
                    SetHandler(Ordered, orderedHandler, ref orderedHandled, choice.Next(2) == 0);
                    SetHandler(Groups, groupsHandler, ref groupsHandled, choice.Next(2) == 0);
                    settled = true;
                }

                if (!settled)
                {
                    switch (choice.Next(12))
                    {
                        case 0:
                        case 1:
                        case 2:
                            server.Poll();
                            break;
                        case 3:
                            server.Poll(choice.Next(0, 5));
                            break;
                        case 4:
                            DrainSome(Ordered, ordered);
                            break;
                        case 5:
                            DrainSome(Groups, groups);
                            break;
                        case 6:
                            ordered.AddRange(DrainAll(server, Ordered));
                            break;
                        case 7:
                            groups.AddRange(DrainAll(server, Groups));
                            break;
                        case 8:
                            SetHandler(Ordered, orderedHandler, ref orderedHandled, !orderedHandled);
                            break;
                        case 9:
                            SetHandler(Groups, groupsHandler, ref groupsHandled, !groupsHandled);
                            break;
                        default:
                            Thread.SpinWait(choice.Next(1, 20_000)); // a late frame
                            break;
                    }
                }
                else
                {
                    if (!onProbe || server.HasPendingWork)
                    {
                        server.Poll();
                    }

                    if (!orderedHandled)
                    {
                        ordered.AddRange(DrainAll(server, Ordered));
                    }

                    if (!groupsHandled)
                    {
                        groups.AddRange(DrainAll(server, Groups));
                    }
                }

                server.Flush();
                int total = ordered.Count + groups.Count + Volatile.Read(ref sentOrdered) + Volatile.Read(ref sentGroups);
                long now = wall.ElapsedMilliseconds;
                if (total != lastTotal)
                {
                    if (gapNoted)
                    {
                        gaps.Append($" => went on after {now - lastProgress} ms");
                        gapNoted = false;
                    }

                    lastTotal = total;
                    lastProgress = now;
                }
                else if (!gapNoted && now - lastProgress > 300)
                {
                    gapNoted = true;
                    gaps.Append($" | GAP at {now} ms settled {settled} got {ordered.Count}/{groups.Count} sent {sentOrdered}/{sentGroups} handlers {orderedHandled}/{groupsHandled} "
                        + $"states {server.Core.Credit.State(server.Core.ChannelIndexOf(Ordered))}/{server.Core.Credit.State(server.Core.ChannelIndexOf(Groups))} "
                        + $"waiting {Waiting(server, Ordered)}/{Waiting(server, Groups)} parked {server.Core.Credit.ParkedStreams} HasWork {server.Core.Credit.HasWork} "
                        + $"HasPendingWork {server.HasPendingWork} open {GroupKit.OpenPeerGroups(server, Groups)} ring {server.Core.ReceiveRing.Count} "
                        + $"pended {server.Core.PendedStreams.Count} simulated {h.Network.NowMicros} us client groups {GroupKit.Groups(client, Groups)}");
                }
                else if (settled && now - lastProgress > 4_000)
                {
                    stall = $"nothing was sent or received for {now - lastProgress} ms";
                    break;
                }
            }
        }
        finally
        {
            Volatile.Write(ref stop, true);
            network.Join(10_000);
            sender.Join(10_000);
        }

        Assert.Null(failure);
        string where = $"seed {seed}: ordered {ordered.Count}/{count} (sent {sentOrdered}, handler {orderedHandled}, state {server.Core.Credit.State(server.Core.ChannelIndexOf(Ordered))}, "
            + $"waiting {Waiting(server, Ordered)}), groups {groups.Count}/{count} (sent {sentGroups}, handler {groupsHandled}, "
            + $"state {server.Core.Credit.State(server.Core.ChannelIndexOf(Groups))}, waiting {Waiting(server, Groups)}), parked {server.Core.Credit.ParkedStreams}, "
            + $"HasWork {server.Core.Credit.HasWork}, HasPendingWork {server.HasPendingWork}, open groups {GroupKit.OpenPeerGroups(server, Groups)}, "
            + $"stream capacity {server.Core.PeerStreamCapacity}, holds {server.Core.Credit.Pends(server.Core.ChannelIndexOf(Groups))} + {server.Core.Credit.Pends(server.Core.ChannelIndexOf(Ordered))}, "
            + $"ring {server.Core.ReceiveRing.Count}, state {server.State}/{client.State}";
        Assert.True(stall is null, stall + "; " + where + gaps);
        Assert.True(Enumerable.Range(0, count).SequenceEqual(ordered), "the ordered channel lost, repeated or reordered a message; " + where);
        Assert.True(Enumerable.Range(0, count).SequenceEqual(groups.Order()), "the group channel lost or repeated a message; " + where);
        Assert.Equal(0, DatagramKit.Statistics(server).CallbackFaults);
        Assert.True(server.Core.Credit.Pends(server.Core.ChannelIndexOf(Groups)) > 0, "no group stream was ever held back for credit; " + where);
    }

    // ------------------------------------------------------------------ outside the lens

    /// <summary>
    /// Not the credit, and not threading: met while the guard above ran with calls from inside handlers, and written down
    /// because it breaks the one promise of a ReliableOrdered channel. <c>Drain(X)</c> moves the messages of a handled
    /// channel it meets in the receive ring into that channel's queue, for the next Poll. Called from inside a handler,
    /// that next Poll is not the one that is running: the running Poll goes on taking from the ring and dispatches the
    /// channel's <em>newer</em> messages at once, and the older ones it had queued follow a Poll later. The code involved
    /// (<c>Route</c>, the ring loop of <c>Drain</c>, <c>TryQueue</c>) is the same on the branch this one started from, and the
    /// same test gives the same [0, 2, 1] there (group-on-drops, cdf7504).
    /// </summary>
    [Fact]
    public void Outside_The_Lens_A_Drain_From_Inside_A_Handler_Keeps_An_Ordered_Channel_In_Order()
    {
        using SessionHarness h = NewHarness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<int> ordered = [];
        List<int> groups = [];
        ReceivedMessage[] one = new ReceivedMessage[1];
        bool first = true;
        server.RegisterHandler(Ordered, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            ordered.Add(IndexOf(payload));
            if (first)
            {
                // The handler of the ordered channel reads the group channel the batch way, one message at a time.
                first = false;
                int taken = server.Drain(Groups, one);
                for (int i = 0; i < taken; i++)
                {
                    groups.Add(IndexOf(one[i].Payload));
                }

                server.Release(one.AsSpan(0, taken));
            }
        });

        // The receiver is late: its ring holds ordered 0, ordered 1, group 100, ordered 2, in that order.
        void SendToTheLateReceiver(ushort channel, params int[] values)
        {
            foreach (int value in values)
            {
                Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(channel), Payload(value)).Status);
            }

            for (int step = 0; step < 4; step++)
            {
                client.Poll();
                client.Flush();
                h.Network.AdvanceTo(h.Network.NowMicros + 1_000);
            }
        }

        SendToTheLateReceiver(Ordered, 0, 1);
        SendToTheLateReceiver(Groups, 100);
        SendToTheLateReceiver(Ordered, 2);
        Assert.Equal(4, server.Core.ReceiveRing.Count);

        Assert.True(h.RunUntil(() => ordered.Count == 3, 1_000_000), $"{ordered.Count} of 3 ordered messages arrived");
        Assert.Equal([100], groups);
        Assert.True(ordered.SequenceEqual([0, 1, 2]), $"the ordered channel's handler was given [{string.Join(", ", ordered)}]");
    }

    // ------------------------------------------------------------------ kit

    private static SessionHarness NewHarness() =>
        new(table: Table, client: GroupKit.Prompt, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = Ring;
        });

    /// <summary>Sends numbered messages in batches of sixteen with both ends pumped in between.</summary>
    private static void Send(SessionHarness h, QuiclyPeer sender, ushort channel, int first, int count)
    {
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(SendStatus.Admitted, sender.SendCopy(new SendHeader(channel), Payload(first + i)).Status);
            if ((i & 15) == 15)
            {
                h.Run(2_000);
            }
        }

        h.Run(2_000);
    }

    /// <summary>An <see cref="ITransport"/> that records the streams it is asked to resume (one thread).</summary>
    private sealed unsafe class RecordingTransport : ITransport
    {
        public List<TransportStreamId> Resumed { get; } = [];

        public TransportCapabilities Capabilities => default;

        public TransportState State => default;

        public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed) => Resumed.Add(id);

        public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags) => throw new NotSupportedException();

        public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id) => throw new NotSupportedException();

        public TransportStatus StartStream(TransportStreamId id) => throw new NotSupportedException();

        public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags) => throw new NotSupportedException();

        public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => throw new NotSupportedException();

        public void SetStreamPriority(TransportStreamId id, ushort priority) => throw new NotSupportedException();

        public long GetQuicStreamId(TransportStreamId id) => throw new NotSupportedException();

        public void CloseStream(TransportStreamId id) => throw new NotSupportedException();

        public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional) => throw new NotSupportedException();

        public void Close(ulong errorCode, ReadOnlySpan<byte> reason) => throw new NotSupportedException();

        public void GetStatistics(out TransportStatistics statistics) => throw new NotSupportedException();

        public void Dispose()
        {
        }
    }

    /// <summary>An <see cref="ITransport"/> whose resumes cross to the model's transport thread.</summary>
    private sealed unsafe class ThreadSafeTransport : ITransport
    {
        public ConcurrentQueue<TransportStreamId> Resumed { get; } = new();

        public TransportCapabilities Capabilities => default;

        public TransportState State => default;

        public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed) => Resumed.Enqueue(id);

        public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags) => throw new NotSupportedException();

        public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id) => throw new NotSupportedException();

        public TransportStatus StartStream(TransportStreamId id) => throw new NotSupportedException();

        public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags) => throw new NotSupportedException();

        public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => throw new NotSupportedException();

        public void SetStreamPriority(TransportStreamId id, ushort priority) => throw new NotSupportedException();

        public long GetQuicStreamId(TransportStreamId id) => throw new NotSupportedException();

        public void CloseStream(TransportStreamId id) => throw new NotSupportedException();

        public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional) => throw new NotSupportedException();

        public void Close(ulong errorCode, ReadOnlySpan<byte> reason) => throw new NotSupportedException();

        public void GetStatistics(out TransportStatistics statistics) => throw new NotSupportedException();

        public void Dispose()
        {
        }
    }
}
