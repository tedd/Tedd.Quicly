using System.Reflection;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// <see cref="PeerOptions.WorkSignal"/> and <see cref="QuiclyPeer.HasPendingWork"/> (docs/design/session-layer.md §4.7):
/// the signal is an edge raised once per <see cref="QuiclyPeer.Poll"/>, never for an idle peer, allocation-free, and a host
/// exception is recorded instead of reaching the transport.
/// </summary>
public class WorkSignalTests
{
    /// <summary>A keyed channel with <c>CoalesceOnReceive</c>, whose messages bypass the receive ring for a mailbox.</summary>
    private static readonly ChannelTable CoalescingTable = ChannelTable.Create()
        .Add(2, "fx", ChannelMode.UnreliableUnordered)
        .Add(3, "state", ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.CoalesceOnReceive = true;
            o.MaxKeys = 64;
        })
        .Build();

    /// <summary>A pair with quiet options and a work signal on each end (the real engines, so every publication path runs).</summary>
    private static SessionHarness NewPair(RecordingWorkSignal clientSignal, RecordingWorkSignal serverSignal, ChannelTable? table = null) =>
        new(table: table,
            client: o =>
            {
                QuietOptions.Apply(o);
                o.WorkSignal = clientSignal;
            },
            server: o =>
            {
                QuietOptions.Apply(o);
                o.WorkSignal = serverSignal;
            });

    [Fact]
    public void A_Received_Message_Signals_The_Receiver_Once_Per_Poll()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal);
        h.Client.Poll();
        h.Server!.Poll();
        clientSignal.Take();
        serverSignal.Take();

        h.Client.SendCopy(new SendHeader(2), [1, 2, 3]);
        h.Client.Flush();
        h.Network.Advance(1_000);
        // One publication to the receive ring; the sender's own datagram completion is work as well.
        Assert.Equal(1, serverSignal.Calls);
        Assert.Same(h.Server, serverSignal.Last);
        Assert.True(clientSignal.Calls >= 1);
        Assert.True(h.Server.HasPendingWork);

        // Set-once: a burst before the host polls costs no further calls.
        for (int i = 0; i < 20; i++)
        {
            h.Client.SendCopy(new SendHeader(2), [(byte)i]);
        }

        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);

        // Poll re-arms the edge.
        h.Server.Poll();
        h.Client.SendCopy(new SendHeader(2), [9]);
        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(2, serverSignal.Calls);
    }

    [Fact]
    public void An_Idle_Peer_Never_Signals_And_Has_No_Pending_Work()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal);
        // Let the single ping of the fast lock and its pong settle, then look at a really idle link.
        h.Run(200_000, 10_000);
        clientSignal.Take();
        serverSignal.Take();

        for (int i = 0; i < 50; i++)
        {
            h.Network.Advance(10_000);
            h.Client.Poll();
            h.Client.Flush();
            h.Server!.Poll();
            h.Server.Flush();
        }

        Assert.Equal(0, clientSignal.Calls);
        Assert.Equal(0, serverSignal.Calls);
        Assert.False(h.Client.HasPendingWork);
        Assert.False(h.Server!.HasPendingWork);
    }

    [Fact]
    public void HasPendingWork_Reports_A_Due_Timer_And_Is_False_Once_Closed()
    {
        using SessionHarness h = new();
        h.Client.Poll();
        h.Client.Flush();
        Assert.False(h.Client.HasPendingWork);

        // The ping schedule is the peer's own timer: once it is due, Poll has work even with a silent network.
        long due = h.Client.NextPollDeadlineMicros;
        Assert.True(due < long.MaxValue);
        h.Network.AdvanceTo(due + 1);
        Assert.True(h.Client.HasPendingWork);

        h.Client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.False(h.Client.HasPendingWork);
    }

    [Fact]
    public void Work_Created_By_Close_And_By_CompleteAdmission_Signals_The_Host()
    {
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = new(connect: false, server: o =>
        {
            QuietOptions.Apply(o);
            o.WorkSignal = serverSignal;
        });
        h.Admission.Handler = static (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Pending;
        Assert.True(h.RunUntil(() => h.Admission.Calls == 1));
        h.Server!.Poll();
        serverSignal.Take();

        // A direct application call, on the game thread, with nothing arriving from the network.
        h.Server.CompleteAdmission(AdmissionResult.Accept());
        Assert.Equal(1, serverSignal.Calls);
        Assert.True(h.RunUntilConnected());
        h.Server.Poll();
        serverSignal.Take();

        h.Server.Close(new CloseReason(QuiclyErrorCode.NoError, "bye"));
        Assert.Equal(1, serverSignal.Calls);
    }

    [Fact]
    public void A_Send_From_Another_Thread_Signals_The_Game_Thread()
    {
        RecordingWorkSignal clientSignal = new();
        using SessionHarness h = new(client: o =>
        {
            QuietOptions.Apply(o);
            o.ThreadSafeSend = true;
            o.WorkSignal = clientSignal;
        }, server: QuietOptions.Apply);
        h.Client.Poll();
        h.Client.Flush();
        clientSignal.Take();

        Thread producer = new(() => h.Client.SendCopy(new SendHeader(2), [7, 7, 7]));
        producer.Start();
        producer.Join();
        Assert.Equal(1, clientSignal.Calls);
        Assert.True(h.Client.HasPendingWork);
    }

    [Fact]
    public void A_Coalescing_Mailbox_Signals_And_Shows_Up_In_HasPendingWork_Once_It_Has_A_Handler()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal, CoalescingTable);
        h.Server!.Poll();
        serverSignal.Take();

        // A coalescing channel bypasses the receive ring: the mailbox raises the signal instead (ADR 0008 §6). Without a
        // handler the value waits for Drain, which no Poll can do: it is not work for the probe, or a host that polls
        // while there is work would poll for good.
        h.Client.SendCopy(new SendHeader(3, 42), [1, 2]);
        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);
        Assert.False(h.Server.HasPendingWork);
        Assert.False(h.Server.HasPendingPollWork);

        // With a handler it is work for the next Poll, without a second arrival.
        int received = 0;
        h.Server.RegisterHandler(3, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        Assert.True(h.Server.HasPendingWork);
        h.Server.Poll();
        Assert.Equal(1, received);
        Assert.False(h.Server.HasPendingWork);
    }

    [Fact]
    public void A_Throwing_Work_Signal_Is_Recorded_And_Closes_The_Connection()
    {
        RecordingWorkSignal serverSignal = new() { Throw = true };
        using SessionHarness h = new(connect: false, server: o => o.WorkSignal = serverSignal);
        Assert.True(h.RunUntilClosed());
        // The first publication called the signal; the close that fault triggered publishes more work, and every Poll re-arms
        // the edge, so a host whose signal keeps throwing keeps being told about it.
        Assert.True(serverSignal.Calls >= 1);
        Assert.NotNull(h.Server!.LastCallbackFault);
        h.Server.GetStatistics(out PeerStatistics statistics);
        Assert.True(statistics.CallbackFaults >= 1);
        Assert.Equal(QuiclyErrorCode.InternalError, h.Server.CloseReason.Code);
    }

    [Fact]
    public void The_Work_Signal_Does_Not_Allocate_In_Steady_State()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int received = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        byte[] payload = new byte[64];
        AllocationAssert.NoAllocations(() =>
        {
            client.SendCopy(new SendHeader(2), payload);
            client.Flush();
            network.Advance(1_000);
            server.Poll();
            client.Poll();
            _ = client.HasPendingWork;
            _ = server.HasPendingWork;
        });

        Assert.True(received > 1_000);
        Assert.True(serverSignal.Calls > 1_000);
    }

    [Fact]
    public void Work_Published_Inside_A_Transport_Callback_Signals_Once_When_The_Outermost_Callback_Ends()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal);
        h.Server!.Poll();
        serverSignal.Take();
        PeerCore core = h.Server.Core;

        core.BeginTransportCallback();
        core.BeginTransportCallback(); // a callback MsQuic delivered re-entrantly
        core.NoteTransportWork();
        core.NoteTransportWork();
        core.EndTransportCallback();
        Assert.Equal(0, serverSignal.Calls);
        core.EndTransportCallback();
        Assert.Equal(1, serverSignal.Calls);

        // Outside a callback it is NoteWork itself: set-once until the next Poll.
        core.NoteTransportWork();
        Assert.Equal(1, serverSignal.Calls);
        h.Server.Poll();
        core.NoteTransportWork();
        Assert.Equal(2, serverSignal.Calls);

        // A callback that published nothing tells the host nothing.
        h.Server.Poll();
        core.BeginTransportCallback();
        core.EndTransportCallback();
        Assert.Equal(2, serverSignal.Calls);
    }

    [Fact]
    public void A_Container_Of_Messages_Signals_Once_Before_Its_Receive_Callback_Returns()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal);
        int received = 0;
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        h.Client.Poll();
        h.Server.Poll();
        serverSignal.Take();
        for (int i = 0; i < 30; i++)
        {
            h.Client.SendCopy(new SendHeader(2), new byte[40]);
        }

        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);
        Assert.True(h.Server.HasPendingWork);
        h.Server.Poll();
        Assert.Equal(30, received);
        Assert.False(h.Server.HasPendingWork);
    }

    [Fact]
    public void Mailbox_Posts_And_Reassembled_Fragments_Signal_Once_Per_Receive_Callback_Burst()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal, DatagramTables.Main);
        QuiclyPeer server = h.Server!;
        int latest = 0;
        int fragmented = 0;
        server.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => latest++);
        server.RegisterHandler(13, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            Assert.Equal(3_000, payload.Length);
            fragmented++;
        });
        h.Client.Poll();
        server.Poll();
        serverSignal.Take();

        // Twenty coalescing posts in one container: one edge.
        for (ulong key = 0; key < 20; key++)
        {
            h.Client.SendCopy(new SendHeader(4, key), new byte[16]);
        }

        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);
        Assert.True(server.HasPendingWork);
        server.Poll();
        Assert.Equal(20, latest);
        Assert.False(server.HasPendingWork);

        // A fragmented message is published when its last fragment arrives: one edge, and the message is there to poll.
        serverSignal.Take();
        h.Client.SendCopy(new SendHeader(13), DatagramKit.Payload(1, 3_000));
        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);
        Assert.True(server.HasPendingWork);
        server.Poll();
        Assert.Equal(1, fragmented);
    }

    /// <summary>
    /// A ReliableLatest value reaches the game thread through the key's mailbox, not the receive ring (ADR 0008 invariant 6),
    /// whichever way it arrives — one datagram, a group stream for a large value, or a key retirement on the control stream
    /// — so each of them must raise the edge itself, or an idle peer's host sleeps on while the value waits.
    /// </summary>
    [Fact]
    public void ReliableLatest_Values_And_Key_Retirements_Signal_The_Receiver()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal, LatestTables.Single);
        QuiclyPeer server = h.Server!;
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        server.RegisterHandler(2, LatestKit.Collect(received));
        SettleLatest(h, clientSignal, serverSignal);

        // A value in one datagram.
        h.Client.SendCopy(new SendHeader(2, 7), [1, 2, 3]);
        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);
        Assert.True(server.HasPendingWork);
        server.Poll();
        Assert.Single(received);

        // A large value on a group stream, posted when its last byte is in.
        SettleLatest(h, clientSignal, serverSignal);
        h.Client.SendCopy(new SendHeader(2, 8), LatestKit.Payload(1, 3_000));
        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);
        Assert.True(server.HasPendingWork);
        server.Poll();
        Assert.Equal(2, received.Count);
        Assert.True(LatestKit.Matches(received[1].Payload, 1, 3_000));

        // A key retirement, posted into the key's mailbox by the control path.
        SettleLatest(h, clientSignal, serverSignal);
        Assert.Equal(SendStatus.Admitted, h.Client.RetireKey(2, 7));
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);
        Assert.True(server.HasPendingWork);
        server.Poll();
        Assert.Equal(3, received.Count);
        Assert.Equal(ReceiveFlags.KeyRetired, received[2].Flags);
    }

    /// <summary>
    /// The acks a received value is owed are work only a scheduler pass does: <see cref="QuiclyPeer.HasPendingWork"/> reports
    /// them until a Flush sends them, so a host that woke on the edge and probes the level does not go back to sleep with
    /// the edge still set.
    /// </summary>
    [Fact]
    public void The_Acks_A_ReliableLatest_Value_Is_Owed_Are_Pending_Work_Until_A_Flush_Sends_Them()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal, LatestTables.Single);
        QuiclyPeer server = h.Server!;
        int received = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        SettleLatest(h, clientSignal, serverSignal);
        Assert.False(server.HasPendingWork);

        h.Client.SendCopy(new SendHeader(2, 7), [1, 2, 3]);
        h.Client.Flush();
        h.Network.Advance(1_000);
        server.Poll();
        Assert.Equal(1, received);
        Assert.True(server.HasPendingWork, "the ack the value is owed waits for a Flush");
        server.Flush();
        Assert.False(server.HasPendingWork);
    }

    /// <summary>
    /// The sender's side: a LatestAck completes a value and a LatestReject re-arms its retry, both in the sender's next
    /// scheduler pass (PROTOCOL.md §2.3, §4.4). The transport thread hands them over as notices, which must raise the edge
    /// and show up in <see cref="QuiclyPeer.HasPendingWork"/>, or a host that sleeps until the edge completes the value only
    /// at its retry timer.
    /// </summary>
    [Fact]
    public void A_LatestAck_Or_A_LatestReject_Reaching_The_Sender_Signals_It()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();

        // The receiver's budget holds one 900-byte value and nothing takes it out of the mailbox (no handler), so a second
        // one is answered with LatestReject(RingFull).
        using SessionHarness h = new(table: LatestTables.Single,
            client: o =>
            {
                QuietOptions.Apply(o);
                o.WorkSignal = clientSignal;
            },
            server: o =>
            {
                QuietOptions.Apply(o);
                o.WorkSignal = serverSignal;
                o.ReceiveBudgetBytes = 2048;
            });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        SettleLatest(h, clientSignal, serverSignal);

        SendResult first = client.SendCopy(new SendHeader(2, 1), LatestKit.Payload(1, 900), SendOptions.Tracked);
        Assert.True(first.IsAdmitted);
        client.Flush();
        h.Network.Advance(1_000); // the value reaches the server; the client's own datagram states come back
        client.Poll();            // the host takes those: nothing is left for the client
        clientSignal.Take();
        Assert.False(client.HasPendingWork);

        server.Poll();
        server.Flush(); // the LatestAck
        h.Network.Advance(1_000);
        Assert.Equal(1, clientSignal.Calls);
        Assert.True(client.HasPendingWork);
        client.Flush(); // the pass applies the ack: Delivered
        client.Poll();
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(first.Token));
        Assert.False(client.HasPendingWork);

        SendResult second = client.SendCopy(new SendHeader(2, 2), LatestKit.Payload(2, 900), SendOptions.Tracked);
        Assert.True(second.IsAdmitted);
        client.Flush();
        h.Network.Advance(1_000);
        Assert.True(DatagramKit.ChannelStats(server, 2).OutOfBuffers > 0, "the receiver did not refuse the second value");
        client.Poll();
        clientSignal.Take();
        Assert.False(client.HasPendingWork);

        h.Network.Advance(10_000); // past the receiver's AckDelay window, which its LatestAck opened
        server.Poll();
        server.Flush(); // the LatestReject
        h.Network.Advance(1_000);
        Assert.Equal(1, clientSignal.Calls);
        Assert.True(client.HasPendingWork);
    }

    /// <summary>
    /// ReliableLatest publications inside a transport callback raise the signal once when the callback ends, not once per
    /// value: twenty values in one container (twenty mailbox posts, twenty acks owed) and their twenty acks in one LatestAck
    /// datagram cost one host call each. The recording signal re-arms the edge itself, so it counts every call the peer makes.
    /// </summary>
    [Fact]
    public void A_Burst_Of_ReliableLatest_Values_And_Their_Acks_Signal_Once_Per_Receive_Callback()
    {
        ReArmingWorkSignal clientSignal = new();
        ReArmingWorkSignal serverSignal = new();
        using SessionHarness h = new(table: LatestTables.Single,
            client: o =>
            {
                QuietOptions.Apply(o);
                o.WorkSignal = clientSignal;
            },
            server: o =>
            {
                QuietOptions.Apply(o);
                o.WorkSignal = serverSignal;
            });
        QuiclyPeer server = h.Server!;
        int received = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        h.Run(20_000);
        h.Client.Poll();
        server.Poll();
        clientSignal.Calls = 0;
        serverSignal.Calls = 0;

        for (ulong key = 0; key < 20; key++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(2, key), new byte[16]).IsAdmitted);
        }

        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);
        server.Poll();
        Assert.Equal(20, received);

        h.Client.Poll(); // the client's own datagram states
        clientSignal.Calls = 0;
        server.Flush(); // twenty acks, one LatestAck datagram
        h.Network.Advance(1_000);
        Assert.Equal(1, clientSignal.Calls);
        Assert.True(h.Client.HasPendingWork);
    }

    [Fact]
    public void The_Work_Signal_Does_Not_Allocate_With_ReliableLatest_Traffic()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal, LatestTables.Single);
        SimulatedNetwork network = h.Network;
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int received = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        byte[] payload = new byte[64];
        ulong key = 0;
        AllocationAssert.NoAllocations(() =>
        {
            client.SendCopy(new SendHeader(2, key++ & 15), payload);
            client.Flush();
            network.Advance(1_000);
            server.Poll();
            _ = server.HasPendingWork;
            server.Flush();
            network.Advance(1_000);
            client.Poll();
            _ = client.HasPendingWork;
            client.Flush();
        });

        Assert.True(received > 1_000);
        Assert.True(serverSignal.Calls > 1_000);
        Assert.True(clientSignal.Calls > 1_000);
    }

    /// <summary>
    /// A pass computes <see cref="QuiclyPeer.NextFlushDeadlineMicros"/> from what it saw, so the acks a value received after
    /// it is owed must bring the deadline forward at the Poll that dispatches the value: to <see cref="PeerOptions.AckDelay"/>
    /// later, the longest delay the option allows (and not before the coalescing window of the last transmission ends). A
    /// Flush before then — the host's own tick — sends the ack with its other traffic. A LatestAck reaching the sender does
    /// not move the sender's deadline: the pass that would retransmit applies it first.
    /// </summary>
    [Fact]
    public void Acks_Owed_Bring_The_Flush_Deadline_Forward_To_AckDelay()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal, LatestTables.Single);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long ackDelay = h.ServerOptions.AckDelay.Ticks / 10;
        Assert.True(ackDelay > 2_000);
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        SettleLatest(h, clientSignal, serverSignal);
        server.Flush();
        Assert.Equal(long.MaxValue, server.NextFlushDeadlineMicros);

        SendResult first = client.SendCopy(new SendHeader(2, 7), [1, 2, 3], SendOptions.Tracked);
        client.Flush();
        h.Network.Advance(1_000);
        server.Poll();
        long owed = h.Clock.NowMicros;
        Assert.Equal(owed + ackDelay, server.NextFlushDeadlineMicros);

        // A Poll later on keeps the deadline where the first one put it.
        h.Network.Advance(1_000);
        server.Poll();
        Assert.Equal(owed + ackDelay, server.NextFlushDeadlineMicros);

        // The host's own flush comes first and sends the ack; nothing is left for the deadline.
        server.Flush();
        long ackSent = h.Clock.NowMicros;
        Assert.Equal(long.MaxValue, server.NextFlushDeadlineMicros);
        Assert.False(server.HasPendingWork);

        // The sender: the LatestAck waits for its next pass, which the retry timer bounds; it does not move the deadline.
        client.Poll();
        long retry = client.NextFlushDeadlineMicros;
        Assert.True(retry > h.Clock.NowMicros && retry < long.MaxValue, "no retry timer armed");
        h.Network.Advance(1_000);
        client.Poll();
        Assert.True(client.HasPendingWork, "the LatestAck waits for the sender's next pass");
        Assert.Equal(retry, client.NextFlushDeadlineMicros);
        client.Flush();
        Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(first.Token));

        // A second value right after the transmission: its ack may not leave before the coalescing window closes, and not
        // later than AckDelay after it was first seen.
        client.SendCopy(new SendHeader(2, 8), [4, 5, 6]);
        client.Flush();
        h.Network.Advance(1_000);
        server.Poll();
        Assert.Equal(Math.Max(ackSent + ackDelay, h.Clock.NowMicros + ackDelay), server.NextFlushDeadlineMicros);
    }

    /// <summary>With <see cref="PeerOptions.AckDelay"/> 0 ("acknowledge at once") owed acks are due at the Poll that saw them.</summary>
    [Fact]
    public void With_No_AckDelay_Owed_Acks_Are_Due_At_Once()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = new(table: LatestTables.Single,
            client: o =>
            {
                QuietOptions.Apply(o);
                o.WorkSignal = clientSignal;
            },
            server: o =>
            {
                QuietOptions.Apply(o);
                o.WorkSignal = serverSignal;
                o.AckDelay = TimeSpan.Zero;
            });
        QuiclyPeer server = h.Server!;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        SettleLatest(h, clientSignal, serverSignal);
        server.Flush();

        h.Client.SendCopy(new SendHeader(2, 7), [1, 2, 3]);
        h.Client.Flush();
        h.Network.Advance(1_000);
        server.Poll();
        Assert.Equal(h.Clock.NowMicros, server.NextFlushDeadlineMicros);
        server.Flush();
        Assert.Equal(long.MaxValue, server.NextFlushDeadlineMicros);
    }

    /// <summary>
    /// Acks a pass could not send at all (no carrier: a datagram limit too small for any batch) keep the time that pass
    /// published, which is none: a Poll must not bring the flush deadline forward to now for them, or a host that sleeps
    /// until the deadline spins on a Flush that cannot send.
    /// </summary>
    [Fact]
    public void Acks_A_Pass_Could_Not_Send_Do_Not_Pin_The_Flush_Deadline_At_Now()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        LinkOptions link = new() { MtuChanges = { new MtuChange(200_000, 8) } };
        using SessionHarness h = new(link: link, table: LatestTables.Single,
            client: o =>
            {
                QuietOptions.Apply(o);
                o.WorkSignal = clientSignal;
            },
            server: o =>
            {
                QuietOptions.Apply(o);
                o.WorkSignal = serverSignal;
            });
        QuiclyPeer server = h.Server!;
        int received = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        SettleLatest(h, clientSignal, serverSignal);
        Assert.True(h.Clock.NowMicros < 190_000);

        h.Client.SendCopy(new SendHeader(2, 7), [1, 2, 3]);
        h.Client.Flush();
        h.Network.Advance(1_000);
        server.Poll();
        Assert.Equal(1, received);
        h.Network.AdvanceTo(210_000); // datagrams now carry at most 8 bytes: no ack batch fits
        server.Poll();
        server.Flush();
        Assert.True(server.HasPendingWork, "the ack is still owed");
        Assert.Equal(long.MaxValue, server.NextFlushDeadlineMicros);

        server.Poll();
        Assert.Equal(long.MaxValue, server.NextFlushDeadlineMicros);
    }

    // ------------------------------------------------------------------ Bulk

    /// <summary>
    /// The Bulk engine hands the peer's control frames (BulkProgress, BulkRequest, BulkCancel, BulkReject) to its next pass
    /// on the transport thread. Like every other publication inside a transport callback they raise the signal once, when the
    /// callback ends: a control-stream read that carries a dozen frames costs one host call.
    /// </summary>
    [Fact]
    public void Bulk_Control_Frames_Inside_A_Transport_Callback_Signal_Once_When_It_Ends()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal, BulkTables.Main);
        QuiclyPeer server = h.Server!;
        h.Run(20_000);
        server.Poll();
        serverSignal.Take();

        Span<byte> frame = stackalloc byte[32];
        Assert.True(ControlCodec.TryWrite(frame, new BulkProgress(1, 100), ControlCarrier.Stream, out int written));
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(frame.Slice(0, written), out ControlType type, out ReadOnlySpan<byte> body, out _));
        BulkEngine bulk = BulkKit.Engine(server);
        PeerCore core = server.Core;
        core.BeginTransportCallback();
        for (int i = 0; i < 3; i++)
        {
            Assert.True(bulk.OnControl(type, body, onStream: true, h.Clock.NowMicros));
        }

        Assert.Equal(0, serverSignal.Calls);
        core.EndTransportCallback();
        Assert.Equal(1, serverSignal.Calls);
    }

    /// <summary>
    /// What the transport thread leaves for the Bulk engine's next pass — a range request from the peer, the start notice of
    /// the stream the transfer opened, the progress the receiver owes for the bytes it accepted — raises the edge, so
    /// <see cref="QuiclyPeer.HasPendingWork"/> reports it until a pass serves it: a host that woke on the edge and probes
    /// the level must not go back to sleep with the work waiting. The flush deadline comes forward for it too.
    /// </summary>
    [Fact]
    public void Bulk_Work_Only_A_Pass_Serves_Is_Pending_Work_Until_A_Flush_Serves_It()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        byte[] payload = new byte[3_000];
        Random.Shared.NextBytes(payload);
        MemoryProvider provider = new(payload);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Receiver(router)(o);
                o.WorkSignal = clientSignal;
            },
            server: o =>
            {
                BulkKit.Server(provider)(o);
                o.WorkSignal = serverSignal;
            });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        SettleLatest(h, clientSignal, serverSignal);
        Assert.False(server.HasPendingWork);

        // The request reaches the provider's peer; only its pass asks the provider and starts the transfer.
        client.RequestBulk(new BulkRangeRequest(5, 1, 1, 0, payload.Length));
        client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);
        server.Poll();
        Assert.True(server.HasPendingWork, "the range request waits for the provider's next pass");
        Assert.True(server.NextFlushDeadlineMicros <= h.Clock.NowMicros, "the range request did not bring the flush deadline forward");
        server.Flush();
        Assert.Single(provider.Requests);
        Assert.Equal(1, BulkKit.SendTransfers(server, 5));
        Assert.False(server.HasPendingWork);

        // The transport reports the transfer's stream started: a notice the next pass applies.
        h.Network.Advance(1_000);
        server.Poll(); // the pieces' completions are the Poll's
        Assert.True(server.HasPendingWork, "the stream's start notice waits for the next pass");
        server.Flush();
        Assert.False(server.HasPendingWork);

        // The receiver accepted bytes whose progress it owes.
        h.Network.Advance(1_000);
        client.Poll();
        Assert.True(router.Sinks.Count == 1 && router.Sink<MemorySink>().BytesWritten > 0, "no body reached the receiver");
        Assert.True(client.HasPendingWork, "the progress the receiver owes waits for its next pass");

        Assert.True(h.RunUntil(() => BulkKit.SendTransfers(server, 5) == 0 && BulkKit.ReceiveStreams(client, 5) == 0), "the transfer did not finish");
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);
        h.Run(200_000);
        Assert.False(client.HasPendingWork);
        Assert.False(server.HasPendingWork);
    }

    /// <summary>
    /// A transfer the application starts, and a cancel it asks for from any thread, raise the edge and are pending work until
    /// the next pass acts on them, so the level agrees with the edge.
    /// </summary>
    [Fact]
    public async Task A_Bulk_Send_Started_Or_Canceled_By_The_Application_Is_Pending_Work_Until_A_Flush()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        byte[] payload = new byte[512 * 1024];
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        using SessionHarness h = new(table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Quiet(o);
                o.WorkSignal = clientSignal;
            },
            server: o =>
            {
                BulkKit.Receiver(router)(o);
                o.WorkSignal = serverSignal;
            });
        QuiclyPeer client = h.Client;
        SettleLatest(h, clientSignal, serverSignal);
        Assert.False(client.HasPendingWork);

        BulkTransfer transfer = await client.BeginBulkSendAsync(new BulkDescriptor(5, 7, 1, payload.Length), new MemorySource(payload));
        Assert.Equal(BulkStatus.Running, transfer.Status);
        Assert.Equal(1, clientSignal.Calls);
        Assert.True(client.HasPendingWork, "the new transfer waits for the next pass");
        client.Flush();
        Assert.False(client.HasPendingWork);

        client.Poll();
        clientSignal.Take();
        await Task.Run(transfer.Cancel);
        Assert.Equal(1, clientSignal.Calls);
        Assert.True(client.HasPendingWork, "the cancel waits for the next pass");
        client.Flush();
        Assert.Equal(BulkStatus.Canceled, transfer.Status);
        Assert.False(client.HasPendingWork);
    }

    /// <summary>Runs both ends until the link is quiet, polls them (re-arming both edges) and forgets the calls so far.</summary>
    private static void SettleLatest(SessionHarness h, RecordingWorkSignal clientSignal, RecordingWorkSignal serverSignal)
    {
        h.Run(20_000);
        h.Client.Poll();
        h.Server!.Poll();
        clientSignal.Take();
        serverSignal.Take();
    }

    /// <summary>
    /// Counts every <see cref="IPeerWorkSignal.OnWork"/> the peer makes: it re-arms the peer's edge at once, as a host that
    /// polled instantly would, so a signal raised per publication instead of per callback shows up as extra calls.
    /// </summary>
    private sealed class ReArmingWorkSignal : IPeerWorkSignal
    {
        private static readonly FieldInfo Signalled =
            typeof(QuiclyPeer).GetField("_workSignalled", BindingFlags.Instance | BindingFlags.NonPublic)!;

        public int Calls;

        public void OnWork(QuiclyPeer peer)
        {
            Calls++;
            Signalled.SetValue(peer, 0);
        }
    }
}
