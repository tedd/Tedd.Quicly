using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
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
    public void A_Coalescing_Mailbox_Signals_And_Shows_Up_In_HasPendingWork()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = NewPair(clientSignal, serverSignal, CoalescingTable);
        h.Server!.Poll();
        serverSignal.Take();

        // A coalescing channel bypasses the receive ring: the mailbox raises the signal instead (ADR 0008 §6).
        h.Client.SendCopy(new SendHeader(3, 42), [1, 2]);
        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);
        Assert.True(h.Server.HasPendingWork);

        int received = 0;
        h.Server.RegisterHandler(3, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
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
}
