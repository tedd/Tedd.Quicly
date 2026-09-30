using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Review (recheck lens) of d6df494 (the work probe no longer counts a mailbox value of a channel without a handler).
/// The work signal is an edge that only a <see cref="QuiclyPeer.Poll"/> re-arms, and <see cref="QuiclyPeer.HasPendingWork"/>
/// is documented as "the authority on whether anything is actually waiting" (IPeerWorkSignal remarks; ARCHITECTURE.md:
/// "HasPendingWork is the level behind this edge"). A mailbox arrival on a handler-less channel now consumes the edge
/// while the level stays clear, so a host that polls only when the level says so never re-arms the edge: every later
/// publication of the peer (a message for a handler, a completion, a close) raises no signal.
/// </summary>
public class ReviewRecheckWorkSignalTests
{
    /// <summary>2 unordered with a handler; 3 keyed sequenced, coalescing (a mailbox), read with Drain.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "fx", ChannelMode.UnreliableUnordered)
        .Add(3, "state", ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.CoalesceOnReceive = true;
            o.MaxKeys = 64;
        })
        .Build();

    [Fact]
    public void A_Mailbox_Value_Nobody_Polls_For_Does_Not_Silence_The_Work_Signal()
    {
        RecordingWorkSignal clientSignal = new();
        RecordingWorkSignal serverSignal = new();
        using SessionHarness h = new(table: Table,
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
        int handled = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => handled++);
        server.Poll();
        server.Flush();
        serverSignal.Take();
        Assert.False(server.HasPendingWork);

        // A value for the Drain-style mailbox channel: the edge fires, the level stays clear (d6df494).
        h.Client.SendCopy(new SendHeader(3, 42), [1, 2]);
        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.Equal(1, serverSignal.Calls);

        // The host wakes on the edge and does what the documentation describes: the level decides whether to poll
        // ("a host that polls while there is work"), and the Drain-style channel is drained "on the signal".
        int polls = 0;
        while (server.HasPendingWork && polls < 8)
        {
            server.Poll();
            server.Flush();
            polls++;
        }

        ReceivedMessage[] buffer = new ReceivedMessage[8];
        int n = server.Drain(3, buffer);
        Assert.Equal(1, n);
        server.Release(buffer.AsSpan(0, n));

        // Now ordinary work arrives: a message for the channel that has a handler. The host sleeps until the signal.
        h.Client.SendCopy(new SendHeader(2), [7]);
        h.Client.Flush();
        h.Network.Advance(1_000);
        Assert.True(server.HasPendingWork, "the message is waiting in the ring");
        Assert.True(serverSignal.Calls == 2,
            $"the work signal was raised {serverSignal.Calls} time(s): the message for the handler arrived without a wake-up, " +
            $"because the host made {polls} Poll call(s) after the mailbox value (HasPendingWork was false) and only a Poll re-arms the edge");
    }
}
