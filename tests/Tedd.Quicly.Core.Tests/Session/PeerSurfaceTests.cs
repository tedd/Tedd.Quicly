using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The host-facing surface added for the client and server layers: the split deadlines, <see cref="PeerOptions.Clone"/> and
/// <see cref="PeerOptions.Validate"/>, <see cref="QuiclyPeer.IsDisposed"/>, and a <see cref="QuiclyPeer.StateChanged"/>
/// handler that throws.
/// </summary>
public class PeerSurfaceTests
{
    [Fact]
    public void NextDeadline_Is_The_Minimum_Of_The_Poll_And_Flush_Deadlines()
    {
        using SessionHarness h = new(connect: false);
        // Before the handshake only the peer's own admission timer is scheduled; no engine work exists yet.
        h.Client.Poll();
        Assert.Equal(long.MaxValue, h.Client.NextFlushDeadlineMicros);
        Assert.True(h.Client.NextPollDeadlineMicros < long.MaxValue);
        Assert.Equal(h.Client.NextPollDeadlineMicros, h.Client.NextDeadlineMicros);

        Assert.True(h.RunUntilConnected());
        Assert.Equal(Math.Min(h.Client.NextPollDeadlineMicros, h.Client.NextFlushDeadlineMicros), h.Client.NextDeadlineMicros);

        h.Client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.Equal(long.MaxValue, h.Client.NextFlushDeadlineMicros);
    }

    [Fact]
    public void A_Send_Cap_Sets_A_Flush_Deadline_That_Poll_Alone_Does_Not_Serve()
    {
        using SessionHarness h = new(client: o =>
        {
            QuietOptions.Apply(o);
            o.MaxSendBytesPerSecond = 4_000;
        }, server: QuietOptions.Apply);

        for (int i = 0; i < 20; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(2), new byte[64]).IsAdmitted);
        }

        h.Client.Flush();
        long flushDeadline = h.Client.NextFlushDeadlineMicros;
        Assert.True(flushDeadline < long.MaxValue, "the cap should hold work back and ask for another flush");
        Assert.True(h.Client.NextPollDeadlineMicros > flushDeadline, "the ping timer is far away with quiet options");
        Assert.Equal(flushDeadline, h.Client.NextDeadlineMicros);

        // Polling does not run the scheduler, so the flush deadline stays where it is.
        h.Network.AdvanceTo(flushDeadline + 1);
        h.Client.Poll();
        Assert.Equal(flushDeadline, h.Client.NextFlushDeadlineMicros);
    }

    [Fact]
    public void A_Host_Loop_Driven_By_The_Two_Deadlines_Delivers_Capped_Traffic()
    {
        using SessionHarness h = new(client: o =>
        {
            QuietOptions.Apply(o);
            o.MaxSendBytesPerSecond = 4_000;
        }, server: QuietOptions.Apply);

        int received = 0;
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        const int Messages = 20;
        for (int i = 0; i < Messages; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(2), new byte[64]).IsAdmitted);
        }

        int flushes = 0;
        int polls = 0;
        h.Client.Flush();
        flushes++;
        long end = h.Network.NowMicros + 5_000_000;
        while (received < Messages && h.Network.NowMicros < end)
        {
            long flushDue = h.Client.NextFlushDeadlineMicros;
            long pollDue = h.Client.NextPollDeadlineMicros;
            long next = Math.Min(flushDue, pollDue);
            h.Network.AdvanceTo(Math.Clamp(next, h.Network.NowMicros + 1, end));
            long now = h.Network.NowMicros;
            if (now >= flushDue)
            {
                h.Client.Flush();
                flushes++;
            }

            if (now >= pollDue || h.Client.HasPendingWork)
            {
                h.Client.Poll();
                polls++;
            }

            h.Server.Poll();
            h.Server.Flush();
        }

        Assert.Equal(Messages, received);
        Assert.True(flushes > 1, $"the cap should need several flushes (got {flushes})");
        Assert.True(polls >= 1);
    }

    [Fact]
    public void Clone_Copies_Every_Option_And_Is_Independent()
    {
        VirtualClock clock = new();
        RecordingWorkSignal signal = new();
        PeerOptions original = new()
        {
            Clock = clock,
            WorkSignal = signal,
            SendTableCapacity = 64,
            ReceiveRingCapacity = 32,
            SegmentArenaCapacity = 16,
            MaxSendBytesPerSecond = 1_234,
            ThreadSafeSend = true,
            CompletionMode = CompletionMode.ThreadPool,
            RequestChannelTable = true,
            LastEpoch = 7,
            SessionToken = new byte[] { 1, 2, 3 },
            MaxReceiveDatagram = 1_100,
            HeartbeatTimeout = TimeSpan.FromSeconds(3),
            FailFastOnCallbackException = false,
            DropWhenBlocked = false,
        };

        Assert.True(new PeerOptions().DropWhenBlocked);
        PeerOptions copy = original.Clone();
        Assert.False(copy.DropWhenBlocked);
        Assert.NotSame(original, copy);
        Assert.Same(clock, copy.Clock);
        Assert.Same(signal, copy.WorkSignal);
        Assert.Equal(64, copy.SendTableCapacity);
        Assert.Equal(32, copy.ReceiveRingCapacity);
        Assert.Equal(16, copy.SegmentArenaCapacity);
        Assert.Equal(1_234, copy.MaxSendBytesPerSecond);
        Assert.True(copy.ThreadSafeSend);
        Assert.Equal(CompletionMode.ThreadPool, copy.CompletionMode);
        Assert.True(copy.RequestChannelTable);
        Assert.Equal(7u, copy.LastEpoch);
        Assert.Equal(new byte[] { 1, 2, 3 }, copy.SessionToken.ToArray());
        Assert.Equal(1_100, copy.MaxReceiveDatagram);
        Assert.Equal(TimeSpan.FromSeconds(3), copy.HeartbeatTimeout);

        copy.SendTableCapacity = 256;
        copy.LastEpoch = 9;
        Assert.Equal(64, original.SendTableCapacity);
        Assert.Equal(7u, original.LastEpoch);
        copy.Validate();
    }

    [Fact]
    public void Validate_Is_Public_And_Rejects_A_Bad_Option_Before_Any_Peer_Is_Built()
    {
        PeerOptions good = new();
        good.Validate();

        PeerOptions bad = new() { SendTableCapacity = 1 };
        ArgumentException error = Assert.Throws<ArgumentException>(() => bad.Validate());
        Assert.Equal(nameof(PeerOptions.SendTableCapacity), error.ParamName);

        PeerOptions unsupported = new() { AutoFlushInterval = TimeSpan.FromMilliseconds(5) };
        Assert.Throws<NotSupportedException>(() => unsupported.Validate());
    }

    [Fact]
    public void A_Cloned_Options_Instance_Builds_A_Working_Peer()
    {
        PeerOptions template = new();
        QuietOptions.Apply(template);
        using SessionHarness h = new(client: o =>
        {
            PeerOptions clone = template.Clone();
            clone.Clock = o.Clock;
            o.PingInterval = clone.PingInterval;
            o.HeartbeatTimeout = clone.HeartbeatTimeout;
            o.FastLockDuration = clone.FastLockDuration;
            o.StreamIdleTimeout = clone.StreamIdleTimeout;
            clone.Validate();
        }, server: QuietOptions.Apply);
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    [Fact]
    public void IsDisposed_Reports_The_Peer_Lifecycle()
    {
        SessionHarness h = new();
        Assert.False(h.Client.IsDisposed);
        Assert.False(h.Server!.IsDisposed);
        h.DisposeClient();
        Assert.True(h.Client.IsDisposed);
        Assert.False(h.Server.IsDisposed);
        Assert.Throws<ObjectDisposedException>(() => h.Client.Poll());
        h.Dispose();
        Assert.True(h.Server.IsDisposed);
    }

    [Fact]
    public void Transport_Outcome_Statistics_Start_At_Zero_And_Read_As_Default_After_Dispose()
    {
        SessionHarness h = new(client: QuietOptions.Apply, server: QuietOptions.Apply);
        h.Run(50_000);
        h.Client.GetStatistics(out PeerStatistics peer);
        Assert.Equal(0, peer.DatagramsAcknowledged);
        Assert.Equal(0, peer.DatagramsLost);
        Assert.Equal(0, peer.DatagramsCanceled);
        Assert.True(h.Client.GetChannelStatistics(2, out ChannelStatistics channel));
        Assert.Equal(0, channel.TransportCanceled);
        Assert.Equal(0, channel.TransportLost);

        // The handshake's control datagrams (the first Ping and its Pong) were acknowledged by now, and are not counted:
        // the outcome counters cover the datagrams of DatagramsSent only.
        Assert.True(peer.PingsSent > 0);
        Assert.Equal(0, peer.DatagramsSent);

        h.DisposeClient();
        h.Client.GetStatistics(out peer);
        Assert.Equal(default, peer);
        Assert.False(h.Client.GetChannelStatistics(2, out channel));
        Assert.Equal(default, channel);
        h.Dispose();
    }

    [Fact]
    public void A_Throwing_StateChanged_Handler_Does_Not_Re_Raise_The_Same_Transition()
    {
        using SessionHarness h = new(connect: false);
        List<(PeerState From, PeerState To)> seen = [];
        h.Client.StateChanged += (_, from, to) =>
        {
            seen.Add((from, to));
            throw new InvalidOperationException("handler fault");
        };

        Assert.True(h.RunUntilConnected());
        h.Client.Poll();
        h.Client.Poll();

        // Each transition was delivered exactly once, even though every handler call threw.
        Assert.Equal(HandshakeTests.ConnectedEvents, seen);
        Assert.Equal(HandshakeTests.ConnectedEvents, h.ClientEvents);
        Assert.NotNull(h.Client.LastCallbackFault);
        h.Client.GetStatistics(out PeerStatistics statistics);
        Assert.Equal(2, statistics.CallbackFaults);

        // The peer keeps working: a throwing handler is the host's bug, not the session's.
        Assert.Equal(PeerState.Connected, h.Client.State);
        h.Client.Close(new CloseReason(QuiclyErrorCode.NoError));
        Assert.True(h.RunUntilClosed());
        Assert.Equal(
            (PeerState.Closing, PeerState.Closed),
            seen[^1]);
        Assert.Equal(4, seen.Count);
    }
}
