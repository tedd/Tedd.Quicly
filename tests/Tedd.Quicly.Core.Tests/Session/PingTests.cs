using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Ping/Pong, RTT, clock offset, rate limits and heartbeat (PROTOCOL.md §2.3, §4.6, §7).</summary>
public class PingTests
{
    [Fact]
    public void Rtt_Matches_The_Link_Delay()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 20_000 });
        h.Run(1_000_000);
        h.Client.GetStatistics(out PeerStatistics client);
        h.Server!.GetStatistics(out PeerStatistics server);
        Assert.True(client.RttSamples >= 5, $"{client.RttSamples} samples");
        Assert.Equal(40_000, client.MinRttMicros);
        Assert.Equal(40_000, client.LatestRttMicros);
        Assert.Equal(40_000, client.MaxRttMicros);
        Assert.Equal(40_000, client.SmoothedRttMicros);
        Assert.Equal(40_000, server.SmoothedRttMicros);
        Assert.Equal(0, client.UnmatchedPongs);
        Assert.Equal(client.PingsSent, server.PongsSent + server.PingsIgnored + (client.PingsSent - client.PongsReceived));
    }

    [Fact]
    public void Rtt_With_Jitter_Stays_Within_The_Jitter_Bound()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 20_000, JitterMicros = 4_000 });
        h.Run(2_000_000);
        h.Client.GetStatistics(out PeerStatistics stats);
        Assert.True(stats.RttSamples >= 10);
        Assert.InRange(stats.MinRttMicros, 40_000, 48_000);
        Assert.InRange(stats.MaxRttMicros, 40_000, 48_000);
        Assert.InRange(stats.SmoothedRttMicros, 40_000, 48_000);
        Assert.True(stats.JitterMicros > 0);
        Assert.True(stats.RttVarianceMicros > 0);
    }

    [Fact]
    public void Clock_Offset_Tracks_Skewed_Clocks()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 15_000 },
            client: o => o.Clock = new OffsetClock(o.Clock, 7_000_000),
            server: o => o.Clock = new OffsetClock(o.Clock, 3_000_000));
        h.Run(1_500_000);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long serverRelative = server.Core.Clock.NowMicros - server.Core.ConnectionStartMicros;
        long clientRelative = client.Core.Clock.NowMicros - client.Core.ConnectionStartMicros;
        Assert.InRange(client.EstimatedRemoteMicros() - serverRelative, -2, 2);
        Assert.InRange(server.EstimatedRemoteMicros() - clientRelative, -2, 2);
        client.GetStatistics(out PeerStatistics stats);
        Assert.Equal(stats.ClockOffsetMicros, client.EstimatedRemoteMicros() - clientRelative);
        Assert.NotEqual(0, stats.ClockOffsetMicros);
    }

    [Fact]
    public void Offset_Is_Slewed_Not_Stepped_After_The_Fast_Lock_With_A_Drifting_Clock()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 10_000 },
            server: o => o.Clock = new OffsetClock(o.Clock, 1_000_000, rate: 1.0005));
        h.Run(3_200_000, 10_000);
        h.Client.GetStatistics(out PeerStatistics start);
        long previous = start.ClockOffsetMicros;
        long maxStep = 0;
        for (int i = 0; i < 500; i++)
        {
            h.Run(10_000, 10_000);
            h.Client.GetStatistics(out PeerStatistics stats);
            maxStep = Math.Max(maxStep, Math.Abs(stats.ClockOffsetMicros - previous));
            previous = stats.ClockOffsetMicros;
        }

        Assert.InRange(maxStep, 1, (10_000 / PingClock.SlewDivisor) + 1);
        QuiclyPeer server = h.Server!;
        long actual = server.Core.Clock.NowMicros - server.Core.ConnectionStartMicros;
        Assert.InRange(h.Client.EstimatedRemoteMicros() - actual, -1_500, 1_500);
    }

    [Fact]
    public void Fast_Lock_Pings_Every_100ms_For_3s_Then_Every_Second()
    {
        using SessionHarness h = new();
        h.Run(2_950_000);
        h.Client.GetStatistics(out PeerStatistics early);
        Assert.InRange(early.PingsSent, 29, 31);
        Assert.True(h.Client.NextDeadline <= TimeSpan.FromMilliseconds(100));
        h.Run(3_000_000);
        h.Client.GetStatistics(out PeerStatistics late);
        Assert.InRange(late.PingsSent - early.PingsSent, 3, 4);
        Assert.True(h.Client.NextDeadline <= TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Excess_Pings_Are_Ignored()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        byte[] ping = Frames.PingFrame(1, ControlCarrier.Datagram);
        for (int i = 0; i < 60; i++)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(ping));
            h.Run(1_000);
        }

        PeerStatistics stats = h.Statistics();
        Assert.Equal(60, stats.PongsSent + stats.PingsIgnored);
        Assert.InRange(stats.PongsSent, 32, 33);
        Assert.Equal(PeerState.Connected, h.Server!.State);
        Assert.Equal(stats.PongsSent, h.Raw.DatagramsOfType(ControlType.Pong));
    }

    [Fact]
    public void A_Burst_Of_Pings_Beyond_The_Pong_Pool_Is_Counted()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        byte[] ping = Frames.PingFrame(1, ControlCarrier.Datagram);
        for (int i = 0; i < 20; i++)
        {
            h.Raw.SendDatagram(ping);
        }

        h.Run(5_000);
        PeerStatistics stats = h.Statistics();
        Assert.Equal(TransportControlPool.SlotCount, stats.PongsSent);
        Assert.Equal(20 - TransportControlPool.SlotCount, stats.ControlSendFailures);
    }

    [Fact]
    public void A_Control_Message_Flood_Closes_With_LimitExceeded()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        byte[] pong = Frames.PongFrame(5, 6, 7, ControlCarrier.Datagram);
        for (int i = 0; i < 250; i++)
        {
            h.Raw.SendDatagram(pong);
        }

        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.LimitExceeded, h.Raw.CloseCode);
        Assert.True(h.RunUntil(() => h.Server!.State == PeerState.Closed));
        Assert.Equal(new CloseReason(QuiclyErrorCode.LimitExceeded, "limit exceeded") { Source = CloseSource.Local }, h.Server!.CloseReason);
    }

    [Fact]
    public void Heartbeat_Timeout_Closes_A_Silent_Session()
    {
        using SessionHarness h = new(link: new LinkOptions { LossPercent = 100 },
            client: o => o.HeartbeatTimeout = TimeSpan.FromSeconds(1),
            server: o => o.HeartbeatTimeout = TimeSpan.FromSeconds(1));
        long connectedAt = h.Network.NowMicros;
        Assert.True(h.RunUntilClosed(5_000_000));
        Assert.Equal(QuiclyErrorCode.Timeout, h.Client.CloseReason.Code);
        Assert.Equal(QuiclyErrorCode.Timeout, h.Server!.CloseReason.Code);
        Assert.InRange(h.Network.NowMicros - connectedAt, 900_000, 1_500_000);
    }

    [Fact]
    public void Zero_Heartbeat_Timeout_Disables_The_Check()
    {
        using SessionHarness h = new(link: new LinkOptions { LossPercent = 100 },
            client: o => o.HeartbeatTimeout = TimeSpan.Zero,
            server: o => o.HeartbeatTimeout = TimeSpan.Zero);
        h.Run(3_000_000, 10_000);
        Assert.Equal(PeerState.Connected, h.Client.State);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void Unmatched_Pongs_Are_Counted_And_Not_Used()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        h.Raw.SendDatagram(Frames.PongFrame(123_456, 1, 2, ControlCarrier.Datagram));
        h.Run(5_000);
        PeerStatistics stats = h.Statistics();
        Assert.Equal(1, stats.UnmatchedPongs);
        Assert.Equal(1, stats.PongsReceived);
        Assert.Equal(0, stats.RttSamples);
    }

    [Fact]
    public void Stream_Ping_From_The_Client_Is_Answered_On_The_Control_Stream()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        h.Raw.SendControl(Frames.PingFrame(777, ControlCarrier.Stream));
        Assert.True(h.RunUntil(() => h.Raw.ServerFrames().Exists(f => f.Type == ControlType.Pong)));
        (ControlType _, byte[] body) = h.Raw.ServerFrames().Find(f => f.Type == ControlType.Pong);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out Pong pong));
        Assert.Equal(777u, pong.EchoedTimeMicros);
        Assert.Equal(1, h.Statistics().PongsSent);
    }

    [Fact]
    public void Malformed_Control_Messages_Are_Handled_By_Carrier()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        // Datagram: a truncated Ping is dropped and counted; the connection survives.
        h.Raw.SendDatagram([0x00, 0x01, 1, 2]);
        h.Run(5_000);
        Assert.Equal(1, h.Statistics().MalformedDatagrams);
        Assert.Equal(PeerState.Connected, h.Server!.State);
        // Stream: the same message closes the connection.
        h.Raw.SendControl(Frames.RawFrame((byte)ControlType.Ping, [1, 2]));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }
}
