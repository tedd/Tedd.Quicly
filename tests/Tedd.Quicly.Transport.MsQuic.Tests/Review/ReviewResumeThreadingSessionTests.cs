using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Adversarial review of fix/msquic-held-stream-resume (8107fc3), threading lens: the session layer end to end over real
/// MsQuic loopback. Two peers, each with its own game thread that polls irregularly, a receive ring of 64 and a small
/// receive budget, exchange tens of thousands of messages in both directions on a ReliableOrdered and a ReliableUnordered
/// channel. Every stream is therefore held (the ring is full, the budget is used up) and resumed from Poll again and
/// again, with the resume landing before, while and after the receive callback returns. Everything a sender was admitted
/// must arrive exactly once (in order on the ordered channel), and the session must come to rest afterwards.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class ReviewResumeThreadingSessionTests
{
    private const ushort Ordered = 4;
    private const ushort Groups = 5;

    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(Ordered, "ordered", ChannelMode.ReliableOrdered)
        .Add(Groups, "groups", ChannelMode.ReliableUnordered)
        .Build();

    private sealed class AcceptAll : IPeerAdmission
    {
        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }

    private sealed class TrackingConnector(MsQuicTransportHarness harness, MsQuicTransportConnector inner) : ITransportConnector
    {
        public MsQuicTransport? Transport;

        public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) =>
            Transport = harness.Track(inner.Connect(endpoint, serverName, sink));
    }

    /// <summary>One peer and the game thread that owns it.</summary>
    private sealed class Endpoint(string name, int count, int seed, bool busy)
    {
        private readonly Random _random = new(seed);
        private readonly bool[] _groupSeen = new bool[count];
        private readonly byte[] _scratch = new byte[2048];
        private int _orderedSent;
        private int _groupsSent;
        private int _orderedNext;

        public QuiclyPeer? Peer;
        public volatile bool Stop;
        public volatile string? Failure;
        public int OrderedReceived;
        public int GroupsReceived;
        public volatile bool SentAll;
        public volatile bool Quiet;
        public long Polls;

        public string Name => name;

        public int Received => Volatile.Read(ref OrderedReceived) + Volatile.Read(ref GroupsReceived);

        public bool ReceivedAll => Volatile.Read(ref OrderedReceived) == count && Volatile.Read(ref GroupsReceived) == count;

        public void Register()
        {
            Peer!.RegisterHandler(Ordered, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
            {
                if (!Check(payload, out int sequence)) return;
                if (sequence != _orderedNext) Failure ??= $"{name}: ordered message {sequence} arrived where {_orderedNext} was expected";
                _orderedNext = sequence + 1;
                Interlocked.Increment(ref OrderedReceived);
            });
            Peer.RegisterHandler(Groups, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
            {
                if (!Check(payload, out int sequence)) return;
                if ((uint)sequence >= (uint)count)
                {
                    Failure ??= $"{name}: group message {sequence} is out of range";
                    return;
                }

                if (_groupSeen[sequence]) Failure ??= $"{name}: group message {sequence} arrived twice";
                _groupSeen[sequence] = true;
                Interlocked.Increment(ref GroupsReceived);
            });
        }

        private bool Check(ReadOnlySpan<byte> payload, out int sequence)
        {
            sequence = -1;
            if (payload.Length < 8)
            {
                Failure ??= $"{name}: a message of {payload.Length} bytes arrived";
                return false;
            }

            sequence = BinaryPrimitives.ReadInt32LittleEndian(payload);
            int length = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(4));
            if (length != payload.Length)
            {
                Failure ??= $"{name}: message {sequence} says {length} bytes but has {payload.Length}";
                return false;
            }

            for (int i = 8; i < payload.Length; i++)
            {
                if (payload[i] != (byte)((sequence * 131) + (i * 7)))
                {
                    Failure ??= $"{name}: message {sequence} is corrupt at byte {i}";
                    return false;
                }
            }

            return true;
        }

        private int Build(int sequence)
        {
            int roll = _random.Next(100);
            int length = roll < 2 ? 1500 : roll < 30 ? _random.Next(8, 40) : _random.Next(8, 400);
            Span<byte> payload = _scratch.AsSpan(0, length);
            BinaryPrimitives.WriteInt32LittleEndian(payload, sequence);
            BinaryPrimitives.WriteInt32LittleEndian(payload.Slice(4), length);
            for (int i = 8; i < length; i++) payload[i] = (byte)((sequence * 131) + (i * 7));
            return length;
        }

        private void SendBurst(ushort channel, ref int sent)
        {
            int burst = _random.Next(0, busy ? 40 : 200);
            for (int i = 0; i < burst && sent < count; i++)
            {
                int length = Build(sent);
                SendResult result = Peer!.SendCopy(new SendHeader(channel), _scratch.AsSpan(0, length));
                if (result.IsAdmitted)
                {
                    sent++;
                    continue;
                }

                if (result.Status is not (SendStatus.QueueFull or SendStatus.OutOfBuffers))
                {
                    Failure ??= $"{name}: send {sent} on channel {channel} was refused with {result.Status}";
                }

                return;
            }
        }

        public void Run()
        {
            try
            {
                QuiclyPeer peer = Peer!;
                while (!Stop)
                {
                    peer.Poll();
                    Interlocked.Increment(ref Polls);
                    if (peer.State == PeerState.Connected)
                    {
                        SendBurst(Ordered, ref _orderedSent);
                        SendBurst(Groups, ref _groupsSent);
                        SentAll = _orderedSent == count && _groupsSent == count;
                    }

                    peer.Flush();
                    Quiet = !peer.HasPendingWork;

                    // An irregular game loop: mostly busy, sometimes a frame, now and then a hitch. The busy variant is a host
                    // that polls as fast as it can (a dedicated server between ticks), with a hitch now and then.
                    int roll = _random.Next(100);
                    if (busy)
                    {
                        if (roll < 1) Thread.Sleep(_random.Next(1, 4));
                        else if (roll < 50) Thread.SpinWait(_random.Next(1, 60));
                    }
                    else if (roll < 60) Thread.Yield();
                    else if (roll < 92) Thread.Sleep(1);
                    else Thread.Sleep(_random.Next(2, 16));
                }
            }
            catch (Exception ex)
            {
                Failure ??= $"{name}: the game thread threw {ex}";
            }
        }

        public string Describe()
        {
            QuiclyPeer peer = Peer!;
            peer.GetStatistics(out PeerStatistics statistics);
            peer.GetChannelStatistics(Groups, out ChannelStatistics groups);
            string more = $", streams reset {statistics.StreamsReset}, idle timeouts {statistics.StreamIdleTimeouts}, groups channel: sent {groups.Sent}, received {groups.Received}, "
                + $"dropped {groups.Dropped}, too large {groups.ReceiveTooLarge}, out of buffers {groups.OutOfBuffers}, canceled {groups.TransportCanceled}, lost {groups.TransportLost}";
            return $"{name}: state {peer.State}, sent {_orderedSent}+{_groupsSent}, received {OrderedReceived}+{GroupsReceived}, polls {Polls}, "
                + $"pends {statistics.StreamReceivePends}, out of receive buffers {statistics.OutOfReceiveBuffers}, callback faults {statistics.CallbackFaults}, "
                + $"pending work {peer.HasPendingWork}, fault {peer.LastCallbackFault?.Message ?? "none"}" + more;
        }
    }

    [Theory]
    [InlineData(64, false)]
    [InlineData(64, true)]
    [InlineData(4, true)]
    public void Reliable_Channels_Lose_Nothing_And_Come_To_Rest_With_A_Slow_Receiver_Over_MsQuic(int ring, bool busy)
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        int count = int.TryParse(Environment.GetEnvironmentVariable("QUICLY_REVIEW_RESUME_MESSAGES"), out int configured) ? configured : 20_000;
        MsQuicTransportHarness harness = new();
        Endpoint client = new("client", count, 101, busy);
        Endpoint server = new("server", count, 202, busy);
        void Small(PeerOptions options)
        {
            options.ReceiveRingCapacity = ring;
            options.ReceiveBudgetBytes = 24 * 1024;
            options.HeartbeatTimeout = TimeSpan.FromSeconds(30);
        }

        PeerOptions serverOptions = new();
        Small(serverOptions);
        PeerOptions clientOptions = new();
        Small(clientOptions);
        // The transports grant nothing beyond the control stream: the sessions raise the limits to what their receive records
        // hold once admitted. (A transport that admits more group streams than the session has records for makes the group
        // engine reset the surplus; that is the subject of fix/group-stream-receive-limit, not of this review.)
        ConformancePairOptions limits = new() { ServerPeerBidiStreams = 1, ServerPeerUnidiStreams = 0, ClientPeerBidiStreams = 0, ClientPeerUnidiStreams = 0 };
        MsQuicTransport? serverTransport = null;
        using ManualResetEventSlim accepted = new();
        MsQuicTransportListener listener = harness.StartListener(harness.ServerOptions(limits), static (in NewConnectionInfo _) => PreHandshakeDecision.Accept,
            (ITransport transport, in NewConnectionInfo info) =>
            {
                serverTransport = harness.Track((MsQuicTransport)transport);
                QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, Table, serverOptions, new AcceptAll());
                server.Peer = peer;
                server.Register();
                accepted.Set();
                return peer.TransportSink;
            });
        TrackingConnector connector = new(harness, harness.CreateConnector(harness.ClientOptions(limits)));
        client.Peer = QuiclyPeer.Connect(connector, listener.LocalEndPoint, "localhost", Table, clientOptions);
        client.Register();
        Assert.True(accepted.Wait(TimeSpan.FromSeconds(10)), "the listener never accepted the connection");
        listener.Stop();

        Thread clientThread = new(client.Run) { IsBackground = true, Name = "review-client-game" };
        Thread serverThread = new(server.Run) { IsBackground = true, Name = "review-server-game" };
        clientThread.Start();
        serverThread.Start();
        string? failure = null;
        try
        {
            long lastProgress = Stopwatch.GetTimestamp();
            int lastReceived = -1;
            while (!(client.ReceivedAll && server.ReceivedAll && client.SentAll && server.SentAll))
            {
                failure = client.Failure ?? server.Failure;
                if (failure is not null) break;
                int received = client.Received + server.Received;
                long now = Stopwatch.GetTimestamp();
                if (received != lastReceived)
                {
                    lastReceived = received;
                    lastProgress = now;
                }
                else if (now - lastProgress > 15 * Stopwatch.Frequency)
                {
                    failure = "STALL: nothing was received for fifteen seconds although both game threads kept polling. "
                        + client.Describe() + " | " + server.Describe()
                        + $" | streams open: client {connector.Transport?.OpenStreamCount}, server {serverTransport?.OpenStreamCount}";
                    break;
                }

                Thread.Sleep(5);
            }

            if (failure is null)
            {
                // Everything arrived. The session must come to rest: no pended stream left, no work for a Poll, and every
                // group stream closed (what stays open is the control stream and one ordered stream per direction).
                long deadline = Stopwatch.GetTimestamp() + (10 * Stopwatch.Frequency);
                bool rest = false;
                while (Stopwatch.GetTimestamp() < deadline)
                {
                    if (client.Quiet && server.Quiet && connector.Transport!.OpenStreamCount <= 3 && serverTransport!.OpenStreamCount <= 3)
                    {
                        rest = true;
                        break;
                    }

                    Thread.Sleep(5);
                }

                failure = client.Failure ?? server.Failure;
                if (failure is null && !rest)
                {
                    failure = "NOT AT REST ten seconds after the last message: " + client.Describe() + " | " + server.Describe()
                        + $" | streams open: client {connector.Transport?.OpenStreamCount}, server {serverTransport?.OpenStreamCount}";
                }
            }
        }
        finally
        {
            client.Stop = true;
            server.Stop = true;
            clientThread.Join(TimeSpan.FromSeconds(10));
            serverThread.Join(TimeSpan.FromSeconds(10));
        }

        client.Peer.GetStatistics(out PeerStatistics clientStatistics);
        server.Peer!.GetStatistics(out PeerStatistics serverStatistics);
        long pends = clientStatistics.StreamReceivePends + serverStatistics.StreamReceivePends;
        client.Peer.Dispose();
        server.Peer.Dispose();
        harness.Dispose();
        Assert.True(failure is null, failure);
        Assert.True(pends > 0, "no stream was ever held: the test did not exercise the resume path");
        Assert.Equal(0, clientStatistics.CallbackFaults + serverStatistics.CallbackFaults);
        Assert.Null(harness.CleanupError);
        Assert.Equal(0, harness.SinkExceptionTotal);
        Console.WriteLine($"review-resume-threading session (ring {ring}, busy {busy}): {4 * count} messages, {pends} holds");
    }
}
