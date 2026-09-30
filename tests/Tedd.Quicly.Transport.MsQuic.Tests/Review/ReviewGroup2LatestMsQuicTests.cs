using System.Diagnostics;
using System.Net;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Second adversarial review of fix/group-stream-receive-limit (lens: honesty of the whole fix end to end), over real
/// MsQuic loopback. One load for every case: two ReliableUnordered channels, a ReliableOrdered channel and a ReliableLatest
/// channel with small values and one large value (it needs a stream), sent to a receiver that is repeatedly late with its
/// Poll by hundreds of milliseconds, long enough in the last hitch for the sender to run into whatever bounds the streams.
/// What must hold: every message the sender completed as Delivered arrived exactly once, the receiver reset no stream, no
/// transport refused a peer stream, every latest key ends at its last value with no value Failed, and the stream tables
/// return to idle.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class ReviewGroup2LatestMsQuicTests
{
    private const ushort Ordered = 10;
    private const ushort GroupA = 11;
    private const ushort GroupB = 12;
    private const ushort Latest = 13;
    private const ulong LargeKey = 100;
    private const int LargeBytes = 20_000;

    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(Ordered, "chat", ChannelMode.ReliableOrdered)
        .Add(GroupA, "events", ChannelMode.ReliableUnordered)
        .Add(GroupB, "effects", ChannelMode.ReliableUnordered)
        .Add(Latest, "state", ChannelMode.ReliableLatest)
        .Build();

    private sealed class TrackingConnector(MsQuicTransportHarness harness, ITransportConnector inner, Action<MsQuicTransport> seen) : ITransportConnector
    {
        public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
        {
            MsQuicTransport transport = harness.Track((MsQuicTransport)inner.Connect(endpoint, serverName, sink));
            seen(transport);
            return transport;
        }
    }

    private sealed class AcceptAll : IPeerAdmission
    {
        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }

    /// <summary>What one run observed.</summary>
    private sealed class Report
    {
        public int GroupSent;
        public int GroupReceived;
        public int GroupDuplicates;
        public int GroupDelivered;
        public int GroupDeliveredButMissing;
        public int GroupFailed;
        public int OrderedSent;
        public int OrderedReceived;
        public bool OrderedInOrder = true;
        public int OrderedFailed;
        public int LatestValuesSet;
        public int LatestFailed;
        public int LatestKeysAtLastValue;
        public int LatestKeys;
        public int LargeSet;
        public int LargeFailed;
        public DeliveryStatus LastLarge;
        public bool LargeArrived;
        public long ReceiverStreamsReset;
        public long SenderStreamsReset;
        public long ReceiverCallbackFaults;
        public long SenderCallbackFaults;
        public long ReceiverRefusedPeerStreams = -1;
        public long SenderRefusedPeerStreams = -1;
        public int ReceiverOpenStreams = -1;
        public int SenderOpenStreams = -1;
        public int HitchesCutShort;
        public long LongestHitchMs;
        public bool Settled;
        public string Stuck = string.Empty;

        public override string ToString() =>
            $"groups: sent {GroupSent}, received {GroupReceived}, duplicates {GroupDuplicates}, Delivered {GroupDelivered}, Delivered but never received {GroupDeliveredButMissing}, Failed {GroupFailed}; "
            + $"ordered: sent {OrderedSent}, received {OrderedReceived}, in order {OrderedInOrder}, Failed {OrderedFailed}; "
            + $"latest: {LatestValuesSet} small values set, {LatestFailed} Failed, {LatestKeysAtLastValue} of {LatestKeys} keys at their last value; "
            + $"large value: set {LargeSet} times, {LargeFailed} Failed, last one {LastLarge}, arrived {LargeArrived}; "
            + $"receiver: StreamsReset {ReceiverStreamsReset}, CallbackFaults {ReceiverCallbackFaults}, RefusedPeerStreamCount {ReceiverRefusedPeerStreams}, open streams {ReceiverOpenStreams}; "
            + $"sender: StreamsReset {SenderStreamsReset}, CallbackFaults {SenderCallbackFaults}, RefusedPeerStreamCount {SenderRefusedPeerStreams}, open streams {SenderOpenStreams}; "
            + $"hitches cut short by the sender's own queue {HitchesCutShort}, longest hitch {LongestHitchMs} ms, settled {Settled}{Stuck}";
    }

    // ------------------------------------------------------------------ MsQuic

    /// <summary>Server → late client and client → late server with the library's default transport options.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Default_Options_A_Late_Receiver_Loses_Nothing(bool clientIsReceiver)
    {
        Report report = RunMsQuic(clientIsReceiver, clientGrant: null, maxStreams: null, lastHitchGroups: 1_400);
        AssertHonest(report, expectIdleStreams: true);
    }

    /// <summary>
    /// <see cref="MsQuicTransportOptions.ClientPeerUnidiStreamCount"/> set below the session's stream limit (21 for this
    /// table): the session's own limit is then what the client grants after admission.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_Small_Explicit_Client_Grant_A_Late_Receiver_Loses_Nothing(bool clientIsReceiver)
    {
        Report report = RunMsQuic(clientIsReceiver, clientGrant: 4, maxStreams: null, lastHitchGroups: 400);
        AssertHonest(report, expectIdleStreams: true);
    }

    /// <summary>The largest grant the session supports, with a stream table raised to hold it on both ends.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_Grant_Of_4096_With_MaxStreams_Raised_A_Late_Receiver_Loses_Nothing(bool clientIsReceiver)
    {
        Report report = RunMsQuic(clientIsReceiver, clientGrant: 4096, maxStreams: 8192, lastHitchGroups: 4_600);
        AssertHonest(report, expectIdleStreams: true);
    }

    /// <summary>
    /// An application that set <see cref="MsQuicTransportOptions.ClientPeerUnidiStreamCount"/> to 4 096 (the most a session
    /// uses) and did not raise <see cref="MsQuicTransportOptions.MaxStreams"/>: left at the new default of 2 048, or at the
    /// previous default of 1 024 that an upgraded configuration may carry explicitly. Nothing rejects the combination; the
    /// transport writes one warning to <c>Diagnostic</c>. A late client is then sent more server streams than its table has
    /// slots for, the transport refuses them below the session, and their messages, which the server completed as
    /// Delivered, are lost with <c>StreamsReset</c> still 0.
    /// </summary>
    [Theory]
    [InlineData(4096, 0)]
    [InlineData(4096, 1024)]
    [InlineData(0, 1024)]
    public void A_Client_Grant_Above_The_Room_Of_Its_Stream_Table_Loses_Nothing(int grant, int maxStreams)
    {
        // grant 0: the default grant of 1 024, next to a table an upgraded configuration still sets to the previous default.
        Report report = RunMsQuic(clientIsReceiver: true, clientGrant: grant == 0 ? null : (ushort)grant, maxStreams: maxStreams == 0 ? null : maxStreams,
            lastHitchGroups: grant == 0 ? 1_400 : 4_600);
        AssertHonest(report, expectIdleStreams: true);
    }

    // ------------------------------------------------------------------ WebTransport carrier

    /// <summary>The same load through the WebTransport carrier over real MsQuic (the connector raises the inner client grant by HTTP/3's three streams).</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void WebTransport_Default_Options_A_Late_Receiver_Loses_Nothing(bool clientIsReceiver)
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using MsQuicTransportHarness harness = new();
        QuiclyPeer? server = null;
        QuiclyPeer? client = null;
        WebTransportListener? listener = null;
        WebTransportConnector? connector = null;
        try
        {
            AcceptAll admission = new();
            PeerOptions serverOptions = Options(receiver: !clientIsReceiver);
            listener = WebTransportListener.CreateMsQuic(new IPEndPoint(IPAddress.Loopback, 0), harness.Certificate, new MsQuicTransportOptions(), null, harness.Registration);
            listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo info) =>
            {
                QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, Table, serverOptions, admission);
                Volatile.Write(ref server, peer);
                return peer.TransportSink;
            });
            connector = WebTransportConnector.CreateMsQuic(new MsQuicTransportOptions
            {
                ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
                PinnedSpkiSha256 = [harness.Pin],
            }, null, harness.Registration);
            client = QuiclyPeer.Connect(connector, listener.LocalEndPoint, "localhost", Table, Options(receiver: clientIsReceiver));
            QuiclyPeer connecting = client;
            Assert.True(Pump(() => connecting.State == PeerState.Connected && Volatile.Read(ref server)?.State == PeerState.Connected, 15_000, connecting, () => Volatile.Read(ref server)),
                "the WebTransport session did not connect");

            Report report = Drive(clientIsReceiver ? server! : client, clientIsReceiver ? client : server!, lastHitchGroups: 1_400);
            AssertHonest(report, expectIdleStreams: false);
        }
        finally
        {
            client?.Dispose();
            Volatile.Read(ref server)?.Dispose();
            connector?.Dispose();
            listener?.Dispose();
        }
    }

    // ------------------------------------------------------------------ the run

    private static Report RunMsQuic(bool clientIsReceiver, ushort? clientGrant, int? maxStreams, int lastHitchGroups)
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using MsQuicTransportHarness harness = new();
        QuiclyPeer? server = null;
        QuiclyPeer? client = null;
        MsQuicTransport? serverTransport = null;
        MsQuicTransport? clientTransport = null;
        try
        {
            AcceptAll admission = new();
            PeerOptions serverOptions = Options(receiver: !clientIsReceiver);
            MsQuicTransportOptions serverTransportOptions = new();
            if (maxStreams is int serverTable)
            {
                serverTransportOptions.MaxStreams = serverTable;
            }

            MsQuicTransportListener listener = harness.StartListener(serverTransportOptions, static (in NewConnectionInfo _) => PreHandshakeDecision.Accept,
                (ITransport transport, in NewConnectionInfo info) =>
                {
                    Volatile.Write(ref serverTransport, harness.Track((MsQuicTransport)transport));
                    QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, Table, serverOptions, admission);
                    Volatile.Write(ref server, peer);
                    return peer.TransportSink;
                });

            MsQuicTransportOptions clientTransportOptions = new()
            {
                ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
                PinnedSpkiSha256 = [harness.Pin],
            };
            if (clientGrant is ushort grant)
            {
                clientTransportOptions.ClientPeerUnidiStreamCount = grant;
            }

            if (maxStreams is int clientTable)
            {
                clientTransportOptions.MaxStreams = clientTable;
            }

            client = QuiclyPeer.Connect(new TrackingConnector(harness, harness.CreateConnector(clientTransportOptions), transport => clientTransport = transport),
                listener.LocalEndPoint, "localhost", Table, Options(receiver: clientIsReceiver));
            QuiclyPeer connecting = client;
            Assert.True(Pump(() => connecting.State == PeerState.Connected && Volatile.Read(ref server)?.State == PeerState.Connected, 15_000, connecting, () => Volatile.Read(ref server)),
                "the session did not connect");

            QuiclyPeer sender = clientIsReceiver ? server! : client;
            QuiclyPeer receiver = clientIsReceiver ? client : server!;
            Report report = Drive(sender, receiver, lastHitchGroups);

            MsQuicTransport receiverTransport = clientIsReceiver ? clientTransport! : serverTransport!;
            MsQuicTransport senderTransport = clientIsReceiver ? serverTransport! : clientTransport!;

            // Idle: the control stream and the ordered channel's one stream are all either end still holds.
            Pump(() => receiverTransport.OpenStreamCount <= 2 && senderTransport.OpenStreamCount <= 2, 5_000, sender, () => receiver);
            report.ReceiverRefusedPeerStreams = receiverTransport.RefusedPeerStreamCount;
            report.SenderRefusedPeerStreams = senderTransport.RefusedPeerStreamCount;
            report.ReceiverOpenStreams = receiverTransport.OpenStreamCount;
            report.SenderOpenStreams = senderTransport.OpenStreamCount;
            if (!report.Settled)
            {
                report.Stuck = DescribeOpenPeerStreams(receiverTransport, receiver);
            }

            return report;
        }
        finally
        {
            client?.Dispose();
            Volatile.Read(ref server)?.Dispose();
        }
    }

    private static PeerOptions Options(bool receiver)
    {
        PeerOptions options = new() { GroupMinInterval = TimeSpan.Zero };
        if (receiver)
        {
            // A small ring, as in the branch's own regression test: a late Poll fills it and the streams are held back.
            options.ReceiveRingCapacity = 64;
        }

        return options;
    }

    /// <summary>
    /// Three hitches of six hundred groups each (three hundred milliseconds and more) with a short catch-up in between, then one of
    /// <paramref name="lastHitchGroups"/> groups: more than the receiver's transport admits, so the sender runs into the
    /// bound and has to wait. Then the receiver catches up for good.
    /// </summary>
    private static Report Drive(QuiclyPeer sender, QuiclyPeer receiver, int lastHitchGroups)
    {
        Report report = new();
        List<int> groupCounts = [];
        List<int> orderedGot = [];
        Dictionary<ulong, int> latestGot = [];
        MessageHandler group = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            int id = BitConverter.ToInt32(payload);
            while (groupCounts.Count <= id)
            {
                groupCounts.Add(0);
            }

            groupCounts[id]++;
        };
        receiver.RegisterHandler(GroupA, group);
        receiver.RegisterHandler(GroupB, group);
        receiver.RegisterHandler(Ordered, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => orderedGot.Add(BitConverter.ToInt32(payload)));
        receiver.RegisterHandler(Latest, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            int id = BitConverter.ToInt32(payload);
            if (header.Key == LargeKey && payload.Length != LargeBytes)
            {
                id = -1;
            }

            latestGot[header.Key] = id;
        });

        // Tokens are harvested as they finish: a finished token's slot is reused by a later send.
        List<(SendToken Token, int Kind, int Id)> open = [];
        Dictionary<int, DeliveryStatus> groupStatus = [];
        Dictionary<ulong, int> latestSet = [];
        int nextGroupId = 0;
        int nextOrdered = 0;
        int nextValue = 1;
        SendToken lastLarge = default;
        int lastLargeId = 0;
        DeliveryStatus lastLargeStatus = DeliveryStatus.Pending;

        void Harvest()
        {
            for (int i = open.Count - 1; i >= 0; i--)
            {
                (SendToken token, int kind, int id) = open[i];
                DeliveryStatus status = sender.GetDeliveryStatus(token);
                if (status == DeliveryStatus.Pending || status == DeliveryStatus.Sent)
                {
                    continue;
                }

                open.RemoveAt(i);
                switch (kind)
                {
                    case 0:
                        groupStatus[id] = status;
                        if (status == DeliveryStatus.Delivered)
                        {
                            report.GroupDelivered++;
                        }
                        else
                        {
                            report.GroupFailed++;
                        }

                        break;
                    case 1:
                        if (status != DeliveryStatus.Delivered)
                        {
                            report.OrderedFailed++;
                        }

                        break;
                    case 2:
                        if (status is not (DeliveryStatus.Delivered or DeliveryStatus.Superseded))
                        {
                            report.LatestFailed++;
                        }

                        break;
                    default:
                        if (status is not (DeliveryStatus.Delivered or DeliveryStatus.Superseded))
                        {
                            report.LargeFailed++;
                        }

                        if (id == lastLargeId)
                        {
                            lastLargeStatus = status;
                        }

                        break;
                }
            }
        }

        byte[] small = new byte[8];
        byte[] large = new byte[LargeBytes];
        new Random(7).NextBytes(large);

        bool SendGroups(ushort channel)
        {
            for (int i = 0; i < 4; i++)
            {
                BitConverter.TryWriteBytes(small, nextGroupId);
                SendResult result = sender.SendCopy(new SendHeader(channel), small, SendOptions.Tracked);
                if (!result.IsAdmitted)
                {
                    return false;
                }

                open.Add((result.Token, 0, nextGroupId));
                nextGroupId++;
            }

            return true;
        }

        void SetLarge()
        {
            BitConverter.TryWriteBytes(large, nextValue);
            SendResult value = sender.SendCopy(new SendHeader(Latest, LargeKey), large, SendOptions.Tracked);
            if (value.IsAdmitted)
            {
                open.Add((value.Token, 3, nextValue));
                lastLarge = value.Token;
                lastLargeId = nextValue;
                lastLargeStatus = DeliveryStatus.Pending;
                latestSet[LargeKey] = nextValue;
                nextValue++;
                report.LargeSet++;
            }
        }

        void Hitch(int groups)
        {
            Stopwatch hitch = Stopwatch.StartNew();
            bool largeSet = false;
            for (int step = 0; step * 2 < groups; step++)
            {
                Harvest();
                if (!SendGroups(GroupA) || !SendGroups(GroupB))
                {
                    // The sender's own queue is full: it has run into the bound and is waiting for stream credit.
                    report.HitchesCutShort++;
                    break;
                }

                if ((step & 7) == 0)
                {
                    BitConverter.TryWriteBytes(small, nextOrdered);
                    SendResult ordered = sender.SendCopy(new SendHeader(Ordered), small, SendOptions.Tracked);
                    if (ordered.IsAdmitted)
                    {
                        open.Add((ordered.Token, 1, nextOrdered));
                        nextOrdered++;
                    }

                    ulong key = (ulong)((step >> 3) & 3) + 1;
                    BitConverter.TryWriteBytes(small, nextValue);
                    SendResult value = sender.SendCopy(new SendHeader(Latest, key), small, SendOptions.Tracked);
                    if (value.IsAdmitted)
                    {
                        open.Add((value.Token, 2, nextValue));
                        latestSet[key] = nextValue;
                        nextValue++;
                        report.LatestValuesSet++;
                    }
                }

                // The large value is set late in the hitch, when the receiver already holds the sender's streams.
                if (step * 2 == (groups * 3 / 4 & ~1))
                {
                    SetLarge();
                    largeSet = true;
                }

                sender.Poll();
                sender.Flush();
                Spin(700);
            }

            if (!largeSet)
            {
                SetLarge();
            }

            // The sender keeps running for a moment while the receiver is still late.
            Stopwatch tail = Stopwatch.StartNew();
            while (tail.ElapsedMilliseconds < 100)
            {
                Harvest();
                sender.Poll();
                sender.Flush();
                Spin(500);
            }

            report.LongestHitchMs = Math.Max(report.LongestHitchMs, hitch.ElapsedMilliseconds);
        }

        void CatchUp(int milliseconds)
        {
            Stopwatch watch = Stopwatch.StartNew();
            while (watch.ElapsedMilliseconds < milliseconds)
            {
                Harvest();
                sender.Poll();
                sender.Flush();
                receiver.Poll();
                receiver.Flush();
                Spin(500);
            }
        }

        for (int round = 0; round < 3; round++)
        {
            Hitch(600);
            CatchUp(150);
        }

        Hitch(lastHitchGroups);

        bool Done()
        {
            Harvest();
            if (open.Count != 0)
            {
                return false;
            }

            int received = 0;
            for (int id = 0; id < groupCounts.Count; id++)
            {
                received += groupCounts[id] > 0 ? 1 : 0;
            }

            if (received != nextGroupId || orderedGot.Count != nextOrdered)
            {
                return false;
            }

            foreach ((ulong key, int id) in latestSet)
            {
                if (!latestGot.TryGetValue(key, out int got) || got != id)
                {
                    return false;
                }
            }

            return true;
        }

        report.Settled = Pump(Done, 40_000, sender, () => receiver);
        Harvest();
        report.GroupSent = nextGroupId;
        for (int id = 0; id < nextGroupId; id++)
        {
            int count = id < groupCounts.Count ? groupCounts[id] : 0;
            report.GroupReceived += count > 0 ? 1 : 0;
            report.GroupDuplicates += count > 1 ? count - 1 : 0;
            if (count == 0 && groupStatus.TryGetValue(id, out DeliveryStatus status) && status == DeliveryStatus.Delivered)
            {
                report.GroupDeliveredButMissing++;
            }
        }

        report.OrderedSent = nextOrdered;
        report.OrderedReceived = orderedGot.Count;
        for (int i = 0; i < orderedGot.Count; i++)
        {
            report.OrderedInOrder &= orderedGot[i] == i;
        }

        report.LatestKeys = latestSet.Count;
        foreach ((ulong key, int id) in latestSet)
        {
            if (latestGot.TryGetValue(key, out int got) && got == id)
            {
                report.LatestKeysAtLastValue++;
            }
        }

        report.LargeArrived = latestSet.TryGetValue(LargeKey, out int largeId) && latestGot.TryGetValue(LargeKey, out int largeGot) && largeGot == largeId;
        report.LastLarge = open.Exists(entry => entry.Id == lastLargeId && entry.Kind == 3) ? sender.GetDeliveryStatus(lastLarge) : lastLargeStatus;
        receiver.GetStatistics(out PeerStatistics receiverStatistics);
        sender.GetStatistics(out PeerStatistics senderStatistics);
        report.ReceiverStreamsReset = receiverStatistics.StreamsReset;
        report.SenderStreamsReset = senderStatistics.StreamsReset;
        report.ReceiverCallbackFaults = receiverStatistics.CallbackFaults;
        report.SenderCallbackFaults = senderStatistics.CallbackFaults;
        return report;
    }

    private static void AssertHonest(Report report, bool expectIdleStreams)
    {
        string text = report.ToString();
        Assert.True(report.Settled, text);
        Assert.True(report.GroupReceived == report.GroupSent && report.GroupDuplicates == 0 && report.GroupDeliveredButMissing == 0 && report.GroupFailed == 0, text);
        Assert.True(report.OrderedReceived == report.OrderedSent && report.OrderedInOrder && report.OrderedFailed == 0, text);
        Assert.True(report.LatestFailed == 0 && report.LargeFailed == 0 && report.LatestKeysAtLastValue == report.LatestKeys, text);
        Assert.True(report.LargeSet == 4 && report.LargeArrived && report.LastLarge == DeliveryStatus.Delivered, text);
        Assert.True(report.ReceiverStreamsReset == 0 && report.SenderStreamsReset == 0, text);
        Assert.True(report.ReceiverCallbackFaults == 0 && report.SenderCallbackFaults == 0, text);
        if (expectIdleStreams)
        {
            Assert.True(report.ReceiverRefusedPeerStreams == 0 && report.SenderRefusedPeerStreams == 0, text);
            Assert.True(report.ReceiverOpenStreams <= 2 && report.SenderOpenStreams <= 2, text);
        }
    }

    /// <summary>What the receiver still holds when a run did not settle (reflection over the transport's stream table; diagnosis only).</summary>
    private static string DescribeOpenPeerStreams(MsQuicTransport transport, QuiclyPeer peer)
    {
        const System.Reflection.BindingFlags Any = System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic;
        System.Text.StringBuilder text = new();
        try
        {
            peer.GetStatistics(out PeerStatistics statistics);
            text.Append($"; receiver StreamReceivePends {statistics.StreamReceivePends}, StreamIdleTimeouts {statistics.StreamIdleTimeouts}, HasPendingWork {peer.HasPendingWork}");
            object? core = typeof(QuiclyPeer).GetField("_core", Any)?.GetValue(peer);
            object? pended = core?.GetType().GetProperty("PendedStreams", Any)?.GetValue(core);
            text.Append($", PendedStreams.Count {pended?.GetType().GetProperty("Count", Any)?.GetValue(pended)}");
            Array? slots = (Array?)typeof(MsQuicTransport).GetField("_slots", Any)?.GetValue(transport);
            int shown = 0;
            foreach (object? slot in slots ?? Array.Empty<object>())
            {
                if (slot is null)
                {
                    continue;
                }

                Type type = slot.GetType();
                if (type.GetField("Stream", Any)?.GetValue(slot) is null || (bool)type.GetField("Local", Any)!.GetValue(slot)!)
                {
                    continue;
                }

                if (shown++ < 6)
                {
                    text.Append($"; open peer stream slot {type.GetField("Index", Any)!.GetValue(slot)}: kind {type.GetField("Kind", Any)!.GetValue(slot)}, ReceiveState {type.GetField("ReceiveState", Any)!.GetValue(slot)} (0 idle, 1 in callback, 2 pending, 3 early resume), "
                        + $"PendingConsumed {type.GetField("PendingConsumed", Any)!.GetValue(slot)} of {type.GetField("PendingTotal", Any)!.GetValue(slot)}, CloseFlags {type.GetField("CloseFlags", Any)!.GetValue(slot)}");
                }
            }
        }
        catch (Exception exception)
        {
            text.Append($"; diagnosis failed: {exception.Message}");
        }

        return text.ToString();
    }

    private static void Spin(int microseconds)
    {
        long end = Stopwatch.GetTimestamp() + (Stopwatch.Frequency * microseconds / 1_000_000);
        while (Stopwatch.GetTimestamp() < end)
        {
            Thread.Yield();
        }
    }

    private static bool Pump(Func<bool> condition, int milliseconds, QuiclyPeer first, Func<QuiclyPeer?> second)
    {
        Stopwatch watch = Stopwatch.StartNew();
        while (true)
        {
            first.Poll();
            first.Flush();
            if (second() is { } peer)
            {
                peer.Poll();
                peer.Flush();
            }

            if (condition())
            {
                return true;
            }

            if (watch.ElapsedMilliseconds > milliseconds)
            {
                return false;
            }

            Thread.Sleep(1);
        }
    }
}
