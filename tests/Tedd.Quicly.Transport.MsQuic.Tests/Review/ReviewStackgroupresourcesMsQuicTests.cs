using System.Diagnostics;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Third adversarial review of the group-stream fix (0ee6b8e), lens "group-resources", over real MsQuic loopback. The fix
/// makes a late receiver hold every group stream its transport admits (an MsQuic client: 1 024) instead of resetting the
/// ones beyond MaxGroups. Held streams keep their bytes unread inside MsQuic, and those bytes count against the connection's
/// flow-control window (16 MiB by default). These tests look at what that does to the resources the session shares between
/// streams: the receive budget, the receive ring, and the connection's flow-control credit.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class ReviewStackgroupresourcesMsQuicTests
{
    private const ushort Small = 11;
    private const ushort Big = 12;
    private const ushort Ordered = 10;

    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
        .Add(Ordered, "stream", ChannelMode.ReliableOrdered)
        .Add(Small, "small", ChannelMode.ReliableUnordered)
        .Add(Big, "big", ChannelMode.ReliableUnordered, o => o.MaxMessageSize = 200 * 1024)
        .Build();

    private sealed class AcceptAll : IPeerAdmission
    {
        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }

    /// <summary>
    /// The failing case. A client with the library's default options (receive budget 256 KiB over the compact pool, whose
    /// one 256 KiB block is what any message above 64 KiB rents; MsQuic connection window 16 MiB) is late with its Poll while
    /// its server sends 30 KiB and 70 KiB group messages. Because the client now holds every group stream MsQuic admits
    /// (1 024) instead of resetting the ones beyond MaxGroups, the held streams' unread bytes use up the whole connection
    /// window. When it polls again, a 70 KiB message whose tail had not arrived yet is staged and holds the entire receive
    /// budget; every other stream is held back for the budget, so nothing is read, so MsQuic raises no connection credit,
    /// so the tail never arrives. The receive path of the whole connection (every channel) stops until StreamIdleTimeout
    /// (30 s) gives up that message, which the sender then reports Failed; then the next one can do the same.
    /// Before the fix the client reset the streams beyond MaxGroups (the silent loss the fix removed), which also freed
    /// their flow-control credit, so the window could not fill with held streams.
    /// </summary>
    [Fact]
    public void A_Late_Client_Sent_More_Than_Its_Connection_Window_Catches_Up_Without_Stalling()
    {
        string report = Run(clientIsReceiver: true, smallBytes: 30 * 1024, bigBytes: 70 * 1024, bigEvery: 2, clientConnWindow: null, lateFor: TimeSpan.FromSeconds(3),
            catchUp: CatchUp(), out bool ok);
        Trace(report);
        Assert.True(ok, report);
    }

    /// <summary>
    /// Control for the failing case: the same load and the same session options, with only the client's MsQuic connection
    /// window raised above what the sender can put in flight (256 MiB). It catches up in a couple of seconds, which pins
    /// the stall on connection flow-control credit that held streams keep.
    /// </summary>
    [Fact]
    public void Control_The_Same_Load_Catches_Up_When_The_Connection_Window_Is_Larger_Than_What_The_Held_Streams_Buffer()
    {
        string report = Run(clientIsReceiver: true, smallBytes: 30 * 1024, bigBytes: 70 * 1024, bigEvery: 2, clientConnWindow: 256u * 1024 * 1024,
            lateFor: TimeSpan.FromSeconds(3), catchUp: CatchUp(), out bool ok);
        Trace(report);
        Assert.True(ok, report);
    }

    /// <summary>
    /// Guards: loads that also exceed the connection window, with messages whose staging leaves room in the budget or
    /// rarer large ones, and the client-to-server direction (whose server grants only the session limit, 17 streams). All
    /// catch up without a stall, loss or duplicate.
    /// </summary>
    [Theory]
    [InlineData(true, 8 * 1024, 100 * 1024, 48)]
    [InlineData(false, 8 * 1024, 100 * 1024, 48)]
    [InlineData(true, 4 * 1024, 150 * 1024, 4)]
    public void Guard_A_Late_Receiver_Sent_More_Than_The_Connection_Window_Catches_Up(bool clientIsReceiver, int smallBytes, int bigBytes, int bigEvery)
    {
        string report = Run(clientIsReceiver, smallBytes, bigBytes, bigEvery, clientConnWindow: null, lateFor: TimeSpan.FromSeconds(3),
            catchUp: CatchUp(), out bool ok);
        Trace(report);
        Assert.True(ok, report);
    }

    private static TimeSpan CatchUp() =>
        TimeSpan.FromSeconds(int.TryParse(Environment.GetEnvironmentVariable("QUICLY_REVIEW_CATCHUP_SECONDS"), out int s) ? s : 20);

    private static string Run(bool clientIsReceiver, int smallBytes, int bigBytes, int bigEvery, uint? clientConnWindow, TimeSpan lateFor, TimeSpan catchUp, out bool ok)
    {
        using MsQuicTransportHarness harness = new();
        QuiclyPeer? server = null;
        AcceptAll admission = new();
        // The receiver keeps the library defaults; the sender gets a pool and a send budget that let it keep the link busy.
        PeerOptions serverOptions = clientIsReceiver ? RoomySender() : new PeerOptions { GroupMinInterval = TimeSpan.Zero };
        PeerOptions clientOptions = clientIsReceiver ? new PeerOptions { GroupMinInterval = TimeSpan.Zero } : RoomySender();
        MsQuicTransportListener listener = harness.StartListener(
            new MsQuicTransportOptions(),
            static (in NewConnectionInfo _) => PreHandshakeDecision.Accept,
            (ITransport transport, in NewConnectionInfo info) =>
            {
                harness.Track((MsQuicTransport)transport);
                QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, Table, serverOptions, admission);
                Volatile.Write(ref server, peer);
                return peer.TransportSink;
            });

        MsQuicTransportConnector connector = harness.CreateConnector(new MsQuicTransportOptions
        {
            ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
            PinnedSpkiSha256 = [harness.Pin],
            ConfigureClientSettings = clientConnWindow is { } window ? settings => settings.ConnFlowControlWindow = window : null,
        });
        QuiclyPeer client = QuiclyPeer.Connect(connector, listener.LocalEndPoint, "localhost", Table, clientOptions);
        try
        {
            Assert.True(Pump(() => client.State == PeerState.Connected && Volatile.Read(ref server)?.State == PeerState.Connected, client, () => Volatile.Read(ref server)),
                "the session did not connect");
            QuiclyPeer sender = clientIsReceiver ? server! : client;
            QuiclyPeer receiver = clientIsReceiver ? client : server!;
            Dictionary<int, int> got = [];
            MessageHandler collect = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
            {
                int id = BitConverter.ToInt32(payload);
                got[id] = got.GetValueOrDefault(id) + 1;
            };
            receiver.RegisterHandler(Small, collect);
            receiver.RegisterHandler(Big, collect);
            receiver.RegisterHandler(Ordered, collect);

            // The receiver is late: only the sender is pumped, for `lateFor`. It sends as fast as its send budget admits.
            List<(int Id, SendToken Token, int Size)> sent = [];
            byte[] small = new byte[smallBytes];
            byte[] big = new byte[bigBytes];
            int next = 0;
            long sentBytes = 0;
            Dictionary<SendStatus, int> refusals = [];
            Stopwatch late = Stopwatch.StartNew();
            while (late.Elapsed < lateFor)
            {
                bool isBig = next % bigEvery == bigEvery - 1;
                byte[] payload = isBig ? big : small;
                BitConverter.TryWriteBytes(payload, next);
                ushort channel = isBig ? Big : (next % 16 == 0 ? Ordered : Small);
                SendResult result = sender.SendCopy(new SendHeader(channel), payload, SendOptions.Tracked);
                if (!result.IsAdmitted)
                {
                    refusals[result.Status] = refusals.GetValueOrDefault(result.Status) + 1;
                }
                else
                {
                    sent.Add((next, result.Token, payload.Length));
                    sentBytes += payload.Length;
                    next++;
                    if ((next & 7) != 0)
                    {
                        continue;
                    }
                }

                sender.Poll();
                sender.Flush();
                Thread.Yield();
            }

            receiver.GetStatistics(out PeerStatistics atCatchUp);
            int deliveredBefore = sent.Count(s => sender.GetDeliveryStatus(s.Token) == DeliveryStatus.Delivered);

            // The receiver comes back. On loopback the bytes take well under a second.
            Stopwatch watch = Stopwatch.StartNew();
            int lastCount = -1;
            TimeSpan lastProgress = TimeSpan.Zero;
            TimeSpan longestGap = TimeSpan.Zero;
            bool all = Pump(() =>
            {
                TimeSpan now = watch.Elapsed;
                if (got.Count != lastCount)
                {
                    longestGap = now - lastProgress > longestGap ? now - lastProgress : longestGap;
                    lastCount = got.Count;
                    lastProgress = now;
                }

                return got.Count == sent.Count;
            }, client, () => server, catchUp);
            TimeSpan took = watch.Elapsed;
            longestGap = took - lastProgress > longestGap ? took - lastProgress : longestGap;
            receiver.GetStatistics(out PeerStatistics statistics);
            sender.GetStatistics(out PeerStatistics senderStatistics);
            int delivered = sent.Count(s => sender.GetDeliveryStatus(s.Token) == DeliveryStatus.Delivered);
            int failed = sent.Count(s => sender.GetDeliveryStatus(s.Token) == DeliveryStatus.Failed);
            int deliveredMissing = sent.Count(s => sender.GetDeliveryStatus(s.Token) == DeliveryStatus.Delivered && !got.ContainsKey(s.Id));
            int duplicates = got.Values.Count(count => count > 1);
            var missing = sent.Where(s => !got.ContainsKey(s.Id)).ToList();
            string report =
                $"{(clientIsReceiver ? "server -> late client" : "client -> late server")}: sent {sent.Count} messages ({sentBytes / 1024} KiB) while the receiver was late for {lateFor.TotalSeconds:0.#} s; "
                + $"{deliveredBefore} were Delivered before it polled again (open peer streams then: pends {atCatchUp.StreamReceivePends}); "
                + $"after {took.TotalSeconds:0.0} s of catching up it received {got.Count} (missing {missing.Count}: {missing.Count(m => m.Size == bigBytes)} large, {missing.Count(m => m.Size != bigBytes)} small), "
                + $"duplicates {duplicates}, longest time without a new message {longestGap.TotalSeconds:0.0} s; the sender reports {delivered} Delivered, {failed} Failed, {deliveredMissing} Delivered but not received; "
                + $"receiver: StreamsReset {statistics.StreamsReset}, CallbackFaults {statistics.CallbackFaults}, StreamReceivePends {statistics.StreamReceivePends}, "
                + $"ReceiveBytesOutstanding {statistics.ReceiveBytesOutstanding}; sender StreamsReset {senderStatistics.StreamsReset}; "
                + $"refusals while late: {string.Join(", ", refusals.Select(r => $"{r.Value} {r.Key}"))}";
            ok = all && duplicates == 0 && failed == 0 && statistics.StreamsReset == 0 && took < TimeSpan.FromSeconds(10);
            return report;
        }
        finally
        {
            client.Dispose();
            Volatile.Read(ref server)?.Dispose();
        }
    }

    private static PeerOptions RoomySender() => new()
    {
        GroupMinInterval = TimeSpan.Zero,
        SendBudgetBytes = 64 * 1024 * 1024,
        SendTableCapacity = 16384,
        SegmentArenaCapacity = 16384,
        AllocatorOptions = new Tedd.Quicly.Core.Memory.SlabAllocatorOptions
        {
            FreeListShards = 2,
            SizeClasses =
            [
                new(64, 4096),
                new(256, 1024),
                new(1536, 1024),
                new(16384, 4096),
                new(65536, 64),
                new(262144, 256),
            ],
        },
    };

    /// <summary>Appends a line to the file named by QUICLY_REVIEW_TRACE, when it is set (for reading numbers of passing runs).</summary>
    private static void Trace(string line)
    {
        string? path = Environment.GetEnvironmentVariable("QUICLY_REVIEW_TRACE");
        if (!string.IsNullOrEmpty(path))
        {
            File.AppendAllText(path, line + Environment.NewLine);
        }
    }

    private static bool Pump(Func<bool> condition, QuiclyPeer client, Func<QuiclyPeer?> server, TimeSpan? timeout = null)
    {
        Stopwatch watch = Stopwatch.StartNew();
        TimeSpan limit = timeout ?? TimeSpan.FromSeconds(15);
        while (true)
        {
            client.Poll();
            client.Flush();
            if (server() is { } peer)
            {
                peer.Poll();
                peer.Flush();
            }

            if (condition())
            {
                return true;
            }

            if (watch.Elapsed > limit)
            {
                return false;
            }

            Thread.Sleep(1);
        }
    }
}
