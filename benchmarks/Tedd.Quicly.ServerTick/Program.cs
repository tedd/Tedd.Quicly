using System.Reflection;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Server;
using Tedd.Quicly.Testing.Simulation;

// A game server's tick over the simulated network (virtual clock, so every number is exact and repeatable): each client sends
// one tracked ReliableLatest value per tick (8 keys round-robin, phases spread over the tick) and polls and flushes every 1 ms;
// the server calls PollAll every 1 ms and FlushAll(tick) once per tick. After a 4 s settle it measures for <secs> seconds and
// reports the senders' retransmissions per value, the time from send to Delivered at the sender (the ack's round trip), and the
// server's peer flushes per tick.
//
// Usage: ServerTick [peers=50] [one-way delay us=5000] [secs=3] [client MaxRetryBytesPerSecond=0] [tick us=16667]
// The simulator reports no congestion window, so with a retry budget of 0 the senders derive none and never retransmit: pass a
// budget (e.g. 10000000) to see retransmissions.
int peers = args.Length > 0 ? int.Parse(args[0]) : 50;
long oneWay = args.Length > 1 ? long.Parse(args[1]) : 5_000;
int seconds = args.Length > 2 ? int.Parse(args[2]) : 3;
long retryBudget = args.Length > 3 ? long.Parse(args[3]) : 0;
ChannelTable table = ChannelTable.Create()
    .Add(2, "state", ChannelMode.UnreliableUnordered)
    .Add(6, "latest", ChannelMode.ReliableLatest)
    .Build();
SlabAllocatorOptions pools = new()
{
    FreeListShards = 1,
    SizeClasses = [new(64, 8192), new(256, 2048), new(1536, 1024), new(4096, 128), new(16384, 32), new(65536, 8)],
};
VirtualClock clock = new();
SimulatedNetwork network = new(clock, 7);
SimulatedListener listener = new(network);
ServerOptions options = new() { Channels = table, ExpectedPeers = peers, MaxPeers = peers * 2 };
options.PeerOptions.Clock = clock;
options.PeerOptions.AllocatorOptions = pools;
options.Admission.MaxUnadmittedConnections = peers * 2;
QuiclyServer server = new(options, listener);
int received = 0;
server.PeerAdmitted += p => p.RegisterHandler(6, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
server.StartAsync().GetAwaiter().GetResult();
SimulatedConnector connector = new(network, new LinkOptions { DelayMicros = oneWay });
SlabAllocator clientPool = new(pools);
List<QuiclyPeer> clients = [];
for (int i = 0; i < peers; i++)
{
    PeerOptions o = new() { Clock = clock, Allocator = clientPool, SendTableCapacity = 256, ReceiveRingCapacity = 64, SegmentArenaCapacity = 64, MaxRetryBytesPerSecond = retryBudget };
    clients.Add(QuiclyPeer.Connect(connector, listener.LocalEndPoint, "game.test", table, o, null));
}

PropertyInfo flushedProperty = typeof(QuiclyServer).GetProperty("PeersFlushed", BindingFlags.Instance | BindingFlags.NonPublic)!;
long Flushed() => (long)flushedProperty.GetValue(server)!;
long TickMicros = args.Length > 4 ? long.Parse(args[4]) : 16_667;
uint tick = 0;
long nextTick = 0;
long serverTicks = 0;
Queue<(SendToken Token, long At)>[] pending = Enumerable.Range(0, peers).Select(_ => new Queue<(SendToken, long)>()).ToArray();
List<long> latencies = [];

void Step(bool send, long[] sentCount)
{
    long now = network.NowMicros;
    if (send)
    {
        for (int i = 0; i < clients.Count; i++)
        {
            long phase = i * TickMicros / clients.Count;
            if ((now - phase) % TickMicros < 1_000 && clients[i].State == PeerState.Connected)
            {
                ulong key = (ulong)(sentCount[i] % 8);
                SendResult r = clients[i].SendCopy(new SendHeader(6, key), new byte[16], SendOptions.Tracked);
                if (r.IsAdmitted)
                {
                    sentCount[i]++;
                    pending[i].Enqueue((r.Token, now));
                }
            }
        }
    }

    for (int i = 0; i < clients.Count; i++)
    {
        QuiclyPeer c = clients[i];
        c.Poll();
        c.Flush();
        while (pending[i].Count > 0)
        {
            (SendToken token, long at) = pending[i].Peek();
            DeliveryStatus status = c.GetDeliveryStatus(token);
            if (status == DeliveryStatus.Pending)
            {
                break;
            }

            pending[i].Dequeue();
            if (status == DeliveryStatus.Delivered && send)
            {
                latencies.Add(network.NowMicros - at);
            }
        }
    }

    network.Advance(1_000);
    server.PollAll();
    if (network.NowMicros >= nextTick)
    {
        server.FlushAll(++tick);
        nextTick += TickMicros;
        serverTicks++;
    }
}

long[] sent = new long[peers];
PropertyInfo rttProperty = typeof(QuiclyPeer).GetProperty("ApplicationRttMicros", BindingFlags.Instance | BindingFlags.NonPublic)!;
for (int ms = 0; ms < 4_000; ms++)
{
    Step(send: false, sent);
}

int connected = clients.Count(c => c.State == PeerState.Connected);
long retriesBefore = clients.Sum(c => c.GetChannelStatistics(6, out ChannelStatistics s) ? s.Retries : 0);
long flushedBefore = Flushed();
long ticksBefore = serverTicks;
for (int ms = 0; ms < seconds * 1_000; ms++)
{
    Step(send: true, sent);
}

for (int ms = 0; ms < 200; ms++)
{
    Step(send: false, sent);
}

long retries = clients.Sum(c => c.GetChannelStatistics(6, out ChannelStatistics s) ? s.Retries : 0) - retriesBefore;
long values = sent.Sum();
long ticks = serverTicks - ticksBefore;
long flushes = Flushed() - flushedBefore;
latencies.Sort();
double rtt = clients.Average(c => (long)rttProperty.GetValue(c)!) / 1000.0;
long P(double q) => latencies.Count == 0 ? -1 : latencies[(int)Math.Min(latencies.Count - 1, q * latencies.Count)];
Console.WriteLine($"SIM tick={TickMicros}us peers={peers} connected={connected} oneway={oneWay}us secs={seconds} values={values} received={received} " +
    $"retries={retries} retries_per_value={retries / (double)values:F3} server_flushes_per_tick={flushes / (double)ticks:F1} " +
    $"app_rtt_ms={rtt:F1} delivered={latencies.Count} ack_ms_p50={P(0.5) / 1000.0:F1} p90={P(0.9) / 1000.0:F1} max={P(1) / 1000.0:F1}");
