using System.Diagnostics;
using Tedd.Quicly.Benchmarks.Session;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Benchmarks.Profiling;

/// <summary>
/// Stage timing of the <see cref="SessionEndToEndBench"/> cycle: the same work with a timestamp between its phases, so the
/// cost of each phase per message is measured directly instead of attributed by a sampling profiler (EventPipe sampling is
/// safe-point biased on this code; see docs/adr/0007-measurement-method.md).
/// </summary>
/// <remarks>
/// Phases, in ns per message: <c>send</c> (the SendCopy calls: admission), <c>flush</c> (client Flush: scheduler pass,
/// packing or gathering, transport submission), <c>deliver</c> (the first <c>Network.Advance(0)</c>: the simulator's
/// delivery and the server's receive callbacks), <c>spoll</c> (server Poll: dispatch), <c>ack</c> (the second
/// <c>Advance(0)</c>: send states back to the client) and <c>cpoll</c> (client Poll: completions, retirement, lease
/// returns). The seven timestamps cost about 1–2 ns per message at a batch of 100. <see cref="Create"/>, <see cref="Measure"/>
/// and <see cref="Names"/> are the entry points PairHost calls by reflection.
/// </remarks>
public static class Stages
{
    /// <summary>The six phases, then their total.</summary>
    public static readonly string[] Names = ["send", "flush", "deliver", "spoll", "ack", "cpoll", "total"];

    /// <summary>The workloads: <c>Packed</c>, <c>Keyed</c>, <c>Ordered64</c>, <c>Ordered4K</c>, <c>Loose</c>.</summary>
    public static readonly string[] Workloads = ["Packed", "Keyed", "Ordered64", "Ordered4K", "Loose"];

    /// <summary>Runs <paramref name="workload"/> for <paramref name="seconds"/> after a 3 s warm-up and prints one line.</summary>
    public static void Run(string workload, double seconds)
    {
        using var cycle = (Cycle)Create(workload);
        Measure(cycle, 3);
        double[] r = Measure(cycle, seconds);
        var parts = new List<string>();
        for (int i = 0; i < 6; i++)
        {
            parts.Add(FormattableString.Invariant($"{Names[i]} {r[i],6:F1}"));
        }

        Console.WriteLine(FormattableString.Invariant(
            $"{Environment.Version} stages {workload,-9} ns/msg: {string.Join(" | ", parts)} | total {r[6]:F1}  (received {cycle.Received})"));
    }

    /// <summary>Connects a client/server pair for <paramref name="workload"/>.</summary>
    public static object Create(string workload) => new Cycle(workload);

    /// <summary>Runs cycles for <paramref name="seconds"/>; returns ns per message for each phase and the total.</summary>
    public static double[] Measure(object state, double seconds)
    {
        var cycle = (Cycle)state;
        long[] t = new long[6];
        long messages = 0;
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed.TotalSeconds < seconds)
        {
            cycle.Run(t);
            messages += cycle.Batch;
        }

        double toNs = 1e9 / Stopwatch.Frequency / messages;
        double[] r = new double[7];
        for (int i = 0; i < 6; i++)
        {
            r[i] = t[i] * toNs;
            r[6] += r[i];
        }

        return r;
    }

    private sealed class Cycle : IDisposable
    {
        private readonly SessionFixture _fixture;
        private readonly QuiclyPeer _client;
        private readonly QuiclyPeer _server;
        private readonly SimulatedNetwork _network;
        private readonly ushort _channel;
        private readonly bool _keyed;
        private readonly byte[] _payload;

        public Cycle(string workload)
        {
            _fixture = new SessionFixture(1);
            _client = _fixture.Clients[0];
            _server = _fixture.Servers[0];
            _network = _fixture.Network;
            MessageHandler count = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => Received++;
            _server.RegisterHandler(2, count);
            _server.RegisterHandler(3, count);
            _server.RegisterHandler(4, count);
            byte[] small = new byte[64], large = new byte[4096];
            (_channel, _keyed, _payload, Batch) = workload switch
            {
                "Packed" => ((ushort)2, false, small, 100),
                "Keyed" => ((ushort)3, true, small, 100),
                "Ordered64" => ((ushort)4, false, small, 100),
                "Ordered4K" => ((ushort)4, false, large, 16),
                "Loose" => ((ushort)2, false, small, 1),
                _ => throw new ArgumentException($"Unknown stages workload '{workload}'; use one of {string.Join(", ", Workloads)}.", nameof(workload)),
            };
        }

        public int Batch { get; }

        public long Received { get; private set; }

        public void Run(long[] t)
        {
            long a = Stopwatch.GetTimestamp();
            if (_keyed)
            {
                for (int i = 0; i < Batch; i++)
                {
                    _client.SendCopy(new SendHeader(_channel, (ulong)i), _payload);
                }
            }
            else
            {
                for (int i = 0; i < Batch; i++)
                {
                    _client.SendCopy(new SendHeader(_channel), _payload);
                }
            }

            long b = Stopwatch.GetTimestamp();
            _client.Flush();
            long c = Stopwatch.GetTimestamp();
            _network.Advance(0);
            long d = Stopwatch.GetTimestamp();
            _server.Poll();
            long e = Stopwatch.GetTimestamp();
            _network.Advance(0);
            long f = Stopwatch.GetTimestamp();
            _client.Poll();
            long g = Stopwatch.GetTimestamp();
            t[0] += b - a;
            t[1] += c - b;
            t[2] += d - c;
            t[3] += e - d;
            t[4] += f - e;
            t[5] += g - f;
        }

        public void Dispose() => _fixture.Dispose();
    }
}
