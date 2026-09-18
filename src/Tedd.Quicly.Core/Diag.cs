using System.Diagnostics;

namespace Tedd.Quicly.Core;

// DIAGNOSTIC ONLY (throwaway branch perf/datagram-diag, never proposed): ablation and timing hooks selected by the QDIAG
// environment variable. Mode is static readonly, so Tier1 folds every `Diag.Mode == n` test to a constant.
internal static class Diag
{
    public static readonly int Mode = int.TryParse(Environment.GetEnvironmentVariable("QDIAG"), out int m) ? m : 0;
    public static long T0, T1, T2, N0, N1, N2, Msgs;

    static Diag()
    {
        if (Mode == 0)
        {
            return;
        }

        long a = Stopwatch.GetTimestamp();
        long overhead = 0;
        for (int i = 0; i < 100000; i++)
        {
            long x = Stopwatch.GetTimestamp();
            overhead += Stopwatch.GetTimestamp() - x;
        }

        double pairNs = overhead * 1e9 / Stopwatch.Frequency / 100000;
        AppDomain.CurrentDomain.ProcessExit += (_, _) =>
        {
            double f = 1e9 / Stopwatch.Frequency;
            double msgs = Math.Max(1, Msgs);
            Console.Error.WriteLine($"QDIAG {Mode}: msgs {Msgs} | T0 {T0 * f / msgs:F2} ns/msg over {N0} calls ({(N0 == 0 ? 0 : T0 * f / N0):F1} ns/call) | T1 {T1 * f / msgs:F2} ns/msg over {N1} calls ({(N1 == 0 ? 0 : T1 * f / N1):F1} ns/call) | T2 {T2 * f / msgs:F2} ns/msg over {N2} calls ({(N2 == 0 ? 0 : T2 * f / N2):F1} ns/call) | timestamp pair {pairNs:F1} ns");
        };
    }
}
