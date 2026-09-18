using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Tedd.Quicly.Benchmarks;

public static class Program
{
    /// <summary>
    /// BenchmarkDotNet by default. Two profiling modes bypass it (docs/adr/0007-measurement-method.md):
    /// <c>stages &lt;Packed|Keyed|Ordered64|Ordered4K|Loose&gt; [seconds]</c> prints the per-phase cost of a session cycle, and
    /// <c>sustain &lt;Class&gt; &lt;Method&gt; [seconds] [windows]</c> runs one benchmark method in a loop (for a profiler).
    /// </summary>
    public static void Main(string[] args)
    {
        if (args.Length >= 2 && args[0] == "stages")
        {
            Profiling.Sustain.Place();
            Profiling.Stages.Run(args[1], args.Length > 2 ? double.Parse(args[2], System.Globalization.CultureInfo.InvariantCulture) : 6);
            return;
        }

        if (args.Length >= 3 && args[0] == "sustain")
        {
            Profiling.Sustain.Place();
            Profiling.Sustain.Run(
                args[1],
                args[2],
                args.Length > 3 ? double.Parse(args[3], System.Globalization.CultureInfo.InvariantCulture) : 10,
                args.Length > 4 ? int.Parse(args[4], System.Globalization.CultureInfo.InvariantCulture) : 5);
            return;
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, CreateConfig());
    }

    /// <summary>
    /// Every benchmark runs as a ShortRun job (finishes in minutes; ADR 0007). BenchmarkDotNet 0.15.8 has no
    /// runtime moniker for the .NET 11 preview and its SDK validator throws for an unrecognised host, so when
    /// the host is .NET 11 or newer the job uses the in-process toolchain; on .NET 10 it is the normal
    /// out-of-process one. Run each target with <c>dotnet run -c Release -f net10.0|net11.0 --project …</c>.
    /// </summary>
    private static IConfig CreateConfig()
    {
        Job job = Job.ShortRun;
        if (Environment.Version.Major >= 11)
            job = job.WithToolchain(InProcessEmitToolchain.Instance);
        return DefaultConfig.Instance.AddJob(job.WithId("ShortRun"));
    }
}
