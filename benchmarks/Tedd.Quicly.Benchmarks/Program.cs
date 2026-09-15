using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Tedd.Quicly.Benchmarks;

public static class Program
{
    public static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, CreateConfig());

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
