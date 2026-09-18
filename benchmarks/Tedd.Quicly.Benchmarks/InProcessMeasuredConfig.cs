using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Tedd.Quicly.Benchmarks;

/// <summary>
/// Jobs with enough iterations for a meaningful confidence interval, and the memory diagnoser attached: one in process,
/// and, on a host BenchmarkDotNet can spawn children for (.NET 10), one out of process.
/// </summary>
/// <remarks>
/// <see cref="InProcessShortRunConfig"/>'s three iterations are enough to rank alternatives whose difference is large, but
/// BenchmarkDotNet's <c>Error</c> (half the 99.9 % confidence interval) is a Student-t quantile over the iteration count, and
/// with three iterations that quantile is about 31: a run whose iterations differ by a few percent reports an error close to
/// the mean itself. Thirty iterations after five warm-ups bring the quantile to about 3.7, so the published interval says
/// something. BenchmarkDotNet 0.15.x cannot spawn <c>net11.0</c> child processes, so on a .NET 11 host only the in-process
/// job runs; its results apply to the target framework the host was started with.
/// </remarks>
public sealed class InProcessMeasuredConfig : ManualConfig
{
    public InProcessMeasuredConfig()
    {
        Job measured = Job.Default.WithLaunchCount(1).WithWarmupCount(5).WithIterationCount(30);
        AddJob(measured.WithToolchain(InProcessEmitToolchain.Instance).WithId("InProcess30"));
        if (Environment.Version.Major < 11)
        {
            AddJob(measured.WithId("OutOfProcess30"));
        }

        AddDiagnoser(MemoryDiagnoser.Default);
    }
}
