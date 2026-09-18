using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Tedd.Quicly.Benchmarks;

/// <summary>
/// In-process job with enough iterations for a meaningful confidence interval, and the memory diagnoser attached.
/// </summary>
/// <remarks>
/// <see cref="InProcessShortRunConfig"/>'s three iterations are enough to rank alternatives whose difference is large, but
/// BenchmarkDotNet's <c>Error</c> (half the 99.9 % confidence interval) is a Student-t quantile over the iteration count, and
/// with three iterations that quantile is about 31: a run whose iterations differ by a few percent reports an error close to
/// the mean itself. Thirty iterations after five warm-ups bring the quantile to about 3.7, so the published interval says
/// something. In process for the same reason as the short-run config (BenchmarkDotNet 0.15.x cannot spawn <c>net11.0</c>
/// child processes); the results apply to the target framework the host was started with.
/// </remarks>
public sealed class InProcessMeasuredConfig : ManualConfig
{
    public InProcessMeasuredConfig()
    {
        AddJob(Job.Default
            .WithToolchain(InProcessEmitToolchain.Instance)
            .WithLaunchCount(1)
            .WithWarmupCount(5)
            .WithIterationCount(30)
            .WithId("InProcess30"));
        AddDiagnoser(MemoryDiagnoser.Default);
    }
}
