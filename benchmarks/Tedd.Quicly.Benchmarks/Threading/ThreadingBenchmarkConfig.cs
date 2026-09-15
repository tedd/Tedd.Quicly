using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Tedd.Quicly.Benchmarks.Threading;

/// <summary>
/// ShortRun job (1 launch, 3 warm-up, 3 measured iterations) with the memory diagnoser, executed in-process.
/// In-process because BenchmarkDotNet 0.15.8 cannot validate/spawn a <c>net11.0</c> child process (it does not
/// know the runtime moniker yet); the runtime under test is therefore the one <c>dotnet run -f</c> picked.
/// Run without <c>--job</c> on the command line, since that would add a second, out-of-process job.
/// </summary>
/// <remarks>
/// Only the job and the diagnoser are added here: BenchmarkDotNet unions an attribute config with the global
/// (default) config, so adding <c>DefaultConfig.Instance</c> again would register every exporter, logger and
/// column provider twice.
/// </remarks>
public sealed class ThreadingBenchmarkConfig : ManualConfig
{
    public ThreadingBenchmarkConfig()
    {
        AddJob(Job.ShortRun.WithToolchain(InProcessEmitToolchain.Instance).WithId("ShortRun-InProcess"));
        AddDiagnoser(MemoryDiagnoser.Default);
    }
}
