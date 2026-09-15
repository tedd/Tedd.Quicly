using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Tedd.Quicly.Benchmarks;

/// <summary>
/// Short-run job executed in the host process with the memory diagnoser attached.
/// </summary>
/// <remarks>
/// BenchmarkDotNet 0.15.x cannot spawn <c>net11.0</c> child processes, so every benchmark class uses this
/// in-process configuration; the results then apply to whichever target framework the host was started with
/// (<c>dotnet run -f net10.0 ...</c> or <c>-f net11.0</c>).
/// </remarks>
public sealed class InProcessShortRunConfig : ManualConfig
{
    public InProcessShortRunConfig()
    {
        AddJob(Job.ShortRun.WithToolchain(InProcessEmitToolchain.Instance));
        AddDiagnoser(MemoryDiagnoser.Default);
    }
}
