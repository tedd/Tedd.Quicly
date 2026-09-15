using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Tedd.Quicly.Benchmarks.Http3;

/// <summary>
/// Shared configuration for the HTTP/3 codec benchmarks: a short in-process run with the memory diagnoser.
/// The in-process toolchain is what lets the same benchmarks run under <c>net11.0</c> — BenchmarkDotNet 0.15.8
/// cannot spawn a <c>net11.0</c> child process — so the recorded numbers cover both target frameworks.
/// </summary>
public sealed class Http3BenchConfig : ManualConfig
{
    /// <summary>Creates the configuration.</summary>
    public Http3BenchConfig()
    {
        AddJob(Job.ShortRun.WithToolchain(InProcessEmitToolchain.Instance));
        AddDiagnoser(MemoryDiagnoser.Default);
    }
}
