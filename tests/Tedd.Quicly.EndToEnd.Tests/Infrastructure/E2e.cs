using Tedd.Quicly.Transport.MsQuic;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>
/// Bounds for every wait in the suite. They are deliberately generous: the suite shares the machine with other test runs,
/// so a wait fails only when something is really stuck, never because the machine is busy. Nothing sleeps for a fixed time
/// to let something happen; every wait is for an event or a condition, bounded by these.
/// </summary>
internal static class E2eTimeouts
{
    /// <summary>One network step: a QUIC handshake, an echo round trip, a TLS probe, a connection shutdown.</summary>
    public static readonly TimeSpan Step = TimeSpan.FromSeconds(30);

    /// <summary>A whole ACME order against the fake CA (account, order, real challenge validation over loopback, finalisation).</summary>
    public static readonly TimeSpan Order = TimeSpan.FromSeconds(90);

    /// <summary>
    /// A handshake that must fail. The failure normally arrives at once (a TLS alert, a refusal), but should that close be
    /// lost, the handshake idle timeout of <see cref="E2eSettings"/> (30 s) ends the handshake: the wait outlasts it.
    /// </summary>
    public static readonly TimeSpan Failure = TimeSpan.FromSeconds(75);

    /// <summary>Per-test safety net for xunit's <c>Timeout</c>, in milliseconds: far above the sum of the bounded waits in any test.</summary>
    public const int TestMilliseconds = 600_000;

    public static TaskCompletionSource<T> NewTcs<T>() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static TaskCompletionSource NewTcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Awaits <paramref name="task"/> for at most <paramref name="timeout"/>, naming what was awaited when it times out.</summary>
    public static async Task<T> Within<T>(this Task<T> task, TimeSpan timeout, string what)
    {
        try
        {
            return await task.WaitAsync(timeout).ConfigureAwait(false);
        }
        catch (TimeoutException e) when (!task.IsCompleted)
        {
            throw new TimeoutException("Timed out after " + timeout + " waiting for " + what + ".", e);
        }
    }

    /// <inheritdoc cref="Within{T}(Task{T}, TimeSpan, string)"/>
    public static async Task Within(this Task task, TimeSpan timeout, string what)
    {
        try
        {
            await task.WaitAsync(timeout).ConfigureAwait(false);
        }
        catch (TimeoutException e) when (!task.IsCompleted)
        {
            throw new TimeoutException("Timed out after " + timeout + " waiting for " + what + ".", e);
        }
    }
}

/// <summary>
/// MsQuic settings for the suite: the library's transport defaults (ARCHITECTURE §7), with the handshake, idle and
/// disconnect bounds widened so that a busy machine cannot time a loopback handshake out, and enough peer streams for the
/// echo checks (the product admits one bidirectional stream before the session Hello; there is no session layer here).
/// </summary>
internal static class E2eSettings
{
    public static MsQuicSettings Server() => new()
    {
        PeerBidiStreamCount = 16,
        HandshakeIdleTimeout = TimeSpan.FromSeconds(30),
        IdleTimeout = TimeSpan.FromSeconds(120),
        DisconnectTimeout = TimeSpan.FromSeconds(30),
    };

    public static MsQuicSettings Client()
    {
        MsQuicSettings settings = MsQuicSettings.Client();
        settings.HandshakeIdleTimeout = TimeSpan.FromSeconds(30);
        settings.IdleTimeout = TimeSpan.FromSeconds(120);
        settings.DisconnectTimeout = TimeSpan.FromSeconds(30);
        return settings;
    }
}

/// <summary>Skips tests whose prerequisites this host genuinely lacks, with the reason.</summary>
internal static class Requires
{
    /// <summary>Skips when MsQuic cannot be opened here (no library, or one older than the binding accepts).</summary>
    public static void MsQuic()
    {
        if (!MsQuicApi.TryGetInstance(out _, out int status))
        {
            Assert.Skip("MsQuic is not available on this host (opening the API table failed with " + MsQuicStatus.GetName(status)
                + "). Windows 11 / Server 2022+ (msquic.dll ships with the .NET runtime) or Linux / macOS with libmsquic 2.4+ is required.");
        }
    }
}
