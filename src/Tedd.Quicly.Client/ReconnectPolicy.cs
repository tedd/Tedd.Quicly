using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Client;

/// <summary>
/// When and how a <see cref="QuiclyClient"/> reconnects after losing its connection: bounded attempts with exponential
/// back-off and jitter, resuming the session with its last token so the server keeps the session's identity and
/// increments its epoch (PROTOCOL.md §4.1).
/// </summary>
/// <remarks>
/// A peer cannot take a new transport, so a reconnect is a new <see cref="QuiclyPeer"/> presenting the previous one's
/// session token; <see cref="QuiclyClient.Reconnected"/> hands over the new peer. The attempts are driven by
/// <see cref="QuiclyClient.Poll"/> on the clock of <see cref="PeerOptions.Clock"/>, so they need no timers.
/// </remarks>
public sealed class ReconnectPolicy
{
    /// <summary>Attempts after one lost connection before the client gives up (<see cref="QuiclyClient.Disconnected"/>); 0 never reconnects. Default 5.</summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>Delay before the first attempt. Default 250 ms.</summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Longest delay between attempts. Default 10 s.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Growth factor of the delay per attempt (at least 1). Default 2.</summary>
    public double Multiplier { get; set; } = 2.0;

    /// <summary>Random spread of each delay as a fraction of it (0 … 1): a delay d becomes d × (1 ± Jitter). Default 0.2.</summary>
    public double Jitter { get; set; } = 0.2;

    /// <summary>Present the lost connection's session token so the server resumes the session. Default <see langword="true"/>.</summary>
    public bool ResumeSession { get; set; } = true;

    /// <summary>
    /// When the server refuses the resume (<see cref="HelloStatus.Rejected"/>: the token expired or the session ended), try a
    /// fresh session instead of giving up; <see cref="ReconnectedInfo.Resumed"/> then reports the new identity. Default
    /// <see langword="true"/>.
    /// </summary>
    public bool FallBackToNewSession { get; set; } = true;

    /// <summary>Decides whether a close is worth reconnecting after; <see langword="null"/> (the default) uses <see cref="IsReconnectable"/>.</summary>
    public Func<CloseReason, bool>? ShouldReconnect { get; set; }

    /// <summary>Source of the jitter; <see langword="null"/> (the default) uses <see cref="System.Random.Shared"/>. Set a seeded instance for reproducible delays.</summary>
    public Random? Random { get; set; }

    /// <summary>
    /// The default reconnect rule: the connection was lost (closed by the transport: link loss, idle timeout) or timed out
    /// (<see cref="QuiclyErrorCode.Timeout"/>, a heartbeat on either side). Deliberate closes (goodbye, kick, shutdown,
    /// <see cref="QuiclyErrorCode.SessionReplaced"/>, refused admission) are not reconnected.
    /// </summary>
    /// <param name="reason">Why the connection closed.</param>
    /// <returns><see langword="true"/> to reconnect.</returns>
    public static bool IsReconnectable(CloseReason reason) =>
        reason.Source == CloseSource.Transport || reason.Code == QuiclyErrorCode.Timeout;

    /// <summary>
    /// The delay before attempt <paramref name="attempt"/>: <c>min(MaxDelay, InitialDelay × Multiplier^(attempt − 1))</c>, spread
    /// by <c>1 + Jitter × (2 × <paramref name="unit"/> − 1)</c> and kept within [0, <see cref="MaxDelay"/>].
    /// </summary>
    /// <param name="attempt">The attempt number, from 1.</param>
    /// <param name="unit">A value in [0, 1) that picks the jitter (0.5 = none).</param>
    /// <returns>The delay.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="attempt"/> is below 1 or <paramref name="unit"/> outside [0, 1].</exception>
    public TimeSpan GetDelay(int attempt, double unit)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        if (!(unit >= 0 && unit <= 1))
        {
            throw new ArgumentOutOfRangeException(nameof(unit), unit, "The jitter unit must be in [0, 1].");
        }

        double max = MaxDelay.Ticks;
        double grown = Math.Min(max, InitialDelay.Ticks * Math.Pow(Multiplier, attempt - 1));
        double spread = grown * (1 + (Jitter * ((2 * unit) - 1)));
        return TimeSpan.FromTicks((long)Math.Clamp(spread, 0, max));
    }

    internal bool Allows(CloseReason reason) => ShouldReconnect?.Invoke(reason) ?? IsReconnectable(reason);

    internal ReconnectPolicy Clone() => new()
    {
        MaxAttempts = MaxAttempts,
        InitialDelay = InitialDelay,
        MaxDelay = MaxDelay,
        Multiplier = Multiplier,
        Jitter = Jitter,
        ResumeSession = ResumeSession,
        FallBackToNewSession = FallBackToNewSession,
        ShouldReconnect = ShouldReconnect,
        Random = Random,
    };

    internal void Validate()
    {
        if (MaxAttempts < 0)
        {
            throw new ArgumentException("MaxAttempts must not be negative.", nameof(MaxAttempts));
        }

        if (InitialDelay < TimeSpan.Zero || MaxDelay < InitialDelay)
        {
            throw new ArgumentException("Delays must satisfy 0 <= InitialDelay <= MaxDelay.", nameof(InitialDelay));
        }

        if (!double.IsFinite(Multiplier) || Multiplier < 1)
        {
            throw new ArgumentException("Multiplier must be a finite number of at least 1.", nameof(Multiplier));
        }

        if (!(Jitter >= 0 && Jitter <= 1))
        {
            throw new ArgumentException("Jitter must be in [0, 1].", nameof(Jitter));
        }
    }
}

/// <summary>A reconnect attempt was scheduled (<see cref="QuiclyClient.Reconnecting"/>).</summary>
/// <param name="Attempt">The attempt number (from 1).</param>
/// <param name="Delay">How long the client waits before making it.</param>
/// <param name="LastReason">Why the connection (or the previous attempt) closed.</param>
public readonly record struct ReconnectingInfo(int Attempt, TimeSpan Delay, CloseReason LastReason);

/// <summary>The client is connected again (<see cref="QuiclyClient.Reconnected"/>).</summary>
/// <param name="PreviousPeer">The lost connection's peer; closed, and disposed right after the event handlers return.</param>
/// <param name="Peer">The new connection's peer (now <see cref="QuiclyClient.Peer"/>).</param>
/// <param name="Resumed">
/// <see langword="true"/> when the server resumed the session (same <see cref="QuiclyPeer.SessionId"/>, higher
/// <see cref="QuiclyPeer.Epoch"/>); <see langword="false"/> for a fresh session after the resume was refused.
/// </param>
/// <param name="Attempts">Attempts it took.</param>
public readonly record struct ReconnectedInfo(QuiclyPeer PreviousPeer, QuiclyPeer Peer, bool Resumed, int Attempts);
