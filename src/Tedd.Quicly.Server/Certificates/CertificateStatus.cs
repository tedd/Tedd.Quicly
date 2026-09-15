namespace Tedd.Quicly.Server.Certificates;

/// <summary>The lifecycle state of a <see cref="CertificateProvisioner"/>.</summary>
public enum CertificateState
{
    /// <summary><see cref="CertificateProvisioner.StartAsync"/> is running: endpoints are starting and the first certificate is being loaded or ordered.</summary>
    Starting,

    /// <summary>A certificate is being served and the last attempt to obtain one succeeded.</summary>
    Valid,

    /// <summary>A replacement certificate is being ordered; the current one (if any) keeps being served.</summary>
    Renewing,

    /// <summary>
    /// The last attempt to obtain or load a certificate failed (<see cref="CertificateStatus.Reason"/> says why). The
    /// provisioner keeps retrying in the background, and a previous certificate, if any, keeps being served.
    /// </summary>
    Failed,

    /// <summary>Not running: before <see cref="CertificateProvisioner.StartAsync"/> or after <see cref="CertificateProvisioner.StopAsync"/>.</summary>
    Stopped,
}

/// <summary>A <see cref="CertificateProvisioner"/> status: the state plus, where useful, a human-readable reason and the exception behind a failure.</summary>
/// <param name="State">The state.</param>
/// <param name="Reason">Why the state was entered (never contains secrets), or <see langword="null"/>.</param>
/// <param name="Error">The exception behind a <see cref="CertificateState.Failed"/> status, or <see langword="null"/>.</param>
public sealed record CertificateStatus(CertificateState State, string? Reason = null, Exception? Error = null)
{
    /// <summary>The status of a provisioner that is not running.</summary>
    public static CertificateStatus Stopped { get; } = new(CertificateState.Stopped);

    /// <summary>Creates a <see cref="CertificateState.Failed"/> status.</summary>
    public static CertificateStatus Failed(string reason, Exception? error = null) => new(CertificateState.Failed, reason, error);

    /// <inheritdoc/>
    public override string ToString() => Reason is null ? State.ToString() : State + ": " + Reason;
}
