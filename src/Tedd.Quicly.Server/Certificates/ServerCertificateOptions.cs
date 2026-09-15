using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Server.Certificates;

/// <summary>Where the server certificate comes from.</summary>
public enum ServerCertificateSourceKind
{
    /// <summary>A certificate supplied by the application; it never changes.</summary>
    Static,

    /// <summary>A PKCS#12 file, optionally reloaded when it changes on disk (certbot, a secrets manager, ...).</summary>
    File,

    /// <summary>Obtained and renewed from an ACME CA.</summary>
    Acme,
}

/// <summary>
/// Chooses one of the three certificate sources a <see cref="CertificateProvisioner"/> can serve: a fixed certificate
/// (<see cref="Static"/>), a PFX file (<see cref="File"/>), or ACME (<see cref="Acme"/>). Create with the factory methods.
/// </summary>
[DebuggerDisplay("{ToString(),nq}")]
public sealed class ServerCertificateOptions
{
    /// <summary>Default polling interval for <see cref="File"/> sources with reload enabled.</summary>
    public static readonly TimeSpan DefaultReloadInterval = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Longest accepted polling interval for <see cref="File"/> sources: one day. A longer poll could notice a renewed file
    /// only after the certificate it replaces has expired.
    /// </summary>
    public static readonly TimeSpan MaxReloadInterval = TimeSpan.FromDays(1);

    private ServerCertificateOptions(ServerCertificateSourceKind kind)
    {
        Kind = kind;
    }

    /// <summary>Which source this is.</summary>
    public ServerCertificateSourceKind Kind { get; }

    /// <summary>The certificate of a <see cref="ServerCertificateSourceKind.Static"/> source.</summary>
    public X509Certificate2? Certificate { get; private init; }

    /// <summary>The PFX path of a <see cref="ServerCertificateSourceKind.File"/> source.</summary>
    public string? FilePath { get; private init; }

    /// <summary>The PFX password of a <see cref="ServerCertificateSourceKind.File"/> source. Secret: redacted from <see cref="ToString"/> and the debugger view.</summary>
    [DebuggerBrowsable(DebuggerBrowsableState.Never)]
    public string? FilePassword { get; private init; }

    /// <summary>Whether a <see cref="ServerCertificateSourceKind.File"/> source reloads the file when its modification time changes.</summary>
    public bool ReloadOnChange { get; private init; }

    /// <summary>How often a reloading <see cref="ServerCertificateSourceKind.File"/> source checks the file.</summary>
    public TimeSpan ReloadInterval { get; private init; }

    /// <summary>The configuration of an <see cref="ServerCertificateSourceKind.Acme"/> source.</summary>
    public AcmeProvisioningOptions? AcmeOptions { get; private init; }

    /// <summary>
    /// Serves <paramref name="certificate"/> unchanged. The application keeps ownership: the provisioner never disposes it.
    /// </summary>
    /// <exception cref="ArgumentException">The certificate has no private key.</exception>
    public static ServerCertificateOptions Static(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        if (!certificate.HasPrivateKey)
        {
            throw new ArgumentException("A server certificate needs its private key.", nameof(certificate));
        }

        return new ServerCertificateOptions(ServerCertificateSourceKind.Static) { Certificate = certificate };
    }

    /// <summary>
    /// Serves the PKCS#12 file at <paramref name="pfxPath"/>. With <paramref name="reloadOnChange"/> the file's modification
    /// time is polled every <paramref name="reloadInterval"/> (default <see cref="DefaultReloadInterval"/>) and a replaced
    /// file is loaded and announced through <see cref="CertificateProvisioner.Changed"/>. Replace the file atomically
    /// (write a temporary file, then rename it over the old one); a half-written file is reported and retried.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="reloadInterval"/> is not positive, or longer than <see cref="MaxReloadInterval"/>.</exception>
    public static ServerCertificateOptions File(string pfxPath, string? password = null, bool reloadOnChange = true, TimeSpan? reloadInterval = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pfxPath);
        TimeSpan interval = reloadInterval ?? DefaultReloadInterval;
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero, nameof(reloadInterval));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(interval, MaxReloadInterval, nameof(reloadInterval));
        return new ServerCertificateOptions(ServerCertificateSourceKind.File)
        {
            FilePath = pfxPath,
            FilePassword = password,
            ReloadOnChange = reloadOnChange,
            ReloadInterval = interval,
        };
    }

    /// <summary>Obtains and renews the certificate from an ACME CA.</summary>
    /// <exception cref="ArgumentException">The options are incomplete or inconsistent (the message says which).</exception>
    public static ServerCertificateOptions Acme(AcmeProvisioningOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return new ServerCertificateOptions(ServerCertificateSourceKind.Acme) { AcmeOptions = options };
    }

    /// <summary>A summary for logs; the PFX password is redacted.</summary>
    public override string ToString()
    {
        return Kind switch
        {
            ServerCertificateSourceKind.Static => "ServerCertificateOptions { Kind = Static, Certificate = " + Certificate!.Subject + " (" + Certificate.Thumbprint + ") }",
            ServerCertificateSourceKind.File => "ServerCertificateOptions { Kind = File, FilePath = " + FilePath + ", FilePassword = " + (FilePassword is null ? "(none)" : "***")
                + ", ReloadOnChange = " + ReloadOnChange + ", ReloadInterval = " + ReloadInterval + " }",
            _ => "ServerCertificateOptions { Kind = Acme, AcmeOptions = " + AcmeOptions + " }",
        };
    }
}
