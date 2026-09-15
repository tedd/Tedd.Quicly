using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// Certificate plumbing for Schannel (ADR 0006): Windows' TLS stack can only use a private key that lives in a
/// key container, so certificates whose key is ephemeral (e.g. from <c>CertificateRequest.CreateSelfSigned</c>
/// or imported with <see cref="X509KeyStorageFlags.EphemeralKeySet"/>) are re-imported through PKCS#12 into a
/// persisted, exportable key before being handed to MsQuic.
/// </summary>
public static class MsQuicCertificateHelper
{
    /// <summary>
    /// True when the certificate's private key is a CNG ephemeral key (Windows only; false elsewhere or when
    /// there is no private key).
    /// </summary>
    public static bool HasEphemeralPrivateKey(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        if (!OperatingSystem.IsWindows() || !certificate.HasPrivateKey)
        {
            return false;
        }
        return HasEphemeralPrivateKeyWindows(certificate);
    }

    [SupportedOSPlatform("windows")]
    private static bool HasEphemeralPrivateKeyWindows(X509Certificate2 certificate)
    {
        using (ECDsa? ecdsa = certificate.GetECDsaPrivateKey())
        {
            if (ecdsa is ECDsaCng ecdsaCng)
            {
                return ecdsaCng.Key.IsEphemeral;
            }
            if (ecdsa is not null)
            {
                return false;
            }
        }
        using (RSA? rsa = certificate.GetRSAPrivateKey())
        {
            if (rsa is RSACng rsaCng)
            {
                return rsaCng.Key.IsEphemeral;
            }
        }
        return false;
    }

    /// <summary>
    /// Returns a certificate whose private key Schannel can use. When the key is already persisted (or the OS
    /// is not Windows) the same instance is returned; otherwise a new instance backed by a persisted, exportable
    /// key container is returned and the caller owns (must dispose) it.
    /// </summary>
    public static X509Certificate2 EnsurePersistedPrivateKey(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        if (!certificate.HasPrivateKey)
        {
            throw new ArgumentException("The certificate has no private key.", nameof(certificate));
        }
        if (!HasEphemeralPrivateKey(certificate))
        {
            return certificate;
        }
        return ReimportWithPersistedKey(certificate);
    }

    /// <summary>Exports to PKCS#12 and re-imports with <see cref="X509KeyStorageFlags.Exportable"/> (no <c>EphemeralKeySet</c>).</summary>
    public static X509Certificate2 ReimportWithPersistedKey(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        byte[] pfx = certificate.Export(X509ContentType.Pkcs12);
        try
        {
            return X509CertificateLoader.LoadPkcs12(pfx, null, X509KeyStorageFlags.Exportable);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pfx);
        }
    }
}
