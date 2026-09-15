using System.Diagnostics.CodeAnalysis;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>Where a key container persisted for the <c>CERTIFICATE_CONTEXT</c> path lives (ADR 0009).</summary>
public enum MsQuicKeyStorage
{
    /// <summary><c>PersistKeySet | UserKeySet</c>: the current user's key store (interactive processes; needs a loaded user profile).</summary>
    User = 0,
    /// <summary><c>PersistKeySet | MachineKeySet</c>: the machine key store (services, whose account often has no loaded profile).</summary>
    Machine = 1,
}

/// <summary>
/// Certificate plumbing for the two server credential paths (ADR 0009).
/// </summary>
/// <remarks>
/// <para><b>Preferred: PKCS#12 in memory.</b> <see cref="TryExportPkcs12"/> serialises the certificate with its
/// private key and MsQuic imports the blob itself (<c>QUIC_CREDENTIAL_TYPE_CERTIFICATE_PKCS12</c>): no store
/// import, no persisted key container. The OpenSSL builds of MsQuic accept it; the Schannel build bundled with
/// .NET (msquic.dll 2.5.10) answers <c>QUIC_STATUS_NOT_SUPPORTED</c>, which is why Windows falls back (see
/// <c>MsQuicConfiguration.LoadServerCredential</c>). It needs an exportable private key; certificates whose key
/// container forbids export make <see cref="TryExportPkcs12"/> return false.</para>
/// <para><b>Fallback: CERTIFICATE_CONTEXT.</b> Schannel can only sign with a key that lives in a non-ephemeral
/// container. Ephemeral CNG keys (from <c>CertificateRequest.CreateSelfSigned</c> or an
/// <see cref="X509KeyStorageFlags.EphemeralKeySet"/> import) are re-imported through PKCS#12 with
/// <see cref="X509KeyStorageFlags.PersistKeySet"/> plus <see cref="X509KeyStorageFlags.UserKeySet"/>
/// (<see cref="MsQuicKeyStorage.User"/>, interactive processes) or <see cref="X509KeyStorageFlags.MachineKeySet"/>
/// (<see cref="MsQuicKeyStorage.Machine"/>, services whose account has no loaded profile) — never
/// <c>Exportable</c> (irrelevant to Schannel, needlessly weakens the key) and never <c>EphemeralKeySet</c>
/// (ADR 0009). A persisted container outlives the certificate object; delete it with
/// <see cref="DeletePersistedPrivateKey"/> once no configuration and no connection uses it any more
/// (<see cref="MsQuicConfiguration"/> does this itself, reference counted). A persisted container is a file in the
/// user's or machine's CNG key store: if the process dies before it is deleted, it is left behind; nothing sweeps
/// such leftovers.</para>
/// </remarks>
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
    /// Returns a certificate whose private key Schannel can use through a certificate context. When the key is
    /// already persisted (or the OS is not Windows) the same instance is returned; otherwise a new instance backed
    /// by a persisted user key container is returned and the caller owns it (dispose it and, when the key is no
    /// longer needed, <see cref="DeletePersistedPrivateKey"/> it).
    /// </summary>
    public static X509Certificate2 EnsurePersistedPrivateKey(X509Certificate2 certificate, MsQuicKeyStorage storage = MsQuicKeyStorage.User)
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
        return ReimportWithPersistedKey(certificate, storage);
    }

    /// <summary>
    /// Exports to PKCS#12 and re-imports with <see cref="X509KeyStorageFlags.PersistKeySet"/> plus
    /// <see cref="X509KeyStorageFlags.UserKeySet"/> (<see cref="MsQuicKeyStorage.User"/>) or
    /// <see cref="X509KeyStorageFlags.MachineKeySet"/> (<see cref="MsQuicKeyStorage.Machine"/>); no <c>Exportable</c>,
    /// no <c>EphemeralKeySet</c>. The source key must be exportable (ephemeral CNG keys are).
    /// </summary>
    public static X509Certificate2 ReimportWithPersistedKey(X509Certificate2 certificate, MsQuicKeyStorage storage = MsQuicKeyStorage.User)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        X509KeyStorageFlags flags = storage switch
        {
            MsQuicKeyStorage.User => X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.UserKeySet,
            MsQuicKeyStorage.Machine => X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet,
            _ => throw new ArgumentOutOfRangeException(nameof(storage)),
        };
        byte[] pfx = certificate.Export(X509ContentType.Pkcs12);
        try
        {
            return X509CertificateLoader.LoadPkcs12(pfx, null, flags);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pfx);
        }
    }

    /// <summary>
    /// Exports the certificate and its private key as an unencrypted PKCS#12 blob for
    /// <c>QUIC_CREDENTIAL_TYPE_CERTIFICATE_PKCS12</c>. Returns false when the certificate has no private key or the
    /// key is not exportable. Zero the blob after use.
    /// </summary>
    public static bool TryExportPkcs12(X509Certificate2 certificate, [NotNullWhen(true)] out byte[]? pkcs12)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        pkcs12 = null;
        if (!certificate.HasPrivateKey)
        {
            return false;
        }
        try
        {
            pkcs12 = certificate.Export(X509ContentType.Pkcs12);
            return true;
        }
        catch (CryptographicException)
        {
            return false;
        }
    }

    /// <summary>
    /// Deletes the persisted CNG key container behind <paramref name="certificate"/> (Windows only). Returns true
    /// when a container was deleted; false when there is nothing to delete (no private key, ephemeral key, non-CNG
    /// key, or not Windows). Only call it after every MsQuic configuration that loaded the certificate is closed.
    /// </summary>
    public static bool DeletePersistedPrivateKey(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        if (!OperatingSystem.IsWindows() || !certificate.HasPrivateKey)
        {
            return false;
        }
        return DeletePersistedPrivateKeyWindows(certificate);
    }

    [SupportedOSPlatform("windows")]
    private static bool DeletePersistedPrivateKeyWindows(X509Certificate2 certificate)
    {
        using (ECDsa? ecdsa = certificate.GetECDsaPrivateKey())
        {
            if (ecdsa is ECDsaCng ecdsaCng)
            {
                return DeleteCngKey(ecdsaCng.Key);
            }
            if (ecdsa is not null)
            {
                return false;
            }
        }
        using (RSA? rsa = certificate.GetRSAPrivateKey())
        {
            return rsa is RSACng rsaCng && DeleteCngKey(rsaCng.Key);
        }
    }

    [SupportedOSPlatform("windows")]
    private static bool DeleteCngKey(CngKey key)
    {
        if (key.IsEphemeral)
        {
            return false;
        }
        key.Delete();
        return true;
    }
}
