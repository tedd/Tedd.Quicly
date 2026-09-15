using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// A certificate whose persisted key container this library created for the <c>CERTIFICATE_CONTEXT</c> path,
/// reference counted by the <see cref="MsQuicConfiguration"/> that loaded it (one reference, dropped by
/// <see cref="MsQuicConfiguration.Close"/>) and by every <see cref="MsQuicConnection"/> that was started or
/// configured with that configuration (one reference each, dropped by <see cref="MsQuicConnection.Close"/>).
/// </summary>
/// <remarks>
/// ADR 0009 renewal sequence: open the new configuration, swap the pointer the listener hands out, close the old
/// configuration; the old key container is deleted only after that and after every connection created with the old
/// configuration is gone. The last <see cref="Release"/> therefore deletes the container and disposes the
/// certificate. Deletion is best effort: a container that has already disappeared (deleted externally) is ignored.
/// Thread-safe.
/// </remarks>
internal sealed class MsQuicOwnedCredential
{
    private X509Certificate2? _certificate;
    private int _references = 1;

    /// <summary>Takes ownership of <paramref name="certificate"/> with one reference (the configuration's).</summary>
    public MsQuicOwnedCredential(X509Certificate2 certificate) => _certificate = certificate;

    /// <summary>The owned certificate; null once the last reference is released.</summary>
    public X509Certificate2? Certificate => Volatile.Read(ref _certificate);

    /// <summary>Current number of references (diagnostics and tests).</summary>
    public int ReferenceCount => Volatile.Read(ref _references);

    /// <summary>Adds a reference unless the credential has already been released for good.</summary>
    public bool TryAddRef()
    {
        int current = Volatile.Read(ref _references);
        while (current > 0)
        {
            int seen = Interlocked.CompareExchange(ref _references, current + 1, current);
            if (seen == current) return true;
            current = seen;
        }
        return false;
    }

    /// <summary>Drops a reference; the last one deletes the persisted key container and disposes the certificate.</summary>
    public void Release()
    {
        if (Interlocked.Decrement(ref _references) > 0) return;
        X509Certificate2? certificate = Interlocked.Exchange(ref _certificate, null);
        if (certificate is null) return;
        try
        {
            MsQuicCertificateHelper.DeletePersistedPrivateKey(certificate);
        }
        catch (CryptographicException)
        {
            // The container is already gone (e.g. deleted by an operator); there is nothing left to clean up.
        }
        finally
        {
            certificate.Dispose();
        }
    }
}
