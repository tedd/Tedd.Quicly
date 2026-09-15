using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Transport.MsQuic;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>The server certificate as MsQuic indicated it to a client in the callback validation mode.</summary>
/// <param name="LeafDer">The leaf certificate, DER.</param>
/// <param name="ChainPkcs7">The chain MsQuic passed along, as a DER PKCS#7 blob (may be empty).</param>
/// <param name="DeferredStatus">The platform validation verdict (QUIC_STATUS_SUCCESS when the platform trusted the chain).</param>
/// <param name="DeferredErrorFlags">The platform validation error flags.</param>
internal sealed record PresentedCertificate(byte[] LeafDer, byte[] ChainPkcs7, int DeferredStatus, uint DeferredErrorFlags)
{
    public X509Certificate2 LoadLeaf() => X509CertificateLoader.LoadCertificate(LeafDer);

    /// <summary>The certificates of <see cref="ChainPkcs7"/> (dispose them).</summary>
    public X509Certificate2Collection LoadChain()
    {
        X509Certificate2Collection chain = new();
        if (ChainPkcs7.Length > 0)
        {
#pragma warning disable SYSLIB0057 // X509CertificateLoader has no PKCS#7 loader; the collection import is the in-box way.
            chain.Import(ChainPkcs7);
#pragma warning restore SYSLIB0057
        }

        return chain;
    }

    /// <summary>SHA-256 of the leaf's SubjectPublicKeyInfo: what ARCHITECTURE §8's PinnedSpki pins.</summary>
    public byte[] SpkiSha256()
    {
        using X509Certificate2 leaf = LoadLeaf();
        return Pins.SpkiSha256(leaf);
    }
}

internal static class Pins
{
    /// <summary>SHA-256 over the DER SubjectPublicKeyInfo.</summary>
    public static byte[] SpkiSha256(X509Certificate2 certificate) => SHA256.HashData(certificate.PublicKey.ExportSubjectPublicKeyInfo());
}

/// <summary>
/// How a test client validates the server certificate: MsQuic's platform mode, or an application validator on MsQuic's
/// callback mode (portable DER leaf and chain, platform verdict deferred to the application).
/// </summary>
internal sealed record ClientValidation(string Name, MsQuicCertificateValidation Mode, Func<PresentedCertificate, bool>? Validator)
{
    /// <summary>Platform validation against the system trust store: the product default (ARCHITECTURE §8).</summary>
    public static ClientValidation SystemRoots { get; } = new("SystemRoots", MsQuicCertificateValidation.SystemRoots, null);

    /// <summary>
    /// ARCHITECTURE §8 PinnedSpki: accept exactly the leaves whose SHA-256(SubjectPublicKeyInfo) is pinned. The pin is the
    /// trust anchor, so the platform verdict (an untrusted private root here) is ignored. The merged MsQuic layer has no pin
    /// mode of its own, so the pin runs on the callback mode, as the in-review transport's PinnedSpki mode does.
    /// </summary>
    public static ClientValidation PinnedSpki(params byte[][] pins) => new("PinnedSpki", MsQuicCertificateValidation.Callback, presented =>
    {
        byte[] actual = presented.SpkiSha256();
        return pins.Any(pin => CryptographicOperations.FixedTimeEquals(pin, actual));
    });

    /// <summary>
    /// An application validator on the callback mode: the leaf must name <paramref name="host"/> and the DER chain must build
    /// to <paramref name="root"/> (a private CA the application trusts) for server authentication.
    /// </summary>
    public static ClientValidation PrivateRoot(X509Certificate2 root, string host)
    {
        return new("Callback(private root)", MsQuicCertificateValidation.Callback, presented => ChainsTo(presented, root, host));
    }

    /// <summary>Whether <paramref name="presented"/> names <paramref name="host"/> and chains to <paramref name="root"/> alone.</summary>
    public static bool ChainsTo(PresentedCertificate presented, X509Certificate2 root, string host)
    {
        using X509Certificate2 leaf = presented.LoadLeaf();
        if (!leaf.MatchesHostname(host))
        {
            return false;
        }

        X509Certificate2Collection extra = presented.LoadChain();
        try
        {
            using X509Chain chain = new();
            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.CustomTrustStore.Add(root);
            chain.ChainPolicy.ExtraStore.AddRange(extra);
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            chain.ChainPolicy.ApplicationPolicy.Add(new Oid("1.3.6.1.5.5.7.3.1"));
            bool valid = chain.Build(leaf);
            foreach (X509ChainElement element in chain.ChainElements)
            {
                element.Certificate.Dispose();
            }

            return valid;
        }
        finally
        {
            foreach (X509Certificate2 certificate in extra)
            {
                certificate.Dispose();
            }
        }
    }
}
