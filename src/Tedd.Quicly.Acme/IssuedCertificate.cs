using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Acme;

/// <summary>
/// An issued certificate chain combined with its private key. The leaf is imported through a PKCS#12 round trip with
/// <see cref="X509KeyStorageFlags.Exportable"/> (not ephemeral) so it works with Schannel / MsQuic on Windows
/// (see ADR 0006).
/// </summary>
public sealed class IssuedCertificate : IDisposable
{
    private IssuedCertificate(X509Certificate2 certificate, X509Certificate2Collection chain, byte[] pfx)
    {
        Certificate = certificate;
        Chain = chain;
        Pfx = pfx;
    }

    /// <summary>The leaf certificate with its private key attached.</summary>
    public X509Certificate2 Certificate { get; }

    /// <summary>The full chain as issued (leaf first, then intermediates), without private keys.</summary>
    public X509Certificate2Collection Chain { get; }

    /// <summary>PKCS#12 bytes containing the private key and the chain (protected with the password given at creation, if any).</summary>
    public byte[] Pfx { get; }

    /// <summary>Expiry of the leaf certificate.</summary>
    public DateTimeOffset NotAfter => new(Certificate.NotAfter.ToUniversalTime(), TimeSpan.Zero);

    /// <summary>Combines the issued chain (leaf first) with the CSR's private key.</summary>
    /// <param name="chain">Chain as downloaded from the CA; element 0 must be the leaf.</param>
    /// <param name="privateKey">The <see cref="ECDsa"/> or <see cref="RSA"/> key the CSR was created with.</param>
    /// <param name="password">Optional password protecting <see cref="Pfx"/>.</param>
    /// <exception cref="ArgumentException">Empty chain, or the key type is unsupported / does not match the leaf.</exception>
    public static IssuedCertificate Create(X509Certificate2Collection chain, AsymmetricAlgorithm privateKey, string? password = null)
    {
        ArgumentNullException.ThrowIfNull(chain);
        ArgumentNullException.ThrowIfNull(privateKey);
        if (chain.Count == 0)
        {
            throw new ArgumentException("The chain is empty.", nameof(chain));
        }

        X509Certificate2 leaf = chain[0];
        using X509Certificate2 withKey = privateKey switch
        {
            ECDsa ecdsa => leaf.CopyWithPrivateKey(ecdsa),
            RSA rsa => leaf.CopyWithPrivateKey(rsa),
            _ => throw new ArgumentException("Only ECDsa and RSA keys are supported.", nameof(privateKey)),
        };

        X509Certificate2Collection bundle = [withKey];
        for (int i = 1; i < chain.Count; i++)
        {
            bundle.Add(chain[i]);
        }

        byte[] pfx = bundle.Export(X509ContentType.Pkcs12, password)!;
        return Load(pfx, password);
    }

    /// <summary>Loads a PKCS#12 produced by <see cref="Create"/> / <see cref="Save"/>.</summary>
    /// <exception cref="ArgumentException">The PKCS#12 contains no certificate with a private key.</exception>
    public static IssuedCertificate Load(byte[] pfx, string? password = null)
    {
        ArgumentNullException.ThrowIfNull(pfx);
        X509Certificate2Collection all = X509CertificateLoader.LoadPkcs12Collection(pfx, password, X509KeyStorageFlags.Exportable);
        X509Certificate2? leaf = null;
        X509Certificate2Collection chain = [];
        foreach (X509Certificate2 cert in all)
        {
            if (leaf is null && cert.HasPrivateKey)
            {
                leaf = cert;
            }
        }

        if (leaf is null)
        {
            foreach (X509Certificate2 cert in all)
            {
                cert.Dispose();
            }

            throw new ArgumentException("The PKCS#12 does not contain a certificate with a private key.", nameof(pfx));
        }

        chain.Add(X509CertificateLoader.LoadCertificate(leaf.RawData));
        foreach (X509Certificate2 cert in all)
        {
            if (!ReferenceEquals(cert, leaf))
            {
                chain.Add(cert);
            }
        }

        return new IssuedCertificate(leaf, chain, pfx);
    }

    /// <summary>Loads a PKCS#12 file.</summary>
    public static IssuedCertificate LoadFile(string path, string? password = null) => Load(File.ReadAllBytes(path), password);

    /// <summary>Writes <see cref="Pfx"/> to <paramref name="path"/> (atomically: temp file + rename).</summary>
    public void Save(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        path = Path.GetFullPath(path);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        string temp = path + ".tmp";
        File.WriteAllBytes(temp, Pfx);
        File.Move(temp, path, overwrite: true);
    }

    /// <summary>Exports the chain as PEM (leaf first).</summary>
    public string ExportChainPem()
    {
        System.Text.StringBuilder sb = new();
        foreach (X509Certificate2 cert in Chain)
        {
            sb.Append(cert.ExportCertificatePem()).Append('\n');
        }

        return sb.ToString();
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Certificate.Dispose();
        foreach (X509Certificate2 cert in Chain)
        {
            cert.Dispose();
        }
    }
}
