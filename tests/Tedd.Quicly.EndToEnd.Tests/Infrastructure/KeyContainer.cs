using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>
/// The CNG key container behind a certificate's private key on Windows. Schannel signs only with a persisted, non-ephemeral
/// key (ADR 0009). The provisioner loads served certificates with <c>DefaultKeySet</c>: the key lands in a container of the
/// user's (or, with <c>MachineKeySet</c>, the machine's) key store that lives as long as the certificate object, and
/// disposing the certificate deletes it.
/// </summary>
[SupportedOSPlatform("windows")]
internal readonly record struct KeyContainer(string Name, CngProvider Provider, bool IsMachineKey, bool IsEphemeral, CngExportPolicies ExportPolicy)
{
    public static KeyContainer Of(X509Certificate2 certificate)
    {
        using ECDsa? ecdsa = certificate.GetECDsaPrivateKey();
        if (ecdsa is not ECDsaCng cng)
        {
            throw new InvalidOperationException("The certificate's private key is not a CNG ECDSA key (" + (ecdsa?.GetType().Name ?? "no key") + ").");
        }

        CngKey key = cng.Key;
        string name = key.KeyName ?? throw new InvalidOperationException("The private key is ephemeral: it has no key container.");
        return new KeyContainer(name, key.Provider!, key.IsMachineKey, key.IsEphemeral, key.ExportPolicy);
    }

    /// <summary>Whether the container still exists in its key store.</summary>
    public bool Exists() => CngKey.Exists(Name, Provider, IsMachineKey ? CngKeyOpenOptions.MachineKey : CngKeyOpenOptions.None);

    /// <summary>Whether the key may leave the container (ADR 0009: never <c>Exportable</c>).</summary>
    public bool AllowsExport => (ExportPolicy & (CngExportPolicies.AllowExport | CngExportPolicies.AllowPlaintextExport)) != 0;
}
