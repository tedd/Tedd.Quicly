namespace Tedd.Quicly.Acme;

/// <summary>Describes a well-known ACME CA endpoint.</summary>
/// <param name="Name">Display name.</param>
/// <param name="Directory">Directory URL.</param>
/// <param name="IsTestEnvironment">True for staging / test endpoints that issue untrusted certificates.</param>
/// <param name="RequiresExternalAccountBinding">True when <c>newAccount</c> must carry an External Account Binding.</param>
/// <param name="Notes">Free-text notes (how to obtain EAB credentials, key type restrictions, ...).</param>
public sealed record AcmeCaInfo(string Name, Uri Directory, bool IsTestEnvironment, bool RequiresExternalAccountBinding, string? Notes = null);

/// <summary>Directory URLs of the popular public ACME v2 CAs and a table of their External Account Binding requirements.</summary>
public static class AcmeDirectories
{
    /// <summary>Let's Encrypt production.</summary>
    public static readonly Uri LetsEncrypt = new("https://acme-v02.api.letsencrypt.org/directory");

    /// <summary>Let's Encrypt staging (untrusted certificates, relaxed rate limits).</summary>
    public static readonly Uri LetsEncryptStaging = new("https://acme-staging-v02.api.letsencrypt.org/directory");

    /// <summary>ZeroSSL (90-day DV). Requires EAB credentials from the ZeroSSL dashboard / API.</summary>
    public static readonly Uri ZeroSsl = new("https://acme.zerossl.com/v2/DV90");

    /// <summary>Buypass Go SSL production.</summary>
    public static readonly Uri Buypass = new("https://api.buypass.com/acme/directory");

    /// <summary>Buypass test environment.</summary>
    public static readonly Uri BuypassTest = new("https://api.test4.buypass.no/acme/directory");

    /// <summary>Google Trust Services production. Requires EAB obtained via Google Cloud Public CA.</summary>
    public static readonly Uri GoogleTrustServices = new("https://dv.acme-v02.api.pki.goog/directory");

    /// <summary>Google Trust Services test environment. Requires EAB.</summary>
    public static readonly Uri GoogleTrustServicesTest = new("https://dv.acme-v02.test-api.pki.goog/directory");

    /// <summary>SSL.com DV, RSA certificates. Requires EAB from the SSL.com account.</summary>
    public static readonly Uri SslComRsa = new("https://acme.ssl.com/sslcom-dv-rsa");

    /// <summary>SSL.com DV, ECC certificates. Requires EAB from the SSL.com account.</summary>
    public static readonly Uri SslComEcc = new("https://acme.ssl.com/sslcom-dv-ecc");

    /// <summary>Table of the known CAs and whether they require External Account Binding.</summary>
    public static IReadOnlyList<AcmeCaInfo> KnownCas { get; } =
    [
        new("Let's Encrypt", LetsEncrypt, false, false, "Free; no EAB. Rate limits per registered domain."),
        new("Let's Encrypt (staging)", LetsEncryptStaging, true, false, "Untrusted test root."),
        new("ZeroSSL", ZeroSsl, false, true, "EAB kid/hmac from the ZeroSSL dashboard (Developer > EAB credentials) or REST API."),
        new("Buypass Go SSL", Buypass, false, false, "Free DV, 180-day certificates; no EAB. Contact email required."),
        new("Buypass Go SSL (test)", BuypassTest, true, false, "Untrusted test root."),
        new("Google Trust Services", GoogleTrustServices, false, true, "EAB via `gcloud publicca external-account-keys create`."),
        new("Google Trust Services (test)", GoogleTrustServicesTest, true, true, "EAB via gcloud against the test endpoint."),
        new("SSL.com DV (RSA)", SslComRsa, false, true, "EAB from the SSL.com account dashboard; RSA CSRs only."),
        new("SSL.com DV (ECC)", SslComEcc, false, true, "EAB from the SSL.com account dashboard; ECC CSRs only."),
    ];

    /// <summary>Looks up a known CA by directory URL; <see langword="null"/> for unknown (self-hosted, Pebble, ...) directories.</summary>
    public static AcmeCaInfo? Find(Uri directory)
    {
        ArgumentNullException.ThrowIfNull(directory);
        IReadOnlyList<AcmeCaInfo> cas = KnownCas;
        for (int i = 0; i < cas.Count; i++)
        {
            if (cas[i].Directory.Equals(directory))
            {
                return cas[i];
            }
        }

        return null;
    }
}
