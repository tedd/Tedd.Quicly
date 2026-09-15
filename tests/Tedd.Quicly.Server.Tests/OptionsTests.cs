using System.Net;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.Server.Tests;

public sealed class OptionsTests
{
    private static readonly Dictionary<string, Action<AcmeProvisioningOptions>> InvalidCases = new()
    {
        ["directory-relative"] = o => o.DirectoryUrl = new Uri("/directory", UriKind.Relative),
        ["directory-null"] = o => o.DirectoryUrl = null!,
        ["terms"] = o => o.AgreeToTermsOfService = false,
        ["account-path"] = o => o.AccountStorePath = null,
        ["certificate-path"] = o => o.CertificatePath = " ",
        ["eab-half"] = o => o.ExternalAccountKeyId = "kid",
        ["no-challenges"] = o => o.ChallengeTypes.Clear(),
        ["unknown-challenge"] = o => o.ChallengeTypes.Add((AcmeChallengeKind)42),
        ["dns01-without-provider"] = o =>
        {
            o.ChallengeTypes.Add(AcmeChallengeKind.Dns01);
            o.Dns01Provider = null;
        },
        ["no-names"] = o => o.DnsNames.Clear(),
        ["name-with-space"] = o => o.DnsNames.Add("bad name"),
        ["name-too-long"] = o => o.DnsNames.Add(new string('a', 254)),
        ["ip-as-dns-name"] = o => o.DnsNames.Add("10.0.0.1"),
        ["wildcard-without-dns01"] = o => o.DnsNames.Add("*.example.test"),
        ["null-ip"] = o => o.IpAddresses.Add(null!),
        ["ip-with-dns01-only"] = o =>
        {
            o.ChallengeTypes.Clear();
            o.ChallengeTypes.Add(AcmeChallengeKind.Dns01);
            o.Dns01Provider = new InMemoryDns01Provider();
            o.IpAddresses.Add(IPAddress.Loopback);
        },
        ["http-endpoint-null"] = o => o.HttpChallengeEndpoint = null!,
        ["tls-endpoint-null"] = o =>
        {
            o.ChallengeTypes.Add(AcmeChallengeKind.TlsAlpn01);
            o.TlsAlpnEndpoint = null!;
        },
        ["redirect-port"] = o => o.RedirectHttpsPort = 0,
        ["health-path"] = o =>
        {
            o.EnableHealthEndpoint = true;
            o.HealthPath = "healthz";
        },
        ["propagation-delay"] = o => o.ChallengePropagationDelay = TimeSpan.FromSeconds(-1),
        ["renewal-null"] = o => o.Renewal = null!,
        ["empty-contact"] = o => o.Contacts.Add(" "),
        ["key-algorithm-unknown"] = o => o.KeyAlgorithm = (AcmeKeyAlgorithm)42,
        ["rsa-key-too-small"] = o =>
        {
            o.KeyAlgorithm = AcmeKeyAlgorithm.RS256;
            o.RsaKeySizeBits = 1024;
        },
        ["eab-blank-key-id"] = o =>
        {
            o.ExternalAccountKeyId = " ";
            o.ExternalAccountHmacKey = "c2VjcmV0LWtleQ";
        },
        ["eab-hmac-not-base64url"] = o =>
        {
            o.ExternalAccountKeyId = "kid";
            o.ExternalAccountHmacKey = "not base64url!";
        },
        ["eab-hmac-blank"] = o =>
        {
            o.ExternalAccountKeyId = "kid";
            o.ExternalAccountHmacKey = string.Empty;
        },
        ["eab-hmac-padding-only"] = o =>
        {
            o.ExternalAccountKeyId = "kid";
            o.ExternalAccountHmacKey = "==";
        },
        ["cleanup-timeout-zero"] = o => o.ChallengeCleanupTimeout = TimeSpan.Zero,
        ["cleanup-timeout-too-long"] = o => o.ChallengeCleanupTimeout = TimeSpan.FromDays(31),
        ["retry-out-of-range"] = o => o.Retry = new AcmeRetryOptions { MaxAttempts = 0 },
        ["renewal-negative-lead"] = o => o.Renewal = new RenewalSchedulerOptions { RenewBefore = TimeSpan.FromDays(-1) },
        ["renewal-zero-retry-delay"] = o => o.Renewal = new RenewalSchedulerOptions { RetryDelay = TimeSpan.Zero },
        ["renewal-null-clock"] = o => o.Renewal = new RenewalSchedulerOptions { TimeProvider = null! },
        ["tls-handler-null"] = o => o.TlsEndpointHandlers.Add(null!),
        ["same-endpoint"] = o =>
        {
            o.HttpChallengeEndpoint = new IPEndPoint(IPAddress.Loopback, 8080);
            o.TlsAlpnEndpoint = new IPEndPoint(IPAddress.Loopback, 8080);
        },
        ["same-port-wildcard"] = o =>
        {
            o.HttpChallengeEndpoint = new IPEndPoint(IPAddress.Loopback, 8080);
            o.TlsAlpnEndpoint = new IPEndPoint(IPAddress.IPv6Any, 8080);
        },
    };

    public static TheoryData<string> InvalidCaseNames => [.. InvalidCases.Keys];

    private static AcmeProvisioningOptions Valid()
    {
        AcmeProvisioningOptions options = new()
        {
            AgreeToTermsOfService = true,
            AccountStorePath = "account.json",
            CertificatePath = "server.pfx",
        };
        options.DnsNames.Add("game.example.test");
        return options;
    }

    [Theory]
    [MemberData(nameof(InvalidCaseNames))]
    public void InvalidAcmeOptions_AreRejectedWithAClearMessage(string name)
    {
        AcmeProvisioningOptions options = Valid();
        InvalidCases[name](options);

        ArgumentException error = Assert.Throws<ArgumentException>(() => ServerCertificateOptions.Acme(options));

        Assert.StartsWith("Invalid ACME provisioning options: ", error.Message, StringComparison.Ordinal);
        Assert.Equal("options", error.ParamName);
    }

    [Fact]
    public void EphemeralKeys_AreRejectedOnWindows()
    {
        AcmeProvisioningOptions options = Valid();
        options.KeyStorageFlags = X509KeyStorageFlags.EphemeralKeySet;
        if (OperatingSystem.IsWindows())
        {
            Assert.Throws<ArgumentException>(() => ServerCertificateOptions.Acme(options));
        }
        else
        {
            Assert.Equal(ServerCertificateSourceKind.Acme, ServerCertificateOptions.Acme(options).Kind);
        }
    }

    [Fact]
    public void OptionsChangedAfterCreation_AreValidatedAgainByTheProvisioner()
    {
        AcmeProvisioningOptions options = Valid();
        ServerCertificateOptions certificates = ServerCertificateOptions.Acme(options);
        options.AgreeToTermsOfService = false;

        Assert.Throws<ArgumentException>(() => new CertificateProvisioner(certificates));
    }

    [Fact]
    public void Defaults_MatchTheDocumentedBehaviour()
    {
        AcmeProvisioningOptions options = new();

        Assert.Equal(AcmeDirectories.LetsEncrypt, options.DirectoryUrl);
        Assert.False(options.AgreeToTermsOfService);
        Assert.Equal([AcmeChallengeKind.Http01, AcmeChallengeKind.TlsAlpn01], options.ChallengeTypes);
        Assert.Equal(new IPEndPoint(IPAddress.Any, 80), options.HttpChallengeEndpoint);
        Assert.Equal(new IPEndPoint(IPAddress.Any, 443), options.TlsAlpnEndpoint);
        Assert.True(options.RedirectToHttps);
        Assert.False(options.EnableHealthEndpoint);
        Assert.Equal(AcmeProvisioningOptions.DefaultHealthPath, options.HealthPath);
        Assert.Equal(X509KeyStorageFlags.DefaultKeySet, options.KeyStorageFlags);
        Assert.Equal(AcmeKeyAlgorithm.ES256, options.KeyAlgorithm);
        Assert.False(options.ReuseKey);
    }

    [Fact]
    public void Identifiers_ChallengeTypes_AndContacts_AreNormalised()
    {
        AcmeProvisioningOptions options = Valid();
        options.DnsNames.Add("GAME.example.test");
        options.IpAddresses.Add(IPAddress.Loopback);
        options.IpAddresses.Add(IPAddress.Parse("127.0.0.1"));
        options.ChallengeTypes.Add(AcmeChallengeKind.Http01);
        options.ChallengeTypes.Add(AcmeChallengeKind.Dns01);
        options.Contacts.Add("ops@example.test");
        options.Contacts.Add("mailto:admin@example.test");

        Assert.Equal([AcmeIdentifier.Dns("game.example.test"), AcmeIdentifier.Ip("127.0.0.1")], options.BuildIdentifiers());
        Assert.Equal([AcmeChallengeTypes.Http01, AcmeChallengeTypes.TlsAlpn01, AcmeChallengeTypes.Dns01], options.BuildChallengeTypes());
        Assert.Equal(["mailto:ops@example.test", "mailto:admin@example.test"], options.BuildContacts());
    }

    [Fact]
    public void ServerCertificateOptions_Factories()
    {
        using X509Certificate2 withKey = Certs.Create();
        using X509Certificate2 publicOnly = X509CertificateLoader.LoadCertificate(withKey.RawData);

        ServerCertificateOptions fixedCertificate = ServerCertificateOptions.Static(withKey);
        Assert.Equal(ServerCertificateSourceKind.Static, fixedCertificate.Kind);
        Assert.Same(withKey, fixedCertificate.Certificate);
        Assert.Throws<ArgumentNullException>(() => ServerCertificateOptions.Static(null!));
        Assert.Throws<ArgumentException>(() => ServerCertificateOptions.Static(publicOnly));

        ServerCertificateOptions file = ServerCertificateOptions.File("server.pfx", "secret");
        Assert.Equal(ServerCertificateSourceKind.File, file.Kind);
        Assert.Equal("server.pfx", file.FilePath);
        Assert.Equal("secret", file.FilePassword);
        Assert.True(file.ReloadOnChange);
        Assert.Equal(ServerCertificateOptions.DefaultReloadInterval, file.ReloadInterval);
        Assert.Null(file.AcmeOptions);
        Assert.Throws<ArgumentException>(() => ServerCertificateOptions.File(" "));
        Assert.Throws<ArgumentOutOfRangeException>(() => ServerCertificateOptions.File("server.pfx", reloadInterval: TimeSpan.Zero));

        AcmeProvisioningOptions acmeOptions = Valid();
        ServerCertificateOptions acme = ServerCertificateOptions.Acme(acmeOptions);
        Assert.Equal(ServerCertificateSourceKind.Acme, acme.Kind);
        Assert.Same(acmeOptions, acme.AcmeOptions);
        Assert.Throws<ArgumentNullException>(() => ServerCertificateOptions.Acme(null!));
    }

    [Fact]
    public void CertificateStatus_Formatting()
    {
        Assert.Equal("Stopped", CertificateStatus.Stopped.ToString());
        InvalidOperationException error = new("boom");
        CertificateStatus failed = CertificateStatus.Failed("CA unreachable", error);
        Assert.Equal(CertificateState.Failed, failed.State);
        Assert.Same(error, failed.Error);
        Assert.Equal("Failed: CA unreachable", failed.ToString());
    }

    [Fact]
    public void CertificateIdentity_ReverseDnsNames_AndDescriptions()
    {
        Assert.Equal("1.0.0.127.in-addr.arpa", CertificateIdentity.ReverseDnsName(IPAddress.Loopback));
        Assert.Equal(string.Concat(Enumerable.Repeat("0.", 31)).Insert(0, "1.") + "ip6.arpa", CertificateIdentity.ReverseDnsName(IPAddress.IPv6Loopback));
        Assert.Equal(FakeAcmeServer.ReverseDnsName(IPAddress.Parse("2001:db8::1")), CertificateIdentity.ReverseDnsName(IPAddress.Parse("2001:db8::1")));

        using X509Certificate2 withSan = Certs.Create("lobby.example.test");
        Assert.StartsWith("[lobby.example.test] expires ", CertificateIdentity.Describe(withSan), StringComparison.Ordinal);
        using X509Certificate2 withoutSan = X509CertificateLoader.LoadPkcs12(Certs.Pfx(null), null);
        Assert.StartsWith("[CN=no-san] expires ", CertificateIdentity.Describe(withoutSan), StringComparison.Ordinal);
        Assert.False(CertificateIdentity.Covers(withoutSan, [AcmeIdentifier.Dns("no-san")]));
        Assert.True(CertificateIdentity.Covers(withSan, [AcmeIdentifier.Dns("LOBBY.example.test")]));
        Assert.False(CertificateIdentity.Covers(withSan, [AcmeIdentifier.Ip("127.0.0.1")]));
    }

    [Fact]
    public void AnInvalidEabKey_IsNeverQuotedInTheMessage()
    {
        AcmeProvisioningOptions options = Valid();
        options.ExternalAccountKeyId = "kid";
        options.ExternalAccountHmacKey = "s3cr3t value!";

        ArgumentException error = Assert.Throws<ArgumentException>(() => ServerCertificateOptions.Acme(options));

        Assert.DoesNotContain("s3cr3t", error.Message, StringComparison.Ordinal);
        Assert.Contains("ExternalAccountHmacKey", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("127.0.0.1", 0, "127.0.0.1", 0)]
    [InlineData("0.0.0.0", 80, "0.0.0.0", 443)]
    [InlineData("127.0.0.1", 8080, "127.0.0.2", 8080)]
    public void ChallengeEndpointsOnSeparatePorts_AreAccepted(string httpAddress, int httpPort, string tlsAddress, int tlsPort)
    {
        AcmeProvisioningOptions options = Valid();
        options.HttpChallengeEndpoint = new IPEndPoint(IPAddress.Parse(httpAddress), httpPort);
        options.TlsAlpnEndpoint = new IPEndPoint(IPAddress.Parse(tlsAddress), tlsPort);

        Assert.Equal(ServerCertificateSourceKind.Acme, ServerCertificateOptions.Acme(options).Kind);
    }

    [Fact]
    public void RsaKeys_AndEabKeysInEitherBase64Alphabet_AreAccepted()
    {
        AcmeProvisioningOptions options = Valid();
        options.KeyAlgorithm = AcmeKeyAlgorithm.RS256;
        options.RsaKeySizeBits = 3072;
        options.ExternalAccountKeyId = "kid";
        foreach (string key in new[] { "c2VjcmV0LWtleS1ieXRlcw", "c2VjcmV0LWtleS1ieXRlcw==", "+/+/c2VjcmV0" })
        {
            options.ExternalAccountHmacKey = key;
            Assert.Equal(ServerCertificateSourceKind.Acme, ServerCertificateOptions.Acme(options).Kind);
        }
    }
}
