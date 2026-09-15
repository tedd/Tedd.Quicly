using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.Acme.Tests;

/// <summary>The fake CA's callback-mode <c>tls-alpn-01</c> check applies the same RFC 8737 rules as its network mode.</summary>
public sealed class FakeCaTlsAlpnCallbackTests : IAsyncDisposable
{
    private const string AcmeIdentifierOid = "1.3.6.1.5.5.7.1.31";
    private readonly FakeAcmeServer _server = new();
    private readonly HttpClient _http = new();
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "quicly-acme-tests", Guid.NewGuid().ToString("N"));
    private readonly TamperingResponder _responder = new();

    public FakeCaTlsAlpnCallbackTests()
    {
        Directory.CreateDirectory(_dir);
        _server.TlsAlpnLookup = _responder.Lookup;
        _server.Start();
    }

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _server.DisposeAsync();
        _responder.Dispose();
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, true);
        }
    }

    [Fact]
    public async Task UntamperedCertificate_Validates()
    {
        using IssuedCertificate issued = await OrderAsync();

        Assert.True(Assert.Single(_server.ValidationLog).Succeeded);
    }

    [Fact]
    public async Task CertificateWithASecondSubjectAlternativeName_FailsValidation()
    {
        _responder.Tamper = (original, domain) => Reissue(domain, "other.example.test", original.Extensions[AcmeIdentifierOid]!.RawData);

        AcmeException error = await Assert.ThrowsAsync<AcmeException>(() => OrderAsync());

        FakeAcmeValidation validation = Assert.Single(_server.ValidationLog);
        Assert.False(validation.Succeeded);
        Assert.Contains("is not exactly game.example.test", validation.Error, StringComparison.Ordinal);
        Assert.NotEqual(HttpStatusCode.InternalServerError, error.StatusCode);
    }

    [Fact]
    public async Task AcmeIdentifierThatIsNotAnOctetString_FailsValidation_WithAProblemRatherThanAServerError()
    {
        _responder.Tamper = (_, domain) => Reissue(domain, null, [0x02, 0x01, 0x05]); // an INTEGER

        AcmeException error = await Assert.ThrowsAsync<AcmeException>(() => OrderAsync());

        FakeAcmeValidation validation = Assert.Single(_server.ValidationLog);
        Assert.Contains("not a DER OCTET STRING", validation.Error, StringComparison.Ordinal);
        Assert.NotEqual(HttpStatusCode.InternalServerError, error.StatusCode);
    }

    private async Task<IssuedCertificate> OrderAsync()
    {
        using AcmeCertificateManager manager = new(_http, new AcmeCertificateManagerOptions
        {
            DirectoryUrl = _server.DirectoryUrl,
            AccountStore = new AcmeAccountStore(Path.Combine(_dir, "account.json"), AcmeStoreProtection.None),
            Identifiers = [AcmeIdentifier.Dns("game.example.test")],
            PreferredChallengeTypes = [AcmeChallengeTypes.TlsAlpn01],
            TlsAlpn01Responder = _responder,
            Retry = new AcmeRetryOptions { MaxAttempts = 1 },
            ClientOptions = new AcmeClientOptions { TimeProvider = new FastTimeProvider(), MaxPollAttempts = 5 },
        });
        return await manager.OrderCertificateAsync();
    }

    /// <summary>A self-signed validation certificate for <paramref name="domain"/> (plus <paramref name="extraName"/>) with the given acmeIdentifier extension value.</summary>
    private static X509Certificate2 Reissue(string domain, string? extraName, byte[] acmeIdentifier)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        CertificateRequest request = new("CN=" + domain, key, HashAlgorithmName.SHA256);
        SubjectAlternativeNameBuilder san = new();
        san.AddDnsName(domain);
        if (extraName is not null)
        {
            san.AddDnsName(extraName);
        }

        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509Extension(AcmeIdentifierOid, acmeIdentifier, critical: true));
        DateTimeOffset now = DateTimeOffset.UtcNow;
        return request.CreateSelfSigned(now.AddMinutes(-5), now.AddDays(1));
    }

    /// <summary>Presents a (possibly modified) copy of the certificate the manager publishes.</summary>
    private sealed class TamperingResponder : ITlsAlpn01Responder, IDisposable
    {
        private readonly Dictionary<string, X509Certificate2> _published = new(StringComparer.OrdinalIgnoreCase);

        public Func<X509Certificate2, string, X509Certificate2>? Tamper { get; set; }

        public X509Certificate2? Lookup(string domain)
        {
            lock (_published)
            {
                return _published.GetValueOrDefault(domain);
            }
        }

        public ValueTask PublishAsync(string domain, X509Certificate2 certificate, CancellationToken cancellationToken)
        {
            X509Certificate2 presented = Tamper is { } tamper ? tamper(certificate, domain) : X509CertificateLoader.LoadCertificate(certificate.RawData);
            lock (_published)
            {
                if (_published.Remove(domain, out X509Certificate2? old))
                {
                    old.Dispose();
                }

                _published[domain] = presented;
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask RemoveAsync(string domain, CancellationToken cancellationToken)
        {
            lock (_published)
            {
                if (_published.Remove(domain, out X509Certificate2? old))
                {
                    old.Dispose();
                }
            }

            return ValueTask.CompletedTask;
        }

        public void Dispose()
        {
            lock (_published)
            {
                foreach (X509Certificate2 certificate in _published.Values)
                {
                    certificate.Dispose();
                }

                _published.Clear();
            }
        }
    }
}
