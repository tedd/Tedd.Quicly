using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.Acme.Tests;

public sealed class AcmeCertificateManagerTests : IAsyncDisposable
{
    private readonly FakeAcmeServer _server = new();
    private readonly HttpClient _http = new();
    private readonly FastTimeProvider _clock = new();
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "quicly-acme-tests", Guid.NewGuid().ToString("N"));
    private readonly Http01TestResponder _http01 = new();
    private readonly InMemoryDns01Provider _dns01 = new();
    private readonly InMemoryTlsAlpn01Responder _tlsAlpn01 = new();

    public AcmeCertificateManagerTests()
    {
        Directory.CreateDirectory(_dir);
        _server.Http01BaseUri = _http01.BaseUri;
        _server.DnsTxtLookup = _dns01.Lookup;
        _server.TlsAlpnLookup = _tlsAlpn01.Lookup;
        _server.Start();
    }

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _http01.DisposeAsync();
        await _server.DisposeAsync();
        foreach (X509Certificate2 c in _tlsAlpn01.Certificates.Values)
        {
            c.Dispose();
        }

        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, true);
        }
    }

    private AcmeCertificateManagerOptions Options(
        IReadOnlyList<string>? challengeTypes = null,
        bool http01 = true,
        bool dns01 = true,
        bool tlsAlpn01 = true,
        IReadOnlyList<AcmeIdentifier>? identifiers = null,
        string? certificatePath = null,
        string? password = null,
        string? preferredChainIssuer = null,
        Uri? directory = null)
    {
        return new AcmeCertificateManagerOptions
        {
            DirectoryUrl = directory ?? _server.DirectoryUrl,
            AccountStore = new AcmeAccountStore(Path.Combine(_dir, "account.json")),
            Identifiers = identifiers ?? [AcmeIdentifier.Dns("game.example.test"), AcmeIdentifier.Dns("www.game.example.test")],
            Contacts = ["mailto:ops@example.test"],
            PreferredChallengeTypes = challengeTypes ?? [AcmeChallengeTypes.Http01, AcmeChallengeTypes.TlsAlpn01, AcmeChallengeTypes.Dns01],
            Http01Responder = http01 ? _http01 : null,
            Dns01Provider = dns01 ? _dns01 : null,
            TlsAlpn01Responder = tlsAlpn01 ? _tlsAlpn01 : null,
            CertificatePath = certificatePath,
            CertificatePassword = password,
            PreferredChainIssuer = preferredChainIssuer,
            ClientOptions = new AcmeClientOptions { TimeProvider = _clock, MaxPollAttempts = 5 },
        };
    }

    [Fact]
    public void Constructor_ValidatesArguments()
    {
        Assert.Throws<ArgumentNullException>(() => new AcmeCertificateManager(null!, Options()));
        Assert.Throws<ArgumentNullException>(() => new AcmeCertificateManager(_http, null!));
        Assert.Throws<ArgumentException>(() => new AcmeCertificateManager(_http, Options(identifiers: [])));
    }

    [Fact]
    public async Task EndToEnd_Http01_PersistsAccountAndCertificate()
    {
        string pfxPath = Path.Combine(_dir, "certs", "game.pfx");
        AcmeCertificateManagerOptions options = Options(certificatePath: pfxPath, password: "pw");
        AcmeCertificateManager manager = new(_http, options);
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;
        Assert.Null(manager.TryLoadPersistedCertificate());

        _server.PendingAuthzPolls = 1;
        _server.ProcessingOrderPolls = 1;
        using IssuedCertificate issued = await manager.OrderCertificateAsync();

        Assert.True(issued.Certificate.HasPrivateKey);
        Assert.True(issued.Certificate.MatchesHostname("game.example.test"));
        Assert.True(issued.Certificate.MatchesHostname("www.game.example.test"));
        Assert.Equal(2, issued.Chain.Count);
        Assert.Equal(_server.Ca.Root.Thumbprint, issued.Chain[1].Thumbprint);
        Assert.True(File.Exists(pfxPath));

        using IssuedCertificate persisted = manager.TryLoadPersistedCertificate()!;
        Assert.Equal(issued.Certificate.Thumbprint, persisted.Certificate.Thumbprint);

        // Both tokens were published to the http-01 responder and removed again.
        Assert.Equal(2, _http01.Published.Count);
        Assert.Equal(2, _http01.Removed.Count);
        Assert.Equal(0, _http01.ActiveTokenCount);
        Assert.Equal(2, _server.ValidatedKeyAuthorizations.Count);
        Assert.Empty(_dns01.Created);
        Assert.Empty(_tlsAlpn01.Published);

        // Account persisted and reused by a second manager (no second newAccount).
        Assert.True(options.AccountStore.Exists);
        AcmeAccountState state = options.AccountStore.Load()!;
        Assert.Equal(_server.DirectoryUrl, state.DirectoryUrl);
        AcmeCertificateManager second = new(_http, options);
        List<AcmeProgress> secondEvents = [];
        second.Progress += secondEvents.Add;
        AcmeClient client = await second.GetClientAsync();
        Assert.Same(client, await second.GetClientAsync());
        Assert.Equal(state.AccountUrl, client.AccountUrl);
        Assert.Equal(1, _server.AccountCount);
        Assert.Contains(secondEvents, e => e.Stage == AcmeStage.AccountReady && e.Message.StartsWith("Reusing", StringComparison.Ordinal));

        AcmeStage[] stages = events.Select(e => e.Stage).Distinct().ToArray();
        Assert.Equal(
            [
                AcmeStage.DirectoryFetched, AcmeStage.AccountReady, AcmeStage.OrderCreated, AcmeStage.ChallengeSelected, AcmeStage.ChallengePublished,
                AcmeStage.ChallengeResponded, AcmeStage.AuthorizationValid, AcmeStage.ChallengeCleanedUp, AcmeStage.OrderReady, AcmeStage.OrderFinalized,
                AcmeStage.CertificateDownloaded, AcmeStage.CertificatePersisted,
            ],
            stages);
        Assert.All(events.Where(e => e.Stage is AcmeStage.ChallengeSelected or AcmeStage.AuthorizationValid), e => Assert.NotNull(e.Identifier));
        Assert.DoesNotContain(events, e => e.Stage == AcmeStage.Failed);

        // Revocation through the manager.
        await second.RevokeAsync(issued.Certificate, AcmeRevocationReason.Superseded);
        Assert.Equal(1, _server.RevokedCount);
    }

    [Fact]
    public async Task EndToEnd_Dns01_WithWildcardAndPropagationDelay()
    {
        AcmeCertificateManagerOptions options = new()
        {
            DirectoryUrl = _server.DirectoryUrl,
            AccountStore = new AcmeAccountStore(Path.Combine(_dir, "account.json")),
            Identifiers = [AcmeIdentifier.Dns("*.wild.example.test"), AcmeIdentifier.Dns("wild.example.test")],
            PreferredChallengeTypes = [AcmeChallengeTypes.Dns01],
            Dns01Provider = _dns01,
            ChallengePropagationDelay = TimeSpan.FromSeconds(5),
            CertificateKeyAlgorithm = AcmeKeyAlgorithm.RS256,
            AccountKeyAlgorithm = AcmeKeyAlgorithm.RS256,
            ClientOptions = new AcmeClientOptions { TimeProvider = _clock, MaxPollAttempts = 5 },
        };
        AcmeCertificateManager manager = new(_http, options);
        using IssuedCertificate issued = await manager.OrderCertificateAsync();

        Assert.True(issued.Certificate.MatchesHostname("anything.wild.example.test"));
        Assert.True(issued.Certificate.MatchesHostname("wild.example.test"));
        using System.Security.Cryptography.RSA? rsa = issued.Certificate.GetRSAPrivateKey();
        Assert.NotNull(rsa);

        Assert.Equal(2, _dns01.Created.Count);
        Assert.All(_dns01.Created, r => Assert.Equal("_acme-challenge.wild.example.test", r.Name));
        Assert.Equal(2, _dns01.Removed.Count);
        Assert.Empty(_dns01.Lookup("_acme-challenge.wild.example.test"));
        Assert.Equal(2, _clock.Delays.Count(d => d == TimeSpan.FromSeconds(5)));
        Assert.Equal(AcmeKeyAlgorithm.RS256, options.AccountStore.Load()!.Algorithm);
    }

    [Fact]
    public async Task EndToEnd_TlsAlpn01_WithIpIdentifier_AndAlternateChainSelection()
    {
        AcmeCertificateManagerOptions options = Options(
            challengeTypes: [AcmeChallengeTypes.TlsAlpn01],
            identifiers: [AcmeIdentifier.Dns("alpn.example.test"), AcmeIdentifier.Ip("192.0.2.55")],
            preferredChainIssuer: "Fake Alternate Root");
        AcmeCertificateManager manager = new(_http, options);
        using IssuedCertificate issued = await manager.OrderCertificateAsync();

        Assert.True(issued.Certificate.MatchesHostname("alpn.example.test"));
        Assert.True(issued.Certificate.MatchesHostname("192.0.2.55"));
        Assert.Equal(_server.AlternateCa.Root.Thumbprint, issued.Chain[1].Thumbprint);
        Assert.Equal(["alpn.example.test", "192.0.2.55"], _tlsAlpn01.Published);
        Assert.Equal(["alpn.example.test", "192.0.2.55"], _tlsAlpn01.Removed);
        Assert.Empty(_tlsAlpn01.Certificates);
    }

    [Fact]
    public async Task PreferredChainIssuer_FallsBackToDefaultChain_WhenNoAlternateMatches()
    {
        AcmeCertificateManager manager = new(_http, Options(preferredChainIssuer: "No Such Root"));
        using IssuedCertificate issued = await manager.OrderCertificateAsync();
        Assert.Equal(_server.Ca.Root.Thumbprint, issued.Chain[1].Thumbprint);

        // Preferred issuer that matches the default chain never downloads alternates.
        int before = _server.RequestLog.Count(r => r.Contains("/alt", StringComparison.Ordinal));
        AcmeCertificateManager direct = new(_http, Options(preferredChainIssuer: "Fake Root CA"));
        using IssuedCertificate second = await direct.OrderCertificateAsync();
        Assert.Equal(before, _server.RequestLog.Count(r => r.Contains("/alt", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task PreauthorizedIdentifiers_SkipChallenges_AndFinalizeMayReturnValidImmediately()
    {
        _server.PreauthorizedIdentifiers.Add("game.example.test");
        _server.PreauthorizedIdentifiers.Add("www.game.example.test");
        AcmeCertificateManager manager = new(_http, Options());
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;
        using IssuedCertificate issued = await manager.OrderCertificateAsync();
        Assert.Equal(2, events.Count(e => e.Stage == AcmeStage.AuthorizationSkipped));
        Assert.DoesNotContain(events, e => e.Stage == AcmeStage.ChallengePublished);
        Assert.Empty(_http01.Published);
    }

    [Fact]
    public async Task ChallengeSelection_HonoursPreferenceAndAvailability()
    {
        // http-01 preferred but no responder for it: tls-alpn-01 wins over dns-01.
        AcmeCertificateManager manager = new(_http, Options(http01: false));
        using IssuedCertificate issued = await manager.OrderCertificateAsync();
        Assert.Equal(2, _tlsAlpn01.Published.Count);
        Assert.Empty(_dns01.Created);

        // Server only offers dns-01, client has no dns provider -> noSupportedChallenge.
        _server.OfferedChallengeTypes.Clear();
        _server.OfferedChallengeTypes.Add(AcmeChallengeTypes.Dns01);
        AcmeCertificateManager none = new(_http, Options(dns01: false, challengeTypes: [AcmeChallengeTypes.Http01, "made-up-01"]));
        List<AcmeProgress> events = [];
        none.Progress += events.Add;
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await none.OrderCertificateAsync());
        Assert.Equal(AcmeErrorTypes.NoSupportedChallenge, e.Type);
        Assert.Equal("game.example.test", e.Problem.Identifier!.Value);
        Assert.Contains(events, ev => ev.Stage == AcmeStage.Failed);
    }

    [Fact]
    public async Task FailedValidation_CleansUp_AndReportsFailure()
    {
        _server.FailValidation = true;
        AcmeCertificateManager manager = new(_http, Options());
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await manager.OrderCertificateAsync());
        Assert.Equal(AcmeErrorTypes.ValidationFailed, e.Type);
        Assert.Single(_http01.Published);
        Assert.Single(_http01.Removed);
        Assert.Equal(0, _http01.ActiveTokenCount);
        Assert.Equal(AcmeStage.Failed, events[^1].Stage);
        Assert.Contains(events, ev => ev.Stage == AcmeStage.ChallengeCleanedUp);
    }

    [Fact]
    public async Task CleanupFailure_DoesNotMaskSuccess()
    {
        _http01.FailRemove = true;
        AcmeCertificateManager manager = new(_http, Options());
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;
        using IssuedCertificate issued = await manager.OrderCertificateAsync();
        Assert.True(issued.Certificate.HasPrivateKey);
        Assert.Contains(events, ev => ev.Stage == AcmeStage.ChallengeCleanedUp && ev.Message.StartsWith("Cleanup failed", StringComparison.Ordinal));
        Assert.Equal(2, _http01.ActiveTokenCount);
    }

    [Fact]
    public async Task ResponderFailure_BeforeResponding_PropagatesAndCleansUp()
    {
        _dns01.FailCreate = true;
        AcmeCertificateManager manager = new(_http, Options(challengeTypes: [AcmeChallengeTypes.Dns01]));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await manager.OrderCertificateAsync());
        Assert.Single(_dns01.Removed);
        Assert.Empty(_server.ValidatedKeyAuthorizations);
    }

    [Fact]
    public async Task Cancellation_IsNotReportedAsFailure()
    {
        AcmeCertificateManager manager = new(_http, Options());
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;
        using CancellationTokenSource cts = new();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await manager.OrderCertificateAsync(cts.Token));
        Assert.DoesNotContain(events, ev => ev.Stage == AcmeStage.Failed);
    }

    [Fact]
    public async Task ValidOrderWithoutCertificateUrl_IsAnError()
    {
        _server.OmitCertificateUrlOnValidOrder = true;
        AcmeCertificateManager manager = new(_http, Options());
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await manager.OrderCertificateAsync());
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);
    }

    [Fact]
    public async Task ChallengeWithoutToken_IsAnError()
    {
        AcmeCertificateManager manager = new(_http, Options(identifiers: [AcmeIdentifier.Dns("game.example.test")]));
        _server.AddOverride("/authz/", 200, "application/json", "{\"identifier\":{\"type\":\"dns\",\"value\":\"game.example.test\"},\"status\":\"pending\",\"challenges\":[{\"type\":\"http-01\",\"url\":\"" + _server.BaseUri + "chall/x\",\"status\":\"pending\"}]}");
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await manager.OrderCertificateAsync());
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);
        Assert.Contains("has no token", e.Message);
        Assert.Empty(_http01.Published);
    }

    [Fact]
    public async Task OrderStillPendingAfterAuthorizations_IsAValidationFailure()
    {
        // The CA reports every authorization valid but then flips the order to invalid.
        AcmeCertificateManager manager = new(_http, Options(identifiers: [AcmeIdentifier.Dns("game.example.test")]));
        _server.AddOverride("/order/", 200, "application/json", "{\"status\":\"invalid\",\"identifiers\":[],\"authorizations\":[],\"finalize\":\"" + _server.BaseUri + "order/x/finalize\",\"error\":{\"type\":\"urn:ietf:params:acme:error:caa\",\"detail\":\"CAA forbids\"}}");
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await manager.OrderCertificateAsync());
        Assert.Equal(AcmeErrorTypes.ValidationFailed, e.Type);
        Assert.Equal(AcmeErrorTypes.Caa, e.Problem.Subproblems!.Single().Type);
        Assert.Single(_http01.Removed);
    }

    [Fact]
    public async Task DirectoryChange_WithReachableDirectory_CreatesNewAccount()
    {
        AcmeCertificateManagerOptions options = Options();
        AcmeCertificateManager manager = new(_http, options);
        await manager.GetClientAsync();
        Assert.Equal(1, _server.AccountCount);

        // Point the persisted state at another directory; the manager must not reuse it.
        AcmeAccountState state = options.AccountStore.Load()!;
        options.AccountStore.Save(state with { DirectoryUrl = AcmeDirectories.LetsEncryptStaging });
        AcmeCertificateManager m2 = new(_http, options);
        List<AcmeProgress> events = [];
        m2.Progress += events.Add;
        await m2.GetClientAsync();
        Assert.Equal(2, _server.AccountCount);
        Assert.Contains(events, ev => ev.Stage == AcmeStage.AccountReady && ev.Message.StartsWith("Directory changed", StringComparison.Ordinal));
        Assert.Equal(_server.DirectoryUrl, options.AccountStore.Load()!.DirectoryUrl);
    }

    [Fact]
    public async Task Eab_IsPassedThrough_AndDirectoryEventMentionsIt()
    {
        _server.RequireEab = true;
        AcmeCertificateManagerOptions options = new()
        {
            DirectoryUrl = _server.DirectoryUrl,
            AccountStore = new AcmeAccountStore(Path.Combine(_dir, "eab.json")),
            Identifiers = [AcmeIdentifier.Dns("game.example.test")],
            ExternalAccountBinding = new ExternalAccountBinding(_server.EabKid, _server.EabHmacKeyBase64Url),
            Http01Responder = _http01,
            ClientOptions = new AcmeClientOptions { TimeProvider = _clock },
        };
        AcmeCertificateManager manager = new(_http, options);
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;
        using IssuedCertificate issued = await manager.OrderCertificateAsync();
        Assert.Contains(events, ev => ev.Stage == AcmeStage.DirectoryFetched && ev.Message.Contains("EAB required", StringComparison.Ordinal));
        Assert.Contains(events, ev => ev.Stage == AcmeStage.AccountReady && ev.Message.StartsWith("Created account", StringComparison.Ordinal));
    }
}
