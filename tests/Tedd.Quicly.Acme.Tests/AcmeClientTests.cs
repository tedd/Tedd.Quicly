using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.Acme.Tests;

public sealed class AcmeClientTests : IAsyncDisposable
{
    private readonly FakeAcmeServer _server = new();
    private readonly HttpClient _http = new();
    private readonly FastTimeProvider _clock = new();

    public AcmeClientTests() => _server.Start();

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _server.DisposeAsync();
    }

    private AcmeClient NewClient(AcmeAccountKey? key = null, AcmeClientOptions? options = null)
    {
        options ??= new AcmeClientOptions { TimeProvider = _clock, MaxPollAttempts = 5 };
        return new AcmeClient(_http, _server.DirectoryUrl, key ?? AcmeAccountKey.Create(), null, options);
    }

    private async Task<AcmeClient> NewClientWithAccountAsync(AcmeKeyAlgorithm algorithm = AcmeKeyAlgorithm.ES256)
    {
        AcmeClient client = NewClient(AcmeAccountKey.Create(algorithm));
        await client.CreateAccountAsync(["mailto:ops@example.test"], agreeTermsOfService: true);
        return client;
    }

    private static async Task<(AcmeOrder Order, AcmeAuthorization Authz, AcmeChallenge Challenge)> NewValidatedOrderAsync(FakeAcmeServer server, AcmeClient client, string domain = "game.example.test")
    {
        AcmeOrder order = await client.NewOrderAsync([AcmeIdentifier.Dns(domain)]);
        AcmeAuthorization authz = await client.GetAuthorizationAsync(order.Authorizations[0]);
        AcmeChallenge challenge = authz.Challenges.Single(c => c.Type == AcmeChallengeTypes.Http01);
        string keyAuth = KeyAuthorization.Compute(challenge.Token!, client.AccountKey);
        server.Http01Lookup = (_, token) => token == challenge.Token ? keyAuth : null;
        await client.RespondToChallengeAsync(challenge.Url);
        authz = await client.WaitForAuthorizationAsync(authz.Location!);
        return (order, authz, challenge);
    }

    [Fact]
    public void Constructor_ValidatesArguments()
    {
        using AcmeAccountKey key = AcmeAccountKey.Create();
        Assert.Throws<ArgumentNullException>(() => new AcmeClient(null!, _server.DirectoryUrl, key));
        Assert.Throws<ArgumentNullException>(() => new AcmeClient(_http, null!, key));
        Assert.Throws<ArgumentNullException>(() => new AcmeClient(_http, _server.DirectoryUrl, null!));
        AcmeClient client = new(_http, _server.DirectoryUrl, key);
        Assert.NotNull(client.Options);
        Assert.Equal(TimeProvider.System, client.Options.TimeProvider);
        Assert.Same(key, client.AccountKey);
        Assert.Equal(_server.DirectoryUrl, client.DirectoryUrl);
    }

    [Fact]
    public async Task Directory_IsFetchedOnce_AndCached()
    {
        AcmeClient client = NewClient();
        AcmeDirectory d1 = await client.GetDirectoryAsync();
        AcmeDirectory d2 = await client.GetDirectoryAsync();
        Assert.Same(d1, d2);
        Assert.Equal(new Uri(_server.BaseUri, "new-nonce"), d1.NewNonce);
        Assert.Equal(new Uri(_server.BaseUri, "new-account"), d1.NewAccount);
        Assert.Equal(new Uri(_server.BaseUri, "new-order"), d1.NewOrder);
        Assert.NotNull(d1.RevokeCert);
        Assert.NotNull(d1.KeyChange);
        Assert.Null(d1.NewAuthz);
        Assert.False(d1.Meta!.ExternalAccountRequired);
        Assert.Equal(["fake-ca.test"], d1.Meta.CaaIdentities);
        Assert.NotNull(d1.Meta.TermsOfService);
        Assert.NotNull(d1.Meta.Website);
        Assert.Single(_server.RequestLog, "GET /directory");
        Assert.Equal("Tedd.Quicly.Acme/1.0", _server.LastUserAgent);
    }

    [Fact]
    public async Task Directory_ErrorsAreSurfaced()
    {
        _server.NextOverride = (500, "text/plain", "boom");
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await NewClient().GetDirectoryAsync());
        Assert.Equal(AcmeErrorTypes.Unknown, e.Type);
        Assert.Equal(HttpStatusCode.InternalServerError, e.StatusCode);
        Assert.Equal(500, e.Problem.Status);

        _server.NextOverride = (200, "application/json", "this is not json");
        e = await Assert.ThrowsAsync<AcmeException>(async () => await NewClient().GetDirectoryAsync());
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);
        Assert.IsType<JsonException>(e.InnerException);

        _server.NextOverride = (200, "application/json", "null");
        e = await Assert.ThrowsAsync<AcmeException>(async () => await NewClient().GetDirectoryAsync());
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);

        // Problem document whose JSON is malformed falls back to the generic problem.
        _server.NextOverride = (503, "application/problem+json", "{ broken");
        e = await Assert.ThrowsAsync<AcmeException>(async () => await NewClient().GetDirectoryAsync());
        Assert.Equal(AcmeErrorTypes.Unknown, e.Type);
        Assert.Equal(503, e.Problem.Status);

        // Problem document with its own status keeps it.
        _server.NextOverride = (503, "application/problem+json", "{\"type\":\"urn:ietf:params:acme:error:serverInternal\",\"status\":503,\"detail\":\"x\"}");
        e = await Assert.ThrowsAsync<AcmeException>(async () => await NewClient().GetDirectoryAsync());
        Assert.Equal(AcmeErrorTypes.ServerInternal, e.Type);
        Assert.Equal(503, e.Problem.Status);
    }

    [Fact]
    public async Task Nonces_ArePooledFromEveryResponse_AndFetchedOnDemand()
    {
        AcmeClient client = NewClient();
        Assert.Equal(0, client.PooledNonceCount);
        string n1 = await client.GetNonceAsync(); // directory GET yields one; consumed here
        Assert.NotEmpty(n1);
        string n2 = await client.GetNonceAsync(); // pool empty -> HEAD newNonce
        Assert.NotEqual(n1, n2);
        Assert.Equal(1, _server.NewNonceRequests);
        Assert.Contains("HEAD /new-nonce", _server.RequestLog);

        await client.CreateAccountAsync(null, true); // pool empty -> one HEAD, then the POST response refills it
        Assert.Equal(1, client.PooledNonceCount);
        Assert.Equal(2, _server.NewNonceRequests);

        await client.GetAccountAsync(); // uses the pooled nonce, harvests the next one
        Assert.Equal(1, client.PooledNonceCount);
        Assert.Equal(2, _server.NewNonceRequests);
    }

    [Fact]
    public async Task Nonce_MissingReplayNonceHeader_IsAnError()
    {
        _server.OmitNonceOnNewNonce = true;
        AcmeClient client = NewClient();
        await client.GetNonceAsync(); // from directory response
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.GetNonceAsync());
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);

        _server.NextOverride = (500, null, string.Empty);
        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.GetNonceAsync());
        Assert.Equal(AcmeErrorTypes.Unknown, e.Type);
    }

    [Theory]
    [InlineData(AcmeKeyAlgorithm.ES256)]
    [InlineData(AcmeKeyAlgorithm.RS256)]
    public async Task CreateAccount_ThenFindGetAndDeactivate(AcmeKeyAlgorithm algorithm)
    {
        using AcmeAccountKey key = AcmeAccountKey.Create(algorithm);
        AcmeClient client = NewClient(key);
        Assert.Null(client.AccountUrl);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.GetAccountAsync());

        AcmeAccount created = await client.CreateAccountAsync(["mailto:a@example.test"], agreeTermsOfService: true);
        Assert.Equal(AcmeAccountStatus.Valid, created.Status);
        Assert.Equal(["mailto:a@example.test"], created.Contact);
        Assert.True(created.TermsOfServiceAgreed);
        Assert.NotNull(created.Orders);
        Assert.NotNull(created.Location);
        Assert.Equal(created.Location, client.AccountUrl);
        Assert.Equal("application/jose+json", _server.LastContentType);

        // Same key again -> existing account.
        AcmeClient second = NewClient(key);
        AcmeAccount again = await second.CreateAccountAsync(null, true);
        Assert.Equal(created.Location, again.Location);
        Assert.Equal(1, _server.AccountCount);

        AcmeClient finder = NewClient(key);
        AcmeAccount found = await finder.FindAccountAsync();
        Assert.Equal(created.Location, found.Location);
        Assert.Equal(created.Location, finder.AccountUrl);

        AcmeAccount fetched = await client.GetAccountAsync();
        Assert.Equal(AcmeAccountStatus.Valid, fetched.Status);
        Assert.Equal(created.Location, fetched.Location);

        AcmeAccount deactivated = await client.DeactivateAccountAsync();
        Assert.Equal(AcmeAccountStatus.Deactivated, deactivated.Status);
        Assert.Equal(AcmeAccountStatus.Deactivated, _server.GetAccountStatus(created.Location!));

        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.GetAccountAsync());
        Assert.Equal(AcmeErrorTypes.Unauthorized, e.Type);
        Assert.Equal(HttpStatusCode.Unauthorized, e.StatusCode);
    }

    [Fact]
    public async Task FindAccount_ForUnknownKey_Fails()
    {
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await NewClient().FindAccountAsync());
        Assert.Equal(AcmeErrorTypes.AccountDoesNotExist, e.Type);
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Equal(400, e.Problem.Status);
        Assert.NotNull(e.Problem.Detail);
    }

    [Fact]
    public async Task CreateAccount_WithoutTermsAgreement_IsRejected()
    {
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await NewClient().CreateAccountAsync(null, agreeTermsOfService: false));
        Assert.Equal(AcmeErrorTypes.Malformed, e.Type);
    }

    [Fact]
    public async Task CreateAccount_WithoutLocationHeader_IsAnError()
    {
        _server.OmitLocationOnNewAccount = true;
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await NewClient().CreateAccountAsync(null, true));
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);
    }

    [Fact]
    public async Task ExternalAccountBinding_IsRequiredVerifiedAndAccepted()
    {
        _server.RequireEab = true;
        AcmeClient noEab = NewClient();
        Assert.True((await noEab.GetDirectoryAsync()).Meta!.ExternalAccountRequired);
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await noEab.CreateAccountAsync(null, true));
        Assert.Equal(AcmeErrorTypes.ExternalAccountRequired, e.Type);

        AcmeClient wrongKid = NewClient();
        e = await Assert.ThrowsAsync<AcmeException>(async () => await wrongKid.CreateAccountAsync(null, true, new ExternalAccountBinding("nope", _server.EabHmacKeyBase64Url)));
        Assert.Equal(AcmeErrorTypes.Unauthorized, e.Type);

        AcmeClient wrongHmac = NewClient();
        e = await Assert.ThrowsAsync<AcmeException>(async () => await wrongHmac.CreateAccountAsync(null, true, new ExternalAccountBinding(_server.EabKid, Base64UrlCodec.Encode(RandomNumberGenerator.GetBytes(32)))));
        Assert.Equal(AcmeErrorTypes.Unauthorized, e.Type);

        AcmeClient ok = NewClient(AcmeAccountKey.Create(AcmeKeyAlgorithm.RS256));
        AcmeAccount account = await ok.CreateAccountAsync(["mailto:eab@example.test"], true, new ExternalAccountBinding(_server.EabKid, _server.EabHmacKeyBase64Url));
        Assert.Equal(AcmeAccountStatus.Valid, account.Status);
        Assert.Equal(1, _server.AccountCount);
    }

    [Fact]
    public async Task BadNonce_IsRetriedExactlyOnce()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        int before = _server.RequestLog.Count;

        _server.BadNonceFailuresRemaining = 1;
        AcmeAccount account = await client.GetAccountAsync();
        Assert.Equal(AcmeAccountStatus.Valid, account.Status);
        Assert.Equal(2, _server.RequestLog.Skip(before).Count(r => r.StartsWith("POST /acct/", StringComparison.Ordinal)));

        _server.BadNonceFailuresRemaining = 2;
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.GetAccountAsync());
        Assert.Equal(AcmeErrorTypes.BadNonce, e.Type);
        Assert.Equal(0, _server.BadNonceFailuresRemaining);
    }

    [Fact]
    public async Task RateLimited_SurfacesRetryAfter()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        _server.RateLimitRemaining = 1;
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.NewOrderAsync([AcmeIdentifier.Dns("x.test")]));
        Assert.Equal(AcmeErrorTypes.RateLimited, e.Type);
        Assert.Equal(HttpStatusCode.TooManyRequests, e.StatusCode);
        Assert.Equal(TimeSpan.FromSeconds(3), e.RetryAfter);
        Assert.Contains("429", e.Message);
    }

    [Fact]
    public async Task NewOrder_ReturnsPendingOrderWithAuthorizations()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        DateTimeOffset notBefore = new(2026, 10, 1, 0, 0, 0, TimeSpan.Zero);
        DateTimeOffset notAfter = new(2026, 12, 1, 0, 0, 0, TimeSpan.Zero);
        AcmeOrder order = await client.NewOrderAsync([AcmeIdentifier.Dns("a.test"), AcmeIdentifier.Ip("192.0.2.7"), AcmeIdentifier.Dns("*.wild.test")], notBefore, notAfter);

        Assert.Equal(AcmeOrderStatus.Pending, order.Status);
        Assert.NotNull(order.Location);
        Assert.Equal(3, order.Identifiers.Count);
        Assert.Equal(3, order.Authorizations.Count);
        Assert.NotNull(order.Finalize);
        Assert.NotNull(order.Expires);
        Assert.Equal(notBefore, order.NotBefore);
        Assert.Equal(notAfter, order.NotAfter);
        Assert.Null(order.Certificate);
        Assert.Null(order.Error);

        AcmeOrder fetched = await client.GetOrderAsync(order.Location!);
        Assert.Equal(order.Status, fetched.Status);
        Assert.Equal(order.Location, fetched.Location);

        AcmeAuthorization dnsAuthz = await client.GetAuthorizationAsync(order.Authorizations[0]);
        Assert.Equal(AcmeIdentifier.Dns("a.test"), dnsAuthz.Identifier);
        Assert.Equal(AcmeAuthorizationStatus.Pending, dnsAuthz.Status);
        Assert.Equal(order.Authorizations[0], dnsAuthz.Location);
        Assert.NotNull(dnsAuthz.Expires);
        Assert.Null(dnsAuthz.Wildcard);
        Assert.Equal([AcmeChallengeTypes.Http01, AcmeChallengeTypes.Dns01, AcmeChallengeTypes.TlsAlpn01], dnsAuthz.Challenges.Select(c => c.Type));

        AcmeAuthorization ipAuthz = await client.GetAuthorizationAsync(order.Authorizations[1]);
        Assert.Equal([AcmeChallengeTypes.Http01, AcmeChallengeTypes.TlsAlpn01], ipAuthz.Challenges.Select(c => c.Type));

        AcmeAuthorization wildAuthz = await client.GetAuthorizationAsync(order.Authorizations[2]);
        Assert.True(wildAuthz.Wildcard);
        Assert.Equal("wild.test", wildAuthz.Identifier.Value);
        Assert.Equal([AcmeChallengeTypes.Dns01], wildAuthz.Challenges.Select(c => c.Type));

        AcmeChallenge challenge = await client.GetChallengeAsync(dnsAuthz.Challenges[0].Url);
        Assert.Equal(AcmeChallengeStatus.Pending, challenge.Status);
        Assert.NotNull(challenge.Token);
        Assert.Null(challenge.Validated);
        Assert.Null(challenge.Error);
        Assert.Equal(dnsAuthz.Challenges[0].Url, challenge.Url);
    }

    [Fact]
    public async Task NewOrder_ValidatesArguments_AndSurfacesCompoundErrors()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.NewOrderAsync(null!));
        await Assert.ThrowsAsync<ArgumentException>(async () => await client.NewOrderAsync([]));

        _server.RejectedIdentifiers.Add("forbidden.test");
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.NewOrderAsync([AcmeIdentifier.Dns("forbidden.test"), new AcmeIdentifier("email", "x@y")]));
        Assert.Equal(AcmeErrorTypes.Compound, e.Type);
        Assert.Equal(2, e.Problem.Subproblems!.Count);
        Assert.Equal(AcmeErrorTypes.RejectedIdentifier, e.Problem.Subproblems[0].Type);
        Assert.Equal("forbidden.test", e.Problem.Subproblems[0].Identifier!.Value);
        Assert.Equal(AcmeErrorTypes.UnsupportedIdentifier, e.Problem.Subproblems[1].Type);
        Assert.Contains("forbidden.test", e.Message);

        _server.OmitLocationOnNewOrder = true;
        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.NewOrderAsync([AcmeIdentifier.Dns("a.test")]));
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);
    }

    [Fact]
    public async Task Order_OfAnotherAccount_IsUnauthorized()
    {
        AcmeClient owner = await NewClientWithAccountAsync();
        AcmeOrder order = await owner.NewOrderAsync([AcmeIdentifier.Dns("a.test")]);
        AcmeClient other = await NewClientWithAccountAsync();
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await other.GetOrderAsync(order.Location!));
        Assert.Equal(AcmeErrorTypes.Unauthorized, e.Type);
        e = await Assert.ThrowsAsync<AcmeException>(async () => await other.GetAuthorizationAsync(order.Authorizations[0]));
        Assert.Equal(AcmeErrorTypes.Unauthorized, e.Type);
        e = await Assert.ThrowsAsync<AcmeException>(async () => await other.GetOrderAsync(new Uri(_server.BaseUri, "order/nope")));
        Assert.Equal(HttpStatusCode.NotFound, e.StatusCode);
        e = await Assert.ThrowsAsync<AcmeException>(async () => await other.GetChallengeAsync(new Uri(_server.BaseUri, "unknown/route")));
        Assert.Equal(HttpStatusCode.NotFound, e.StatusCode);
    }

    [Fact]
    public async Task Challenge_RespondAndWait_HappyPath_WithRetryAfterPolling()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        _server.PendingAuthzPolls = 2;
        _server.RetryAfterSeconds = 7;
        (AcmeOrder order, AcmeAuthorization authz, AcmeChallenge challenge) = await NewValidatedOrderAsync(_server, client);

        Assert.Equal(AcmeAuthorizationStatus.Valid, authz.Status);
        AcmeChallenge done = authz.Challenges.Single(c => c.Url == challenge.Url);
        Assert.Equal(AcmeChallengeStatus.Valid, done.Status);
        Assert.NotNull(done.Validated);
        Assert.Equal([TimeSpan.FromSeconds(7), TimeSpan.FromSeconds(7)], _clock.Delays);
        Assert.Single(_server.ValidatedKeyAuthorizations);

        AcmeOrder ready = await client.WaitForOrderAsync(order.Location!);
        Assert.Equal(AcmeOrderStatus.Ready, ready.Status);
    }

    [Fact]
    public async Task Wait_UsesPollIntervalWithoutRetryAfter_AndClampsLargeValues()
    {
        AcmeClientOptions options = new() { TimeProvider = _clock, MaxPollAttempts = 5, PollInterval = TimeSpan.FromMilliseconds(250), MaxRetryAfter = TimeSpan.FromSeconds(2) };
        AcmeClient client = NewClient(options: options);
        await client.CreateAccountAsync(null, true);

        AcmeOrder order = await client.NewOrderAsync([AcmeIdentifier.Dns("a.test")]);
        // Order stays pending (no challenge answered): polls use PollInterval, then time out.
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.WaitForOrderAsync(order.Location!));
        Assert.Equal(AcmeErrorTypes.PollTimeout, e.Type);
        Assert.Equal(5, _clock.Delays.Count);
        Assert.All(_clock.Delays, d => Assert.Equal(TimeSpan.FromMilliseconds(250), d));
        _clock.Delays.Clear();

        // Retry-After larger than MaxRetryAfter is clamped.
        _server.PendingAuthzPolls = 1;
        _server.RetryAfterSeconds = 60;
        await NewValidatedOrderAsync(_server, client, "b.test");
        Assert.Equal([TimeSpan.FromSeconds(2)], _clock.Delays);
        _clock.Delays.Clear();

        // Retry-After as an HTTP date is parsed relative to the client's clock (HTTP dates have second precision).
        _clock.Now = new DateTimeOffset(2026, 9, 15, 13, 0, 0, TimeSpan.Zero);
        _server.Clock = () => _clock.Now;
        _server.RetryAfterAsHttpDate = true;
        _server.RetryAfterSeconds = 1;
        await NewValidatedOrderAsync(_server, client, "c.test");
        Assert.Equal([TimeSpan.FromSeconds(1)], _clock.Delays);
    }

    [Fact]
    public async Task Wait_HttpDateInThePast_ClampsToZero()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        _server.Clock = () => _clock.Now;
        _server.PendingAuthzPolls = 1;
        _server.RetryAfterAsHttpDate = true;
        _server.RetryAfterSeconds = -3600;
        // A negative Retry-After is clamped to zero: Task.Delay(0) completes without creating a timer
        // (a negative TimeSpan would have thrown ArgumentOutOfRangeException).
        await NewValidatedOrderAsync(_server, client, "past.test");
        Assert.Empty(_clock.Delays);
        Assert.Contains("POST /authz/", string.Join(";", _server.RequestLog));
    }

    [Fact]
    public async Task WaitForAuthorization_FailedValidation_ThrowsWithChallengeError()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        AcmeOrder order = await client.NewOrderAsync([AcmeIdentifier.Dns("fail.test")]);
        AcmeAuthorization authz = await client.GetAuthorizationAsync(order.Authorizations[0]);
        AcmeChallenge challenge = authz.Challenges[0];
        // No http-01 lookup configured -> the CA cannot fetch the key authorization.
        AcmeChallenge responded = await client.RespondToChallengeAsync(challenge.Url);
        Assert.Equal(AcmeChallengeStatus.Invalid, responded.Status);
        Assert.NotNull(responded.Error);

        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.WaitForAuthorizationAsync(order.Authorizations[0]));
        Assert.Equal(AcmeErrorTypes.ValidationFailed, e.Type);
        Assert.Equal("fail.test", e.Problem.Identifier!.Value);
        Assert.Equal(AcmeErrorTypes.IncorrectResponse, e.Problem.Subproblems!.Single().Type);

        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.WaitForOrderAsync(order.Location!));
        Assert.Equal(AcmeErrorTypes.ValidationFailed, e.Type);
        Assert.Equal(AcmeErrorTypes.IncorrectResponse, e.Problem.Subproblems!.Single().Type);

        // Responding again to a non-pending challenge is rejected by the CA.
        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.RespondToChallengeAsync(challenge.Url));
        Assert.Equal(AcmeErrorTypes.Malformed, e.Type);
    }

    [Fact]
    public async Task WaitForAuthorization_TimesOut()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        AcmeOrder order = await client.NewOrderAsync([AcmeIdentifier.Dns("slow.test")]);
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.WaitForAuthorizationAsync(order.Authorizations[0]));
        Assert.Equal(AcmeErrorTypes.PollTimeout, e.Type);
        Assert.Equal(5, _clock.Delays.Count);
    }

    [Fact]
    public async Task WaitForOrder_InvalidWithoutError_HasNoSubproblems()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        AcmeOrder order = await client.NewOrderAsync([AcmeIdentifier.Dns("a.test")]);
        _server.NextOverride = (200, "application/json", "{\"status\":\"invalid\",\"identifiers\":[],\"authorizations\":[],\"finalize\":\"" + order.Finalize + "\"}");
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.WaitForOrderAsync(order.Location!));
        Assert.Equal(AcmeErrorTypes.ValidationFailed, e.Type);
        Assert.Null(e.Problem.Subproblems);

        // Authorization in a terminal non-valid status without challenge errors.
        _server.NextOverride = (200, "application/json", "{\"identifier\":{\"type\":\"dns\",\"value\":\"a.test\"},\"status\":\"expired\",\"challenges\":[]}");
        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.WaitForAuthorizationAsync(order.Authorizations[0]));
        Assert.Equal(AcmeErrorTypes.ValidationFailed, e.Type);
        Assert.Null(e.Problem.Subproblems);
    }

    [Theory]
    [InlineData(AcmeKeyAlgorithm.ES256, AcmeKeyAlgorithm.ES256)]
    [InlineData(AcmeKeyAlgorithm.RS256, AcmeKeyAlgorithm.RS256)]
    public async Task Finalize_Download_Revoke_FullFlow(AcmeKeyAlgorithm accountAlgorithm, AcmeKeyAlgorithm certAlgorithm)
    {
        AcmeClient client = await NewClientWithAccountAsync(accountAlgorithm);
        _server.ProcessingOrderPolls = 2;
        (AcmeOrder order, _, _) = await NewValidatedOrderAsync(_server, client);
        AcmeOrder ready = await client.WaitForOrderAsync(order.Location!);
        Assert.Equal(AcmeOrderStatus.Ready, ready.Status);

        using System.Security.Cryptography.AsymmetricAlgorithm key = CsrBuilder.CreateKey(certAlgorithm);
        byte[] csr = CsrBuilder.CreateCsr(ready.Identifiers, key);
        AcmeOrder finalized = await client.FinalizeOrderAsync(ready.Finalize, csr);
        Assert.Equal(AcmeOrderStatus.Processing, finalized.Status);
        Assert.Equal(order.Location, finalized.Location);
        Assert.Null(finalized.Certificate);

        AcmeOrder valid = await client.WaitForOrderAsync(finalized.Location!);
        Assert.Equal(AcmeOrderStatus.Valid, valid.Status);
        Assert.NotNull(valid.Certificate);
        Assert.Equal(2, _clock.Delays.Count);

        AcmeCertificateChain chain = await client.DownloadCertificateAsync(valid.Certificate!);
        Assert.Equal("application/pem-certificate-chain", _server.LastAccept);
        Assert.Equal(2, chain.Certificates.Count);
        Assert.True(chain.Leaf.MatchesHostname("game.example.test"));
        Assert.Equal(_server.Ca.Root.Thumbprint, chain.Certificates[1].Thumbprint);
        Assert.Contains("BEGIN CERTIFICATE", chain.Pem);
        Uri alt = Assert.Single(chain.AlternateChainUrls);

        AcmeCertificateChain alternate = await client.DownloadCertificateAsync(alt);
        Assert.Empty(alternate.AlternateChainUrls);
        Assert.Equal(chain.Leaf.Thumbprint, alternate.Leaf.Thumbprint);
        Assert.Equal(_server.AlternateCa.Root.Thumbprint, alternate.Certificates[1].Thumbprint);

        using IssuedCertificate issued = IssuedCertificate.Create(chain.Certificates, key);
        Assert.True(issued.Certificate.HasPrivateKey);

        await client.RevokeCertificateAsync(chain.Leaf, AcmeRevocationReason.Superseded);
        Assert.Equal(1, _server.RevokedCount);
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.RevokeCertificateAsync(chain.Leaf.RawData));
        Assert.Equal(AcmeErrorTypes.AlreadyRevoked, e.Type);

        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.RevokeCertificateAsync(_server.Ca.Root.RawData));
        Assert.Equal(HttpStatusCode.NotFound, e.StatusCode);
        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.RevokeCertificateAsync(chain.Leaf.RawData, (AcmeRevocationReason)7));
        Assert.Equal(AcmeErrorTypes.BadRevocationReason, e.Type);
    }

    [Fact]
    public async Task Finalize_Errors()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        AcmeOrder pending = await client.NewOrderAsync([AcmeIdentifier.Dns("a.test")]);
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        byte[] csr = CsrBuilder.CreateCsr(pending.Identifiers, key);

        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.FinalizeOrderAsync(pending.Finalize, csr));
        Assert.Equal(AcmeErrorTypes.OrderNotReady, e.Type);
        Assert.Equal(HttpStatusCode.Forbidden, e.StatusCode);

        (AcmeOrder order, _, _) = await NewValidatedOrderAsync(_server, client, "b.test");
        byte[] wrongCsr = CsrBuilder.CreateCsr([AcmeIdentifier.Dns("other.test")], key);
        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.FinalizeOrderAsync(order.Finalize, wrongCsr));
        Assert.Equal(AcmeErrorTypes.BadCsr, e.Type);

        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.FinalizeOrderAsync(order.Finalize, [1, 2, 3]));
        Assert.Equal(AcmeErrorTypes.BadCsr, e.Type);

        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.FinalizeOrderAsync(null!, csr));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.FinalizeOrderAsync(order.Finalize, null!));

        // Finalize response without Location keeps the (null) order location.
        _server.NextOverride = (200, "application/json", "{\"status\":\"valid\",\"identifiers\":[],\"authorizations\":[],\"finalize\":\"" + order.Finalize + "\"}");
        AcmeOrder noLocation = await client.FinalizeOrderAsync(order.Finalize, csr);
        Assert.Null(noLocation.Location);
    }

    [Fact]
    public async Task Download_Errors_AndLinkParsing()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        (AcmeOrder order, _, _) = await NewValidatedOrderAsync(_server, client);
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        AcmeOrder valid = await client.FinalizeOrderAsync(order.Finalize, CsrBuilder.CreateCsr(order.Identifiers, key));
        Assert.Equal(AcmeOrderStatus.Valid, valid.Status);

        _server.ReturnEmptyPemChain = true;
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.DownloadCertificateAsync(valid.Certificate!));
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);
        _server.ReturnEmptyPemChain = false;

        _server.ReturnInvalidPem = true;
        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.DownloadCertificateAsync(valid.Certificate!));
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);
        Assert.IsType<CryptographicException>(e.InnerException);
        _server.ReturnInvalidPem = false;

        _server.OmitAlternateChainLink = true;
        _server.ExtraLinkHeader = "garbage, <not a url>;rel=\"alternate\", <https://example.test/other>;title=x, <https://example.test/alt2>; rel=alternate, x";
        AcmeCertificateChain chain = await client.DownloadCertificateAsync(valid.Certificate!);
        Assert.Equal([new Uri("https://example.test/alt2")], chain.AlternateChainUrls);

        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.DownloadCertificateAsync(null!));
        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.DownloadCertificateAsync(new Uri(_server.BaseUri, "cert/nope")));
        Assert.Equal(HttpStatusCode.NotFound, e.StatusCode);
    }

    [Fact]
    public async Task RenewalInfo_IsNull_WhenTheDirectoryHasNoEndpoint()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        (AcmeOrder order, _, _) = await NewValidatedOrderAsync(_server, client);
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        AcmeOrder valid = await client.FinalizeOrderAsync(order.Finalize, CsrBuilder.CreateCsr(order.Identifiers, key));
        AcmeCertificateChain chain = await client.DownloadCertificateAsync(valid.Certificate!);
        Assert.Null((await client.GetDirectoryAsync()).RenewalInfo);
        Assert.Null(await client.GetRenewalInfoAsync(chain.Leaf));
        Assert.DoesNotContain(_server.RequestLog, r => r.Contains("renewal-info", StringComparison.Ordinal));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.GetRenewalInfoAsync(null!));
    }

    [Fact]
    public async Task RenewalInfo_ReturnsWindowRetryAfterAndExplanation()
    {
        _server.RenewalInfoEnabled = true;
        _server.RenewalInfoRetryAfterSeconds = 1800;
        _server.RenewalInfoExplanationUrl = "https://ca.test/why";
        AcmeClient client = await NewClientWithAccountAsync();
        (AcmeOrder order, _, _) = await NewValidatedOrderAsync(_server, client);
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        AcmeOrder valid = await client.FinalizeOrderAsync(order.Finalize, CsrBuilder.CreateCsr(order.Identifiers, key));
        AcmeCertificateChain chain = await client.DownloadCertificateAsync(valid.Certificate!);

        AcmeRenewalInfo info = (await client.GetRenewalInfoAsync(chain.Leaf))!;
        DateTimeOffset notBefore = new(chain.Leaf.NotBefore.ToUniversalTime(), TimeSpan.Zero);
        DateTimeOffset notAfter = new(chain.Leaf.NotAfter.ToUniversalTime(), TimeSpan.Zero);
        TimeSpan lifetime = notAfter - notBefore;
        Assert.Equal(notBefore + lifetime * 0.5, info.SuggestedWindow.Start);
        Assert.Equal(notBefore + lifetime * 0.6, info.SuggestedWindow.End);
        Assert.Equal(TimeSpan.FromMinutes(30), info.RetryAfter);
        Assert.Equal(new Uri("https://ca.test/why"), info.ExplanationUrl);
        Assert.Equal("GET /renewal-info/" + AcmeClient.GetAriCertificateId(chain.Leaf), _server.RequestLog[^1]);

        // Random selection stays inside the window; a degenerate window yields its start.
        Random random = new(42);
        for (int i = 0; i < 100; i++)
        {
            Assert.InRange(info.SelectRenewalTime(random), info.SuggestedWindow.Start, info.SuggestedWindow.End);
        }

        AcmeRenewalInfo point = info with { SuggestedWindow = new AcmeRenewalWindow { Start = notAfter, End = notAfter } };
        Assert.Equal(notAfter, point.SelectRenewalTime(random));
        AcmeRenewalInfo inverted = info with { SuggestedWindow = new AcmeRenewalWindow { Start = notAfter, End = notBefore } };
        Assert.Equal(notAfter, inverted.SelectRenewalTime(random));
        Assert.Throws<ArgumentNullException>(() => info.SelectRenewalTime(null!));

        // Without Retry-After the property is null; a server error surfaces as AcmeException.
        _server.RenewalInfoRetryAfterSeconds = null;
        Assert.Null((await client.GetRenewalInfoAsync(chain.Leaf))!.RetryAfter);
        _server.NextOverride = (503, "application/problem+json", "{\"type\":\"urn:ietf:params:acme:error:serverInternal\",\"detail\":\"x\"}");
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.GetRenewalInfoAsync(chain.Leaf));
        Assert.Equal(AcmeErrorTypes.ServerInternal, e.Type);

        // A certificate the CA does not know is a 404 problem; a trailing slash on the endpoint is handled.
        using X509Certificate2 stranger = _server.AlternateCa.IssueLeaf("stranger.test", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.GetRenewalInfoAsync(stranger));
        Assert.Equal(HttpStatusCode.NotFound, e.StatusCode);
        AcmeClient slash = NewClient();
        _server.NextOverride = (200, "application/json", "{\"newNonce\":\"" + _server.BaseUri + "new-nonce\",\"newAccount\":\"" + _server.BaseUri + "new-account\",\"newOrder\":\"" + _server.BaseUri + "new-order\",\"renewalInfo\":\"" + _server.BaseUri + "renewal-info/\"}");
        Assert.NotNull(await slash.GetRenewalInfoAsync(chain.Leaf));
        Assert.Equal("GET /renewal-info/" + AcmeClient.GetAriCertificateId(chain.Leaf), _server.RequestLog[^1]);
    }

    [Fact]
    public void AriCertificateId_IsBase64UrlAkiDotSerial()
    {
        using TestCa ca = new("ARI Root");
        using X509Certificate2 leaf = ca.IssueLeaf("ari.test", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        X509AuthorityKeyIdentifierExtension aki = leaf.Extensions.OfType<X509AuthorityKeyIdentifierExtension>().Single();
        string expected = Base64UrlCodec.Encode(aki.KeyIdentifier!.Value.Span) + "." + Base64UrlCodec.Encode(leaf.SerialNumberBytes.Span);
        Assert.Equal(expected, AcmeClient.GetAriCertificateId(leaf));
        Assert.DoesNotContain('=', expected);

        // Without an AKI the certificate cannot be identified.
        ca.IncludeAuthorityKeyIdentifier = false;
        using X509Certificate2 noAki = ca.IssueLeaf("ari.test", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        Assert.Throws<ArgumentException>(() => AcmeClient.GetAriCertificateId(noAki));
        Assert.Throws<ArgumentNullException>(() => AcmeClient.GetAriCertificateId(null!));
    }

    [Fact]
    public async Task NewOrder_WithReplaces_IsForwarded_AndAlreadyReplacedSurfaces()
    {
        _server.RenewalInfoEnabled = true;
        AcmeClient client = await NewClientWithAccountAsync();
        (AcmeOrder order, _, _) = await NewValidatedOrderAsync(_server, client);
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        AcmeOrder valid = await client.FinalizeOrderAsync(order.Finalize, CsrBuilder.CreateCsr(order.Identifiers, key));
        AcmeCertificateChain chain = await client.DownloadCertificateAsync(valid.Certificate!);
        string id = AcmeClient.GetAriCertificateId(chain.Leaf);

        AcmeOrder renewal = await client.NewOrderAsync([AcmeIdentifier.Dns("game.example.test")], replaces: id);
        Assert.Equal(AcmeOrderStatus.Pending, renewal.Status);
        Assert.Equal([null, id], _server.ReplacesSeen);

        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.NewOrderAsync([AcmeIdentifier.Dns("game.example.test")], replaces: id));
        Assert.Equal(AcmeErrorTypes.AlreadyReplaced, e.Type);
        Assert.Equal(HttpStatusCode.Conflict, e.StatusCode);
        e = await Assert.ThrowsAsync<AcmeException>(async () => await client.NewOrderAsync([AcmeIdentifier.Dns("game.example.test")], replaces: "bm9wZQ.AQ"));
        Assert.Equal(AcmeErrorTypes.Malformed, e.Type);
    }

    [Fact]
    public async Task Revoke_WithoutDirectoryEntry_Fails()
    {
        _server.OmitRevokeCertFromDirectory = true;
        AcmeClient client = await NewClientWithAccountAsync();
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.RevokeCertificateAsync(new byte[] { 1 }));
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.RevokeCertificateAsync((byte[])null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.RevokeCertificateAsync((X509Certificate2)null!));
    }

    [Fact]
    public async Task ArgumentValidation_OnUrlMethods()
    {
        AcmeClient client = NewClient();
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.GetOrderAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.GetAuthorizationAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.GetChallengeAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.RespondToChallengeAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.WaitForAuthorizationAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.WaitForOrderAsync(null!));
    }

    [Fact]
    public async Task Server_RejectsMalformedJws()
    {
        AcmeClient client = await NewClientWithAccountAsync();
        Uri newOrder = (await client.GetDirectoryAsync()).NewOrder;

        async Task<(HttpStatusCode, string)> PostRawAsync(string body, string contentType = "application/jose+json")
        {
            using HttpRequestMessage req = new(HttpMethod.Post, newOrder) { Content = new ByteArrayContent(Encoding.UTF8.GetBytes(body)) };
            req.Content.Headers.ContentType = new MediaTypeHeaderValue(contentType);
            using HttpResponseMessage resp = await _http.SendAsync(req);
            string text = await resp.Content.ReadAsStringAsync();
            return (resp.StatusCode, text);
        }

        (HttpStatusCode status, string text) = await PostRawAsync("{}", "application/json");
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, status);

        (status, text) = await PostRawAsync("not json");
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("malformed", text);

        (status, text) = await PostRawAsync("{\"protected\":\"x\"}");
        Assert.Equal(HttpStatusCode.BadRequest, status);

        (status, text) = await PostRawAsync("{\"protected\":\"!!\",\"payload\":\"\",\"signature\":\"AA\"}");
        Assert.Equal(HttpStatusCode.BadRequest, status);

        // Valid structure but tampered signature.
        string nonce = await client.GetNonceAsync();
        JwsEnvelope good = JwsSigner.Sign(client.AccountKey, newOrder, nonce, "{\"identifiers\":[]}"u8, client.AccountUrl);
        JwsEnvelope tampered = good with { Signature = Base64UrlCodec.Encode(new byte[64]) };
        (status, text) = await PostRawAsync(JsonSerializer.Serialize(tampered, AcmeJsonContext.Default.JwsEnvelope));
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("signature verification failed", text);

        // Wrong URL in the header.
        nonce = await client.GetNonceAsync();
        JwsEnvelope wrongUrl = JwsSigner.Sign(client.AccountKey, new Uri(_server.BaseUri, "other"), nonce, "{}"u8, client.AccountUrl);
        (status, text) = await PostRawAsync(JsonSerializer.Serialize(wrongUrl, AcmeJsonContext.Default.JwsEnvelope));
        Assert.Contains("does not match request URL", text);

        // Reused nonce.
        nonce = await client.GetNonceAsync();
        JwsEnvelope first = JwsSigner.Sign(client.AccountKey, newOrder, nonce, "{\"identifiers\":[{\"type\":\"dns\",\"value\":\"n.test\"}]}"u8, client.AccountUrl);
        (status, _) = await PostRawAsync(JsonSerializer.Serialize(first, AcmeJsonContext.Default.JwsEnvelope));
        Assert.Equal(HttpStatusCode.Created, status);
        (status, text) = await PostRawAsync(JsonSerializer.Serialize(first, AcmeJsonContext.Default.JwsEnvelope));
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("badNonce", text);

        // jwk where kid is required.
        nonce = await client.GetNonceAsync();
        JwsEnvelope jwkOnOrder = JwsSigner.Sign(client.AccountKey, newOrder, nonce, "{}"u8, null);
        (status, text) = await PostRawAsync(JsonSerializer.Serialize(jwkOnOrder, AcmeJsonContext.Default.JwsEnvelope));
        Assert.Contains("only allowed for newAccount", text);

        // Unknown kid.
        using AcmeAccountKey stranger = AcmeAccountKey.Create();
        nonce = await client.GetNonceAsync();
        JwsEnvelope unknownKid = JwsSigner.Sign(stranger, newOrder, nonce, "{}"u8, new Uri(_server.BaseUri, "acct/nope"));
        (status, text) = await PostRawAsync(JsonSerializer.Serialize(unknownKid, AcmeJsonContext.Default.JwsEnvelope));
        Assert.Contains("accountDoesNotExist", text);

        // Unsupported alg / both jwk and kid / neither.
        static string Env(string header, string payload = "") => JsonSerializer.Serialize(new JwsEnvelope { Protected = Base64UrlCodec.Encode(Encoding.UTF8.GetBytes(header)), Payload = payload, Signature = Base64UrlCodec.Encode(new byte[64]) }, AcmeJsonContext.Default.JwsEnvelope);
        (status, text) = await PostRawAsync(Env("{\"alg\":\"none\",\"nonce\":\"x\",\"url\":\"" + newOrder + "\",\"kid\":\"k\"}"));
        Assert.Contains("badSignatureAlgorithm", text);
        (status, text) = await PostRawAsync(Env("{\"alg\":\"ES256\",\"nonce\":\"x\",\"url\":\"" + newOrder + "\"}"));
        Assert.Contains("Exactly one of jwk and kid", text);
        nonce = await client.GetNonceAsync();
        (status, text) = await PostRawAsync(Env("{\"alg\":\"RS256\",\"nonce\":\"" + nonce + "\",\"url\":\"" + newOrder + "\",\"kid\":\"" + client.AccountUrl + "\"}"));
        Assert.Contains("does not match the key type", text);

        // Non-JSON payload.
        nonce = await client.GetNonceAsync();
        JwsEnvelope badPayload = JwsSigner.Sign(client.AccountKey, newOrder, nonce, "not json"u8, client.AccountUrl);
        (status, text) = await PostRawAsync(JsonSerializer.Serialize(badPayload, AcmeJsonContext.Default.JwsEnvelope));
        Assert.Contains("Payload is not a JSON object", text);

        // GET on a POST-only resource.
        using HttpResponseMessage get = await _http.GetAsync(newOrder);
        Assert.Equal(HttpStatusCode.MethodNotAllowed, get.StatusCode);
    }
}
