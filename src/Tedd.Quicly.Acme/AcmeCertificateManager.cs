using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme;

/// <summary>
/// Runs the complete certificate order flow: create / reuse the account, create (or resume) the order, satisfy one
/// challenge per authorization through the configured responders (always cleaning up afterwards), finalize with a CSR,
/// download the chain, combine it with the private key and optionally persist it as PKCS#12. The in-flight order and
/// its key are persisted to the <see cref="AcmeAccountStore"/> so a crash can resume; transient failures are retried
/// with exponential back-off and jitter honouring <c>Retry-After</c> (ADR 0009).
/// </summary>
/// <remarks>
/// The manager owns the account key it loads from the store or creates; <see cref="Dispose"/> releases it, after which
/// the client returned by <see cref="GetClientAsync"/> can no longer sign.
/// </remarks>
public sealed class AcmeCertificateManager : IDisposable
{
    private readonly HttpClient _http;
    private AcmeClient? _client;
    private bool _disposed;

    /// <summary>Creates a manager.</summary>
    /// <param name="httpClient">Injected HTTP client (not disposed by this class).</param>
    /// <param name="options">Configuration.</param>
    /// <exception cref="ArgumentException">No identifiers, or <see cref="AcmeCertificateManagerOptions.ReuseKey"/> without a certificate path.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A delay option is out of range.</exception>
    public AcmeCertificateManager(HttpClient httpClient, AcmeCertificateManagerOptions options)
    {
        _http = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        Options = options ?? throw new ArgumentNullException(nameof(options));
        if (options.Identifiers.Count == 0)
        {
            throw new ArgumentException("At least one identifier is required.", nameof(options));
        }

        if (options.ReuseKey && options.CertificatePath is null)
        {
            throw new ArgumentException("ReuseKey requires CertificatePath (the key is taken from the persisted certificate).", nameof(options));
        }

        if (options.ChallengeCleanupTimeout <= TimeSpan.Zero || options.ChallengeCleanupTimeout > AcmeTimers.MaxDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "ChallengeCleanupTimeout must be positive and at most 30 days.");
        }

        options.Retry.Validate();
    }

    /// <summary>Configuration.</summary>
    public AcmeCertificateManagerOptions Options { get; }

    /// <summary>Raised at every stage of the flow. Handlers should not throw; exceptions they throw are ignored.</summary>
    public event Action<AcmeProgress>? Progress;

    /// <summary>Loads the certificate previously persisted to <see cref="AcmeCertificateManagerOptions.CertificatePath"/>, or <see langword="null"/>.</summary>
    public IssuedCertificate? TryLoadPersistedCertificate()
    {
        string? path = Options.CertificatePath;
        return path is not null && File.Exists(path) ? IssuedCertificate.LoadFile(path, Options.CertificatePassword) : null;
    }

    /// <summary>
    /// Returns the client bound to the (created or reused) account. A stored account is verified once (POST-as-GET of the
    /// account URL); when the CA no longer knows it (<c>accountDoesNotExist</c>), refuses it (<c>unauthorized</c>) or
    /// reports it as not <c>valid</c> (deactivated / revoked, or the CA was reset), a new account with a new key replaces it.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The manager was disposed.</exception>
    public async ValueTask<AcmeClient> GetClientAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_client is { } existing)
        {
            return existing;
        }

        AcmeAccountState? state = Options.AccountStore.Load();
        AcmeClient? client = null;
        string createReason = "Created account ";
        if (state is not null && state.DirectoryUrl.Equals(Options.DirectoryUrl))
        {
            client = await TryReuseAccountAsync(state, cancellationToken).ConfigureAwait(false);
            createReason = "Stored account " + state.AccountUrl + " is no longer usable; created account ";
        }
        else if (state is not null)
        {
            createReason = "Directory changed; created new account ";
        }

        client ??= await CreateAccountAsync(createReason, cancellationToken).ConfigureAwait(false);
        _client = client;
        return client;
    }

    /// <summary>Orders a certificate for <see cref="AcmeCertificateManagerOptions.Identifiers"/>.</summary>
    /// <exception cref="AcmeException">The CA rejected a step, validation failed, or no responder supports the offered challenges.</exception>
    public Task<IssuedCertificate> OrderCertificateAsync(CancellationToken cancellationToken = default)
    {
        return OrderCertificateAsync(null, cancellationToken);
    }

    /// <summary>
    /// Orders a certificate, marking it as the replacement of an existing one (ARI <c>replaces</c>, RFC 9773 §5). When the
    /// CA rejects the <paramref name="replaces"/> value (<c>alreadyReplaced</c>, or <c>malformed</c> about <c>replaces</c>)
    /// the order is retried once without it.
    /// </summary>
    /// <param name="replaces">ARI certificate identifier (<see cref="AcmeClient.GetAriCertificateId"/>) or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <exception cref="AcmeException">The CA rejected a step, validation failed, or no responder supports the offered challenges.</exception>
    public async Task<IssuedCertificate> OrderCertificateAsync(string? replaces, CancellationToken cancellationToken = default)
    {
        AcmeRetryOptions retry = Options.Retry;
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return await OrderCoreAsync(replaces, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (!(e is OperationCanceledException && cancellationToken.IsCancellationRequested))
            {
                // An OperationCanceledException not caused by our token is an HttpClient timeout: classified by IsTransient.
                bool retrying = attempt < retry.MaxAttempts && AcmeRetryOptions.IsTransient(e);
                Report(retrying ? AcmeStage.Retrying : AcmeStage.Failed, e.Message);
                if (!retrying)
                {
                    throw;
                }

                TimeSpan delay = retry.GetDelay(attempt, (e as AcmeException)?.RetryAfter);
                await AcmeTimers.Delay(delay, Options.ClientOptions.TimeProvider, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Revokes a certificate using the account.</summary>
    public async Task RevokeAsync(X509Certificate2 certificate, AcmeRevocationReason? reason = null, CancellationToken cancellationToken = default)
    {
        AcmeClient client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
        await client.RevokeCertificateAsync(certificate, reason, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Releases the account key held by the cached client. Idempotent.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _client?.AccountKey.Dispose();
        _client = null;
    }

    private async Task<AcmeClient?> TryReuseAccountAsync(AcmeAccountState state, CancellationToken cancellationToken)
    {
        AcmeAccountKey key = AcmeAccountKey.Import(state.PrivateKeyPem);
        try
        {
            AcmeClient client = new(_http, Options.DirectoryUrl, key, state.AccountUrl, Options.ClientOptions);
            await client.GetDirectoryAsync(cancellationToken).ConfigureAwait(false);
            Report(AcmeStage.DirectoryFetched, "Directory " + Options.DirectoryUrl);

            string? unusable = null;
            try
            {
                AcmeAccount account = await client.GetAccountAsync(cancellationToken).ConfigureAwait(false);
                if (!string.Equals(account.Status, AcmeAccountStatus.Valid, StringComparison.Ordinal))
                {
                    unusable = "status " + account.Status;
                }
            }
            catch (AcmeException e) when (e.IsType(AcmeErrorTypes.AccountDoesNotExist) || e.IsType(AcmeErrorTypes.Unauthorized))
            {
                unusable = e.Type;
            }

            if (unusable is not null)
            {
                Report(AcmeStage.AccountReady, "Stored account " + state.AccountUrl + " is not usable (" + unusable + "); creating a new account");
                key.Dispose();
                return null;
            }

            Report(AcmeStage.AccountReady, "Reusing account " + state.AccountUrl);
            return client;
        }
        catch
        {
            key.Dispose();
            throw;
        }
    }

    private async Task<AcmeClient> CreateAccountAsync(string reason, CancellationToken cancellationToken)
    {
        AcmeAccountKey key = AcmeAccountKey.Create(Options.AccountKeyAlgorithm);
        try
        {
            AcmeClient client = new(_http, Options.DirectoryUrl, key, null, Options.ClientOptions);
            AcmeDirectory directory = await client.GetDirectoryAsync(cancellationToken).ConfigureAwait(false);
            Report(AcmeStage.DirectoryFetched, "Directory " + Options.DirectoryUrl + (directory.Meta?.ExternalAccountRequired == true ? " (EAB required)" : string.Empty));
            AcmeAccount account = await client.CreateAccountAsync(Options.Contacts, Options.AgreeTermsOfService, Options.ExternalAccountBinding, onlyReturnExisting: false, cancellationToken).ConfigureAwait(false);

            // A new state has no pending order: orders belong to the account that created them.
            Options.AccountStore.Save(AcmeAccountStore.CreateState(key, account.Location!, Options.DirectoryUrl));
            Report(AcmeStage.AccountReady, reason + account.Location);
            return client;
        }
        catch
        {
            key.Dispose();
            throw;
        }
    }

    private async Task<IssuedCertificate> OrderCoreAsync(string? replaces, CancellationToken cancellationToken)
    {
        AcmeClient client = await GetClientAsync(cancellationToken).ConfigureAwait(false);

        (AcmeOrder order, AsymmetricAlgorithm certificateKey) = await ResumeOrCreateOrderAsync(client, replaces, cancellationToken).ConfigureAwait(false);
        using (certificateKey)
        {
            // The order URL is kept here: finalize responses need not carry a Location header.
            Uri orderUrl = order.Location!;
            for (int i = 0; i < order.Authorizations.Count; i++)
            {
                await CompleteAuthorizationAsync(client, order.Authorizations[i], cancellationToken).ConfigureAwait(false);
            }

            order = await client.WaitForOrderAsync(orderUrl, cancellationToken).ConfigureAwait(false);
            Report(AcmeStage.OrderReady, "Order status " + order.Status);

            if (string.Equals(order.Status, AcmeOrderStatus.Ready, StringComparison.Ordinal))
            {
                byte[] csr = CsrBuilder.CreateCsr(Options.Identifiers, certificateKey);
                order = await client.FinalizeOrderAsync(order.Finalize, csr, cancellationToken).ConfigureAwait(false);
                Report(AcmeStage.OrderFinalized, "Finalized; status " + order.Status);
                if (!string.Equals(order.Status, AcmeOrderStatus.Valid, StringComparison.Ordinal))
                {
                    order = await client.WaitForOrderAsync(orderUrl, cancellationToken).ConfigureAwait(false);
                }
            }

            Uri certificateUrl = order.Certificate
                ?? throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "The order is " + order.Status + " but carries no certificate URL.");
            using AcmeCertificateChain chain = await SelectChainAsync(client, certificateUrl, cancellationToken).ConfigureAwait(false);
            Report(AcmeStage.CertificateDownloaded, chain.Certificates.Count + " certificate(s); leaf " + chain.Leaf.Subject + " expires " + chain.Leaf.NotAfter.ToUniversalTime().ToString("u", System.Globalization.CultureInfo.InvariantCulture));

            IssuedCertificate issued = IssuedCertificate.Create(chain.Certificates, certificateKey, Options.CertificatePassword);
            if (Options.CertificatePath is { } path)
            {
                issued.Save(path);
                Report(AcmeStage.CertificatePersisted, path);
            }

            Options.AccountStore.SavePendingOrder(null);
            return issued;
        }
    }

    /// <summary>
    /// Resumes the order persisted by an earlier run when it is for the same identifiers and still usable at the CA;
    /// otherwise creates a new order (and its certificate key) and persists both before any challenge is touched.
    /// </summary>
    private async Task<(AcmeOrder Order, AsymmetricAlgorithm Key)> ResumeOrCreateOrderAsync(AcmeClient client, string? replaces, CancellationToken cancellationToken)
    {
        DateTimeOffset now = Options.ClientOptions.TimeProvider.GetUtcNow();
        AcmePendingOrder? pending = Options.AccountStore.Load()?.PendingOrder;
        if (pending is not null)
        {
            if (SameIdentifiers(pending.Identifiers, Options.Identifiers) && (pending.Expires is null || pending.Expires > now))
            {
                AcmeOrder? existing = null;
                try
                {
                    existing = await client.GetOrderAsync(pending.OrderUrl, cancellationToken).ConfigureAwait(false);
                }
                catch (AcmeException e)
                {
                    Report(AcmeStage.OrderResumed, "Persisted order " + pending.OrderUrl + " could not be fetched (" + e.Type + "); starting a new order");
                }

                if (existing is not null && !string.Equals(existing.Status, AcmeOrderStatus.Invalid, StringComparison.Ordinal))
                {
                    Report(AcmeStage.OrderResumed, "Resuming order " + pending.OrderUrl + " (status " + existing.Status + ", " + existing.Authorizations.Count + " authorizations)");
                    return (existing, CsrBuilder.ImportKeyPem(pending.CertificateKeyPem));
                }
            }

            Options.AccountStore.SavePendingOrder(null);
        }

        AsymmetricAlgorithm key = (Options.ReuseKey ? LoadReusableKey() : null) ?? CsrBuilder.CreateKey(Options.CertificateKeyAlgorithm, Options.RsaKeySizeBits);
        try
        {
            AcmeOrder order = await NewOrderAsync(client, replaces, cancellationToken).ConfigureAwait(false);
            Options.AccountStore.SavePendingOrder(new AcmePendingOrder
            {
                OrderUrl = order.Location!,
                Identifiers = Options.Identifiers,
                CertificateKeyPem = CsrBuilder.ExportKeyPem(key),
                Expires = order.Expires,
                CreatedAt = now,
            });
            Report(AcmeStage.OrderCreated, "Order " + order.Location + " (" + order.Authorizations.Count + " authorizations)");
            return (order, key);
        }
        catch
        {
            key.Dispose();
            throw;
        }
    }

    private async Task<AcmeOrder> NewOrderAsync(AcmeClient client, string? replaces, CancellationToken cancellationToken)
    {
        try
        {
            return await client.NewOrderAsync(Options.Identifiers, Options.NotBefore, Options.NotAfter, replaces, cancellationToken).ConfigureAwait(false);
        }
        catch (AcmeException e) when (replaces is not null && IsReplacesRejection(e))
        {
            // RFC 9773 §5: a CA may reject the replaces field (alreadyReplaced, unknown certificate); order without it.
            Report(AcmeStage.OrderCreated, "CA rejected replaces=" + replaces + " (" + e.Type + "); ordering without it");
            return await client.NewOrderAsync(Options.Identifiers, Options.NotBefore, Options.NotAfter, null, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// True for errors that concern the <c>replaces</c> field itself: <c>alreadyReplaced</c>, or <c>malformed</c> whose
    /// detail names <c>replaces</c>. Anything else (rejectedIdentifier, caa, unauthorized, ...) would fail the same way
    /// without it, so it is not worth a second request.
    /// </summary>
    internal static bool IsReplacesRejection(AcmeException exception)
    {
        return exception.IsType(AcmeErrorTypes.AlreadyReplaced)
            || (exception.IsType(AcmeErrorTypes.Malformed) && exception.Problem.Detail?.Contains("replaces", StringComparison.OrdinalIgnoreCase) == true);
    }

    /// <summary>Returns the persisted certificate's private key as a standalone key, or <see langword="null"/> when nothing is persisted.</summary>
    private AsymmetricAlgorithm? LoadReusableKey()
    {
        using IssuedCertificate? current = TryLoadPersistedCertificate();
        if (current is null)
        {
            return null;
        }

        using ECDsa? ecdsa = current.Certificate.GetECDsaPrivateKey();
        if (ecdsa is not null)
        {
            return CsrBuilder.ImportKeyPem(ecdsa.ExportPkcs8PrivateKeyPem());
        }

        using RSA rsa = current.Certificate.GetRSAPrivateKey()!;
        return CsrBuilder.ImportKeyPem(rsa.ExportPkcs8PrivateKeyPem());
    }

    /// <summary>True when both lists hold the same identifiers as sets (DNS names case-insensitively), i.e. each contains the other.</summary>
    internal static bool SameIdentifiers(IReadOnlyList<AcmeIdentifier> a, IReadOnlyList<AcmeIdentifier> b)
    {
        return a.Count == b.Count && ContainsAll(a, b) && ContainsAll(b, a);

        static bool ContainsAll(IReadOnlyList<AcmeIdentifier> haystack, IReadOnlyList<AcmeIdentifier> needles)
        {
            for (int i = 0; i < needles.Count; i++)
            {
                bool found = false;
                for (int j = 0; j < haystack.Count && !found; j++)
                {
                    found = string.Equals(needles[i].Type, haystack[j].Type, StringComparison.Ordinal)
                        && string.Equals(needles[i].Value, haystack[j].Value, StringComparison.OrdinalIgnoreCase);
                }

                if (!found)
                {
                    return false;
                }
            }

            return true;
        }
    }

    private async Task CompleteAuthorizationAsync(AcmeClient client, Uri authorizationUrl, CancellationToken cancellationToken)
    {
        AcmeAuthorization authz = await client.GetAuthorizationAsync(authorizationUrl, cancellationToken).ConfigureAwait(false);
        AcmeIdentifier identifier = authz.Identifier;
        if (string.Equals(authz.Status, AcmeAuthorizationStatus.Valid, StringComparison.Ordinal))
        {
            Report(AcmeStage.AuthorizationSkipped, "Already valid", identifier);
            return;
        }

        if (!string.Equals(authz.Status, AcmeAuthorizationStatus.Pending, StringComparison.Ordinal))
        {
            throw new AcmeException(new AcmeProblem
            {
                Type = AcmeErrorTypes.ValidationFailed,
                Detail = "Authorization for " + identifier + " is '" + authz.Status + "' and cannot be completed.",
                Identifier = identifier,
            });
        }

        AcmeChallenge challenge = SelectChallenge(authz);
        Report(AcmeStage.ChallengeSelected, challenge.Type, identifier);
        string token = challenge.Token ?? throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "Challenge " + challenge.Url + " has no token.");
        if (!ChallengeToken.IsValid(token))
        {
            throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "Challenge " + challenge.Url + " has a token that does not match [A-Za-z0-9_-]{22,}.");
        }

        string keyAuthorization = KeyAuthorization.Compute(token, client.AccountKey);
        string domain = authz.Wildcard == true ? "*." + identifier.Value : identifier.Value;

        X509Certificate2? alpnCertificate = null;
        string? txtName = null;
        string? txtValue = null;
        try
        {
            switch (challenge.Type)
            {
                case AcmeChallengeTypes.Http01:
                    await Options.Http01Responder!.PublishAsync(token, keyAuthorization, cancellationToken).ConfigureAwait(false);
                    break;
                case AcmeChallengeTypes.Dns01:
                    txtName = Dns01Challenge.GetRecordName(domain);
                    txtValue = Dns01Challenge.ComputeTxtValue(keyAuthorization);
                    await Options.Dns01Provider!.CreateTxtAsync(txtName, txtValue, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    alpnCertificate = TlsAlpn01Challenge.CreateCertificate(identifier.Value, keyAuthorization);
                    await Options.TlsAlpn01Responder!.PublishAsync(identifier.Value, alpnCertificate, cancellationToken).ConfigureAwait(false);
                    break;
            }

            Report(AcmeStage.ChallengePublished, challenge.Type + " response published", identifier);
            if (Options.ChallengePropagationDelay > TimeSpan.Zero)
            {
                await AcmeTimers.Delay(Options.ChallengePropagationDelay, Options.ClientOptions.TimeProvider, cancellationToken).ConfigureAwait(false);
            }

            // A resumed order may find the challenge already responded to (processing); posting again is an error at
            // some CAs, so only wait for it then. The response material is still (re)published above.
            if (string.Equals(challenge.Status, AcmeChallengeStatus.Pending, StringComparison.Ordinal))
            {
                await client.RespondToChallengeAsync(challenge.Url, cancellationToken).ConfigureAwait(false);
                Report(AcmeStage.ChallengeResponded, challenge.Url.ToString(), identifier);
            }
            else
            {
                Report(AcmeStage.ChallengeResponded, challenge.Url + " already " + challenge.Status + "; waiting", identifier);
            }

            await client.WaitForAuthorizationAsync(authorizationUrl, cancellationToken).ConfigureAwait(false);
            Report(AcmeStage.AuthorizationValid, "Validated via " + challenge.Type, identifier);
        }
        finally
        {
            await CleanupAsync(challenge.Type, token, domain, identifier, txtName, txtValue, alpnCertificate).ConfigureAwait(false);
        }
    }

    private async Task CleanupAsync(string challengeType, string token, string domain, AcmeIdentifier identifier, string? txtName, string? txtValue, X509Certificate2? alpnCertificate)
    {
        // Cleanup runs even when the caller was cancelled, so it does not use the caller's token; a wall-clock bound
        // (not the client TimeProvider, which tests replace) stops a hanging responder from blocking the flow forever.
        using CancellationTokenSource timeout = new(Options.ChallengeCleanupTimeout, TimeProvider.System);
        try
        {
            switch (challengeType)
            {
                case AcmeChallengeTypes.Http01:
                    await Options.Http01Responder!.RemoveAsync(token, timeout.Token).AsTask().WaitAsync(timeout.Token).ConfigureAwait(false);
                    break;
                case AcmeChallengeTypes.Dns01:
                    await Options.Dns01Provider!.RemoveTxtAsync(txtName!, txtValue!, timeout.Token).AsTask().WaitAsync(timeout.Token).ConfigureAwait(false);
                    break;
                default:
                    await Options.TlsAlpn01Responder!.RemoveAsync(identifier.Value, timeout.Token).AsTask().WaitAsync(timeout.Token).ConfigureAwait(false);
                    break;
            }

            Report(AcmeStage.ChallengeCleanedUp, challengeType + " response removed for " + domain, identifier);
        }
        catch (Exception e)
        {
            // A failed or timed-out cleanup must not mask the outcome of the validation (Report never throws).
            string reason = timeout.IsCancellationRequested ? "timed out after " + Options.ChallengeCleanupTimeout : e.Message;
            Report(AcmeStage.ChallengeCleanedUp, "Cleanup failed: " + reason, identifier);
        }
        finally
        {
            alpnCertificate?.Dispose();
        }
    }

    private AcmeChallenge SelectChallenge(AcmeAuthorization authz)
    {
        IReadOnlyList<string> preferred = Options.PreferredChallengeTypes;
        for (int p = 0; p < preferred.Count; p++)
        {
            if (!HasResponder(preferred[p]))
            {
                continue;
            }

            for (int c = 0; c < authz.Challenges.Count; c++)
            {
                if (string.Equals(authz.Challenges[c].Type, preferred[p], StringComparison.Ordinal))
                {
                    return authz.Challenges[c];
                }
            }
        }

        throw new AcmeException(new AcmeProblem
        {
            Type = AcmeErrorTypes.NoSupportedChallenge,
            Detail = "No configured responder matches the challenges offered for " + authz.Identifier + ".",
            Identifier = authz.Identifier,
        });
    }

    private bool HasResponder(string challengeType)
    {
        return challengeType switch
        {
            AcmeChallengeTypes.Http01 => Options.Http01Responder is not null,
            AcmeChallengeTypes.Dns01 => Options.Dns01Provider is not null,
            AcmeChallengeTypes.TlsAlpn01 => Options.TlsAlpn01Responder is not null,
            _ => false,
        };
    }

    private async Task<AcmeCertificateChain> SelectChainAsync(AcmeClient client, Uri certificateUrl, CancellationToken cancellationToken)
    {
        AcmeCertificateChain chain = await client.DownloadCertificateAsync(certificateUrl, cancellationToken).ConfigureAwait(false);
        string? preferred = Options.PreferredChainIssuer;
        if (preferred is null || ChainMatches(chain, preferred))
        {
            return chain;
        }

        try
        {
            for (int i = 0; i < chain.AlternateChainUrls.Count; i++)
            {
                AcmeCertificateChain alternate = await client.DownloadCertificateAsync(chain.AlternateChainUrls[i], cancellationToken).ConfigureAwait(false);
                if (ChainMatches(alternate, preferred))
                {
                    chain.Dispose();
                    return alternate;
                }

                alternate.Dispose();
            }
        }
        catch
        {
            chain.Dispose();
            throw;
        }

        return chain;
    }

    private static bool ChainMatches(AcmeCertificateChain chain, string preferredIssuer)
    {
        X509Certificate2 top = chain.Certificates[chain.Certificates.Count - 1];
        return top.Issuer.Contains(preferredIssuer, StringComparison.OrdinalIgnoreCase);
    }

    private void Report(AcmeStage stage, string message, AcmeIdentifier? identifier = null)
    {
        try
        {
            Progress?.Invoke(new AcmeProgress(stage, message, identifier));
        }
        catch (Exception)
        {
            // Progress is observational: a throwing handler must not change the outcome of the flow (or of its cleanup).
        }
    }
}
