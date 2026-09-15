using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme;

/// <summary>Options for <see cref="RenewalScheduler"/>.</summary>
public sealed class RenewalSchedulerOptions
{
    /// <summary>
    /// Fixed lead time before <c>notAfter</c> at which renewal becomes due. <see langword="null"/> (default) renews when one
    /// third of the certificate lifetime remains (30 days for a 90-day certificate). A lead time of at least the whole
    /// lifetime also falls back to the one-third rule. Ignored while ARI supplies a window.
    /// </summary>
    public TimeSpan? RenewBefore { get; set; }

    /// <summary>Longest single sleep while waiting for the renewal time (the loop re-evaluates, including ARI, after each). Default 1 hour.</summary>
    public TimeSpan CheckInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Delay before retrying after a failed renewal (after the manager's own back-off retries are exhausted). Default 1 hour.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Shortest time between two successful renewals by one <see cref="RenewalScheduler.RunAsync"/> loop, whatever the
    /// renewal rule or an ARI window says: a guard against re-ordering back to back (and being rate-limited) when a
    /// freshly issued certificate already looks due. Default 1 hour.
    /// </summary>
    public TimeSpan MinimumRenewalInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// A restart never triggers a renewal by itself: a due renewal is deferred until this long after <see cref="RenewalScheduler.RunAsync"/>
    /// started, unless the certificate expires within <see cref="ImmediateRenewalThreshold"/>. Default 1 hour.
    /// </summary>
    public TimeSpan StartupDelay { get; set; } = TimeSpan.FromHours(1);

    /// <summary>A certificate expiring within this span is renewed immediately, even on startup. Default 24 hours.</summary>
    public TimeSpan ImmediateRenewalThreshold { get; set; } = TimeSpan.FromHours(24);

    /// <summary>Use ARI (RFC 9773) renewal windows when the CA advertises <c>renewalInfo</c>. Default true.</summary>
    public bool UseRenewalInfo { get; set; } = true;

    /// <summary>
    /// How often ARI is re-fetched when the CA sends no <c>Retry-After</c>. Default 6 hours. A server <c>Retry-After</c>
    /// is honoured but clamped to <c>[1 minute, 24 hours]</c> (RFC 9773 §4.3).
    /// </summary>
    public TimeSpan RenewalInfoRefreshInterval { get; set; } = TimeSpan.FromHours(6);

    /// <summary>Clock / timer source (replace in tests).</summary>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    /// <summary>Random source used to pick a time inside the ARI window (replace in tests).</summary>
    public Random Random { get; set; } = Random.Shared;
}

/// <summary>
/// Decides when a certificate should be renewed (ARI window when available, otherwise one third of the lifetime remaining
/// or a fixed lead time) and runs a cancellation-aware renewal loop around <see cref="AcmeCertificateManager"/>.
/// </summary>
public sealed class RenewalScheduler
{
    /// <summary>Lower bound applied to an ARI <c>Retry-After</c> (RFC 9773 §4.3 suggests clamping to a sane range).</summary>
    internal static readonly TimeSpan MinAriRetryAfter = TimeSpan.FromMinutes(1);

    /// <summary>Upper bound applied to an ARI <c>Retry-After</c>.</summary>
    internal static readonly TimeSpan MaxAriRetryAfter = TimeSpan.FromHours(24);

    private readonly AcmeCertificateManager _manager;

    /// <summary>Creates a scheduler.</summary>
    /// <exception cref="ArgumentOutOfRangeException">An option is out of range.</exception>
    public RenewalScheduler(AcmeCertificateManager manager, RenewalSchedulerOptions? options = null)
    {
        _manager = manager ?? throw new ArgumentNullException(nameof(manager));
        Options = options ?? new RenewalSchedulerOptions();
        if (Options.RenewBefore < TimeSpan.Zero
            || Options.CheckInterval <= TimeSpan.Zero
            || Options.RetryDelay < TimeSpan.Zero
            || Options.MinimumRenewalInterval < TimeSpan.Zero
            || Options.StartupDelay < TimeSpan.Zero
            || Options.ImmediateRenewalThreshold < TimeSpan.Zero
            || Options.RenewalInfoRefreshInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "RenewBefore, RetryDelay, MinimumRenewalInterval, StartupDelay and ImmediateRenewalThreshold must be non-negative; CheckInterval and RenewalInfoRefreshInterval positive.");
        }

        ArgumentNullException.ThrowIfNull(Options.TimeProvider, nameof(options));
        ArgumentNullException.ThrowIfNull(Options.Random, nameof(options));
    }

    /// <summary>Scheduler options.</summary>
    public RenewalSchedulerOptions Options { get; }

    /// <summary>
    /// The instant renewal becomes due without ARI: <c>notAfter - renewBefore</c> when <paramref name="renewBefore"/> is set
    /// and shorter than the lifetime (<c>notAfter - notBefore</c>), otherwise when one third of the lifetime remains.
    /// </summary>
    /// <remarks>
    /// A lead time of at least the whole lifetime (say 30 days against a 6-day short-lived certificate) would make every
    /// new certificate due the moment it is issued and the loop would re-order back to back until rate-limited, so it
    /// falls back to the one-third rule.
    /// </remarks>
    public static DateTimeOffset GetRenewalTime(DateTimeOffset notBefore, DateTimeOffset notAfter, TimeSpan? renewBefore)
    {
        TimeSpan lifetime = notAfter - notBefore;
        if (lifetime <= TimeSpan.Zero)
        {
            return notAfter;
        }

        if (renewBefore is TimeSpan fixedLead && fixedLead < lifetime)
        {
            return notAfter - fixedLead;
        }

        return notAfter - TimeSpan.FromTicks(lifetime.Ticks / 3);
    }

    /// <summary>The non-ARI renewal time of <paramref name="certificate"/> under <see cref="RenewalSchedulerOptions.RenewBefore"/>.</summary>
    public DateTimeOffset GetRenewalTime(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        return GetRenewalTime(ToUtc(certificate.NotBefore), ToUtc(certificate.NotAfter), Options.RenewBefore);
    }

    /// <summary>True when the certificate should be renewed now according to the non-ARI rule and the option clock.</summary>
    public bool IsRenewalDue(X509Certificate2 certificate)
    {
        return Options.TimeProvider.GetUtcNow() >= GetRenewalTime(certificate);
    }

    /// <summary>
    /// Runs until <paramref name="cancellationToken"/> is cancelled: waits until renewal of the current certificate is due
    /// (immediately when <paramref name="current"/> is <see langword="null"/>; never on startup unless it expires within
    /// <see cref="RenewalSchedulerOptions.ImmediateRenewalThreshold"/>; never sooner than
    /// <see cref="RenewalSchedulerOptions.MinimumRenewalInterval"/> after the previous renewal), orders a new one (passing
    /// the ARI <c>replaces</c> identifier when ARI is in use), invokes <paramref name="onRenewed"/> and repeats. Failures
    /// are reported to <paramref name="onError"/> and retried after <see cref="RenewalSchedulerOptions.RetryDelay"/>. ARI
    /// lookups that fail silently fall back to the lifetime rule.
    /// </summary>
    /// <param name="current">The certificate currently in use, or <see langword="null"/> when none exists yet. Only its public part is retained.</param>
    /// <param name="onRenewed">Receives every newly issued certificate (the callee owns and disposes it).</param>
    /// <param name="onError">Receives renewal / callback failures.</param>
    /// <param name="cancellationToken">Stops the loop.</param>
    /// <exception cref="OperationCanceledException">Always, once the token is cancelled.</exception>
    public async Task RunAsync(
        IssuedCertificate? current,
        Func<IssuedCertificate, CancellationToken, Task> onRenewed,
        Action<Exception>? onError,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(onRenewed);
        TimeProvider clock = Options.TimeProvider;
        DateTimeOffset startedAt = clock.GetUtcNow();
        DateTimeOffset? lastRenewedAt = null;
        X509Certificate2? certificate = current is null ? null : X509CertificateLoader.LoadCertificate(current.Certificate.RawData);
        RenewalInfoState ari = new();

        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (certificate is not null)
                {
                    DateTimeOffset now = clock.GetUtcNow();
                    DateTimeOffset due = await GetDueTimeAsync(certificate, ari, now, cancellationToken).ConfigureAwait(false);
                    if (now < ToUtc(certificate.NotAfter) - Options.ImmediateRenewalThreshold)
                    {
                        DateTimeOffset earliest = startedAt + Options.StartupDelay;
                        if (due < earliest)
                        {
                            due = earliest;
                        }
                    }

                    if (lastRenewedAt is DateTimeOffset last && due < last + Options.MinimumRenewalInterval)
                    {
                        due = last + Options.MinimumRenewalInterval;
                    }

                    if (now < due)
                    {
                        TimeSpan remaining = due - now;
                        await AcmeTimers.Delay(remaining < Options.CheckInterval ? remaining : Options.CheckInterval, clock, cancellationToken).ConfigureAwait(false);
                        continue;
                    }
                }

                string? replaces = certificate is not null && ari.Available ? AcmeClient.GetAriCertificateId(certificate) : null;
                try
                {
                    IssuedCertificate issued = await _manager.OrderCertificateAsync(replaces, cancellationToken).ConfigureAwait(false);
                    certificate?.Dispose();
                    certificate = X509CertificateLoader.LoadCertificate(issued.Certificate.RawData);
                    lastRenewedAt = clock.GetUtcNow();
                    ari.Reset();
                    await onRenewed(issued, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception e)
                {
                    onError?.Invoke(e);
                    await AcmeTimers.Delay(Options.RetryDelay, clock, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            certificate?.Dispose();
        }
    }

    private async ValueTask<DateTimeOffset> GetDueTimeAsync(X509Certificate2 certificate, RenewalInfoState ari, DateTimeOffset now, CancellationToken cancellationToken)
    {
        if (Options.UseRenewalInfo && ari.NextFetch <= now)
        {
            try
            {
                AcmeClient client = await _manager.GetClientAsync(cancellationToken).ConfigureAwait(false);
                AcmeRenewalInfo? info = await client.GetRenewalInfoAsync(certificate, cancellationToken).ConfigureAwait(false);
                ari.Available = info is not null;
                ari.Update(info, Options.Random);
                ari.NextFetch = now + GetAriRefreshDelay(info?.RetryAfter, Options.RenewalInfoRefreshInterval);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                // ARI is advisory (RFC 9773 §4.3): fall back to the lifetime rule and try again after the refresh interval.
                ari.Available = false;
                ari.Update(null, Options.Random);
                ari.NextFetch = now + Options.RenewalInfoRefreshInterval;
            }
        }

        return ari.SelectedTime ?? GetRenewalTime(certificate);
    }

    /// <summary>The wait before the next ARI fetch: the server <c>Retry-After</c> clamped to <c>[1 min, 24 h]</c>, else <paramref name="fallback"/>.</summary>
    internal static TimeSpan GetAriRefreshDelay(TimeSpan? retryAfter, TimeSpan fallback)
    {
        if (retryAfter is not TimeSpan ra || ra <= TimeSpan.Zero)
        {
            return fallback;
        }

        return ra < MinAriRetryAfter ? MinAriRetryAfter : ra > MaxAriRetryAfter ? MaxAriRetryAfter : ra;
    }

    private static DateTimeOffset ToUtc(DateTime local) => new(local.ToUniversalTime(), TimeSpan.Zero);

    private sealed class RenewalInfoState
    {
        public bool Available;
        public DateTimeOffset? SelectedTime;
        public DateTimeOffset NextFetch = DateTimeOffset.MinValue;
        private AcmeRenewalWindow? _window;

        /// <summary>
        /// Adopts the fetched window. RFC 9773 intends one uniform draw per window: re-drawing on every refresh would skew
        /// renewals towards the start of the window, so the selected time is kept while the window is unchanged.
        /// </summary>
        public void Update(AcmeRenewalInfo? info, Random random)
        {
            if (info is null)
            {
                _window = null;
                SelectedTime = null;
                return;
            }

            if (_window != info.SuggestedWindow || SelectedTime is null)
            {
                _window = info.SuggestedWindow;
                SelectedTime = info.SelectRenewalTime(random);
            }
        }

        public void Reset()
        {
            Available = false;
            SelectedTime = null;
            _window = null;
            NextFetch = DateTimeOffset.MinValue;
        }
    }
}
