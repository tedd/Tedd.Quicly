using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme;

/// <summary>Options for <see cref="RenewalScheduler"/>.</summary>
public sealed class RenewalSchedulerOptions
{
    /// <summary>
    /// Fixed lead time before <c>notAfter</c> at which renewal becomes due. <see langword="null"/> (default) renews when one
    /// third of the certificate lifetime remains (30 days for a 90-day certificate). Ignored while ARI supplies a window.
    /// </summary>
    public TimeSpan? RenewBefore { get; set; }

    /// <summary>Longest single sleep while waiting for the renewal time (the loop re-evaluates, including ARI, after each). Default 1 hour.</summary>
    public TimeSpan CheckInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Delay before retrying after a failed renewal (after the manager's own back-off retries are exhausted). Default 1 hour.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// A restart never triggers a renewal by itself: a due renewal is deferred until this long after <see cref="RenewalScheduler.RunAsync"/>
    /// started, unless the certificate expires within <see cref="ImmediateRenewalThreshold"/>. Default 1 hour.
    /// </summary>
    public TimeSpan StartupDelay { get; set; } = TimeSpan.FromHours(1);

    /// <summary>A certificate expiring within this span is renewed immediately, even on startup. Default 24 hours.</summary>
    public TimeSpan ImmediateRenewalThreshold { get; set; } = TimeSpan.FromHours(24);

    /// <summary>Use ARI (RFC 9773) renewal windows when the CA advertises <c>renewalInfo</c>. Default true.</summary>
    public bool UseRenewalInfo { get; set; } = true;

    /// <summary>How often ARI is re-fetched when the CA sends no <c>Retry-After</c>. Default 6 hours.</summary>
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
            || Options.StartupDelay < TimeSpan.Zero
            || Options.ImmediateRenewalThreshold < TimeSpan.Zero
            || Options.RenewalInfoRefreshInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "RenewBefore, RetryDelay, StartupDelay and ImmediateRenewalThreshold must be non-negative; CheckInterval and RenewalInfoRefreshInterval positive.");
        }

        ArgumentNullException.ThrowIfNull(Options.TimeProvider, nameof(options));
        ArgumentNullException.ThrowIfNull(Options.Random, nameof(options));
    }

    /// <summary>Scheduler options.</summary>
    public RenewalSchedulerOptions Options { get; }

    /// <summary>
    /// The instant renewal becomes due without ARI: <c>notAfter - renewBefore</c> when <paramref name="renewBefore"/> is set,
    /// otherwise when one third of the lifetime (<c>notAfter - notBefore</c>) remains.
    /// </summary>
    public static DateTimeOffset GetRenewalTime(DateTimeOffset notBefore, DateTimeOffset notAfter, TimeSpan? renewBefore)
    {
        if (renewBefore is TimeSpan fixedLead)
        {
            return notAfter - fixedLead;
        }

        TimeSpan lifetime = notAfter - notBefore;
        if (lifetime <= TimeSpan.Zero)
        {
            return notAfter;
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
    /// <see cref="RenewalSchedulerOptions.ImmediateRenewalThreshold"/>), orders a new one (passing the ARI <c>replaces</c>
    /// identifier when ARI is in use), invokes <paramref name="onRenewed"/> and repeats. Failures are reported to
    /// <paramref name="onError"/> and retried after <see cref="RenewalSchedulerOptions.RetryDelay"/>. ARI lookups that fail
    /// silently fall back to the lifetime rule.
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

                    if (now < due)
                    {
                        TimeSpan remaining = due - now;
                        await Task.Delay(remaining < Options.CheckInterval ? remaining : Options.CheckInterval, clock, cancellationToken).ConfigureAwait(false);
                        continue;
                    }
                }

                string? replaces = certificate is not null && ari.Available ? AcmeClient.GetAriCertificateId(certificate) : null;
                try
                {
                    IssuedCertificate issued = await _manager.OrderCertificateAsync(replaces, cancellationToken).ConfigureAwait(false);
                    certificate?.Dispose();
                    certificate = X509CertificateLoader.LoadCertificate(issued.Certificate.RawData);
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
                    await Task.Delay(Options.RetryDelay, clock, cancellationToken).ConfigureAwait(false);
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
                ari.SelectedTime = info?.SelectRenewalTime(Options.Random);
                ari.NextFetch = now + (info?.RetryAfter is TimeSpan ra && ra > TimeSpan.Zero ? ra : Options.RenewalInfoRefreshInterval);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                // ARI is advisory (RFC 9773 §4.3): fall back to the lifetime rule and try again after the refresh interval.
                ari.Available = false;
                ari.SelectedTime = null;
                ari.NextFetch = now + Options.RenewalInfoRefreshInterval;
            }
        }

        return ari.SelectedTime ?? GetRenewalTime(certificate);
    }

    private static DateTimeOffset ToUtc(DateTime local) => new(local.ToUniversalTime(), TimeSpan.Zero);

    private sealed class RenewalInfoState
    {
        public bool Available;
        public DateTimeOffset? SelectedTime;
        public DateTimeOffset NextFetch = DateTimeOffset.MinValue;

        public void Reset()
        {
            Available = false;
            SelectedTime = null;
            NextFetch = DateTimeOffset.MinValue;
        }
    }
}
