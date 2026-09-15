using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Server.Certificates;

/// <summary>Configuration for <see cref="CertificateBinder"/>.</summary>
public sealed class CertificateBinderOptions
{
    /// <summary>Default <see cref="SupersededCertificateGracePeriod"/>: two minutes.</summary>
    public static readonly TimeSpan DefaultGracePeriod = TimeSpan.FromMinutes(2);

    /// <summary>Longest accepted <see cref="SupersededCertificateGracePeriod"/>.</summary>
    public static readonly TimeSpan MaxGracePeriod = TimeSpan.FromDays(30);

    /// <summary>
    /// How long a replaced certificate stays alive after every consumer switched to its successor before it is disposed.
    /// Default <see cref="DefaultGracePeriod"/>; zero disposes it at once.
    /// </summary>
    /// <remarks>
    /// A consumer switches only <em>new</em> handshakes to the new certificate. Handshakes that already selected the old
    /// one (a QUIC connection whose Initial arrived just before the swap, a TLS client that is slow to send its
    /// Finished) still need its private key for the CertificateVerify signature, and on Windows disposing a certificate
    /// imported from PKCS#12 deletes its key container. The grace period must therefore exceed the longest handshake: the
    /// MsQuic handshake idle timeout (5 s) and the HTTP header-read timeout (5 s) bound them far below the default.
    /// After the handshake TLS 1.3 no longer needs the key, so established connections are unaffected.
    /// </remarks>
    public TimeSpan SupersededCertificateGracePeriod { get; set; } = DefaultGracePeriod;

    /// <summary>
    /// Whether the binder disposes superseded certificates at all. Default <see langword="true"/>, which suits sources that
    /// hand out certificates they created (<see cref="CertificateProvisioner"/>, <see cref="FileCertificateSource"/>). Set
    /// <see langword="false"/> when the application owns the certificates the source raises and disposes them itself.
    /// </summary>
    public bool DisposeSupersededCertificates { get; set; } = true;

    /// <summary>Clock for the grace period (replace in tests).</summary>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
}

/// <summary>A consumer that threw from <see cref="ICertificateConsumer.UpdateCertificate"/>.</summary>
/// <param name="Consumer">The consumer.</param>
/// <param name="Certificate">The certificate it was given.</param>
/// <param name="Exception">What it threw.</param>
public sealed record CertificateConsumerFailure(ICertificateConsumer Consumer, X509Certificate2 Certificate, Exception Exception);

/// <summary>
/// Keeps any number of <see cref="ICertificateConsumer"/>s (the QUIC listener, the HTTPS endpoint, ...) on the current
/// certificate of an <see cref="ICertificateSource"/>: the current certificate is applied as soon as the binder is
/// created (or a consumer is added) and again on every <see cref="ICertificateSource.Changed"/>. This is how a renewal
/// hot-swaps into running listeners without a restart.
/// </summary>
/// <remarks>
/// <para>Consumers are updated serially, in the order they were added, never concurrently; one that throws is reported
/// through <see cref="ConsumerFailed"/> and the others are still updated (the failing one keeps its previous certificate
/// until the next change). A source raising the certificate that is already applied is ignored.</para>
/// <para>The certificate that was replaced is disposed after <see cref="CertificateBinderOptions.SupersededCertificateGracePeriod"/>
/// (see there for why it is not disposed at once). A certificate that the source raises again within its grace period
/// is kept. Bind each source through one binder: two binders would each dispose the certificates it replaces.</para>
/// <para>Disposing the binder unsubscribes from the source and immediately disposes the certificates still in their
/// grace period, so dispose it after the consumers have stopped. The current certificate belongs to the source and is
/// never disposed by the binder.</para>
/// </remarks>
public sealed class CertificateBinder : IDisposable, IAsyncDisposable
{
    private readonly Lock _lock = new();
    private readonly List<ICertificateConsumer> _consumers = [];
    private readonly Dictionary<X509Certificate2, ITimer?> _pending = new(ReferenceEqualityComparer.Instance);
    private X509Certificate2? _applied;
    private bool _disposed;

    /// <summary>Binds <paramref name="source"/>; add consumers with <see cref="Add(ICertificateConsumer)"/>.</summary>
    public CertificateBinder(ICertificateSource source, CertificateBinderOptions? options = null)
        : this(source, [], options)
    {
    }

    /// <summary>Binds <paramref name="source"/> to <paramref name="consumers"/> and applies the source's current certificate, if any, to each.</summary>
    /// <exception cref="ArgumentException"><paramref name="consumers"/> contains <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The grace period is negative or longer than <see cref="CertificateBinderOptions.MaxGracePeriod"/>.</exception>
    public CertificateBinder(ICertificateSource source, IEnumerable<ICertificateConsumer> consumers, CertificateBinderOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(consumers);
        Options = options ?? new CertificateBinderOptions();
        if (Options.SupersededCertificateGracePeriod < TimeSpan.Zero || Options.SupersededCertificateGracePeriod > CertificateBinderOptions.MaxGracePeriod)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "SupersededCertificateGracePeriod must be between zero and 30 days.");
        }

        ArgumentNullException.ThrowIfNull(Options.TimeProvider, nameof(options));
        foreach (ICertificateConsumer consumer in consumers)
        {
            _consumers.Add(consumer ?? throw new ArgumentException("The consumer list contains null.", nameof(consumers)));
        }

        Source = source;
        source.Changed += OnChanged;
        if (source.Current is { } current)
        {
            Apply(current);
        }
    }

    /// <summary>The bound source.</summary>
    public ICertificateSource Source { get; }

    /// <summary>The options.</summary>
    public CertificateBinderOptions Options { get; }

    /// <summary>The certificate most recently applied to the consumers, or <see langword="null"/> before the source had one.</summary>
    public X509Certificate2? Certificate
    {
        get
        {
            lock (_lock)
            {
                return _applied;
            }
        }
    }

    /// <summary>A snapshot of the consumers, in update order.</summary>
    public IReadOnlyList<ICertificateConsumer> Consumers
    {
        get
        {
            lock (_lock)
            {
                return [.. _consumers];
            }
        }
    }

    /// <summary>Number of superseded certificates waiting for their grace period to end.</summary>
    public int PendingDisposalCount
    {
        get
        {
            lock (_lock)
            {
                return _pending.Count;
            }
        }
    }

    /// <summary>Raised when a consumer throws from <see cref="ICertificateConsumer.UpdateCertificate"/>. Exceptions thrown by handlers are ignored.</summary>
    public event Action<CertificateConsumerFailure>? ConsumerFailed;

    /// <summary>Raised after a certificate was applied to every consumer. Exceptions thrown by handlers are ignored.</summary>
    public event Action<X509Certificate2>? CertificateApplied;

    /// <summary>Adds a consumer and applies the current certificate to it at once.</summary>
    /// <exception cref="ObjectDisposedException">The binder was disposed.</exception>
    public void Add(ICertificateConsumer consumer)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _consumers.Add(consumer);
            if (_applied is { } certificate)
            {
                Update(consumer, certificate);
            }
        }
    }

    /// <summary>Adds a delegate consumer (for example <c>listener.UpdateCertificate</c>) and returns it, for <see cref="Remove"/>.</summary>
    public ICertificateConsumer Add(Action<X509Certificate2> update)
    {
        ICertificateConsumer consumer = CertificateConsumers.FromDelegate(update);
        Add(consumer);
        return consumer;
    }

    /// <summary>Stops updating <paramref name="consumer"/>. Returns <see langword="false"/> when it was not bound.</summary>
    public bool Remove(ICertificateConsumer consumer)
    {
        lock (_lock)
        {
            return _consumers.Remove(consumer);
        }
    }

    /// <summary>Unsubscribes from the source and disposes the certificates still in their grace period. Idempotent.</summary>
    public void Dispose()
    {
        KeyValuePair<X509Certificate2, ITimer?>[] pending;
        lock (_lock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Source.Changed -= OnChanged;
            pending = [.. _pending];
            _pending.Clear();
        }

        foreach ((X509Certificate2 certificate, ITimer? timer) in pending)
        {
            timer?.Dispose();
            certificate.Dispose();
        }
    }

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private void OnChanged(X509Certificate2 certificate)
    {
        // A misbehaving source raising null must not reach the consumers.
        if (certificate is not null)
        {
            Apply(certificate);
        }
    }

    private void Apply(X509Certificate2 certificate)
    {
        lock (_lock)
        {
            if (_disposed || ReferenceEquals(certificate, _applied))
            {
                return;
            }

            // A certificate that comes back within its grace period is in use again and must not be disposed.
            if (_pending.Remove(certificate, out ITimer? timer))
            {
                timer?.Dispose();
            }

            X509Certificate2? previous = _applied;
            _applied = certificate;
            foreach (ICertificateConsumer consumer in _consumers.ToArray())
            {
                Update(consumer, certificate);
            }

            Raise(CertificateApplied, certificate);
            if (previous is not null && Options.DisposeSupersededCertificates)
            {
                ScheduleDisposal(previous);
            }
        }
    }

    private void Update(ICertificateConsumer consumer, X509Certificate2 certificate)
    {
        try
        {
            consumer.UpdateCertificate(certificate);
        }
        catch (Exception e)
        {
            Raise(ConsumerFailed, new CertificateConsumerFailure(consumer, certificate, e));
        }
    }

    private void ScheduleDisposal(X509Certificate2 certificate)
    {
        TimeSpan grace = Options.SupersededCertificateGracePeriod;
        if (grace == TimeSpan.Zero)
        {
            certificate.Dispose();
            return;
        }

        // Registered before the timer exists: a time provider may fire the callback synchronously inside CreateTimer.
        _pending[certificate] = null;
        ITimer created = Options.TimeProvider.CreateTimer(
            static state =>
            {
                (CertificateBinder binder, X509Certificate2 cert) = ((CertificateBinder, X509Certificate2))state!;
                binder.OnGraceElapsed(cert);
            },
            (this, certificate),
            grace,
            Timeout.InfiniteTimeSpan);
        if (_pending.ContainsKey(certificate))
        {
            _pending[certificate] = created;
        }
        else
        {
            created.Dispose();
        }
    }

    private void OnGraceElapsed(X509Certificate2 certificate)
    {
        ITimer? timer;
        lock (_lock)
        {
            // Gone when the certificate was applied again or the binder was disposed in the meantime.
            if (!_pending.Remove(certificate, out timer))
            {
                return;
            }
        }

        timer?.Dispose();
        certificate.Dispose();
    }

    private static void Raise<T>(Action<T>? handlers, T value)
    {
        if (handlers is null)
        {
            return;
        }

        foreach (Delegate handler in handlers.GetInvocationList())
        {
            try
            {
                ((Action<T>)handler)(value);
            }
            catch (Exception)
            {
                // Observers must not be able to break the binding.
            }
        }
    }
}
