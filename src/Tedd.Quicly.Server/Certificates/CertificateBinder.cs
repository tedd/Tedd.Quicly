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
/// through <see cref="ConsumerFailed"/> and the others are still updated. A source raising the certificate that is
/// already applied is ignored.</para>
/// <para>The binder tracks which certificate each consumer presents: the last one it accepted. A replaced certificate is
/// disposed <see cref="CertificateBinderOptions.SupersededCertificateGracePeriod"/> after the last consumer stopped
/// presenting it (see there for why it is not disposed at once). A consumer that threw keeps presenting its previous
/// certificate, so that certificate is kept (<see cref="RetainedCertificateCount"/>) until the consumer accepts a newer
/// one (its grace period starts then), the consumer is removed, or the binder is disposed. A certificate that the source
/// raises again within its grace period is kept. Bind each source through one binder: two binders would each dispose
/// the certificates they replace.</para>
/// <para>Disposing the binder unsubscribes from the source and immediately disposes the certificates still in their
/// grace period or retained for a consumer, so dispose it after the consumers have stopped. The current certificate
/// belongs to the source and is never disposed by the binder.</para>
/// </remarks>
public sealed class CertificateBinder : IDisposable, IAsyncDisposable
{
    private readonly Lock _lock = new();
    private readonly List<Binding> _bindings = [];
    private readonly Dictionary<X509Certificate2, PendingDisposal> _pending = new(ReferenceEqualityComparer.Instance);
    private readonly HashSet<X509Certificate2> _retained = new(ReferenceEqualityComparer.Instance);
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
            _bindings.Add(new Binding(consumer ?? throw new ArgumentException("The consumer list contains null.", nameof(consumers))));
        }

        Source = source;
        source.Changed += OnChanged;

        // Read outside the lock (a source may raise Changed while holding a lock of its own). A certificate the source
        // raised between the subscription and this read is newer than the one read here, so the value read is applied
        // only while nothing has been applied yet: applying it afterwards would roll the consumers back.
        if (source.Current is { } current)
        {
            Apply(current, onlyIfNothingApplied: true);
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
                return [.. _bindings.Select(static b => b.Consumer)];
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

    /// <summary>
    /// Number of superseded certificates kept alive because a consumer that failed to switch (see
    /// <see cref="ConsumerFailed"/>) still presents them. Non-zero means a consumer is serving an old certificate.
    /// </summary>
    public int RetainedCertificateCount
    {
        get
        {
            lock (_lock)
            {
                return _retained.Count;
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
            Binding binding = new(consumer);
            _bindings.Add(binding);
            if (_applied is { } certificate)
            {
                Update(binding, certificate);
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

    /// <summary>
    /// Stops updating <paramref name="consumer"/>. Returns <see langword="false"/> when it was not bound. A superseded
    /// certificate that was kept only because this consumer still presented it starts its grace period, so remove a
    /// consumer once it has stopped presenting the binder's certificates.
    /// </summary>
    public bool Remove(ICertificateConsumer consumer)
    {
        lock (_lock)
        {
            int index = _bindings.FindIndex(b => Equals(b.Consumer, consumer));
            if (index < 0)
            {
                return false;
            }

            X509Certificate2? held = _bindings[index].Held;
            _bindings.RemoveAt(index);
            if (held is not null)
            {
                Release(held);
            }

            return true;
        }
    }

    /// <summary>Unsubscribes from the source and disposes the certificates still in their grace period or retained for a consumer. Idempotent.</summary>
    public void Dispose()
    {
        KeyValuePair<X509Certificate2, PendingDisposal>[] pending;
        X509Certificate2[] retained;
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
            retained = [.. _retained];
            _retained.Clear();
        }

        foreach ((X509Certificate2 certificate, PendingDisposal entry) in pending)
        {
            entry.Timer?.Dispose();
            certificate.Dispose();
        }

        foreach (X509Certificate2 certificate in retained)
        {
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
            Apply(certificate, onlyIfNothingApplied: false);
        }
    }

    private void Apply(X509Certificate2 certificate, bool onlyIfNothingApplied)
    {
        lock (_lock)
        {
            if (_disposed || ReferenceEquals(certificate, _applied) || (onlyIfNothingApplied && _applied is not null))
            {
                return;
            }

            // A certificate that comes back within its grace period (or while retained) is in use again: keep it.
            if (_pending.Remove(certificate, out PendingDisposal? entry))
            {
                entry.Timer?.Dispose();
            }

            _retained.Remove(certificate);
            X509Certificate2? previous = _applied;
            _applied = certificate;
            List<X509Certificate2>? released = null;
            foreach (Binding binding in _bindings.ToArray())
            {
                X509Certificate2? before = binding.Held;
                if (Update(binding, certificate) && before is not null && !ReferenceEquals(before, certificate))
                {
                    (released ??= []).Add(before);
                }
            }

            Raise(CertificateApplied, certificate);
            if (previous is not null)
            {
                Release(previous);
            }

            if (released is not null)
            {
                foreach (X509Certificate2 stale in released)
                {
                    Release(stale);
                }
            }
        }
    }

    /// <summary>Hands <paramref name="certificate"/> to one consumer; true when it accepted it (it presents it from then on).</summary>
    private bool Update(Binding binding, X509Certificate2 certificate)
    {
        try
        {
            binding.Consumer.UpdateCertificate(certificate);
            binding.Held = certificate;
            return true;
        }
        catch (Exception e)
        {
            // By contract the consumer keeps presenting what it had, so Held stays as it was.
            Raise(ConsumerFailed, new CertificateConsumerFailure(binding.Consumer, certificate, e));
            return false;
        }
    }

    /// <summary>
    /// Called under the lock for a certificate that may no longer be needed: starts its grace period when no consumer
    /// presents it, or keeps it (retained) while one still does. The current certificate is never released.
    /// </summary>
    private void Release(X509Certificate2 certificate)
    {
        if (!Options.DisposeSupersededCertificates || ReferenceEquals(certificate, _applied) || _pending.ContainsKey(certificate))
        {
            return;
        }

        if (IsHeld(certificate))
        {
            _retained.Add(certificate);
            return;
        }

        _retained.Remove(certificate);
        ScheduleDisposal(certificate);
    }

    private bool IsHeld(X509Certificate2 certificate)
    {
        foreach (Binding binding in _bindings)
        {
            if (ReferenceEquals(binding.Held, certificate))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>True while the certificate is current, waiting for its grace period, retained, or presented by a consumer.</summary>
    private bool IsInUse(X509Certificate2 certificate)
    {
        return ReferenceEquals(certificate, _applied) || _pending.ContainsKey(certificate) || _retained.Contains(certificate) || IsHeld(certificate);
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
        PendingDisposal entry = new();
        _pending[certificate] = entry;
        ITimer created = Options.TimeProvider.CreateTimer(
            static state =>
            {
                (CertificateBinder binder, X509Certificate2 cert, PendingDisposal registration) = ((CertificateBinder, X509Certificate2, PendingDisposal))state!;
                binder.OnGraceElapsed(cert, registration);
            },
            (this, certificate, entry),
            grace,
            Timeout.InfiniteTimeSpan);
        if (IsPending(certificate, entry))
        {
            entry.Timer = created;
        }
        else
        {
            created.Dispose();
        }
    }

    private bool IsPending(X509Certificate2 certificate, PendingDisposal entry)
    {
        return _pending.TryGetValue(certificate, out PendingDisposal? current) && ReferenceEquals(current, entry);
    }

    private void OnGraceElapsed(X509Certificate2 certificate, PendingDisposal entry)
    {
        lock (_lock)
        {
            // Not this registration any more when the certificate was applied again (and perhaps superseded again, with a
            // new timer) or the binder was disposed in the meantime: a callback already on its way when its timer was
            // stopped must not end the new grace period early.
            if (!IsPending(certificate, entry))
            {
                return;
            }

            _pending.Remove(certificate);
        }

        // The timer is released outside the lock, so other threads may run meanwhile and the source may raise this
        // certificate again: it is disposed only if it is still unused once the lock is held again.
        entry.Timer?.Dispose();
        lock (_lock)
        {
            if (!IsInUse(certificate))
            {
                certificate.Dispose();
            }
        }
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

    /// <summary>A consumer and the certificate it presents (the last one it accepted), or <see langword="null"/> before it accepted any.</summary>
    private sealed class Binding(ICertificateConsumer consumer)
    {
        public ICertificateConsumer Consumer { get; } = consumer;

        public X509Certificate2? Held { get; set; }
    }

    /// <summary>One grace period: identifies the timer callback that may end it (a certificate can be scheduled more than once).</summary>
    private sealed class PendingDisposal
    {
        public ITimer? Timer { get; set; }
    }
}
