using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Http.Tls;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.Server.Tests;

public sealed class CertificateBinderTests : IDisposable
{
    private readonly List<X509Certificate2> _certificates = [];
    private readonly ManualTimeProvider _time = new();

    public void Dispose()
    {
        foreach (X509Certificate2 certificate in _certificates)
        {
            certificate.Dispose();
        }
    }

    private X509Certificate2 NewCertificate()
    {
        X509Certificate2 certificate = Certs.Create();
        _certificates.Add(certificate);
        return certificate;
    }

    private CertificateBinderOptions Grace(TimeSpan grace) => new() { SupersededCertificateGracePeriod = grace, TimeProvider = _time };

    [Fact]
    public void AppliesTheCurrentCertificateAtOnce_AndEveryChange_InOrder()
    {
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        TestSource source = new() { Current = a };
        List<string> order = [];
        RecordingConsumer first = new();
        RecordingConsumer second = new();
        using CertificateBinder binder = new(source, [first, second, CertificateConsumers.FromDelegate(_ => order.Add("delegate"))], Grace(TimeSpan.FromMinutes(1)));
        List<X509Certificate2> applied = [];
        binder.CertificateApplied += applied.Add;

        Assert.Same(a, binder.Certificate);
        Assert.Same(a, Assert.Single(first.Received));
        Assert.Same(a, Assert.Single(second.Received));
        Assert.Single(order);
        Assert.Equal(3, binder.Consumers.Count);
        Assert.Same(source, binder.Source);

        source.Raise(b);

        Assert.Equal([a, b], first.Received);
        Assert.Equal([a, b], second.Received);
        Assert.Same(b, Assert.Single(applied));
        Assert.Same(b, binder.Certificate);
    }

    [Fact]
    public void SourceWithoutCertificate_AppliesNothingUntilTheFirstChange()
    {
        TestSource source = new();
        RecordingConsumer consumer = new();
        using CertificateBinder binder = new(source, [consumer]);

        Assert.Null(binder.Certificate);
        Assert.Empty(consumer.Received);
        Assert.Equal(CertificateBinderOptions.DefaultGracePeriod, binder.Options.SupersededCertificateGracePeriod);

        X509Certificate2 a = NewCertificate();
        source.Raise(a);
        Assert.Same(a, Assert.Single(consumer.Received));
    }

    [Fact]
    public void AddedConsumers_GetTheCurrentCertificateAtOnce_AndRemovedOnesStopReceiving()
    {
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        TestSource source = new() { Current = a };
        using CertificateBinder binder = new(source, Grace(TimeSpan.FromMinutes(1)));
        RecordingConsumer consumer = new();
        List<X509Certificate2> viaDelegate = [];

        binder.Add(consumer);
        ICertificateConsumer delegateConsumer = binder.Add(viaDelegate.Add);

        Assert.Same(a, Assert.Single(consumer.Received));
        Assert.Same(a, Assert.Single(viaDelegate));
        Assert.True(binder.Remove(delegateConsumer));
        Assert.False(binder.Remove(delegateConsumer));

        source.Raise(b);
        Assert.Equal([a, b], consumer.Received);
        Assert.Single(viaDelegate);
    }

    [Fact]
    public void ConsumerThatThrows_IsReported_AndTheOthersAreStillUpdated()
    {
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        TestSource source = new() { Current = a };
        RecordingConsumer failing = new() { ThrowOnUpdate = new InvalidOperationException("listener refused the certificate") };
        RecordingConsumer healthy = new();
        List<CertificateConsumerFailure> failures = [];
        using CertificateBinder binder = new(source, [failing, healthy], Grace(TimeSpan.FromMinutes(1)));
        binder.ConsumerFailed += failures.Add;
        binder.ConsumerFailed += _ => throw new InvalidOperationException("observers cannot break the binding");
        binder.CertificateApplied += _ => throw new InvalidOperationException("observers cannot break the binding");

        source.Raise(b);

        Assert.Equal([a, b], healthy.Received);
        Assert.Equal([a, b], failing.Received);
        CertificateConsumerFailure failure = Assert.Single(failures);
        Assert.Same(failing, failure.Consumer);
        Assert.Same(b, failure.Certificate);
        Assert.Equal("listener refused the certificate", failure.Exception.Message);
        Assert.Same(b, binder.Certificate);
    }

    [Fact]
    public void SameCertificateRaisedAgain_OrNull_IsIgnored()
    {
        X509Certificate2 a = NewCertificate();
        TestSource source = new() { Current = a };
        RecordingConsumer consumer = new();
        using CertificateBinder binder = new(source, [consumer], Grace(TimeSpan.FromMinutes(1)));

        source.Raise(a);
        source.RaiseNull();

        Assert.Same(a, Assert.Single(consumer.Received));
        Assert.Equal(0, binder.PendingDisposalCount);
    }

    [Fact]
    public void SupersededCertificate_IsDisposedOnlyAfterTheGracePeriod()
    {
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        TestSource source = new() { Current = a };
        using CertificateBinder binder = new(source, [new RecordingConsumer()], Grace(TimeSpan.FromMinutes(2)));

        source.Raise(b);
        _time.Advance(TimeSpan.FromMinutes(1));

        Assert.False(Certs.IsDisposed(a));
        Assert.Equal(1, binder.PendingDisposalCount);

        _time.Advance(TimeSpan.FromMinutes(1));

        Assert.True(Certs.IsDisposed(a));
        Assert.False(Certs.IsDisposed(b));
        Assert.Equal(0, binder.PendingDisposalCount);
    }

    [Fact]
    public void ZeroGracePeriod_DisposesTheSupersededCertificateAtOnce()
    {
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        TestSource source = new() { Current = a };
        using CertificateBinder binder = new(source, Grace(TimeSpan.Zero));

        source.Raise(b);

        Assert.True(Certs.IsDisposed(a));
        Assert.Equal(0, binder.PendingDisposalCount);
    }

    [Fact]
    public void DisposeSupersededCertificatesFalse_LeavesThemToTheOwner()
    {
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        TestSource source = new() { Current = a };
        using CertificateBinder binder = new(source, new CertificateBinderOptions { DisposeSupersededCertificates = false, SupersededCertificateGracePeriod = TimeSpan.Zero });

        source.Raise(b);

        Assert.False(Certs.IsDisposed(a));
        Assert.Equal(0, binder.PendingDisposalCount);
    }

    [Fact]
    public void CertificateRaisedAgainWithinItsGracePeriod_IsKept()
    {
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        TestSource source = new() { Current = a };
        RecordingConsumer consumer = new();
        using CertificateBinder binder = new(source, [consumer], Grace(TimeSpan.FromMinutes(1)));

        source.Raise(b);
        source.Raise(a); // rolled back
        _time.Advance(TimeSpan.FromMinutes(2));

        Assert.False(Certs.IsDisposed(a));
        Assert.True(Certs.IsDisposed(b));
        Assert.Equal([a, b, a], consumer.Received);
    }

    [Fact]
    public void StaleGraceCallback_ForACertificateInUseAgain_IsIgnored()
    {
        _time.FireDisposedTimers = true; // the callback was already on its way when the timer was stopped
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        TestSource source = new() { Current = a };
        using CertificateBinder binder = new(source, Grace(TimeSpan.FromMinutes(1)));

        source.Raise(b);
        source.Raise(a);
        _time.Advance(TimeSpan.FromMinutes(2));

        Assert.False(Certs.IsDisposed(a));
        Assert.True(Certs.IsDisposed(b));
    }

    [Fact]
    public void TimerThatFiresSynchronously_StillDisposesExactlyOnce()
    {
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        TestSource source = new() { Current = a };
        using CertificateBinder binder = new(source, new CertificateBinderOptions { SupersededCertificateGracePeriod = TimeSpan.FromMinutes(1), TimeProvider = new FastTimeProvider() });

        source.Raise(b);

        Assert.True(Certs.IsDisposed(a));
        Assert.Equal(0, binder.PendingDisposalCount);
    }

    [Fact]
    public async Task Dispose_Unsubscribes_AndDisposesCertificatesStillInTheirGracePeriod()
    {
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        X509Certificate2 c = NewCertificate();
        TestSource source = new() { Current = a };
        RecordingConsumer consumer = new();
        CertificateBinder binder = new(source, [consumer], Grace(TimeSpan.FromMinutes(1)));
        source.Raise(b);
        Assert.Equal(1, source.SubscriberCount);

        await binder.DisposeAsync();
        binder.Dispose();

        Assert.True(Certs.IsDisposed(a));
        Assert.False(Certs.IsDisposed(b)); // the current certificate belongs to the source
        Assert.Equal(0, source.SubscriberCount);
        Assert.Equal(0, _time.ActiveTimers);
        source.Raise(c);
        Assert.Equal([a, b], consumer.Received);
        Assert.Throws<ObjectDisposedException>(() => binder.Add(new RecordingConsumer()));
    }

    [Fact]
    public void Constructor_ValidatesArguments()
    {
        TestSource source = new();
        Assert.Throws<ArgumentNullException>(() => new CertificateBinder(null!));
        Assert.Throws<ArgumentNullException>(() => new CertificateBinder(source, (IEnumerable<ICertificateConsumer>)null!));
        Assert.Throws<ArgumentException>(() => new CertificateBinder(source, [null!]));
        Assert.Throws<ArgumentOutOfRangeException>(() => new CertificateBinder(source, new CertificateBinderOptions { SupersededCertificateGracePeriod = TimeSpan.FromSeconds(-1) }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new CertificateBinder(source, new CertificateBinderOptions { SupersededCertificateGracePeriod = CertificateBinderOptions.MaxGracePeriod + TimeSpan.FromSeconds(1) }));
        Assert.Throws<ArgumentNullException>(() => new CertificateBinder(source, new CertificateBinderOptions { TimeProvider = null! }));
        using CertificateBinder binder = new(source);
        Assert.Throws<ArgumentNullException>(() => binder.Add((ICertificateConsumer)null!));
        Assert.Throws<ArgumentNullException>(() => binder.Add((Action<X509Certificate2>)null!));
        Assert.Equal(0, source.SubscriberCount - 1);
    }

    [Fact]
    public void Consumers_FromSource_ForwardsIntoAStaticSource()
    {
        X509Certificate2 a = NewCertificate();
        StaticCertificateSource https = new();
        List<X509Certificate2> seen = [];
        https.Changed += seen.Add;

        CertificateConsumers.FromSource(https).UpdateCertificate(a);

        Assert.Same(a, https.Current);
        Assert.Same(a, Assert.Single(seen));
        Assert.Throws<ArgumentNullException>(() => CertificateConsumers.FromSource(null!));
        Assert.Throws<ArgumentNullException>(() => CertificateConsumers.FromDelegate(null!));
    }
}
