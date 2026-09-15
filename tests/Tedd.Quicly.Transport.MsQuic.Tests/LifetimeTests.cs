using System.Net;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

using System.Security.Cryptography;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

/// <summary>Registration / configuration / listener handle rules: explicit lifetimes, idempotent Close, no use after Close.</summary>
[Collection(MsQuicCollection.Name)]
public class LifetimeTests
{
    private sealed class NoListenerEvents : IMsQuicListenerEvents
    {
        public MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, in MsQuicNewConnectionInfo info) => null;
    }

    [Fact]
    public unsafe void Registration_close_is_idempotent_and_blocks_use_after_close()
    {
        var registration = new MsQuicRegistration("lifetime", QUIC_EXECUTION_PROFILE.MAX_THROUGHPUT);
        Assert.False(registration.IsClosed);
        Assert.True(registration.Handle != null);
        Assert.Same(MsQuicApi.Instance, registration.Api);
        registration.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        registration.Close();
        registration.Close();
        registration.Dispose();
        Assert.True(registration.IsClosed);
        Assert.True(registration.Handle == null);
        Assert.Throws<ObjectDisposedException>(() => registration.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0));
        Assert.Throws<ObjectDisposedException>(() => new MsQuicConfiguration(registration, ["a"]));
        Assert.Throws<ObjectDisposedException>(() => new MsQuicListener(registration, new NoListenerEvents()));
        Assert.Throws<ObjectDisposedException>(() => new MsQuicConnection(registration));
    }

    [Fact]
    public void Registration_without_app_name_opens()
    {
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        Assert.False(registration.IsClosed);
    }

    [Fact]
    public unsafe void Configuration_validates_arguments_and_closes_idempotently()
    {
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        Assert.Throws<ArgumentNullException>(() => new MsQuicConfiguration(null!, ["a"]));
        Assert.Throws<ArgumentException>(() => new MsQuicConfiguration(registration, ReadOnlySpan<string>.Empty));
        Assert.Throws<ArgumentException>(() => new MsQuicConfiguration(registration, [""]));
        Assert.Throws<ArgumentException>(() => new MsQuicConfiguration(registration, [new string('a', 256)]));

        var config = new MsQuicConfiguration(registration, ["one", "two"], MsQuicSettings.Empty);
        Assert.Equal(["one", "two"], config.Alpns);
        Assert.Same(registration, config.Registration);
        Assert.False(config.HasCredential);
        Assert.Equal(QUIC_CREDENTIAL_TYPE.NONE, config.CredentialType);
        Assert.False(config.IsClosed);
        Assert.True(config.Handle != null);
        config.LoadClientCredential(MsQuicCertificateValidation.InsecureSkipValidation);
        Assert.True(config.HasCredential);
        Assert.Equal(QUIC_CREDENTIAL_FLAGS.CLIENT | QUIC_CREDENTIAL_FLAGS.NO_CERTIFICATE_VALIDATION, config.CredentialFlags);
        Assert.False(config.IndicatesPortableCertificate);
        Assert.False(config.DefersCertificateValidation);
        config.Close();
        config.Close();
        config.Dispose();
        Assert.True(config.IsClosed);
        Assert.Throws<ObjectDisposedException>(() => config.LoadClientCredential());
        Assert.Throws<ObjectDisposedException>(() => RawCredentials.LoadInvalid(config));
        using X509Certificate2 cert = TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromHours(1));
        Assert.Throws<ObjectDisposedException>(() => config.LoadServerCredential(cert));
    }

    [Fact]
    public void Client_credential_modes_map_to_the_documented_flags()
    {
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        using var systemRoots = MsQuicConfiguration.CreateClient(registration, ["a"]);
        Assert.Equal(QUIC_CREDENTIAL_FLAGS.CLIENT, systemRoots.CredentialFlags);
        using var callback = MsQuicConfiguration.CreateClient(registration, ["a"], MsQuicCertificateValidation.Callback);
        Assert.Equal(QUIC_CREDENTIAL_FLAGS.CLIENT | QUIC_CREDENTIAL_FLAGS.INDICATE_CERTIFICATE_RECEIVED | QUIC_CREDENTIAL_FLAGS.DEFER_CERTIFICATE_VALIDATION | QUIC_CREDENTIAL_FLAGS.USE_PORTABLE_CERTIFICATES, callback.CredentialFlags);
        Assert.True(callback.IndicatesPortableCertificate);
        Assert.True(callback.DefersCertificateValidation);
        Assert.Equal(QUIC_CREDENTIAL_TYPE.NONE, callback.CredentialType);
    }

    [Fact]
    public void Configuration_native_settings_constructor_and_invalid_validation()
    {
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        QUIC_SETTINGS settings = default;
        settings.SetIdleTimeoutMs(1000);
        using var config = new MsQuicConfiguration(registration, ["a"], in settings);
        Assert.Throws<ArgumentOutOfRangeException>(() => config.LoadClientCredential((MsQuicCertificateValidation)99));
        Assert.False(config.HasCredential);
    }

    /// <summary>
    /// Records which server credential path the loaded library accepts. Observed on Windows with the msquic.dll
    /// 2.5.10 bundled in the .NET shared framework (Schannel): CERTIFICATE_PKCS12 is answered with
    /// QUIC_STATUS_NOT_SUPPORTED, so Auto falls back to CERTIFICATE_CONTEXT. OpenSSL builds take PKCS#12.
    /// The PKCS#12 attempt needs an exportable key, so this uses an ephemeral key straight from CertificateRequest
    /// (TestCertificates keys are deliberately not exportable).
    /// </summary>
    [Fact]
    public void Server_credential_prefers_pkcs12_and_records_the_path()
    {
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        using X509Certificate2 withKey = CreateExportable();
        using X509Certificate2 publicOnly = X509CertificateLoader.LoadCertificate(withKey.Export(X509ContentType.Cert));
        bool schannel = MsQuicApi.Instance.TlsProvider == QUIC_TLS_PROVIDER.SCHANNEL;

        using var config = new MsQuicConfiguration(registration, ["a"]);
        Assert.Throws<ArgumentNullException>(() => config.LoadServerCredential(null!));
        Assert.Throws<ArgumentException>(() => config.LoadServerCredential(publicOnly));
        Assert.Throws<ArgumentOutOfRangeException>(() => config.LoadServerCredential(withKey, (MsQuicServerCredentialMode)7));

        // First Auto load in a "fresh" process: PKCS#12 is attempted and, on Schannel, rejected then remembered.
        MsQuicConfiguration.Pkcs12KnownUnsupported = false;
        config.LoadServerCredential(withKey);
        Assert.True(config.HasCredential);
        Assert.Equal(QUIC_CREDENTIAL_FLAGS.NONE, config.CredentialFlags);
        Assert.Equal(schannel ? QUIC_CREDENTIAL_TYPE.CERTIFICATE_CONTEXT : QUIC_CREDENTIAL_TYPE.CERTIFICATE_PKCS12, config.CredentialType);
        Assert.Equal(schannel, MsQuicConfiguration.Pkcs12KnownUnsupported);

        // Second Auto load: the remembered verdict skips the PKCS#12 attempt.
        using var again = MsQuicConfiguration.CreateServer(registration, ["a"], withKey);
        Assert.Equal(config.CredentialType, again.CredentialType);

        if (schannel)
        {
            MsQuicException ex = Assert.Throws<MsQuicException>(() => MsQuicConfiguration.CreateServer(registration, ["a"], withKey, mode: MsQuicServerCredentialMode.Pkcs12));
            Assert.Equal(MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED, ex.Status);
            Assert.Contains("CERTIFICATE_PKCS12", ex.Operation, StringComparison.Ordinal);
        }
        else
        {
            using var explicitPkcs12 = MsQuicConfiguration.CreateServer(registration, ["a"], withKey, mode: MsQuicServerCredentialMode.Pkcs12);
            Assert.Equal(QUIC_CREDENTIAL_TYPE.CERTIFICATE_PKCS12, explicitPkcs12.CredentialType);
        }
    }

    private static X509Certificate2 CreateExportable()
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        return new CertificateRequest("CN=x", key, HashAlgorithmName.SHA256).CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddHours(1));
    }

    /// <summary>
    /// Recorded path for ADR 0009 keys (no Exportable): the PKCS#12 blob cannot be produced, so Auto goes straight to
    /// CERTIFICATE_CONTEXT on Windows without attempting PKCS#12 and without re-importing (nothing persisted).
    /// </summary>
    [Fact]
    public void Non_exportable_key_falls_back_to_certificate_context_on_windows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "key containers are a Windows concept");
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        using X509Certificate2 nonExportable = TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromHours(1));
        Assert.False(MsQuicCertificateHelper.TryExportPkcs12(nonExportable, out _));

        MsQuicConfiguration.Pkcs12KnownUnsupported = false;
        using var auto = MsQuicConfiguration.CreateServer(registration, ["a"], nonExportable);
        Assert.Equal(QUIC_CREDENTIAL_TYPE.CERTIFICATE_CONTEXT, auto.CredentialType);
        Assert.Null(auto.OwnedCertificate);
        Assert.False(MsQuicConfiguration.Pkcs12KnownUnsupported); // no PKCS#12 attempt was made

        using X509Certificate2 exportable = CreateExportable();
        using var forcedContext = MsQuicConfiguration.CreateServer(registration, ["a"], exportable, mode: MsQuicServerCredentialMode.CertificateContext);
        Assert.Equal(QUIC_CREDENTIAL_TYPE.CERTIFICATE_CONTEXT, forcedContext.CredentialType);

        Assert.Throws<ArgumentException>(() => MsQuicConfiguration.CreateServer(registration, ["a"], nonExportable, mode: MsQuicServerCredentialMode.Pkcs12));
    }

    [Fact]
    public void Non_exportable_key_is_an_error_off_windows()
    {
        Assert.SkipWhen(OperatingSystem.IsWindows(), "the CERTIFICATE_CONTEXT fallback exists on Windows");
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        using X509Certificate2 withKey = TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromHours(1));
        using var config = new MsQuicConfiguration(registration, ["a"]);
        Assert.Throws<PlatformNotSupportedException>(() => config.LoadServerCredential(withKey, MsQuicServerCredentialMode.CertificateContext));
    }

    [Fact]
    public void Create_helpers_close_the_configuration_when_loading_fails()
    {
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        using X509Certificate2 withKey = TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromHours(1));
        using X509Certificate2 publicOnly = X509CertificateLoader.LoadCertificate(withKey.Export(X509ContentType.Cert));
        Assert.Throws<ArgumentException>(() => MsQuicConfiguration.CreateServer(registration, ["a"], publicOnly));
        Assert.Throws<ArgumentOutOfRangeException>(() => MsQuicConfiguration.CreateClient(registration, ["a"], (MsQuicCertificateValidation)42));
        // The registration can still be closed promptly, proving no configuration handle leaked.
    }

    [Fact]
    public void Raw_credential_load_reports_failure_status_without_throwing()
    {
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        using var config = new MsQuicConfiguration(registration, ["a"]);
        int status = RawCredentials.LoadInvalid(config);
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        Assert.False(config.HasCredential);
    }

    [Fact]
    public async Task Listener_lifecycle()
    {
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        var events = new RecordingListenerEvents();
        using var listener = new MsQuicListener(registration, events);
        Assert.Same(registration, listener.Registration);
        Assert.False(listener.IsStarted);
        Assert.Null(listener.Tag);
        Assert.Null(listener.LastCallbackException);
        // Not started: MsQuic reports an AF_UNSPEC address, which the wrapper turns into InvalidOperationException.
        Assert.Throws<InvalidOperationException>(() => listener.LocalEndPoint);
        Assert.Throws<ArgumentNullException>(() => listener.Start(null!, ["a"]));
        Assert.Throws<ArgumentException>(() => listener.Start(new IPEndPoint(IPAddress.Loopback, 0), ReadOnlySpan<string>.Empty));

        listener.Start(new IPEndPoint(IPAddress.Loopback, 0), ["a", "b"]);
        Assert.True(listener.IsStarted);
        IPEndPoint ep = listener.LocalEndPoint;
        Assert.NotEqual(0, ep.Port);
        Assert.Equal(IPAddress.Loopback, ep.Address);
        Assert.True(MsQuicStatus.Succeeded(listener.GetLocalAddress(out QUIC_ADDR addr)));
        Assert.Equal(ep.Port, addr.Port);

        Assert.Throws<MsQuicException>(() => listener.Start(new IPEndPoint(IPAddress.Loopback, 0), ["a"]));

        using var second = new MsQuicListener(registration, events);
        MsQuicException ex = Assert.Throws<MsQuicException>(() => second.Start(ep, ["a"]));
        Assert.True(ex.Status == MsQuicStatus.QUIC_STATUS_ALPN_IN_USE || ex.Status == MsQuicStatus.QUIC_STATUS_ADDRESS_IN_USE, MsQuicStatus.GetName(ex.Status));
        second.Close();

        listener.Stop();
        Assert.False(listener.IsStarted);
        Assert.True(await events.StopCompleteTcs.Within());
        listener.Stop();
        listener.Close();
        listener.Close();
        Assert.True(listener.IsClosed);
        Assert.Throws<ObjectDisposedException>(() => listener.Stop());
        Assert.Throws<ObjectDisposedException>(() => listener.Start(new IPEndPoint(IPAddress.Loopback, 0), ["a"]));
        Assert.Throws<ObjectDisposedException>(() => listener.GetLocalAddress(out _));
        Assert.Throws<ArgumentNullException>(() => new MsQuicListener(null!, events));
        Assert.Throws<ArgumentNullException>(() => new MsQuicListener(registration, null!));
    }

    [Fact]
    public void Listener_close_without_stop_delivers_stop_complete_with_app_close()
    {
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        var events = new RecordingListenerEvents();
        var listener = new MsQuicListener(registration, events) { Tag = "t" };
        Assert.Equal("t", listener.Tag);
        listener.Start(new IPEndPoint(IPAddress.IPv6Loopback, 0), ["a"]);
        Assert.Equal(IPAddress.IPv6Loopback, listener.LocalEndPoint.Address);
        listener.Dispose();
        Assert.True(listener.IsClosed);
        Assert.True(events.StopCompleteTcs.Task.IsCompleted);
        Assert.True(events.AppCloseInProgress);
    }

    [Fact]
    public async Task Listener_callback_exception_rejects_connection_and_is_recorded()
    {
        using var loopback = new Loopback();
        var throwing = new ThrowingListenerEvents();
        var listener = new MsQuicListener(loopback.Registration, throwing);
        listener.Start(new IPEndPoint(IPAddress.Loopback, 0), ["throw"]);
        MsQuicConfiguration clientConfig = loopback.CreateClientConfiguration(alpn: "throw");
        var clientEvents = new ConnectionRecorder();
        var client = new MsQuicConnection(loopback.Registration, clientEvents);
        IPEndPoint ep = listener.LocalEndPoint;
        MsQuicException.ThrowIfFailed(client.Start(clientConfig, "127.0.0.1", (ushort)ep.Port, QuicAddressFamily.INET), "start");
        (int status, _) = await clientEvents.TransportShutdownTcs.Within();
        Assert.True(MsQuicStatus.Failed(status));
        await clientEvents.ShutdownCompleteTcs.Within();
        Assert.IsType<InvalidOperationException>(listener.LastCallbackException);
        client.Close();
        listener.Close();
    }

    [Fact]
    public async Task Listener_close_from_its_own_callback_is_refused()
    {
        using var registrationScope = new TestRegistration(); MsQuicRegistration registration = registrationScope.Registration;
        var events = new ClosingListenerEvents();
        var listener = new MsQuicListener(registration, events);
        events.Listener = listener;
        listener.Start(new IPEndPoint(IPAddress.Loopback, 0), ["a"]);
        listener.Stop();
        Assert.True(await events.StopCompleteTcs.Within());
        Assert.IsType<InvalidOperationException>(events.Caught);
        Assert.False(listener.IsClosed);
        listener.Close();
    }

    [Fact]
    public async Task Closed_configuration_is_rejected_by_the_listener()
    {
        using var loopback = new Loopback();
        MsQuicConfiguration closed = loopback.CreateServerConfiguration(Loopback.Alpn);
        closed.Close();
        loopback.SelectConfiguration = _ => closed;
        var clientEvents = new ConnectionRecorder();
        loopback.Connect(clientEvents);
        (int status, _) = await clientEvents.TransportShutdownTcs.Within();
        Assert.True(MsQuicStatus.Failed(status));
        await clientEvents.ShutdownCompleteTcs.Within();
        Assert.False(clientEvents.ConnectedTcs.Task.IsCompleted);
    }

    private sealed class RecordingListenerEvents : IMsQuicListenerEvents
    {
        public readonly TaskCompletionSource<bool> StopCompleteTcs = TestTimeouts.NewTcs<bool>();
        public bool AppCloseInProgress;

        public MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, in MsQuicNewConnectionInfo info) => null;

        public void StopComplete(MsQuicListener listener, bool appCloseInProgress)
        {
            AppCloseInProgress = appCloseInProgress;
            StopCompleteTcs.TrySetResult(true);
        }
    }

    private sealed class ThrowingListenerEvents : IMsQuicListenerEvents
    {
        public MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, in MsQuicNewConnectionInfo info) => throw new InvalidOperationException("boom");
    }

    private sealed class ClosingListenerEvents : IMsQuicListenerEvents
    {
        public readonly TaskCompletionSource<bool> StopCompleteTcs = TestTimeouts.NewTcs<bool>();
        public MsQuicListener? Listener;
        public Exception? Caught;

        public MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, in MsQuicNewConnectionInfo info) => null;

        public void StopComplete(MsQuicListener listener, bool appCloseInProgress)
        {
            try
            {
                Assert.True(MsQuicCallbackScope.IsInsideCallback);
                Listener!.Close();
            }
            catch (Exception ex)
            {
                Caught = ex;
            }
            StopCompleteTcs.TrySetResult(true);
        }
    }
}
