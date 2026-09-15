using System.Net;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

/// <summary>Registration / configuration / listener handle rules: explicit lifetimes, idempotent Close, no use after Close.</summary>
public unsafe class LifetimeTests
{
    private sealed class NoListenerEvents : IMsQuicListenerEvents
    {
        public MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, ref QUIC_NEW_CONNECTION_INFO info) => null;
    }

    [Fact]
    public void Registration_close_is_idempotent_and_blocks_use_after_close()
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
        using var registration = new MsQuicRegistration();
        Assert.False(registration.IsClosed);
    }

    [Fact]
    public void Configuration_validates_arguments_and_closes_idempotently()
    {
        using var registration = new MsQuicRegistration();
        Assert.Throws<ArgumentNullException>(() => new MsQuicConfiguration(null!, ["a"]));
        Assert.Throws<ArgumentException>(() => new MsQuicConfiguration(registration, ReadOnlySpan<string>.Empty));
        Assert.Throws<ArgumentException>(() => new MsQuicConfiguration(registration, [""]));
        Assert.Throws<ArgumentException>(() => new MsQuicConfiguration(registration, [new string('a', 256)]));

        var config = new MsQuicConfiguration(registration, ["one", "two"], MsQuicSettings.Empty);
        Assert.Equal(["one", "two"], config.Alpns);
        Assert.Same(registration, config.Registration);
        Assert.False(config.HasCredential);
        Assert.False(config.IsClosed);
        Assert.True(config.Handle != null);
        config.LoadClientCredential(MsQuicCertificateValidation.InsecureSkipValidation);
        Assert.True(config.HasCredential);
        Assert.False(config.IndicatesPortableCertificate);
        config.Close();
        config.Close();
        config.Dispose();
        Assert.True(config.IsClosed);
        Assert.Throws<ObjectDisposedException>(() => config.LoadClientCredential());
        Assert.Throws<ObjectDisposedException>(() => config.LoadCredential(null));
        using X509Certificate2 cert = TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromHours(1));
        Assert.Throws<ObjectDisposedException>(() => config.LoadServerCredential(cert));
    }

    [Fact]
    public void Configuration_native_settings_constructor_and_invalid_validation()
    {
        using var registration = new MsQuicRegistration();
        QUIC_SETTINGS settings = default;
        settings.SetIdleTimeoutMs(1000);
        using var config = new MsQuicConfiguration(registration, ["a"], in settings);
        Assert.Throws<ArgumentOutOfRangeException>(() => config.LoadClientCredential((MsQuicCertificateValidation)99));
        Assert.False(config.HasCredential);
    }

    [Fact]
    public void Server_credential_requires_private_key_and_validates_mode()
    {
        using var registration = new MsQuicRegistration();
        using var config = new MsQuicConfiguration(registration, ["a"]);
        using X509Certificate2 withKey = TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromHours(1));
        using X509Certificate2 publicOnly = X509CertificateLoader.LoadCertificate(withKey.Export(X509ContentType.Cert));
        Assert.Throws<ArgumentNullException>(() => config.LoadServerCredential(null!));
        Assert.Throws<ArgumentException>(() => config.LoadServerCredential(publicOnly));
        if (!OperatingSystem.IsWindows())
        {
            Assert.Throws<PlatformNotSupportedException>(() => config.LoadServerCredential(withKey, MsQuicServerCredentialMode.CertificateContext));
        }
        config.LoadServerCredential(withKey, MsQuicServerCredentialMode.Pkcs12);
        Assert.True(config.HasCredential);
    }

    [Fact]
    public void Create_server_closes_configuration_when_credential_load_fails()
    {
        using var registration = new MsQuicRegistration();
        using X509Certificate2 withKey = TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromHours(1));
        using X509Certificate2 publicOnly = X509CertificateLoader.LoadCertificate(withKey.Export(X509ContentType.Cert));
        Assert.Throws<ArgumentException>(() => MsQuicConfiguration.CreateServer(registration, ["a"], publicOnly));
        Assert.Throws<ArgumentOutOfRangeException>(() => MsQuicConfiguration.CreateClient(registration, ["a"], (MsQuicCertificateValidation)42));
        // The registration can still be closed promptly, proving no configuration handle leaked.
    }

    [Fact]
    public void Raw_credential_load_reports_failure_status_without_throwing()
    {
        using var registration = new MsQuicRegistration();
        using var config = new MsQuicConfiguration(registration, ["a"]);
        QUIC_CREDENTIAL_CONFIG cred = default;
        cred.Type = (QUIC_CREDENTIAL_TYPE)999;
        int status = config.LoadCredential(&cred);
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        Assert.False(config.HasCredential);
    }

    [Fact]
    public async Task Listener_lifecycle()
    {
        using var registration = new MsQuicRegistration();
        var events = new RecordingListenerEvents();
        var listener = new MsQuicListener(registration, events);
        Assert.Same(registration, listener.Registration);
        Assert.False(listener.IsStarted);
        Assert.Null(listener.Tag);
        Assert.Null(listener.LastCallbackException);
        Assert.Throws<MsQuicException>(() => listener.LocalEndPoint);
        Assert.Throws<ArgumentNullException>(() => listener.Start(null!, ["a"]));
        Assert.Throws<ArgumentException>(() => listener.Start(new IPEndPoint(IPAddress.Loopback, 0), ReadOnlySpan<string>.Empty));

        listener.Start(new IPEndPoint(IPAddress.Loopback, 0), ["a", "b"]);
        Assert.True(listener.IsStarted);
        IPEndPoint ep = listener.LocalEndPoint;
        Assert.NotEqual(0, ep.Port);
        Assert.Equal(IPAddress.Loopback, ep.Address);
        Assert.True(MsQuicStatus.Succeeded(listener.GetLocalAddress(out QuicAddr addr)));
        Assert.Equal(ep.Port, addr.Port);

        // Starting twice is an MsQuic error surfaced as an exception.
        Assert.Throws<MsQuicException>(() => listener.Start(new IPEndPoint(IPAddress.Loopback, 0), ["a"]));

        // A second listener on the same port with the same ALPN is refused.
        var second = new MsQuicListener(registration, events);
        MsQuicException ex = Assert.Throws<MsQuicException>(() => second.Start(ep, ["a"]));
        Assert.True(ex.Status == MsQuicStatus.QUIC_STATUS_ALPN_IN_USE || ex.Status == MsQuicStatus.QUIC_STATUS_ADDRESS_IN_USE, MsQuicStatus.GetName(ex.Status));
        second.Close();

        listener.Stop();
        Assert.False(listener.IsStarted);
        Assert.True(await events.StopComplete.Within());
        listener.Stop(); // no-op when not started
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
        using var registration = new MsQuicRegistration();
        var events = new RecordingListenerEvents();
        var listener = new MsQuicListener(registration, events) { Tag = "t" };
        Assert.Equal("t", listener.Tag);
        listener.Start(new IPEndPoint(IPAddress.IPv6Loopback, 0), ["a"]);
        Assert.Equal(IPAddress.IPv6Loopback, listener.LocalEndPoint.Address);
        listener.Dispose();
        Assert.True(listener.IsClosed);
        // ListenerClose is synchronous: STOP_COMPLETE (with AppCloseInProgress) has been delivered by now.
        Assert.True(events.StopComplete.Task.IsCompleted);
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
        (int status, _) = await clientEvents.TransportShutdown.Within();
        Assert.True(MsQuicStatus.Failed(status));
        await clientEvents.ShutdownComplete.Within();
        Assert.IsType<InvalidOperationException>(listener.LastCallbackException);
        client.Close();
        listener.Close();
    }

    private sealed class RecordingListenerEvents : IMsQuicListenerEvents
    {
        public readonly TaskCompletionSource<bool> StopComplete = TestTimeouts.NewTcs<bool>();
        public bool AppCloseInProgress;

        public MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, ref QUIC_NEW_CONNECTION_INFO info) => null;

        public void StopComplete(MsQuicListener listener, bool appCloseInProgress)
        {
            AppCloseInProgress = appCloseInProgress;
            this.StopComplete.TrySetResult(true);
        }
    }

    private sealed class ThrowingListenerEvents : IMsQuicListenerEvents
    {
        public MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, ref QUIC_NEW_CONNECTION_INFO info) => throw new InvalidOperationException("boom");
    }
}
