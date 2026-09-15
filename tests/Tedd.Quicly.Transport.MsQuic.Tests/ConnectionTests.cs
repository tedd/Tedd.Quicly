using System.Net;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

[Collection(MsQuicCollection.Name)]
public class ConnectionTests
{
    [Fact]
    public async Task Client_connects_and_both_sides_see_negotiated_alpn()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, MsQuicConnection server, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        Assert.False(client.IsServer);
        Assert.True(server.IsServer);
        Assert.True(clientEvents.InsideCallbackSeen);
        Assert.Contains(Loopback.Alpn, loopback.LastClientAlpns);
        Assert.Equal(Loopback.Alpn, loopback.LastNegotiatedAlpn);
        Assert.Equal(IPAddress.Loopback.ToString(), loopback.LastServerName);
        Assert.Equal(1u, loopback.LastQuicVersion);
        Assert.NotNull(loopback.LastRemoteEndPoint);
        Assert.Equal(IPAddress.Loopback, loopback.LastRemoteEndPoint.Address);

        IPEndPoint? remote = client.RemoteEndPoint;
        Assert.NotNull(remote);
        Assert.Equal(loopback.EndPoint.Port, remote.Port);
        Assert.Equal(IPAddress.Loopback, remote.Address);
        IPEndPoint? local = client.LocalEndPoint;
        Assert.NotNull(local);
        Assert.Equal(IPAddress.Loopback, local.Address);
        Assert.Equal(local.Port, server.RemoteEndPoint!.Port);
        Assert.Equal(loopback.LastRemoteEndPoint.Port, local.Port);
        Assert.Equal(loopback.EndPoint.Port, server.LocalEndPoint!.Port);

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.GetStatisticsV2(out QUIC_STATISTICS_V2 stats, out uint bytesWritten));
        Assert.Equal(client.Api.StatisticsV2Size, bytesWritten);
        Assert.True(stats.SendTotalPackets > 0);
        Assert.True(stats.RecvTotalPackets > 0);
        Assert.True(stats.HandshakeClientFlight1Bytes > 0);
        Assert.True(stats.SendPathMtu >= 1200);
        Assert.False(stats.VersionNegotiation);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.GetStatisticsV2(out QUIC_STATISTICS_V2 again));
        Assert.Equal(stats.CorrelationId, again.CorrelationId);

        Assert.True(MsQuicStatus.Succeeded(client.GetParam(MsQuicParam.QUIC_PARAM_TLS_HANDSHAKE_INFO, out QUIC_HANDSHAKE_INFO hs)));
        Assert.Equal(QUIC_TLS_PROTOCOL_VERSION.TLS_1_3, hs.TlsProtocolVersion);
        if (client.Api.Version >= new Version(2, 5))
        {
            Assert.True(MsQuicStatus.Succeeded(client.GetParam(MsQuicParam.QUIC_PARAM_TLS_HANDSHAKE_INFO, out QUIC_HANDSHAKE_INFO_2_5 hs25)));
            Assert.Equal(hs.CipherSuite, hs25.CipherSuite);
            Assert.NotEqual(QUIC_TLS_GROUP.UNKNOWN, hs25.TlsGroup);
        }
        Assert.True(MsQuicStatus.Succeeded(client.GetParam(MsQuicParam.QUIC_PARAM_CONN_QUIC_VERSION, out uint quicVersion)));
        Assert.Equal(1u, quicVersion);

        (ushort bidi, ushort unidi) = await serverEvents.StreamsAvailableTcs.Within();
        Assert.Equal(16, bidi);
        Assert.Equal(16, unidi);
        Assert.False(clientEvents.TransportShutdownTcs.Task.IsCompleted);

        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents.ShutdownCompleteTcs.Within();
        await serverEvents.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public async Task Graceful_shutdown_propagates_error_code_to_peer()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, MsQuicConnection server, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0x1234);
        Assert.Equal(0x1234UL, await serverEvents.PeerShutdownTcs.Within());
        (bool hsClient, bool ackClient, bool appCloseClient) = await clientEvents.ShutdownCompleteTcs.Within();
        (bool hsServer, _, _) = await serverEvents.ShutdownCompleteTcs.Within();
        Assert.True(hsClient);
        Assert.True(hsServer);
        Assert.True(ackClient);
        Assert.False(appCloseClient);
        Assert.False(clientEvents.PeerShutdownTcs.Task.IsCompleted);
        Assert.False(serverEvents.TransportShutdownTcs.Task.IsCompleted);

        server.Close();
        Assert.True(server.IsClosed);
        client.Close();
        client.Close();
        Assert.True(client.IsClosed);
        Assert.Throws<ObjectDisposedException>(() => client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0));
        Assert.Throws<ObjectDisposedException>(() => client.GetStatisticsV2(out _));
    }

    [Fact]
    public async Task Server_initiated_abortive_shutdown_reaches_client()
    {
        using var loopback = new Loopback();
        (_, ConnectionRecorder clientEvents, MsQuicConnection server, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        server.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0xBEEF);
        Assert.Equal(0xBEEFUL, await clientEvents.PeerShutdownTcs.Within());
        await clientEvents.ShutdownCompleteTcs.Within();
        await serverEvents.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public async Task Silent_shutdown_sends_nothing_and_completes_locally()
    {
        MsQuicSettings serverSettings = Loopback.TestServerSettings();
        serverSettings.IdleTimeout = TimeSpan.FromSeconds(2);
        MsQuicSettings clientSettings = Loopback.TestClientSettings();
        clientSettings.IdleTimeout = TimeSpan.FromSeconds(2);
        clientSettings.KeepAliveInterval = TimeSpan.Zero;
        using var loopback = new Loopback(serverSettings);
        (MsQuicConnection client, ConnectionRecorder clientEvents, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync(loopback.CreateClientConfiguration(settings: clientSettings));

        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.SILENT, 7);
        (_, bool peerAcknowledged, _) = await clientEvents.ShutdownCompleteTcs.Within();
        Assert.False(peerAcknowledged);
        (int status, _) = await serverEvents.TransportShutdownTcs.Within();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_CONNECTION_IDLE, status);
        await serverEvents.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public async Task Wrong_alpn_fails_the_handshake()
    {
        using var loopback = new Loopback();
        var clientEvents = new ConnectionRecorder();
        MsQuicConnection client = loopback.Connect(clientEvents, loopback.CreateClientConfiguration(alpn: "nope"));

        (int status, ulong errorCode) = await clientEvents.TransportShutdownTcs.Within();
        int[] acceptable =
        [
            MsQuicStatus.QUIC_STATUS_ALPN_NEG_FAILURE,
            MsQuicStatus.QUIC_STATUS_HANDSHAKE_FAILURE,
            MsQuicStatus.QUIC_STATUS_CONNECTION_REFUSED,
            MsQuicStatus.TlsAlert(120),
        ];
        Assert.True(Array.IndexOf(acceptable, status) >= 0, $"unexpected status {MsQuicStatus.GetName(status)} error {errorCode:X}");
        (bool handshakeCompleted, _, _) = await clientEvents.ShutdownCompleteTcs.Within();
        Assert.False(handshakeCompleted);
        Assert.False(clientEvents.ConnectedTcs.Task.IsCompleted);
        Assert.Empty(loopback.Accepted);
        Assert.Contains("nope", loopback.LastClientAlpns);
        client.Close();
    }

    [Fact]
    public async Task Listener_rejection_refuses_the_connection()
    {
        using var loopback = new Loopback { RejectConnections = true };
        var clientEvents = new ConnectionRecorder();
        loopback.Connect(clientEvents);

        (int status, _) = await clientEvents.TransportShutdownTcs.Within();
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        await clientEvents.ShutdownCompleteTcs.Within();
        Assert.False(clientEvents.ConnectedTcs.Task.IsCompleted);
        Assert.Empty(loopback.Accepted);
    }

    [Fact]
    public async Task Two_configurations_on_one_listener_are_selected_by_alpn()
    {
        using var loopback = new Loopback(listenerAlpns: [Loopback.Alpn, "quicly-other"]);
        MsQuicSettings otherSettings = Loopback.TestServerSettings();
        otherSettings.PeerBidiStreamCount = 3;
        otherSettings.PeerUnidiStreamCount = 5;
        MsQuicConfiguration other = loopback.CreateServerConfiguration("quicly-other", otherSettings);
        loopback.SelectConfiguration = alpn => alpn == "quicly-other" ? other : null;

        (MsQuicConnection client, ConnectionRecorder clientEvents, _, _) = await loopback.ConnectPairAsync(loopback.CreateClientConfiguration(alpn: "quicly-other"), "quicly-other");
        Assert.Equal("quicly-other", loopback.LastNegotiatedAlpn);
        (ushort bidi, ushort unidi) = await clientEvents.StreamsAvailableTcs.Within();
        Assert.Equal(3, bidi);
        Assert.Equal(5, unidi);
        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents.ShutdownCompleteTcs.Within();

        // The default ALPN still lands on the default configuration (16/16).
        using var second = new Loopback(listenerAlpns: [Loopback.Alpn, "quicly-other"]);
        second.SelectConfiguration = alpn => alpn == "quicly-other" ? other : null;
        (MsQuicConnection client2, ConnectionRecorder clientEvents2, _, _) = await second.ConnectPairAsync();
        (bidi, unidi) = await clientEvents2.StreamsAvailableTcs.Within();
        Assert.Equal(16, bidi);
        Assert.Equal(16, unidi);
        client2.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents2.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public async Task System_roots_validation_rejects_self_signed_certificate()
    {
        using var loopback = new Loopback();
        var clientEvents = new ConnectionRecorder();
        loopback.Connect(clientEvents, loopback.CreateClientConfiguration(MsQuicCertificateValidation.SystemRoots));

        (int status, _) = await clientEvents.TransportShutdownTcs.Within();
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        await clientEvents.ShutdownCompleteTcs.Within();
        Assert.False(clientEvents.ConnectedTcs.Task.IsCompleted);
    }

    [Fact]
    public async Task Callback_validation_receives_der_chain_and_can_accept()
    {
        using var loopback = new Loopback();
        var clientEvents = new ConnectionRecorder();
        MsQuicConfiguration config = loopback.CreateClientConfiguration(MsQuicCertificateValidation.Callback);
        Assert.True(config.IndicatesPortableCertificate);
        loopback.Connect(clientEvents, config);

        (string alpn, _) = await clientEvents.ConnectedTcs.Within();
        Assert.Equal(Loopback.Alpn, alpn);
        Assert.True(await clientEvents.CertificateReceivedTcs.Within());
        Assert.True(clientEvents.CertificateIsPortable);
        Assert.True(clientEvents.CertificateCanDefer);
        Assert.NotNull(clientEvents.CertificateDer);
        using X509Certificate2 received = X509CertificateLoader.LoadCertificate(clientEvents.CertificateDer);
        Assert.Equal(loopback.Certificate.Thumbprint, received.Thumbprint);
        // Platform validation ran and (self-signed) did not pass; the app decided anyway.
        Assert.True(MsQuicStatus.Failed(clientEvents.CertificateDeferredStatus), MsQuicStatus.GetName(clientEvents.CertificateDeferredStatus));
        Assert.NotNull(clientEvents.ChainPkcs7);
        if (clientEvents.ChainPkcs7.Length > 0)
        {
            var chain = new X509Certificate2Collection();
            chain.Import(clientEvents.ChainPkcs7);
            Assert.Contains(chain.Cast<X509Certificate2>(), c => c.Thumbprint == loopback.Certificate.Thumbprint);
        }
    }

    [Fact]
    public async Task Callback_validation_can_reject()
    {
        using var loopback = new Loopback();
        var clientEvents = new ConnectionRecorder { CertificateDecision = MsQuicCertificateDecision.Reject };
        loopback.Connect(clientEvents, loopback.CreateClientConfiguration(MsQuicCertificateValidation.Callback));

        (int status, _) = await clientEvents.TransportShutdownTcs.Within();
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        await clientEvents.ShutdownCompleteTcs.Within();
        Assert.NotNull(clientEvents.CertificateDer);
        Assert.False(clientEvents.ConnectedTcs.Task.IsCompleted);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Callback_validation_can_defer_and_complete_later(bool accept)
    {
        using var loopback = new Loopback();
        var clientEvents = new ConnectionRecorder { CertificateDecision = MsQuicCertificateDecision.Defer };
        MsQuicConnection client = loopback.Connect(clientEvents, loopback.CreateClientConfiguration(MsQuicCertificateValidation.Callback));

        Assert.True(await clientEvents.CertificateReceivedTcs.Within());
        await Task.Delay(100);
        Assert.False(clientEvents.ConnectedTcs.Task.IsCompleted);
        Assert.False(clientEvents.TransportShutdownTcs.Task.IsCompleted);

        Assert.True(MsQuicStatus.Succeeded(client.CompleteCertificateValidation(accept, QUIC_TLS_ALERT_CODES.UNKNOWN_CA)));
        if (accept)
        {
            (string alpn, _) = await clientEvents.ConnectedTcs.Within();
            Assert.Equal(Loopback.Alpn, alpn);
            client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        }
        else
        {
            (int status, _) = await clientEvents.TransportShutdownTcs.Within();
            Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
            Assert.False(clientEvents.ConnectedTcs.Task.IsCompleted);
        }
        await clientEvents.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public async Task Defer_without_deferring_credential_is_treated_as_reject()
    {
        using var loopback = new Loopback();
        var clientEvents = new ConnectionRecorder { CertificateDecision = MsQuicCertificateDecision.Defer };
        // Raw credential: indicate + portable, but no DEFER flag.
        var config = new MsQuicConfiguration(loopback.Registration, [Loopback.Alpn], Loopback.TestClientSettings());
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, RawCredentials.LoadClient(config, QUIC_CREDENTIAL_FLAGS.CLIENT | QUIC_CREDENTIAL_FLAGS.INDICATE_CERTIFICATE_RECEIVED | QUIC_CREDENTIAL_FLAGS.NO_CERTIFICATE_VALIDATION | QUIC_CREDENTIAL_FLAGS.USE_PORTABLE_CERTIFICATES));
        Assert.False(config.DefersCertificateValidation);
        MsQuicConnection client = loopback.Connect(clientEvents, config);
        (int status, _) = await clientEvents.TransportShutdownTcs.Within();
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        Assert.False(clientEvents.CertificateCanDefer);
        await clientEvents.ShutdownCompleteTcs.Within();
        client.Close();
        config.Close();
    }

    [Fact]
    public async Task Platform_certificate_is_passed_through_when_not_portable()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "platform certificate contexts are exercised on Schannel");
        using var loopback = new Loopback();
        var clientEvents = new PlatformCertificateEvents();
        var config = new MsQuicConfiguration(loopback.Registration, [Loopback.Alpn], Loopback.TestClientSettings());
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, RawCredentials.LoadClient(config, QUIC_CREDENTIAL_FLAGS.CLIENT | QUIC_CREDENTIAL_FLAGS.INDICATE_CERTIFICATE_RECEIVED | QUIC_CREDENTIAL_FLAGS.NO_CERTIFICATE_VALIDATION));
        Assert.False(config.IndicatesPortableCertificate);
        MsQuicConnection client = loopback.Connect(clientEvents, config);
        Assert.True(await clientEvents.ConnectedTcs.Within());
        Assert.False(clientEvents.IsPortable);
        Assert.True(clientEvents.HadPlatformCertificate);
        Assert.Equal(loopback.Certificate.Thumbprint, clientEvents.Thumbprint);
        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        Assert.True(await clientEvents.ShutdownTcs.Within());
        client.Close();
        config.Close();
    }

    private sealed unsafe class PlatformCertificateEvents : IMsQuicConnectionEvents
    {
        public readonly TaskCompletionSource<bool> ConnectedTcs = TestTimeouts.NewTcs<bool>();
        public readonly TaskCompletionSource<bool> ShutdownTcs = TestTimeouts.NewTcs<bool>();
        public bool IsPortable;
        public bool HadPlatformCertificate;
        public string? Thumbprint;

        public void Connected(MsQuicConnection connection, ReadOnlySpan<byte> negotiatedAlpn, bool sessionResumed) => ConnectedTcs.TrySetResult(true);
        public void ShutdownComplete(MsQuicConnection connection, bool handshakeCompleted, bool peerAcknowledgedShutdown, bool appCloseInProgress) => ShutdownTcs.TrySetResult(true);

        public MsQuicCertificateDecision PeerCertificateReceived(MsQuicConnection connection, in MsQuicPeerCertificateInfo info)
        {
            IsPortable = info.IsPortable;
            HadPlatformCertificate = info.PlatformCertificate != null;
            if (HadPlatformCertificate)
            {
                using var cert = new X509Certificate2((nint)info.PlatformCertificate);
                Thumbprint = cert.Thumbprint;
            }
            return MsQuicCertificateDecision.Accept;
        }
    }

    [Fact]
    public async Task Pkcs12_and_certificate_context_credentials_both_complete_a_handshake()
    {
        using var pkcs12 = new Loopback(credentialMode: MsQuicServerCredentialMode.Pkcs12);
        Assert.Equal(QUIC_CREDENTIAL_TYPE.CERTIFICATE_PKCS12, pkcs12.ServerConfiguration.CredentialType);
        (MsQuicConnection client, ConnectionRecorder clientEvents, _, _) = await pkcs12.ConnectPairAsync();
        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents.ShutdownCompleteTcs.Within();

        if (OperatingSystem.IsWindows())
        {
            using var context = new Loopback(credentialMode: MsQuicServerCredentialMode.CertificateContext);
            Assert.Equal(QUIC_CREDENTIAL_TYPE.CERTIFICATE_CONTEXT, context.ServerConfiguration.CredentialType);
            (client, clientEvents, _, _) = await context.ConnectPairAsync();
            client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
            await clientEvents.ShutdownCompleteTcs.Within();
        }
    }

    [Fact]
    public async Task Handler_exceptions_are_recorded_and_poison_the_connection()
    {
        using var loopback = new Loopback();
        var clientEvents = new ThrowingEvents();
        MsQuicConnection client = loopback.Connect(clientEvents);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => client.LastCallbackException is not null, TestTimeouts.Default));
        Assert.IsType<InvalidOperationException>(client.LastCallbackException);
        Assert.True(client.IsPoisoned);
        // The poison shutdown carries the documented error code to the peer and completes locally without help.
        MsQuicConnection server = await loopback.FirstAcceptedTcs.Within();
        var serverEvents = (ConnectionRecorder)server.Events;
        Assert.Equal(MsQuicConnection.CallbackFailureErrorCode, await serverEvents.PeerShutdownTcs.Within());
        Assert.True(await TestTimeouts.WaitUntilAsync(() => clientEvents.ShutdownCompleted, TestTimeouts.Default));
    }

    private sealed class ThrowingEvents : IMsQuicConnectionEvents
    {
        public volatile bool ShutdownCompleted;
        public void Connected(MsQuicConnection connection, ReadOnlySpan<byte> negotiatedAlpn, bool sessionResumed) => throw new InvalidOperationException("boom");
        public void ShutdownComplete(MsQuicConnection connection, bool handshakeCompleted, bool peerAcknowledgedShutdown, bool appCloseInProgress) => ShutdownCompleted = true;
    }

    [Fact]
    public async Task Close_from_a_callback_thread_is_refused()
    {
        using var loopback = new Loopback();
        Exception? caught = null;
        var clientEvents = new ConnectionRecorder
        {
            OnShutdownComplete = connection =>
            {
                try
                {
                    connection.Close();
                }
                catch (Exception ex)
                {
                    caught = ex;
                }
            },
        };
        (MsQuicConnection client, _, _, _) = await loopback.ConnectPairAsync();
        client.Events = clientEvents;
        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents.ShutdownCompleteTcs.Within();
        Assert.IsType<InvalidOperationException>(caught);
        Assert.False(client.IsClosed);
        Assert.Null(client.LastCallbackException);
    }

    [Fact]
    public async Task Update_settings_and_stream_counts_apply_to_a_live_connection()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, MsQuicConnection server, _) = await loopback.ConnectPairAsync();
        await clientEvents.StreamsAvailableTcs.Within();

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, server.SetLocalUnidiStreamCount(40));
        Assert.True(await TestTimeouts.WaitUntilAsync(() => clientEvents.StreamsAvailableHistory.Any(s => s.Unidi == 40), TestTimeouts.Default));
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, server.SetLocalBidiStreamCount(41));
        Assert.True(await TestTimeouts.WaitUntilAsync(() => clientEvents.StreamsAvailableHistory.Any(s => s.Bidi == 41), TestTimeouts.Default));

        var builder = new MsQuicSettings { PeerUnidiStreamCount = 42 };
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, server.UpdateSettings(builder));
        Assert.True(await TestTimeouts.WaitUntilAsync(() => clientEvents.StreamsAvailableHistory.Any(s => s.Unidi == 42), TestTimeouts.Default));

        QUIC_SETTINGS native = default;
        native.SetKeepAliveIntervalMs(2000);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.UpdateSettings(in native));
        Assert.True(MsQuicStatus.Succeeded(client.GetParam(MsQuicParam.QUIC_PARAM_CONN_SETTINGS, out QUIC_SETTINGS readBack)));
        Assert.Equal(2000u, readBack.KeepAliveIntervalMs);
        Assert.Throws<ArgumentNullException>(() => client.UpdateSettings((MsQuicSettings)null!));

        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public async Task Resumption_ticket_send_validates_length()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, MsQuicConnection server, _) = await loopback.ConnectPairAsync();
        Assert.Throws<ArgumentOutOfRangeException>(() => server.SendResumptionTicket(QUIC_SEND_RESUMPTION_FLAGS.NONE, new byte[ushort.MaxValue + 1]));
        // Resumption is disabled (NO_RESUME), so MsQuic refuses the ticket; the call itself is exercised.
        int status = server.SendResumptionTicket(QUIC_SEND_RESUMPTION_FLAGS.FINAL, [1, 2, 3]);
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public unsafe void Start_validates_arguments()
    {
        using var loopback = new Loopback();
        using var connection = new MsQuicConnection(loopback.Registration);
        MsQuicConfiguration config = loopback.CreateClientConfiguration();
        Assert.Throws<ArgumentNullException>(() => connection.Start(null!, "h", 1));
        Assert.Throws<ArgumentException>(() => connection.Start(config, "", 1));
        Assert.Throws<ArgumentNullException>(() => connection.SetConfiguration(null!));
        connection.Close();
        Assert.Throws<ObjectDisposedException>(() => connection.Start(config, "127.0.0.1", 1));
        Assert.Throws<ObjectDisposedException>(() => connection.SetConfiguration(config));
        Assert.Throws<ObjectDisposedException>(() => connection.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, null, out _));
        Assert.Throws<ObjectDisposedException>(() => connection.GetRemoteAddress(out _));
        Assert.Throws<ObjectDisposedException>(() => connection.GetLocalAddress(out _));
        Assert.Throws<ObjectDisposedException>(() => connection.SendDatagram(null, 0, QUIC_SEND_FLAGS.NONE, null));
        Assert.Throws<ObjectDisposedException>(() => connection.UpdateSettings(MsQuicSettings.Empty));
        Assert.Throws<ObjectDisposedException>(() => connection.SetLocalUnidiStreamCount(1));
        Assert.Throws<ObjectDisposedException>(() => connection.SetLocalBidiStreamCount(1));
        Assert.Throws<ObjectDisposedException>(() => connection.CompleteCertificateValidation(true));
        Assert.Throws<ObjectDisposedException>(() => connection.SendResumptionTicket(QUIC_SEND_RESUMPTION_FLAGS.NONE, default));
        ulong value = 0;
        Assert.Throws<ObjectDisposedException>(() => connection.SetParam(0, in value));
        Assert.Throws<ObjectDisposedException>(() => connection.GetParam(0, out ulong _));
        Assert.Throws<ObjectDisposedException>(() => connection.SetParam(0, 0, null));
        Assert.Throws<ObjectDisposedException>(() => connection.GetParam(0, null, null));
        Assert.Throws<ObjectDisposedException>(() => connection.RemoteEndPoint);
        Assert.Throws<ObjectDisposedException>(() => connection.LocalEndPoint);
    }

    [Fact]
    public unsafe void Unstarted_connection_has_no_addresses_and_accepts_raw_params()
    {
        using var loopback = new Loopback();
        using var connection = new MsQuicConnection(loopback.Registration);
        Assert.Null(connection.RemoteEndPoint);
        Assert.Null(connection.LocalEndPoint);
        Assert.Null(connection.Tag);
        connection.Tag = "x";
        Assert.Equal("x", connection.Tag);
        Assert.Same(connection.Api, loopback.Registration.Api);
        Assert.False(connection.IsClosed);
        Assert.False(connection.IsPoisoned);
        Assert.Null(connection.LastCallbackException);

        byte share = 1;
        Assert.True(MsQuicStatus.Succeeded(connection.SetParam(MsQuicParam.QUIC_PARAM_CONN_SHARE_UDP_BINDING, 1, &share)));
        uint length = 1;
        byte read = 0;
        Assert.True(MsQuicStatus.Succeeded(connection.GetParam(MsQuicParam.QUIC_PARAM_CONN_SHARE_UDP_BINDING, &length, &read)));
        Assert.Equal(1, read);
        Assert.True(MsQuicStatus.Failed(connection.GetStatisticsV2(out _, out uint written)));
        Assert.Equal(0u, written);

        connection.Events = null!;
        Assert.NotNull(connection.Events);
    }
}
