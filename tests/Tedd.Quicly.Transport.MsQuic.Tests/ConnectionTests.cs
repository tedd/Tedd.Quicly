using System.Net;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

public class ConnectionTests
{
    [Fact]
    public async Task Client_connects_and_both_sides_see_negotiated_alpn()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, MsQuicConnection server, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        Assert.False(client.IsServer);
        Assert.True(server.IsServer);
        Assert.Contains(Loopback.Alpn, loopback.LastClientAlpns);
        Assert.Equal(IPAddress.Loopback.ToString(), loopback.LastServerName);

        IPEndPoint? remote = client.GetRemoteEndPoint();
        Assert.NotNull(remote);
        Assert.Equal(loopback.EndPoint.Port, remote.Port);
        Assert.Equal(IPAddress.Loopback, remote.Address);
        IPEndPoint? local = client.GetLocalEndPoint();
        Assert.NotNull(local);
        Assert.Equal(IPAddress.Loopback, local.Address);
        Assert.Equal(local.Port, server.GetRemoteEndPoint()!.Port);
        Assert.Equal(loopback.EndPoint.Port, server.GetLocalEndPoint()!.Port);

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.GetStatisticsV2(out QUIC_STATISTICS_V2 stats));
        Assert.True(stats.SendTotalPackets > 0);
        Assert.True(stats.RecvTotalPackets > 0);
        Assert.True(stats.HandshakeClientFlight1Bytes > 0);
        Assert.True(stats.SendPathMtu >= 1200);

        Assert.True(MsQuicStatus.Succeeded(client.GetParam(MsQuicParam.QUIC_PARAM_TLS_HANDSHAKE_INFO, out QUIC_HANDSHAKE_INFO hs)));
        Assert.Equal(QUIC_TLS_PROTOCOL_VERSION.TLS_1_3, hs.TlsProtocolVersion);
        Assert.True(MsQuicStatus.Succeeded(client.GetParam(MsQuicParam.QUIC_PARAM_CONN_QUIC_VERSION, out uint quicVersion)));
        Assert.Equal(1u, quicVersion);

        (ushort bidi, ushort unidi) = await serverEvents.StreamsAvailable.Within();
        Assert.Equal(128, bidi);
        Assert.Equal(128, unidi);
        Assert.False(clientEvents.TransportShutdown.Task.IsCompleted);

        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents.ShutdownComplete.Within();
        await serverEvents.ShutdownComplete.Within();
    }

    [Fact]
    public async Task Graceful_shutdown_propagates_error_code_to_peer()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, MsQuicConnection server, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0x1234);
        Assert.Equal(0x1234UL, await serverEvents.PeerShutdown.Within());
        (bool hsClient, bool ackClient, bool appCloseClient) = await clientEvents.ShutdownComplete.Within();
        (bool hsServer, _, _) = await serverEvents.ShutdownComplete.Within();
        Assert.True(hsClient);
        Assert.True(hsServer);
        Assert.True(ackClient);
        Assert.False(appCloseClient);
        Assert.False(clientEvents.PeerShutdown.Task.IsCompleted);
        Assert.False(serverEvents.TransportShutdown.Task.IsCompleted);

        server.Close();
        Assert.True(server.IsClosed);
        client.Close();
        client.Close(); // idempotent
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
        Assert.Equal(0xBEEFUL, await clientEvents.PeerShutdown.Within());
        await clientEvents.ShutdownComplete.Within();
        await serverEvents.ShutdownComplete.Within();
    }

    [Fact]
    public async Task Silent_shutdown_sends_nothing_and_completes_locally()
    {
        using var loopback = new Loopback(new MsQuicSettings { IdleTimeout = TimeSpan.FromSeconds(2), KeepAliveInterval = TimeSpan.Zero });
        (MsQuicConnection client, ConnectionRecorder clientEvents, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync(loopback.CreateClientConfiguration(settings: new MsQuicSettings { IdleTimeout = TimeSpan.FromSeconds(2), KeepAliveInterval = TimeSpan.Zero }));

        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.SILENT, 7);
        (_, bool peerAcknowledged, _) = await clientEvents.ShutdownComplete.Within();
        Assert.False(peerAcknowledged);
        // The server only learns about it through the idle timeout.
        (int status, _) = await serverEvents.TransportShutdown.Within();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_CONNECTION_IDLE, status);
        await serverEvents.ShutdownComplete.Within();
    }

    [Fact]
    public async Task Wrong_alpn_fails_the_handshake_with_alpn_negotiation_failure()
    {
        using var loopback = new Loopback();
        var clientEvents = new ConnectionRecorder();
        MsQuicConnection client = loopback.Connect(clientEvents, loopback.CreateClientConfiguration(alpn: "nope"));

        (int status, ulong errorCode) = await clientEvents.TransportShutdown.Within();
        int[] acceptable =
        [
            MsQuicStatus.QUIC_STATUS_ALPN_NEG_FAILURE,
            MsQuicStatus.QUIC_STATUS_HANDSHAKE_FAILURE,
            MsQuicStatus.QUIC_STATUS_CONNECTION_REFUSED,
            MsQuicStatus.TlsAlert(120),
        ];
        Assert.True(Array.IndexOf(acceptable, status) >= 0, $"unexpected status {MsQuicStatus.GetName(status)} error {errorCode:X}");
        (bool handshakeCompleted, _, _) = await clientEvents.ShutdownComplete.Within();
        Assert.False(handshakeCompleted);
        Assert.False(clientEvents.Connected.Task.IsCompleted);
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

        (int status, _) = await clientEvents.TransportShutdown.Within();
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        await clientEvents.ShutdownComplete.Within();
        Assert.False(clientEvents.Connected.Task.IsCompleted);
        Assert.Empty(loopback.Accepted);
    }

    [Fact]
    public async Task System_validation_rejects_self_signed_certificate()
    {
        using var loopback = new Loopback();
        var clientEvents = new ConnectionRecorder();
        loopback.Connect(clientEvents, loopback.CreateClientConfiguration(MsQuicCertificateValidation.System));

        (int status, _) = await clientEvents.TransportShutdown.Within();
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        await clientEvents.ShutdownComplete.Within();
        Assert.False(clientEvents.Connected.Task.IsCompleted);
    }

    [Fact]
    public async Task Custom_validation_receives_the_server_certificate_and_can_accept()
    {
        using var loopback = new Loopback();
        var clientEvents = new ConnectionRecorder();
        MsQuicConfiguration config = loopback.CreateClientConfiguration(MsQuicCertificateValidation.Custom);
        Assert.True(config.IndicatesPortableCertificate);
        loopback.Connect(clientEvents, config);

        (string alpn, _) = await clientEvents.Connected.Within();
        Assert.Equal(Loopback.Alpn, alpn);
        Assert.NotNull(clientEvents.ReceivedCertificate);
        Assert.Equal(loopback.Certificate.Thumbprint, clientEvents.ReceivedCertificate.Thumbprint);
        clientEvents.ReceivedCertificate.Dispose();
    }

    [Fact]
    public async Task Custom_validation_can_reject()
    {
        using var loopback = new Loopback();
        var clientEvents = new ConnectionRecorder { AcceptCertificate = false };
        loopback.Connect(clientEvents, loopback.CreateClientConfiguration(MsQuicCertificateValidation.Custom));

        (int status, _) = await clientEvents.TransportShutdown.Within();
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        await clientEvents.ShutdownComplete.Within();
        Assert.NotNull(clientEvents.ReceivedCertificate);
        Assert.False(clientEvents.Connected.Task.IsCompleted);
        clientEvents.ReceivedCertificate.Dispose();
    }

    [Fact]
    public async Task Pkcs12_server_credential_works()
    {
        using var loopback = new Loopback(credentialMode: MsQuicServerCredentialMode.Pkcs12);
        (MsQuicConnection client, _, _, _) = await loopback.ConnectPairAsync();
        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
    }

    [Fact]
    public async Task Handler_exceptions_are_recorded_not_propagated()
    {
        using var loopback = new Loopback();
        var clientEvents = new ThrowingEvents();
        MsQuicConnection client = loopback.Connect(clientEvents);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => client.LastCallbackException is not null, TestTimeouts.Default));
        Assert.IsType<InvalidOperationException>(client.LastCallbackException);
        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => clientEvents.ShutdownCompleted, TestTimeouts.Default));
    }

    private sealed class ThrowingEvents : IMsQuicConnectionEvents
    {
        public volatile bool ShutdownCompleted;
        public void Connected(MsQuicConnection connection, ReadOnlySpan<byte> negotiatedAlpn, bool sessionResumed) => throw new InvalidOperationException("boom");
        public void ShutdownComplete(MsQuicConnection connection, bool handshakeCompleted, bool peerAcknowledgedShutdown, bool appCloseInProgress) => ShutdownCompleted = true;
    }

    [Fact]
    public unsafe void Start_validates_arguments()
    {
        using var loopback = new Loopback();
        using var connection = new MsQuicConnection(loopback.Registration);
        MsQuicConfiguration config = loopback.CreateClientConfiguration();
        Assert.Throws<ArgumentNullException>(() => connection.Start(null!, "h", 1));
        Assert.Throws<ArgumentException>(() => connection.Start(config, "", 1));
        connection.Close();
        Assert.Throws<ObjectDisposedException>(() => connection.Start(config, "127.0.0.1", 1));
        Assert.Throws<ObjectDisposedException>(() => connection.SetConfiguration(config));
        Assert.Throws<ObjectDisposedException>(() => connection.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, null, out _));
        Assert.Throws<ObjectDisposedException>(() => connection.GetRemoteAddress(out _));
        Assert.Throws<ObjectDisposedException>(() => connection.GetLocalAddress(out _));
        ulong value = 0;
        Assert.Throws<ObjectDisposedException>(() => connection.SetParam(0, in value));
        Assert.Throws<ObjectDisposedException>(() => connection.GetParam(0, out ulong _));
        Assert.Throws<ObjectDisposedException>(() => connection.SetParam(0, 0, null));
        Assert.Throws<ObjectDisposedException>(() => connection.GetParam(0, null, null));
        Assert.Null(connection.GetRemoteEndPoint());
        Assert.Null(connection.GetLocalEndPoint());
    }

    [Fact]
    public unsafe void Unstarted_connection_has_no_addresses_and_accepts_raw_params()
    {
        using var loopback = new Loopback();
        using var connection = new MsQuicConnection(loopback.Registration);
        Assert.Null(connection.GetRemoteEndPoint());
        Assert.Null(connection.GetLocalEndPoint());
        Assert.Null(connection.Tag);
        connection.Tag = "x";
        Assert.Equal("x", connection.Tag);
        Assert.Same(connection.Api, loopback.Registration.Api);
        Assert.False(connection.IsClosed);
        Assert.Null(connection.LastCallbackException);

        byte share = 1;
        Assert.True(MsQuicStatus.Succeeded(connection.SetParam(MsQuicParam.QUIC_PARAM_CONN_SHARE_UDP_BINDING, 1, &share)));
        uint length = 1;
        byte read = 0;
        Assert.True(MsQuicStatus.Succeeded(connection.GetParam(MsQuicParam.QUIC_PARAM_CONN_SHARE_UDP_BINDING, &length, &read)));
        Assert.Equal(1, read);

        connection.Events = null!;
        Assert.NotNull(connection.Events);
    }
}
