using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Interop;

[Collection(MsQuicCollection.Name)]
public class SettingsTests
{
    private delegate void Setter(ref QUIC_SETTINGS s);

    /// <summary>Every Set* helper with the bit index msquic.h assigns to that field (IsSet bitfield order).</summary>
    public static TheoryData<string, int> Setters()
    {
        var data = new TheoryData<string, int>();
        foreach ((string name, int bit, _) in AllSetters()) data.Add(name, bit);
        return data;
    }

    private static IEnumerable<(string Name, int Bit, Setter Set)> AllSetters() =>
    [
        ("MaxBytesPerKey", 0, (ref QUIC_SETTINGS s) => s.SetMaxBytesPerKey(1)),
        ("HandshakeIdleTimeoutMs", 1, (ref QUIC_SETTINGS s) => s.SetHandshakeIdleTimeoutMs(1)),
        ("IdleTimeoutMs", 2, (ref QUIC_SETTINGS s) => s.SetIdleTimeoutMs(1)),
        ("MtuDiscoverySearchCompleteTimeoutUs", 3, (ref QUIC_SETTINGS s) => s.SetMtuDiscoverySearchCompleteTimeoutUs(1)),
        ("TlsClientMaxSendBuffer", 4, (ref QUIC_SETTINGS s) => s.SetTlsClientMaxSendBuffer(1)),
        ("TlsServerMaxSendBuffer", 5, (ref QUIC_SETTINGS s) => s.SetTlsServerMaxSendBuffer(1)),
        ("StreamRecvWindowDefault", 6, (ref QUIC_SETTINGS s) => s.SetStreamRecvWindowDefault(1)),
        ("StreamRecvBufferDefault", 7, (ref QUIC_SETTINGS s) => s.SetStreamRecvBufferDefault(1)),
        ("ConnFlowControlWindow", 8, (ref QUIC_SETTINGS s) => s.SetConnFlowControlWindow(1)),
        ("MaxWorkerQueueDelayUs", 9, (ref QUIC_SETTINGS s) => s.SetMaxWorkerQueueDelayUs(1)),
        ("MaxStatelessOperations", 10, (ref QUIC_SETTINGS s) => s.SetMaxStatelessOperations(1)),
        ("InitialWindowPackets", 11, (ref QUIC_SETTINGS s) => s.SetInitialWindowPackets(1)),
        ("SendIdleTimeoutMs", 12, (ref QUIC_SETTINGS s) => s.SetSendIdleTimeoutMs(1)),
        ("InitialRttMs", 13, (ref QUIC_SETTINGS s) => s.SetInitialRttMs(1)),
        ("MaxAckDelayMs", 14, (ref QUIC_SETTINGS s) => s.SetMaxAckDelayMs(1)),
        ("DisconnectTimeoutMs", 15, (ref QUIC_SETTINGS s) => s.SetDisconnectTimeoutMs(1)),
        ("KeepAliveIntervalMs", 16, (ref QUIC_SETTINGS s) => s.SetKeepAliveIntervalMs(1)),
        ("CongestionControlAlgorithm", 17, (ref QUIC_SETTINGS s) => s.SetCongestionControlAlgorithm(QUIC_CONGESTION_CONTROL_ALGORITHM.CUBIC)),
        ("PeerBidiStreamCount", 18, (ref QUIC_SETTINGS s) => s.SetPeerBidiStreamCount(1)),
        ("PeerUnidiStreamCount", 19, (ref QUIC_SETTINGS s) => s.SetPeerUnidiStreamCount(1)),
        ("MaxBindingStatelessOperations", 20, (ref QUIC_SETTINGS s) => s.SetMaxBindingStatelessOperations(1)),
        ("StatelessOperationExpirationMs", 21, (ref QUIC_SETTINGS s) => s.SetStatelessOperationExpirationMs(1)),
        ("MinimumMtu", 22, (ref QUIC_SETTINGS s) => s.SetMinimumMtu(1)),
        ("MaximumMtu", 23, (ref QUIC_SETTINGS s) => s.SetMaximumMtu(1)),
        ("SendBufferingEnabled", 24, (ref QUIC_SETTINGS s) => s.SetSendBufferingEnabled(true)),
        ("PacingEnabled", 25, (ref QUIC_SETTINGS s) => s.SetPacingEnabled(true)),
        ("MigrationEnabled", 26, (ref QUIC_SETTINGS s) => s.SetMigrationEnabled(true)),
        ("DatagramReceiveEnabled", 27, (ref QUIC_SETTINGS s) => s.SetDatagramReceiveEnabled(true)),
        ("ServerResumptionLevel", 28, (ref QUIC_SETTINGS s) => s.SetServerResumptionLevel(QUIC_SERVER_RESUMPTION_LEVEL.RESUME_ONLY)),
        ("MaxOperationsPerDrain", 29, (ref QUIC_SETTINGS s) => s.SetMaxOperationsPerDrain(1)),
        ("MtuDiscoveryMissingProbeCount", 30, (ref QUIC_SETTINGS s) => s.SetMtuDiscoveryMissingProbeCount(1)),
        ("DestCidUpdateIdleTimeoutMs", 31, (ref QUIC_SETTINGS s) => s.SetDestCidUpdateIdleTimeoutMs(1)),
        ("GreaseQuicBitEnabled", 32, (ref QUIC_SETTINGS s) => s.SetGreaseQuicBitEnabled(true)),
        ("EcnEnabled", 33, (ref QUIC_SETTINGS s) => s.SetEcnEnabled(true)),
        ("HyStartEnabled", 34, (ref QUIC_SETTINGS s) => s.SetHyStartEnabled(true)),
        ("StreamRecvWindowBidiLocalDefault", 35, (ref QUIC_SETTINGS s) => s.SetStreamRecvWindowBidiLocalDefault(1)),
        ("StreamRecvWindowBidiRemoteDefault", 36, (ref QUIC_SETTINGS s) => s.SetStreamRecvWindowBidiRemoteDefault(1)),
        ("StreamRecvWindowUnidiDefault", 37, (ref QUIC_SETTINGS s) => s.SetStreamRecvWindowUnidiDefault(1)),
    ];

    [Theory]
    [MemberData(nameof(Setters))]
    public void Each_setter_sets_exactly_its_is_set_bit(string name, int bit)
    {
        (_, _, Setter set) = AllSetters().Single(s => s.Name == name);
        QUIC_SETTINGS s = default;
        set(ref s);
        Assert.Equal(1UL << bit, s.IsSetFlags);
        Assert.Equal(1UL << bit, s.Anonymous1.IsSetFlags);
        Assert.True((s.IsSet._bitfield & (1UL << bit)) != 0);
    }

    [Fact]
    public void Is_set_properties_cover_every_bit_including_preview_ones()
    {
        QUIC_SETTINGS s = default;
        s.IsSet.MaxBytesPerKey = true;
        s.IsSet.StreamRecvWindowUnidiDefault = true;
        s.IsSet.EncryptionOffloadAllowed = true;
        s.IsSet.ReliableResetEnabled = true;
        s.IsSet.OneWayDelayEnabled = true;
        s.IsSet.NetStatsEventEnabled = true;
        s.IsSet.StreamMultiReceiveEnabled = true;
        s.IsSet.XdpEnabled = true;
        s.IsSet.QTIPEnabled = true;
        s.IsSet.ReservedRioEnabled = true;
        ulong expected = (1UL << 0) | (1UL << 37) | (1UL << 38) | (1UL << 39) | (1UL << 40) | (1UL << 41) | (1UL << 42) | (1UL << 43) | (1UL << 44) | (1UL << 45);
        Assert.Equal(expected, s.IsSetFlags);
        Assert.True(s.IsSet.MaxBytesPerKey);
        Assert.True(s.IsSet.ReservedRioEnabled);
        Assert.False(s.IsSet.IdleTimeoutMs);
        s.IsSet.MaxBytesPerKey = false;
        Assert.False(s.IsSet.MaxBytesPerKey);
        s.IsSetFlags = 0;
        Assert.Equal(0UL, s.IsSet._bitfield);
    }

    [Fact]
    public void Boolean_bitfield_bits_follow_msquic_h_order()
    {
        QUIC_SETTINGS s = default;
        s.SendBufferingEnabled = true;
        Assert.Equal(0x01, s._bitfield);
        s.PacingEnabled = true;
        Assert.Equal(0x03, s._bitfield);
        s.MigrationEnabled = true;
        Assert.Equal(0x07, s._bitfield);
        s.DatagramReceiveEnabled = true;
        Assert.Equal(0x0F, s._bitfield);
        s.ServerResumptionLevel = QUIC_SERVER_RESUMPTION_LEVEL.RESUME_AND_ZERORTT;
        Assert.Equal(0x2F, s._bitfield);
        Assert.Equal(QUIC_SERVER_RESUMPTION_LEVEL.RESUME_AND_ZERORTT, s.ServerResumptionLevel);
        s.GreaseQuicBitEnabled = true;
        Assert.Equal(0x6F, s._bitfield);
        s.EcnEnabled = true;
        Assert.Equal(0xEF, s._bitfield);
        Assert.True(s.SendBufferingEnabled && s.PacingEnabled && s.MigrationEnabled && s.DatagramReceiveEnabled && s.GreaseQuicBitEnabled && s.EcnEnabled);

        s.SendBufferingEnabled = false;
        Assert.Equal(0xEE, s._bitfield);
        s.ServerResumptionLevel = QUIC_SERVER_RESUMPTION_LEVEL.RESUME_ONLY;
        Assert.Equal(0xDE, s._bitfield);
        s.ServerResumptionLevel = QUIC_SERVER_RESUMPTION_LEVEL.NO_RESUME;
        s.PacingEnabled = false;
        s.MigrationEnabled = false;
        s.DatagramReceiveEnabled = false;
        s.GreaseQuicBitEnabled = false;
        s.EcnEnabled = false;
        Assert.Equal(0, s._bitfield);
    }

    [Fact]
    public void Flags_word_bits_follow_msquic_h_order()
    {
        QUIC_SETTINGS s = default;
        s.HyStartEnabled = true;
        Assert.Equal(1UL, s.Flags);
        Assert.True(s.Anonymous2.Anonymous.HyStartEnabled);
        s.Anonymous2.Anonymous.EncryptionOffloadAllowed = true;
        s.Anonymous2.Anonymous.ReliableResetEnabled = true;
        s.Anonymous2.Anonymous.OneWayDelayEnabled = true;
        s.Anonymous2.Anonymous.NetStatsEventEnabled = true;
        s.Anonymous2.Anonymous.StreamMultiReceiveEnabled = true;
        s.Anonymous2.Anonymous.XdpEnabled = true;
        s.Anonymous2.Anonymous.QTIPEnabled = true;
        s.Anonymous2.Anonymous.ReservedRioEnabled = true;
        Assert.Equal(0x1FFUL, s.Flags);
        Assert.True(s.Anonymous2.Anonymous.EncryptionOffloadAllowed && s.Anonymous2.Anonymous.ReliableResetEnabled && s.Anonymous2.Anonymous.OneWayDelayEnabled
            && s.Anonymous2.Anonymous.NetStatsEventEnabled && s.Anonymous2.Anonymous.StreamMultiReceiveEnabled && s.Anonymous2.Anonymous.XdpEnabled
            && s.Anonymous2.Anonymous.QTIPEnabled && s.Anonymous2.Anonymous.ReservedRioEnabled);
        s.HyStartEnabled = false;
        Assert.Equal(0x1FEUL, s.Flags);
        s.Flags = 0;
        Assert.False(s.Anonymous2.Anonymous.ReservedRioEnabled);
    }

    [Fact]
    public void Setters_store_values()
    {
        QUIC_SETTINGS s = default;
        s.SetIdleTimeoutMs(1234);
        s.SetPeerBidiStreamCount(7);
        s.SetCongestionControlAlgorithm(QUIC_CONGESTION_CONTROL_ALGORITHM.BBR);
        s.SetServerResumptionLevel(QUIC_SERVER_RESUMPTION_LEVEL.RESUME_ONLY);
        s.SetSendBufferingEnabled(false);
        Assert.Equal(1234UL, s.IdleTimeoutMs);
        Assert.Equal(7, s.PeerBidiStreamCount);
        Assert.Equal((ushort)QUIC_CONGESTION_CONTROL_ALGORITHM.BBR, s.CongestionControlAlgorithm);
        Assert.Equal(QUIC_SERVER_RESUMPTION_LEVEL.RESUME_ONLY, s.ServerResumptionLevel);
        Assert.False(s.SendBufferingEnabled);
        Assert.True(s.IsSet.SendBufferingEnabled);
    }

    [Fact]
    public void Builder_defaults_follow_architecture_section_7()
    {
        QUIC_SETTINGS s = MsQuicSettings.Default.ToNative();
        Assert.True(s.IsSet.IdleTimeoutMs);
        Assert.Equal(30_000UL, s.IdleTimeoutMs);
        Assert.True(s.IsSet.HandshakeIdleTimeoutMs);
        Assert.Equal(5_000UL, s.HandshakeIdleTimeoutMs);
        Assert.True(s.IsSet.DisconnectTimeoutMs);
        Assert.Equal(6_000u, s.DisconnectTimeoutMs);
        Assert.True(s.IsSet.KeepAliveIntervalMs);
        Assert.Equal(0u, s.KeepAliveIntervalMs);
        Assert.True(s.IsSet.PeerBidiStreamCount);
        Assert.Equal(1, s.PeerBidiStreamCount);
        Assert.True(s.IsSet.PeerUnidiStreamCount);
        Assert.Equal(0, s.PeerUnidiStreamCount);
        Assert.True(s.IsSet.StreamRecvWindowUnidiDefault);
        Assert.Equal(2u * 1024 * 1024, s.StreamRecvWindowUnidiDefault);
        Assert.True(s.IsSet.ConnFlowControlWindow);
        Assert.Equal(16u * 1024 * 1024, s.ConnFlowControlWindow);
        Assert.True(s.IsSet.MinimumMtu && s.IsSet.MaximumMtu);
        Assert.Equal(1248, s.MinimumMtu);
        Assert.Equal(1500, s.MaximumMtu);
        Assert.True(s.IsSet.MaxAckDelayMs);
        Assert.Equal(5u, s.MaxAckDelayMs);
        Assert.True(s.IsSet.PacingEnabled && s.PacingEnabled);
        Assert.True(s.IsSet.MigrationEnabled && s.MigrationEnabled);
        Assert.True(s.IsSet.DatagramReceiveEnabled && s.DatagramReceiveEnabled);
        Assert.True(s.IsSet.SendBufferingEnabled);
        Assert.False(s.SendBufferingEnabled);
        Assert.True(s.IsSet.ServerResumptionLevel);
        Assert.Equal(QUIC_SERVER_RESUMPTION_LEVEL.NO_RESUME, s.ServerResumptionLevel);
        Assert.True(s.IsSet.CongestionControlAlgorithm);
        Assert.Equal((ushort)QUIC_CONGESTION_CONTROL_ALGORITHM.CUBIC, s.CongestionControlAlgorithm);
        // Stream multi-receive (preview) is never enabled.
        Assert.False(s.IsSet.StreamMultiReceiveEnabled);
        Assert.Equal(0UL, s.Flags);
        Assert.False(s.IsSet.StreamRecvWindowBidiLocalDefault || s.IsSet.StreamRecvWindowBidiRemoteDefault || s.IsSet.GreaseQuicBitEnabled);

        QUIC_SETTINGS client = MsQuicSettings.Client().ToNative();
        Assert.Equal(10_000u, client.KeepAliveIntervalMs);
        client.IsSet.KeepAliveIntervalMs = false;
        client.KeepAliveIntervalMs = 0;
        Assert.Equal(s.IsSetFlags & ~(1UL << 16), client.IsSetFlags);
    }

    [Fact]
    public void Builder_empty_sets_nothing()
    {
        QUIC_SETTINGS s = MsQuicSettings.Empty.ToNative();
        Assert.Equal(0UL, s.IsSetFlags);
        Assert.Equal(0UL, s.Flags);
        Assert.Equal(0, s._bitfield);
    }

    [Fact]
    public void Builder_maps_every_property()
    {
        var b = new MsQuicSettings
        {
            IdleTimeout = TimeSpan.FromSeconds(1),
            HandshakeIdleTimeout = TimeSpan.FromSeconds(2),
            KeepAliveInterval = TimeSpan.FromSeconds(3),
            DisconnectTimeout = TimeSpan.FromSeconds(4),
            PeerBidiStreamCount = 5,
            PeerUnidiStreamCount = 6,
            DatagramReceiveEnabled = false,
            SendBufferingEnabled = true,
            PacingEnabled = true,
            MigrationEnabled = false,
            GreaseQuicBitEnabled = true,
            EcnEnabled = true,
            HyStartEnabled = true,
            ConnFlowControlWindow = 7,
            StreamRecvWindowDefault = 8,
            StreamRecvBufferDefault = 9,
            InitialRttMs = 10,
            MaxAckDelayMs = 11,
            SendIdleTimeoutMs = 12,
            MaxBytesPerKey = 13,
            MinimumMtu = 1300,
            MaximumMtu = 1400,
            ServerResumptionLevel = QUIC_SERVER_RESUMPTION_LEVEL.RESUME_AND_ZERORTT,
            CongestionControlAlgorithm = QUIC_CONGESTION_CONTROL_ALGORITHM.CUBIC,
            MaxOperationsPerDrain = 14,
            StreamRecvWindowUnidiDefault = 1 << 20,
            StreamRecvWindowBidiLocalDefault = 1 << 21,
            StreamRecvWindowBidiRemoteDefault = 1 << 22,
            InitialWindowPackets = 15,
        };
        QUIC_SETTINGS s = b.ToNative();
        Assert.Equal(1u << 20, s.StreamRecvWindowUnidiDefault);
        Assert.Equal(1u << 21, s.StreamRecvWindowBidiLocalDefault);
        Assert.Equal(1u << 22, s.StreamRecvWindowBidiRemoteDefault);
        Assert.Equal(15u, s.InitialWindowPackets);
        Assert.Equal(1000UL, s.IdleTimeoutMs);
        Assert.Equal(2000UL, s.HandshakeIdleTimeoutMs);
        Assert.Equal(3000u, s.KeepAliveIntervalMs);
        Assert.Equal(4000u, s.DisconnectTimeoutMs);
        Assert.Equal(5, s.PeerBidiStreamCount);
        Assert.Equal(6, s.PeerUnidiStreamCount);
        Assert.False(s.DatagramReceiveEnabled);
        Assert.True(s.SendBufferingEnabled);
        Assert.True(s.PacingEnabled);
        Assert.False(s.MigrationEnabled);
        Assert.True(s.IsSet.MigrationEnabled);
        Assert.True(s.GreaseQuicBitEnabled);
        Assert.True(s.EcnEnabled);
        Assert.True(s.HyStartEnabled);
        Assert.Equal(7u, s.ConnFlowControlWindow);
        Assert.Equal(8u, s.StreamRecvWindowDefault);
        Assert.Equal(9u, s.StreamRecvBufferDefault);
        Assert.Equal(10u, s.InitialRttMs);
        Assert.Equal(11u, s.MaxAckDelayMs);
        Assert.Equal(12u, s.SendIdleTimeoutMs);
        Assert.Equal(13UL, s.MaxBytesPerKey);
        Assert.Equal(1300, s.MinimumMtu);
        Assert.Equal(1400, s.MaximumMtu);
        Assert.Equal(QUIC_SERVER_RESUMPTION_LEVEL.RESUME_AND_ZERORTT, s.ServerResumptionLevel);
        Assert.Equal(0, s.CongestionControlAlgorithm);
        Assert.Equal(14, s.MaxOperationsPerDrain);
        const ulong all = (1UL << 0) | (1UL << 1) | (1UL << 2) | (1UL << 8) | (1UL << 6) | (1UL << 7) | (1UL << 12) | (1UL << 13) | (1UL << 14) | (1UL << 15) | (1UL << 16)
            | (1UL << 17) | (1UL << 18) | (1UL << 19) | (1UL << 22) | (1UL << 23) | (1UL << 24) | (1UL << 25) | (1UL << 26) | (1UL << 27) | (1UL << 28) | (1UL << 29)
            | (1UL << 32) | (1UL << 33) | (1UL << 34) | (1UL << 11) | (1UL << 35) | (1UL << 36) | (1UL << 37);
        Assert.Equal(all, s.IsSetFlags);
    }

    [Fact]
    public void Statistics_bitfield_properties_decode_bits()
    {
        QUIC_STATISTICS_V2 s = default;
        Assert.False(s.VersionNegotiation);
        s._bitfield = 0x7F;
        Assert.True(s.VersionNegotiation && s.StatelessRetry && s.ResumptionAttempted && s.ResumptionSucceeded && s.GreaseBitNegotiated && s.EcnCapable && s.EncryptionOffloaded);
        s._bitfield = 0x02;
        Assert.False(s.VersionNegotiation);
        Assert.True(s.StatelessRetry);
        Assert.False(s.ResumptionAttempted);
    }

    [Fact]
    public void Tls_secrets_is_set_bits()
    {
        QUIC_TLS_SECRETS t = default;
        t.IsSet.ClientRandom = true;
        t.IsSet.ServerTrafficSecret0 = true;
        Assert.Equal(0x21, t.IsSet._bitfield);
        Assert.True(t.IsSet.ClientRandom && t.IsSet.ServerTrafficSecret0);
        Assert.False(t.IsSet.ClientEarlyTrafficSecret || t.IsSet.ClientHandshakeTrafficSecret || t.IsSet.ServerHandshakeTrafficSecret || t.IsSet.ClientTrafficSecret0);
        t.IsSet.ClientEarlyTrafficSecret = true;
        t.IsSet.ClientHandshakeTrafficSecret = true;
        t.IsSet.ServerHandshakeTrafficSecret = true;
        t.IsSet.ClientTrafficSecret0 = true;
        Assert.Equal(0x3F, t.IsSet._bitfield);
        t.IsSet.ClientRandom = false;
        t.IsSet.ClientEarlyTrafficSecret = false;
        t.IsSet.ClientHandshakeTrafficSecret = false;
        t.IsSet.ServerHandshakeTrafficSecret = false;
        t.IsSet.ClientTrafficSecret0 = false;
        t.IsSet.ServerTrafficSecret0 = false;
        Assert.Equal(0, t.IsSet._bitfield);
    }

    [Fact]
    public unsafe void Event_bitfield_properties_decode_bits()
    {
        QUIC_CONNECTION_EVENT c = default;
        c.SHUTDOWN_COMPLETE._bitfield = 0x07;
        Assert.True(c.SHUTDOWN_COMPLETE.HandshakeCompleted && c.SHUTDOWN_COMPLETE.PeerAcknowledgedShutdown && c.SHUTDOWN_COMPLETE.AppCloseInProgress);
        c.SHUTDOWN_COMPLETE._bitfield = 0x02;
        Assert.False(c.SHUTDOWN_COMPLETE.HandshakeCompleted);
        Assert.True(c.SHUTDOWN_COMPLETE.PeerAcknowledgedShutdown);
        Assert.False(c.SHUTDOWN_COMPLETE.AppCloseInProgress);

        QUIC_STREAM_EVENT s = default;
        s.START_COMPLETE._bitfield = 1;
        Assert.True(s.START_COMPLETE.PeerAccepted);
        s.SHUTDOWN_COMPLETE._bitfield = 0x07;
        Assert.True(s.SHUTDOWN_COMPLETE.AppCloseInProgress && s.SHUTDOWN_COMPLETE.ConnectionShutdownByApp && s.SHUTDOWN_COMPLETE.ConnectionClosedRemotely);
        s.SHUTDOWN_COMPLETE._bitfield = 0x04;
        Assert.False(s.SHUTDOWN_COMPLETE.AppCloseInProgress);
        Assert.False(s.SHUTDOWN_COMPLETE.ConnectionShutdownByApp);
        Assert.True(s.SHUTDOWN_COMPLETE.ConnectionClosedRemotely);

        QUIC_LISTENER_EVENT l = default;
        l.STOP_COMPLETE._bitfield = 1;
        Assert.True(l.STOP_COMPLETE.AppCloseInProgress);
        l.DOS_MODE_CHANGED._bitfield = 1;
        Assert.True(l.DOS_MODE_CHANGED.DosModeEnabled);
        l.STOP_COMPLETE._bitfield = 0;
        Assert.False(l.STOP_COMPLETE.AppCloseInProgress);
        Assert.False(l.DOS_MODE_CHANGED.DosModeEnabled);

        // Accessors alias the union.
        c.SHUTDOWN_INITIATED_BY_PEER.ErrorCode = 5;
        Assert.Equal(5UL, c.Anonymous.SHUTDOWN_INITIATED_BY_PEER.ErrorCode);
        s.PEER_SEND_ABORTED.ErrorCode = 9;
        Assert.Equal(9UL, s.PEER_RECEIVE_ABORTED.ErrorCode);
        Assert.Equal(9UL, s.IDEAL_SEND_BUFFER_SIZE.ByteCount);
        Assert.Equal(9UL, s.CANCEL_ON_LOSS.ErrorCode);
        l.NEW_CONNECTION.Connection = (QUIC_HANDLE*)8;
        Assert.True(l.Anonymous.NEW_CONNECTION.Connection == (QUIC_HANDLE*)8);

        QUIC_CREDENTIAL_CONFIG cred = default;
        cred.CertificateContext = (void*)1;
        Assert.True(cred.CertificatePkcs12 == (QUIC_CERTIFICATE_PKCS12*)1);
        Assert.True(cred.CertificateHash == (QUIC_CERTIFICATE_HASH*)1);
        Assert.True(cred.CertificateHashStore == (QUIC_CERTIFICATE_HASH_STORE*)1);
        Assert.True(cred.CertificateFile == (QUIC_CERTIFICATE_FILE*)1);
        Assert.True(cred.CertificateFileProtected == (QUIC_CERTIFICATE_FILE_PROTECTED*)1);

        byte* p = stackalloc byte[3];
        var buffer = new QUIC_BUFFER(p, 3);
        Assert.Equal(3, buffer.Span.Length);
        Assert.True(buffer.Buffer == p);
        c.CONNECTED.NegotiatedAlpn = p;
        c.CONNECTED.NegotiatedAlpnLength = 2;
        Assert.Equal(2, c.CONNECTED.NegotiatedAlpnSpan.Length);
        QUIC_NEW_CONNECTION_INFO info = default;
        info.NegotiatedAlpn = p;
        info.NegotiatedAlpnLength = 3;
        info.ServerName = (sbyte*)p;
        info.ServerNameLength = 1;
        Assert.Equal(3, info.NegotiatedAlpnSpan.Length);
        Assert.Equal(1, info.ServerNameSpan.Length);
        Assert.True(QuicDatagramSendState.IsFinal(QUIC_DATAGRAM_SEND_STATE.LOST_DISCARDED));
        Assert.True(QuicDatagramSendState.IsFinal(QUIC_DATAGRAM_SEND_STATE.CANCELED));
        Assert.False(QuicDatagramSendState.IsFinal(QUIC_DATAGRAM_SEND_STATE.SENT));
        Assert.False(QuicDatagramSendState.IsFinal(QUIC_DATAGRAM_SEND_STATE.LOST_SUSPECT));
    }
}
