using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// Builder for <see cref="QUIC_SETTINGS"/> with the transport defaults of ARCHITECTURE §7. A null property means
/// "leave MsQuic's default". The parameterless constructor produces the server posture (one bidirectional
/// stream, no unidirectional streams before admission, no keep-alive); <see cref="Client"/> differs only in the
/// keep-alive interval (10 s, below common NAT binding timeouts). Stream multi-receive is never enabled: it is a
/// preview feature and the wrapper does not expose it.
/// </summary>
public sealed class MsQuicSettings
{
    /// <summary>Connection idle timeout (30 s): dead-client detection when nothing is in flight.</summary>
    public TimeSpan? IdleTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Handshake must finish within this time (5 s).</summary>
    public TimeSpan? HandshakeIdleTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Time without acknowledgement before the connection is considered lost (6 s).</summary>
    public TimeSpan? DisconnectTimeout { get; set; } = TimeSpan.FromSeconds(6);

    /// <summary>Interval between keep-alive PINGs; <see cref="TimeSpan.Zero"/> disables them (server default).</summary>
    public TimeSpan? KeepAliveInterval { get; set; } = TimeSpan.Zero;

    /// <summary>Number of bidirectional streams the peer may open (1 before admission: the control stream).</summary>
    public ushort? PeerBidiStreamCount { get; set; } = 1;

    /// <summary>Number of unidirectional streams the peer may open (0 before admission; raised with <see cref="MsQuicConnection.SetLocalUnidiStreamCount"/>).</summary>
    public ushort? PeerUnidiStreamCount { get; set; } = 0;

    /// <summary>Per-stream receive window for peer-initiated unidirectional streams (2 MiB).</summary>
    public uint? StreamRecvWindowUnidiDefault { get; set; } = 2 * 1024 * 1024;

    /// <summary>Per-stream receive window for locally-initiated bidirectional streams (null: MsQuic default).</summary>
    public uint? StreamRecvWindowBidiLocalDefault { get; set; }

    /// <summary>Per-stream receive window for peer-initiated bidirectional streams (null: MsQuic default).</summary>
    public uint? StreamRecvWindowBidiRemoteDefault { get; set; }

    /// <summary>Connection-wide flow control window (16 MiB).</summary>
    public uint? ConnFlowControlWindow { get; set; } = 16 * 1024 * 1024;

    /// <summary>Smallest MTU probed (1 248).</summary>
    public ushort? MinimumMtu { get; set; } = 1248;

    /// <summary>Largest MTU probed (1 500).</summary>
    public ushort? MaximumMtu { get; set; } = 1500;

    /// <summary>Maximum ACK delay (5 ms): faster loss detection and send-state reporting.</summary>
    public uint? MaxAckDelayMs { get; set; } = 5;

    /// <summary>Pacing (true; benchmarked, configurable).</summary>
    public bool? PacingEnabled { get; set; } = true;

    /// <summary>Connection migration (true: Wi-Fi to LTE without reconnect).</summary>
    public bool? MigrationEnabled { get; set; } = true;

    /// <summary>Advertise support for receiving QUIC DATAGRAM frames (true).</summary>
    public bool? DatagramReceiveEnabled { get; set; } = true;

    /// <summary>When false (default) MsQuic sends directly from application buffers (zero-copy).</summary>
    public bool? SendBufferingEnabled { get; set; } = false;

    /// <summary>Server resumption level (NO_RESUME; see PROTOCOL §4.2).</summary>
    public QUIC_SERVER_RESUMPTION_LEVEL? ServerResumptionLevel { get; set; } = QUIC_SERVER_RESUMPTION_LEVEL.NO_RESUME;

    /// <summary>Congestion control (CUBIC; BBR is the option).</summary>
    public QUIC_CONGESTION_CONTROL_ALGORITHM? CongestionControlAlgorithm { get; set; } = QUIC_CONGESTION_CONTROL_ALGORITHM.CUBIC;

    public bool? GreaseQuicBitEnabled { get; set; }
    public bool? EcnEnabled { get; set; }
    public bool? HyStartEnabled { get; set; }
    public uint? StreamRecvWindowDefault { get; set; }
    public uint? StreamRecvBufferDefault { get; set; }
    public uint? InitialRttMs { get; set; }
    public uint? SendIdleTimeoutMs { get; set; }
    public uint? InitialWindowPackets { get; set; }
    public ulong? MaxBytesPerKey { get; set; }
    public byte? MaxOperationsPerDrain { get; set; }

    /// <summary>A builder with the server defaults of ARCHITECTURE §7.</summary>
    public static MsQuicSettings Default => new();

    /// <summary>A builder with the client defaults of ARCHITECTURE §7 (keep-alive every 10 s).</summary>
    public static MsQuicSettings Client() => new() { KeepAliveInterval = TimeSpan.FromSeconds(10) };

    /// <summary>A builder with every property null, i.e. pure MsQuic defaults.</summary>
    public static MsQuicSettings Empty => new()
    {
        IdleTimeout = null,
        HandshakeIdleTimeout = null,
        DisconnectTimeout = null,
        KeepAliveInterval = null,
        PeerBidiStreamCount = null,
        PeerUnidiStreamCount = null,
        StreamRecvWindowUnidiDefault = null,
        ConnFlowControlWindow = null,
        MinimumMtu = null,
        MaximumMtu = null,
        MaxAckDelayMs = null,
        PacingEnabled = null,
        MigrationEnabled = null,
        DatagramReceiveEnabled = null,
        SendBufferingEnabled = null,
        ServerResumptionLevel = null,
        CongestionControlAlgorithm = null,
    };

    /// <summary>Produces the native struct with the correct <c>IsSet</c> bits.</summary>
    public QUIC_SETTINGS ToNative()
    {
        QUIC_SETTINGS s = default;
        if (IdleTimeout is { } idle) s.SetIdleTimeoutMs((ulong)idle.TotalMilliseconds);
        if (HandshakeIdleTimeout is { } hs) s.SetHandshakeIdleTimeoutMs((ulong)hs.TotalMilliseconds);
        if (DisconnectTimeout is { } dc) s.SetDisconnectTimeoutMs((uint)dc.TotalMilliseconds);
        if (KeepAliveInterval is { } ka) s.SetKeepAliveIntervalMs((uint)ka.TotalMilliseconds);
        if (PeerBidiStreamCount is { } bidi) s.SetPeerBidiStreamCount(bidi);
        if (PeerUnidiStreamCount is { } unidi) s.SetPeerUnidiStreamCount(unidi);
        if (StreamRecvWindowUnidiDefault is { } uniWindow) s.SetStreamRecvWindowUnidiDefault(uniWindow);
        if (StreamRecvWindowBidiLocalDefault is { } bidiLocal) s.SetStreamRecvWindowBidiLocalDefault(bidiLocal);
        if (StreamRecvWindowBidiRemoteDefault is { } bidiRemote) s.SetStreamRecvWindowBidiRemoteDefault(bidiRemote);
        if (ConnFlowControlWindow is { } cfc) s.SetConnFlowControlWindow(cfc);
        if (MinimumMtu is { } minMtu) s.SetMinimumMtu(minMtu);
        if (MaximumMtu is { } maxMtu) s.SetMaximumMtu(maxMtu);
        if (MaxAckDelayMs is { } ack) s.SetMaxAckDelayMs(ack);
        if (PacingEnabled is { } pacing) s.SetPacingEnabled(pacing);
        if (MigrationEnabled is { } mig) s.SetMigrationEnabled(mig);
        if (DatagramReceiveEnabled is { } dg) s.SetDatagramReceiveEnabled(dg);
        if (SendBufferingEnabled is { } sb) s.SetSendBufferingEnabled(sb);
        if (ServerResumptionLevel is { } srl) s.SetServerResumptionLevel(srl);
        if (CongestionControlAlgorithm is { } cc) s.SetCongestionControlAlgorithm(cc);
        if (GreaseQuicBitEnabled is { } grease) s.SetGreaseQuicBitEnabled(grease);
        if (EcnEnabled is { } ecn) s.SetEcnEnabled(ecn);
        if (HyStartEnabled is { } hy) s.SetHyStartEnabled(hy);
        if (StreamRecvWindowDefault is { } srw) s.SetStreamRecvWindowDefault(srw);
        if (StreamRecvBufferDefault is { } srb) s.SetStreamRecvBufferDefault(srb);
        if (InitialRttMs is { } rtt) s.SetInitialRttMs(rtt);
        if (SendIdleTimeoutMs is { } sit) s.SetSendIdleTimeoutMs(sit);
        if (InitialWindowPackets is { } iwp) s.SetInitialWindowPackets(iwp);
        if (MaxBytesPerKey is { } mbk) s.SetMaxBytesPerKey(mbk);
        if (MaxOperationsPerDrain is { } mopd) s.SetMaxOperationsPerDrain(mopd);
        return s;
    }
}
