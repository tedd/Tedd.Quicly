using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// Builder for <see cref="QUIC_SETTINGS"/> with defaults suited to a game transport: datagram receive enabled,
/// send buffering disabled (zero-copy sends, buffers stay alive until SEND_COMPLETE), generous peer stream
/// counts, a 30 s idle timeout and a 15 s keep-alive. A null property means "leave MsQuic's default".
/// </summary>
public sealed class MsQuicSettings
{
    /// <summary>Connection idle timeout; MsQuic closes the connection after this much silence.</summary>
    public TimeSpan? IdleTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Handshake must finish within this time.</summary>
    public TimeSpan? HandshakeIdleTimeout { get; set; }

    /// <summary>Interval between keep-alive PINGs; <see cref="TimeSpan.Zero"/> disables them.</summary>
    public TimeSpan? KeepAliveInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Time without acknowledgement before the connection is considered lost.</summary>
    public TimeSpan? DisconnectTimeout { get; set; }

    /// <summary>Number of bidirectional streams the peer may open.</summary>
    public ushort? PeerBidiStreamCount { get; set; } = 128;

    /// <summary>Number of unidirectional streams the peer may open.</summary>
    public ushort? PeerUnidiStreamCount { get; set; } = 128;

    /// <summary>Advertise support for receiving QUIC DATAGRAM frames.</summary>
    public bool? DatagramReceiveEnabled { get; set; } = true;

    /// <summary>When false (default) MsQuic sends directly from application buffers.</summary>
    public bool? SendBufferingEnabled { get; set; } = false;

    public bool? PacingEnabled { get; set; }
    public bool? MigrationEnabled { get; set; }
    public bool? GreaseQuicBitEnabled { get; set; }
    public bool? EcnEnabled { get; set; }
    public bool? HyStartEnabled { get; set; }
    public uint? ConnFlowControlWindow { get; set; }
    public uint? StreamRecvWindowDefault { get; set; }
    public uint? StreamRecvBufferDefault { get; set; }
    public uint? InitialRttMs { get; set; }
    public uint? MaxAckDelayMs { get; set; }
    public uint? SendIdleTimeoutMs { get; set; }
    public ulong? MaxBytesPerKey { get; set; }
    public ushort? MinimumMtu { get; set; }
    public ushort? MaximumMtu { get; set; }
    public QUIC_SERVER_RESUMPTION_LEVEL? ServerResumptionLevel { get; set; }
    public QUIC_CONGESTION_CONTROL_ALGORITHM? CongestionControlAlgorithm { get; set; }
    public byte? MaxOperationsPerDrain { get; set; }

    /// <summary>A builder with every property at its default.</summary>
    public static MsQuicSettings Default => new();

    /// <summary>A builder with every property null, i.e. pure MsQuic defaults.</summary>
    public static MsQuicSettings Empty => new()
    {
        IdleTimeout = null,
        KeepAliveInterval = null,
        PeerBidiStreamCount = null,
        PeerUnidiStreamCount = null,
        DatagramReceiveEnabled = null,
        SendBufferingEnabled = null,
    };

    /// <summary>Produces the native struct with the correct <c>IsSet</c> bits.</summary>
    public QUIC_SETTINGS ToNative()
    {
        QUIC_SETTINGS s = default;
        if (IdleTimeout is { } idle) s.SetIdleTimeoutMs((ulong)idle.TotalMilliseconds);
        if (HandshakeIdleTimeout is { } hs) s.SetHandshakeIdleTimeoutMs((ulong)hs.TotalMilliseconds);
        if (KeepAliveInterval is { } ka) s.SetKeepAliveIntervalMs((uint)ka.TotalMilliseconds);
        if (DisconnectTimeout is { } dc) s.SetDisconnectTimeoutMs((uint)dc.TotalMilliseconds);
        if (PeerBidiStreamCount is { } bidi) s.SetPeerBidiStreamCount(bidi);
        if (PeerUnidiStreamCount is { } unidi) s.SetPeerUnidiStreamCount(unidi);
        if (DatagramReceiveEnabled is { } dg) s.SetDatagramReceiveEnabled(dg);
        if (SendBufferingEnabled is { } sb) s.SetSendBufferingEnabled(sb);
        if (PacingEnabled is { } pacing) s.SetPacingEnabled(pacing);
        if (MigrationEnabled is { } mig) s.SetMigrationEnabled(mig);
        if (GreaseQuicBitEnabled is { } grease) s.SetGreaseQuicBitEnabled(grease);
        if (EcnEnabled is { } ecn) s.SetEcnEnabled(ecn);
        if (HyStartEnabled is { } hy) s.SetHyStartEnabled(hy);
        if (ConnFlowControlWindow is { } cfc) s.SetConnFlowControlWindow(cfc);
        if (StreamRecvWindowDefault is { } srw) s.SetStreamRecvWindowDefault(srw);
        if (StreamRecvBufferDefault is { } srb) s.SetStreamRecvBufferDefault(srb);
        if (InitialRttMs is { } rtt) s.SetInitialRttMs(rtt);
        if (MaxAckDelayMs is { } ack) s.SetMaxAckDelayMs(ack);
        if (SendIdleTimeoutMs is { } sit) s.SetSendIdleTimeoutMs(sit);
        if (MaxBytesPerKey is { } mbk) s.SetMaxBytesPerKey(mbk);
        if (MinimumMtu is { } minMtu) s.SetMinimumMtu(minMtu);
        if (MaximumMtu is { } maxMtu) s.SetMaximumMtu(maxMtu);
        if (ServerResumptionLevel is { } srl) s.SetServerResumptionLevel(srl);
        if (CongestionControlAlgorithm is { } cc) s.SetCongestionControlAlgorithm(cc);
        if (MaxOperationsPerDrain is { } mopd) s.SetMaxOperationsPerDrain(mopd);
        return s;
    }
}
