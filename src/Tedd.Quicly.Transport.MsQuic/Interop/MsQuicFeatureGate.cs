namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>
/// Which optional API surface a given MsQuic library version offers. msquic.h documents the introduction
/// version of table entries ("Available from v2.5") and, for the newer send/start flags, they are accepted only
/// by the library version that added them (older libraries reject unknown flag bits with
/// <c>QUIC_STATUS_INVALID_PARAMETER</c>). The binding refuses libraries older than <see cref="MinimumVersion"/>
/// (ARCHITECTURE §7), so on an accepted library every flag below is available; the gate still exists so callers
/// can express intent and so the rule is testable against synthetic versions.
/// </summary>
public static class MsQuicFeatureGate
{
    /// <summary>The oldest library the binding accepts.</summary>
    public static readonly Version MinimumVersion = new(2, 4);

    /// <summary>Version that added <see cref="QUIC_API_TABLE_EXT_2_5"/> (and the preview block).</summary>
    public static readonly Version Version25 = new(2, 5);

    private static readonly Version s_version22 = new(2, 2);
    private static readonly Version s_version24 = new(2, 4);

    /// <summary>Send flags every 2.x library accepts.</summary>
    public const QUIC_SEND_FLAGS BaseSendFlags = QUIC_SEND_FLAGS.ALLOW_0_RTT | QUIC_SEND_FLAGS.START | QUIC_SEND_FLAGS.FIN | QUIC_SEND_FLAGS.DGRAM_PRIORITY | QUIC_SEND_FLAGS.DELAY_SEND;

    /// <summary>Send flags added in MsQuic 2.2 (<c>CANCEL_ON_LOSS</c> with its stream event, <c>PRIORITY_WORK</c>).</summary>
    public const QUIC_SEND_FLAGS SendFlags22 = QUIC_SEND_FLAGS.CANCEL_ON_LOSS | QUIC_SEND_FLAGS.PRIORITY_WORK;

    /// <summary>Send flags added in MsQuic 2.4 (<c>CANCEL_ON_BLOCKED</c> for datagrams).</summary>
    public const QUIC_SEND_FLAGS SendFlags24 = QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED;

    /// <summary>Start flags every 2.x library accepts.</summary>
    public const QUIC_STREAM_START_FLAGS BaseStartFlags = QUIC_STREAM_START_FLAGS.IMMEDIATE | QUIC_STREAM_START_FLAGS.FAIL_BLOCKED | QUIC_STREAM_START_FLAGS.SHUTDOWN_ON_FAIL | QUIC_STREAM_START_FLAGS.INDICATE_PEER_ACCEPT;

    /// <summary>Start flags added in MsQuic 2.2 (<c>PRIORITY_WORK</c>).</summary>
    public const QUIC_STREAM_START_FLAGS StartFlags22 = QUIC_STREAM_START_FLAGS.PRIORITY_WORK;

    /// <summary>True when <paramref name="version"/> is at least <see cref="MinimumVersion"/>.</summary>
    public static bool IsSupported(Version version)
    {
        ArgumentNullException.ThrowIfNull(version);
        return version >= MinimumVersion;
    }

    /// <summary>True when the library has the 2.5 table extension.</summary>
    public static bool HasExtension25(Version version)
    {
        ArgumentNullException.ThrowIfNull(version);
        return version >= Version25;
    }

    /// <summary>The <see cref="QUIC_SEND_FLAGS"/> bits <paramref name="version"/> accepts.</summary>
    public static QUIC_SEND_FLAGS SupportedSendFlags(Version version)
    {
        ArgumentNullException.ThrowIfNull(version);
        QUIC_SEND_FLAGS flags = BaseSendFlags;
        if (version >= s_version22) flags |= SendFlags22;
        if (version >= s_version24) flags |= SendFlags24;
        return flags;
    }

    /// <summary>The <see cref="QUIC_STREAM_START_FLAGS"/> bits <paramref name="version"/> accepts.</summary>
    public static QUIC_STREAM_START_FLAGS SupportedStartFlags(Version version)
    {
        ArgumentNullException.ThrowIfNull(version);
        QUIC_STREAM_START_FLAGS flags = BaseStartFlags;
        if (version >= s_version22) flags |= StartFlags22;
        return flags;
    }

    /// <summary>True when every bit in <paramref name="flags"/> is accepted by <paramref name="version"/>.</summary>
    public static bool AreSendFlagsSupported(Version version, QUIC_SEND_FLAGS flags) => (flags & ~SupportedSendFlags(version)) == 0;

    /// <summary>True when every bit in <paramref name="flags"/> is accepted by <paramref name="version"/>.</summary>
    public static bool AreStartFlagsSupported(Version version, QUIC_STREAM_START_FLAGS flags) => (flags & ~SupportedStartFlags(version)) == 0;
}
