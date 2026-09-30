using System.Text;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>How a client validates the server certificate (ARCHITECTURE section 8).</summary>
public enum ServerCertificateValidationMode
{
    /// <summary>Platform validation against the system trust store (default).</summary>
    SystemRoots = 0,
    /// <summary>
    /// The SHA-256 hash of the leaf certificate's SubjectPublicKeyInfo must be one of
    /// <see cref="MsQuicTransportOptions.PinnedSpkiSha256"/>. The pin is the trust anchor: the platform chain result is
    /// ignored, so self-signed server certificates work (the server must keep its key across renewals).
    /// </summary>
    PinnedSpki = 1,
    /// <summary>
    /// <see cref="MsQuicTransportOptions.ServerCertificateValidator"/> decides from the DER leaf, the PKCS#7 chain and the
    /// platform validation result. Platform validation still counts unless the callback opts out with
    /// <see cref="ServerCertificateDecision.AcceptIgnoringPlatformValidation"/>.
    /// </summary>
    Callback = 2,
    /// <summary>
    /// No validation at all. Refused (the connector throws) unless the library was built in DEBUG or the environment
    /// variable <c>QUICLY_ALLOW_INSECURE</c> is <c>1</c>; every connection logs a warning through
    /// <see cref="MsQuicTransportOptions.Diagnostic"/>.
    /// </summary>
    DangerousAcceptAnyServerCertificate = 3,
}

/// <summary>Verdict of a <see cref="ServerCertificateValidator"/>.</summary>
public enum ServerCertificateDecision
{
    /// <summary>Reject: the handshake fails with a <c>bad_certificate</c> alert.</summary>
    Reject = 0,
    /// <summary>Accept, provided the platform validation passed as well.</summary>
    Accept = 1,
    /// <summary>Accept whatever the platform validation said (the callback is the trust decision).</summary>
    AcceptIgnoringPlatformValidation = 2,
}

/// <summary>The server certificate as seen by a <see cref="ServerCertificateValidator"/>. Spans are valid only during the call.</summary>
public readonly ref struct ServerCertificateContext
{
    /// <summary>The leaf certificate, DER encoded.</summary>
    public ReadOnlySpan<byte> LeafDer { get; init; }

    /// <summary>The chain the server sent as a DER-encoded PKCS#7 blob (may be empty).</summary>
    public ReadOnlySpan<byte> ChainPkcs7 { get; init; }

    /// <summary>True when the platform validated the chain against the system trust store and the server name.</summary>
    public bool PlatformValid { get; init; }

    /// <summary>The platform validation status (<c>QUIC_STATUS</c>; 0 when valid).</summary>
    public int PlatformStatus { get; init; }

    /// <summary>The server name the client connected to (SNI), if any.</summary>
    public string? ServerName { get; init; }
}

/// <summary>Validates a server certificate (<see cref="ServerCertificateValidationMode.Callback"/>). Called on an MsQuic worker thread; must not block.</summary>
public delegate ServerCertificateDecision ServerCertificateValidator(in ServerCertificateContext context);

/// <summary>Severity of a <see cref="TransportDiagnosticCallback"/> message.</summary>
public enum TransportDiagnosticLevel
{
    /// <summary>Informational.</summary>
    Information = 0,
    /// <summary>Something insecure or unexpected that does not stop the connection.</summary>
    Warning = 1,
    /// <summary>A failure (for example an exception thrown by a sink).</summary>
    Error = 2,
}

/// <summary>Receives diagnostics from the MsQuic transport. May be called on MsQuic worker threads; must not block or throw.</summary>
public delegate void TransportDiagnosticCallback(TransportDiagnosticLevel level, string message, Exception? exception);

/// <summary>
/// Options for <see cref="MsQuicTransportConnector"/> and <see cref="MsQuicTransportListener"/>: ALPNs, the MsQuic settings of
/// ARCHITECTURE section 7 (separate server and client postures), execution profile, certificate validation and the
/// per-connection stream table size. Copied when a connector or listener is created.
/// </summary>
public sealed class MsQuicTransportOptions
{
    /// <summary>The raw-QUIC ALPN of PROTOCOL.md.</summary>
    public const string DefaultAlpn = "quicly/1";

    /// <summary>Environment variable that permits <see cref="ServerCertificateValidationMode.DangerousAcceptAnyServerCertificate"/> in release builds (value <c>1</c>).</summary>
    public const string AllowInsecureEnvironmentVariable = "QUICLY_ALLOW_INSECURE";

    /// <summary>ALPNs offered (client) or accepted (server, one MsQuic configuration each). Default <c>["quicly/1"]</c>.</summary>
    public string[] Alpns { get; set; } = [DefaultAlpn];

    /// <summary>Worker execution profile of a registration the connector or listener creates itself. Default LOW_LATENCY.</summary>
    public QUIC_EXECUTION_PROFILE ExecutionProfile { get; set; } = QUIC_EXECUTION_PROFILE.LOW_LATENCY;

    /// <summary>Application name of a registration the connector or listener creates itself.</summary>
    public string AppName { get; set; } = "quicly";

    /// <summary>Connection idle timeout (30 s).</summary>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>The handshake must complete within this time (5 s).</summary>
    public TimeSpan HandshakeIdleTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Time without acknowledgement before the connection is considered lost (6 s).</summary>
    public TimeSpan DisconnectTimeout { get; set; } = TimeSpan.FromSeconds(6);

    /// <summary>Client keep-alive interval (10 s, below common NAT binding timeouts). <see cref="TimeSpan.Zero"/> disables it.</summary>
    public TimeSpan ClientKeepAliveInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Server keep-alive interval (0: disabled; the client keeps the path alive).</summary>
    public TimeSpan ServerKeepAliveInterval { get; set; } = TimeSpan.Zero;

    /// <summary>Bidirectional streams a client may open before admission (1: the control stream). Raised with <see cref="Core.Transport.ITransport.UpdatePeerStreamLimits"/>.</summary>
    public ushort ServerPeerBidiStreamCount { get; set; } = 1;

    /// <summary>Unidirectional streams a client may open before admission (0). Raised with <see cref="Core.Transport.ITransport.UpdatePeerStreamLimits"/>.</summary>
    public ushort ServerPeerUnidiStreamCount { get; set; }

    /// <summary>Bidirectional streams the server may open towards the client (0: PROTOCOL section 3 rejects them).</summary>
    public ushort ClientPeerBidiStreamCount { get; set; }

    /// <summary>
    /// Unidirectional streams the server may have open towards the client from the first packet on (per-channel ordered
    /// streams, groups and bulk transfers; ARCHITECTURE section 7 caps the sum at 4 096). Default 1 024.
    /// </summary>
    /// <remarks>
    /// This is a grant in the connection's transport parameters, and QUIC never takes granted credit back: when a QUICLY
    /// session later asks for fewer (its channel table's sum, after admission), the server can still have up to this many
    /// open. The session is told (<see cref="Core.Transport.TransportCapabilities.PeerUnidirectionalStreams"/>) and keeps a
    /// receive record for each of them (64 bytes, plus 8 in a ring), so a client that falls behind its server holds the
    /// streams instead of resetting them. <see cref="MaxStreams"/> must leave room for them next to the local streams.
    /// </remarks>
    public ushort ClientPeerUnidiStreamCount { get; set; } = 1024;

    /// <summary>
    /// Capacity of each connection's stream table (local and peer streams open at once, including streams whose close is
    /// still being processed). <see cref="Core.Transport.ITransport.OpenStream"/> answers <c>OutOfMemory</c> and peer streams are
    /// refused when it is full. Default 2 048; must cover the peer limits plus the local streams.
    /// </summary>
    /// <remarks>
    /// Slots are created as they are needed, so the capacity costs nothing until it is used. A peer stream that finds no slot
    /// is refused below the session (counted in <see cref="MsQuicTransport.RefusedPeerStreamCount"/>) and whatever it carried
    /// is lost, which is why the table must hold everything the peer may open: the larger of the role's initial grant
    /// (<see cref="ClientPeerUnidiStreamCount"/> / <see cref="ServerPeerUnidiStreamCount"/>) and what the session asks for after
    /// admission, next to the local streams. The transport keeps a quarter of the table for the local streams
    /// (<see cref="PeerStreamRoom"/>) and reports through <see cref="Diagnostic"/> when the peer is allowed more than the
    /// rest. The default leaves room for the default client grant of 1 024.
    /// </remarks>
    public int MaxStreams { get; set; } = 2048;

    /// <summary>
    /// Streams the peer can have open in a stream table of <paramref name="maxStreams"/> slots while a quarter of it (at least
    /// one slot) stays free for the local streams.
    /// </summary>
    /// <param name="maxStreams">The table's capacity (<see cref="MaxStreams"/>).</param>
    /// <returns>The room for peer streams.</returns>
    public static int PeerStreamRoom(int maxStreams) => maxStreams - Math.Max(1, maxStreams / 4);

    /// <summary>Stream scheduling (ROUND_ROBIN: bulk must not starve ordered channels).</summary>
    public QUIC_STREAM_SCHEDULING_SCHEME StreamSchedulingScheme { get; set; } = QUIC_STREAM_SCHEDULING_SCHEME.ROUND_ROBIN;

    /// <summary>
    /// How a listener hands its certificate to MsQuic (server only). Default <see cref="MsQuicServerCredentialMode.Auto"/>:
    /// PKCS#12 where MsQuic accepts it, <c>CERTIFICATE_CONTEXT</c> on Windows (the bundled msquic 2.5.10 rejects PKCS#12).
    /// </summary>
    public MsQuicServerCredentialMode ServerCredentialMode { get; set; } = MsQuicServerCredentialMode.Auto;

    /// <summary>Where a key persisted for the <c>CERTIFICATE_CONTEXT</c> path lives (server only, ADR 0009). Default <see cref="MsQuicKeyStorage.User"/>.</summary>
    public MsQuicKeyStorage ServerKeyStorage { get; set; } = MsQuicKeyStorage.User;

    /// <summary>How the client validates the server certificate. Default <see cref="ServerCertificateValidationMode.SystemRoots"/>.</summary>
    public ServerCertificateValidationMode ServerCertificateValidation { get; set; } = ServerCertificateValidationMode.SystemRoots;

    /// <summary>SHA-256 hashes (32 bytes each) of accepted SubjectPublicKeyInfo structures (<see cref="ServerCertificateValidationMode.PinnedSpki"/>).</summary>
    public IList<byte[]> PinnedSpkiSha256 { get; set; } = new List<byte[]>();

    /// <summary>The validator for <see cref="ServerCertificateValidationMode.Callback"/>.</summary>
    public ServerCertificateValidator? ServerCertificateValidator { get; set; }

    /// <summary>Optional diagnostics sink (insecure-mode warnings, sink exceptions, refused peer streams).</summary>
    public TransportDiagnosticCallback? Diagnostic { get; set; }

    /// <summary>Last-chance hook to adjust the server settings built by <see cref="CreateServerSettings"/>.</summary>
    public Action<MsQuicSettings>? ConfigureServerSettings { get; set; }

    /// <summary>Last-chance hook to adjust the client settings built by <see cref="CreateClientSettings"/>.</summary>
    public Action<MsQuicSettings>? ConfigureClientSettings { get; set; }

    /// <summary>The server posture: ARCHITECTURE section 7 defaults, pre-admission stream limits, server keep-alive.</summary>
    public MsQuicSettings CreateServerSettings()
    {
        MsQuicSettings s = CreateCommon();
        s.KeepAliveInterval = ServerKeepAliveInterval;
        s.PeerBidiStreamCount = ServerPeerBidiStreamCount;
        s.PeerUnidiStreamCount = ServerPeerUnidiStreamCount;
        ConfigureServerSettings?.Invoke(s);
        return s;
    }

    /// <summary>The client posture: ARCHITECTURE section 7 defaults, room for the server's streams, client keep-alive.</summary>
    public MsQuicSettings CreateClientSettings()
    {
        MsQuicSettings s = CreateCommon();
        s.KeepAliveInterval = ClientKeepAliveInterval;
        s.PeerBidiStreamCount = ClientPeerBidiStreamCount;
        s.PeerUnidiStreamCount = ClientPeerUnidiStreamCount;
        ConfigureClientSettings?.Invoke(s);
        return s;
    }

    private MsQuicSettings CreateCommon() => new()
    {
        IdleTimeout = IdleTimeout,
        HandshakeIdleTimeout = HandshakeIdleTimeout,
        DisconnectTimeout = DisconnectTimeout,
    };

    /// <summary>A deep copy (lists and arrays are copied).</summary>
    public MsQuicTransportOptions Clone()
    {
        var copy = (MsQuicTransportOptions)MemberwiseClone();
        copy.Alpns = (string[])Alpns.Clone();
        var pins = new List<byte[]>(PinnedSpkiSha256.Count);
        foreach (byte[] pin in PinnedSpkiSha256) pins.Add((byte[])pin.Clone());
        copy.PinnedSpkiSha256 = pins;
        return copy;
    }

    /// <summary>Checks the options; throws <see cref="ArgumentException"/> (or <see cref="InvalidOperationException"/> for the insecure guard) on a problem.</summary>
    /// <param name="client">True when the options configure a client (certificate validation is checked).</param>
    public void Validate(bool client)
    {
        if (Alpns is null || Alpns.Length == 0) throw new ArgumentException("At least one ALPN is required.", nameof(Alpns));
        foreach (string alpn in Alpns)
        {
            if (string.IsNullOrEmpty(alpn) || Encoding.UTF8.GetByteCount(alpn) > 255) throw new ArgumentException($"ALPN '{alpn}' must be 1..255 bytes.", nameof(Alpns));
        }
        if (MaxStreams is < 1 or > 1 << 20) throw new ArgumentOutOfRangeException(nameof(MaxStreams), MaxStreams, "Must be between 1 and 1048576.");
        if (!client) return;
        switch (ServerCertificateValidation)
        {
            case ServerCertificateValidationMode.SystemRoots:
                break;
            case ServerCertificateValidationMode.PinnedSpki:
                if (PinnedSpkiSha256 is null || PinnedSpkiSha256.Count == 0) throw new ArgumentException("PinnedSpki needs at least one pin.", nameof(PinnedSpkiSha256));
                foreach (byte[] pin in PinnedSpkiSha256)
                {
                    if (pin is null || pin.Length != 32) throw new ArgumentException("Every SPKI pin is a 32-byte SHA-256 hash.", nameof(PinnedSpkiSha256));
                }
                break;
            case ServerCertificateValidationMode.Callback:
                if (ServerCertificateValidator is null) throw new ArgumentException("Callback validation needs a ServerCertificateValidator.", nameof(ServerCertificateValidator));
                break;
            case ServerCertificateValidationMode.DangerousAcceptAnyServerCertificate:
                if (!IsInsecureAllowed(Environment.GetEnvironmentVariable(AllowInsecureEnvironmentVariable), IsDebugBuild))
                {
                    throw new InvalidOperationException($"DangerousAcceptAnyServerCertificate disables server authentication; it is only available in DEBUG builds or when {AllowInsecureEnvironmentVariable}=1.");
                }
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(ServerCertificateValidation));
        }
    }

    /// <summary>True when this library was compiled with DEBUG.</summary>
    internal static bool IsDebugBuild
    {
        get
        {
#if DEBUG
            return true;
#else
            return false;
#endif
        }
    }

    /// <summary>The insecure-mode guard: allowed in DEBUG builds or when the environment variable is exactly <c>1</c>.</summary>
    internal static bool IsInsecureAllowed(string? environmentValue, bool debugBuild) => debugBuild || environmentValue == "1";
}
