using System.Text;

namespace Tedd.Quicly.Transport.MsQuic.WebTransport;

/// <summary>
/// How a server treats the <c>origin</c> field of an Extended CONNECT request (draft-ietf-webtrans-http3 §3.3).
/// Browsers always send it; native clients normally do not.
/// </summary>
public enum WebTransportOriginPolicy
{
    /// <summary>The field may be absent; when present it is not checked. Default (native clients).</summary>
    Allow = 0,

    /// <summary>The field must be present and must be one of <see cref="WebTransportOptions.AllowedOrigins"/>.</summary>
    Require = 1,

    /// <summary>
    /// The field may be absent (native clients), but when present it must be one of
    /// <see cref="WebTransportOptions.AllowedOrigins"/>.
    /// </summary>
    CheckIfPresent = 2,
}

/// <summary>
/// Options of the WebTransport-over-HTTP/3 carrier (PROTOCOL.md §5): the session path, the origin policy, the HTTP/3
/// limits and the size of the carrier's own tables. Copied when a connector or listener is created.
/// </summary>
/// <remarks>
/// <see cref="MaxStreams"/> must be at least the <see cref="MsQuicTransportOptions.MaxStreams"/> of the transport the
/// carrier runs over, because the carrier mirrors the inner transport's stream slots;
/// <see cref="WebTransportConnector"/> and <see cref="WebTransportListener"/> keep the two in step for you.
/// </remarks>
public sealed class WebTransportOptions
{
    /// <summary>The ALPN of HTTP/3, used by the carrier's connector and listener.</summary>
    public const string Alpn = "h3";

    /// <summary>The default session path (<c>/quicly</c>).</summary>
    public const string DefaultPath = "/quicly";

    /// <summary>Largest capsule the carrier accepts on the CONNECT stream (32 KiB, PROTOCOL.md §5).</summary>
    public const int DefaultMaxCapsuleLength = 32 * 1024;

    private string _path = DefaultPath;

    /// <summary>
    /// The <c>:path</c> of the session. The client sends it, the server accepts only this value. Must start with
    /// <c>/</c>. Default <see cref="DefaultPath"/>.
    /// </summary>
    public string Path
    {
        get => _path;
        set
        {
            ArgumentException.ThrowIfNullOrEmpty(value);
            _path = value;
        }
    }

    /// <summary>
    /// The <c>:authority</c> the client sends. When null the connector uses the server name it was given (or the
    /// endpoint's address when there is none).
    /// </summary>
    public string? Authority { get; set; }

    /// <summary>The <c>origin</c> the client sends. Null (the default) omits the field, which is what native clients do.</summary>
    public string? Origin { get; set; }

    /// <summary>How the server treats the request's <c>origin</c>. Default <see cref="WebTransportOriginPolicy.Allow"/>.</summary>
    public WebTransportOriginPolicy OriginPolicy { get; set; } = WebTransportOriginPolicy.Allow;

    /// <summary>
    /// Origins a server accepts when <see cref="OriginPolicy"/> checks them (exact, case-sensitive, for example
    /// <c>https://game.example</c>).
    /// </summary>
    public IList<string> AllowedOrigins { get; set; } = new List<string>();

    /// <summary>
    /// Capacity of the carrier's stream table; must be at least the inner transport's. Default 1 024.
    /// </summary>
    public int MaxStreams { get; set; } = 1024;

    /// <summary>
    /// Datagram sends the carrier can have outstanding towards a final <see cref="Core.Transport.DatagramSendState"/>.
    /// Each costs 16 bytes. <see cref="Core.Transport.ITransport.SendDatagram"/> answers
    /// <see cref="Core.Transport.TransportStatus.OutOfMemory"/> when they are all in use. Default 1 024.
    /// </summary>
    public int MaxPendingDatagrams { get; set; } = 1024;

    /// <summary>
    /// Datagram sends that can be waiting for packetisation at once. Each holds a gather array of
    /// <see cref="MaxDatagramSegments"/> + 1 segments and is returned as soon as the transport reports the payload
    /// released. Default 64.
    /// </summary>
    public int MaxUnsentDatagrams { get; set; } = 64;

    /// <summary>
    /// Largest number of segments one <see cref="Core.Transport.ITransport.SendDatagram"/> may gather. Larger sends are
    /// refused with <see cref="Core.Transport.TransportStatus.TooLarge"/>. Default 8.
    /// </summary>
    public int MaxDatagramSegments { get; set; } = 8;

    /// <summary>
    /// Largest number of segments one <see cref="Core.Transport.ITransport.SendStream"/> may gather. Only the first send
    /// of a stream, which carries the WebTransport preamble, is bounded by it; later sends are handed on untouched.
    /// Default 64, which is Core's own maximum.
    /// </summary>
    public int MaxStreamSegments { get; set; } = 64;

    /// <summary>
    /// Streams whose first send — the one the preamble rides along with — can be outstanding at once. Each holds a gather
    /// array of <see cref="MaxStreamSegments"/> + 1 segments, returned as soon as that send completes.
    /// <see cref="Core.Transport.ITransport.SendStream"/> answers
    /// <see cref="Core.Transport.TransportStatus.OutOfMemory"/> when they are all in use. Default 16.
    /// </summary>
    public int MaxConcurrentStreamStarts { get; set; } = 16;

    /// <summary>Largest QPACK-encoded field section the carrier accepts (16 KiB, PROTOCOL.md §5).</summary>
    public int MaxFieldSectionSize { get; set; } = (int)Http3.Http3Settings.DefaultMaxFieldSectionSize;

    /// <summary>Largest capsule accepted on the CONNECT stream. Default <see cref="DefaultMaxCapsuleLength"/>.</summary>
    public int MaxCapsuleLength { get; set; } = DefaultMaxCapsuleLength;

    /// <summary>
    /// Largest HTTP/3 frame payload accepted on a control stream. Frames larger than this close the connection with
    /// <c>H3_EXCESSIVE_LOAD</c>. Default 16 KiB.
    /// </summary>
    public int MaxControlFrameLength { get; set; } = 16 * 1024;

    /// <summary>
    /// Bidirectional request streams a server accepts beyond the session's CONNECT stream. The carrier rejects further
    /// ones with <c>H3_REQUEST_REJECTED</c>. Default 15 (16 including the session, PROTOCOL.md §5).
    /// </summary>
    public int MaxExtraRequestStreams { get; set; } = 15;

    /// <summary>Optional diagnostics sink; shares <see cref="MsQuicTransportOptions.Diagnostic"/>'s contract.</summary>
    public TransportDiagnosticCallback? Diagnostic { get; set; }

    /// <summary>The HTTP/3 SETTINGS the carrier sends (PROTOCOL.md §5).</summary>
    public Http3.Http3Settings CreateSettings()
    {
        Http3.Http3Settings s = Http3.Http3Settings.CreateWebTransportServerDefaults(1);
        s.MaxFieldSectionSize = (ulong)MaxFieldSectionSize;
        return s;
    }

    /// <summary>A deep copy (the origin list is copied).</summary>
    public WebTransportOptions Clone()
    {
        var copy = (WebTransportOptions)MemberwiseClone();
        copy.AllowedOrigins = new List<string>(AllowedOrigins);
        return copy;
    }

    /// <summary>Checks the options; throws <see cref="ArgumentException"/> on a problem.</summary>
    public void Validate()
    {
        if (!Path.StartsWith('/')) throw new ArgumentException("Path must start with '/'.", nameof(Path));
        if (Encoding.UTF8.GetByteCount(Path) > 1024) throw new ArgumentException("Path must be at most 1024 bytes.", nameof(Path));
        if (MaxStreams is < 1 or > 1 << 20) throw new ArgumentOutOfRangeException(nameof(MaxStreams), MaxStreams, "Must be between 1 and 1048576.");
        if (MaxPendingDatagrams is < 1 or > 1 << 20) throw new ArgumentOutOfRangeException(nameof(MaxPendingDatagrams), MaxPendingDatagrams, "Must be between 1 and 1048576.");
        if (MaxUnsentDatagrams is < 1 or > 1 << 20) throw new ArgumentOutOfRangeException(nameof(MaxUnsentDatagrams), MaxUnsentDatagrams, "Must be between 1 and 1048576.");
        if (MaxUnsentDatagrams > MaxPendingDatagrams) throw new ArgumentException("MaxUnsentDatagrams cannot exceed MaxPendingDatagrams.", nameof(MaxUnsentDatagrams));
        if (MaxDatagramSegments is < 1 or > 64) throw new ArgumentOutOfRangeException(nameof(MaxDatagramSegments), MaxDatagramSegments, "Must be between 1 and 64.");
        if (MaxStreamSegments is < 1 or > 256) throw new ArgumentOutOfRangeException(nameof(MaxStreamSegments), MaxStreamSegments, "Must be between 1 and 256.");
        if (MaxConcurrentStreamStarts is < 1 or > (1 << 20)) throw new ArgumentOutOfRangeException(nameof(MaxConcurrentStreamStarts), MaxConcurrentStreamStarts, "Must be between 1 and 1048576.");
        if (MaxFieldSectionSize is < 64 or > (1 << 20)) throw new ArgumentOutOfRangeException(nameof(MaxFieldSectionSize), MaxFieldSectionSize, "Must be between 64 and 1048576.");
        if (MaxCapsuleLength is < 16 or > (1 << 20)) throw new ArgumentOutOfRangeException(nameof(MaxCapsuleLength), MaxCapsuleLength, "Must be between 16 and 1048576.");
        if (MaxControlFrameLength is < 16 or > (1 << 20)) throw new ArgumentOutOfRangeException(nameof(MaxControlFrameLength), MaxControlFrameLength, "Must be between 16 and 1048576.");
        if (MaxExtraRequestStreams is < 0 or > 1024) throw new ArgumentOutOfRangeException(nameof(MaxExtraRequestStreams), MaxExtraRequestStreams, "Must be between 0 and 1024.");
        if (OriginPolicy == WebTransportOriginPolicy.Require && AllowedOrigins.Count == 0)
        {
            throw new ArgumentException("OriginPolicy.Require needs at least one allowed origin.", nameof(AllowedOrigins));
        }
    }
}
