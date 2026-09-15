using Tedd.Quicly.Http3.Qpack;

namespace Tedd.Quicly.Http3.WebTransport;

/// <summary>Outcome of <see cref="WebTransportRequest.Validate"/>.</summary>
public enum WebTransportRequestStatus : byte
{
    /// <summary>The field list is a WebTransport Extended CONNECT request.</summary>
    Ok = 0,
    /// <summary>The <c>:method</c> pseudo-header is missing or not <c>CONNECT</c>.</summary>
    NotConnect,
    /// <summary>The <c>:protocol</c> pseudo-header is missing or not <c>webtransport</c>.</summary>
    NotWebTransport,
    /// <summary>The <c>:scheme</c> pseudo-header is missing or not <c>https</c>.</summary>
    InvalidScheme,
    /// <summary>The <c>:authority</c> pseudo-header is missing or empty.</summary>
    MissingAuthority,
    /// <summary>The <c>:path</c> pseudo-header is missing or empty.</summary>
    MissingPath,
}

/// <summary>The request components extracted by <see cref="WebTransportRequest.Validate"/>.</summary>
public readonly ref struct WebTransportConnectRequest
{
    /// <summary>The <c>:authority</c> value.</summary>
    public readonly ReadOnlySpan<byte> Authority;
    /// <summary>The <c>:path</c> value.</summary>
    public readonly ReadOnlySpan<byte> Path;
    /// <summary>The <c>origin</c> value, or empty when absent (non-browser clients).</summary>
    public readonly ReadOnlySpan<byte> Origin;
    /// <summary>
    /// True when the request carried the legacy <c>sec-webtransport-http3-draft02</c> field (draft-02 clients),
    /// i.e. the response should include <c>sec-webtransport-http3-draft: draft02</c>.
    /// </summary>
    public readonly bool IsLegacyDraft02;

    /// <summary>Creates the view.</summary>
    public WebTransportConnectRequest(ReadOnlySpan<byte> authority, ReadOnlySpan<byte> path, ReadOnlySpan<byte> origin, bool isLegacyDraft02 = false)
    {
        Authority = authority;
        Path = path;
        Origin = origin;
        IsLegacyDraft02 = isLegacyDraft02;
    }
}

/// <summary>
/// Builds and validates the header lists of WebTransport session establishment (draft-ietf-webtrans-http3 §3,
/// RFC 9220 Extended CONNECT) and plain HTTP/3 responses. Field lists are written into an
/// <see cref="Http3HeaderCollection"/>; the <c>Encode*</c> variants write the QPACK-encoded HEADERS payload directly.
/// </summary>
public static class WebTransportRequest
{
    /// <summary>Value of <c>sec-webtransport-http3-draft</c> sent to legacy (draft-02) clients.</summary>
    public static ReadOnlySpan<byte> LegacyDraftValue => "draft02"u8;

    private static ReadOnlySpan<byte> MethodName => ":method"u8;
    private static ReadOnlySpan<byte> ProtocolName => ":protocol"u8;
    private static ReadOnlySpan<byte> SchemeName => ":scheme"u8;
    private static ReadOnlySpan<byte> AuthorityName => ":authority"u8;
    private static ReadOnlySpan<byte> PathName => ":path"u8;
    private static ReadOnlySpan<byte> OriginName => "origin"u8;
    private static ReadOnlySpan<byte> StatusName => ":status"u8;
    private static ReadOnlySpan<byte> ContentTypeName => "content-type"u8;
    private static ReadOnlySpan<byte> ContentLengthName => "content-length"u8;
    private static ReadOnlySpan<byte> LegacyDraftName => "sec-webtransport-http3-draft"u8;
    private static ReadOnlySpan<byte> LegacyDraft02RequestName => "sec-webtransport-http3-draft02"u8;
    private static ReadOnlySpan<byte> CapsuleProtocolName => "capsule-protocol"u8;
    private static ReadOnlySpan<byte> CapsuleProtocolValue => "?1"u8;
    private static ReadOnlySpan<byte> Connect => "CONNECT"u8;
    private static ReadOnlySpan<byte> WebTransportProtocol => "webtransport"u8;
    private static ReadOnlySpan<byte> Https => "https"u8;

    /// <summary>
    /// Appends the Extended CONNECT request fields (<c>:method CONNECT</c>, <c>:protocol webtransport</c>,
    /// <c>:scheme https</c>, <c>:authority</c>, <c>:path</c> and, when non-empty, <c>origin</c>).
    /// Returns false when the collection is full.
    /// </summary>
    public static bool TryBuildConnectRequest(Http3HeaderCollection headers, ReadOnlySpan<byte> authority, ReadOnlySpan<byte> path, ReadOnlySpan<byte> origin)
    {
        ArgumentNullException.ThrowIfNull(headers);
        return headers.TryAdd(MethodName, Connect)
            && headers.TryAdd(ProtocolName, WebTransportProtocol)
            && headers.TryAdd(SchemeName, Https)
            && headers.TryAdd(AuthorityName, authority)
            && headers.TryAdd(PathName, path)
            && (origin.IsEmpty || headers.TryAdd(OriginName, origin));
    }

    /// <summary>Writes the QPACK-encoded Extended CONNECT request. Returns bytes written or -1 when the destination is too small.</summary>
    public static int EncodeConnectRequest(Span<byte> destination, ReadOnlySpan<byte> authority, ReadOnlySpan<byte> path, ReadOnlySpan<byte> origin, bool useHuffman = true)
    {
        var e = new QpackEncoder(destination, useHuffman);
        bool ok = e.TryWrite(MethodName, Connect)
            && e.TryWrite(ProtocolName, WebTransportProtocol)
            && e.TryWrite(SchemeName, Https)
            && e.TryWrite(AuthorityName, authority)
            && e.TryWrite(PathName, path)
            && (origin.IsEmpty || e.TryWrite(OriginName, origin));
        return ok ? e.BytesWritten : -1;
    }

    /// <summary>Checks that <paramref name="headers"/> is a WebTransport Extended CONNECT request and extracts its components.</summary>
    public static WebTransportRequestStatus Validate(Http3HeaderCollection headers, out WebTransportConnectRequest request)
    {
        ArgumentNullException.ThrowIfNull(headers);
        request = default;
        if (!headers.TryGet(MethodName, out ReadOnlySpan<byte> method) || !method.SequenceEqual(Connect)) return WebTransportRequestStatus.NotConnect;
        if (!headers.TryGet(ProtocolName, out ReadOnlySpan<byte> protocol) || !protocol.SequenceEqual(WebTransportProtocol)) return WebTransportRequestStatus.NotWebTransport;
        if (!headers.TryGet(SchemeName, out ReadOnlySpan<byte> scheme) || !scheme.SequenceEqual(Https)) return WebTransportRequestStatus.InvalidScheme;
        if (!headers.TryGet(AuthorityName, out ReadOnlySpan<byte> authority) || authority.IsEmpty) return WebTransportRequestStatus.MissingAuthority;
        if (!headers.TryGet(PathName, out ReadOnlySpan<byte> path) || path.IsEmpty) return WebTransportRequestStatus.MissingPath;
        headers.TryGet(OriginName, out ReadOnlySpan<byte> origin);
        request = new WebTransportConnectRequest(authority, path, origin, headers.Contains(LegacyDraft02RequestName));
        return WebTransportRequestStatus.Ok;
    }

    /// <summary>
    /// Appends the fields of the session-accepting response: <c>:status 200</c>; when
    /// <paramref name="includeLegacyDraftHeader"/> is set (pass <see cref="WebTransportConnectRequest.IsLegacyDraft02"/>),
    /// <c>sec-webtransport-http3-draft: draft02</c>; and when <paramref name="includeCapsuleProtocol"/> is set,
    /// <c>capsule-protocol: ?1</c> (RFC 9297 §3.1 — optional for WebTransport, which draft-ietf-webtrans-http3-13
    /// says endpoints may ignore; off by default). Returns false when the collection is full.
    /// </summary>
    public static bool TryBuildConnectResponse(Http3HeaderCollection headers, bool includeLegacyDraftHeader = false, bool includeCapsuleProtocol = false)
    {
        ArgumentNullException.ThrowIfNull(headers);
        return headers.TryAdd(StatusName, "200"u8)
            && (!includeLegacyDraftHeader || headers.TryAdd(LegacyDraftName, LegacyDraftValue))
            && (!includeCapsuleProtocol || headers.TryAdd(CapsuleProtocolName, CapsuleProtocolValue));
    }

    /// <summary>
    /// Writes the QPACK-encoded session-accepting response (same fields as <see cref="TryBuildConnectResponse"/>).
    /// Returns bytes written or -1 when the destination is too small.
    /// </summary>
    public static int EncodeConnectResponse(Span<byte> destination, bool includeLegacyDraftHeader = false, bool includeCapsuleProtocol = false, bool useHuffman = true)
    {
        var e = new QpackEncoder(destination, useHuffman);
        bool ok = e.TryWrite(StatusName, "200"u8)
            && (!includeLegacyDraftHeader || e.TryWrite(LegacyDraftName, LegacyDraftValue))
            && (!includeCapsuleProtocol || e.TryWrite(CapsuleProtocolName, CapsuleProtocolValue));
        return ok ? e.BytesWritten : -1;
    }

    /// <summary>
    /// Appends a plain HTTP/3 response header list: <c>:status</c>, optional <c>content-type</c> (when non-empty)
    /// and <c>content-length</c> (when <paramref name="contentLength"/> is non-negative).
    /// Returns false when the collection is full or the status is not a three-digit code.
    /// </summary>
    public static bool TryBuildResponse(Http3HeaderCollection headers, int status, ReadOnlySpan<byte> contentType, long contentLength)
    {
        ArgumentNullException.ThrowIfNull(headers);
        Span<byte> statusText = stackalloc byte[3];
        if (!TryFormatStatus(status, statusText)) return false;
        Span<byte> lengthText = stackalloc byte[20];
        int lengthLen = contentLength >= 0 ? FormatInt64(contentLength, lengthText) : 0;
        return headers.TryAdd(StatusName, statusText)
            && (contentType.IsEmpty || headers.TryAdd(ContentTypeName, contentType))
            && (contentLength < 0 || headers.TryAdd(ContentLengthName, lengthText.Slice(0, lengthLen)));
    }

    /// <summary>
    /// Writes the QPACK-encoded plain HTTP/3 response header block. Returns bytes written, or -1 when the destination
    /// is too small or the status is not a three-digit code.
    /// </summary>
    public static int EncodeResponse(Span<byte> destination, int status, ReadOnlySpan<byte> contentType, long contentLength, bool useHuffman = true)
    {
        Span<byte> statusText = stackalloc byte[3];
        if (!TryFormatStatus(status, statusText)) return -1;
        Span<byte> lengthText = stackalloc byte[20];
        int lengthLen = contentLength >= 0 ? FormatInt64(contentLength, lengthText) : 0;
        var e = new QpackEncoder(destination, useHuffman);
        bool ok = e.TryWrite(StatusName, statusText)
            && (contentType.IsEmpty || e.TryWrite(ContentTypeName, contentType))
            && (contentLength < 0 || e.TryWrite(ContentLengthName, lengthText.Slice(0, lengthLen)));
        return ok ? e.BytesWritten : -1;
    }

    private static bool TryFormatStatus(int status, Span<byte> destination)
    {
        if (status < 100 || status > 999) return false;
        destination[0] = (byte)('0' + status / 100);
        destination[1] = (byte)('0' + status / 10 % 10);
        destination[2] = (byte)('0' + status % 10);
        return true;
    }

    private static int FormatInt64(long value, Span<byte> destination)
    {
        value.TryFormat(destination, out int written, default, System.Globalization.CultureInfo.InvariantCulture);
        return written;
    }
}
