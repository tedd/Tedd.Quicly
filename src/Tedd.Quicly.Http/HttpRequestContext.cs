using System.Net;
using Tedd.Quicly.Http.Internal;

namespace Tedd.Quicly.Http;

/// <summary>
/// The request being handled plus its <see cref="Response"/>. The instance belongs to the connection and is reused
/// for the next request as soon as the handler completes; do not retain it.
/// </summary>
public sealed class HttpRequestContext
{
    private readonly HttpConnection _connection;

    internal HttpRequestContext(HttpConnection connection, HttpResponse response)
    {
        _connection = connection;
        Response = response;
        Headers = new HttpHeaderCollection(16);
    }

    /// <summary>Request method, upper-case as sent (<c>GET</c>, <c>HEAD</c>, <c>POST</c>, ...).</summary>
    public string Method { get; internal set; } = "GET";

    /// <summary>Percent-decoded path (origin form, always starts with <c>/</c>; <c>*</c> for <c>OPTIONS *</c>).</summary>
    public string Path { get; internal set; } = "/";

    /// <summary>Raw query string without the leading <c>?</c>; empty when absent.</summary>
    public string QueryString { get; internal set; } = string.Empty;

    /// <summary>The request target exactly as received.</summary>
    public string RawTarget { get; internal set; } = "/";

    /// <summary>Protocol version.</summary>
    public HttpProtocolVersion Version { get; internal set; } = HttpProtocolVersion.Http11;

    /// <summary>Request headers.</summary>
    public HttpHeaderCollection Headers { get; }

    /// <summary>Value of the <c>Host</c> header (or the authority of an absolute-form target), or <see langword="null"/>.</summary>
    public string? Host => Headers["Host"];

    /// <summary>Declared body length (<c>Content-Length</c>); 0 when there is no body.</summary>
    public long ContentLength { get; internal set; }

    /// <summary>Whether the request carries a body.</summary>
    public bool HasBody => ContentLength > 0;

    /// <summary>Streams the request body; reading it sends <c>100 Continue</c> first when the client asked for it.</summary>
    public Stream Body => _connection.BodyStream;

    /// <summary>The response for this request.</summary>
    public HttpResponse Response { get; }

    /// <summary>Remote peer address.</summary>
    public IPEndPoint? RemoteEndPoint => _connection.RemoteEndPoint;

    /// <summary>Local address the request arrived on.</summary>
    public IPEndPoint? LocalEndPoint => _connection.LocalEndPoint;

    /// <summary>Whether the request arrived over TLS.</summary>
    public bool IsSecure => _connection.IsSecure;

    /// <summary>SNI server name presented during the TLS handshake, if any.</summary>
    public string? TlsServerName => _connection.TlsServerName;

    /// <summary>Identifier of the underlying connection (monotonic per server process).</summary>
    public long ConnectionId => _connection.Id;

    /// <summary>Canceled when the connection is aborted (client gone, server shutting down hard).</summary>
    public CancellationToken RequestAborted => _connection.AbortToken;

    /// <summary>
    /// Reads the remaining body into a pooled buffer that stays valid until the handler returns. Subsequent calls
    /// return the same memory.
    /// </summary>
    public ValueTask<ReadOnlyMemory<byte>> ReadBodyAsync(CancellationToken cancellationToken = default) => _connection.ReadWholeBodyAsync(cancellationToken);

    internal void Reset()
    {
        Method = "GET";
        Path = "/";
        QueryString = string.Empty;
        RawTarget = "/";
        Version = HttpProtocolVersion.Http11;
        ContentLength = 0;
        Headers.Clear();
    }
}
