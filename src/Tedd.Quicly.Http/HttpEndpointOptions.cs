using System.Net;

namespace Tedd.Quicly.Http;

/// <summary>A listening endpoint: an address/port plus optional TLS.</summary>
public sealed class HttpEndpointOptions
{
    /// <summary>Creates an endpoint. Port 0 binds an ephemeral port; see <see cref="HttpServer.BoundEndpoints"/>.</summary>
    public HttpEndpointOptions(IPEndPoint endPoint, HttpTlsOptions? tls = null)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        EndPoint = endPoint;
        Tls = tls;
    }

    /// <summary>Address and port to bind.</summary>
    public IPEndPoint EndPoint { get; }

    /// <summary>TLS settings, or <see langword="null"/> for plain TCP.</summary>
    public HttpTlsOptions? Tls { get; set; }

    /// <summary>Whether the endpoint serves TLS.</summary>
    public bool IsSecure => Tls is not null;
}
