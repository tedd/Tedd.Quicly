using System.Net;

namespace Tedd.Quicly.Http;

/// <summary>Configuration for <see cref="HttpServer"/>.</summary>
public sealed class HttpServerOptions
{
    /// <summary>Endpoints to listen on. At least one is required.</summary>
    public IList<HttpEndpointOptions> Endpoints { get; } = new List<HttpEndpointOptions>();

    /// <summary>Handlers tried in order for every request; the first that returns <see langword="true"/> wins. No match yields 404.</summary>
    public IList<IHttpHandler> Handlers { get; } = new List<IHttpHandler>();

    /// <summary>Limits and timeouts.</summary>
    public HttpServerLimits Limits { get; } = new();

    /// <summary>Whether to emit a <c>Server</c> response header. Default <see langword="true"/>.</summary>
    public bool AddServerHeader { get; set; } = true;

    /// <summary>Value of the <c>Server</c> header when <see cref="AddServerHeader"/> is set.</summary>
    public string ServerHeaderValue { get; set; } = "Tedd.Quicly";

    /// <summary>Listen backlog per endpoint.</summary>
    public int ListenBacklog { get; set; } = 512;

    /// <summary>How long <see cref="HttpServer.StopAsync"/> / <see cref="HttpServer.DisposeAsync"/> wait for in-flight requests before aborting them. Default 5 s.</summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Invoked with exceptions thrown by handlers (the request gets a 500 or is aborted) and other unexpected connection errors.</summary>
    public Action<Exception>? OnError { get; set; }

    /// <summary>Adds an endpoint.</summary>
    public HttpServerOptions Listen(IPEndPoint endPoint, HttpTlsOptions? tls = null)
    {
        Endpoints.Add(new HttpEndpointOptions(endPoint, tls));
        return this;
    }

    /// <summary>Adds an endpoint on <paramref name="address"/>:<paramref name="port"/>.</summary>
    public HttpServerOptions Listen(IPAddress address, int port, HttpTlsOptions? tls = null)
        => Listen(new IPEndPoint(address, port), tls);

    /// <summary>Appends a handler.</summary>
    public HttpServerOptions Use(IHttpHandler handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        Handlers.Add(handler);
        return this;
    }
}
