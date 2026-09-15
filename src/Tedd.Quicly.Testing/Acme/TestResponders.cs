using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Acme.Challenges;

namespace Tedd.Quicly.Testing.Acme;

/// <summary>An <see cref="IHttp01Responder"/> that really serves the key authorization over HTTP on a loopback port.</summary>
public sealed class Http01TestResponder : IHttp01Responder, IAsyncDisposable
{
    private readonly HttpListener _listener = new();
    private readonly Dictionary<string, string> _tokens = new(StringComparer.Ordinal);
    private readonly object _lock = new();
    private readonly CancellationTokenSource _cts = new();
    private Task? _loop;

    /// <summary>Starts serving on a free loopback port.</summary>
    public Http01TestResponder()
    {
        BaseUri = FreePort.StartOnFreePort(_listener);
        _loop = Task.Run(LoopAsync);
    }

    /// <summary>Base URL of the responder (<c>http://127.0.0.1:{port}/</c>).</summary>
    public Uri BaseUri { get; }

    /// <summary>Tokens published, in order.</summary>
    public List<string> Published { get; } = [];

    /// <summary>Tokens removed, in order.</summary>
    public List<string> Removed { get; } = [];

    /// <summary>Makes <see cref="RemoveAsync"/> throw, to simulate a failing cleanup.</summary>
    public bool FailRemove { get; set; }

    /// <summary>Number of tokens currently served.</summary>
    public int ActiveTokenCount
    {
        get { lock (_lock) { return _tokens.Count; } }
    }

    /// <inheritdoc/>
    public ValueTask PublishAsync(string token, string keyAuthorization, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            _tokens[token] = keyAuthorization;
            Published.Add(token);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask RemoveAsync(string token, CancellationToken cancellationToken)
    {
        if (FailRemove)
        {
            throw new IOException("simulated cleanup failure");
        }

        lock (_lock)
        {
            _tokens.Remove(token);
            Removed.Add(token);
        }

        return ValueTask.CompletedTask;
    }

    private async Task LoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            HttpListenerContext ctx;
            try
            {
                ctx = await _listener.GetContextAsync();
            }
            catch (Exception) when (_cts.IsCancellationRequested)
            {
                return;
            }
            catch (HttpListenerException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            string? token = Http01Challenge.TryGetToken(ctx.Request.Url!.AbsolutePath);
            string? keyAuth = null;
            if (token is not null)
            {
                lock (_lock)
                {
                    _tokens.TryGetValue(token, out keyAuth);
                }
            }

            if (keyAuth is null)
            {
                ctx.Response.StatusCode = 404;
                ctx.Response.Close();
                continue;
            }

            byte[] body = Encoding.ASCII.GetBytes(keyAuth);
            ctx.Response.StatusCode = 200;
            ctx.Response.ContentType = Http01Challenge.ContentType;
            ctx.Response.ContentLength64 = body.Length;
            await ctx.Response.OutputStream.WriteAsync(body);
            ctx.Response.Close();
        }
    }

    /// <summary>Stops serving.</summary>
    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener.Stop();
        _listener.Close();
        if (_loop is not null)
        {
            await _loop;
        }
    }
}

/// <summary>An in-memory DNS TXT store.</summary>
public sealed class InMemoryDns01Provider : IDns01Provider
{
    private readonly object _lock = new();

    /// <summary>TXT values by record name.</summary>
    public Dictionary<string, List<string>> Records { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Every record created, in order.</summary>
    public List<(string Name, string Value)> Created { get; } = [];

    /// <summary>Every record removed, in order.</summary>
    public List<(string Name, string Value)> Removed { get; } = [];

    /// <summary>Makes <see cref="CreateTxtAsync"/> throw, to simulate a failing DNS provider.</summary>
    public bool FailCreate { get; set; }

    /// <inheritdoc/>
    public ValueTask CreateTxtAsync(string name, string value, CancellationToken cancellationToken)
    {
        if (FailCreate)
        {
            throw new InvalidOperationException("simulated DNS provider failure");
        }

        lock (_lock)
        {
            if (!Records.TryGetValue(name, out List<string>? values))
            {
                Records[name] = values = [];
            }

            values.Add(value);
            Created.Add((name, value));
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask RemoveTxtAsync(string name, string value, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (Records.TryGetValue(name, out List<string>? values))
            {
                values.Remove(value);
            }

            Removed.Add((name, value));
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>The TXT values published under <paramref name="name"/>; plug it into <see cref="FakeAcmeServer.DnsTxtLookup"/>.</summary>
    public IReadOnlyList<string> Lookup(string name)
    {
        lock (_lock)
        {
            return Records.TryGetValue(name, out List<string>? values) ? values.ToArray() : [];
        }
    }
}

/// <summary>An in-memory tls-alpn-01 certificate store.</summary>
public sealed class InMemoryTlsAlpn01Responder : ITlsAlpn01Responder
{
    private readonly object _lock = new();

    /// <summary>Published validation certificates (copies) by domain.</summary>
    public Dictionary<string, X509Certificate2> Certificates { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Domains published, in order.</summary>
    public List<string> Published { get; } = [];

    /// <summary>Domains removed, in order.</summary>
    public List<string> Removed { get; } = [];

    /// <inheritdoc/>
    public ValueTask PublishAsync(string domain, X509Certificate2 certificate, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            // Copy: the manager disposes its instance after cleanup.
            Certificates[domain] = X509CertificateLoader.LoadCertificate(certificate.RawData);
            Published.Add(domain);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask RemoveAsync(string domain, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            Certificates.Remove(domain);
            Removed.Add(domain);
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>The certificate published for <paramref name="domain"/>; plug it into <see cref="FakeAcmeServer.TlsAlpnLookup"/>.</summary>
    public X509Certificate2? Lookup(string domain)
    {
        lock (_lock)
        {
            return Certificates.TryGetValue(domain, out X509Certificate2? cert) ? cert : null;
        }
    }
}

/// <summary>Free loopback TCP ports for listeners that cannot bind port 0 themselves, such as <see cref="HttpListener"/>.</summary>
public static class FreePort
{
    /// <summary>Returns a TCP port that was free on 127.0.0.1 a moment ago. Another process may take it first, so prefer binding port 0.</summary>
    public static int Get()
    {
        TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>
    /// Starts an <see cref="HttpListener"/> on a free loopback port, retrying with a new port when another test class
    /// (running in parallel) grabbed the same port between <see cref="Get"/> and <see cref="HttpListener.Start"/>.
    /// </summary>
    public static Uri StartOnFreePort(HttpListener listener)
    {
        for (int attempt = 0; ; attempt++)
        {
            Uri baseUri = new($"http://127.0.0.1:{Get()}/");
            listener.Prefixes.Clear();
            listener.Prefixes.Add(baseUri.ToString());
            try
            {
                listener.Start();
                return baseUri;
            }
            catch (HttpListenerException) when (attempt < 10)
            {
                // Port raced; pick another.
            }
        }
    }
}
