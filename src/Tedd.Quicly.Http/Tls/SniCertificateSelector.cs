using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Http.Tls;

/// <summary>
/// Maps server names to certificate sources. Lookup order: exact host, then wildcard (<c>*.example.com</c> matches one
/// label), then the default source.
/// </summary>
public sealed class SniCertificateSelector : ICertificateSelector
{
    private readonly ConcurrentDictionary<string, ICertificateSource> _hosts = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Creates a selector with an optional fallback source.</summary>
    public SniCertificateSelector(ICertificateSource? defaultSource = null)
    {
        Default = defaultSource;
    }

    /// <summary>Source used when no host matches or the client sent no SNI.</summary>
    public ICertificateSource? Default { get; set; }

    /// <summary>Number of registered host names.</summary>
    public int Count => _hosts.Count;

    /// <summary>Registers (or replaces) the source for <paramref name="hostName"/>; a leading <c>*.</c> makes it a wildcard.</summary>
    public SniCertificateSelector Add(string hostName, ICertificateSource source)
    {
        ArgumentException.ThrowIfNullOrEmpty(hostName);
        ArgumentNullException.ThrowIfNull(source);
        _hosts[hostName] = source;
        return this;
    }

    /// <summary>Registers a fixed certificate for <paramref name="hostName"/>.</summary>
    public SniCertificateSelector Add(string hostName, X509Certificate2 certificate) => Add(hostName, new StaticCertificateSource(certificate));

    /// <summary>Removes a host registration.</summary>
    public bool Remove(string hostName) => _hosts.TryRemove(hostName, out _);

    /// <inheritdoc/>
    public X509Certificate2? SelectCertificate(string? serverName)
    {
        if (!string.IsNullOrEmpty(serverName))
        {
            if (_hosts.TryGetValue(serverName, out var exact))
                return exact.Current ?? Default?.Current;
            int dot = serverName.IndexOf('.', StringComparison.Ordinal);
            if (dot > 0 && dot < serverName.Length - 1 && _hosts.TryGetValue("*" + serverName[dot..], out var wildcard))
                return wildcard.Current ?? Default?.Current;
        }
        return Default?.Current;
    }
}
