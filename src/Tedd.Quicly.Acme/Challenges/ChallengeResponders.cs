using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Acme.Challenges;

/// <summary>Serves <c>http-01</c> key authorizations at <c>/.well-known/acme-challenge/{token}</c> on port 80.</summary>
public interface IHttp01Responder
{
    /// <summary>Starts serving <paramref name="keyAuthorization"/> (as <c>text/plain</c>) for <paramref name="token"/>.</summary>
    ValueTask PublishAsync(string token, string keyAuthorization, CancellationToken cancellationToken);

    /// <summary>Stops serving the token.</summary>
    ValueTask RemoveAsync(string token, CancellationToken cancellationToken);
}

/// <summary>Creates and removes <c>dns-01</c> TXT records (<c>_acme-challenge.&lt;domain&gt;</c>).</summary>
public interface IDns01Provider
{
    /// <summary>Creates a TXT record <paramref name="name"/> with <paramref name="value"/>.</summary>
    ValueTask CreateTxtAsync(string name, string value, CancellationToken cancellationToken);

    /// <summary>Removes the TXT record.</summary>
    ValueTask RemoveTxtAsync(string name, string value, CancellationToken cancellationToken);
}

/// <summary>Presents <c>tls-alpn-01</c> validation certificates on port 443 for ALPN <c>acme-tls/1</c>.</summary>
public interface ITlsAlpn01Responder
{
    /// <summary>Starts presenting <paramref name="certificate"/> for SNI <paramref name="domain"/>.</summary>
    ValueTask PublishAsync(string domain, X509Certificate2 certificate, CancellationToken cancellationToken);

    /// <summary>Stops presenting the validation certificate for the domain.</summary>
    ValueTask RemoveAsync(string domain, CancellationToken cancellationToken);
}
