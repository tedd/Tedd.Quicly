// MERGE-SHIM: temporary copies of the responder interfaces owned by Tedd.Quicly.Acme, which is being written
// concurrently. Delete this file as soon as Tedd.Quicly.Acme defines IHttp01Responder and ITlsAlpn01Responder
// (identical signatures, same namespace). Nothing else in Tedd.Quicly.Http references this file by name.
using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Acme;

/// <summary>Publishes and removes ACME <c>http-01</c> challenge responses.</summary>
public interface IHttp01Responder
{
    /// <summary>Makes <paramref name="keyAuthorization"/> available at <c>/.well-known/acme-challenge/{token}</c>.</summary>
    ValueTask PublishAsync(string token, string keyAuthorization, CancellationToken ct);

    /// <summary>Removes a previously published challenge response.</summary>
    ValueTask RemoveAsync(string token, CancellationToken ct);
}

/// <summary>Publishes and removes ACME <c>tls-alpn-01</c> challenge certificates.</summary>
public interface ITlsAlpn01Responder
{
    /// <summary>Presents <paramref name="certificate"/> to clients that offer ALPN <c>acme-tls/1</c> with SNI <paramref name="domain"/>.</summary>
    ValueTask PublishAsync(string domain, X509Certificate2 certificate, CancellationToken ct);

    /// <summary>Removes a previously published challenge certificate.</summary>
    ValueTask RemoveAsync(string domain, CancellationToken ct);
}
