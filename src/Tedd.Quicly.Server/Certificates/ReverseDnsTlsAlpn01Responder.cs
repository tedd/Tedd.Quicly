using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Server.Certificates;

/// <summary>
/// Publishes <c>tls-alpn-01</c> certificates into the TLS endpoint's <see cref="TlsAlpn01Responder"/>. For an IP
/// identifier the CA sends the reverse-DNS name as SNI (RFC 8738 §6), so the certificate is published under that name
/// too; the responder itself matches SNI literally.
/// </summary>
internal sealed class ReverseDnsTlsAlpn01Responder(TlsAlpn01Responder inner) : ITlsAlpn01Responder
{
    public async ValueTask PublishAsync(string domain, X509Certificate2 certificate, CancellationToken cancellationToken)
    {
        await inner.PublishAsync(domain, certificate, cancellationToken).ConfigureAwait(false);
        if (IPAddress.TryParse(domain, out IPAddress? address))
        {
            await inner.PublishAsync(CertificateIdentity.ReverseDnsName(address), certificate, cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask RemoveAsync(string domain, CancellationToken cancellationToken)
    {
        await inner.RemoveAsync(domain, cancellationToken).ConfigureAwait(false);
        if (IPAddress.TryParse(domain, out IPAddress? address))
        {
            await inner.RemoveAsync(CertificateIdentity.ReverseDnsName(address), cancellationToken).ConfigureAwait(false);
        }
    }
}

/// <summary>Certificate naming helpers.</summary>
internal static class CertificateIdentity
{
    private const string SubjectAlternativeNameOid = "2.5.29.17";

    /// <summary>The reverse-DNS name of an address: <c>4.3.2.1.in-addr.arpa</c>, or the nibble form under <c>ip6.arpa</c>.</summary>
    public static string ReverseDnsName(IPAddress address)
    {
        byte[] bytes = address.GetAddressBytes();
        StringBuilder sb = new();
        if (address.AddressFamily == AddressFamily.InterNetwork)
        {
            for (int i = bytes.Length - 1; i >= 0; i--)
            {
                sb.Append(bytes[i].ToString(CultureInfo.InvariantCulture)).Append('.');
            }

            return sb.Append("in-addr.arpa").ToString();
        }

        const string hex = "0123456789abcdef";
        for (int i = bytes.Length - 1; i >= 0; i--)
        {
            sb.Append(hex[bytes[i] & 0xF]).Append('.').Append(hex[bytes[i] >> 4]).Append('.');
        }

        return sb.Append("ip6.arpa").ToString();
    }

    /// <summary>True when the certificate's subjectAltName lists every identifier (DNS names case-insensitively, wildcards literally).</summary>
    public static bool Covers(X509Certificate2 certificate, IReadOnlyList<AcmeIdentifier> identifiers)
    {
        X509Extension? extension = certificate.Extensions[SubjectAlternativeNameOid];
        if (extension is null)
        {
            return false;
        }

        X509SubjectAlternativeNameExtension san = new(extension.RawData, extension.Critical);
        List<string> dnsNames = [.. san.EnumerateDnsNames()];
        List<IPAddress> addresses = [.. san.EnumerateIPAddresses()];
        foreach (AcmeIdentifier identifier in identifiers)
        {
            bool found = identifier.IsIp
                ? addresses.Contains(IPAddress.Parse(identifier.Value))
                : dnsNames.Contains(identifier.Value, StringComparer.OrdinalIgnoreCase);
            if (!found)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>A short description for status messages: names, expiry and thumbprint (no secrets).</summary>
    public static string Describe(X509Certificate2 certificate)
    {
        string names = string.Empty;
        X509Extension? extension = certificate.Extensions[SubjectAlternativeNameOid];
        if (extension is not null)
        {
            X509SubjectAlternativeNameExtension san = new(extension.RawData, extension.Critical);
            names = string.Join(", ", san.EnumerateDnsNames().Concat(san.EnumerateIPAddresses().Select(static a => a.ToString())));
        }

        return "[" + (names.Length > 0 ? names : certificate.Subject) + "] expires "
            + certificate.NotAfter.ToUniversalTime().ToString("u", CultureInfo.InvariantCulture)
            + ", thumbprint " + certificate.Thumbprint;
    }
}
