using System.Formats.Asn1;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>What one TLS client handshake against a TLS endpoint saw.</summary>
internal sealed class TlsProbeResult(X509Certificate2? certificate, string? negotiatedAlpn, Exception? failure) : IDisposable
{
    /// <summary>The certificate the server presented, or <see langword="null"/> when the handshake was refused.</summary>
    public X509Certificate2? Certificate { get; } = certificate;

    public string? NegotiatedAlpn { get; } = negotiatedAlpn;

    public Exception? Failure { get; } = failure;

    public bool Succeeded => Certificate is not null;

    public void Dispose() => Certificate?.Dispose();

    public override string ToString() => Succeeded
        ? "presented " + Certificate!.Subject + " (" + Certificate.Thumbprint + "), ALPN [" + NegotiatedAlpn + "]"
        : "refused: " + Failure?.GetType().Name + ": " + Failure?.Message;
}

/// <summary>TLS client handshakes against the provisioner's TLS endpoint, as a normal client or like the CA's tls-alpn-01 check.</summary>
internal static class TlsProbe
{
    public const string AcmeTls1 = "acme-tls/1";

    public const string Http11 = "http/1.1";

    /// <summary>OID of the critical acmeIdentifier extension of a tls-alpn-01 validation certificate (RFC 8737).</summary>
    public const string AcmeIdentifierOid = "1.3.6.1.5.5.7.1.31";

    /// <summary>Handshakes with <paramref name="serverName"/> as SNI offering <paramref name="alpns"/>. A refused handshake is a result, not an exception.</summary>
    public static async Task<TlsProbeResult> HandshakeAsync(IPEndPoint endPoint, string serverName, params string[] alpns)
    {
        using CancellationTokenSource timeout = new(E2eTimeouts.Step);
        using Socket socket = new(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        await socket.ConnectAsync(endPoint, timeout.Token);
        using SslStream ssl = new(new NetworkStream(socket, ownsSocket: false), leaveInnerStreamOpen: false);
        SslClientAuthenticationOptions options = new()
        {
            TargetHost = serverName,
            ApplicationProtocols = [.. alpns.Select(static a => new SslApplicationProtocol(a))],

            // The probe records what was presented; whether to trust it is not its question.
            RemoteCertificateValidationCallback = static (_, _, _, _) => true,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
        };
        try
        {
            await ssl.AuthenticateAsClientAsync(options, timeout.Token);
        }
        catch (Exception e) when (e is AuthenticationException or IOException or SocketException)
        {
            return new TlsProbeResult(null, null, e);
        }
        catch (OperationCanceledException e) when (timeout.IsCancellationRequested)
        {
            throw new TimeoutException("The TLS handshake with " + endPoint + " (SNI " + serverName + ") did not finish within " + E2eTimeouts.Step + ".", e);
        }

        X509Certificate remote = ssl.RemoteCertificate ?? throw new InvalidOperationException("The TLS handshake completed without a server certificate.");
        return new TlsProbeResult(X509CertificateLoader.LoadCertificate(remote.GetRawCertData()), ssl.NegotiatedApplicationProtocol.ToString(), null);
    }

    /// <summary>Whether <paramref name="certificate"/> is a tls-alpn-01 validation certificate (it carries the critical acmeIdentifier extension).</summary>
    public static bool IsChallengeCertificate(X509Certificate2 certificate) => certificate.Extensions[AcmeIdentifierOid] is { Critical: true };

    /// <summary>The SHA-256 digest a tls-alpn-01 certificate carries in its acmeIdentifier extension.</summary>
    public static byte[] AcmeIdentifierDigest(X509Certificate2 certificate)
    {
        X509Extension extension = certificate.Extensions[AcmeIdentifierOid] ?? throw new InvalidOperationException("The certificate has no acmeIdentifier extension.");
        AsnReader reader = new(extension.RawData, AsnEncodingRules.DER);
        byte[] digest = reader.ReadOctetString();
        reader.ThrowIfNotEmpty();
        return digest;
    }

    /// <summary>SHA-256 of a key authorization: what RFC 8737 puts in the acmeIdentifier extension.</summary>
    public static byte[] KeyAuthorizationDigest(string keyAuthorization) => SHA256.HashData(Encoding.ASCII.GetBytes(keyAuthorization));
}
