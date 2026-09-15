namespace Tedd.Quicly.Acme.Models;

/// <summary>An RFC 7807 problem document as used by ACME (RFC 8555 §6.7).</summary>
public sealed record AcmeProblem
{
    /// <summary>Problem type URN, e.g. <c>urn:ietf:params:acme:error:badNonce</c>.</summary>
    public string? Type { get; init; }

    /// <summary>Human-readable explanation.</summary>
    public string? Detail { get; init; }

    /// <summary>Optional short summary.</summary>
    public string? Title { get; init; }

    /// <summary>HTTP status code the problem was returned with.</summary>
    public int? Status { get; init; }

    /// <summary>The identifier this (sub)problem relates to, when present.</summary>
    public AcmeIdentifier? Identifier { get; init; }

    /// <summary>Per-identifier subproblems (RFC 8555 §6.7.1).</summary>
    public IReadOnlyList<AcmeProblem>? Subproblems { get; init; }

    /// <summary>True when <see cref="Type"/> equals <paramref name="type"/> (ordinal comparison).</summary>
    public bool IsType(string type) => string.Equals(Type, type, StringComparison.Ordinal);

    /// <inheritdoc/>
    public override string ToString()
    {
        string s = Type ?? AcmeErrorTypes.Unknown;
        if (Status is int st)
        {
            s += " (" + st.ToString(System.Globalization.CultureInfo.InvariantCulture) + ")";
        }

        if (!string.IsNullOrEmpty(Detail))
        {
            s += ": " + Detail;
        }

        if (Subproblems is { Count: > 0 } subs)
        {
            s += " [";
            for (int i = 0; i < subs.Count; i++)
            {
                if (i > 0)
                {
                    s += "; ";
                }

                if (subs[i].Identifier is { } id)
                {
                    s += id.Value + ": ";
                }

                s += subs[i].ToString();
            }

            s += "]";
        }

        return s;
    }
}

/// <summary>ACME error type URNs (RFC 8555 §6.7) plus a few client-side types.</summary>
public static class AcmeErrorTypes
{
    /// <summary>Prefix shared by all standard ACME error types.</summary>
    public const string Prefix = "urn:ietf:params:acme:error:";

    /// <summary>The request specified an account that does not exist.</summary>
    public const string AccountDoesNotExist = Prefix + "accountDoesNotExist";

    /// <summary>The certificate is already revoked.</summary>
    public const string AlreadyRevoked = Prefix + "alreadyRevoked";

    /// <summary>The certificate named in <c>replaces</c> has already been replaced (RFC 9773 §5).</summary>
    public const string AlreadyReplaced = Prefix + "alreadyReplaced";

    /// <summary>The CSR is unacceptable.</summary>
    public const string BadCsr = Prefix + "badCSR";

    /// <summary>The nonce is invalid or already used; the client retries once with a fresh nonce.</summary>
    public const string BadNonce = Prefix + "badNonce";

    /// <summary>The public key is unacceptable.</summary>
    public const string BadPublicKey = Prefix + "badPublicKey";

    /// <summary>The revocation reason is not allowed.</summary>
    public const string BadRevocationReason = Prefix + "badRevocationReason";

    /// <summary>The signature algorithm is not supported.</summary>
    public const string BadSignatureAlgorithm = Prefix + "badSignatureAlgorithm";

    /// <summary>CAA records forbid issuance.</summary>
    public const string Caa = Prefix + "caa";

    /// <summary>Multiple subproblems apply.</summary>
    public const string Compound = Prefix + "compound";

    /// <summary>Connecting to the validation target failed.</summary>
    public const string Connection = Prefix + "connection";

    /// <summary>DNS resolution failed.</summary>
    public const string Dns = Prefix + "dns";

    /// <summary>The CA requires External Account Binding.</summary>
    public const string ExternalAccountRequired = Prefix + "externalAccountRequired";

    /// <summary>Response received did not match the challenge.</summary>
    public const string IncorrectResponse = Prefix + "incorrectResponse";

    /// <summary>The contact URL is invalid.</summary>
    public const string InvalidContact = Prefix + "invalidContact";

    /// <summary>The request is malformed.</summary>
    public const string Malformed = Prefix + "malformed";

    /// <summary>The order was not ready for the requested operation.</summary>
    public const string OrderNotReady = Prefix + "orderNotReady";

    /// <summary>Too many requests; honour <c>Retry-After</c>.</summary>
    public const string RateLimited = Prefix + "rateLimited";

    /// <summary>The identifier is rejected by policy.</summary>
    public const string RejectedIdentifier = Prefix + "rejectedIdentifier";

    /// <summary>An internal server error occurred.</summary>
    public const string ServerInternal = Prefix + "serverInternal";

    /// <summary>TLS handshake failed during validation.</summary>
    public const string Tls = Prefix + "tls";

    /// <summary>The client lacks sufficient authorization.</summary>
    public const string Unauthorized = Prefix + "unauthorized";

    /// <summary>The contact URL scheme is unsupported.</summary>
    public const string UnsupportedContact = Prefix + "unsupportedContact";

    /// <summary>The identifier type is unsupported.</summary>
    public const string UnsupportedIdentifier = Prefix + "unsupportedIdentifier";

    /// <summary>The user agreed to a different terms of service version.</summary>
    public const string UserActionRequired = Prefix + "userActionRequired";

    /// <summary>Client-side prefix for errors raised locally (not received from the CA).</summary>
    public const string ClientPrefix = "urn:tedd:quicly:acme:error:";

    /// <summary>The server returned a non-success status without a problem document.</summary>
    public const string Unknown = ClientPrefix + "unknown";

    /// <summary>Polling for an order / authorization exceeded the configured bound.</summary>
    public const string PollTimeout = ClientPrefix + "pollTimeout";

    /// <summary>An authorization or order ended in an invalid / failed state.</summary>
    public const string ValidationFailed = ClientPrefix + "validationFailed";

    /// <summary>No configured responder supports any of the challenges offered for an authorization.</summary>
    public const string NoSupportedChallenge = ClientPrefix + "noSupportedChallenge";

    /// <summary>The response body could not be parsed.</summary>
    public const string InvalidResponse = ClientPrefix + "invalidResponse";

    /// <summary>
    /// A URL to be requested is not HTTPS (RFC 8555 §6.1) and neither loopback nor allowed by
    /// <see cref="AcmeClientOptions.AllowInsecureHttp"/>; the request was not sent.
    /// </summary>
    public const string InsecureUrl = ClientPrefix + "insecureUrl";
}
