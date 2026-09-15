using System.Text.Json.Serialization;

namespace Tedd.Quicly.Acme.Models;

/// <summary>Flattened JWS JSON serialization (RFC 7515 §7.2.2) as required by ACME (RFC 8555 §6.2).</summary>
public sealed record JwsEnvelope
{
    /// <summary>Base64url of the UTF-8 protected header JSON.</summary>
    [JsonPropertyName("protected")]
    public required string Protected { get; init; }

    /// <summary>Base64url of the payload; empty for POST-as-GET.</summary>
    [JsonPropertyName("payload")]
    public required string Payload { get; init; }

    /// <summary>Base64url of the signature.</summary>
    [JsonPropertyName("signature")]
    public required string Signature { get; init; }
}

/// <summary>JWS protected header (RFC 8555 §6.2). Exactly one of <see cref="Jwk"/> / <see cref="Kid"/> is set.</summary>
internal sealed record JwsProtectedHeader
{
    [JsonPropertyName("alg")]
    public required string Alg { get; init; }

    [JsonPropertyName("nonce")]
    public string? Nonce { get; init; }

    [JsonPropertyName("url")]
    public required string Url { get; init; }

    [JsonPropertyName("jwk")]
    public Jwk? Jwk { get; init; }

    [JsonPropertyName("kid")]
    public string? Kid { get; init; }
}
