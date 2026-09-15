using System.Text.Json.Serialization;

namespace Tedd.Quicly.Acme.Models;

/// <summary>JSON Web Key (RFC 7517) public-key representation of an ACME account key.</summary>
public sealed record Jwk
{
    /// <summary>Key type: <c>EC</c> or <c>RSA</c>.</summary>
    [JsonPropertyOrder(0)]
    public required string Kty { get; init; }

    /// <summary>EC curve name (<c>P-256</c>); <see langword="null"/> for RSA keys.</summary>
    [JsonPropertyOrder(1)]
    public string? Crv { get; init; }

    /// <summary>EC public point X coordinate (base64url); <see langword="null"/> for RSA keys.</summary>
    [JsonPropertyOrder(2)]
    public string? X { get; init; }

    /// <summary>EC public point Y coordinate (base64url); <see langword="null"/> for RSA keys.</summary>
    [JsonPropertyOrder(3)]
    public string? Y { get; init; }

    /// <summary>RSA public exponent (base64url); <see langword="null"/> for EC keys.</summary>
    [JsonPropertyOrder(4)]
    public string? E { get; init; }

    /// <summary>RSA modulus (base64url); <see langword="null"/> for EC keys.</summary>
    [JsonPropertyOrder(5)]
    public string? N { get; init; }
}
