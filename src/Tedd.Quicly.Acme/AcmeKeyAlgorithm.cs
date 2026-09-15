namespace Tedd.Quicly.Acme;

/// <summary>Key / JWS signature algorithm used for ACME account keys and certificate keys.</summary>
public enum AcmeKeyAlgorithm
{
    /// <summary>ECDSA over NIST P-256 with SHA-256 (JWS <c>ES256</c>). Default and recommended.</summary>
    ES256 = 0,

    /// <summary>RSASSA-PKCS1-v1_5 with SHA-256 (JWS <c>RS256</c>), 2048-bit keys or larger.</summary>
    RS256 = 1,
}
