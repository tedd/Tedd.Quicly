namespace Tedd.Quicly.Acme;

/// <summary>Tunables for <see cref="AcmeClient"/> polling and retry behaviour.</summary>
public sealed class AcmeClientOptions
{
    /// <summary>Delay between polls of a pending order / authorization when the server sends no <c>Retry-After</c>. Default 2 s.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Upper bound applied to server-provided <c>Retry-After</c> values while polling. Default 30 s.</summary>
    public TimeSpan MaxRetryAfter { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Maximum number of polls before <see cref="Models.AcmeErrorTypes.PollTimeout"/> is raised. Default 60.</summary>
    public int MaxPollAttempts { get; set; } = 60;

    /// <summary>The <c>User-Agent</c> sent with every request (RFC 8555 §6.1 asks clients to identify themselves).</summary>
    public string UserAgent { get; set; } = "Tedd.Quicly.Acme/1.0";

    /// <summary>Clock and timer source used for polling delays; replace in tests to avoid real waits.</summary>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
}

/// <summary>Certificate revocation reason codes (RFC 5280 §5.3.1) accepted by <c>revokeCert</c>.</summary>
public enum AcmeRevocationReason
{
    /// <summary>No reason given.</summary>
    Unspecified = 0,

    /// <summary>The private key was compromised.</summary>
    KeyCompromise = 1,

    /// <summary>The CA key was compromised.</summary>
    CaCompromise = 2,

    /// <summary>The subject's affiliation changed.</summary>
    AffiliationChanged = 3,

    /// <summary>The certificate was replaced.</summary>
    Superseded = 4,

    /// <summary>The service was discontinued.</summary>
    CessationOfOperation = 5,

    /// <summary>Temporarily on hold.</summary>
    CertificateHold = 6,

    /// <summary>Remove from CRL.</summary>
    RemoveFromCrl = 8,

    /// <summary>Privilege withdrawn.</summary>
    PrivilegeWithdrawn = 9,

    /// <summary>Attribute authority compromised.</summary>
    AaCompromise = 10,
}
