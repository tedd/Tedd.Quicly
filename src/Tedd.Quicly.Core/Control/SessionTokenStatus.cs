namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Outcome of <see cref="SessionTokenAuthority.TryValidate"/> / <see cref="SessionTokenAuthority.TryInspect"/>.
/// Every value other than <see cref="Valid"/> MUST be reported to the peer as <see cref="HelloStatus.Rejected"/>
/// (PROTOCOL.md §3.4: causes are never distinguished); the distinction exists for local counters and logs.
/// </summary>
public enum SessionTokenStatus : byte
{
    /// <summary>Authentic, unexpired and (for <see cref="SessionTokenAuthority.TryValidate"/>) now consumed.</summary>
    Valid = 0,

    /// <summary>Wrong length or unknown format version.</summary>
    Malformed = 1,

    /// <summary>The HMAC does not verify under the current key or the (still accepted) previous key.</summary>
    BadSignature = 2,

    /// <summary>The token's expiry has passed.</summary>
    Expired = 3,

    /// <summary>The token was already used (single-use).</summary>
    Replayed = 4,

    /// <summary>
    /// The replay cache is full of unexpired entries, so single use cannot be guaranteed and the token is refused
    /// (fail closed: the client falls back to a fresh session).
    /// </summary>
    ReplayCacheFull = 5,
}
