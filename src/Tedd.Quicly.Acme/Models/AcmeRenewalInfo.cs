using System.Text.Json.Serialization;

namespace Tedd.Quicly.Acme.Models;

/// <summary>ACME Renewal Information (ARI, RFC 9773 §4.2): the CA's suggested renewal window for a certificate.</summary>
public sealed record AcmeRenewalInfo
{
    /// <summary>The window in which the CA suggests renewing.</summary>
    public required AcmeRenewalWindow SuggestedWindow { get; init; }

    /// <summary>Optional URL explaining why the window was chosen (e.g. a pending revocation).</summary>
    public Uri? ExplanationUrl { get; init; }

    /// <summary>Server-suggested interval before fetching renewal information again (from <c>Retry-After</c>).</summary>
    [JsonIgnore]
    public TimeSpan? RetryAfter { get; init; }

    /// <summary>
    /// Picks the renewal instant per RFC 9773 §4.2: a uniformly random time inside the window; when that time is already in
    /// the past the certificate should be renewed immediately (the returned value is then simply the chosen past instant).
    /// </summary>
    public DateTimeOffset SelectRenewalTime(Random random)
    {
        ArgumentNullException.ThrowIfNull(random);
        DateTimeOffset start = SuggestedWindow.Start;
        DateTimeOffset end = SuggestedWindow.End;
        if (end <= start)
        {
            return start;
        }

        long span = (end - start).Ticks;
        return start.AddTicks(random.NextInt64(span + 1));
    }
}

/// <summary>An ARI suggested window (RFC 9773 §4.2).</summary>
public sealed record AcmeRenewalWindow
{
    /// <summary>Earliest suggested renewal time.</summary>
    public required DateTimeOffset Start { get; init; }

    /// <summary>Latest suggested renewal time.</summary>
    public required DateTimeOffset End { get; init; }
}
