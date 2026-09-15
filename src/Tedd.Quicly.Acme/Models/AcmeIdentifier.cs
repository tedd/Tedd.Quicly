namespace Tedd.Quicly.Acme.Models;

/// <summary>An identifier to be certified (RFC 8555 §9.7.7): a DNS name or (RFC 8738) an IP address.</summary>
/// <param name="Type"><c>dns</c> or <c>ip</c>.</param>
/// <param name="Value">The host name or the textual IP address.</param>
public sealed record AcmeIdentifier(string Type, string Value)
{
    /// <summary>Identifier type for DNS names.</summary>
    public const string DnsType = "dns";

    /// <summary>Identifier type for IP addresses (RFC 8738).</summary>
    public const string IpType = "ip";

    /// <summary>Creates a DNS identifier.</summary>
    public static AcmeIdentifier Dns(string hostName) => new(DnsType, hostName);

    /// <summary>Creates an IP identifier.</summary>
    public static AcmeIdentifier Ip(string ipAddress) => new(IpType, ipAddress);

    /// <summary>True when this is a DNS identifier.</summary>
    public bool IsDns => string.Equals(Type, DnsType, StringComparison.Ordinal);

    /// <summary>True when this is an IP identifier.</summary>
    public bool IsIp => string.Equals(Type, IpType, StringComparison.Ordinal);

    /// <inheritdoc/>
    public override string ToString() => Type + ":" + Value;
}
