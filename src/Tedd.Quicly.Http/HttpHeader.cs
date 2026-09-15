namespace Tedd.Quicly.Http;

/// <summary>A single HTTP header field.</summary>
/// <param name="Name">Field name (case-insensitive for lookups; stored as given).</param>
/// <param name="Value">Field value with surrounding whitespace removed.</param>
public readonly record struct HttpHeader(string Name, string Value);
