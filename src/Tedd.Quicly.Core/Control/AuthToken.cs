using System.Security.Cryptography;

namespace Tedd.Quicly.Core.Control;

/// <summary>Helpers for the application auth token carried in <see cref="Hello.AuthToken"/> (PROTOCOL.md §3.4, §4.1).</summary>
public static class AuthToken
{
    /// <summary>Largest auth token accepted on the wire.</summary>
    public const int MaxLength = ControlCodec.MaxTokenLength;

    /// <summary>
    /// Compares a presented token with the expected one in time independent of their contents
    /// (<see cref="CryptographicOperations.FixedTimeEquals(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>). Tokens of different lengths compare unequal at once:
    /// the length is not treated as secret.
    /// </summary>
    /// <param name="presented">The token received from the peer.</param>
    /// <param name="expected">The token to compare against.</param>
    /// <returns><see langword="true"/> when both have the same length and bytes.</returns>
    public static bool FixedTimeEquals(ReadOnlySpan<byte> presented, ReadOnlySpan<byte> expected) =>
        CryptographicOperations.FixedTimeEquals(presented, expected);
}
