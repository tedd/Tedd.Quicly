using System.Buffers.Binary;
using System.Net;

namespace Tedd.Quicly.Server;

/// <summary>
/// The key a remote address is counted under for the per-address connection limit: IPv4 and IPv4-mapped IPv6 by full
/// address (one key space), IPv6 by a prefix (default /64), the same aggregation <see cref="Core.Control.AuthFailureRateLimiter"/> uses.
/// </summary>
/// <param name="High">The upper 64 bits (0 for IPv4).</param>
/// <param name="Low">The lower 64 bits.</param>
internal readonly record struct AddressKey(ulong High, ulong Low)
{
    private const ulong Ipv4MappedPrefix = 0x0000_FFFF_0000_0000UL;

    /// <summary>Builds the key of <paramref name="endPoint"/>'s address.</summary>
    /// <param name="endPoint">The remote endpoint, or <see langword="null"/> when the transport does not know it.</param>
    /// <param name="ipv6PrefixLength">IPv6 prefix length (1 … 128).</param>
    /// <param name="key">The key.</param>
    /// <returns><see langword="false"/> when there is no address.</returns>
    public static bool TryCreate(IPEndPoint? endPoint, int ipv6PrefixLength, out AddressKey key)
    {
        key = default;
        if (endPoint is null)
        {
            return false;
        }

        Span<byte> bytes = stackalloc byte[16];
        if (!endPoint.Address.TryWriteBytes(bytes, out int written))
        {
            return false;
        }

        if (written == 4)
        {
            key = new AddressKey(0, Ipv4MappedPrefix | BinaryPrimitives.ReadUInt32BigEndian(bytes));
            return true;
        }

        ulong high = BinaryPrimitives.ReadUInt64BigEndian(bytes);
        ulong low = BinaryPrimitives.ReadUInt64BigEndian(bytes.Slice(8));
        if (high == 0 && (low >> 32) == 0xFFFF)
        {
            key = new AddressKey(0, low); // IPv4-mapped: the same key as the plain IPv4 form
            return true;
        }

        ulong highMask = ipv6PrefixLength >= 64 ? ulong.MaxValue : ulong.MaxValue << (64 - ipv6PrefixLength);
        ulong lowMask = ipv6PrefixLength <= 64 ? 0 : ipv6PrefixLength == 128 ? ulong.MaxValue : ulong.MaxValue << (128 - ipv6PrefixLength);
        key = new AddressKey(high & highMask, low & lowMask);
        return true;
    }
}
