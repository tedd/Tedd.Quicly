using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>Platform values of <c>QUIC_ADDRESS_FAMILY_*</c> (the OS socket address family constants).</summary>
public static class QuicAddressFamily
{
    /// <summary><c>AF_UNSPEC</c>.</summary>
    public const int UNSPEC = 0;
    /// <summary><c>AF_INET</c> (2 everywhere).</summary>
    public const int INET = 2;
    /// <summary><c>AF_INET6</c>: 23 on Windows, 30 on the BSD family (macOS, iOS, FreeBSD), 10 elsewhere (Linux).</summary>
    public static readonly int INET6 = OperatingSystem.IsWindows() ? 23 : IsBsdLayout ? 30 : 10;

    /// <summary>True on platforms whose <c>sockaddr</c> starts with a length byte followed by a one-byte family.</summary>
    public static readonly bool IsBsdLayout = OperatingSystem.IsMacOS() || OperatingSystem.IsIOS() || OperatingSystem.IsTvOS() || OperatingSystem.IsFreeBSD();
}

/// <summary>The first two bytes of a <c>sockaddr</c>: a 16-bit family (Windows/Linux) or length + 8-bit family (BSD).</summary>
[StructLayout(LayoutKind.Explicit, Size = 2)]
public struct QuicAddrFamilyAndLen
{
    [FieldOffset(0)] public ushort sin_family;
    [FieldOffset(0)] public byte sin_len;
    [FieldOffset(1)] public byte sin_family_bsd;
}

/// <summary><c>sockaddr_in</c> (the IPv4 view of <see cref="QUIC_ADDR"/>).</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QuicAddrIn
{
    public QuicAddrFamilyAndLen sin_family;
    /// <summary>Port in network byte order.</summary>
    public ushort sin_port;
    public fixed byte sin_addr[4];
}

/// <summary><c>sockaddr_in6</c> (the IPv6 view of <see cref="QUIC_ADDR"/>).</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QuicAddrIn6
{
    public QuicAddrFamilyAndLen sin6_family;
    /// <summary>Port in network byte order.</summary>
    public ushort sin6_port;
    public uint sin6_flowinfo;
    public fixed byte sin6_addr[16];
    public uint sin6_scope_id;
}

/// <summary><c>QUIC_ADDR</c>: a 28-byte union of <c>sockaddr_in</c> / <c>sockaddr_in6</c> (the runtime's bindings call it <c>QuicAddr</c>).</summary>
[StructLayout(LayoutKind.Explicit)]
public unsafe struct QUIC_ADDR
{
    [FieldOffset(0)] public QuicAddrIn Ipv4;
    [FieldOffset(0)] public QuicAddrIn6 Ipv6;
    [FieldOffset(0)] public QuicAddrFamilyAndLen FamilyLen;

    /// <summary>The platform address family value (see <see cref="QuicAddressFamily"/>).</summary>
    public int Family
    {
        readonly get => QuicAddressFamily.IsBsdLayout ? FamilyLen.sin_family_bsd : FamilyLen.sin_family;
        set
        {
            if (QuicAddressFamily.IsBsdLayout)
            {
                FamilyLen.sin_family_bsd = (byte)value;
                FamilyLen.sin_len = (byte)(value == QuicAddressFamily.INET6 ? sizeof(QuicAddrIn6) : sizeof(QuicAddrIn));
            }
            else
            {
                FamilyLen.sin_family = (ushort)value;
            }
        }
    }

    /// <summary>Port in host byte order (the field itself is stored in network order at the same offset for v4 and v6).</summary>
    public ushort Port
    {
        readonly get => BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(Ipv4.sin_port) : Ipv4.sin_port;
        set => Ipv4.sin_port = BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
    }

    /// <summary>True for <c>AF_INET</c>.</summary>
    public readonly bool IsIPv4 => Family == QuicAddressFamily.INET;

    /// <summary>True for <c>AF_INET6</c>.</summary>
    public readonly bool IsIPv6 => Family == QuicAddressFamily.INET6;

    /// <summary>Builds a <see cref="QUIC_ADDR"/> from an <see cref="IPEndPoint"/> without allocating.</summary>
    public static QUIC_ADDR FromIPEndPoint(IPEndPoint endPoint)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        QUIC_ADDR addr = default;
        IPAddress address = endPoint.Address;
        if (address.AddressFamily == AddressFamily.InterNetwork)
        {
            addr.Family = QuicAddressFamily.INET;
            address.TryWriteBytes(new Span<byte>(addr.Ipv4.sin_addr, 4), out _);
        }
        else if (address.AddressFamily == AddressFamily.InterNetworkV6)
        {
            addr.Family = QuicAddressFamily.INET6;
            address.TryWriteBytes(new Span<byte>(addr.Ipv6.sin6_addr, 16), out _);
            addr.Ipv6.sin6_scope_id = (uint)address.ScopeId;
        }
        else
        {
            throw new ArgumentException("Only IPv4 and IPv6 end points are supported.", nameof(endPoint));
        }
        addr.Port = (ushort)endPoint.Port;
        return addr;
    }

    /// <summary>Converts to an <see cref="IPEndPoint"/> (allocates). Returns null for <c>AF_UNSPEC</c> / unknown families.</summary>
    public readonly IPEndPoint? ToIPEndPoint()
    {
        int family = Family;
        if (family == QuicAddressFamily.INET)
        {
            fixed (byte* p = Ipv4.sin_addr)
            {
                return new IPEndPoint(new IPAddress(new ReadOnlySpan<byte>(p, 4)), Port);
            }
        }
        if (family == QuicAddressFamily.INET6)
        {
            fixed (byte* p = Ipv6.sin6_addr)
            {
                return new IPEndPoint(new IPAddress(new ReadOnlySpan<byte>(p, 16), Ipv6.sin6_scope_id), Port);
            }
        }
        return null;
    }
}
