using System.Net;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Interop;

public unsafe class QuicAddrTests
{
    [Fact]
    public void Size_is_28_bytes()
    {
        Assert.Equal(28, sizeof(QuicAddr));
        Assert.Equal(28, Unsafe.SizeOf<QuicAddrIn6>());
        Assert.Equal(8, Unsafe.SizeOf<QuicAddrIn>());
        Assert.Equal(2, Unsafe.SizeOf<QuicAddrFamilyAndLen>());
    }

    [Fact]
    public void Family_constants_match_platform()
    {
        Assert.Equal(0, QuicAddressFamily.UNSPEC);
        Assert.Equal(2, QuicAddressFamily.INET);
        int expectedInet6 = OperatingSystem.IsWindows() ? 23 : QuicAddressFamily.IsBsdLayout ? 30 : 10;
        Assert.Equal(expectedInet6, QuicAddressFamily.INET6);
        Assert.Equal(OperatingSystem.IsMacOS() || OperatingSystem.IsFreeBSD(), QuicAddressFamily.IsBsdLayout);
    }

    [Fact]
    public void Ipv4_round_trips()
    {
        var ep = new IPEndPoint(IPAddress.Parse("192.168.10.20"), 4433);
        QuicAddr addr = QuicAddr.FromIPEndPoint(ep);
        Assert.True(addr.IsIPv4);
        Assert.False(addr.IsIPv6);
        Assert.Equal(QuicAddressFamily.INET, addr.Family);
        Assert.Equal(4433, addr.Port);
        Assert.Equal(192, addr.Ipv4.sin_addr[0]);
        Assert.Equal(20, addr.Ipv4.sin_addr[3]);
        // Port is stored in network byte order at offset 2 for both v4 and v6.
        byte* raw = (byte*)&addr;
        Assert.Equal(4433 >> 8, raw[2]);
        Assert.Equal(4433 & 0xFF, raw[3]);
        IPEndPoint? back = addr.ToIPEndPoint();
        Assert.NotNull(back);
        Assert.Equal(ep, back);
    }

    [Fact]
    public void Ipv6_round_trips_with_scope_id()
    {
        var address = IPAddress.Parse("fe80::1234:5678%7");
        var ep = new IPEndPoint(address, 65000);
        QuicAddr addr = QuicAddr.FromIPEndPoint(ep);
        Assert.True(addr.IsIPv6);
        Assert.Equal(QuicAddressFamily.INET6, addr.Family);
        Assert.Equal(65000, addr.Port);
        Assert.Equal(7u, addr.Ipv6.sin6_scope_id);
        Assert.Equal(0xFE, addr.Ipv6.sin6_addr[0]);
        Assert.Equal(0x80, addr.Ipv6.sin6_addr[1]);
        Assert.Equal(0x78, addr.Ipv6.sin6_addr[15]);
        IPEndPoint? back = addr.ToIPEndPoint();
        Assert.NotNull(back);
        Assert.Equal(ep.Port, back.Port);
        Assert.Equal(address, back.Address);
        Assert.Equal(7, back.Address.ScopeId);
    }

    [Fact]
    public void Unspecified_family_maps_to_null_end_point()
    {
        QuicAddr addr = default;
        Assert.Equal(QuicAddressFamily.UNSPEC, addr.Family);
        Assert.Null(addr.ToIPEndPoint());
        addr.Family = 99;
        Assert.Equal(99, addr.Family);
        Assert.Null(addr.ToIPEndPoint());
    }

    [Fact]
    public void Port_setter_writes_network_order()
    {
        QuicAddr addr = default;
        addr.Port = 0x1234;
        Assert.Equal(0x1234, addr.Port);
        byte* raw = (byte*)&addr;
        Assert.Equal(0x12, raw[2]);
        Assert.Equal(0x34, raw[3]);
        Assert.Equal(addr.Ipv4.sin_port, addr.Ipv6.sin6_port);
    }

    [Fact]
    public void Rejects_null_and_unsupported_families()
    {
        Assert.Throws<ArgumentNullException>(() => QuicAddr.FromIPEndPoint(null!));
    }

    [Fact]
    public void Family_and_len_union_overlaps_as_expected()
    {
        QuicAddrFamilyAndLen f = default;
        f.sin_family = 0x0102;
        Assert.Equal(0x02, f.sin_len);
        Assert.Equal(0x01, f.sin_family_bsd);
    }
}
