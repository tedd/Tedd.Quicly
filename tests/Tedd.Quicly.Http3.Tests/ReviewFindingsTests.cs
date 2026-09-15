namespace Tedd.Quicly.Http3.Tests;

/// <summary>
/// Failing tests written during review; each documents a deviation from docs/PROTOCOL.md §5 that the
/// implementation must fix. Leave them failing until the code is corrected.
/// </summary>
public class ReviewFindingsTests
{
    /// <summary>
    /// docs/PROTOCOL.md §5: "Server SETTINGS: QPACK_MAX_TABLE_CAPACITY=0, QPACK_BLOCKED_STREAMS=0,
    /// MAX_FIELD_SECTION_SIZE=16384, ENABLE_CONNECT_PROTOCOL=1, H3_DATAGRAM=1, ENABLE_WEBTRANSPORT=1,
    /// WT_MAX_SESSIONS=1". <see cref="Http3Settings.CreateWebTransportServerDefaults"/> documents itself as
    /// those defaults but omits MAX_FIELD_SECTION_SIZE, so the server never advertises its 16 KiB header limit.
    /// </summary>
    [Fact]
    public void Server_Defaults_Advertise_Max_Field_Section_Size_16384()
    {
        Http3Settings s = Http3Settings.CreateWebTransportServerDefaults(1);
        Assert.Equal(16384UL, s.MaxFieldSectionSize);

        Span<byte> dst = stackalloc byte[64];
        int n = s.WritePayload(dst);
        Assert.True(n > 0);
        // id 0x06, value 16384 = 4-byte varint 0x80004000
        Assert.Contains("0680004000", TestUtil.ToHex(dst.Slice(0, n)), StringComparison.Ordinal);
    }
}
