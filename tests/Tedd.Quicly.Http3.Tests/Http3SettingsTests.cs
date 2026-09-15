namespace Tedd.Quicly.Http3.Tests;

public class Http3SettingsTests
{
    [Fact]
    public void Round_Trip_All_Known_Settings()
    {
        var s = new Http3Settings
        {
            QpackMaxTableCapacity = 0,
            MaxFieldSectionSize = 16384,
            QpackBlockedStreams = 0,
            EnableConnectProtocol = 1,
            H3Datagram = 1,
            EnableWebTransport = 1,
            WebTransportMaxSessions = 64,
        };
        Span<byte> dst = stackalloc byte[128];
        int n = s.WritePayload(dst);
        Assert.Equal(s.GetPayloadLength(), n);
        Assert.Equal(Http3SettingsDecodeStatus.Ok, Http3Settings.Decode(dst.Slice(0, n), out Http3Settings back));
        Assert.Equal(s, back);
        Assert.True(s == back);
        Assert.False(s != back);
        Assert.True(s.Equals((object)back));
        Assert.Equal(s.GetHashCode(), back.GetHashCode());
    }

    [Fact]
    public void Wire_Format_Matches_Rfc()
    {
        var s = new Http3Settings { MaxFieldSectionSize = 300, EnableWebTransport = 1 };
        Span<byte> dst = stackalloc byte[32];
        int n = s.WritePayload(dst);
        // id 0x06 (1 byte), value 300 (2 bytes: 0x412c), id 0x2b603742 (4 bytes: 0xab603742), value 1
        Assert.Equal("06412cab60374201", TestUtil.ToHex(dst.Slice(0, n)));
    }

    [Fact]
    public void WriteFrame_Includes_Header()
    {
        var s = Http3Settings.CreateWebTransportServerDefaults(16);
        Span<byte> dst = stackalloc byte[128];
        int n = s.WriteFrame(dst);
        Assert.Equal(s.GetFrameLength(), n);
        Assert.Equal(0x04, dst[0]);
        Assert.Equal(s.GetPayloadLength(), dst[1]);
        Assert.True(Http3FrameReader.TryReadFrame(dst.Slice(0, n), out ulong type, out ReadOnlySpan<byte> payload, out _));
        Assert.Equal((ulong)Http3FrameType.Settings, type);
        Assert.Equal(Http3SettingsDecodeStatus.Ok, Http3Settings.Decode(payload, out Http3Settings back));
        Assert.Equal(0UL, back.QpackMaxTableCapacity);
        Assert.Equal(0UL, back.QpackBlockedStreams);
        Assert.Equal(1UL, back.EnableConnectProtocol);
        Assert.Equal(1UL, back.H3Datagram);
        Assert.Equal(1UL, back.EnableWebTransport);
        Assert.Equal(16UL, back.WebTransportMaxSessions);
        Assert.Null(back.MaxFieldSectionSize);
    }

    [Fact]
    public void Empty_Settings_Encode_To_Empty_Payload()
    {
        var s = default(Http3Settings);
        Assert.Equal(0, s.GetPayloadLength());
        Assert.Equal(0, s.WritePayload(Span<byte>.Empty));
        Assert.Equal(2, s.GetFrameLength());
        Assert.Equal(Http3SettingsDecodeStatus.Ok, Http3Settings.Decode(ReadOnlySpan<byte>.Empty, out Http3Settings back));
        Assert.Equal(s, back);
    }

    [Fact]
    public void Write_Fails_When_Too_Small()
    {
        var s = Http3Settings.CreateWebTransportServerDefaults(1);
        Assert.Equal(-1, s.WritePayload(stackalloc byte[3]));
        Assert.Equal(-1, s.WritePayload(stackalloc byte[s.GetPayloadLength() - 1]));
        Assert.Equal(-1, s.WriteFrame(stackalloc byte[1]));
        Assert.Equal(-1, s.WriteFrame(stackalloc byte[s.GetFrameLength() - 1]));
        // Each identifier individually: destination too small for id, and for value.
        Assert.Equal(-1, new Http3Settings { QpackMaxTableCapacity = 0 }.WritePayload(stackalloc byte[1]));
        Assert.Equal(-1, new Http3Settings { MaxFieldSectionSize = 0 }.WritePayload(stackalloc byte[1]));
        Assert.Equal(-1, new Http3Settings { QpackBlockedStreams = 0 }.WritePayload(stackalloc byte[1]));
        Assert.Equal(-1, new Http3Settings { EnableConnectProtocol = 0 }.WritePayload(stackalloc byte[1]));
        Assert.Equal(-1, new Http3Settings { H3Datagram = 0 }.WritePayload(stackalloc byte[1]));
        Assert.Equal(-1, new Http3Settings { EnableWebTransport = 0 }.WritePayload(stackalloc byte[1]));
        Assert.Equal(-1, new Http3Settings { WebTransportMaxSessions = 0 }.WritePayload(stackalloc byte[4]));
        Assert.Equal(-1, new Http3Settings { WebTransportMaxSessions = 0 }.WritePayload(stackalloc byte[1]));
    }

    [Fact]
    public void Unknown_And_Grease_Identifiers_Are_Ignored()
    {
        // unknown id 0x09 = 5, grease id 0x21 = 7, then known MAX_FIELD_SECTION_SIZE = 100 (2-byte varint 0x4064)
        byte[] payload = TestUtil.Hex("0905 2107 064064");
        Assert.Equal(Http3SettingsDecodeStatus.Ok, Http3Settings.Decode(payload, out Http3Settings s));
        Assert.Equal(100UL, s.MaxFieldSectionSize);
        Assert.Null(s.QpackMaxTableCapacity);

        // Unknown identifiers on every side of the known ones: 0x0a, 0x32, 0x34, 0x2b603741, 0x2b603743, 0xc6717069, 0xc671706b, 2^62-1.
        byte[] more = TestUtil.Hex("0a01 3201 3401 ab60374101 ab60374301 c0000000c671706901 c0000000c671706b01 ffffffffffffffff01");
        Assert.Equal(Http3SettingsDecodeStatus.Ok, Http3Settings.Decode(more, out s));
        Assert.Equal(default, s);
    }

    [Theory]
    [InlineData("0000")]
    [InlineData("0200")]
    [InlineData("0300")]
    [InlineData("0400")]
    [InlineData("0500")]
    public void Reserved_Http2_Identifiers_Are_Errors(string hex)
    {
        Assert.Equal(Http3SettingsDecodeStatus.ReservedSetting, Http3Settings.Decode(TestUtil.Hex(hex), out _));
    }

    [Theory]
    [InlineData("0100 0100")]
    [InlineData("0600 0600")]
    [InlineData("0700 0700")]
    [InlineData("0801 0801")]
    [InlineData("3301 3301")]
    [InlineData("ab60374201 ab60374201")]
    [InlineData("c0000000c671706a01 c0000000c671706a01")]
    public void Duplicate_Known_Identifiers_Are_Errors(string hex)
    {
        Assert.Equal(Http3SettingsDecodeStatus.DuplicateSetting, Http3Settings.Decode(TestUtil.Hex(hex), out _));
    }

    [Theory]
    [InlineData("0802")]
    [InlineData("3302")]
    [InlineData("ab60374202")]
    public void Boolean_Settings_Reject_Values_Above_One(string hex)
    {
        Assert.Equal(Http3SettingsDecodeStatus.InvalidValue, Http3Settings.Decode(TestUtil.Hex(hex), out _));
    }

    [Theory]
    [InlineData("06")]
    [InlineData("0641")]
    [InlineData("80")]
    public void Truncated_Payload_Is_Malformed(string hex)
    {
        Assert.Equal(Http3SettingsDecodeStatus.Malformed, Http3Settings.Decode(TestUtil.Hex(hex), out _));
    }

    [Fact]
    public void Inequality_Detects_Each_Field()
    {
        var a = Http3Settings.CreateWebTransportServerDefaults(1);
        Assert.NotEqual(a, a with { QpackMaxTableCapacity = 1 });
        Assert.NotEqual(a, a with { MaxFieldSectionSize = 1 });
        Assert.NotEqual(a, a with { QpackBlockedStreams = 1 });
        Assert.NotEqual(a, a with { EnableConnectProtocol = 0 });
        Assert.NotEqual(a, a with { H3Datagram = 0 });
        Assert.NotEqual(a, a with { EnableWebTransport = 0 });
        Assert.NotEqual(a, a with { WebTransportMaxSessions = 2 });
        Assert.False(a.Equals("not settings"));
    }
}
