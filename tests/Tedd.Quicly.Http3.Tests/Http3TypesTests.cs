namespace Tedd.Quicly.Http3.Tests;

public class Http3TypesTests
{
    [Fact]
    public void Frame_Type_Values_Match_Rfc9114()
    {
        Assert.Equal(0x0UL, (ulong)Http3FrameType.Data);
        Assert.Equal(0x1UL, (ulong)Http3FrameType.Headers);
        Assert.Equal(0x3UL, (ulong)Http3FrameType.CancelPush);
        Assert.Equal(0x4UL, (ulong)Http3FrameType.Settings);
        Assert.Equal(0x5UL, (ulong)Http3FrameType.PushPromise);
        Assert.Equal(0x7UL, (ulong)Http3FrameType.GoAway);
        Assert.Equal(0xdUL, (ulong)Http3FrameType.MaxPushId);
        Assert.Equal(0x41UL, (ulong)Http3FrameType.WebTransportStream);
    }

    [Fact]
    public void Stream_Type_Values_Match_Specs()
    {
        Assert.Equal(0x00UL, (ulong)Http3StreamType.Control);
        Assert.Equal(0x01UL, (ulong)Http3StreamType.Push);
        Assert.Equal(0x02UL, (ulong)Http3StreamType.QpackEncoder);
        Assert.Equal(0x03UL, (ulong)Http3StreamType.QpackDecoder);
        Assert.Equal(0x54UL, (ulong)Http3StreamType.WebTransport);
    }

    [Fact]
    public void Error_Code_Values_Match_Rfc9114_Section_8_1()
    {
        Assert.Equal(0x33UL, (ulong)Http3ErrorCode.DatagramError);
        Assert.Equal(0x100UL, (ulong)Http3ErrorCode.NoError);
        Assert.Equal(0x101UL, (ulong)Http3ErrorCode.GeneralProtocolError);
        Assert.Equal(0x102UL, (ulong)Http3ErrorCode.InternalError);
        Assert.Equal(0x103UL, (ulong)Http3ErrorCode.StreamCreationError);
        Assert.Equal(0x104UL, (ulong)Http3ErrorCode.ClosedCriticalStream);
        Assert.Equal(0x105UL, (ulong)Http3ErrorCode.FrameUnexpected);
        Assert.Equal(0x106UL, (ulong)Http3ErrorCode.FrameError);
        Assert.Equal(0x107UL, (ulong)Http3ErrorCode.ExcessiveLoad);
        Assert.Equal(0x108UL, (ulong)Http3ErrorCode.IdError);
        Assert.Equal(0x109UL, (ulong)Http3ErrorCode.SettingsError);
        Assert.Equal(0x10aUL, (ulong)Http3ErrorCode.MissingSettings);
        Assert.Equal(0x10bUL, (ulong)Http3ErrorCode.RequestRejected);
        Assert.Equal(0x10cUL, (ulong)Http3ErrorCode.RequestCancelled);
        Assert.Equal(0x10dUL, (ulong)Http3ErrorCode.RequestIncomplete);
        Assert.Equal(0x10eUL, (ulong)Http3ErrorCode.MessageError);
        Assert.Equal(0x10fUL, (ulong)Http3ErrorCode.ConnectError);
        Assert.Equal(0x110UL, (ulong)Http3ErrorCode.VersionFallback);
        Assert.Equal(0x200UL, (ulong)Http3ErrorCode.QpackDecompressionFailed);
        Assert.Equal(0x201UL, (ulong)Http3ErrorCode.QpackEncoderStreamError);
        Assert.Equal(0x202UL, (ulong)Http3ErrorCode.QpackDecoderStreamError);
        Assert.Equal(0x170d7b68UL, (ulong)Http3ErrorCode.WebTransportSessionGone);
        Assert.Equal(0x3994bd84UL, (ulong)Http3ErrorCode.WebTransportBufferedStreamRejected);
    }

    [Fact]
    public void Setting_Id_Values_Match_Specs()
    {
        Assert.Equal(0x1UL, (ulong)Http3SettingId.QpackMaxTableCapacity);
        Assert.Equal(0x6UL, (ulong)Http3SettingId.MaxFieldSectionSize);
        Assert.Equal(0x7UL, (ulong)Http3SettingId.QpackBlockedStreams);
        Assert.Equal(0x8UL, (ulong)Http3SettingId.EnableConnectProtocol);
        Assert.Equal(0x33UL, (ulong)Http3SettingId.H3Datagram);
        Assert.Equal(0x2b603742UL, (ulong)Http3SettingId.EnableWebTransport);
        Assert.Equal(0xc671706aUL, (ulong)Http3SettingId.WebTransportMaxSessions);
        Assert.Equal(0xc671706bUL, (ulong)Http3SettingId.WebTransportInitialMaxData);
        Assert.Equal(0xc671706cUL, (ulong)Http3SettingId.WebTransportInitialMaxStreamsUni);
        Assert.Equal(0xc671706dUL, (ulong)Http3SettingId.WebTransportInitialMaxStreamsBidi);
    }

    [Theory]
    [InlineData(0x21UL, true)]
    [InlineData(0x40UL, true)]
    [InlineData(0x5fUL, true)]
    [InlineData(0x1f * 1000UL + 0x21, true)]
    [InlineData(0x00UL, false)]
    [InlineData(0x01UL, false)]
    [InlineData(0x20UL, false)]
    [InlineData(0x22UL, false)]
    [InlineData(0x41UL, false)]
    [InlineData(0x54UL, false)]
    public void Grease_Detection(ulong value, bool reserved)
    {
        Assert.Equal(reserved, Http3Grease.IsReserved(value));
        Assert.Equal(reserved, ((Http3FrameType)value).IsReservedGrease());
        Assert.Equal(reserved, ((Http3StreamType)value).IsReservedGrease());
    }

    [Fact]
    public void Grease_Make_Produces_Reserved_Values()
    {
        for (ulong n = 0; n < 100; n++)
        {
            Assert.True(Http3Grease.IsReserved(Http3Grease.Make(n)));
        }
        Assert.Equal(0x21UL, Http3Grease.Make(0));
        Assert.Equal(0x40UL, Http3Grease.Make(1));
    }

    [Fact]
    public void Forbidden_Http2_Frame_Types()
    {
        Assert.True(((Http3FrameType)0x02).IsForbidden());
        Assert.True(((Http3FrameType)0x06).IsForbidden());
        Assert.True(((Http3FrameType)0x08).IsForbidden());
        Assert.True(((Http3FrameType)0x09).IsForbidden());
        Assert.False(Http3FrameType.Data.IsForbidden());
        Assert.False(Http3FrameType.Settings.IsForbidden());
        Assert.False(((Http3FrameType)0x0a).IsForbidden());
    }

    [Fact]
    public void Known_Frame_And_Stream_Types()
    {
        foreach (Http3FrameType t in Enum.GetValues<Http3FrameType>())
        {
            // WEBTRANSPORT_STREAM (0x41) is a stream signal value, not a length-prefixed frame.
            Assert.Equal(t != Http3FrameType.WebTransportStream, t.IsKnown());
        }
        Assert.False(Http3FrameType.WebTransportStream.IsKnown());
        Assert.False(((Http3FrameType)0x02).IsKnown());
        Assert.False(((Http3FrameType)0x21).IsKnown());
        foreach (Http3StreamType t in Enum.GetValues<Http3StreamType>()) Assert.True(t.IsKnown());
        Assert.False(((Http3StreamType)0x04).IsKnown());
        Assert.False(((Http3StreamType)0x21).IsKnown());
    }
}
