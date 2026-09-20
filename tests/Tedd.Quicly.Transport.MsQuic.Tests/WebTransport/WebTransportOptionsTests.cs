using Tedd.Quicly.Http3;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;

/// <summary>What <see cref="WebTransportOptions"/> accepts, what it refuses and what it copies.</summary>
public class WebTransportOptionsTests
{
    [Fact]
    public void The_defaults_are_valid_and_match_the_protocol_document()
    {
        var options = new WebTransportOptions();
        options.Validate();
        Assert.Equal("/quicly", options.Path);
        Assert.Equal("h3", WebTransportOptions.Alpn);
        Assert.Equal(WebTransportOriginPolicy.Allow, options.OriginPolicy);
        Assert.Equal(32 * 1024, options.MaxCapsuleLength);
    }

    [Fact]
    public void The_settings_are_the_ones_protocol_section_five_specifies()
    {
        Http3Settings settings = new WebTransportOptions().CreateSettings();
        Assert.Equal(0ul, settings.QpackMaxTableCapacity);
        Assert.Equal(0ul, settings.QpackBlockedStreams);
        Assert.Equal(16384ul, settings.MaxFieldSectionSize);
        Assert.Equal(1ul, settings.EnableConnectProtocol);
        Assert.Equal(1ul, settings.H3Datagram);
        Assert.Equal(1ul, settings.EnableWebTransport);
        Assert.Equal(1ul, settings.WebTransportMaxSessions);
    }

    [Fact]
    public void A_path_that_does_not_start_with_a_slash_is_refused()
    {
        var options = new WebTransportOptions { Path = "quicly" };
        ArgumentException error = Assert.Throws<ArgumentException>(options.Validate);
        Assert.Equal("Path", error.ParamName);
    }

    [Fact]
    public void An_empty_path_is_refused_when_it_is_set()
    {
        var options = new WebTransportOptions();
        Assert.Throws<ArgumentException>(() => options.Path = "");
        Assert.Throws<ArgumentNullException>(() => options.Path = null!);
    }

    [Fact]
    public void Require_without_an_allowed_origin_is_refused()
    {
        var options = new WebTransportOptions { OriginPolicy = WebTransportOriginPolicy.Require };
        ArgumentException error = Assert.Throws<ArgumentException>(options.Validate);
        Assert.Equal("AllowedOrigins", error.ParamName);
    }

    [Theory]
    [InlineData("MaxStreams", 0)]
    [InlineData("MaxPendingDatagrams", 0)]
    [InlineData("MaxUnsentDatagrams", 0)]
    [InlineData("MaxDatagramSegments", 0)]
    [InlineData("MaxDatagramSegments", 65)]
    [InlineData("MaxStreamSegments", 0)]
    [InlineData("MaxConcurrentStreamStarts", 0)]
    [InlineData("MaxFieldSectionSize", 8)]
    [InlineData("MaxCapsuleLength", 8)]
    [InlineData("MaxControlFrameLength", 8)]
    [InlineData("MaxExtraRequestStreams", -1)]
    public void Sizes_outside_their_range_are_refused(string property, int value)
    {
        var options = new WebTransportOptions();
        typeof(WebTransportOptions).GetProperty(property)!.SetValue(options, value);
        ArgumentOutOfRangeException error = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(property, error.ParamName);
    }

    [Fact]
    public void More_unsent_than_pending_datagrams_is_refused()
    {
        var options = new WebTransportOptions { MaxPendingDatagrams = 8, MaxUnsentDatagrams = 16 };
        ArgumentException error = Assert.Throws<ArgumentException>(options.Validate);
        Assert.Equal("MaxUnsentDatagrams", error.ParamName);
    }

    [Fact]
    public void Clone_copies_the_origin_list()
    {
        var options = new WebTransportOptions { Path = "/arena", Origin = "https://a.example" };
        options.AllowedOrigins.Add("https://a.example");
        WebTransportOptions copy = options.Clone();
        copy.AllowedOrigins.Add("https://b.example");
        copy.Path = "/other";

        Assert.Single(options.AllowedOrigins);
        Assert.Equal(2, copy.AllowedOrigins.Count);
        Assert.Equal("/arena", options.Path);
        Assert.Equal("https://a.example", copy.Origin);
    }

    [Fact]
    public void The_msquic_options_the_carrier_prepares_use_h3_and_leave_room_for_its_streams()
    {
        var carrier = new WebTransportOptions { MaxStreams = 64 };
        var transport = new MsQuicTransportOptions { MaxStreams = 256, ServerPeerUnidiStreamCount = 0, ServerPeerBidiStreamCount = 1, ClientPeerUnidiStreamCount = 1024 };
        MsQuicTransportOptions prepared = WebTransportConnector.PrepareTransportOptions(transport, carrier);

        Assert.Equal(["h3"], prepared.Alpns);
        Assert.Equal(3, prepared.ServerPeerUnidiStreamCount);
        Assert.Equal(2, prepared.ServerPeerBidiStreamCount);
        Assert.Equal(1027, prepared.ClientPeerUnidiStreamCount);

        // The carrier mirrors the inner stream table, so it is never allowed to be the smaller of the two.
        Assert.Equal(256, carrier.MaxStreams);

        // The source options are untouched.
        Assert.Equal(["quicly/1"], transport.Alpns);
        Assert.Equal(0, transport.ServerPeerUnidiStreamCount);
    }
}
