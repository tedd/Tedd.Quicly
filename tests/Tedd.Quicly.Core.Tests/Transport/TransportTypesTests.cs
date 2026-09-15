using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Transport;

public class TransportTypesTests
{
    [Fact]
    public unsafe void TransportSegment_Matches_QUIC_BUFFER_Layout()
    {
        Assert.Equal(16, sizeof(TransportSegment));
        Assert.Equal(0, (int)Marshal.OffsetOf<TransportSegment>(nameof(TransportSegment.Length)));
        Assert.Equal(8, (int)Marshal.OffsetOf<TransportSegment>(nameof(TransportSegment.Buffer)));
    }

    [Fact]
    public unsafe void TransportSegment_AsSpan_Covers_Buffer()
    {
        byte* p = stackalloc byte[4];
        p[0] = 1; p[1] = 2; p[2] = 3; p[3] = 4;
        var seg = new TransportSegment(p, 4);
        Assert.Equal(4u, seg.Length);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, seg.AsSpan().ToArray());
    }

    [Theory]
    [InlineData(DatagramSendState.Unknown, false, false)]
    [InlineData(DatagramSendState.Sent, false, true)]
    [InlineData(DatagramSendState.LostSuspect, false, true)]
    [InlineData(DatagramSendState.LostDiscarded, true, true)]
    [InlineData(DatagramSendState.Acknowledged, true, true)]
    [InlineData(DatagramSendState.AcknowledgedSpurious, true, true)]
    [InlineData(DatagramSendState.Canceled, true, true)]
    public void DatagramSendState_Final_And_Release_Rules(DatagramSendState state, bool isFinal, bool releases)
    {
        Assert.Equal(isFinal, state.IsFinal());
        Assert.Equal(releases, state.ReleasesPayload());
    }

    [Fact]
    public void TransportStreamId_Validity()
    {
        Assert.False(TransportStreamId.None.IsValid);
        Assert.False(default(TransportStreamId).IsValid);
        Assert.True(new TransportStreamId(3, 1).IsValid);
    }

    [Fact]
    public void ReceiveResult_Factories()
    {
        var c = ReceiveResult.Consumed(10);
        Assert.Equal(10, c.BytesConsumed);
        Assert.False(c.Pending);
        var p = ReceiveResult.PendingAfter(4);
        Assert.Equal(4, p.BytesConsumed);
        Assert.True(p.Pending);
    }

    [Fact]
    public void AlpnBuffer_Holds_255_Bytes()
    {
        var info = new TransportConnectedInfo();
        Span<byte> alpn = info.Alpn;
        Assert.Equal(255, alpn.Length);
        alpn[0] = (byte)'h';
        alpn[1] = (byte)'3';
        info.AlpnLength = 2;
        Assert.Equal("h3", System.Text.Encoding.ASCII.GetString(((ReadOnlySpan<byte>)info.Alpn)[..info.AlpnLength]));
    }
}
