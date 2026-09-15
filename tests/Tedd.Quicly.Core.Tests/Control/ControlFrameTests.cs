using Tedd.Quicly.Core.Control;
using static Tedd.Quicly.Core.Tests.Control.ControlTestData;

namespace Tedd.Quicly.Core.Tests.Control;

public class ControlFrameTests
{
    [Fact]
    public void Constants_Match_Protocol()
    {
        Assert.Equal(1, ControlCodec.ProtocolVersion);
        Assert.Equal(16384, ControlCodec.MaxFrameLength);
        Assert.Equal(16388, ControlCodec.MaxEncodedStreamFrameLength);
        Assert.Equal(4096, ControlCodec.MaxTokenLength);
        Assert.Equal(512, ControlCodec.MaxReasonLength);
        Assert.Equal(64, ControlCodec.MaxChannelNameLength);
        Assert.Equal(2, ControlCodec.MinChannelId);
        Assert.Equal(16383, ControlCodec.MaxChannelId);
        Assert.Equal(0, ControlCodec.StreamPreamble);
        Assert.Equal("QLCY"u8.ToArray(), ControlCodec.Magic.ToArray());
        Assert.Equal(ControlCodec.MagicValue, BitConverter.ToUInt32("QLCY"u8));
    }

    [Fact]
    public void Enum_Values_Match_Protocol()
    {
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17 }, AllTypes.Select(t => (byte)t).ToArray());
        Assert.Equal(0x11UL, (ulong)QuiclyErrorCode.BulkRejected);
        Assert.Equal(0x10UL, (ulong)QuiclyErrorCode.BulkCanceled);
        Assert.Equal(7UL, (ulong)QuiclyErrorCode.InternalError);
        Assert.Equal(6, (byte)HelloStatus.InternalError);
        Assert.Equal(7, (byte)HelloStatus.DatagramsRequired);
        Assert.Equal(0xFF, (byte)HelloStatus.Informational);
        Assert.Equal(4, (ushort)PeerCaps.Lz4);
        Assert.Equal(2, (ushort)PeerCaps.DatagramSendState);
        Assert.Equal(4, (byte)LatestRejectReason.KeyTableFull);
        Assert.Equal(1, (ushort)HelloFlags.RequestChannelTable);
    }

    [Fact]
    public void Datagram_Empty_And_Header_Only_Are_Truncated()
    {
        Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryReadDatagram([], out _, out _, out int consumed));
        Assert.Equal(0, consumed);
        Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryReadDatagram([0x00], out _, out _, out _));
    }

    [Fact]
    public void Datagram_Returns_Type_Body_And_Whole_Length()
    {
        byte[] frame = [0x00, 0x01, 1, 2, 3, 4];
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadDatagram(frame, out ControlType type, out ReadOnlySpan<byte> body, out int consumed));
        Assert.Equal(ControlType.Ping, type);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, body.ToArray());
        Assert.Equal(frame.Length, consumed);
    }

    [Theory]
    [InlineData(new byte[] { 0x02, 0x01 }, ControlParseStatus.NotControlChannel)]
    [InlineData(new byte[] { 0x01, 0x01 }, ControlParseStatus.NotControlChannel)]
    [InlineData(new byte[] { 0x40 }, ControlParseStatus.NotControlChannel)]
    [InlineData(new byte[] { 0x40, 0x05, 0x01 }, ControlParseStatus.NotControlChannel)]
    [InlineData(new byte[] { 0x40, 0x00, 0x01 }, ControlParseStatus.NonMinimalVarint)]
    [InlineData(new byte[] { 0x80, 0x00, 0x00, 0x00, 0x01 }, ControlParseStatus.NonMinimalVarint)]
    public void Datagram_Rejects_Other_Channels(byte[] frame, ControlParseStatus expected) =>
        Assert.Equal(expected, ControlCodec.TryReadDatagram(frame, out _, out _, out _));

    [Fact]
    public void Datagram_Type_Classification_Covers_Every_Byte()
    {
        for (int t = 0; t < 256; t++)
        {
            ControlParseStatus status = ControlCodec.TryReadDatagram([0x00, (byte)t], out ControlType type, out ReadOnlySpan<byte> body, out _);
            if (t is >= 1 and <= 5)
            {
                Assert.Equal(ControlParseStatus.Ok, status);
                Assert.Equal((ControlType)t, type);
                Assert.True(body.IsEmpty);
            }
            else if (t is >= 0x10 and <= 0x17)
            {
                Assert.Equal(ControlParseStatus.TypeNotAllowed, status);
            }
            else
            {
                Assert.Equal(ControlParseStatus.UnknownType, status);
            }
        }
    }

    [Fact]
    public void Stream_Type_Classification_Covers_Every_Byte()
    {
        for (int t = 0; t < 256; t++)
        {
            ControlParseStatus status = ControlCodec.TryReadStream([0x01, (byte)t], out ControlType type, out _, out int consumed);
            if (t is (>= 1 and <= 5) or (>= 0x10 and <= 0x17))
            {
                Assert.Equal(ControlParseStatus.Ok, status);
                Assert.Equal((ControlType)t, type);
                Assert.Equal(2, consumed);
            }
            else
            {
                Assert.Equal(ControlParseStatus.UnknownType, status);
                Assert.Equal(0, consumed);
            }
        }
    }

    [Theory]
    [InlineData(new byte[] { }, ControlParseStatus.NeedMoreData)]
    [InlineData(new byte[] { 0x40 }, ControlParseStatus.NeedMoreData)]
    [InlineData(new byte[] { 0x80, 0x00, 0x00 }, ControlParseStatus.NeedMoreData)]
    [InlineData(new byte[] { 0xC0, 0, 0, 0, 0, 0, 0 }, ControlParseStatus.NeedMoreData)]
    [InlineData(new byte[] { 0x01 }, ControlParseStatus.NeedMoreData)]
    [InlineData(new byte[] { 0x05, 0x01, 1, 2 }, ControlParseStatus.NeedMoreData)]
    [InlineData(new byte[] { 0x00 }, ControlParseStatus.InvalidFrameLength)]
    [InlineData(new byte[] { 0x00, 0x01 }, ControlParseStatus.InvalidFrameLength)]
    [InlineData(new byte[] { 0x80, 0x00, 0x40, 0x01 }, ControlParseStatus.InvalidFrameLength)]
    [InlineData(new byte[] { 0xBF, 0xFF, 0xFF, 0xFF }, ControlParseStatus.InvalidFrameLength)]
    [InlineData(new byte[] { 0xC0, 0, 0, 0, 0x40, 0, 0, 0 }, ControlParseStatus.InvalidFrameLength)]
    [InlineData(new byte[] { 0x40, 0x02, 0x16, 0x00 }, ControlParseStatus.NonMinimalVarint)]
    [InlineData(new byte[] { 0x80, 0x00, 0x00, 0x02, 0x16, 0x00 }, ControlParseStatus.NonMinimalVarint)]
    [InlineData(new byte[] { 0xC0, 0, 0, 0, 0, 0, 0, 0x02, 0x16 }, ControlParseStatus.NonMinimalVarint)]
    [InlineData(new byte[] { 0x05, 0x06 }, ControlParseStatus.UnknownType)]
    [InlineData(new byte[] { 0x05, 0x00, 1 }, ControlParseStatus.UnknownType)]
    [InlineData(new byte[] { 0x05, 0x18 }, ControlParseStatus.UnknownType)]
    public void Stream_Frame_Header_Statuses(byte[] input, ControlParseStatus expected)
    {
        Assert.Equal(expected, ControlCodec.TryReadStream(input, out ControlType type, out ReadOnlySpan<byte> body, out int consumed));
        Assert.Equal(0, consumed);
        Assert.Equal((ControlType)0, type);
        Assert.True(body.IsEmpty);
    }

    [Fact]
    public void Stream_Length_Boundaries()
    {
        // Length 1: type only.
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream([0x01, 0x16], out ControlType type, out ReadOnlySpan<byte> body, out int consumed));
        Assert.Equal(ControlType.ChannelTableRequest, type);
        Assert.True(body.IsEmpty);
        Assert.Equal(2, consumed);

        // Length 63 (1-byte varint), 64 (2-byte), 16383 (2-byte max), 16384 (4-byte, protocol max).
        foreach ((int length, int prefix) in new[] { (63, 1), (64, 2), (16383, 2), (16384, 4) })
        {
            byte[] frame = Cat(V((ulong)length), [0x12], Bytes(length - 1));
            Assert.Equal(prefix + length, frame.Length);
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(frame, out type, out body, out consumed));
            Assert.Equal(ControlType.Close, type);
            Assert.Equal(length - 1, body.Length);
            Assert.Equal(frame.Length, consumed);

            // One byte short of the declared length.
            Assert.Equal(ControlParseStatus.NeedMoreData, ControlCodec.TryReadStream(frame.AsSpan(0, frame.Length - 1), out _, out _, out consumed));
            Assert.Equal(0, consumed);
        }

        // 16385 is one above the limit.
        Assert.Equal(ControlParseStatus.InvalidFrameLength, ControlCodec.TryReadStream(Cat(V(16385), [0x12], Bytes(16384)), out _, out _, out _));
    }

    [Fact]
    public void Stream_Reads_Back_To_Back_Frames_And_Leaves_Partial_Tail()
    {
        byte[] ping = Encode(new Ping(7), ControlCarrier.Stream);
        byte[] close = Encode(new Close(QuiclyErrorCode.NoError, "bye"u8));
        byte[] stream = Cat(ping, close, close.AsSpan(0, 3).ToArray());

        ReadOnlySpan<byte> rest = stream;
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(rest, out ControlType type, out _, out int consumed));
        Assert.Equal(ControlType.Ping, type);
        Assert.Equal(ping.Length, consumed);
        rest = rest.Slice(consumed);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(rest, out type, out ReadOnlySpan<byte> body, out consumed));
        Assert.Equal(ControlType.Close, type);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out Close parsed));
        Assert.Equal("bye"u8.ToArray(), parsed.Reason.ToArray());
        rest = rest.Slice(consumed);
        Assert.Equal(ControlParseStatus.NeedMoreData, ControlCodec.TryReadStream(rest, out _, out _, out consumed));
        Assert.Equal(0, consumed);
    }

    [Fact]
    public void Datagram_And_Stream_Forms_Of_Ping()
    {
        Assert.Equal(new byte[] { 0x00, 0x01, 0x04, 0x03, 0x02, 0x01 }, Encode(new Ping(0x01020304), ControlCarrier.Datagram));
        Assert.Equal(new byte[] { 0x05, 0x01, 0x04, 0x03, 0x02, 0x01 }, Encode(new Ping(0x01020304), ControlCarrier.Stream));
    }

    [Fact]
    public void Writers_Report_Too_Small_Destination_For_Every_Short_Length()
    {
        foreach (byte[] frame in ValidFrames())
        {
            for (int size = 0; size < frame.Length; size++)
            {
                Assert.False(TryRewrite(frame, new byte[size], out int written), $"size {size} of {frame.Length}");
                Assert.Equal(0, written);
            }

            byte[] exact = new byte[frame.Length];
            Assert.True(TryRewrite(frame, exact, out int exactWritten));
            Assert.Equal(frame.Length, exactWritten);
            Assert.Equal(frame, exact);
        }
    }

    [Fact]
    public void Undefined_Carrier_Throws()
    {
        byte[] buffer = new byte[64];
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new Ping(1), (ControlCarrier)2, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new Pong(1, 2, 3), (ControlCarrier)9, out _));
    }

    /// <summary>Parses a valid frame and writes it again into <paramref name="destination"/> (a batch writer shrinks nothing).</summary>
    private static bool TryRewrite(byte[] frame, byte[] destination, out int written)
    {
        ControlCarrier carrier = frame[0] == 0x00 ? ControlCarrier.Datagram : ControlCarrier.Stream;
        ReadOnlySpan<byte> body;
        ControlType type;
        if (carrier == ControlCarrier.Datagram)
        {
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadDatagram(frame, out type, out body, out _));
        }
        else
        {
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(frame, out type, out body, out _));
        }

        switch (type)
        {
            case ControlType.Ping:
                ControlCodec.TryParse(body, out Ping ping);
                return ControlCodec.TryWrite(destination, in ping, carrier, out written);
            case ControlType.Pong:
                ControlCodec.TryParse(body, out Pong pong);
                return ControlCodec.TryWrite(destination, in pong, carrier, out written);
            case ControlType.BulkProgress:
                ControlCodec.TryParse(body, out BulkProgress progress);
                return ControlCodec.TryWrite(destination, in progress, carrier, out written);
            case ControlType.LatestAck:
            {
                ControlCodec.TryParse(body, out LatestAckBatchReader reader);
                LatestAckBatchWriter writer = new(destination, carrier);
                foreach (LatestAckEntry entry in reader)
                {
                    if (!writer.TryAdd(entry))
                    {
                        written = 0;
                        return false;
                    }
                }

                written = writer.Finish();
                return true;
            }

            case ControlType.LatestReject:
            {
                ControlCodec.TryParse(body, out LatestRejectBatchReader reader);
                LatestRejectBatchWriter writer = new(destination, carrier);
                foreach (LatestRejectEntry entry in reader)
                {
                    if (!writer.TryAdd(entry))
                    {
                        written = 0;
                        return false;
                    }
                }

                written = writer.Finish();
                return true;
            }

            case ControlType.Hello:
                ControlCodec.TryParse(body, out Hello hello);
                return ControlCodec.TryWrite(destination, in hello, out written);
            case ControlType.HelloAck:
                ControlCodec.TryParse(body, out HelloAck ack);
                return ControlCodec.TryWrite(destination, in ack, out written);
            case ControlType.Close:
                ControlCodec.TryParse(body, out Close close);
                return ControlCodec.TryWrite(destination, in close, out written);
            case ControlType.BulkRequest:
                ControlCodec.TryParse(body, out BulkRequest request);
                return ControlCodec.TryWrite(destination, in request, out written);
            case ControlType.BulkCancel:
                ControlCodec.TryParse(body, out BulkCancel cancel);
                return ControlCodec.TryWrite(destination, in cancel, out written);
            case ControlType.BulkReject:
                ControlCodec.TryParse(body, out BulkReject reject);
                return ControlCodec.TryWrite(destination, in reject, out written);
            case ControlType.KeyRetired:
                ControlCodec.TryParse(body, out KeyRetired retired);
                return ControlCodec.TryWrite(destination, in retired, out written);
            default:
                return ControlCodec.TryWriteChannelTableRequest(destination, out written);
        }
    }
}
