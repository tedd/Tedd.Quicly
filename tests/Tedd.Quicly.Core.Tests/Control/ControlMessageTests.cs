using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Primitives;
using static Tedd.Quicly.Core.Tests.Control.ControlTestData;

namespace Tedd.Quicly.Core.Tests.Control;

public class ControlMessageTests
{
    public static TheoryData<ulong> VarIntBoundaries => new() { 0, 63, 64, 16383, 16384, (1UL << 30) - 1, 1UL << 30, VarInt.MaxValue };

    public static TheoryData<ControlCarrier> Carriers => new() { ControlCarrier.Datagram, ControlCarrier.Stream };

    private static ReadOnlySpan<byte> Body(byte[] frame, ControlCarrier carrier) => carrier == ControlCarrier.Datagram ? DatagramBody(frame) : StreamBody(frame);

    [Theory]
    [MemberData(nameof(Carriers))]
    public void Ping_Round_Trips(ControlCarrier carrier)
    {
        foreach (uint t in new uint[] { 0, 1, 0x80000000, uint.MaxValue })
        {
            byte[] frame = Encode(new Ping(t), carrier);
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(Body(frame, carrier), out Ping parsed));
            Assert.Equal(new Ping(t), parsed);
        }
    }

    [Theory]
    [MemberData(nameof(Carriers))]
    public void Pong_Round_Trips(ControlCarrier carrier)
    {
        Pong message = new(0xAABBCCDD, 0, uint.MaxValue);
        byte[] frame = Encode(message, carrier);
        Assert.Equal(carrier == ControlCarrier.Datagram ? 14 : 14, frame.Length);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(Body(frame, carrier), out Pong parsed));
        Assert.Equal(message, parsed);
    }

    [Theory]
    [InlineData(0, ControlParseStatus.Truncated)]
    [InlineData(3, ControlParseStatus.Truncated)]
    [InlineData(5, ControlParseStatus.TrailingBytes)]
    public void Ping_Body_Must_Be_Exactly_Four_Bytes(int length, ControlParseStatus expected)
    {
        Assert.Equal(expected, ControlCodec.TryParse(Bytes(length), out Ping parsed));
        Assert.Equal(default, parsed);
    }

    [Theory]
    [InlineData(0, ControlParseStatus.Truncated)]
    [InlineData(11, ControlParseStatus.Truncated)]
    [InlineData(13, ControlParseStatus.TrailingBytes)]
    public void Pong_Body_Must_Be_Exactly_Twelve_Bytes(int length, ControlParseStatus expected)
    {
        Assert.Equal(expected, ControlCodec.TryParse(Bytes(length), out Pong parsed));
        Assert.Equal(default, parsed);
    }

    [Theory]
    [MemberData(nameof(VarIntBoundaries))]
    public void BulkProgress_Round_Trips_Every_VarInt_Width(ulong value)
    {
        foreach (ControlCarrier carrier in new[] { ControlCarrier.Datagram, ControlCarrier.Stream })
        {
            BulkProgress message = new(value, VarInt.MaxValue - value);
            byte[] frame = Encode(message, carrier);
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(Body(frame, carrier), out BulkProgress parsed));
            Assert.Equal(message, parsed);
        }
    }

    [Fact]
    public void BulkProgress_Malformed()
    {
        Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse([], out BulkProgress _));
        Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse([0x05], out BulkProgress _));
        Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse([0x05, 0x40], out BulkProgress _));
        Assert.Equal(ControlParseStatus.TrailingBytes, ControlCodec.TryParse([0x05, 0x06, 0x00], out BulkProgress _));
        Assert.Equal(ControlParseStatus.NonMinimalVarint, ControlCodec.TryParse([0x40, 0x05, 0x06], out BulkProgress _));
        Assert.Equal(ControlParseStatus.NonMinimalVarint, ControlCodec.TryParse([0x05, 0x80, 0x00, 0x00, 0x06], out BulkProgress progress));
        Assert.Equal(default, progress);
    }

    [Fact]
    public void BulkProgress_Above_VarInt_Max_Throws()
    {
        byte[] buffer = new byte[64];
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new BulkProgress(VarInt.MaxValue + 1, 0), ControlCarrier.Datagram, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new BulkProgress(0, ulong.MaxValue), ControlCarrier.Stream, out _));
    }

    [Fact]
    public void Close_Round_Trips_Codes_And_Reasons()
    {
        string multiByte = new('æ', 256); // 512 bytes of 2-byte UTF-8
        foreach (QuiclyErrorCode code in new[] { QuiclyErrorCode.NoError, QuiclyErrorCode.BulkRejected, (QuiclyErrorCode)uint.MaxValue })
        {
            foreach (byte[] reason in new[] { Array.Empty<byte>(), Utf8Bytes("x"), Bytes(511, (byte)'a'), Bytes(512, (byte)'b'), Utf8Bytes(multiByte), Utf8Bytes("\U0001F600 emoji") })
            {
                byte[] frame = Encode(new Close(code, reason));
                Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(frame, out ControlType type, out ReadOnlySpan<byte> body, out _));
                Assert.Equal(ControlType.Close, type);
                Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out Close parsed));
                Assert.Equal(code, parsed.Code);
                Assert.Equal(reason, parsed.Reason.ToArray());
            }
        }
    }

    [Fact]
    public void Close_Reason_Bounds()
    {
        byte[] buffer = new byte[1024];
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new Close(QuiclyErrorCode.NoError, Bytes(513, (byte)'a')), out _));
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new Close(QuiclyErrorCode.NoError, [0xC3]), out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new Close((QuiclyErrorCode)(1UL << 32), default), out _));

        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(Cat(U32(1), V(512), Bytes(512, (byte)'a')), out Close _));
        Assert.Equal(ControlParseStatus.FieldTooLong, ControlCodec.TryParse(Cat(U32(1), V(513), Bytes(513, (byte)'a')), out Close _));
        Assert.Equal(ControlParseStatus.FieldTooLong, ControlCodec.TryParse(Cat(U32(1), V(VarInt.MaxValue)), out Close _));
    }

    [Theory]
    [InlineData(new byte[] { 0xC3 })]
    [InlineData(new byte[] { 0xFF })]
    [InlineData(new byte[] { 0xC0, 0x80 })]
    [InlineData(new byte[] { 0xED, 0xA0, 0x80 })]
    [InlineData(new byte[] { 0xF4, 0x90, 0x80, 0x80 })]
    [InlineData(new byte[] { 0x61, 0xE2, 0x82 })]
    public void Close_Rejects_Invalid_Utf8(byte[] reason)
    {
        Assert.Equal(ControlParseStatus.InvalidUtf8, ControlCodec.TryParse(Cat(U32(0), V((ulong)reason.Length), reason), out Close parsed));
        Assert.Equal(QuiclyErrorCode.NoError, parsed.Code);
        Assert.True(parsed.Reason.IsEmpty);
    }

    [Fact]
    public void Close_Malformed()
    {
        Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse([1, 0, 0], out Close _));
        Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse(U32(1), out Close _));
        Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse(Cat(U32(1), V(10), Bytes(5)), out Close _));
        Assert.Equal(ControlParseStatus.TrailingBytes, ControlCodec.TryParse(Cat(U32(1), V(1), Bytes(2)), out Close _));
        Assert.Equal(ControlParseStatus.NonMinimalVarint, ControlCodec.TryParse(Cat(U32(1), VWide(1, 2), Bytes(1)), out Close _));
    }

    [Fact]
    public void BulkRequest_Round_Trips_And_Channel_Bounds()
    {
        foreach (ushort channel in new ushort[] { 2, 63, 64, 16383 })
        {
            BulkRequest message = new(VarInt.MaxValue, channel, 0, 16384, VarInt.MaxValue - 5, 5);
            byte[] frame = Encode(message);
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(StreamBody(frame), out BulkRequest parsed));
            Assert.Equal(message, parsed);
        }

        byte[] buffer = new byte[128];
        foreach (ushort channel in new ushort[] { 0, 1, 16384, ushort.MaxValue })
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new BulkRequest(1, channel, 1, 1, 0, 1), out _));
        }

        foreach (ulong channel in new ulong[] { 0, 1, 16384, VarInt.MaxValue })
        {
            Assert.Equal(ControlParseStatus.InvalidChannel, ControlCodec.TryParse(Cat(V(1), V(channel), V(1), V(1), V(0), V(1)), out BulkRequest _));
        }
    }

    [Fact]
    public void BulkRequest_Range_End_Bound()
    {
        byte[] buffer = new byte[128];
        Assert.True(ControlCodec.TryWrite(buffer, new BulkRequest(1, 2, 1, 1, VarInt.MaxValue, 0), out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new BulkRequest(1, 2, 1, 1, VarInt.MaxValue, 1), out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new BulkRequest(1, 2, 1, 1, 1, VarInt.MaxValue), out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new BulkRequest(VarInt.MaxValue + 1, 2, 1, 1, 0, 0), out _));

        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(Cat(V(1), V(2), V(1), V(1), V(1), V(VarInt.MaxValue - 1)), out BulkRequest _));
        Assert.Equal(ControlParseStatus.InvalidValue, ControlCodec.TryParse(Cat(V(1), V(2), V(1), V(1), V(2), V(VarInt.MaxValue - 1)), out BulkRequest parsed));
        Assert.Equal(default, parsed);
    }

    [Fact]
    public void BulkRequest_Truncated_At_Every_Field_And_Trailing()
    {
        byte[] body = Cat(V(1), V(2), V(3), V(4), V(5), V(6));
        for (int length = 0; length < body.Length; length++)
        {
            Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse(body.AsSpan(0, length), out BulkRequest _));
        }

        Assert.Equal(ControlParseStatus.TrailingBytes, ControlCodec.TryParse(Cat(body, [0]), out BulkRequest _));
        Assert.Equal(ControlParseStatus.NonMinimalVarint, ControlCodec.TryParse(Cat(V(1), V(2), V(3), V(4), V(5), VWide(6, 8)), out BulkRequest _));
    }

    [Fact]
    public void BulkCancel_And_BulkReject_Round_Trip()
    {
        foreach (ulong id in new ulong[] { 0, 64, VarInt.MaxValue })
        {
            foreach (QuiclyErrorCode code in new[] { QuiclyErrorCode.BulkCanceled, (QuiclyErrorCode)uint.MaxValue, (QuiclyErrorCode)0x1234 })
            {
                BulkCancel cancel = new(id, code);
                Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(StreamBody(Encode(cancel)), out BulkCancel parsedCancel));
                Assert.Equal(cancel, parsedCancel);
                BulkReject reject = new(id, code);
                Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(StreamBody(Encode(reject)), out BulkReject parsedReject));
                Assert.Equal(reject, parsedReject);
            }
        }
    }

    [Fact]
    public void BulkCancel_And_BulkReject_Bounds_And_Malformed()
    {
        byte[] buffer = new byte[64];
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new BulkCancel(1, (QuiclyErrorCode)(1UL << 32)), out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new BulkReject(1, (QuiclyErrorCode)ulong.MaxValue), out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new BulkCancel(VarInt.MaxValue + 1, 0), out _));

        foreach (byte[] body in new[] { Array.Empty<byte>(), V(1), Cat(V(1), [1, 2, 3]) })
        {
            Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse(body, out BulkCancel cancel));
            Assert.Equal(default, cancel);
            Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse(body, out BulkReject reject));
            Assert.Equal(default, reject);
        }

        Assert.Equal(ControlParseStatus.TrailingBytes, ControlCodec.TryParse(Cat(V(1), U32(2), [0]), out BulkCancel _));
        Assert.Equal(ControlParseStatus.TrailingBytes, ControlCodec.TryParse(Cat(V(1), U32(2), [0]), out BulkReject _));
        Assert.Equal(ControlParseStatus.NonMinimalVarint, ControlCodec.TryParse(Cat(VWide(1, 4), U32(2)), out BulkCancel _));
    }

    [Fact]
    public void KeyRetired_Round_Trips_And_Bounds()
    {
        foreach (ushort channel in new ushort[] { 2, 16383 })
        {
            foreach (ulong key in new ulong[] { 0, 63, 64, VarInt.MaxValue })
            {
                KeyRetired message = new(channel, key);
                Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(StreamBody(Encode(message)), out KeyRetired parsed));
                Assert.Equal(message, parsed);
            }
        }

        byte[] buffer = new byte[64];
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new KeyRetired(1, 0), out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new KeyRetired(16384, 0), out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new KeyRetired(2, VarInt.MaxValue + 1), out _));

        Assert.Equal(ControlParseStatus.InvalidChannel, ControlCodec.TryParse(Cat(V(1), V(0)), out KeyRetired _));
        Assert.Equal(ControlParseStatus.InvalidChannel, ControlCodec.TryParse(Cat(V(16384), V(0)), out KeyRetired _));
        Assert.Equal(ControlParseStatus.NonMinimalVarint, ControlCodec.TryParse(Cat(VWide(2, 2), V(0)), out KeyRetired _));
        Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse(V(2), out KeyRetired _));
        Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse([], out KeyRetired _));
        Assert.Equal(ControlParseStatus.TrailingBytes, ControlCodec.TryParse(Cat(V(2), V(0), [0]), out KeyRetired parsedTrailing));
        Assert.Equal(default, parsedTrailing);
    }

    [Fact]
    public void ChannelTableRequest_Has_Empty_Body()
    {
        byte[] buffer = new byte[8];
        Assert.True(ControlCodec.TryWriteChannelTableRequest(buffer, out int written));
        Assert.Equal(new byte[] { 0x01, 0x16 }, buffer.AsSpan(0, written).ToArray());
        Assert.False(ControlCodec.TryWriteChannelTableRequest(new byte[1], out written));
        Assert.Equal(0, written);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParseChannelTableRequest([]));
        Assert.Equal(ControlParseStatus.TrailingBytes, ControlCodec.TryParseChannelTableRequest([0]));
    }

    [Fact]
    public void Stream_Frame_Uses_Two_Byte_Length_From_64()
    {
        byte[] frame = Encode(new Close(QuiclyErrorCode.NoError, Bytes(100, (byte)'r')));
        Assert.Equal(0x40, frame[0] & 0xC0);
        Assert.Equal(1 + 4 + 2 + 100, ((frame[0] & 0x3F) << 8) | frame[1]); // type, code, 2-byte reason length, reason
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(frame, out _, out _, out int consumed));
        Assert.Equal(frame.Length, consumed);
    }
}
