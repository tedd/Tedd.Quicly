using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Primitives;
using static Tedd.Quicly.Core.Tests.Control.ControlTestData;

namespace Tedd.Quicly.Core.Tests.Control;

public class HelloTests
{
    [Fact]
    public void Hello_Round_Trips_Every_Field()
    {
        foreach (int tokenLength in new[] { 0, 1, 63, 64, 4096 })
        {
            byte[] session = Bytes(tokenLength, 0x21);
            byte[] auth = Bytes(4096 - tokenLength, 0x42);
            Hello message = new()
            {
                Flags = (HelloFlags)0xFFFF,
                TableHash = 0xFEDCBA9876543210,
                LastEpoch = uint.MaxValue,
                SessionToken = session,
                AuthToken = auth,
                MaxReceiveDatagram = ushort.MaxValue,
                Caps = (PeerCaps)0x8007,
            };

            byte[] frame = Encode(message);
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(frame, out ControlType type, out ReadOnlySpan<byte> body, out _));
            Assert.Equal(ControlType.Hello, type);
            Assert.True(body.StartsWith("QLCY"u8));
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out Hello parsed));
            Assert.Equal(ControlCodec.ProtocolVersion, parsed.Version);
            Assert.Equal(message.Flags, parsed.Flags);
            Assert.Equal(message.TableHash, parsed.TableHash);
            Assert.Equal(message.LastEpoch, parsed.LastEpoch);
            Assert.Equal(session, parsed.SessionToken.ToArray());
            Assert.Equal(auth, parsed.AuthToken.ToArray());
            Assert.Equal(message.MaxReceiveDatagram, parsed.MaxReceiveDatagram);
            Assert.Equal(message.Caps, parsed.Caps);
        }
    }

    [Fact]
    public void Hello_Default_Writes_Version_One()
    {
        byte[] frame = Encode(new Hello());
        Assert.Equal(1 + 1 + 24 + 2, frame.Length);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(StreamBody(frame), out Hello parsed));
        Assert.Equal(1, parsed.Version);
        Assert.True(parsed.SessionToken.IsEmpty);
        Assert.True(parsed.AuthToken.IsEmpty);
    }

    [Fact]
    public void Hello_Token_Bounds()
    {
        byte[] buffer = new byte[16384];
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new Hello { SessionToken = new byte[4097] }, out _));
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new Hello { AuthToken = new byte[4097] }, out _));
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(HelloBody(Cat(V(4096), Bytes(4096))), out Hello _));
        Assert.Equal(ControlParseStatus.FieldTooLong, ControlCodec.TryParse(HelloBody(Cat(V(4097), Bytes(4097))), out Hello _));
        Assert.Equal(ControlParseStatus.FieldTooLong, ControlCodec.TryParse(HelloBody(authTokenField: Cat(V(4097), Bytes(4097))), out Hello _));
        Assert.Equal(ControlParseStatus.NonMinimalVarint, ControlCodec.TryParse(HelloBody(Cat(VWide(1, 2), [1])), out Hello _));
        Assert.Equal(ControlParseStatus.NonMinimalVarint, ControlCodec.TryParse(HelloBody(authTokenField: Cat(VWide(0, 8))), out Hello _));
    }

    [Fact]
    public void Hello_Magic_And_Version()
    {
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(HelloBody(), out Hello _));
        Assert.Equal(ControlParseStatus.InvalidMagic, ControlCodec.TryParse(HelloBody(magic: 0x59434C52), out Hello bad));
        Assert.Equal(0, bad.Version);
        Assert.Equal(ControlParseStatus.InvalidMagic, ControlCodec.TryParse("QLCX"u8, out Hello _));

        foreach (ushort version in new ushort[] { 0, 2, ushort.MaxValue })
        {
            Assert.Equal(ControlParseStatus.UnsupportedVersion, ControlCodec.TryParse(HelloBody(version: version), out Hello other));
            Assert.Equal(version, other.Version);
            Assert.True(other.SessionToken.IsEmpty);
            // A future version may have any layout after the version field.
            Assert.Equal(ControlParseStatus.UnsupportedVersion, ControlCodec.TryParse(Cat("QLCY"u8.ToArray(), U16(version)), out other));
        }
    }

    [Fact]
    public void Hello_Truncated_At_Every_Length_And_Trailing()
    {
        byte[] body = HelloBody();
        for (int length = 0; length < body.Length; length++)
        {
            ControlParseStatus status = ControlCodec.TryParse(body.AsSpan(0, length), out Hello parsed);
            Assert.Equal(ControlParseStatus.Truncated, status);
            Assert.True(parsed.SessionToken.IsEmpty);
        }

        Assert.Equal(ControlParseStatus.TrailingBytes, ControlCodec.TryParse(Cat(body, [0]), out Hello _));
        // Token length larger than what is left.
        Assert.Equal(ControlParseStatus.Truncated, ControlCodec.TryParse(Cat(body.AsSpan(0, 20).ToArray(), V(100), Bytes(10)), out Hello _));
    }

    public static TheoryData<HelloStatus> Statuses => new()
    {
        HelloStatus.Accepted, HelloStatus.VersionMismatch, HelloStatus.ChannelTableMismatch, HelloStatus.Rejected,
        HelloStatus.ServerFull, HelloStatus.InternalError, HelloStatus.DatagramsRequired, HelloStatus.Informational,
    };

    [Theory]
    [MemberData(nameof(Statuses))]
    public void HelloAck_Round_Trips_Every_Status(HelloStatus status)
    {
        byte[] token = Bytes(69, 0x77);
        byte[] table = Table((2, 1, 2, 128, 1200, "pos"), (64, 4, 0x13, 255, 1200, "ævent"));
        foreach (byte[] tableSection in new[] { Array.Empty<byte>(), table, [0x00] })
        {
            HelloAck message = new()
            {
                Status = status,
                SessionId = ulong.MaxValue,
                Epoch = 17,
                MaxReceiveDatagram = 1452,
                Caps = (PeerCaps)0xFFFF,
                MaxMessageSize = VarInt.MaxValue,
                HeartbeatMicros = 64,
                GraceMicros = 16384,
                SessionToken = token,
                Table = tableSection,
                Reason = "réason"u8,
            };

            byte[] frame = Encode(message);
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(frame, out ControlType type, out ReadOnlySpan<byte> body, out _));
            Assert.Equal(ControlType.HelloAck, type);
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out HelloAck parsed));
            Assert.Equal(status, parsed.Status);
            Assert.Equal(message.SessionId, parsed.SessionId);
            Assert.Equal(message.Epoch, parsed.Epoch);
            Assert.Equal(message.MaxReceiveDatagram, parsed.MaxReceiveDatagram);
            Assert.Equal(message.Caps, parsed.Caps);
            Assert.Equal(message.MaxMessageSize, parsed.MaxMessageSize);
            Assert.Equal(message.HeartbeatMicros, parsed.HeartbeatMicros);
            Assert.Equal(message.GraceMicros, parsed.GraceMicros);
            Assert.Equal(token, parsed.SessionToken.ToArray());
            Assert.Equal(tableSection, parsed.Table.ToArray());
            Assert.Equal(tableSection.Length > 0, parsed.HasTable);
            Assert.Equal("réason"u8.ToArray(), parsed.Reason.ToArray());
        }
    }

    [Theory]
    [InlineData(5)]
    [InlineData(8)]
    [InlineData(0x7F)]
    [InlineData(0xFE)]
    public void HelloAck_Undefined_Status(byte status)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(new byte[256], new HelloAck { Status = (HelloStatus)status }, out _));
        Assert.Equal(ControlParseStatus.InvalidValue, ControlCodec.TryParse(HelloAckBody(status), out HelloAck parsed));
        Assert.Equal(HelloStatus.Accepted, parsed.Status);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(0xFF)]
    public void HelloAck_TableIncluded_Must_Be_Zero_Or_One(byte tableIncluded) =>
        Assert.Equal(ControlParseStatus.InvalidValue, ControlCodec.TryParse(HelloAckBody(tail: Cat([tableIncluded], V(0))), out HelloAck _));

    [Fact]
    public void HelloAck_Writer_Bounds()
    {
        byte[] buffer = new byte[20000];
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new HelloAck { SessionToken = new byte[4097] }, out _));
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new HelloAck { Reason = new byte[513] }, out _));
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new HelloAck { Reason = [0xFF] }, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new HelloAck { MaxMessageSize = VarInt.MaxValue + 1 }, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new HelloAck { HeartbeatMicros = ulong.MaxValue }, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.TryWrite(buffer, new HelloAck { GraceMicros = ulong.MaxValue }, out _));

        // Malformed table sections are refused rather than emitted.
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new HelloAck { Table = [0x05] }, out _));
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new HelloAck { Table = Cat(Table((2, 0, 0, 0, 1, "a")), [0x00]) }, out _));
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new HelloAck { Table = Table((1, 0, 0, 0, 1, "a")) }, out _));
    }

    [Fact]
    public void HelloAck_At_The_Frame_Limit()
    {
        // Fixed 18 + three 1-byte varints + token length 0 + reason prefix 2 + 400 reason bytes + table = 16383-byte body.
        int tableLength = 16383 - 18 - 3 - 1 - 2 - 400;
        byte[] reason = Bytes(400, (byte)'z');
        byte[] table = TableOfLength(tableLength);
        HelloAck atLimit = new() { Status = HelloStatus.Informational, Table = table, Reason = reason };
        byte[] frame = Encode(atLimit);
        Assert.Equal(4 + 16384, frame.Length);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(frame, out _, out ReadOnlySpan<byte> body, out _));
        Assert.Equal(16383, body.Length);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out HelloAck parsed));
        Assert.Equal(table, parsed.Table.ToArray());

        byte[] buffer = new byte[20000];
        byte[] oneMore = TableOfLength(tableLength + 1);
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new HelloAck { Table = oneMore, Reason = reason }, out _));
        Assert.Throws<ArgumentException>(() => ControlCodec.TryWrite(buffer, new HelloAck { Table = TableOfLength(16000, 255), Reason = reason }, out _));
    }

    [Fact]
    public void HelloAck_Truncated_At_Every_Length_And_Trailing()
    {
        byte[] body = HelloAckBody(tail: Cat([0x01], Table((5, 1, 0, 1, 70, "x")), V(1), [(byte)'k']));
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out HelloAck _));
        for (int length = 0; length < body.Length; length++)
        {
            ControlParseStatus status = ControlCodec.TryParse(body.AsSpan(0, length), out HelloAck parsed);
            // Inside the table section the entry-count bound (count ≤ remaining / 6) trips before the bytes run out.
            Assert.True(status is ControlParseStatus.Truncated or ControlParseStatus.CountTooLarge, $"{length}: {status}");
            Assert.True(parsed.Table.IsEmpty);
        }

        Assert.Equal(ControlParseStatus.TrailingBytes, ControlCodec.TryParse(Cat(body, [0]), out HelloAck _));
    }

    [Fact]
    public void HelloAck_Field_Violations()
    {
        Assert.Equal(ControlParseStatus.NonMinimalVarint, ControlCodec.TryParse(Cat([0], U64(1), U32(1), U16(0), U16(0), VWide(5, 2)), out HelloAck _));
        Assert.Equal(ControlParseStatus.FieldTooLong, ControlCodec.TryParse(HelloAckBody(tokenField: Cat(V(4097), Bytes(4097))), out HelloAck _));
        Assert.Equal(ControlParseStatus.FieldTooLong, ControlCodec.TryParse(HelloAckBody(tail: Cat([0], V(513), Bytes(513, (byte)'a'))), out HelloAck _));
        Assert.Equal(ControlParseStatus.InvalidUtf8, ControlCodec.TryParse(HelloAckBody(tail: Cat([0], V(2), [0xC3, 0x28])), out HelloAck _));
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(HelloAckBody(tail: Cat([0], V(512), Bytes(512, (byte)'a'))), out HelloAck _));
    }

    public static TheoryData<byte[], ControlParseStatus> MalformedTables => new()
    {
        { [], ControlParseStatus.Truncated },
        { [0x40], ControlParseStatus.Truncated },
        { VWide(1, 2), ControlParseStatus.NonMinimalVarint },
        { V(3), ControlParseStatus.CountTooLarge },
        { Cat(V(1), V(2), [0, 0], V(1)), ControlParseStatus.CountTooLarge },
        { Cat(V(1), V(1), [0, 0, 0], V(1), V(0)), ControlParseStatus.InvalidChannel },
        { Cat(V(1), V(16384), [0, 0, 0], V(1), V(0)), ControlParseStatus.InvalidChannel },
        { Cat(V(1), VWide(2, 2), [0, 0, 0], V(0)), ControlParseStatus.NonMinimalVarint },
        { Cat(V(1), V(2), [0, 0, 0], VWide(1, 4)), ControlParseStatus.NonMinimalVarint },
        { Cat(V(1), V(2), [0, 0, 0], [0xC0, 0]), ControlParseStatus.Truncated },
        { Cat(V(1), V(2), [0, 0, 0], V(1), V(65), Bytes(65, (byte)'n')), ControlParseStatus.FieldTooLong },
        { Cat(V(1), V(2), [0, 0, 0], V(1), V(2), [0xC3, 0x28]), ControlParseStatus.InvalidUtf8 },
        { Cat(V(1), V(2), [0, 0, 0], V(1), V(5), [(byte)'a']), ControlParseStatus.Truncated },
    };

    [Theory]
    [MemberData(nameof(MalformedTables))]
    public void HelloAck_Table_Section_Is_Structurally_Checked(byte[] table, ControlParseStatus expected)
    {
        Assert.Equal(expected, ControlCodec.TryParse(HelloAckBody(tail: Cat([0x01], table)), out HelloAck parsed));
        Assert.True(parsed.Table.IsEmpty);
    }

    [Fact]
    public void Table_Section_Measure()
    {
        byte[] table = Table((2, 1, 0, 1, 1200, new string('a', 64)), (3, 1, 0, 1, 1200, ""));
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.MeasureTableSection(Cat(table, [1, 2, 3]), out int length));
        Assert.Equal(table.Length, length);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.MeasureTableSection([0x00], out length));
        Assert.Equal(1, length);
    }
}
