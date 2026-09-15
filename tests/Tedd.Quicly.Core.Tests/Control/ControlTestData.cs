using System.Text;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Tests.Control;

/// <summary>Frame builders, encoders and a parse-then-re-encode oracle shared by the control tests.</summary>
internal static class ControlTestData
{
    public static readonly ControlType[] AllTypes =
    [
        ControlType.Ping, ControlType.Pong, ControlType.LatestAck, ControlType.LatestReject, ControlType.BulkProgress,
        ControlType.Hello, ControlType.HelloAck, ControlType.Close, ControlType.BulkRequest, ControlType.BulkCancel,
        ControlType.BulkReject, ControlType.ChannelTableRequest, ControlType.KeyRetired,
    ];

    public static readonly ControlType[] DatagramTypes =
    [
        ControlType.Ping, ControlType.Pong, ControlType.LatestAck, ControlType.LatestReject, ControlType.BulkProgress,
    ];

    /// <summary>Minimal varint encoding.</summary>
    public static byte[] V(ulong value)
    {
        byte[] buffer = new byte[8];
        int n = VarInt.Write(buffer, value);
        return buffer.AsSpan(0, n).ToArray();
    }

    /// <summary>Encodes <paramref name="value"/> with a <paramref name="length"/>-byte (2, 4 or 8) varint, minimal or not.</summary>
    public static byte[] VWide(ulong value, int length)
    {
        ulong prefix = length switch { 2 => 1UL, 4 => 2UL, 8 => 3UL, _ => throw new ArgumentOutOfRangeException(nameof(length)) };
        ulong encoded = (prefix << (8 * length - 2)) | value;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++)
        {
            bytes[i] = (byte)(encoded >> (8 * (length - 1 - i)));
        }

        return bytes;
    }

    public static byte[] U16(ushort value) => BitConverter.GetBytes(value);

    public static byte[] U32(uint value) => BitConverter.GetBytes(value);

    public static byte[] U64(ulong value) => BitConverter.GetBytes(value);

    public static byte[] Cat(params byte[][] parts)
    {
        List<byte> all = [];
        foreach (byte[] part in parts)
        {
            all.AddRange(part);
        }

        return [.. all];
    }

    public static byte[] Bytes(int length, byte value = 0x5A)
    {
        byte[] bytes = new byte[length];
        Array.Fill(bytes, value);
        return bytes;
    }

    public static byte[] Utf8Bytes(string text) => Encoding.UTF8.GetBytes(text);

    public static byte[] StreamFrame(ControlType type, byte[] body) => Cat(V((ulong)body.Length + 1), [(byte)type], body);

    public static byte[] DatagramFrame(ControlType type, byte[] body) => Cat([0x00, (byte)type], body);

    public static byte[] StreamBody(byte[] frame)
    {
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(frame, out _, out ReadOnlySpan<byte> body, out int consumed));
        Assert.Equal(frame.Length, consumed);
        return body.ToArray();
    }

    public static byte[] DatagramBody(byte[] frame)
    {
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadDatagram(frame, out _, out ReadOnlySpan<byte> body, out _));
        return body.ToArray();
    }

    /// <summary>Hello body as the v1 layout, with every field controllable (used to craft malformed input).</summary>
    public static byte[] HelloBody(byte[]? sessionTokenField = null, byte[]? authTokenField = null, ushort version = 1, uint magic = 0x59434C51)
    {
        return Cat(U32(magic), U16(version), U16(0x0001), U64(0x0123456789ABCDEF), U32(7),
            sessionTokenField ?? Cat(V(3), [1, 2, 3]), authTokenField ?? Cat(V(2), [9, 9]), U16(1200), U16(7));
    }

    /// <summary>HelloAck body as the v1 layout with a controllable table/reason tail.</summary>
    public static byte[] HelloAckBody(byte status = 0, byte[]? tokenField = null, byte[]? tail = null)
    {
        return Cat([status], U64(42), U32(3), U16(1400), U16(5), V(65536), V(1_000_000), V(30_000_000),
            tokenField ?? Cat(V(4), [4, 3, 2, 1]), tail ?? Cat([0x00], V(2), Utf8Bytes("ok")));
    }

    public static byte[] Table(params (ushort Id, byte Mode, byte Flags, byte Priority, ulong MaxMessageSize, string Name)[] channels)
    {
        List<byte> bytes = [.. V((ulong)channels.Length)];
        foreach ((ushort id, byte mode, byte flags, byte priority, ulong maxMessageSize, _) in channels)
        {
            bytes.AddRange(V(id));
            bytes.Add(mode);
            bytes.Add(flags);
            bytes.Add(priority);
            bytes.AddRange(V(maxMessageSize));
        }

        foreach ((_, _, _, _, _, string name) in channels)
        {
            byte[] utf8 = Utf8Bytes(name);
            bytes.AddRange(V((ulong)utf8.Length));
            bytes.AddRange(utf8);
        }

        return [.. bytes];
    }

    /// <summary>A structurally valid table section of exactly <paramref name="length"/> bytes.</summary>
    public static byte[] TableOfLength(int length, int count = 240)
    {
        int baseLength = V((ulong)count).Length + count;
        for (int i = 0; i < count; i++)
        {
            baseLength += V((ulong)(i + 2)).Length + 3 + V(1200).Length;
        }

        int nameBytes = length - baseLength;
        if (nameBytes < 0 || nameBytes > count * 63)
        {
            throw new ArgumentOutOfRangeException(nameof(length));
        }

        var channels = new (ushort, byte, byte, byte, ulong, string)[count];
        for (int i = 0; i < count; i++)
        {
            int n = Math.Min(63, nameBytes);
            nameBytes -= n;
            channels[i] = ((ushort)(i + 2), 2, 0, 128, 1200, new string('n', n));
        }

        byte[] table = Table(channels);
        Assert.Equal(length, table.Length);
        return table;
    }

    public static byte[] Encode(in Ping message, ControlCarrier carrier) => Finish(ControlCodec.TryWrite(Buffer(out byte[] b), in message, carrier, out int n), b, n);

    public static byte[] Encode(in Pong message, ControlCarrier carrier) => Finish(ControlCodec.TryWrite(Buffer(out byte[] b), in message, carrier, out int n), b, n);

    public static byte[] Encode(in BulkProgress message, ControlCarrier carrier) => Finish(ControlCodec.TryWrite(Buffer(out byte[] b), in message, carrier, out int n), b, n);

    public static byte[] Encode(in Hello message) => Finish(ControlCodec.TryWrite(Buffer(out byte[] b), in message, out int n), b, n);

    public static byte[] Encode(in HelloAck message) => Finish(ControlCodec.TryWrite(Buffer(out byte[] b), in message, out int n), b, n);

    public static byte[] Encode(in Close message) => Finish(ControlCodec.TryWrite(Buffer(out byte[] b), in message, out int n), b, n);

    public static byte[] Encode(in BulkRequest message) => Finish(ControlCodec.TryWrite(Buffer(out byte[] b), in message, out int n), b, n);

    public static byte[] Encode(in BulkCancel message) => Finish(ControlCodec.TryWrite(Buffer(out byte[] b), in message, out int n), b, n);

    public static byte[] Encode(in BulkReject message) => Finish(ControlCodec.TryWrite(Buffer(out byte[] b), in message, out int n), b, n);

    public static byte[] Encode(in KeyRetired message) => Finish(ControlCodec.TryWrite(Buffer(out byte[] b), in message, out int n), b, n);

    public static byte[] EncodeAcks(ControlCarrier carrier, params LatestAckEntry[] entries)
    {
        byte[] buffer = new byte[ControlCodec.MaxEncodedStreamFrameLength + 64];
        LatestAckBatchWriter writer = new(buffer, carrier);
        foreach (LatestAckEntry entry in entries)
        {
            Assert.True(writer.TryAdd(entry));
        }

        return buffer.AsSpan(0, writer.Finish()).ToArray();
    }

    public static byte[] EncodeRejects(ControlCarrier carrier, params LatestRejectEntry[] entries)
    {
        byte[] buffer = new byte[ControlCodec.MaxEncodedStreamFrameLength + 64];
        LatestRejectBatchWriter writer = new(buffer, carrier);
        foreach (LatestRejectEntry entry in entries)
        {
            Assert.True(writer.TryAdd(entry));
        }

        return buffer.AsSpan(0, writer.Finish()).ToArray();
    }

    /// <summary>A valid frame of every message type and shape, used as fuzz seeds.</summary>
    public static List<byte[]> ValidFrames()
    {
        byte[] token = Bytes(69, 0x11);
        byte[] table = Table((2, 1, 0x02, 128, 1200, "state"), (70, 2, 0x01, 200, 65536, "events"), (16383, 5, 0, 0, 16 << 20, "bulk"));
        List<byte[]> frames = [];
        foreach (ControlCarrier carrier in new[] { ControlCarrier.Datagram, ControlCarrier.Stream })
        {
            frames.Add(Encode(new Ping(0x01020304), carrier));
            frames.Add(Encode(new Pong(1, 70000, 0xFFFFFFFF), carrier));
            frames.Add(Encode(new BulkProgress(64, 16384), carrier));
            frames.Add(EncodeAcks(carrier, new LatestAckEntry(2, 0, 1), new LatestAckEntry(64, 64, 2), new LatestAckEntry(16383, VarInt.MaxValue, uint.MaxValue)));
            frames.Add(EncodeRejects(carrier, new LatestRejectEntry(3, 16384, 5, LatestRejectReason.RingFull), new LatestRejectEntry(100, 1, 6, LatestRejectReason.KeyTableFull)));
        }

        frames.Add(Encode(new Hello { Flags = HelloFlags.RequestChannelTable, TableHash = 0xDEADBEEF, LastEpoch = 3, SessionToken = token, AuthToken = Utf8Bytes("secret"), MaxReceiveDatagram = 1200, Caps = PeerCaps.Datagrams | PeerCaps.Lz4 }));
        frames.Add(Encode(new Hello()));
        frames.Add(Encode(new HelloAck { Status = HelloStatus.Accepted, SessionId = 99, Epoch = 2, MaxReceiveDatagram = 1400, Caps = PeerCaps.Datagrams, MaxMessageSize = 65536, HeartbeatMicros = 1_000_000, GraceMicros = 30_000_000, SessionToken = token, Table = table, Reason = Utf8Bytes("welcome") }));
        frames.Add(Encode(new HelloAck { Status = HelloStatus.Rejected, Reason = Utf8Bytes("nope") }));
        frames.Add(Encode(new Close(QuiclyErrorCode.ProtocolViolation, Utf8Bytes("bad frame æøå"))));
        frames.Add(Encode(new BulkRequest(1, 5, 77, 3, 1 << 20, 4096)));
        frames.Add(Encode(new BulkCancel(8, QuiclyErrorCode.BulkCanceled)));
        frames.Add(Encode(new BulkReject(9, QuiclyErrorCode.BulkRejected)));
        frames.Add(Encode(new KeyRetired(300, 123456789)));
        Assert.True(ControlCodec.TryWriteChannelTableRequest(Buffer(out byte[] ctr), out int ctrLength));
        frames.Add(ctr.AsSpan(0, ctrLength).ToArray());
        return frames;
    }

    /// <summary>
    /// Parses <paramref name="body"/> as <paramref name="type"/>; on success re-encodes the message in
    /// <paramref name="carrier"/> form. Returns null when parsing failed and an empty array for an empty ack batch
    /// (which the writers never produce).
    /// </summary>
    public static byte[]? Reencode(ControlType type, ReadOnlySpan<byte> body, ControlCarrier carrier, out ControlParseStatus status)
    {
        byte[] buffer = new byte[ControlCodec.MaxEncodedStreamFrameLength + 64];
        int written;
        switch (type)
        {
            case ControlType.Ping:
                status = ControlCodec.TryParse(body, out Ping ping);
                if (status != ControlParseStatus.Ok) return null;
                Assert.True(ControlCodec.TryWrite(buffer, in ping, carrier, out written));
                break;
            case ControlType.Pong:
                status = ControlCodec.TryParse(body, out Pong pong);
                if (status != ControlParseStatus.Ok) return null;
                Assert.True(ControlCodec.TryWrite(buffer, in pong, carrier, out written));
                break;
            case ControlType.BulkProgress:
                status = ControlCodec.TryParse(body, out BulkProgress progress);
                if (status != ControlParseStatus.Ok) return null;
                Assert.True(ControlCodec.TryWrite(buffer, in progress, carrier, out written));
                break;
            case ControlType.LatestAck:
            {
                status = ControlCodec.TryParse(body, out LatestAckBatchReader acks);
                if (status != ControlParseStatus.Ok) return null;
                if (acks.Count == 0) return [];
                LatestAckBatchWriter writer = new(buffer, carrier);
                foreach (LatestAckEntry entry in acks)
                {
                    Assert.True(writer.TryAdd(entry));
                }

                written = writer.Finish();
                break;
            }

            case ControlType.LatestReject:
            {
                status = ControlCodec.TryParse(body, out LatestRejectBatchReader rejects);
                if (status != ControlParseStatus.Ok) return null;
                if (rejects.Count == 0) return [];
                LatestRejectBatchWriter writer = new(buffer, carrier);
                foreach (LatestRejectEntry entry in rejects)
                {
                    Assert.True(writer.TryAdd(entry));
                }

                written = writer.Finish();
                break;
            }

            case ControlType.Hello:
                status = ControlCodec.TryParse(body, out Hello hello);
                if (status != ControlParseStatus.Ok) return null;
                Assert.True(ControlCodec.TryWrite(buffer, in hello, out written));
                break;
            case ControlType.HelloAck:
                status = ControlCodec.TryParse(body, out HelloAck ack);
                if (status != ControlParseStatus.Ok) return null;
                Assert.True(ControlCodec.TryWrite(buffer, in ack, out written));
                break;
            case ControlType.Close:
                status = ControlCodec.TryParse(body, out Close close);
                if (status != ControlParseStatus.Ok) return null;
                Assert.True(ControlCodec.TryWrite(buffer, in close, out written));
                break;
            case ControlType.BulkRequest:
                status = ControlCodec.TryParse(body, out BulkRequest request);
                if (status != ControlParseStatus.Ok) return null;
                Assert.True(ControlCodec.TryWrite(buffer, in request, out written));
                break;
            case ControlType.BulkCancel:
                status = ControlCodec.TryParse(body, out BulkCancel cancel);
                if (status != ControlParseStatus.Ok) return null;
                Assert.True(ControlCodec.TryWrite(buffer, in cancel, out written));
                break;
            case ControlType.BulkReject:
                status = ControlCodec.TryParse(body, out BulkReject reject);
                if (status != ControlParseStatus.Ok) return null;
                Assert.True(ControlCodec.TryWrite(buffer, in reject, out written));
                break;
            case ControlType.KeyRetired:
                status = ControlCodec.TryParse(body, out KeyRetired retired);
                if (status != ControlParseStatus.Ok) return null;
                Assert.True(ControlCodec.TryWrite(buffer, in retired, out written));
                break;
            case ControlType.ChannelTableRequest:
                status = ControlCodec.TryParseChannelTableRequest(body);
                if (status != ControlParseStatus.Ok) return null;
                Assert.True(ControlCodec.TryWriteChannelTableRequest(buffer, out written));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }

        return buffer.AsSpan(0, written).ToArray();
    }

    /// <summary>
    /// Runs a datagram and a stream read over <paramref name="data"/>; whenever a frame parses completely, asserts
    /// that re-encoding the parsed message reproduces the exact input bytes (every encoding is canonical).
    /// </summary>
    public static void CheckAllReaders(ReadOnlySpan<byte> data)
    {
        if (ControlCodec.TryReadDatagram(data, out ControlType type, out ReadOnlySpan<byte> body, out int consumed) == ControlParseStatus.Ok)
        {
            Assert.Equal(data.Length, consumed);
            Assert.True(ControlCodec.IsDatagramType((byte)type));
            byte[]? again = Reencode(type, body, ControlCarrier.Datagram, out _);
            if (again is { Length: > 0 })
            {
                Assert.Equal(data.ToArray(), again);
            }
        }

        ReadOnlySpan<byte> rest = data;
        while (true)
        {
            ControlParseStatus status = ControlCodec.TryReadStream(rest, out type, out body, out consumed);
            if (status != ControlParseStatus.Ok)
            {
                Assert.Equal(0, consumed);
                break;
            }

            Assert.InRange(consumed, 2, Math.Min(rest.Length, ControlCodec.MaxEncodedStreamFrameLength));
            byte[]? again = Reencode(type, body, ControlCarrier.Stream, out _);
            if (again is { Length: > 0 })
            {
                Assert.Equal(rest.Slice(0, consumed).ToArray(), again);
            }

            rest = rest.Slice(consumed);
        }

        foreach (ControlType candidate in AllTypes)
        {
            byte[]? again = Reencode(candidate, data, ControlCarrier.Stream, out _);
            if (again is { Length: > 0 })
            {
                Assert.Equal(StreamFrame(candidate, data.ToArray()), again);
            }
        }
    }

    private static Span<byte> Buffer(out byte[] buffer)
    {
        buffer = new byte[ControlCodec.MaxEncodedStreamFrameLength + 64];
        return buffer;
    }

    private static byte[] Finish(bool ok, byte[] buffer, int length)
    {
        Assert.True(ok);
        return buffer.AsSpan(0, length).ToArray();
    }
}
