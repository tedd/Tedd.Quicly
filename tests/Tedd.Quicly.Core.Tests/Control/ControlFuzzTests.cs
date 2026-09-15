using Tedd.Quicly.Core.Control;
using static Tedd.Quicly.Core.Tests.Control.ControlTestData;

namespace Tedd.Quicly.Core.Tests.Control;

/// <summary>
/// Random and mutated input must never throw (every span access is bounds-checked, so "never throws" also means
/// "never reads out of bounds"), and whenever anything parses, re-encoding the parsed message must reproduce the
/// exact input bytes: the codec accepts only canonical encodings.
/// </summary>
public class ControlFuzzTests
{
    private static readonly byte[] InterestingBytes = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x10, 0x11, 0x12, 0x16, 0x17, 0x3F, 0x40, 0x7F, 0x80, 0xBF, 0xC0, 0xFF];

    [Fact]
    public void Random_Bytes_Never_Throw()
    {
        Random random = new(20260915);
        byte[] buffer = new byte[4096];
        for (int iteration = 0; iteration < 20_000; iteration++)
        {
            int length = iteration % 50 == 0 ? random.Next(0, buffer.Length) : random.Next(0, 40);
            Span<byte> data = buffer.AsSpan(0, length);
            random.NextBytes(data);
            if (length > 0 && random.Next(3) == 0)
            {
                data[0] = random.Next(2) == 0 ? (byte)0 : (byte)(length - 1);
            }

            if (length > 1 && random.Next(2) == 0)
            {
                data[1] = (byte)AllTypes[random.Next(AllTypes.Length)];
            }

            CheckAllReaders(data);
        }
    }

    [Fact]
    public void Single_Byte_Mutations_Of_Valid_Frames_Never_Throw()
    {
        foreach (byte[] seed in ValidFrames())
        {
            CheckAllReaders(seed);
            byte[] mutated = (byte[])seed.Clone();
            for (int position = 0; position < seed.Length; position++)
            {
                byte original = seed[position];
                foreach (byte value in Candidates(original))
                {
                    mutated[position] = value;
                    CheckAllReaders(mutated);
                }

                mutated[position] = original;
            }
        }
    }

    [Fact]
    public void Random_Multi_Byte_Mutations_Never_Throw()
    {
        Random random = new(7);
        foreach (byte[] seed in ValidFrames())
        {
            for (int iteration = 0; iteration < 300; iteration++)
            {
                byte[] mutated = (byte[])seed.Clone();
                int changes = random.Next(1, 4);
                for (int c = 0; c < changes; c++)
                {
                    mutated[random.Next(mutated.Length)] = random.Next(2) == 0 ? InterestingBytes[random.Next(InterestingBytes.Length)] : (byte)random.Next(256);
                }

                CheckAllReaders(mutated);

                // Insertions and deletions shift every later field.
                int cut = random.Next(mutated.Length);
                CheckAllReaders(mutated.AsSpan(0, cut).ToArray().Concat(mutated.AsSpan(cut + 1).ToArray()).ToArray());
                CheckAllReaders(mutated.AsSpan(0, cut).ToArray().Concat(new[] { (byte)random.Next(256) }).Concat(mutated.AsSpan(cut).ToArray()).ToArray());
            }
        }
    }

    [Fact]
    public void Every_Strict_Prefix_Of_A_Valid_Frame_Is_Incomplete_Or_Invalid()
    {
        foreach (byte[] seed in ValidFrames())
        {
            bool datagram = seed[0] == 0x00;
            for (int length = 0; length < seed.Length; length++)
            {
                ReadOnlySpan<byte> prefix = seed.AsSpan(0, length);
                if (datagram)
                {
                    ControlParseStatus frameStatus = ControlCodec.TryReadDatagram(prefix, out ControlType type, out ReadOnlySpan<byte> body, out _);
                    if (frameStatus == ControlParseStatus.Ok)
                    {
                        Assert.Null(Reencode(type, body, ControlCarrier.Datagram, out ControlParseStatus status));
                        Assert.NotEqual(ControlParseStatus.Ok, status);
                    }
                }
                else
                {
                    Assert.Equal(ControlParseStatus.NeedMoreData, ControlCodec.TryReadStream(prefix, out _, out _, out int consumed));
                    Assert.Equal(0, consumed);
                }
            }

            ReadOnlySpan<byte> fullBody = datagram ? DatagramBody(seed) : StreamBody(seed);
            ControlType fullType = (ControlType)(datagram ? seed[1] : seed[StreamPrefixLength(seed)]);
            for (int length = 0; length < fullBody.Length; length++)
            {
                Assert.Null(Reencode(fullType, fullBody.Slice(0, length), ControlCarrier.Stream, out ControlParseStatus status));
                Assert.NotEqual(ControlParseStatus.Ok, status);
            }
        }
    }

    [Fact]
    public void Random_Valid_Messages_Round_Trip()
    {
        Random random = new(99);
        for (int i = 0; i < 2000; i++)
        {
            ControlCarrier carrier = random.Next(2) == 0 ? ControlCarrier.Datagram : ControlCarrier.Stream;
            ulong Var() => (ulong)random.NextInt64(0, long.MaxValue) >> random.Next(2, 64);
            ushort Channel() => (ushort)random.Next(ControlCodec.MinChannelId, ControlCodec.MaxChannelId + 1);
            byte[] Blob(int max) => Bytes(random.Next(0, max + 1), (byte)random.Next(0x20, 0x7F));

            CheckAllReaders(Encode(new Ping((uint)random.Next()), carrier));
            CheckAllReaders(Encode(new Pong((uint)random.Next(), (uint)random.Next(), (uint)random.Next()), carrier));
            CheckAllReaders(Encode(new BulkProgress(Var(), Var()), carrier));
            ulong offset = Var();
            CheckAllReaders(Encode(new BulkRequest(Var(), Channel(), Var(), Var(), offset, Var() % (Tedd.Quicly.Core.Primitives.VarInt.MaxValue - offset + 1))));
            CheckAllReaders(Encode(new BulkCancel(Var(), (QuiclyErrorCode)(uint)random.Next())));
            CheckAllReaders(Encode(new BulkReject(Var(), (QuiclyErrorCode)(uint)random.Next())));
            CheckAllReaders(Encode(new KeyRetired(Channel(), Var())));
            CheckAllReaders(Encode(new Close((QuiclyErrorCode)(uint)random.Next(), Blob(512))));
            CheckAllReaders(Encode(new Hello { Flags = (HelloFlags)random.Next(65536), TableHash = Var(), LastEpoch = (uint)random.Next(), SessionToken = Blob(200), AuthToken = Blob(200), MaxReceiveDatagram = (ushort)random.Next(65536), Caps = (PeerCaps)random.Next(65536) }));
            CheckAllReaders(Encode(new HelloAck { Status = HelloStatus.Accepted, SessionId = Var(), Epoch = (uint)random.Next(), MaxMessageSize = Var(), HeartbeatMicros = Var(), GraceMicros = Var(), SessionToken = Blob(100), Table = Table(((ushort)random.Next(2, 16384), 1, 2, 3, Var(), "n")), Reason = Blob(512) }));
        }
    }

    private static IEnumerable<byte> Candidates(byte original)
    {
        yield return (byte)(original ^ 0x01);
        yield return (byte)(original ^ 0x40);
        yield return (byte)(original ^ 0x80);
        yield return (byte)(original + 1);
        yield return (byte)(original - 1);
        foreach (byte b in InterestingBytes)
        {
            if (b != original)
            {
                yield return b;
            }
        }
    }

    private static int StreamPrefixLength(byte[] frame) => Tedd.Quicly.Core.Primitives.VarInt.PeekLength(frame[0]);
}
