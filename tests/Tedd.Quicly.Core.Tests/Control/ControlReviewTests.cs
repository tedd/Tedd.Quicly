using System.Buffers.Binary;
using System.Security.Cryptography;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Time;
using static Tedd.Quicly.Core.Tests.Control.ControlTestData;

namespace Tedd.Quicly.Core.Tests.Control;

/// <summary>
/// Review findings (quicly/b2-control-codec). Each test states the property the implementation must have; the tests in
/// this class failed against the reviewed code and are kept as regression guards for the fixes.
/// </summary>
public class ControlReviewTests
{
    /// <summary>A clock whose next read runs a callback first (lets a test interleave a concurrent call deterministically).</summary>
    private sealed class ReentrantClock : IClock
    {
        public Action? OnNextRead;

        public long NowMicros
        {
            get
            {
                Action? callback = OnNextRead;
                OnNextRead = null;
                callback?.Invoke();
                return 0;
            }
        }
    }

    private static byte[] Fill(byte value) => Enumerable.Repeat(value, SessionTokenAuthority.KeyLength).ToArray();

    /// <summary>A structurally valid v1 token whose MAC was computed with <paramref name="key"/> (attacker-chosen).</summary>
    private static byte[] Forge(ulong sessionId, uint epoch, long expiry, byte[] key)
    {
        byte[] token = new byte[SessionTokenAuthority.TokenLength];
        token[0] = SessionTokenAuthority.FormatVersion;
        BinaryPrimitives.WriteUInt64LittleEndian(token.AsSpan(1), sessionId);
        BinaryPrimitives.WriteUInt32LittleEndian(token.AsSpan(9), epoch);
        BinaryPrimitives.WriteInt64LittleEndian(token.AsSpan(13), expiry);
        RandomNumberGenerator.Fill(token.AsSpan(21, 16));
        HMACSHA256.HashData(key, token.AsSpan(0, 37), token.AsSpan(37, 32));
        return token;
    }

    /// <summary>
    /// Fixed defect: <see cref="SessionTokenAuthority"/> validates against a key snapshot read before the clock, while
    /// <see cref="SessionTokenAuthority.RotateKey"/> zeroes the outgoing "previous" key array in place. A validation that
    /// overlaps a rotation therefore computes HMAC with an all-zero key, so a token forged with the all-zero key (which
    /// anyone can compute) is accepted as <see cref="SessionTokenStatus.Valid"/> for any sessionId. The re-entrant clock
    /// makes the interleaving deterministic; in production it is a race between an admission thread and the rotation.
    /// </summary>
    [Fact]
    public void Key_Rotation_During_Validation_Must_Not_Accept_A_Zero_Key_Forgery()
    {
        ReentrantClock clock = new();
        using SessionTokenAuthority authority = new(Fill(0x11), Fill(0x22), long.MaxValue, clock);
        byte[] forged = Forge(sessionId: 0xBAD_5E55, epoch: 1, expiry: 1_000_000, key: new byte[SessionTokenAuthority.KeyLength]);
        Assert.Equal(SessionTokenStatus.BadSignature, authority.TryInspect(forged, out _, out _));

        clock.OnNextRead = () => authority.RotateKey(Fill(0x33), long.MaxValue);
        SessionTokenStatus status = authority.TryValidate(forged, out ulong sessionId, out _);

        Assert.True(status != SessionTokenStatus.Valid, $"Forged token accepted for session 0x{sessionId:X} while the key rotated.");
    }

    /// <summary>
    /// Fixed defect (same root cause): <see cref="SessionTokenAuthority.Dispose"/> zeroes the current key while a validation that
    /// already passed the disposed check is still running, so the zero-key forgery is accepted during shutdown.
    /// </summary>
    [Fact]
    public void Dispose_During_Validation_Must_Not_Accept_A_Zero_Key_Forgery()
    {
        ReentrantClock clock = new();
        SessionTokenAuthority authority = new(Fill(0x44), clock);
        byte[] forged = Forge(sessionId: 0xBAD_5E55, epoch: 1, expiry: 1_000_000, key: new byte[SessionTokenAuthority.KeyLength]);

        clock.OnNextRead = authority.Dispose;
        SessionTokenStatus? status = null;
        try
        {
            status = authority.TryValidate(forged, out _, out _);
        }
        catch (ObjectDisposedException)
        {
            // Acceptable outcome.
        }

        Assert.True(status != SessionTokenStatus.Valid, "Forged token accepted while the authority was being disposed.");
    }

    /// <summary>
    /// Fixed defect / spec deviation (PROTOCOL.md §4.1: "failed auth attempts are rate-limited per remote address"): once every
    /// 8-way bucket is occupied and the single shared overflow bucket is exhausted, <see cref="AuthFailureRateLimiter.IsAllowed(ReadOnlySpan{byte})"/>
    /// refuses every address it is not tracking, including addresses that have never failed. With the defaults an attacker
    /// holding one IPv6 /48 (65 536 /64 keys) locks out all new clients: ~41 k failed handshakes keep every slot busy for
    /// 60 s and one failure per 6 s keeps the overflow bucket empty. The limiter then turns a cheap failure flood into a
    /// global admission outage. An address with no recorded failures must be admitted (evict the entry closest to
    /// refilled, or fail open for untracked addresses).
    /// </summary>
    [Fact]
    public void Addresses_That_Never_Failed_Are_Not_Locked_Out_By_A_Distributed_Failure_Flood()
    {
        VirtualClock clock = new(0);
        AuthFailureRateLimiter limiter = new(clock); // defaults: 4096 entries, burst 10, refill 6 s, IPv6 /64
        byte[] attacker = [0x20, 0x01, 0x0D, 0xB8, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]; // 2001:db8:0:XXXX::1
        int attackerFailures = 0;
        for (int i = 0; i < 65_536; i++)
        {
            attacker[6] = (byte)(i >> 8);
            attacker[7] = (byte)i;
            for (int k = 0; k < AuthFailureRateLimiter.DefaultBurst && limiter.IsAllowed(attacker); k++)
            {
                limiter.RecordFailure(attacker);
                attackerFailures++;
            }
        }

        clock.AdvanceMicros(5_000_000); // five seconds after the flood stopped
        int refused = 0;
        for (int i = 0; i < 256; i++)
        {
            if (!limiter.IsAllowed([198, 51, 100, (byte)i]))
            {
                refused++;
            }
        }

        Assert.True(refused == 0, $"{refused}/256 addresses with no failures were refused after {attackerFailures} failures from other addresses.");
    }

    /// <summary>
    /// Follow-up (spec gap: the HelloAck, table included, must fit in one 16384-byte frame): the budget returned by
    /// <see cref="ControlCodec.GetMaxHelloAckTableLength"/> is exact even with worst-case (8-byte) varint fields.
    /// </summary>
    [Theory]
    [InlineData(0, 0)]
    [InlineData(SessionTokenAuthority.TokenLength, 0)]
    [InlineData(SessionTokenAuthority.TokenLength, ControlCodec.MaxReasonLength)]
    [InlineData(63, 63)]
    [InlineData(64, 64)]
    [InlineData(ControlCodec.MaxTokenLength, ControlCodec.MaxReasonLength)]
    public void HelloAck_Table_Budget_Is_Exact(int tokenLength, int reasonLength)
    {
        int budget = ControlCodec.GetMaxHelloAckTableLength(tokenLength, reasonLength);
        byte[] token = Bytes(tokenLength);
        byte[] reason = Bytes(reasonLength, (byte)'r');
        byte[] buffer = new byte[ControlCodec.MaxEncodedStreamFrameLength];
        byte[] fits = TableOfLength(budget);
        byte[] tooLarge = TableOfLength(budget + 1);

        Assert.True(ControlCodec.TryWrite(buffer, Ack(fits), out int written));
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(buffer.AsSpan(0, written), out ControlType type, out ReadOnlySpan<byte> body, out _));
        Assert.Equal(ControlType.HelloAck, type);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out HelloAck parsed));
        Assert.Equal(budget, parsed.Table.Length);
        Assert.ThrowsAny<ArgumentException>(() => ControlCodec.TryWrite(buffer, Ack(tooLarge), out _));

        HelloAck Ack(byte[] table) => new()
        {
            SessionToken = token,
            Table = table,
            Reason = reason,
            MaxMessageSize = VarInt.MaxValue,
            HeartbeatMicros = VarInt.MaxValue,
            GraceMicros = VarInt.MaxValue,
        };
    }

    [Fact]
    public void HelloAck_Table_Budget_Argument_Validation()
    {
        Assert.Equal(15_756, ControlCodec.GetMaxHelloAckTableLength(SessionTokenAuthority.TokenLength, ControlCodec.MaxReasonLength));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.GetMaxHelloAckTableLength(-1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.GetMaxHelloAckTableLength(ControlCodec.MaxTokenLength + 1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.GetMaxHelloAckTableLength(0, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => ControlCodec.GetMaxHelloAckTableLength(0, ControlCodec.MaxReasonLength + 1));
    }
}

/// <summary>
/// Additional adversarial probes written during the review. These PASS against the reviewed code; they are kept as
/// regression guards for the batch writer's header reservation and for incremental stream reassembly.
/// </summary>
public class ControlReviewProbeTests
{
    private static readonly LatestAckEntry[] AckEntries = Enumerable.Range(0, 3000).Select(i => new LatestAckEntry(
        (ushort)(i % 2 == 0 ? 2 + (i % 60) : 64 + (i % 16000)),
        (i % 4) switch
        {
            0 => (ulong)(i % 64),
            1 => 64 + (ulong)i,
            2 => 16384 + ((ulong)i * 7),
            _ => (1UL << 30) + ((ulong)i * 1_000_003),
        },
        (uint)i * 2654435761u)).ToArray();

    private static IEnumerable<int> DestinationSizes()
    {
        for (int size = 0; size <= 400; size++)
        {
            yield return size;
        }

        for (int size = 1190; size <= 1210; size++)
        {
            yield return size;
        }

        for (int size = 16_370; size <= 16_400; size++)
        {
            yield return size;
        }

        yield return 20_000;
        yield return 65_535;
    }

    [Fact]
    public void Ack_Batch_Writer_Output_Always_Parses_For_Every_Destination_Size()
    {
        foreach (ControlCarrier carrier in new[] { ControlCarrier.Datagram, ControlCarrier.Stream })
        {
            foreach (int size in DestinationSizes())
            {
                byte[] destination = new byte[size];
                LatestAckBatchWriter writer = new(destination, carrier);
                int added = 0;
                while (added < AckEntries.Length && writer.TryAdd(AckEntries[added]))
                {
                    added++;
                }

                int length = writer.Finish();
                if (added == 0)
                {
                    Assert.Equal(0, length);
                    continue;
                }

                Assert.InRange(length, 1, size);
                ReadOnlySpan<byte> frame = destination.AsSpan(0, length);
                ControlParseStatus status = carrier == ControlCarrier.Datagram
                    ? ControlCodec.TryReadDatagram(frame, out ControlType type, out ReadOnlySpan<byte> body, out int consumed)
                    : ControlCodec.TryReadStream(frame, out type, out body, out consumed);
                Assert.Equal(ControlParseStatus.Ok, status);
                Assert.Equal(length, consumed);
                Assert.Equal(ControlType.LatestAck, type);
                Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out LatestAckBatchReader reader));
                Assert.Equal(added, reader.Count);
                int index = 0;
                foreach (LatestAckEntry entry in reader)
                {
                    Assert.Equal(AckEntries[index++], entry);
                }

                // The writer stopped because the next entry really did not fit.
                if (added < AckEntries.Length)
                {
                    LatestAckEntry next = AckEntries[added];
                    int nextLength = VarInt.GetLength(next.Channel) + VarInt.GetLength(next.Key) + 4;
                    int entriesLength = body.Length - VarInt.GetLength((ulong)added);
                    bool frameLimit = carrier == ControlCarrier.Stream
                        && 1 + VarInt.GetLength((ulong)added + 1) + entriesLength + nextLength > ControlCodec.MaxFrameLength;
                    Assert.True(frameLimit || EntriesStart(carrier, size) + entriesLength + nextLength > size,
                        $"carrier {carrier}, size {size}: stopped at {added} entries with room to spare.");
                }
            }
        }
    }

    // The writer reserves the largest header it could need up front (see LatestBatchWriterCore).
    private static int EntriesStart(ControlCarrier carrier, int size)
    {
        int usable = carrier == ControlCarrier.Stream ? Math.Min(size, ControlCodec.MaxEncodedStreamFrameLength) : size;
        int headerReserve = carrier == ControlCarrier.Datagram ? 2 : VarInt.GetLength((ulong)Math.Min(usable, ControlCodec.MaxFrameLength)) + 1;
        return headerReserve + VarInt.GetLength((ulong)(usable / 6));
    }

    [Fact]
    public void Stream_Reader_Reassembles_Frames_Fed_One_Byte_At_A_Time()
    {
        List<byte[]> frames = ValidFrames().Where(frame => frame[0] != 0x00).ToList();
        frames.Add(Encode(new Hello { SessionToken = Bytes(ControlCodec.MaxTokenLength, 1), AuthToken = Bytes(ControlCodec.MaxTokenLength, 2) }));
        frames.Add(Encode(new HelloAck { Table = TableOfLength(16_383 - 18 - 3 - 1 - 1), Reason = [] }));
        byte[] stream = Cat([.. frames]);

        List<byte[]> received = [];
        int start = 0;
        for (int end = 0; end <= stream.Length; end++)
        {
            while (true)
            {
                ControlParseStatus status = ControlCodec.TryReadStream(stream.AsSpan(start, end - start), out _, out _, out int consumed);
                if (status == ControlParseStatus.NeedMoreData)
                {
                    break;
                }

                Assert.Equal(ControlParseStatus.Ok, status);
                received.Add(stream.AsSpan(start, consumed).ToArray());
                start += consumed;
            }
        }

        Assert.Equal(stream.Length, start);
        Assert.Equal(frames.Count, received.Count);
        for (int i = 0; i < frames.Count; i++)
        {
            Assert.Equal(frames[i], received[i]);
        }
    }
}
