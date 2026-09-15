using System.Net;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Time;
using static Tedd.Quicly.Core.Tests.Control.ControlTestData;

namespace Tedd.Quicly.Core.Tests.Control;

/// <summary>
/// Hot paths must not allocate on the GC heap in steady state (ADR 0008). Each test warms the path up and then
/// measures <see cref="GC.GetAllocatedBytesForCurrentThread"/> around windows of calls.
/// </summary>
public class ControlZeroAllocationTests
{
    private const int Warmup = 1_000;
    private const int Iterations = 2_000;
    private const int Windows = 5;

    private static readonly ControlCarrier[] Carriers = [ControlCarrier.Datagram, ControlCarrier.Stream];

    /// <summary>
    /// Warms <paramref name="body"/> up, then measures <see cref="Windows"/> consecutive windows of
    /// <see cref="Iterations"/> calls; at most one window may allocate. A one-off runtime event on this thread (tier-up
    /// or OSR compilation landing inside a window) does not repeat, so it cannot fail the test on its own, while a
    /// steady-state allocation, including an amortised one that recurs only every few thousand calls (a rarely
    /// resized buffer), shows up in at least two windows.
    /// </summary>
    private static void AssertNoAllocations(Action body)
    {
        for (int i = 0; i < Warmup; i++)
        {
            body();
        }

        long[] deltas = new long[Windows];
        int allocatingWindows = 0;
        for (int window = 0; window < Windows; window++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < Iterations; i++)
            {
                body();
            }

            deltas[window] = GC.GetAllocatedBytesForCurrentThread() - before;
            if (deltas[window] != 0)
            {
                allocatingWindows++;
            }
        }

        Assert.True(allocatingWindows <= 1,
            $"Allocated in {allocatingWindows} of {Windows} windows: {string.Join(", ", deltas)} bytes per {Iterations} calls.");
    }

    private static ReadOnlySpan<byte> ReadFrame(ReadOnlySpan<byte> frame, ControlCarrier carrier, out ControlType type)
    {
        ControlParseStatus status = carrier == ControlCarrier.Datagram
            ? ControlCodec.TryReadDatagram(frame, out type, out ReadOnlySpan<byte> body, out _)
            : ControlCodec.TryReadStream(frame, out type, out body, out _);
        if (status != ControlParseStatus.Ok)
        {
            throw new InvalidOperationException(status.ToString());
        }

        return body;
    }

    [Fact]
    public void Ping_Pong_Encode_Decode()
    {
        byte[] buffer = new byte[64];
        AssertNoAllocations(() =>
        {
            foreach (ControlCarrier carrier in Carriers)
            {
                ControlCodec.TryWrite(buffer, new Ping(123), carrier, out int n);
                ControlCodec.TryParse(ReadFrame(buffer.AsSpan(0, n), carrier, out _), out Ping ping);
                ControlCodec.TryWrite(buffer, new Pong(ping.TimeMicros, 5, 6), carrier, out n);
                if (ControlCodec.TryParse(ReadFrame(buffer.AsSpan(0, n), carrier, out _), out Pong pong) != ControlParseStatus.Ok || pong.EchoedTimeMicros != 123)
                {
                    throw new InvalidOperationException();
                }
            }
        });
    }

    [Fact]
    public void LatestAck_And_LatestReject_Batches_Of_32()
    {
        byte[] buffer = new byte[1200];
        LatestAckEntry[] acks = new LatestAckEntry[32];
        LatestRejectEntry[] rejects = new LatestRejectEntry[32];
        for (int i = 0; i < 32; i++)
        {
            acks[i] = new LatestAckEntry((ushort)(2 + i * 3), (ulong)i * 1000, (uint)i);
            rejects[i] = new LatestRejectEntry((ushort)(2 + i), (ulong)i, (uint)i, LatestRejectReason.RingFull);
        }

        AssertNoAllocations(() =>
        {
            foreach (ControlCarrier carrier in Carriers)
            {
                LatestAckBatchWriter writer = new(buffer, carrier);
                foreach (LatestAckEntry entry in acks)
                {
                    writer.TryAdd(entry);
                }

                int length = writer.Finish();
                ControlCodec.TryParse(ReadFrame(buffer.AsSpan(0, length), carrier, out _), out LatestAckBatchReader reader);
                ulong sum = 0;
                foreach (LatestAckEntry entry in reader)
                {
                    sum += entry.Key;
                }

                LatestRejectBatchWriter rejectWriter = new(buffer, carrier);
                foreach (LatestRejectEntry entry in rejects)
                {
                    rejectWriter.TryAdd(entry);
                }

                length = rejectWriter.Finish();
                ControlCodec.TryParse(ReadFrame(buffer.AsSpan(0, length), carrier, out _), out LatestRejectBatchReader rejectReader);
                foreach (LatestRejectEntry entry in rejectReader)
                {
                    sum += entry.Key;
                }

                if (sum != 496_000 + 496)
                {
                    throw new InvalidOperationException();
                }
            }
        });
    }

    [Fact]
    public void Stream_Messages_Parse_And_Write()
    {
        byte[] hello = StreamBody(Encode(new Hello { SessionToken = Bytes(69), AuthToken = Bytes(32), Caps = PeerCaps.Datagrams }));
        byte[] helloAck = StreamBody(Encode(new HelloAck { SessionToken = Bytes(69), Table = Table((2, 1, 0, 1, 1200, "a")), Reason = "ok"u8 }));
        byte[] close = StreamBody(Encode(new Close(QuiclyErrorCode.NoError, "bye"u8)));
        byte[] buffer = new byte[1024];
        char[] text = new char[64];
        AssertNoAllocations(() =>
        {
            ControlCodec.TryParse(hello, out Hello h);
            ControlCodec.TryWrite(buffer, in h, out _);
            ControlCodec.TryParse(helloAck, out HelloAck a);
            ControlCodec.TryWrite(buffer, in a, out _);
            ControlCodec.TryParse(close, out Close c);
            ControlCodec.TryWrite(buffer, in c, out _);
            ControlCodec.CopySanitizedReason(c.Reason, text);
            ControlCodec.TryWrite(buffer, new BulkRequest(1, 2, 3, 4, 5, 6), out int n);
            ControlCodec.TryParse(buffer.AsSpan(2, n - 2), out BulkRequest _);
            ControlCodec.TryWrite(buffer, new KeyRetired(2, 9), out _);
            ControlCodec.TryWrite(buffer, new BulkProgress(1, 2), ControlCarrier.Datagram, out _);
        });
    }

    [Fact]
    public void Session_Token_Mint_Validate_Inspect()
    {
        VirtualClock clock = new(0);
        using SessionTokenAuthority authority = new(Bytes(32, 7), clock);
        const int tokenCount = Warmup + (Windows * Iterations);
        byte[] tokens = new byte[tokenCount * SessionTokenAuthority.TokenLength];
        for (int i = 0; i < tokenCount; i++)
        {
            authority.Mint((ulong)i, 1, 1_000_000, tokens.AsSpan(i * SessionTokenAuthority.TokenLength));
        }

        byte[] scratch = new byte[SessionTokenAuthority.TokenLength];
        int next = 0;
        AssertNoAllocations(() =>
        {
            ReadOnlySpan<byte> token = tokens.AsSpan(next++ * SessionTokenAuthority.TokenLength, SessionTokenAuthority.TokenLength);
            if (authority.TryInspect(token, out _, out _) != SessionTokenStatus.Valid
                || authority.TryValidate(token, out _, out _) != SessionTokenStatus.Valid
                || authority.TryValidate(token, out _, out _) != SessionTokenStatus.Replayed)
            {
                throw new InvalidOperationException();
            }

            authority.Mint(1, 2, 3, scratch);
        });
    }

    [Fact]
    public void Rate_Limiter_Steady_State()
    {
        VirtualClock clock = new(0);
        AuthFailureRateLimiter limiter = new(clock);
        IPAddress v6 = IPAddress.Parse("2001:db8::7");
        byte[] v4 = [203, 0, 113, 9];
        AssertNoAllocations(() =>
        {
            clock.AdvanceMicros(10_000_000);
            limiter.IsAllowed(v4);
            limiter.RecordFailure(v4);
            limiter.IsAllowed(v6);
            limiter.RecordFailure(v6);
        });
    }
}
