using System.Text;
using Tedd.Quicly.Http.Parsing;

namespace Tedd.Quicly.Http.Tests;

/// <summary>
/// Seeded mutation fuzzing of <see cref="HttpParser"/>. The parser must never throw; every request it accepts must
/// satisfy the invariants <c>HttpConnection</c> relies on; every strict prefix of an accepted request must report
/// <see cref="HttpParseStatus.NeedMore"/> (a request split across packets is never rejected); and the verdict
/// must not depend on bytes after <see cref="ParsedRequest.Consumed"/> (pipelined data cannot change it).
/// </summary>
public class HttpParserFuzzTests
{
    private const int MaxHeaderCount = 32;

    private static readonly byte[][] Seeds =
    [
        Encoding.ASCII.GetBytes("GET / HTTP/1.1\r\nHost: a\r\n\r\n"),
        Encoding.ASCII.GetBytes("GET /.well-known/acme-challenge/abcdefghijklmnopqrstuvwxyz HTTP/1.1\r\nHost: example.com\r\nUser-Agent: validator\r\nAccept: */*\r\n\r\n"),
        Encoding.ASCII.GetBytes("POST /api?x=1&y=%20 HTTP/1.0\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nhello"),
        Encoding.ASCII.GetBytes("\r\n\r\nHEAD http://h.example/p HTTP/1.1\r\nHost:\th.example \r\nX-Empty:\r\n\r\n"),
        Encoding.ASCII.GetBytes("OPTIONS * HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nTransfer-Encoding: chunked\r\n\r\n"),
        BuildMany(40),
    ];

    private static readonly byte[] Interesting =
        [0, (byte)'\r', (byte)'\n', (byte)' ', (byte)'\t', (byte)':', (byte)'%', (byte)'/', 0x7F, 0x80, 0xFF, (byte)'?', (byte)'H', (byte)'.', (byte)'1', (byte)'0', (byte)'2'];

    private static byte[] BuildMany(int count)
    {
        var sb = new StringBuilder("GET /many HTTP/1.1\r\n");
        for (int i = 0; i < count; i++)
            sb.Append("X-").Append(i).Append(": v\r\n");
        return Encoding.ASCII.GetBytes(sb.Append("\r\n").ToString());
    }

    [Fact]
    public void Mutated_requests_never_throw_and_accepted_ones_are_consistent()
    {
        var random = new Random(20260915);
        var parsed = new ParsedRequest(MaxHeaderCount);
        var outcomes = new int[Enum.GetValues<HttpParseStatus>().Length];
        for (int iteration = 0; iteration < 150_000; iteration++)
        {
            var input = Mutate(random);
            int maxLine = random.Next(16, 160);
            int maxHeaders = random.Next(2, 700);
            var status = HttpParser.TryParse(input, maxLine, maxHeaders, ref parsed);
            outcomes[(int)status]++;
            if (status != HttpParseStatus.Ok)
                continue;
            AssertConsistent(input, in parsed, maxLine, maxHeaders);
            if (iteration % 8 == 0)
                AssertIncremental(input, parsed.Consumed, maxLine, maxHeaders);
        }
        foreach (int count in outcomes)
            Assert.True(count > 0, "every outcome should be reached: " + string.Join(", ", outcomes));
    }

    [Fact]
    public void Random_bytes_never_throw()
    {
        var random = new Random(7);
        var parsed = new ParsedRequest(MaxHeaderCount);
        var buffer = new byte[1024];
        for (int iteration = 0; iteration < 50_000; iteration++)
        {
            int length = random.Next(buffer.Length);
            random.NextBytes(buffer.AsSpan(0, length));
            var status = HttpParser.TryParse(buffer.AsSpan(0, length), 2048, 8192, ref parsed);
            Assert.True(Enum.IsDefined(status));
        }
    }

    private static byte[] Mutate(Random random)
    {
        var data = new List<byte>(Seeds[random.Next(Seeds.Length)]);
        int mutations = random.Next(0, 5);
        for (int m = 0; m < mutations && data.Count > 0; m++)
        {
            int at = random.Next(data.Count);
            switch (random.Next(6))
            {
                case 0:
                    data[at] = (byte)random.Next(256);
                    break;
                case 1:
                    data[at] = Interesting[random.Next(Interesting.Length)];
                    break;
                case 2:
                    data.Insert(at, Interesting[random.Next(Interesting.Length)]);
                    break;
                case 3:
                    data.RemoveAt(at);
                    break;
                case 4:
                    data.InsertRange(at, data.GetRange(at, Math.Min(random.Next(1, 40), data.Count - at)));
                    break;
                default:
                    data.InsertRange(at, Seeds[random.Next(Seeds.Length)]);
                    break;
            }
        }
        if (random.Next(6) == 0 && data.Count > 0)
            data = data.GetRange(0, random.Next(data.Count)); // truncation: incomplete requests
        return [.. data];
    }

    private static void AssertConsistent(byte[] input, in ParsedRequest p, int maxLine, int maxHeaders)
    {
        Assert.InRange(p.Consumed, 4, input.Length);
        Assert.Equal((byte)'\r', input[p.Consumed - 2]);
        Assert.Equal((byte)'\n', input[p.Consumed - 1]);

        // Request line: method SP target SP HTTP/1.x CRLF, within the limit.
        Assert.True(p.MethodStart is 0 or 2 or 4);
        Assert.True(p.MethodLength > 0 && p.TargetLength > 0);
        Assert.Equal((byte)' ', input[p.MethodStart + p.MethodLength]);
        Assert.Equal(p.MethodStart + p.MethodLength + 1, p.TargetStart);
        Assert.Equal((byte)' ', input[p.TargetStart + p.TargetLength]);
        foreach (byte b in input.AsSpan(p.MethodStart, p.MethodLength))
            Assert.True(IsTchar(b));
        foreach (byte b in input.AsSpan(p.TargetStart, p.TargetLength))
            Assert.True(b > 0x20 && b != 0x7F);
        int lineEnd = p.MethodStart + input.AsSpan(p.MethodStart).IndexOf((byte)'\n');
        Assert.True(lineEnd - 1 - p.MethodStart <= maxLine);
        Assert.Equal(p.TargetStart + p.TargetLength + 1 + 8, lineEnd - 1);
        Assert.True(input.AsSpan(p.TargetStart + p.TargetLength + 1, 8).SequenceEqual(p.Version == HttpProtocolVersion.Http11 ? "HTTP/1.1"u8 : "HTTP/1.0"u8));

        // Header section: bounded, every field is name ":" OWS value OWS CRLF with a clean value.
        int headersStart = lineEnd + 1;
        Assert.True(p.Consumed - headersStart <= maxHeaders);
        Assert.InRange(p.HeaderCount, 0, MaxHeaderCount);
        int previousEnd = headersStart;
        for (int i = 0; i < p.HeaderCount; i++)
        {
            var h = p.Headers[i];
            Assert.True(h.NameLength > 0);
            Assert.True(h.NameStart >= previousEnd);
            Assert.True(h.ValueStart + h.ValueLength <= p.Consumed - 2);
            Assert.Equal((byte)':', input[h.NameStart + h.NameLength]);
            foreach (byte b in input.AsSpan(h.NameStart, h.NameLength))
                Assert.True(IsTchar(b));
            var value = input.AsSpan(h.ValueStart, h.ValueLength);
            foreach (byte b in value)
                Assert.True(b == (byte)'\t' || (b >= 0x20 && b != 0x7F));
            if (!value.IsEmpty)
            {
                Assert.False(value[0] is (byte)' ' or (byte)'\t');
                Assert.False(value[^1] is (byte)' ' or (byte)'\t');
            }
            previousEnd = h.ValueStart + h.ValueLength;
        }
    }

    private static void AssertIncremental(byte[] input, int consumed, int maxLine, int maxHeaders)
    {
        var scratch = new ParsedRequest(MaxHeaderCount);
        for (int cut = 0; cut < consumed; cut++)
            Assert.Equal(HttpParseStatus.NeedMore, HttpParser.TryParse(input.AsSpan(0, cut), maxLine, maxHeaders, ref scratch));
        Assert.Equal(HttpParseStatus.Ok, HttpParser.TryParse(input.AsSpan(0, consumed), maxLine, maxHeaders, ref scratch));
        Assert.Equal(consumed, scratch.Consumed);
    }

    private static bool IsTchar(byte b)
        => (b < 0x80 && char.IsAsciiLetterOrDigit((char)b)) || "!#$%&'*+-.^_`|~"u8.IndexOf(b) >= 0;
}
