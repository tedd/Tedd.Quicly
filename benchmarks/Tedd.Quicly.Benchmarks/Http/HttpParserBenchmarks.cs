using System.Text;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.Http;
using Tedd.Quicly.Http.Parsing;

namespace Tedd.Quicly.Benchmarks.Http;

/// <summary>
/// HTTP/1.1 request-line + header parsing: V0 (shipping, vectorised IndexOf + SearchValues per line) versus V1
/// (archive, single-pass table-driven scalar loop). See docs/benchmarks/http.md.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
[MemoryDiagnoser]
public class HttpParserBenchmarks
{
    private byte[] _request = [];
    private ParsedRequest _parsedV0;
    private ParsedRequestV1 _parsedV1;

    /// <summary>Which request shape to parse.</summary>
    [Params("acme", "browser", "wide")]
    public string Shape { get; set; } = "acme";

    [GlobalSetup]
    public void Setup()
    {
        _request = Encoding.ASCII.GetBytes(Shape switch
        {
            // What Let's Encrypt's validator sends: tiny request line, three headers (~120 B).
            "acme" => "GET /.well-known/acme-challenge/TOKENxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx HTTP/1.1\r\nHost: example.com\r\nUser-Agent: Mozilla/5.0 (compatible; Let's Encrypt validation server; +https://www.letsencrypt.org)\r\nAccept: */*\r\n\r\n",
            // A typical browser page load: 12 headers, ~520 B.
            "browser" => "GET /app/index.html?v=42 HTTP/1.1\r\nHost: game.example.com\r\nConnection: keep-alive\r\nCache-Control: max-age=0\r\nUpgrade-Insecure-Requests: 1\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36\r\nAccept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8\r\nSec-Fetch-Site: none\r\nSec-Fetch-Mode: navigate\r\nSec-Fetch-Dest: document\r\nAccept-Encoding: gzip, deflate, br\r\nAccept-Language: en-US,en;q=0.9\r\nCookie: session=abcdef0123456789abcdef0123456789; theme=dark\r\n\r\n",
            // Long values near the limits: 30 headers of ~200 B each (~6 KiB), the worst case a legitimate client produces.
            _ => BuildWide(),
        });
        _parsedV0 = new ParsedRequest(32);
        _parsedV1 = new ParsedRequestV1(32);
        if (HttpParser.TryParse(_request, 2048, 8192, ref _parsedV0) != HttpParseStatus.Ok)
            throw new InvalidOperationException("V0 rejected the sample.");
        if (HttpParserV1.TryParse(_request, 2048, 8192, ref _parsedV1) != HttpParseStatusV1.Ok)
            throw new InvalidOperationException("V1 rejected the sample.");
        if (_parsedV0.HeaderCount != _parsedV1.HeaderCount || _parsedV0.Consumed != _parsedV1.Consumed)
            throw new InvalidOperationException("V0 and V1 disagree.");
    }

    private static string BuildWide()
    {
        var sb = new StringBuilder("GET /static/bundle.js HTTP/1.1\r\nHost: example.com\r\n");
        for (int i = 0; i < 29; i++)
            sb.Append("X-Header-").Append(i).Append(": ").Append('v', 190).Append("\r\n");
        return sb.Append("\r\n").ToString();
    }

    [Benchmark(Baseline = true)]
    public int V0_SearchValues()
    {
        HttpParser.TryParse(_request, 2048, 8192, ref _parsedV0);
        return _parsedV0.Consumed;
    }

    [Benchmark]
    public int V1_TableDriven()
    {
        HttpParserV1.TryParse(_request, 2048, 8192, ref _parsedV1);
        return _parsedV1.Consumed;
    }
}
