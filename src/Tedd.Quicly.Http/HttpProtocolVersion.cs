namespace Tedd.Quicly.Http;

/// <summary>HTTP protocol version of a request.</summary>
public enum HttpProtocolVersion : byte
{
    /// <summary>HTTP/1.0: the connection closes after the response unless <c>Connection: keep-alive</c> was sent; no chunked encoding.</summary>
    Http10 = 0,

    /// <summary>HTTP/1.1: persistent connections by default, chunked transfer encoding, <c>Expect: 100-continue</c>.</summary>
    Http11 = 1,
}
