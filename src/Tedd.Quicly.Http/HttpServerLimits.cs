namespace Tedd.Quicly.Http;

/// <summary>Size, count and time limits applied to every connection. Defaults follow ADR 0009.</summary>
public sealed class HttpServerLimits
{
    /// <summary>Maximum request-line length in bytes (method, target and version, excluding CRLF). Exceeding it yields 414. Default 2 KiB.</summary>
    public int MaxRequestLineBytes { get; set; } = 2 * 1024;

    /// <summary>Maximum size of the header section in bytes (all header lines including the terminating CRLF). Exceeding it yields 431. Default 8 KiB.</summary>
    public int MaxHeadersBytes { get; set; } = 8 * 1024;

    /// <summary>Maximum number of header fields per request. Exceeding it yields 431. Default 32.</summary>
    public int MaxHeaderCount { get; set; } = 32;

    /// <summary>
    /// Maximum <c>Content-Length</c> accepted for a request body. Exceeding it yields 413 and closes the connection.
    /// Default 0: request bodies are refused unless a handler opts in through <see cref="IHttpBodyHandler"/>; the
    /// effective limit is the largest of this value and every handler's opt-in.
    /// </summary>
    public long MaxRequestBodyBytes { get; set; }

    /// <summary>Maximum number of simultaneously open connections across all endpoints; accepting pauses while the limit is reached. Default 2048.</summary>
    public int MaxConnections { get; set; } = 2048;

    /// <summary>Maximum number of simultaneously open connections from one remote address; further connections are closed immediately. Default 64.</summary>
    public int MaxConnectionsPerAddress { get; set; } = 64;

    /// <summary>Maximum time from the first byte of a request (or from accept, for the TLS handshake) until the header section is complete. Exceeding it yields 408. Default 5 s.</summary>
    public TimeSpan HeaderReadTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Maximum time a persistent connection may sit idle between requests. Default 15 s.</summary>
    public TimeSpan KeepAliveTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Maximum time for a single request-body read to make progress. Default 30 s.</summary>
    public TimeSpan RequestBodyReadTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Maximum time for a single response write to complete. Default 30 s.</summary>
    public TimeSpan ResponseWriteTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Maximum number of requests served on one connection before the server closes it. Default 1000.</summary>
    public int MaxRequestsPerConnection { get; set; } = 1000;

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxRequestLineBytes, 16);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxHeadersBytes, 2);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxHeaderCount, 1);
        ValidateBodyLimit(MaxRequestBodyBytes);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxConnections, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxConnectionsPerAddress, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxRequestsPerConnection, 1);
        ValidateTimeout(HeaderReadTimeout);
        ValidateTimeout(KeepAliveTimeout);
        ValidateTimeout(RequestBodyReadTimeout);
        ValidateTimeout(ResponseWriteTimeout);
    }

    internal static void ValidateBodyLimit(long value)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(value);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(value, int.MaxValue);
    }

    private static void ValidateTimeout(TimeSpan t)
    {
        if (t != Timeout.InfiniteTimeSpan)
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(t, TimeSpan.Zero);
    }
}
