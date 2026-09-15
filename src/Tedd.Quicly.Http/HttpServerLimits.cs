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

    /// <summary>
    /// Largest finite timeout accepted: every timeout ends up in <see cref="CancellationTokenSource.CancelAfter(TimeSpan)"/>,
    /// which refuses delays above <c>uint.MaxValue - 1</c> ms (about 49.7 days). Use <see cref="Timeout.InfiniteTimeSpan"/>
    /// for "no timeout".
    /// </summary>
    internal static readonly TimeSpan MaxTimeout = TimeSpan.FromMilliseconds(4_294_967_294L);

    /// <summary>Upper bound for <see cref="MaxRequestLineBytes"/> and <see cref="MaxHeadersBytes"/>; the header buffer is pre-allocated per connection.</summary>
    internal const int MaxHeaderBufferLimit = 1024 * 1024;

    /// <summary>Upper bound for <see cref="MaxHeaderCount"/>; the header offset table is pre-allocated per connection.</summary>
    internal const int MaxHeaderCountLimit = 1024;

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxRequestLineBytes, 16, nameof(MaxRequestLineBytes));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(MaxRequestLineBytes, MaxHeaderBufferLimit, nameof(MaxRequestLineBytes));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxHeadersBytes, 2, nameof(MaxHeadersBytes));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(MaxHeadersBytes, MaxHeaderBufferLimit, nameof(MaxHeadersBytes));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxHeaderCount, 1, nameof(MaxHeaderCount));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(MaxHeaderCount, MaxHeaderCountLimit, nameof(MaxHeaderCount));
        ValidateBodyLimit(MaxRequestBodyBytes);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxConnections, 1, nameof(MaxConnections));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxConnectionsPerAddress, 1, nameof(MaxConnectionsPerAddress));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxRequestsPerConnection, 1, nameof(MaxRequestsPerConnection));
        ValidateTimeout(HeaderReadTimeout, allowZero: false, nameof(HeaderReadTimeout));
        ValidateTimeout(KeepAliveTimeout, allowZero: false, nameof(KeepAliveTimeout));
        ValidateTimeout(RequestBodyReadTimeout, allowZero: false, nameof(RequestBodyReadTimeout));
        ValidateTimeout(ResponseWriteTimeout, allowZero: false, nameof(ResponseWriteTimeout));
    }

    internal static void ValidateBodyLimit(long value)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(value);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(value, int.MaxValue);
    }

    /// <summary>
    /// Accepts <see cref="Timeout.InfiniteTimeSpan"/> or a positive (or, with <paramref name="allowZero"/>, zero)
    /// duration no longer than <see cref="MaxTimeout"/>.
    /// </summary>
    internal static void ValidateTimeout(TimeSpan value, bool allowZero, string paramName)
    {
        if (value == Timeout.InfiniteTimeSpan)
            return;
        if (value < TimeSpan.Zero || (!allowZero && value == TimeSpan.Zero))
            throw new ArgumentOutOfRangeException(paramName, value, "A timeout must be positive or Timeout.InfiniteTimeSpan.");
        if (value > MaxTimeout)
            throw new ArgumentOutOfRangeException(paramName, value, "A finite timeout cannot exceed " + MaxTimeout + "; use Timeout.InfiniteTimeSpan for no timeout.");
    }
}
