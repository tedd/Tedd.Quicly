namespace Tedd.Quicly.Http.Internal;

/// <summary>Read-only view of the current request body, bounded by <c>Content-Length</c>. One instance per connection, reused per request.</summary>
internal sealed class HttpRequestBodyStream : Stream
{
    private readonly HttpConnection _connection;

    public HttpRequestBodyStream(HttpConnection connection)
    {
        _connection = connection;
    }

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(Span<byte> buffer) => _connection.ReadBody(buffer);

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => _connection.ReadBodyAsync(buffer, cancellationToken);

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override void Write(ReadOnlySpan<byte> buffer) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        // The stream is owned by the connection; disposing it must not close anything.
    }
}
