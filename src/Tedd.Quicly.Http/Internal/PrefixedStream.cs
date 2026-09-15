using System.Buffers;

namespace Tedd.Quicly.Http.Internal;

/// <summary>
/// A read/write stream that first replays bytes already consumed from the inner stream (the peeked TLS ClientHello)
/// and then delegates to it. Writes always go straight to the inner stream.
/// </summary>
/// <remarks>
/// <see cref="System.Net.Security.SslStream"/> issues zero-length reads to wait for data availability; while prefix
/// bytes remain those must complete immediately instead of being forwarded to the socket, otherwise the handshake
/// deadlocks (the client is waiting for our ServerHello, we are waiting for bytes it already sent).
/// </remarks>
internal sealed class PrefixedStream : Stream
{
    private readonly Stream _inner;
    private byte[]? _prefix;
    private int _prefixOffset;
    private readonly int _prefixLength;

    /// <summary>Wraps <paramref name="inner"/>; <paramref name="prefix"/> is a pooled array returned to the pool once consumed.</summary>
    public PrefixedStream(Stream inner, byte[] prefix, int prefixLength)
    {
        _inner = inner;
        _prefix = prefixLength > 0 ? prefix : null;
        _prefixLength = prefixLength;
        if (prefixLength == 0)
            ArrayPool<byte>.Shared.Return(prefix);
    }

    /// <summary>Number of replay bytes not yet handed out.</summary>
    public int PrefixRemaining => _prefix is null ? 0 : _prefixLength - _prefixOffset;

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(Span<byte> buffer)
    {
        if (_prefix is not null)
            return ReadPrefix(buffer);
        return _inner.Read(buffer);
    }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_prefix is not null)
            return new ValueTask<int>(ReadPrefix(buffer.Span));
        return _inner.ReadAsync(buffer, cancellationToken);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    /// <summary>Copies prefix bytes; a zero-length destination returns 0 without touching the inner stream (data is available).</summary>
    private int ReadPrefix(Span<byte> destination)
    {
        var prefix = _prefix!;
        int n = Math.Min(destination.Length, _prefixLength - _prefixOffset);
        prefix.AsSpan(_prefixOffset, n).CopyTo(destination);
        _prefixOffset += n;
        if (_prefixOffset == _prefixLength)
        {
            _prefix = null;
            ArrayPool<byte>.Shared.Return(prefix);
        }
        return n;
    }

    public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);

    public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => _inner.WriteAsync(buffer, cancellationToken);

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _inner.WriteAsync(buffer, offset, count, cancellationToken);

    public override void Flush() => _inner.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            var prefix = _prefix;
            _prefix = null;
            if (prefix is not null)
                ArrayPool<byte>.Shared.Return(prefix);
            _inner.Dispose();
        }
        base.Dispose(disposing);
    }
}
