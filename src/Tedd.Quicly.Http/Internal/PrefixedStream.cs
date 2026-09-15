using System.Buffers;

namespace Tedd.Quicly.Http.Internal;

/// <summary>
/// A read/write stream that first replays bytes already consumed from the inner stream (the peeked TLS ClientHello)
/// and then delegates to it. Writes always go straight to the inner stream.
/// </summary>
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
        _prefix = prefix;
        _prefixLength = prefixLength;
    }

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
        int n = ReadPrefix(buffer);
        return n > 0 ? n : _inner.Read(buffer);
    }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        int n = ReadPrefix(buffer.Span);
        return n > 0 ? new ValueTask<int>(n) : _inner.ReadAsync(buffer, cancellationToken);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    private int ReadPrefix(Span<byte> destination)
    {
        var prefix = _prefix;
        if (prefix is null || destination.IsEmpty)
            return 0;
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
