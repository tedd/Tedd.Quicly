using System.Buffers;
using Tedd.Quicly.Http.Internal;

namespace Tedd.Quicly.Http.Tests;

public class PrefixedStreamTests
{
    private static PrefixedStream Create(byte[] prefix, byte[] rest)
    {
        var rented = ArrayPool<byte>.Shared.Rent(Math.Max(prefix.Length, 1));
        prefix.CopyTo(rented, 0);
        return new PrefixedStream(new MemoryStream(rest), rented, prefix.Length);
    }

    [Fact]
    public async Task Replays_prefix_then_reads_inner()
    {
        using var stream = Create([1, 2, 3, 4, 5], [6, 7, 8]);
        Assert.True(stream.CanRead);
        Assert.True(stream.CanWrite);
        Assert.False(stream.CanSeek);
        Assert.Equal(5, stream.PrefixRemaining);

        var buffer = new byte[2];
        Assert.Equal(2, await stream.ReadAsync(buffer));
        Assert.Equal(new byte[] { 1, 2 }, buffer);
        Assert.Equal(3, stream.PrefixRemaining);

        // zero-length reads complete immediately while prefix bytes remain (SslStream's availability probe)
        Assert.Equal(0, await stream.ReadAsync(Memory<byte>.Empty));
        Assert.Equal(0, stream.Read(Span<byte>.Empty));
        Assert.Equal(3, stream.PrefixRemaining);

        Assert.Equal(1, stream.Read(buffer, 0, 1));
        Assert.Equal(3, buffer[0]);
        Assert.Equal(2, await stream.ReadAsync(buffer, 0, 2, CancellationToken.None));
        Assert.Equal(new byte[] { 4, 5 }, buffer);
        Assert.Equal(0, stream.PrefixRemaining);

        var rest = new byte[8];
        Assert.Equal(3, await stream.ReadAsync(rest));
        Assert.Equal(new byte[] { 6, 7, 8 }, rest[..3]);
        Assert.Equal(0, stream.Read(rest));
    }

    [Fact]
    public async Task Writes_and_flushes_go_to_inner()
    {
        var inner = new MemoryStream();
        var rented = ArrayPool<byte>.Shared.Rent(4);
        using var stream = new PrefixedStream(inner, rented, 0); // empty prefix is returned to the pool immediately
        Assert.Equal(0, stream.PrefixRemaining);
        stream.Write([1, 2]);
        stream.Write([3, 4], 0, 2);
        await stream.WriteAsync(new byte[] { 5 });
        await stream.WriteAsync([6], 0, 1, CancellationToken.None);
        stream.Flush();
        await stream.FlushAsync(CancellationToken.None);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6 }, inner.ToArray());
        Assert.Throws<NotSupportedException>(() => stream.Length);
        Assert.Throws<NotSupportedException>(() => stream.Position);
        Assert.Throws<NotSupportedException>(() => stream.Position = 0);
        Assert.Throws<NotSupportedException>(() => stream.Seek(0, SeekOrigin.Begin));
        Assert.Throws<NotSupportedException>(() => stream.SetLength(0));
    }

    [Fact]
    public void Dispose_returns_unconsumed_prefix_and_disposes_inner()
    {
        var inner = new MemoryStream();
        var stream = Create([1, 2, 3], []);
        stream.Dispose();
        stream.Dispose();
        var withInner = new PrefixedStream(inner, ArrayPool<byte>.Shared.Rent(1), 0);
        withInner.Dispose();
        Assert.False(inner.CanRead);
    }
}
