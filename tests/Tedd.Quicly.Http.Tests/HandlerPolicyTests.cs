using System.Net;
using Tedd.Quicly.Http.Handlers;

namespace Tedd.Quicly.Http.Tests;

/// <summary>A manually advanced clock for expiry tests.</summary>
internal sealed class FakeTimeProvider : TimeProvider
{
    private long _timestamp = 1_000_000;

    public override long TimestampFrequency => 1000;

    public override long GetTimestamp() => Volatile.Read(ref _timestamp);

    public void Advance(TimeSpan by) => Interlocked.Add(ref _timestamp, (long)by.TotalMilliseconds);
}

public class Http01ChallengeExpiryTests
{
    private const string Tok = "expiring-token_0123456789abcdef";

    [Fact]
    public async Task Entries_expire_after_lifetime()
    {
        var clock = new FakeTimeProvider();
        var handler = new Http01ChallengeHandler(clock, TimeSpan.FromMinutes(10));
        Assert.Equal(TimeSpan.FromMinutes(10), handler.Lifetime);
        await using var host = TestHost.Start(o => o.Use(handler));
        using var client = host.CreateClient();

        await handler.PublishAsync(Tok, "ka", CancellationToken.None);
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync(Http01ChallengeHandler.PathPrefix + Tok)).StatusCode);

        clock.Advance(TimeSpan.FromMinutes(9));
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync(Http01ChallengeHandler.PathPrefix + Tok)).StatusCode);

        clock.Advance(TimeSpan.FromMinutes(1));
        Assert.Equal(1, handler.Count); // lazily purged
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync(Http01ChallengeHandler.PathPrefix + Tok)).StatusCode);
        Assert.Equal(0, handler.Count); // the lookup removed the stale entry
        Assert.False(handler.TryGet(Tok, out _));

        // publishing purges every other expired entry
        await handler.PublishAsync(Tok + "a", "ka", CancellationToken.None);
        await handler.PublishAsync(Tok + "b", "kb", CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(11));
        Assert.Equal(2, handler.Count);
        await handler.PublishAsync(Tok + "c", "kc", CancellationToken.None);
        Assert.Equal(1, handler.Count);
        Assert.True(handler.TryGet(Tok + "c", out var kc));
        Assert.Equal("kc", kc);

        // re-publishing refreshes the deadline
        clock.Advance(TimeSpan.FromMinutes(9));
        await handler.PublishAsync(Tok + "c", "kc2", CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(9));
        Assert.True(handler.TryGet(Tok + "c", out kc));
        Assert.Equal("kc2", kc);
    }
}

public class StaticFilePolicyTests : IDisposable
{
    private readonly string _root;

    public StaticFilePolicyTests()
    {
        _root = Path.Combine(Path.GetTempPath(), "quicly-http-policy-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(_root, "sub"));
        File.WriteAllText(Path.Combine(_root, "index.html"), "<h1>root</h1>");
        File.WriteAllText(Path.Combine(_root, "small.txt"), "small");
        File.WriteAllBytes(Path.Combine(_root, "large.bin.txt"), new byte[10_000]);
        File.WriteAllText(Path.Combine(_root, "sub", "file.unknownext"), "?");
        File.WriteAllText(Path.Combine(_root, "noext"), "?");
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    [Fact]
    public async Task Unknown_extensions_are_refused_by_default()
    {
        var handler = new StaticFileHandler(_root);
        Assert.Null(handler.Options.DefaultContentType);
        await using var host = TestHost.Start(o => o.Use(handler));
        using var client = host.CreateClient();
        Assert.Equal("small", await client.GetStringAsync("/small.txt"));
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/sub/file.unknownext")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/noext")).StatusCode);
    }

    [Fact]
    public async Task Files_over_max_size_are_not_served()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new StaticFileHandler(_root, new StaticFileOptions { MaxFileSizeBytes = -1 }));
        var handler = new StaticFileHandler(_root, new StaticFileOptions { MaxFileSizeBytes = 100 });
        await using var host = TestHost.Start(o => o.Use(handler));
        using var client = host.CreateClient();
        Assert.Equal("small", await client.GetStringAsync("/small.txt"));
        using var large = await client.GetAsync("/large.bin.txt");
        Assert.Equal(HttpStatusCode.NotFound, large.StatusCode);
        Assert.Equal(64L * 1024 * 1024, new StaticFileOptions().MaxFileSizeBytes);
    }

    [Fact]
    public async Task Symbolic_links_are_refused()
    {
        var outside = Path.Combine(Path.GetTempPath(), "quicly-outside-" + Path.GetFileName(_root));
        Directory.CreateDirectory(outside);
        File.WriteAllText(Path.Combine(outside, "secret.txt"), "secret");
        string linkFile = Path.Combine(_root, "link.txt");
        string linkDir = Path.Combine(_root, "linkdir");
        try
        {
            try
            {
                File.CreateSymbolicLink(linkFile, Path.Combine(outside, "secret.txt"));
                Directory.CreateSymbolicLink(linkDir, outside);
            }
            catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
            {
                Assert.Skip("Creating symbolic links requires elevated privileges on this machine: " + ex.Message);
                return;
            }

            var handler = new StaticFileHandler(_root);
            Assert.True(handler.ContainsReparsePoint(linkFile));
            Assert.True(handler.ContainsReparsePoint(Path.Combine(linkDir, "secret.txt")));
            Assert.False(handler.ContainsReparsePoint(Path.Combine(_root, "small.txt")));
            Assert.False(handler.ContainsReparsePoint(Path.Combine(_root, "sub")));
            Assert.False(handler.ContainsReparsePoint(Path.Combine(_root, "does-not-exist.txt")));
            Assert.Throws<ArgumentNullException>(() => handler.ContainsReparsePoint(null!));

            await using var host = TestHost.Start(o => o.Use(handler));
            using var client = host.CreateClient();
            Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/link.txt")).StatusCode);
            Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/linkdir/secret.txt")).StatusCode);
            Assert.Equal("small", await client.GetStringAsync("/small.txt"));
        }
        finally
        {
            try
            {
                File.Delete(linkFile);
                Directory.Delete(linkDir);
                Directory.Delete(outside, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
