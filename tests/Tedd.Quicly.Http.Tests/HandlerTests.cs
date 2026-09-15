using Tedd.Quicly.Acme.Challenges;
using System.Net;
using System.Net.Http.Headers;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Http.Handlers;

namespace Tedd.Quicly.Http.Tests;

public class Http01ChallengeHandlerTests
{
    private const string Tok = "tok-en_1_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    [Fact]
    public async Task Publish_serve_remove()
    {
        var handler = new Http01ChallengeHandler();
        IHttp01Responder responder = handler;
        await using var host = TestHost.Start(o => o.Use(handler));
        using var client = host.CreateClient();

        using var missing = await client.GetAsync(Http01ChallengeHandler.PathPrefix + Tok);
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);

        await responder.PublishAsync(Tok, Tok + ".thumbprint", CancellationToken.None);
        Assert.Equal(1, handler.Count);
        Assert.True(handler.TryGet(Tok, out var ka));
        Assert.Equal(Tok + ".thumbprint", ka);
        Assert.False(handler.TryGet("other", out ka));
        Assert.Equal(string.Empty, ka);

        using var found = await client.GetAsync(Http01ChallengeHandler.PathPrefix + Tok);
        Assert.Equal(HttpStatusCode.OK, found.StatusCode);
        Assert.Equal("application/octet-stream", found.Content.Headers.ContentType!.MediaType);
        Assert.Equal(Tok + ".thumbprint", await found.Content.ReadAsStringAsync());
        Assert.Equal("no-store", found.Headers.CacheControl!.ToString());

        using var head = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, Http01ChallengeHandler.PathPrefix + Tok));
        Assert.Equal(HttpStatusCode.OK, head.StatusCode);
        Assert.Equal(Tok.Length + ".thumbprint".Length, head.Content.Headers.ContentLength);

        using var post = await client.PostAsync(Http01ChallengeHandler.PathPrefix + Tok, new StringContent("x"));
        Assert.Equal(HttpStatusCode.MethodNotAllowed, post.StatusCode);

        using var badToken = await client.GetAsync(Http01ChallengeHandler.PathPrefix + "tok/en_aaaaaaaaaaaaaaaaaaaaaaaaaa");
        Assert.Equal(HttpStatusCode.NotFound, badToken.StatusCode);
        using var emptyToken = await client.GetAsync(Http01ChallengeHandler.PathPrefix);
        Assert.Equal(HttpStatusCode.NotFound, emptyToken.StatusCode);

        using var other = await client.GetAsync("/.well-known/other");
        Assert.Equal(HttpStatusCode.NotFound, other.StatusCode); // not ours: falls through to the server's 404

        await responder.RemoveAsync(Tok, CancellationToken.None);
        Assert.Equal(0, handler.Count);
        using var gone = await client.GetAsync(Http01ChallengeHandler.PathPrefix + Tok);
        Assert.Equal(HttpStatusCode.NotFound, gone.StatusCode);
    }

    [Fact]
    public async Task Validates_tokens()
    {
        var handler = new Http01ChallengeHandler();
        await Assert.ThrowsAsync<ArgumentException>(async () => await handler.PublishAsync("", "k", CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentException>(async () => await handler.PublishAsync("a/b" + Tok, "k", CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentException>(async () => await handler.PublishAsync(Tok, "", CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentException>(async () => await handler.PublishAsync("short-token-21-chars_", "k", CancellationToken.None)); // 21 chars
        await Assert.ThrowsAsync<ArgumentException>(async () => await handler.RemoveAsync("a b" + Tok, CancellationToken.None));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Http01ChallengeHandler(lifetime: TimeSpan.Zero));
        Assert.True(Http01ChallengeHandler.IsValidToken("abcdefghijklmnopqrstuv"));
        Assert.False(Http01ChallengeHandler.IsValidToken("abcdefghijklmnopqrstu"));
        Assert.False(Http01ChallengeHandler.IsValidToken("abcdefghijklmnopqrstu+"));
        Assert.Equal(Http01ChallengeHandler.DefaultLifetime, handler.Lifetime);
        Assert.Throws<ArgumentNullException>(() => handler.TryGet(null!, out _));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await handler.TryHandleAsync(null!, CancellationToken.None));
    }
}

public class HealthHandlerTests
{
    [Fact]
    public async Task Serves_ok()
    {
        await using var host = TestHost.Start(o => o.Use(new HealthHandler()).Use(new HealthHandler("/live")));
        using var client = host.CreateClient();
        using var response = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("ok", await response.Content.ReadAsStringAsync());
        Assert.Equal("text/plain", response.Content.Headers.ContentType!.MediaType);
        Assert.Equal("ok", await client.GetStringAsync("/live"));
        using var post = await client.PostAsync("/healthz", new StringContent(""));
        Assert.Equal(HttpStatusCode.MethodNotAllowed, post.StatusCode);
        Assert.Equal("GET, HEAD", string.Join(", ", post.Content.Headers.Allow));
        using var other = await client.GetAsync("/healthzz");
        Assert.Equal(HttpStatusCode.NotFound, other.StatusCode);
        Assert.Equal("/healthz", new HealthHandler().Path);
        Assert.Throws<ArgumentException>(() => new HealthHandler(""));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await new HealthHandler().TryHandleAsync(null!, CancellationToken.None));
    }
}

public class RedirectToHttpsHandlerTests
{
    [Fact]
    public async Task Redirects_plain_requests()
    {
        await using var host = TestHost.Start(o => o.Use(new RedirectToHttpsHandler()).Use(new HealthHandler()));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET /path?q=1 HTTP/1.1\r\nHost: example.com:8080\r\n\r\n");
        var r = await raw.ReadResponseAsync();
        Assert.Equal(301, r.StatusCode);
        Assert.Equal("https://example.com/path?q=1", r["Location"]);
        Assert.Equal("0", r["Content-Length"]);

        await raw.SendAsync("GET /x HTTP/1.1\r\nHost: [::1]:80\r\n\r\n");
        Assert.Equal("https://[::1]/x", (await raw.ReadResponseAsync())["Location"]);
        // Hosts that are not a plain name or IP literal are refused rather than echoed into Location.
        foreach (var hostile in new[] { "[::1", "user@evil.example", "evil.example/x", "a%2fb", "h:80x", "h:123456" })
        {
            await raw.SendAsync("GET /x HTTP/1.1\r\nHost: " + hostile + "\r\n\r\n");
            var refused = await raw.ReadResponseAsync();
            Assert.Equal(400, refused.StatusCode);
            Assert.Null(refused["Location"]);
        }

        await raw.SendAsync("GET http://abs.example/abs?x=1 HTTP/1.1\r\n\r\n");
        Assert.Equal("https://abs.example/abs?x=1", (await raw.ReadResponseAsync())["Location"]); // absolute-form targets keep path and query
        await raw.SendAsync("GET http://abs.example HTTP/1.1\r\nHost: ignored.example\r\n\r\n");
        Assert.Equal("https://abs.example/", (await raw.ReadResponseAsync())["Location"]); // the authority replaces Host

        await raw.SendAsync("GET / HTTP/1.0\r\n\r\n");
        var noHost = await raw.ReadResponseAsync();
        Assert.Equal(400, noHost.StatusCode);
    }

    [Fact]
    public async Task Temporary_and_custom_port()
    {
        await using var host = TestHost.Start(o => o.Use(new RedirectToHttpsHandler(8443, permanent: false)));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("POST /p HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\n\r\n");
        var r = await raw.ReadResponseAsync();
        Assert.Equal(307, r.StatusCode);
        Assert.Equal("https://h:8443/p", r["Location"]);
        Assert.Throws<ArgumentOutOfRangeException>(() => new RedirectToHttpsHandler(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RedirectToHttpsHandler(70000));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await new RedirectToHttpsHandler().TryHandleAsync(null!, CancellationToken.None));
    }

    [Fact]
    public async Task Leaves_secure_requests_alone()
    {
        using var cert = TestCertificates.CreateSelfSigned("localhost", "localhost");
        await using var host = TestHost.Start(o => o.Use(new RedirectToHttpsHandler()).Use(new HealthHandler()), HttpTlsOptions.FromCertificate(cert));
        using var client = host.CreateClient();
        Assert.Equal("ok", await client.GetStringAsync("/healthz"));
    }
}

public class RouteTableTests
{
    [Fact]
    public async Task Validates_registrations()
    {
        var t = new RouteTable();
        Assert.Throws<ArgumentException>(() => t.Map("GET", "", (_, _) => default));
        Assert.Throws<ArgumentException>(() => t.Map("GET", "nope", (_, _) => default));
        Assert.Throws<ArgumentNullException>(() => t.Map("GET", "/", null!));
        Assert.Throws<ArgumentNullException>(() => t.MapPrefix("GET", "/", null!));
        Assert.Throws<ArgumentException>(() => t.MapPrefix("GET", "x", (_, _) => default));
        Assert.Throws<ArgumentException>(() => t.Map(" ", "/", (_, _) => default));
        Assert.Equal(0, t.Count);
        for (int i = 0; i < 20; i++)
        {
            t.Map("get", "/" + i, (_, _) => default);
            t.MapPrefix(null, "/p" + new string('x', i % 5), (_, _) => default);
        }
        Assert.Equal(40, t.Count);
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await t.TryHandleAsync(null!, CancellationToken.None));
    }

    [Fact]
    public async Task Longest_prefix_wins_and_exact_beats_prefix()
    {
        var t = new RouteTable()
            .MapPrefix(null, "/a/", (ctx, ct) => ctx.Response.SendTextAsync("short", cancellationToken: ct))
            .MapPrefix(null, "/a/b/", (ctx, ct) => ctx.Response.SendTextAsync("long", cancellationToken: ct))
            .Map(null, "/a/b/c", (ctx, ct) => ctx.Response.SendTextAsync("exact", cancellationToken: ct));
        await using var host = TestHost.Start(o => o.Use(t));
        using var client = host.CreateClient();
        Assert.Equal("short", await client.GetStringAsync("/a/x"));
        Assert.Equal("long", await client.GetStringAsync("/a/b/x"));
        Assert.Equal("exact", await client.GetStringAsync("/a/b/c"));
        using var nf = await client.GetAsync("/b");
        Assert.Equal(HttpStatusCode.NotFound, nf.StatusCode);
    }
}

public class StaticFileHandlerTests : IDisposable
{
    private readonly string _root;

    public StaticFileHandlerTests()
    {
        _root = Path.Combine(Path.GetTempPath(), "quicly-http-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(_root, "sub"));
        File.WriteAllText(Path.Combine(_root, "index.html"), "<h1>root</h1>");
        File.WriteAllText(Path.Combine(_root, "app.js"), "console.log(1)");
        File.WriteAllText(Path.Combine(_root, "style.css"), "body{}");
        File.WriteAllText(Path.Combine(_root, "data.json"), "{}");
        File.WriteAllBytes(Path.Combine(_root, "mod.wasm"), [0, 0x61, 0x73, 0x6d]);
        File.WriteAllText(Path.Combine(_root, "sub", "index.html"), "sub index");
        File.WriteAllText(Path.Combine(_root, "sub", "file.unknownext"), "?");
        File.WriteAllBytes(Path.Combine(_root, "big.bin"), new byte[150_000]);
        File.WriteAllText(Path.Combine(Path.GetTempPath(), "quicly-outside-" + Path.GetFileName(_root) + ".txt"), "secret");
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
            File.Delete(Path.Combine(Path.GetTempPath(), "quicly-outside-" + Path.GetFileName(_root) + ".txt"));
        }
        catch (IOException)
        {
        }
    }

    [Fact]
    public async Task Serves_files_with_content_types_and_conditional_requests()
    {
        var handler = new StaticFileHandler(_root, new StaticFileOptions { CacheControl = "public, max-age=60", DefaultContentType = "application/octet-stream" });
        Assert.Equal(Path.GetFullPath(_root), handler.RootDirectory);
        await using var host = TestHost.Start(o => o.Use(handler));
        using var client = host.CreateClient();

        using var js = await client.GetAsync("/app.js");
        Assert.Equal(HttpStatusCode.OK, js.StatusCode);
        Assert.Equal("text/javascript", js.Content.Headers.ContentType!.MediaType);
        Assert.Equal("console.log(1)", await js.Content.ReadAsStringAsync());
        Assert.Equal("public, max-age=60", js.Headers.CacheControl!.ToString());
        Assert.NotNull(js.Headers.ETag);
        Assert.NotNull(js.Content.Headers.LastModified);

        Assert.Equal("text/css", (await client.GetAsync("/style.css")).Content.Headers.ContentType!.MediaType);
        Assert.Equal("application/json", (await client.GetAsync("/data.json")).Content.Headers.ContentType!.MediaType);
        Assert.Equal("application/wasm", (await client.GetAsync("/mod.wasm")).Content.Headers.ContentType!.MediaType);
        Assert.Equal("text/html", (await client.GetAsync("/")).Content.Headers.ContentType!.MediaType);
        Assert.Equal("<h1>root</h1>", await client.GetStringAsync("/index.html"));
        Assert.Equal("sub index", await client.GetStringAsync("/sub/"));
        Assert.Equal("sub index", await client.GetStringAsync("/sub"));
        Assert.Equal("application/octet-stream", (await client.GetAsync("/sub/file.unknownext")).Content.Headers.ContentType!.MediaType);
        Assert.Equal(150_000, (await client.GetByteArrayAsync("/big.bin")).Length);

        // If-None-Match
        var etag = js.Headers.ETag!.Tag;
        var req = new HttpRequestMessage(HttpMethod.Get, "/app.js");
        req.Headers.IfNoneMatch.Add(new EntityTagHeaderValue(etag));
        using var notModified = await client.SendAsync(req);
        Assert.Equal(HttpStatusCode.NotModified, notModified.StatusCode);
        Assert.Equal(etag, notModified.Headers.ETag!.Tag);

        req = new HttpRequestMessage(HttpMethod.Get, "/app.js");
        req.Headers.IfNoneMatch.Add(new EntityTagHeaderValue("\"other\""));
        using var modified = await client.SendAsync(req);
        Assert.Equal(HttpStatusCode.OK, modified.StatusCode);

        req = new HttpRequestMessage(HttpMethod.Get, "/app.js");
        req.Headers.IfNoneMatch.Add(EntityTagHeaderValue.Any);
        using var any = await client.SendAsync(req);
        Assert.Equal(HttpStatusCode.NotModified, any.StatusCode);

        // If-Modified-Since
        req = new HttpRequestMessage(HttpMethod.Get, "/app.js");
        req.Headers.IfModifiedSince = js.Content.Headers.LastModified;
        using var ims = await client.SendAsync(req);
        Assert.Equal(HttpStatusCode.NotModified, ims.StatusCode);

        req = new HttpRequestMessage(HttpMethod.Get, "/app.js");
        req.Headers.IfModifiedSince = js.Content.Headers.LastModified!.Value.AddHours(-1);
        using var imsOld = await client.SendAsync(req);
        Assert.Equal(HttpStatusCode.OK, imsOld.StatusCode);

        req = new HttpRequestMessage(HttpMethod.Get, "/app.js");
        req.Headers.TryAddWithoutValidation("If-Modified-Since", "garbage");
        using var imsBad = await client.SendAsync(req);
        Assert.Equal(HttpStatusCode.OK, imsBad.StatusCode);

        // HEAD
        using var head = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/app.js"));
        Assert.Equal(HttpStatusCode.OK, head.StatusCode);
        Assert.Equal(14, head.Content.Headers.ContentLength);
        Assert.Empty(await head.Content.ReadAsByteArrayAsync());

        // methods
        using var post = await client.PostAsync("/app.js", new StringContent("x"));
        Assert.Equal(HttpStatusCode.MethodNotAllowed, post.StatusCode);

        // missing
        using var missing = await client.GetAsync("/nope.txt");
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
        using var missingDir = await client.GetAsync("/sub/nothing/");
        Assert.Equal(HttpStatusCode.NotFound, missingDir.StatusCode);
    }

    [Theory]
    [InlineData("/../outside.txt")]
    [InlineData("/sub/../../outside.txt")]
    [InlineData("/..%2Foutside.txt")]
    [InlineData("/%2e%2e/outside.txt")]
    [InlineData("/sub\\..\\..\\x")]
    [InlineData("/c:/windows/win.ini")]
    [InlineData("/app.js:$DATA")]
    [InlineData("/app.js.")]
    [InlineData("/app.js%20")]
    [InlineData("/%00")]
    [InlineData("/app.js%0a")]
    [InlineData("/a*b")]
    public async Task Traversal_and_unsafe_paths_are_rejected(string path)
    {
        var handler = new StaticFileHandler(_root);
        await using var host = TestHost.Start(o => o.Use(handler));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET " + path + " HTTP/1.1\r\nHost: a\r\n\r\n");
        var response = await raw.ReadResponseAsync();
        Assert.True(response.StatusCode is 400 or 404, "status " + response.StatusCode);
    }

    [Fact]
    public void TryResolve_normalises()
    {
        var handler = new StaticFileHandler(_root);
        Assert.True(handler.TryResolve("/", out var p));
        Assert.Equal(handler.RootDirectory, p);
        Assert.True(handler.TryResolve("/./sub//index.html", out p));
        Assert.Equal(Path.Combine(handler.RootDirectory, "sub", "index.html"), p);
        Assert.False(handler.TryResolve("relative", out _));
        Assert.False(handler.TryResolve("", out _));
        Assert.False(handler.TryResolve("/..", out _));
        Assert.False(handler.TryResolve("/a/../..", out _));
        Assert.Throws<ArgumentNullException>(() => handler.TryResolve(null!, out _));
        Assert.Throws<ArgumentNullException>(() => handler.GetContentType(null!));
        Assert.Equal("text/html; charset=utf-8", handler.GetContentType("x.HTML"));
        Assert.Null(handler.GetContentType("noext"));
        Assert.Null(handler.GetContentType("x.unknownext"));
        Assert.Same(handler.Options.ContentTypes, handler.Options.ContentTypes);
    }

    [Fact]
    public async Task Prefix_mount_and_unknown_types_refused()
    {
        var options = new StaticFileOptions { RequestPathPrefix = "/static", DefaultContentType = null };
        options.DefaultFileNames.Clear();
        options.DefaultFileNames.Add("missing.html");
        options.DefaultFileNames.Add("index.html");
        var handler = new StaticFileHandler(_root, options);
        Assert.False(handler.TryResolve("/staticx/app.js", out _));
        Assert.True(handler.TryResolve("/static", out var root));
        Assert.Equal(handler.RootDirectory, root);
        Assert.True(handler.TryResolve("/static/app.js", out _));

        await using var host = TestHost.Start(o => o.Use(handler));
        using var client = host.CreateClient();
        Assert.Equal("console.log(1)", await client.GetStringAsync("/static/app.js"));
        Assert.Equal("<h1>root</h1>", await client.GetStringAsync("/static/"));
        Assert.Equal("<h1>root</h1>", await client.GetStringAsync("/static"));
        using var unknown = await client.GetAsync("/static/sub/file.unknownext");
        Assert.Equal(HttpStatusCode.NotFound, unknown.StatusCode);
        using var outside = await client.GetAsync("/app.js");
        Assert.Equal(HttpStatusCode.NotFound, outside.StatusCode);

        var slashPrefix = new StaticFileHandler(_root, new StaticFileOptions { RequestPathPrefix = "/s/" });
        Assert.True(slashPrefix.TryResolve("/s/app.js", out _));
        Assert.False(slashPrefix.TryResolve("/s", out _));
    }

    [Fact]
    public void Constructor_validation()
    {
        Assert.Throws<ArgumentException>(() => new StaticFileHandler(""));
        Assert.Throws<DirectoryNotFoundException>(() => new StaticFileHandler(Path.Combine(_root, "does-not-exist")));
        Assert.Throws<ArgumentException>(() => new StaticFileHandler(_root, new StaticFileOptions { RequestPathPrefix = "" }));
        Assert.Throws<ArgumentException>(() => new StaticFileHandler(_root, new StaticFileOptions { RequestPathPrefix = "static" }));
        var handler = new StaticFileHandler(_root + Path.DirectorySeparatorChar);
        Assert.Equal(Path.GetFullPath(_root), handler.RootDirectory);
    }

    [Fact]
    public async Task File_vanishing_between_stat_and_open_falls_through()
    {
        var path = Path.Combine(_root, "vanish.txt");
        File.WriteAllText(path, "x");
        var handler = new VanishingHandler(_root, path);
        await using var host = TestHost.Start(o => o.Use(handler));
        using var client = host.CreateClient();
        using var response = await client.GetAsync("/vanish.txt");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await new StaticFileHandler(_root).TryHandleAsync(null!, CancellationToken.None));
    }

    /// <summary>Deletes the target file right before the static handler opens it, exercising the open-failure path.</summary>
    private sealed class VanishingHandler(string root, string path) : IHttpHandler
    {
        private readonly StaticFileHandler _inner = new(root);

        public ValueTask<bool> TryHandleAsync(HttpRequestContext context, CancellationToken cancellationToken)
        {
            // Make the file a directory-less dangling name: exists for the FileInfo check, then gone on open.
            File.Delete(path);
            Directory.CreateDirectory(path); // same name now resolves to a directory without default files -> open fails / not served
            return _inner.TryHandleAsync(context, cancellationToken);
        }
    }
}
