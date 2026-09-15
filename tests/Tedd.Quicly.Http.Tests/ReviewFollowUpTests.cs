using System.Diagnostics;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Http.Handlers;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Http.Tests;

/// <summary>Tests for the fixes made in response to the second review (see <see cref="ReviewFindingsTests"/> for the blocking findings).</summary>
public class ReviewFollowUpTests
{
    private static RouteTable WhoRoutes()
        => new RouteTable().MapGet("/who", (ctx, ct) => ctx.Response.SendTextAsync(ctx.Host ?? "(null)", cancellationToken: ct));

    [Fact]
    public async Task Absolute_form_authority_is_used_without_Host_and_userinfo_or_duplicate_Host_is_refused()
    {
        await using var host = TestHost.Start(o => o.Use(WhoRoutes()));
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync("GET http://only.example/who HTTP/1.1\r\n\r\n");
            Assert.Equal("only.example", (await raw.ReadResponseAsync()).Body);
        }
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync("GET http://t.example/who HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n");
            Assert.Equal(400, (await raw.ReadResponseAsync()).StatusCode);
        }
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync("GET http://user@evil.example/who HTTP/1.1\r\nHost: a\r\n\r\n");
            Assert.Equal(400, (await raw.ReadResponseAsync()).StatusCode);
        }
    }

    [Theory]
    [InlineData("/p?q", "/p?q")]
    [InlineData("http://h/p?q=1", "/p?q=1")]
    [InlineData("https://h:8080/a/b", "/a/b")]
    [InlineData("http://h", "/")]
    [InlineData("*", "/")]
    public void Redirect_target_keeps_path_and_query(string rawTarget, string expected)
        => Assert.Equal(expected, RedirectToHttpsHandler.ToOriginForm(rawTarget));

    [Fact]
    public async Task Maximal_request_preceded_by_tolerated_empty_lines_is_accepted()
    {
        await using var host = TestHost.Start(o =>
        {
            o.Limits.MaxRequestLineBytes = 64;
            o.Limits.MaxHeadersBytes = 64;
            o.Use(new HealthHandler());
        });
        string line = "GET /healthz?" + new string('q', 64 - "GET /healthz? HTTP/1.1".Length) + " HTTP/1.1";
        Assert.Equal(64, line.Length);
        string headers = "Host: a\r\n";
        headers += "X: " + new string('v', 64 - headers.Length - "X: \r\n\r\n".Length) + "\r\n\r\n";
        Assert.Equal(64, headers.Length);

        // baseline: exactly at both limits
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync(line + "\r\n" + headers);
            Assert.Equal(200, (await raw.ReadResponseAsync()).StatusCode);
        }

        // the same request after the two empty lines the parser tolerates, delivered so that the server sees more
        // than line + headers bytes before the terminator arrives
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            string request = "\r\n\r\n" + line + "\r\n" + headers;
            await raw.SendAsync(request[..^2]);
            await Task.Delay(150);
            await raw.SendAsync(request[^2..]);
            Assert.Equal(200, (await raw.ReadResponseAsync()).StatusCode);
        }
    }

    [Fact]
    public async Task Connection_setup_failure_is_contained_and_accepting_continues()
    {
        var errors = new List<Exception>();
        await using var host = TestHost.Start(o =>
        {
            o.OnError = e =>
            {
                lock (errors)
                    errors.Add(e);
                throw new InvalidOperationException("the error callback fails too");
            };
            o.Use(new HealthHandler());
        });
        int calls = 0;
        host.Server.BeforeConnectionCreated = _ =>
        {
            if (Interlocked.Increment(ref calls) == 1)
                throw new SocketException((int)SocketError.ConnectionReset);
        };

        using (var first = await RawClient.ConnectAsync(host.EndPoint))
            Assert.True(await first.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        await HttpServerTests.WaitUntilAsync(() =>
        {
            lock (errors)
                return errors.Count == 1;
        });
        lock (errors)
            Assert.IsType<SocketException>(errors[0]);
        Assert.Equal(0, host.Server.TrackedAddressCount); // the per-address reservation was undone
        Assert.Equal(0, host.Server.ActiveConnections);
        Assert.Equal(0, host.Server.TotalConnectionsAccepted);

        using var client = host.CreateClient();
        Assert.Equal("ok", await client.GetStringAsync("/healthz"));
        Assert.Equal(1, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Throwing_error_callback_does_not_break_the_500_or_shutdown()
    {
        var routes = new RouteTable().MapGet("/boom", (ctx, ct) => throw new InvalidOperationException("handler failed"));
        int reported = 0;
        var host = TestHost.Start(o =>
        {
            o.OnError = _ =>
            {
                Interlocked.Increment(ref reported);
                throw new InvalidOperationException("the error callback fails too");
            };
            o.Use(routes);
        });
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync("GET /boom HTTP/1.1\r\nHost: a\r\n\r\n");
            Assert.Equal(500, (await raw.ReadResponseAsync()).StatusCode);
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        }
        Assert.Equal(1, Volatile.Read(ref reported));
        await host.Server.StopAsync(); // no faulted connection task surfaces here
        await host.DisposeAsync();
    }

    [Fact]
    public void Client_hello_reader_is_linear_for_a_hello_trickled_as_one_byte_records()
    {
        var handshake = ClientHelloAssemblyTests.BuildHandshake("trickle.example", ["acme-tls/1", "http/1.1"], padding: 12_000);
        var raw = ClientHelloAssemblyTests.Fragment(handshake, 1);
        Assert.True(raw.Length <= ClientHelloParser.MaxPeekBytes);

        var reader = default(ClientHelloReader);
        try
        {
            var sw = Stopwatch.StartNew();
            var status = ClientHelloAssembleStatus.NeedMore;
            ClientHelloInfo hello = default;
            bool parsed = false;
            for (int n = 0; n <= raw.Length && status == ClientHelloAssembleStatus.NeedMore; n++)
                status = reader.Read(raw.AsSpan(0, n), out hello, out parsed);
            sw.Stop();

            Assert.Equal(ClientHelloAssembleStatus.Complete, status);
            Assert.True(parsed);
            Assert.Equal("trickle.example", hello.ServerName);
            Assert.True(hello.Offers("acme-tls/1"));
            // ~73 000 calls over ~12 000 records. Re-walking every record on every call (the previous design) is
            // ~450 million record visits; the resumable reader does one per record. The bound is deliberately loose.
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5), "reassembly took " + sw.Elapsed);
        }
        finally
        {
            reader.Dispose();
        }
    }

    [Fact]
    public void Client_hello_reader_grows_for_a_record_larger_than_its_buffer()
    {
        var handshake = ClientHelloAssemblyTests.BuildHandshake("grow.example", ["http/1.1"], padding: 3_000);
        // first record: 3 payload bytes (length still unknown), second record: everything else
        var raw = ClientHelloAssemblyTests.Fragment(handshake[..3], 3).Concat(ClientHelloAssemblyTests.Fragment(handshake[3..], 16_384)).ToArray();

        var reader = default(ClientHelloReader);
        try
        {
            Assert.Equal(ClientHelloAssembleStatus.NeedMore, reader.Read(raw.AsSpan(0, 8), out _, out _)); // first record only
            Assert.Equal(ClientHelloAssembleStatus.NeedMore, reader.Read(raw.AsSpan(0, raw.Length - 1), out _, out _));
            Assert.Equal(ClientHelloAssembleStatus.Complete, reader.Read(raw, out var hello, out bool parsed));
            Assert.True(parsed);
            Assert.Equal("grow.example", hello.ServerName);
        }
        finally
        {
            reader.Dispose();
            reader.Dispose(); // idempotent
        }
    }

    [Fact]
    public void Resumable_assembly_matches_one_shot_assembly_at_every_split()
    {
        var handshake = ClientHelloAssemblyTests.BuildHandshake("split.example", ["h2"], padding: 200);
        var raw = ClientHelloAssemblyTests.Fragment(handshake, 37);
        var scratch = new byte[ClientHelloParser.MaxClientHelloLength];
        for (int split = 0; split <= raw.Length; split++)
        {
            var state = default(ClientHelloAssemblyState);
            var first = ClientHelloParser.TryAssemble(raw.AsSpan(0, split), scratch, ref state, out _, out _);
            Assert.Equal(split == raw.Length ? ClientHelloAssembleStatus.Complete : ClientHelloAssembleStatus.NeedMore, first);
            var second = ClientHelloParser.TryAssemble(raw, scratch, ref state, out int length, out int recordBytes);
            Assert.Equal(ClientHelloAssembleStatus.Complete, second);
            Assert.Equal(handshake.Length, length);
            Assert.Equal(raw.Length, recordBytes);
            Assert.Equal(handshake, scratch[..length]);
        }
    }

    [Fact]
    public void Filesystem_root_can_be_the_static_root()
    {
        string root = Path.GetPathRoot(Path.GetTempPath())!;
        var handler = new StaticFileHandler(root);
        Assert.Equal(root, handler.RootDirectory);
        Assert.True(handler.TryResolve("/", out var full));
        Assert.Equal(root, full);
        Assert.True(handler.TryResolve("/./", out full));
        Assert.Equal(root, full);
        Assert.True(handler.TryResolve("/a/b.txt", out full));
        Assert.Equal(Path.Combine(root, "a", "b.txt"), full);
        Assert.False(handler.TryResolve("/../x", out _));
    }

    [Fact]
    public void Reloading_an_unchanged_certificate_file_keeps_the_instance()
    {
        using var cert = TestCertificates.CreateSelfSigned("same", "localhost");
        var path = Path.Combine(Path.GetTempPath(), "quicly-cert-" + Guid.NewGuid().ToString("N") + ".pfx");
        try
        {
            File.WriteAllBytes(path, TestCertificates.ExportPfx(cert, null));
            using var source = new FileCertificateSource(path);
            var loaded = source.Current!;
            int changes = 0;
            source.Changed += _ => changes++;

            File.WriteAllBytes(path, TestCertificates.ExportPfx(cert, null)); // rewritten, same certificate
            Assert.False(source.Reload());
            Assert.Same(loaded, source.Current);
            Assert.Equal(0, changes);
            Assert.True(loaded.HasPrivateKey);

            if (OperatingSystem.IsWindows())
            {
                // ADR 0009: the key is not imported as exportable
                using AsymmetricAlgorithm key = (AsymmetricAlgorithm?)loaded.GetRSAPrivateKey() ?? loaded.GetECDsaPrivateKey()!;
                Assert.ThrowsAny<CryptographicException>(() => key.ExportPkcs8PrivateKey());
            }
        }
        finally
        {
            File.Delete(path);
        }
    }
}
