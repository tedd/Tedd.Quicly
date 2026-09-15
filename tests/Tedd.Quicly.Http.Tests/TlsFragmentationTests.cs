using System.Buffers.Binary;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Http.Handlers;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Http.Tests;

/// <summary>End-to-end: a client whose ClientHello is spread over many small TLS records still gets SNI/ALPN-based selection.</summary>
public class TlsFragmentationTests
{
    /// <summary>Re-frames the first flight (the ClientHello record) into handshake records of <paramref name="fragment"/> bytes each.</summary>
    private sealed class FragmentingStream(Stream inner, int fragment) : Stream
    {
        private bool _first = true;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => inner.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => inner.ReadAsync(buffer, cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => WriteAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (!_first)
            {
                await inner.WriteAsync(buffer, cancellationToken);
                return;
            }
            _first = false;
            // The first write is one or more records; take the first handshake record and split its payload.
            var span = buffer.Span;
            Assert.Equal(0x16, span[0]);
            int recordLength = BinaryPrimitives.ReadUInt16BigEndian(span[3..]);
            var payload = buffer.Slice(5, recordLength);
            var rest = buffer[(5 + recordLength)..];
            for (int offset = 0; offset < payload.Length; offset += fragment)
            {
                int len = Math.Min(fragment, payload.Length - offset);
                var header = new byte[] { 0x16, 0x03, 0x01, (byte)(len >> 8), (byte)len };
                await inner.WriteAsync(header, cancellationToken);
                await inner.WriteAsync(payload.Slice(offset, len), cancellationToken);
                await inner.FlushAsync(cancellationToken);
                await Task.Yield(); // give the server a chance to observe separate reads
            }
            if (!rest.IsEmpty)
                await inner.WriteAsync(rest, cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }
    }

    private static async Task<SslStream> ConnectFragmentedAsync(TestHost host, string sni, int fragment, params string[] alpn)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        await socket.ConnectAsync(host.EndPoint);
        var ssl = new SslStream(new FragmentingStream(new NetworkStream(socket, ownsSocket: true), fragment), leaveInnerStreamOpen: false);
        var options = new SslClientAuthenticationOptions
        {
            TargetHost = sni,
            RemoteCertificateValidationCallback = static (_, _, _, _) => true,
            ApplicationProtocols = [],
        };
        foreach (var p in alpn)
            options.ApplicationProtocols.Add(new SslApplicationProtocol(p));
        await ssl.AuthenticateAsClientAsync(options);
        return ssl;
    }

    /// <remarks>SChannel itself rejects very small first records (a 4-byte fragment gets a ProtocolVersion alert), so fragments start at 17 here; our own assembler is tested down to 1 byte in <see cref="ClientHelloAssemblyTests"/>.</remarks>
    [Theory]
    [InlineData(17)]
    [InlineData(64)]
    [InlineData(200)]
    public async Task Fragmented_client_hello_still_selects_by_sni_and_alpn(int fragment)
    {
        using var normal = TestCertificates.CreateSelfSigned("site.example", "site.example");
        using var challenge = TestCertificates.CreateAcmeChallenge("site.example", "token.thumb");
        var responder = new TlsAlpn01Responder();
        await responder.PublishAsync("site.example", challenge, CancellationToken.None);
        var tls = new HttpTlsOptions { CertificateSource = new StaticCertificateSource(normal), Alpn01Responder = responder };
        await using var host = TestHost.Start(o => o.Use(new HealthHandler()), tls);

        using (var acme = await ConnectFragmentedAsync(host, "site.example", fragment, "acme-tls/1"))
        {
            Assert.Equal(TlsAlpn01Responder.AcmeTls1, acme.NegotiatedApplicationProtocol);
            Assert.Equal(challenge.Thumbprint, new X509Certificate2(acme.RemoteCertificate!).Thumbprint);
        }
        await HttpServerTests.WaitUntilAsync(() => host.Server.AcmeTlsAlpnHandshakes == 1);

        using (var regular = await ConnectFragmentedAsync(host, "site.example", fragment, "http/1.1"))
        {
            Assert.Equal(normal.Thumbprint, new X509Certificate2(regular.RemoteCertificate!).Thumbprint);
            await regular.WriteAsync("GET /healthz HTTP/1.1\r\nHost: site.example\r\n\r\n"u8.ToArray());
            var buffer = new byte[512];
            int n = await regular.ReadAsync(buffer);
            Assert.Contains("\r\n\r\nok", System.Text.Encoding.ASCII.GetString(buffer, 0, n), StringComparison.Ordinal);
        }
        Assert.Equal(0, host.Server.HandshakeFailures);
    }

    [Fact]
    public async Task Fragmented_then_garbage_and_oversized_hellos_are_closed()
    {
        using var cert = TestCertificates.CreateSelfSigned("localhost", "localhost");
        await using var host = TestHost.Start(o =>
        {
            o.Limits.HeaderReadTimeout = TimeSpan.FromSeconds(2);
            o.Use(new HealthHandler());
        }, HttpTlsOptions.FromCertificate(cert));

        // first record is a valid fragment, the second is not a handshake record
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync([0x16, 0x03, 0x01, 0x00, 0x02, 0x01, 0x00]);
            await raw.SendAsync([0x17, 0x03, 0x03, 0x00, 0x01, 0x00]);
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        }
        // a partial ClientHello followed by the client hanging up
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync([0x16, 0x03, 0x01, 0x00, 0x40, 0x01, 0x00, 0x00, 0x3C]);
            raw.Socket.Shutdown(SocketShutdown.Send);
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        }
        await HttpServerTests.WaitUntilAsync(() => host.Server.HandshakeFailures == 2);
        // a ClientHello announcing a 16 MiB body
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync([0x16, 0x03, 0x01, 0x00, 0x04, 0x01, 0xFF, 0xFF, 0xFF]);
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        }
        // records that keep coming but never complete the hello: MaxPeekBytes reached -> closed without waiting for the timeout
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            var record = new byte[5 + 16384];
            record[0] = 0x16; record[1] = 0x03; record[2] = 0x01; record[3] = 0x40; record[4] = 0x00;
            record[5] = 0x01; record[6] = 0x00; record[7] = 0xFF; record[8] = 0xF0; // 64 KiB - 16 body: fits the assembly buffer, so keep feeding records
            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                for (int i = 0; i < 8; i++)
                    await raw.SendAsync(record);
            }
            catch (IOException)
            {
                // the server may already have closed
            }
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(10)));
        }
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 0);
        Assert.Equal(4, host.Server.HandshakeFailures);
    }
}
