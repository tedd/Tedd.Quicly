using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using Tedd.Quicly.Http.Internal;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Http.Tests;

public class DebugTlsTests
{
    [Fact]
    public async Task Debug_prefixed_stream_handshake()
    {
        using var cert = TestCertificates.CreateSelfSigned("localhost", "localhost");
        using var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        listener.Listen(1);
        var ep = (IPEndPoint)listener.LocalEndPoint!;

        var clientTask = Task.Run(async () =>
        {
            var s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await s.ConnectAsync(ep);
            var ssl = new SslStream(new NetworkStream(s, true), false, (_, _, _, _) => true);
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions { TargetHost = "localhost", ApplicationProtocols = [SslApplicationProtocol.Http11] });
            return ssl.NegotiatedApplicationProtocol.ToString();
        });

        using var accepted = await listener.AcceptAsync();
        var network = new NetworkStream(accepted, true);
        var rented = ArrayPool<byte>.Shared.Rent(ClientHelloParser.RecordHeaderLength + ClientHelloParser.MaxRecordLength);
        int filled = 0, recordLength = 0;
        while (true)
        {
            if (filled >= 5)
            {
                if (recordLength == 0)
                    recordLength = ClientHelloParser.GetRecordLength(rented.AsSpan(0, filled));
                if (recordLength < 0 || filled >= recordLength)
                    break;
            }
            int n = await network.ReadAsync(rented.AsMemory(filled));
            if (n == 0) break;
            filled += n;
        }
        var parsed = ClientHelloParser.TryParse(rented.AsSpan(5, recordLength - 5), out var hello);
        var info = $"filled={filled} recordLength={recordLength} parsed={parsed} sni={hello.ServerName} alpn={string.Join(",", hello.ApplicationProtocols)}";

        var ssl = new SslStream(new PrefixedStream(network, rented, filled), false);
        var opts = new SslServerAuthenticationOptions
        {
            ServerCertificateSelectionCallback = (_, h) => cert,
            ApplicationProtocols = [SslApplicationProtocol.Http11],
        };
        using var cts = new CancellationTokenSource(5000);
        try
        {
            await ssl.AuthenticateAsServerAsync(opts, cts.Token);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(info + " -> " + ex);
        }
        Assert.Equal("http/1.1", await clientTask);
    }
}
