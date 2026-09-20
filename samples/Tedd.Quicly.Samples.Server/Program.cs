using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Win32.SafeHandles;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Server;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Transport.MsQuic;

namespace Tedd.Quicly.Samples.Server;

/// <summary>
/// Receives a bulk object of any size and writes it to disk. Run this, then the client sample.
/// </summary>
public static class Program
{
    private const ushort FilesChannel = 5;
    private const int Port = 5001;

    public static async Task Main()
    {
        string directory = Path.Combine(Path.GetTempPath(), "quicly-sample");
        Directory.CreateDirectory(directory);

        // Both ends must declare the same table; the handshake compares a hash of it.
        ChannelTable channels = ChannelTable.Create()
            .Add(FilesChannel, "files", ChannelMode.Bulk)
            .Build();

        using X509Certificate2 certificate = DevelopmentCertificate();
        ServerOptions options = new()
        {
            Channels = channels,
            Certificate = ServerCertificateOptions.Static(certificate),
        };

        // Asked once per object rather than once per transfer, however many ranges the object takes.
        options.PeerOptions.BulkObjectRouter = new FileRouter(directory);

        using MsQuicTransportListener listener = new(new IPEndPoint(IPAddress.Loopback, Port), certificate);
        await using QuiclyServer server = new(options, listener);
        server.PeerAdmitted += peer => Console.WriteLine($"peer {peer.Index} admitted");
        await server.StartAsync();
        Console.WriteLine($"listening on 127.0.0.1:{Port}, writing objects to {directory}");
        Console.WriteLine("press Ctrl+C to stop");

        using CancellationTokenSource stopping = new();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            stopping.Cancel();
        };

        // The host owns the cadence: poll delivers what arrived, flush sends what is queued.
        while (!stopping.IsCancellationRequested)
        {
            server.PollAll();
            server.FlushAll();
            try
            {
                await Task.Delay(1, stopping.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        Console.WriteLine("stopped");
    }

    /// <summary>A throwaway self-signed certificate, so the sample needs no setup. A real server uses ACME or its own PFX.</summary>
    private static X509Certificate2 DevelopmentCertificate()
    {
        using RSA key = RSA.Create(2048);
        CertificateRequest request = new("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], false)); // server authentication

        SubjectAlternativeNameBuilder names = new();
        names.AddDnsName("localhost");
        names.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(names.Build());

        using X509Certificate2 created = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));

        // Schannel needs the key to be persisted with the certificate, which a PFX round-trip does.
        return X509CertificateLoader.LoadPkcs12(created.Export(X509ContentType.Pfx), password: null);
    }
}

/// <summary>Sends every object to a file named after its id and version.</summary>
internal sealed class FileRouter(string directory) : IBulkObjectRouter
{
    public BulkObjectReceiveDecision SelectTarget(in BulkObjectInfo info)
    {
        string path = Path.Combine(directory, $"object-{info.ObjectId}-v{info.ObjectVersion}.bin");
        Console.WriteLine($"receiving object {info.ObjectId} ({info.TotalLength:N0} bytes) into {path}");
        return BulkObjectReceiveDecision.Accept(
            new FileSink(path, info.TotalLength),
            progress: (in BulkObjectProgress p) => Console.Write($"\r  {p.Fraction:P0}"));
    }
}

/// <summary>
/// Writes an object's bytes to a file at their absolute offsets.
/// </summary>
/// <remarks>
/// <b>Write runs on the transport thread</b>, whose budget is no blocking and no allocation — and a file write blocks.
/// That is tolerable in a sample and wrong in a server: hand the bytes to a writer thread instead. What is <em>not</em>
/// negotiable is the random access. A range that fails its checksum is re-sent, and the retry overwrites the same
/// offsets, so a sink that appended rather than seeking would corrupt the object.
/// </remarks>
internal sealed class FileSink : IBulkObjectSink, IDisposable
{
    private readonly SafeFileHandle _file;

    public FileSink(string path, long totalLength)
    {
        _file = File.OpenHandle(path, FileMode.Create, FileAccess.Write, FileShare.Read, FileOptions.None, totalLength);
    }

    public void Write(long objectOffset, ReadOnlySpan<byte> data) => RandomAccess.Write(_file, data, objectOffset);

    public void Finish(in BulkObjectResult result)
    {
        Console.WriteLine();
        Console.WriteLine($"  {result.Status}: {result.BytesTransferred:N0} bytes in {result.Ranges} ranges, {result.Retries} re-sent");
        Dispose();
    }

    public void Dispose() => _file.Dispose();
}
