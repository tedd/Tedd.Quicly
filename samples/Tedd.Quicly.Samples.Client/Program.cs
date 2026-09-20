using System.Diagnostics;
using System.Net;
using Tedd.Quicly.Client;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Transport.MsQuic;

namespace Tedd.Quicly.Samples.Client;

/// <summary>
/// Sends a bulk object to the server sample. Start the server first.
/// </summary>
public static class Program
{
    private const ushort FilesChannel = 5;
    private const int Port = 5001;

    public static async Task Main(string[] args)
    {
        // Default to something bigger than one transfer can carry, so the driver really does split it.
        long size = args.Length > 0 && long.TryParse(args[0], out long given) ? given : 64L * 1024 * 1024;

        ChannelTable channels = ChannelTable.Create()
            .Add(FilesChannel, "files", ChannelMode.Bulk)
            .Build();

        MsQuicTransportOptions transport = new()
        {
            // The server sample signs its own certificate, so there is nothing for the platform to chain to.
            // A real client leaves this at SystemRoots, or pins the SPKI it expects.
            ServerCertificateValidation = ServerCertificateValidationMode.Callback,
            ServerCertificateValidator = static (in ServerCertificateContext _) =>
                ServerCertificateDecision.AcceptIgnoringPlatformValidation,
        };

        using MsQuicTransportConnector connector = new(transport);
        using QuiclyClient client = new(connector);

        Console.WriteLine($"connecting to 127.0.0.1:{Port}");
        QuiclyPeer peer = await client.ConnectAsync(
            new IPEndPoint(IPAddress.Loopback, Port),
            new ClientOptions { Channels = channels, ServerName = "localhost" });
        Console.WriteLine("connected");

        // One call for the whole object, whatever its size. The source is read at absolute object offsets and never
        // buffered, so this would cost the same for 10 GB as it does here.
        BulkObjectDescriptor descriptor = new(FilesChannel, ObjectId: 42, ObjectVersion: 1, TotalLength: size);
        BulkObjectTransfer transfer = peer.BeginBulkObjectSend(
            in descriptor,
            new PatternSource(size),
            (in BulkObjectProgress p) => Console.Write($"\r  {p.Fraction:P0} ({p.RangesCompleted} ranges)"));

        Console.WriteLine($"sending {size:N0} bytes");
        Stopwatch clock = Stopwatch.StartNew();

        // Poll and flush at the host's own cadence; the driver starts each next range from Poll.
        while (!transfer.IsFinished)
        {
            client.Poll();
            client.Flush();
            await Task.Delay(1);
        }

        clock.Stop();
        BulkObjectResult result = transfer.Result;
        Console.WriteLine();
        Console.WriteLine($"  {result.Status}: {result.BytesTransferred:N0} bytes in {result.Ranges} ranges, {result.Retries} re-sent");
        if (result.Status == BulkStatus.Completed && clock.Elapsed.TotalSeconds > 0)
        {
            Console.WriteLine($"  {result.BytesTransferred / clock.Elapsed.TotalSeconds / (1024 * 1024):N1} MiB/s");
        }
    }
}

/// <summary>
/// Supplies an object's bytes without holding it: byte <c>n</c> is a function of <c>n</c>, so the sample moves a large
/// object without a large buffer. A real source reads a file at the offset it is given.
/// </summary>
/// <remarks>
/// Read is called on the game thread inside a scheduler pass, and may be asked for the same range twice — a stream start
/// the peer refuses makes the engine read it again — so it must be random access and repeatable.
/// </remarks>
internal sealed class PatternSource(long total) : IBulkSource
{
    public int Read(long offset, Span<byte> destination)
    {
        if (offset >= total)
        {
            return 0;
        }

        int take = (int)Math.Min(destination.Length, total - offset);
        for (int i = 0; i < take; i++)
        {
            destination[i] = (byte)((offset + i) * 131 >> 3);
        }

        return take;
    }
}
