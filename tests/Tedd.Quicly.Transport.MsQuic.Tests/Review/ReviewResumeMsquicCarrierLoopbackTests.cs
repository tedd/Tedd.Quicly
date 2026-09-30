using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Adversarial review of 8107fc3, lens "msquic": the WebTransport carrier over real MsQuic loopback with a receiver that
/// holds. The carrier's conformance suite runs over the simulator only; here the carrier sits on the transport the commit
/// changed, so the bytes the inner transport skips and the offsets the carrier shifts by its preamble have to line up.
/// </summary>
[Collection(MsQuicCollection.Name)]
public sealed unsafe class ReviewResumeMsquicCarrierLoopbackTests : IDisposable
{
    private readonly TestRegistration _registration = new("quicly-review-resume-carrier");
    private readonly X509Certificate2 _certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
    private readonly List<IDisposable> _owned = [];
    private ITransport? _client;
    private ITransport? _server;

    private sealed class HoldingSink : NullTransportSink
    {
        private readonly Random _random = new(3);
        public ITransport? Transport;
        public int Connected;
        public long Next;
        public int Errors;
        public string? FirstError;
        public int Holds;
        public int Calls;
        public int Shutdowns;
        public int Completes;
        public bool FinSeen;
        public int Signal;
        public int Credit;
        public TransportStreamId Held;

        public override void OnConnected(in TransportConnectedInfo info) => Interlocked.Increment(ref Connected);

        private void Error(string message)
        {
            Errors++;
            FirstError ??= message;
        }

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            int total = 0;
            for (int i = 0; i < segments.Length; i++) total += (int)segments[i].Length;
            Calls++;
            if ((long)absoluteOffset != Next) Error($"indication {Calls} at offset {absoluteOffset}, the sink stopped at {Next} ({total} bytes, fin {fin})");
            if (FinSeen && total == 0) Error("the FIN was shown again with nothing new");
            int r = _random.Next(100);
            int consumed;
            bool hold = true;
            int credit = 0;
            if (total == 0)
            {
                consumed = 0;
                hold = r < 50;
            }
            else if (r < 25)
            {
                consumed = total;
                hold = false;
            }
            else if (r < 50)
            {
                consumed = total;
            }
            else if (r < 75)
            {
                consumed = 0;
            }
            else
            {
                consumed = _random.Next(0, total + 1);
                credit = _random.Next(0, total - consumed + 1);
            }

            int take = consumed + credit;
            long position = (long)absoluteOffset;
            int left = take;
            for (int i = 0; i < segments.Length && left > 0; i++)
            {
                ReadOnlySpan<byte> span = segments[i].AsSpan();
                int n = Math.Min(span.Length, left);
                for (int k = 0; k < n; k++)
                {
                    if (span[k] != ReviewResumeMsquicStressTests.PatternAt(position + k, 9))
                    {
                        Error($"wrong byte at offset {position + k}");
                        break;
                    }
                }

                position += n;
                left -= n;
            }

            Next = (long)absoluteOffset + take;
            if (fin) FinSeen = true;
            if (!hold) return ReceiveResult.Consumed(consumed);
            Holds++;
            Held = id;
            Volatile.Write(ref Credit, credit);
            Volatile.Write(ref Signal, 1);
            return ReceiveResult.PendingAfter(consumed);
        }

        public override void OnStreamPeerSendShutdown(TransportStreamId id) => Interlocked.Increment(ref Shutdowns);

        public override void OnStreamShutdownComplete(TransportStreamId id)
        {
            Interlocked.Increment(ref Completes);
            Transport?.CloseStream(id);
        }
    }

    private sealed class ClosingSink : NullTransportSink
    {
        public ITransport? Transport;
        public int Connected;

        public override void OnConnected(in TransportConnectedInfo info) => Interlocked.Increment(ref Connected);

        public override void OnStreamShutdownComplete(TransportStreamId id) => Transport?.CloseStream(id);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(3 * 1024 * 1024)]
    public void A_stream_through_the_carrier_survives_a_receiver_that_holds(int length)
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        var clientSink = new ClosingSink();
        var serverSink = new HoldingSink();
        WebTransportListener listener = WebTransportListener.CreateMsQuic(
            new IPEndPoint(IPAddress.Loopback, 0),
            _certificate,
            new MsQuicTransportOptions { ServerPeerBidiStreamCount = 16, ServerPeerUnidiStreamCount = 16 },
            null,
            _registration.Registration);
        _owned.Add(listener);
        listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            _server = transport;
            serverSink.Transport = transport;
            return serverSink;
        });
        WebTransportConnector connector = WebTransportConnector.CreateMsQuic(
            new MsQuicTransportOptions
            {
                ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
                PinnedSpkiSha256 = [SpkiPin.Compute(_certificate)],
                ClientPeerBidiStreamCount = 16,
                ClientPeerUnidiStreamCount = 16,
            },
            null,
            _registration.Registration);
        _owned.Add(connector);
        _client = connector.Connect(listener.LocalEndPoint, "localhost", clientSink);
        clientSink.Transport = _client;
        Assert.True(Spin.Until(() => Volatile.Read(ref clientSink.Connected) == 1 && Volatile.Read(ref serverSink.Connected) == 1, TimeSpan.FromSeconds(10)), "the session");

        int stop = 0;
        var resumer = new Thread(() =>
        {
            var random = new Random(8);
            while (Volatile.Read(ref stop) == 0)
            {
                if (Interlocked.Exchange(ref serverSink.Signal, 0) == 0)
                {
                    Thread.Yield();
                    continue;
                }

                int spins = random.Next(0, 60);
                for (int i = 0; i < spins; i++) Thread.SpinWait(1);
                _server!.ResumeStreamReceive(serverSink.Held, Volatile.Read(ref serverSink.Credit));
            }
        })
        {
            IsBackground = true,
        };
        byte* data = (byte*)NativeMemory.Alloc((nuint)length);
        var segment = (TransportSegment*)NativeMemory.Alloc((nuint)sizeof(TransportSegment));
        try
        {
            for (int i = 0; i < length; i++) data[i] = ReviewResumeMsquicStressTests.PatternAt(i, 9);
            *segment = new TransportSegment(data, length);
            resumer.Start();
            Assert.Equal(TransportStatus.Success, _client.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId id));
            Assert.Equal(TransportStatus.Success, _client.SendStream(id, segment, 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin));
            bool ended = Spin.Until(() => Volatile.Read(ref serverSink.Completes) == 1, TimeSpan.FromSeconds(30));
            Volatile.Write(ref stop, 1);
            resumer.Join(TimeSpan.FromSeconds(5));
            string summary = $"{serverSink.Next} of {length} bytes, {serverSink.Calls} indications, {serverSink.Holds} holds, fin {serverSink.FinSeen}, shutdowns {serverSink.Shutdowns}, first error: {serverSink.FirstError ?? "none"}";
            Assert.True(ended, "the stream never closed: " + summary);
            Assert.True(serverSink.Errors == 0, summary);
            Assert.True(serverSink.Next == length && serverSink.FinSeen && serverSink.Shutdowns == 1, summary);
        }
        finally
        {
            Volatile.Write(ref stop, 1);
            _client?.Dispose();
            _server?.Dispose();
            Thread.Sleep(200);
            NativeMemory.Free(data);
            NativeMemory.Free(segment);
        }
    }

    public void Dispose()
    {
        _client?.Dispose();
        _server?.Dispose();
        for (int i = _owned.Count - 1; i >= 0; i--) _owned[i].Dispose();
        _certificate.Dispose();
        _registration.Dispose();
    }
}
