using System.Net;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Adversarial review of fix/msquic-held-stream-resume (8107fc3), threading lens: the WebTransport carrier on top of the
/// changed MsQuic transport. The conformance suite runs the carrier only over the simulator, so nothing in the repository
/// holds and resumes a carrier stream whose inner transport is the one the commit changed. The carrier strips a preamble
/// from every stream and passes holds and resumes through; these tests hold and resume its streams over real loopback.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class ReviewResumeThreadingCarrierTests
{
    /// <summary>The carrier over MsQuic loopback, built the way an application builds it, as a conformance harness.</summary>
    private sealed class CarrierLoopbackHarness : ITransportTestHarness
    {
        private readonly TestRegistration _registration = new("quicly-review-resume-carrier");
        private readonly X509Certificate2 _certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        private readonly List<IDisposable> _owned = [];
        private readonly List<ITransport> _transports = [];

        public string Name => "WebTransport carrier over MsQuic (loopback)";

        public TimeSpan DefaultTimeout => TestTimeouts.Default;

        public string? CleanupError { get; private set; }

        public ConformancePair CreatePair(ITransportSink clientSink, ITransportSink serverSink, ConformancePairOptions? options = null)
        {
            options ??= new ConformancePairOptions();
            ITransport? server = null;
            using ManualResetEventSlim accepted = new();
            WebTransportListener listener = WebTransportListener.CreateMsQuic(
                new IPEndPoint(IPAddress.Loopback, 0),
                _certificate,
                new MsQuicTransportOptions { ServerPeerBidiStreamCount = options.ServerPeerBidiStreams, ServerPeerUnidiStreamCount = options.ServerPeerUnidiStreams },
                null,
                _registration.Registration);
            lock (_owned) _owned.Add(listener);
            listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
            {
                server = transport;
                lock (_owned) _transports.Add(transport);
                accepted.Set();
                return serverSink;
            });
            WebTransportConnector connector = WebTransportConnector.CreateMsQuic(
                new MsQuicTransportOptions
                {
                    ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
                    PinnedSpkiSha256 = [SpkiPin.Compute(_certificate)],
                    ClientPeerBidiStreamCount = options.ClientPeerBidiStreams,
                    ClientPeerUnidiStreamCount = options.ClientPeerUnidiStreams,
                },
                null,
                _registration.Registration);
            lock (_owned) _owned.Add(connector);
            ITransport client = connector.Connect(listener.LocalEndPoint, "localhost", clientSink);
            lock (_owned) _transports.Add(client);
            if (!accepted.Wait(DefaultTimeout)) throw new ConformanceException($"[{Name}] the listener never accepted the connection.");
            return new ConformancePair(client, server!);
        }

        public ITransport Connect(ITransportSink clientSink, PreHandshakeCallback preHandshake, AcceptCallback accept, ConformancePairOptions? options = null) =>
            throw new NotSupportedException("Not needed by the scenarios this harness runs.");

        public bool Pump(Func<bool> condition, TimeSpan timeout)
        {
            long deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
            while (!condition())
            {
                if (Environment.TickCount64 >= deadline) return false;
                Thread.Sleep(1);
            }

            return true;
        }

        public void Dispose()
        {
            lock (_owned)
            {
                foreach (ITransport transport in _transports) transport.Dispose();
                for (int i = _owned.Count - 1; i >= 0; i--) _owned[i].Dispose();
            }

            _certificate.Dispose();
            try
            {
                _registration.Dispose();
            }
            catch (TimeoutException ex)
            {
                CleanupError = ex.Message;
            }
        }
    }

    [Fact]
    public void The_Carrier_Over_MsQuic_Passes_The_Held_Stream_Scenario()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        CarrierLoopbackHarness harness = new();
        try
        {
            TransportConformance.Run(nameof(TransportConformance.HeldStreamIsIndicatedAgainAfterEveryResume), harness);
        }
        finally
        {
            harness.Dispose();
        }

        Assert.Null(harness.CleanupError);
    }

    [Fact]
    public void Every_Byte_Of_A_Carrier_Stream_Arrives_Exactly_Once_Whatever_The_Sink_Holds_And_Credits()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        CarrierLoopbackHarness harness = new();
        List<string> problems = ReviewResumeThreadingRaceTests.RunChaos(harness, streams: 12, rounds: 2, out long holds);
        Assert.True(problems.Count == 0, string.Join("\n", problems.Take(12)));
        Assert.True(holds > 100, $"only {holds} holds: the test did not exercise the resume path");
        Assert.Null(harness.CleanupError);
    }
}
