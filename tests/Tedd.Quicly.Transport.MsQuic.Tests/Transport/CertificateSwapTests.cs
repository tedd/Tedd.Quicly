using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>Certificate retirement and hot swap of <see cref="MsQuicTransportListener"/>, including under load.</summary>
[Collection(MsQuicCollection.Name)]
public class CertificateSwapTests
{
    private static readonly TimeSpan Timeout = TestTimeouts.Default;

    /// <summary>
    /// CertificateRetired: "Called when no open configuration uses a certificate instance any more ... the owner may dispose
    /// the certificate then". Re-applying the certificate the listener already serves (a renewal that returned the same
    /// instance) must not report it retired while the new configurations use it; replacing it for real retires it once.
    /// </summary>
    [Fact]
    public void Re_applying_the_current_certificate_does_not_retire_it()
    {
        var harness = new MsQuicTransportHarness();
        X509Certificate2? next = null;
        try
        {
            var retired = new ConcurrentQueue<X509Certificate2>();
            MsQuicTransportListener listener = harness.StartListener(harness.ServerOptions(), static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport t, in NewConnectionInfo _) =>
                new RecordingSink { Transport = harness.Track((MsQuicTransport)t) });
            listener.CertificateRetired = retired.Enqueue;
            X509Certificate2 current = listener.CurrentCertificate;
            listener.UpdateCertificate(current);
            listener.UpdateCertificate(current);
            Thread.Sleep(200);
            Assert.Same(current, listener.CurrentCertificate);
            Assert.Equal(1, listener.OpenConfigurationCount);
            Assert.True(retired.IsEmpty, $"CertificateRetired reported {retired.Count} certificate(s) although the listener still serves the same instance.");

            // It still serves handshakes, and replacing it for real retires it exactly once.
            var sink = new RecordingSink();
            MsQuicTransport client = harness.Track(harness.CreateConnector(harness.ClientOptions()).Connect(listener.LocalEndPoint, "localhost", sink));
            sink.Transport = client;
            Assert.True(sink.WaitFor(static e => e.Kind == RecordedEventKind.Connected, Timeout), "a client could not connect with the re-applied certificate");
            next = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
            listener.UpdateCertificate(next);
            Assert.True(Spin.Until(() => retired.Count == 1, Timeout), $"retired {retired.Count}");
            Thread.Sleep(100);
            Assert.Same(current, Assert.Single(retired));
        }
        finally
        {
            harness.Dispose();
            next?.Dispose();
        }
        Assert.Null(harness.CleanupError);
    }

    /// <summary>
    /// Many parallel handshakes while the certificate is swapped over and over: every handshake succeeds (a swap never
    /// closes a configuration a handshaking connection still needs, including the window between NEW_CONNECTION returning
    /// the configuration and MsQuic applying it after the callback) and every certificate reported retired really is
    /// unused: no handshake validates it after the report, and the owner disposes it right there without breaking anything.
    /// </summary>
    [Fact]
    public void Parallel_handshakes_survive_repeated_certificate_swaps_and_retired_certificates_are_unused()
    {
        const int Clients = 8;
        const int MinConnectionsPerClient = 10;
        const int MaxConnectionsPerClient = 400;
        const int Swaps = 20;
        var harness = new MsQuicTransportHarness();
        var fresh = new X509Certificate2[Swaps];
        var thumbprintByDer = new Dictionary<string, string>();
        var retiredAt = new ConcurrentDictionary<string, long>();
        var validatedAt = new ConcurrentQueue<(string Thumbprint, long Sequence)>();
        var failures = new ConcurrentQueue<string>();
        long sequence = 0;
        int attempts = 0;
        bool swapsDone = false;
        Exception? swapFailure = null;
        try
        {
            thumbprintByDer[Convert.ToBase64String(harness.Certificate.RawData)] = harness.Certificate.Thumbprint;
            for (int i = 0; i < Swaps; i++)
            {
                fresh[i] = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
                thumbprintByDer[Convert.ToBase64String(fresh[i].RawData)] = fresh[i].Thumbprint;
            }
            string lastThumbprint = fresh[^1].Thumbprint;
            MsQuicTransportListener listener = harness.StartListener(harness.ServerOptions(), static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport t, in NewConnectionInfo _) =>
                new RecordingSink { Transport = harness.Track((MsQuicTransport)t) });
            listener.CertificateRetired = certificate =>
            {
                retiredAt[certificate.Thumbprint] = Interlocked.Increment(ref sequence);
                // The contract lets the owner dispose a retired certificate at once (the harness disposes its own at the end).
                if (!ReferenceEquals(certificate, harness.Certificate)) certificate.Dispose();
            };
            var clientOptions = new MsQuicTransportOptions
            {
                ServerCertificateValidation = ServerCertificateValidationMode.Callback,
                ServerCertificateValidator = (in ServerCertificateContext context) =>
                {
                    string thumbprint = thumbprintByDer.GetValueOrDefault(Convert.ToBase64String(context.LeafDer), "unknown");
                    validatedAt.Enqueue((thumbprint, Interlocked.Increment(ref sequence)));
                    return ServerCertificateDecision.AcceptIgnoringPlatformValidation;
                },
            };
            MsQuicTransportConnector connector = harness.CreateConnector(clientOptions);
            var clients = new Thread[Clients];
            for (int c = 0; c < Clients; c++)
            {
                clients[c] = new Thread(() =>
                {
                    // Keep handshaking for as long as the swaps go on, so they overlap every swap.
                    for (int n = 0; n < MaxConnectionsPerClient && (n < MinConnectionsPerClient || !Volatile.Read(ref swapsDone)); n++)
                    {
                        Interlocked.Increment(ref attempts);
                        var sink = new RecordingSink();
                        MsQuicTransport client = harness.Track(connector.Connect(listener.LocalEndPoint, "localhost", sink));
                        sink.Transport = client;
                        if (!sink.WaitFor(static e => e.Kind is RecordedEventKind.Connected or RecordedEventKind.Closed, Timeout))
                        {
                            failures.Enqueue("a handshake did not finish");
                        }
                        else if (sink.CountOf(RecordedEventKind.Connected) == 0)
                        {
                            RecordedEvent closed = sink.OfKind(RecordedEventKind.Closed)[0];
                            failures.Enqueue($"a handshake failed: {closed.CloseReason} {MsQuicStatus.GetName(closed.TransportStatus)} code 0x{closed.ErrorCode:X}");
                        }
                        client.Dispose();
                    }
                })
                {
                    IsBackground = true,
                    Name = "swap-stress-client",
                };
            }
            var swapper = new Thread(() =>
            {
                try
                {
                    foreach (X509Certificate2 certificate in fresh)
                    {
                        listener.UpdateCertificate(certificate);
                        Thread.Sleep(3);
                    }
                }
                catch (Exception ex)
                {
                    swapFailure = ex;
                }
                finally
                {
                    Volatile.Write(ref swapsDone, true);
                }
            })
            {
                IsBackground = true,
                Name = "swap-stress-swapper",
            };
            foreach (Thread client in clients) client.Start();
            swapper.Start();
            Assert.True(swapper.Join(TimeSpan.FromSeconds(60)), "the swapper did not finish");
            foreach (Thread client in clients) Assert.True(client.Join(TimeSpan.FromSeconds(120)), "a client thread did not finish");
            Assert.Null(swapFailure);
            Assert.True(failures.IsEmpty, $"{failures.Count} of {attempts} handshakes failed: {string.Join("; ", failures.Take(5))}");
            Assert.Equal(attempts, validatedAt.Count);

            // Every certificate but the last was retired once no handshake used it any more ...
            Assert.True(Spin.Until(() => retiredAt.Count == Swaps && listener.OpenConfigurationCount == 1, Timeout), $"retired {retiredAt.Count} of {Swaps}, open configurations {listener.OpenConfigurationCount}");
            Assert.False(retiredAt.ContainsKey(lastThumbprint), "the certificate the listener still serves was reported retired");
            // ... and no handshake validated a certificate after it was reported retired.
            foreach ((string thumbprint, long validated) in validatedAt)
            {
                if (retiredAt.TryGetValue(thumbprint, out long retired)) Assert.True(validated < retired, $"certificate {thumbprint} was validated by a handshake (#{validated}) after it was reported retired (#{retired})");
            }
            TestContext.Current.SendDiagnosticMessage($"certificate swap stress: {attempts} handshakes across {Swaps} swaps, {retiredAt.Count} certificates retired");
        }
        finally
        {
            harness.Dispose();
            foreach (X509Certificate2? certificate in fresh) certificate?.Dispose();
        }
        Assert.Null(harness.CleanupError);
    }
}
