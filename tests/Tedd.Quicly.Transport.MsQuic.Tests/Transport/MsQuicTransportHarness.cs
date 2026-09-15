using System.Net;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Testing.Conformance;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>
/// <see cref="ITransportTestHarness"/> over MsQuic loopback: one test registration per harness (closed with a deadline so a
/// leak fails the test instead of hanging the run), one <see cref="MsQuicTransportListener"/> + <see cref="MsQuicTransportConnector"/>
/// per pair on 127.0.0.1:0, and a self-signed certificate the client pins (<see cref="ServerCertificateValidationMode.PinnedSpki"/>).
/// </summary>
internal sealed class MsQuicTransportHarness : ITransportTestHarness
{
    private readonly TestRegistration _registration = new("quicly-transport-tests");
    private readonly Lock _gate = new();
    private readonly List<MsQuicTransport> _transports = [];
    private readonly List<MsQuicTransportListener> _listeners = [];
    private readonly List<MsQuicTransportConnector> _connectors = [];
    private readonly Action<MsQuicTransportOptions>? _configure;

    public MsQuicTransportHarness(Action<MsQuicTransportOptions>? configure = null)
    {
        _configure = configure;
        Certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        Pin = SpkiPin.Compute(Certificate);
    }

    public X509Certificate2 Certificate { get; }

    public byte[] Pin { get; }

    public MsQuicRegistration Registration => _registration.Registration;

    /// <summary>Set by <see cref="Dispose"/> when cleanup did not finish (a transport kept its handles, dropped late events, or the registration hung).</summary>
    public string? CleanupError { get; private set; }

    /// <summary>Exceptions thrown by sinks, summed over every transport (computed by <see cref="Dispose"/>).</summary>
    public long SinkExceptionTotal { get; private set; }

    public string Name => "MsQuicTransport (loopback)";

    public TimeSpan DefaultTimeout => TestTimeouts.Default;

    public MsQuicTransportOptions ServerOptions(ConformancePairOptions? options = null)
    {
        options ??= new ConformancePairOptions();
        var server = new MsQuicTransportOptions { ServerPeerBidiStreamCount = options.ServerPeerBidiStreams, ServerPeerUnidiStreamCount = options.ServerPeerUnidiStreams };
        _configure?.Invoke(server);
        return server;
    }

    public MsQuicTransportOptions ClientOptions(ConformancePairOptions? options = null)
    {
        options ??= new ConformancePairOptions();
        var client = new MsQuicTransportOptions
        {
            ClientPeerBidiStreamCount = options.ClientPeerBidiStreams,
            ClientPeerUnidiStreamCount = options.ClientPeerUnidiStreams,
            ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
            PinnedSpkiSha256 = [Pin],
        };
        _configure?.Invoke(client);
        return client;
    }

    public MsQuicTransportListener StartListener(MsQuicTransportOptions options, PreHandshakeCallback preHandshake, AcceptCallback accept, X509Certificate2? certificate = null, IPEndPoint? endPoint = null)
    {
        var listener = new MsQuicTransportListener(endPoint ?? new IPEndPoint(IPAddress.Loopback, 0), certificate ?? Certificate, options, Registration);
        lock (_gate) _listeners.Add(listener);
        listener.Start(preHandshake, accept);
        return listener;
    }

    public MsQuicTransportConnector CreateConnector(MsQuicTransportOptions options)
    {
        var connector = new MsQuicTransportConnector(options, Registration);
        lock (_gate) _connectors.Add(connector);
        return connector;
    }

    /// <summary>Registers a transport for disposal (thread-safe: accept callbacks call it on MsQuic worker threads).</summary>
    public MsQuicTransport Track(MsQuicTransport transport)
    {
        lock (_gate) _transports.Add(transport);
        return transport;
    }

    public ConformancePair CreatePair(ITransportSink clientSink, ITransportSink serverSink, ConformancePairOptions? options = null)
    {
        options ??= new ConformancePairOptions();
        MsQuicTransport? server = null;
        var accepted = new ManualResetEventSlim();
        MsQuicTransportListener listener = StartListener(ServerOptions(options), static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            server = Track((MsQuicTransport)transport);
            accepted.Set();
            return serverSink;
        });
        MsQuicTransportConnector connector = CreateConnector(ClientOptions(options));
        MsQuicTransport client = Track(connector.Connect(listener.LocalEndPoint, "localhost", clientSink));
        if (!accepted.Wait(DefaultTimeout)) throw new ConformanceException($"[{Name}] the listener never accepted the connection.");
        listener.Stop();
        return new ConformancePair(client, server!);
    }

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

    /// <summary>Disposes every transport, waits for their handles, then listeners, connectors and the registration (with a deadline). Never throws.</summary>
    public void Dispose()
    {
        var problems = new List<string>();
        MsQuicTransport[] transports;
        lock (_gate) transports = [.. _transports];
        foreach (MsQuicTransport transport in transports) transport.Dispose();
        foreach (MsQuicTransport transport in transports)
        {
            if (!transport.WaitForHandlesClosed(TimeSpan.FromSeconds(10))) problems.Add("a transport did not release its handles");
            if (transport.LateEventCount != 0) problems.Add($"a transport dropped {transport.LateEventCount} events after OnClosed");
            SinkExceptionTotal += transport.SinkExceptionCount;
        }
        lock (_gate)
        {
            foreach (MsQuicTransportListener listener in _listeners) listener.Dispose();
            foreach (MsQuicTransportConnector connector in _connectors) connector.Dispose();
        }
        try
        {
            _registration.Dispose();
        }
        catch (TimeoutException ex)
        {
            problems.Add(ex.Message);
        }
        Certificate.Dispose();
        if (problems.Count > 0) CleanupError = string.Join("; ", problems);
    }
}
