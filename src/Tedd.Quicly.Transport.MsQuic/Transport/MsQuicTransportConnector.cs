using System.Net;
using System.Net.Sockets;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// Client-side <see cref="ITransportConnector"/> over MsQuic. One client configuration (the ALPNs, the client posture of
/// <see cref="MsQuicTransportOptions.CreateClientSettings"/> and the server-certificate validation policy) is shared by
/// every connection the connector starts.
/// </summary>
/// <remarks>
/// <para>The connector creates its own registration (<see cref="MsQuicTransportOptions.AppName"/>,
/// <see cref="MsQuicTransportOptions.ExecutionProfile"/>) or borrows the one passed in. <see cref="Dispose"/> refuses
/// new connections; the configuration, and a registration the connector created, are closed once every transport it
/// created has released its handles, so transports may outlive the connector.</para>
/// <para><see cref="ServerCertificateValidationMode.PinnedSpki"/> and <see cref="ServerCertificateValidationMode.Callback"/>
/// use the wrapper's deferred portable validation (<see cref="MsQuicCertificateValidation.Callback"/>): the certificate
/// arrives as DER bytes together with the platform verdict and the policy decides on the MsQuic worker thread.
/// <see cref="ServerCertificateValidationMode.DangerousAcceptAnyServerCertificate"/> is refused by the constructor unless
/// the library is a DEBUG build or <c>QUICLY_ALLOW_INSECURE=1</c>, and every connection logs a warning through
/// <see cref="MsQuicTransportOptions.Diagnostic"/>.</para>
/// </remarks>
public sealed class MsQuicTransportConnector : ITransportConnector, IDisposable
{
    private readonly MsQuicTransportOptions _options;
    private readonly MsQuicRegistration _registration;
    private readonly bool _ownsRegistration;
    private readonly MsQuicConfiguration _configuration;
    private readonly ServerCertificatePolicy _policy;
    private readonly Action<MsQuicTransport> _onHandlesClosed;
    private int _liveTransports;
    private int _disposed;
    private int _resourcesClosed;

    /// <summary>Creates a connector with <paramref name="options"/> (copied and validated now).</summary>
    /// <param name="options">Client options; see <see cref="MsQuicTransportOptions.Validate"/>.</param>
    /// <param name="registration">A registration to borrow (the caller closes it after this connector's transports); null to create and own one.</param>
    /// <exception cref="ArgumentException">The options are invalid.</exception>
    /// <exception cref="InvalidOperationException">Insecure validation was requested where it is not allowed.</exception>
    /// <exception cref="MsQuicException">MsQuic refused the registration or the configuration.</exception>
    /// <exception cref="PlatformNotSupportedException">The process is 32-bit (see <see cref="MsQuicTransport.SegmentLayoutMatchesQuicBuffer"/>).</exception>
    public MsQuicTransportConnector(MsQuicTransportOptions options, MsQuicRegistration? registration = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        MsQuicTransport.ThrowIfSegmentLayoutUnsupported();
        _options = options.Clone();
        _options.Validate(client: true);
        _policy = new ServerCertificatePolicy(_options);
        _ownsRegistration = registration is null;
        _registration = registration ?? new MsQuicRegistration(_options.AppName, _options.ExecutionProfile);
        try
        {
            _configuration = MsQuicConfiguration.CreateClient(_registration, _options.Alpns, _policy.WrapperValidation, _options.CreateClientSettings());
        }
        catch
        {
            if (_ownsRegistration) _registration.Close();
            throw;
        }
        _onHandlesClosed = OnTransportHandlesClosed;
    }

    /// <summary>The registration connections are created in.</summary>
    public MsQuicRegistration Registration => _registration;

    /// <summary>Transports created by this connector whose handles are not closed yet.</summary>
    public int LiveTransportCount => Volatile.Read(ref _liveTransports);

    /// <summary>True once the configuration (and an owned registration) has been closed.</summary>
    public bool ResourcesClosed => Volatile.Read(ref _resourcesClosed) != 0;

    ITransport ITransportConnector.Connect(EndPoint endpoint, string? serverName, ITransportSink sink) => Connect(endpoint, serverName, sink);

    /// <summary>
    /// Starts a connection to <paramref name="endpoint"/>. The transport raises <see cref="ITransportSink.OnConnected"/> when
    /// the handshake completes or <see cref="ITransportSink.OnClosed"/> (<see cref="TransportCloseReason.Transport"/> with the
    /// MsQuic status) when it fails.
    /// </summary>
    /// <param name="endpoint">An <see cref="IPEndPoint"/> (connected to directly) or a <see cref="DnsEndPoint"/> (resolved by MsQuic).</param>
    /// <param name="serverName">
    /// The name sent as SNI and validated against the certificate. With an <see cref="IPEndPoint"/> it is independent of the
    /// address (null sends no SNI); with a <see cref="DnsEndPoint"/> it replaces the host name (null uses it).
    /// </param>
    /// <param name="sink">Receives the transport's callbacks; set before the connection starts.</param>
    /// <exception cref="MsQuicException">MsQuic refused to start the connection (no callback follows).</exception>
    public MsQuicTransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(sink);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        IPEndPoint? address = endpoint as IPEndPoint;
        string target;
        int port;
        switch (endpoint)
        {
            case IPEndPoint ip:
                target = string.IsNullOrEmpty(serverName) ? ip.Address.ToString() : serverName;
                port = ip.Port;
                break;
            case DnsEndPoint dns:
                target = string.IsNullOrEmpty(serverName) ? dns.Host : serverName;
                port = dns.Port;
                break;
            default:
                throw new ArgumentException("Only IPEndPoint and DnsEndPoint are supported.", nameof(endpoint));
        }
        var connection = new MsQuicConnection(_registration);
        string? validatedName = string.IsNullOrEmpty(serverName) ? (address is null ? target : null) : serverName;
        var transport = new MsQuicTransport(connection, sink, _options, _policy, validatedName);
        transport.HandlesClosedCallback = _onHandlesClosed;
        Interlocked.Increment(ref _liveTransports);
        if (_policy.Mode == ServerCertificateValidationMode.DangerousAcceptAnyServerCertificate)
        {
            Diagnose(TransportDiagnosticLevel.Warning, "Connecting to " + endpoint + " WITHOUT validating the server certificate (DangerousAcceptAnyServerCertificate).");
        }
        int status;
        if (address is not null)
        {
            // Connect to the address itself; the server name only feeds SNI and certificate validation.
            QUIC_ADDR remote = QUIC_ADDR.FromIPEndPoint(address);
            status = connection.SetParam(MsQuicParam.QUIC_PARAM_CONN_REMOTE_ADDRESS, in remote);
            if (MsQuicStatus.Succeeded(status))
            {
                int family = address.AddressFamily == AddressFamily.InterNetworkV6 ? QuicAddressFamily.INET6 : QuicAddressFamily.INET;
                status = connection.Start(_configuration, target, (ushort)port, family);
            }
        }
        else
        {
            status = connection.Start(_configuration, target, (ushort)port);
        }
        if (MsQuicStatus.Failed(status))
        {
            transport.ReleaseUnstarted();
            throw new MsQuicException(status, "ConnectionStart");
        }
        return transport;
    }

    /// <summary>
    /// Refuses new connections. The configuration (and a registration this connector created) is closed once every
    /// transport created by this connector has released its handles (at once when there is none).
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        if (Volatile.Read(ref _liveTransports) == 0) CloseResources();
    }

    private void OnTransportHandlesClosed(MsQuicTransport transport)
    {
        if (Interlocked.Decrement(ref _liveTransports) == 0 && Volatile.Read(ref _disposed) != 0) CloseResources();
    }

    private void CloseResources()
    {
        if (Interlocked.Exchange(ref _resourcesClosed, 1) != 0) return;
        if (MsQuicCallbackScope.IsInsideCallback)
        {
            ThreadPool.UnsafeQueueUserWorkItem(static connector => connector.CloseResourcesNow(), this, preferLocal: false);
            return;
        }
        CloseResourcesNow();
    }

    private void CloseResourcesNow()
    {
        _configuration.Close();
        if (_ownsRegistration) _registration.Close();
    }

    private void Diagnose(TransportDiagnosticLevel level, string message)
    {
        try
        {
            _options.Diagnostic?.Invoke(level, message, null);
        }
        catch
        {
            // A diagnostics sink must not break the connector.
        }
    }
}
