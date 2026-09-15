using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// Server-side <see cref="ITransportListener"/> over MsQuic: one UDP end point, one MsQuic configuration per ALPN of
/// <see cref="MsQuicTransportOptions.Alpns"/> (the server posture of <see cref="MsQuicTransportOptions.CreateServerSettings"/>:
/// one bidirectional and no unidirectional peer stream before admission), certificate hot swap.
/// </summary>
/// <remarks>
/// <para><b>Admission.</b> For every new connection MsQuic's NEW_CONNECTION callback runs, before any TLS work, the
/// <see cref="PreHandshakeCallback"/> with the remote end point, the SNI and the negotiated ALPN; <see cref="PreHandshakeDecision.Reject"/>
/// refuses the connection there. Otherwise the <see cref="AcceptCallback"/> runs (still inside NEW_CONNECTION) with the new
/// <see cref="MsQuicTransport"/>; it returns the sink that receives the transport's callbacks, or null to refuse the
/// connection. Both callbacks run on MsQuic worker threads, possibly concurrently for different connections, and must not
/// block or call methods of the new transport (its events start after the accept callback returns).</para>
/// <para><b>Certificates.</b> The listener borrows certificates: keep each one alive until
/// <see cref="CertificateRetired"/> reports it. <see cref="UpdateCertificate"/> opens new configurations (one credential
/// per configuration) and atomically switches the ones handed to new connections; an old configuration is closed once
/// no connection created with it is still handshaking (reference counted per configuration: one reference for "current",
/// one per handshaking connection), so connections established with the old certificate keep working. On Windows the
/// credential goes through <c>CERTIFICATE_CONTEXT</c> (the bundled msquic 2.5.10 rejects PKCS#12).</para>
/// <para><b>Lifetime.</b> <see cref="Stop"/> refuses new connections; existing ones are unaffected. <see cref="Dispose"/>
/// also closes the listener handle and releases the current configurations; a registration the listener created is
/// closed once every accepted transport has released its handles and every configuration is closed.</para>
/// </remarks>
public sealed class MsQuicTransportListener : ITransportListener, IMsQuicListenerEvents
{
    /// <summary>
    /// One MsQuic configuration and its reference count: one reference while it is the current configuration for its ALPN,
    /// one per connection created with it until that connection finished (or abandoned) its handshake.
    /// </summary>
    internal sealed class ConfigurationEntry : IThreadPoolWorkItem
    {
        private readonly MsQuicTransportListener _owner;
        private readonly CertificateGeneration _generation;
        private int _references = 1;

        public ConfigurationEntry(MsQuicTransportListener owner, MsQuicConfiguration configuration, CertificateGeneration generation)
        {
            _owner = owner;
            Configuration = configuration;
            _generation = generation;
        }

        public MsQuicConfiguration Configuration { get; }

        public X509Certificate2 Certificate => _generation.Certificate;

        public int ReferenceCount => Volatile.Read(ref _references);

        /// <summary>Adds a reference unless the entry has already been released for good.</summary>
        public bool TryAddRef()
        {
            int current = Volatile.Read(ref _references);
            while (current > 0)
            {
                int seen = Interlocked.CompareExchange(ref _references, current + 1, current);
                if (seen == current) return true;
                current = seen;
            }
            return false;
        }

        /// <summary>Drops a reference; the last one closes the configuration (on a thread-pool thread when inside a callback).</summary>
        public void Release()
        {
            if (Interlocked.Decrement(ref _references) != 0) return;
            if (MsQuicCallbackScope.IsInsideCallback) ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
            else Close();
        }

        void IThreadPoolWorkItem.Execute() => Close();

        private void Close()
        {
            Configuration.Close();
            _owner.OnConfigurationClosed(_generation);
        }
    }

    /// <summary>The configurations (one per ALPN) created for one certificate.</summary>
    internal sealed class CertificateGeneration(X509Certificate2 certificate, int configurations)
    {
        public readonly X509Certificate2 Certificate = certificate;
        public int OpenConfigurations = configurations;
    }

    private readonly MsQuicTransportOptions _options;
    private readonly MsQuicRegistration _registration;
    private readonly bool _ownsRegistration;
    private readonly IPEndPoint _requestedEndPoint;
    private readonly byte[][] _alpns;
    private readonly Lock _gate = new();
    private readonly Action<MsQuicTransport> _onHandlesClosed;
    private ConfigurationEntry[] _current;
    private MsQuicListener? _listener;
    private IPEndPoint? _boundEndPoint;
    private PreHandshakeCallback? _preHandshake;
    private AcceptCallback? _accept;
    private volatile bool _accepting;
    private int _disposed;
    private int _liveTransports;
    private int _openConfigurations;
    private int _registrationClosed;
    private long _preHandshakeRejected;
    private long _acceptRefused;
    private long _otherRefused;

    /// <summary>Creates a listener (not started) serving <paramref name="certificate"/>.</summary>
    /// <param name="localEndPoint">The UDP end point to bind (port 0 picks a free port; see <see cref="LocalEndPoint"/> after <see cref="Start"/>).</param>
    /// <param name="certificate">The server certificate with its private key; borrowed until <see cref="CertificateRetired"/> reports it.</param>
    /// <param name="options">Server options (copied); null for the defaults.</param>
    /// <param name="registration">A registration to borrow; null to create and own one.</param>
    /// <exception cref="MsQuicException">MsQuic refused the registration, a configuration or the credential.</exception>
    public MsQuicTransportListener(IPEndPoint localEndPoint, X509Certificate2 certificate, MsQuicTransportOptions? options = null, MsQuicRegistration? registration = null)
    {
        ArgumentNullException.ThrowIfNull(localEndPoint);
        ArgumentNullException.ThrowIfNull(certificate);
        _options = (options ?? new MsQuicTransportOptions()).Clone();
        _options.Validate(client: false);
        _requestedEndPoint = localEndPoint;
        _alpns = new byte[_options.Alpns.Length][];
        for (int i = 0; i < _alpns.Length; i++) _alpns[i] = Encoding.UTF8.GetBytes(_options.Alpns[i]);
        _ownsRegistration = registration is null;
        _registration = registration ?? new MsQuicRegistration(_options.AppName, _options.ExecutionProfile);
        try
        {
            _current = CreateEntries(certificate);
        }
        catch
        {
            if (_ownsRegistration) _registration.Close();
            throw;
        }
        _onHandlesClosed = OnTransportHandlesClosed;
    }

    /// <inheritdoc/>
    /// <remarks>The bound end point (with the port MsQuic picked) once started; the requested one before.</remarks>
    public IPEndPoint LocalEndPoint => Volatile.Read(ref _boundEndPoint) ?? _requestedEndPoint;

    /// <summary>The registration connections are accepted in.</summary>
    public MsQuicRegistration Registration => _registration;

    /// <summary>
    /// Called (on a thread-pool thread or the disposing thread) when every configuration that used a certificate has been
    /// closed, after <see cref="UpdateCertificate"/> or <see cref="Dispose"/>: the owner may dispose the certificate then.
    /// </summary>
    public Action<X509Certificate2>? CertificateRetired { get; set; }

    /// <summary>The certificate handed to new connections.</summary>
    public X509Certificate2 CurrentCertificate => Volatile.Read(ref _current)[0].Certificate;

    /// <summary>MsQuic configurations not closed yet (current ones plus old ones still used by handshaking connections).</summary>
    public int OpenConfigurationCount => Volatile.Read(ref _openConfigurations);

    /// <summary>Accepted transports whose handles are not closed yet.</summary>
    public int LiveTransportCount => Volatile.Read(ref _liveTransports);

    /// <summary>Connections refused by the pre-handshake callback.</summary>
    public long PreHandshakeRejectedCount => Interlocked.Read(ref _preHandshakeRejected);

    /// <summary>Connections refused because the accept callback returned null (or threw).</summary>
    public long AcceptRefusedCount => Interlocked.Read(ref _acceptRefused);

    /// <summary>Connections refused for another reason (stopped, unknown ALPN, configuration failure).</summary>
    public long OtherRefusedCount => Interlocked.Read(ref _otherRefused);

    /// <inheritdoc/>
    /// <exception cref="InvalidOperationException">The listener was already started (it can be started once).</exception>
    /// <exception cref="MsQuicException">MsQuic could not bind the end point.</exception>
    public void Start(PreHandshakeCallback preHandshake, AcceptCallback accept)
    {
        ArgumentNullException.ThrowIfNull(preHandshake);
        ArgumentNullException.ThrowIfNull(accept);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_listener is not null) throw new InvalidOperationException("The listener has already been started.");
            _preHandshake = preHandshake;
            _accept = accept;
            var listener = new MsQuicListener(_registration, this);
            try
            {
                _accepting = true;
                listener.Start(_requestedEndPoint, _options.Alpns);
                Volatile.Write(ref _boundEndPoint, listener.LocalEndPoint);
            }
            catch
            {
                _accepting = false;
                listener.Close();
                throw;
            }
            _listener = listener;
        }
    }

    /// <inheritdoc/>
    /// <remarks>New connections are refused at once; MsQuic stops the listener asynchronously.</remarks>
    public void Stop()
    {
        lock (_gate)
        {
            _accepting = false;
            if (_listener is { IsClosed: false } listener) listener.Stop();
        }
    }

    /// <summary>
    /// Switches the certificate for new connections: opens one new configuration per ALPN, swaps them in atomically and
    /// releases the old ones, which close once no connection created with them is still handshaking.
    /// </summary>
    /// <exception cref="MsQuicException">MsQuic refused the new credential (nothing was swapped).</exception>
    public void UpdateCertificate(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            ConfigurationEntry[] fresh = CreateEntries(certificate);
            ConfigurationEntry[] old = Interlocked.Exchange(ref _current, fresh);
            foreach (ConfigurationEntry entry in old) entry.Release();
        }
    }

    /// <summary>
    /// Stops accepting, closes the listener handle (never from an MsQuic callback: it waits for MsQuic to stop) and releases
    /// the current configurations. Accepted transports are unaffected and must be disposed by their owners.
    /// </summary>
    public void Dispose()
    {
        MsQuicListener? listener;
        lock (_gate)
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            _accepting = false;
            listener = _listener;
            foreach (ConfigurationEntry entry in Volatile.Read(ref _current)) entry.Release();
        }
        if (listener is not null)
        {
            if (MsQuicCallbackScope.IsInsideCallback) ThreadPool.UnsafeQueueUserWorkItem(static l => l.Close(), listener, preferLocal: false);
            else listener.Close();
        }
        TryCloseRegistration();
    }

    // ------------------------------------------------------------------ MsQuic listener events (worker threads)

    MsQuicConfiguration? IMsQuicListenerEvents.NewConnection(MsQuicListener listener, MsQuicConnection connection, in MsQuicNewConnectionInfo info)
    {
        if (!_accepting || Volatile.Read(ref _disposed) != 0)
        {
            Interlocked.Increment(ref _otherRefused);
            return null;
        }
        int alpnIndex = IndexOfAlpn(info.NegotiatedAlpn);
        if (alpnIndex < 0)
        {
            Interlocked.Increment(ref _otherRefused);
            return null;
        }
        NewConnectionInfo managed = default;
        managed.RemoteEndPoint = info.RemoteAddress.ToIPEndPoint();
        ReadOnlySpan<byte> serverName = info.ServerName;
        managed.ServerName = serverName.IsEmpty ? null : Encoding.UTF8.GetString(serverName);
        ReadOnlySpan<byte> alpn = info.NegotiatedAlpn;
        int alpnLength = Math.Min(alpn.Length, 255);
        alpn[..alpnLength].CopyTo(managed.Alpn);
        managed.AlpnLength = (byte)alpnLength;

        PreHandshakeDecision decision;
        try
        {
            decision = _preHandshake!(in managed);
        }
        catch (Exception ex)
        {
            Diagnose(TransportDiagnosticLevel.Error, "The pre-handshake callback threw; the connection is refused.", ex);
            decision = PreHandshakeDecision.Reject;
        }
        if (decision != PreHandshakeDecision.Accept)
        {
            Interlocked.Increment(ref _preHandshakeRejected);
            return null;
        }

        ConfigurationEntry? entry = AcquireConfiguration(alpnIndex);
        if (entry is null)
        {
            Interlocked.Increment(ref _otherRefused);
            return null;
        }
        var transport = new MsQuicTransport(connection, sink: null, _options, certificatePolicy: null, managed.ServerName);
        ITransportSink? sink;
        try
        {
            sink = _accept!(transport, in managed);
        }
        catch (Exception ex)
        {
            Diagnose(TransportDiagnosticLevel.Error, "The accept callback threw; the connection is refused.", ex);
            sink = null;
        }
        if (sink is null)
        {
            entry.Release();
            Interlocked.Increment(ref _acceptRefused);
            return null;
        }
        transport.SetSink(sink);
        transport.ConfigurationLease = entry;
        transport.HandlesClosedCallback = _onHandlesClosed;
        Interlocked.Increment(ref _liveTransports);
        return entry.Configuration;
    }

    void IMsQuicListenerEvents.ConnectionConfigurationFailed(MsQuicListener listener, MsQuicConnection connection, int status)
    {
        Interlocked.Increment(ref _otherRefused);
        if (connection.Events is MsQuicTransport transport) transport.FailBeforeStart(status);
    }

    // ------------------------------------------------------------------ configurations

    private ConfigurationEntry[] CreateEntries(X509Certificate2 certificate)
    {
        var generation = new CertificateGeneration(certificate, _alpns.Length);
        var entries = new ConfigurationEntry[_alpns.Length];
        int created = 0;
        try
        {
            for (int i = 0; i < entries.Length; i++)
            {
                MsQuicConfiguration configuration = MsQuicConfiguration.CreateServer(_registration, [_options.Alpns[i]], certificate, _options.CreateServerSettings(), _options.ServerCredentialMode, _options.ServerKeyStorage);
                entries[i] = new ConfigurationEntry(this, configuration, generation);
                Interlocked.Increment(ref _openConfigurations);
                created++;
            }
        }
        catch
        {
            for (int i = 0; i < created; i++)
            {
                entries[i].Configuration.Close();
                Interlocked.Decrement(ref _openConfigurations);
            }
            throw;
        }
        return entries;
    }

    private ConfigurationEntry? AcquireConfiguration(int alpnIndex)
    {
        while (Volatile.Read(ref _disposed) == 0)
        {
            ConfigurationEntry entry = Volatile.Read(ref _current)[alpnIndex];
            if (entry.TryAddRef()) return entry;
            // Swapped out and closed between the read and the reference: the new entry is current by now.
        }
        return null;
    }

    private int IndexOfAlpn(ReadOnlySpan<byte> alpn)
    {
        for (int i = 0; i < _alpns.Length; i++)
        {
            if (alpn.SequenceEqual(_alpns[i])) return i;
        }
        return -1;
    }

    private void OnConfigurationClosed(CertificateGeneration generation)
    {
        Interlocked.Decrement(ref _openConfigurations);
        if (Interlocked.Decrement(ref generation.OpenConfigurations) == 0)
        {
            try
            {
                CertificateRetired?.Invoke(generation.Certificate);
            }
            catch (Exception ex)
            {
                Diagnose(TransportDiagnosticLevel.Error, "The CertificateRetired callback threw.", ex);
            }
        }
        TryCloseRegistration();
    }

    private void OnTransportHandlesClosed(MsQuicTransport transport)
    {
        Interlocked.Decrement(ref _liveTransports);
        TryCloseRegistration();
    }

    private void TryCloseRegistration()
    {
        if (!_ownsRegistration || Volatile.Read(ref _disposed) == 0) return;
        if (Volatile.Read(ref _liveTransports) != 0 || Volatile.Read(ref _openConfigurations) != 0) return;
        if (Interlocked.Exchange(ref _registrationClosed, 1) != 0) return;
        if (MsQuicCallbackScope.IsInsideCallback) ThreadPool.UnsafeQueueUserWorkItem(static r => r.Close(), _registration, preferLocal: false);
        else _registration.Close();
    }

    private void Diagnose(TransportDiagnosticLevel level, string message, Exception? exception)
    {
        try
        {
            _options.Diagnostic?.Invoke(level, message, exception);
        }
        catch
        {
            // A diagnostics sink must not break the listener.
        }
    }
}
