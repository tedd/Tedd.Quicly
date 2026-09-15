using System.Globalization;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Transport.MsQuic;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>
/// A real MsQuic listener on 127.0.0.1:0 for one ALPN (quicly/1), and the <see cref="ICertificateConsumer"/> adapter that
/// hands it certificates: the QUIC-listener consumer of ACME.md ("Hot swap into running listeners") and ADR 0009. Every
/// certificate gets a new MsQuic configuration, because a credential loads once per configuration; new connections get the
/// newest one; a replaced configuration is closed once no connection created with it is still handshaking (one reference
/// while it is current, one per handshaking connection), after which MsQuic keeps it alive for the established connections.
/// Accepted connections echo bidirectional streams, so a test can prove that a connection still works.
/// </summary>
/// <remarks>
/// The references matter: the listener applies the configuration returned by <see cref="IMsQuicListenerEvents.NewConnection"/>
/// only after the callback has returned, so closing a replaced configuration the moment it is swapped out could close it
/// under a connection that selected it an instant earlier.
/// </remarks>
internal sealed class QuicTestServer : IMsQuicListenerEvents, ICertificateConsumer, IAsyncDisposable
{
    private readonly MsQuicRegistration _registration;
    private readonly string _alpn;
    private readonly MsQuicSettings _settings = E2eSettings.Server();
    private readonly MsQuicListener _listener;
    private readonly Lock _lock = new();
    private readonly List<ServedConfiguration> _configurations = [];
    private readonly List<ServerConnection> _connections = [];
    private TaskCompletionSource _connectionAdded = E2eTimeouts.NewTcs();
    private ServedConfiguration? _current;
    private int _refused;
    private int _disposed;

    public QuicTestServer(MsQuicRegistration registration, string alpn)
    {
        _registration = registration;
        _alpn = alpn;
        _listener = new MsQuicListener(registration, this);
        try
        {
            _listener.Start(new IPEndPoint(IPAddress.Loopback, 0), [alpn]);
            EndPoint = _listener.LocalEndPoint;
        }
        catch
        {
            _listener.Close();
            throw;
        }
    }

    /// <summary>The bound UDP end point (loopback, ephemeral port).</summary>
    public IPEndPoint EndPoint { get; }

    /// <summary>The configuration new connections get, or <see langword="null"/> before the first certificate.</summary>
    public ServedConfiguration? Current => Volatile.Read(ref _current);

    /// <summary>Every configuration created, oldest first: one per certificate applied.</summary>
    public ServedConfiguration[] Configurations
    {
        get
        {
            lock (_lock)
            {
                return [.. _configurations];
            }
        }
    }

    /// <summary>Every accepted connection, in the order the listener accepted them.</summary>
    public ServerConnection[] Connections
    {
        get
        {
            lock (_lock)
            {
                return [.. _connections];
            }
        }
    }

    /// <summary>Connections refused because no certificate had been applied yet.</summary>
    public int RefusedWithoutCertificate => Volatile.Read(ref _refused);

    /// <summary>The consumer contract: open a configuration for <paramref name="certificate"/> and give it to new connections.</summary>
    public void UpdateCertificate(X509Certificate2 certificate)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        // Open first: when MsQuic refuses the credential this throws, the binder reports it, and the previous configuration
        // stays in use, as the consumer contract requires.
        MsQuicConfiguration configuration = MsQuicConfiguration.CreateServer(_registration, [_alpn], certificate, _settings);
        ServedConfiguration? previous;
        lock (_lock)
        {
            ServedConfiguration served = new(configuration, certificate, _configurations.Count + 1);
            _configurations.Add(served);
            previous = _current;
            Volatile.Write(ref _current, served);
        }

        // Swapped (ADR 0009: open new, swap, close old): drop the replaced configuration's "current" reference.
        previous?.Release();
    }

    /// <summary>Waits for the connection the listener accepted from <paramref name="client"/> (matched by the client's UDP port).</summary>
    public async Task<ServerConnection> AcceptedFromAsync(QuicTestClient client, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            Task added;
            int? port = client.LocalEndPoint?.Port;
            lock (_lock)
            {
                ServerConnection? match = port is null ? null : _connections.Find(c => c.RemoteEndPoint?.Port == port);
                if (match is not null)
                {
                    return match;
                }

                added = _connectionAdded.Task;
            }

            TimeSpan left = deadline - DateTime.UtcNow;
            if (left <= TimeSpan.Zero)
            {
                throw new TimeoutException("Timed out after " + timeout + " waiting for the listener to accept the connection from local port "
                    + (port?.ToString(CultureInfo.InvariantCulture) ?? "(unknown)") + ".");
            }

            // The client's port may become known only once its handshake has progressed: poll it too, not just new connections.
            await Task.WhenAny(added, Task.Delay(TimeSpan.FromMilliseconds(Math.Min(50, left.TotalMilliseconds)))).ConfigureAwait(false);
        }
    }

    MsQuicConfiguration? IMsQuicListenerEvents.NewConnection(MsQuicListener listener, MsQuicConnection connection, in MsQuicNewConnectionInfo info)
    {
        ServedConfiguration? served;
        do
        {
            served = Volatile.Read(ref _current);
            if (served is null)
            {
                // No certificate yet: refuse before any TLS work.
                Interlocked.Increment(ref _refused);
                return null;
            }
        }
        while (!served.TryAddRef()); // lost a race with a swap that closed it: take the new one

        ServerConnection accepted = new(connection, served, Encoding.UTF8.GetString(info.ServerName), Encoding.ASCII.GetString(info.NegotiatedAlpn), info.RemoteAddress.ToIPEndPoint());
        connection.Events = accepted;
        TaskCompletionSource added;
        lock (_lock)
        {
            _connections.Add(accepted);
            added = _connectionAdded;
            _connectionAdded = E2eTimeouts.NewTcs();
        }

        added.TrySetResult();
        return served.Configuration;
    }

    /// <summary>Closes the listener (no callback runs afterwards), then every connection, then every configuration.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _listener.Close();
        foreach (ServerConnection connection in Connections)
        {
            await connection.CloseAsync(E2eTimeouts.Step).ConfigureAwait(false);
        }

        ServedConfiguration? current;
        lock (_lock)
        {
            current = _current;
            Volatile.Write(ref _current, null);
        }

        current?.Release();
        foreach (ServedConfiguration configuration in Configurations)
        {
            // Idempotent; also closes one whose handshaking connection never reported back (a refused connection).
            configuration.Close();
        }
    }
}
