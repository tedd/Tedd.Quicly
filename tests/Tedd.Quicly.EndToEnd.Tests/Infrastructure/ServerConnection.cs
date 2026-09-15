using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Transport.MsQuic;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>One MsQuic configuration the <see cref="QuicTestServer"/> adapter created for one certificate, and the references that keep it open.</summary>
internal sealed class ServedConfiguration
{
    private readonly TaskCompletionSource _closed = E2eTimeouts.NewTcs();
    private int _references = 1; // the "current" reference
    private int _closing;

    public ServedConfiguration(MsQuicConfiguration configuration, X509Certificate2 certificate, int generation)
    {
        Configuration = configuration;
        Certificate = certificate;
        Generation = generation;
        Thumbprint = certificate.Thumbprint;
        CredentialType = configuration.CredentialType;
    }

    public MsQuicConfiguration Configuration { get; }

    /// <summary>The certificate; the source owns it and the binder disposes it after its grace period.</summary>
    public X509Certificate2 Certificate { get; }

    /// <summary>1 for the first certificate applied, 2 for the next, and so on.</summary>
    public int Generation { get; }

    /// <summary>The certificate's thumbprint, captured while it was alive.</summary>
    public string Thumbprint { get; }

    /// <summary>How the credential reached MsQuic: CERTIFICATE_CONTEXT with the Schannel build, PKCS12 with OpenSSL builds.</summary>
    public QUIC_CREDENTIAL_TYPE CredentialType { get; }

    /// <summary>Completes once the adapter has closed the configuration.</summary>
    public Task WhenClosed => _closed.Task;

    public bool IsClosed => Volatile.Read(ref _closing) != 0;

    /// <summary>Adds a reference unless the configuration has already been released for good.</summary>
    public bool TryAddRef()
    {
        int current = Volatile.Read(ref _references);
        while (current > 0)
        {
            int seen = Interlocked.CompareExchange(ref _references, current + 1, current);
            if (seen == current)
            {
                return true;
            }

            current = seen;
        }

        return false;
    }

    /// <summary>Drops a reference; the last one closes the configuration (from a thread-pool thread when inside an MsQuic callback).</summary>
    public void Release()
    {
        if (Interlocked.Decrement(ref _references) != 0)
        {
            return;
        }

        if (MsQuicCallbackScope.IsInsideCallback)
        {
            ThreadPool.UnsafeQueueUserWorkItem(static self => self.Close(), this, preferLocal: false);
        }
        else
        {
            Close();
        }
    }

    /// <summary>Closes the configuration once. MsQuic keeps its own reference for connections that still use it.</summary>
    public void Close()
    {
        if (Interlocked.Exchange(ref _closing, 1) != 0)
        {
            return;
        }

        Configuration.Close();
        _closed.TrySetResult();
    }
}

/// <summary>A connection the listener accepted: what NEW_CONNECTION reported, its events, and its echo streams.</summary>
internal sealed class ServerConnection : IMsQuicConnectionEvents
{
    private readonly Lock _lock = new();
    private readonly List<MsQuicStream> _streams = [];
    private readonly TaskCompletionSource<string> _connected = E2eTimeouts.NewTcs<string>();
    private readonly TaskCompletionSource<bool> _shutdownComplete = E2eTimeouts.NewTcs<bool>();
    private ServedConfiguration? _lease;
    private bool _closing;
    private int _echoes;

    public ServerConnection(MsQuicConnection connection, ServedConfiguration configuration, string serverName, string negotiatedAlpn, IPEndPoint? remoteEndPoint)
    {
        Connection = connection;
        Configuration = configuration;
        ServerName = serverName;
        NegotiatedAlpn = negotiatedAlpn;
        RemoteEndPoint = remoteEndPoint;
        _lease = configuration;
    }

    public MsQuicConnection Connection { get; }

    /// <summary>The configuration (certificate generation) the listener handed this connection.</summary>
    public ServedConfiguration Configuration { get; }

    /// <summary>The SNI the client sent, as NEW_CONNECTION reported it.</summary>
    public string ServerName { get; }

    /// <summary>The ALPN MsQuic negotiated, as NEW_CONNECTION reported it.</summary>
    public string NegotiatedAlpn { get; }

    public IPEndPoint? RemoteEndPoint { get; }

    /// <summary>Completes with the ALPN of the server's CONNECTED event.</summary>
    public Task<string> WhenConnected => _connected.Task;

    /// <summary>Completes at SHUTDOWN_COMPLETE with whether the handshake had completed.</summary>
    public Task<bool> WhenShutdownComplete => _shutdownComplete.Task;

    /// <summary>Streams echoed so far.</summary>
    public int Echoes => Volatile.Read(ref _echoes);

    public void Connected(MsQuicConnection connection, ReadOnlySpan<byte> negotiatedAlpn, bool sessionResumed)
    {
        // TLS 1.3 needs the certificate's private key only during the handshake.
        ReleaseLease();
        _connected.TrySetResult(Encoding.ASCII.GetString(negotiatedAlpn));
    }

    public void ShutdownComplete(MsQuicConnection connection, bool handshakeCompleted, bool peerAcknowledgedShutdown, bool appCloseInProgress)
    {
        ReleaseLease();
        _shutdownComplete.TrySetResult(handshakeCompleted);
    }

    public bool PeerStreamStarted(MsQuicConnection connection, MsQuicStream stream, QUIC_STREAM_OPEN_FLAGS flags)
    {
        if ((flags & QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL) != 0)
        {
            return false;
        }

        lock (_lock)
        {
            if (_closing)
            {
                return false;
            }

            stream.Events = new EchoStream(this);
            _streams.Add(stream);
        }

        return true;
    }

    internal void OnEchoed() => Interlocked.Increment(ref _echoes);

    private void ReleaseLease() => Interlocked.Exchange(ref _lease, null)?.Release();

    /// <summary>Shuts the connection down unless it already is, then closes its streams and the connection from this thread.</summary>
    public async Task CloseAsync(TimeSpan timeout)
    {
        MsQuicStream[] streams;
        lock (_lock)
        {
            _closing = true;
            streams = [.. _streams];
        }

        if (!_shutdownComplete.Task.IsCompleted)
        {
            Connection.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
            try
            {
                await _shutdownComplete.Task.WaitAsync(timeout).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // Closed abortively below.
            }
        }

        foreach (MsQuicStream stream in streams)
        {
            stream.Close();
        }

        Connection.Close();
    }
}

/// <summary>Echoes a bidirectional stream: everything received goes back, with FIN, once the peer's FIN has arrived.</summary>
internal sealed unsafe class EchoStream(ServerConnection owner) : IMsQuicStreamEvents
{
    private readonly MemoryStream _received = new();

    public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
    {
        for (uint i = 0; i < bufferCount; i++)
        {
            _received.Write(buffers[i].Span);
        }

        if ((flags & QUIC_RECEIVE_FLAGS.FIN) != 0 && MsQuicStatus.Succeeded(NativePayload.Send(stream, _received.ToArray(), QUIC_SEND_FLAGS.FIN)))
        {
            owner.OnEchoed();
        }

        return MsQuicReceiveResult.Consumed(totalLength);
    }

    public void SendComplete(MsQuicStream stream, void* clientContext, bool canceled) => NativePayload.Free(clientContext);
}
