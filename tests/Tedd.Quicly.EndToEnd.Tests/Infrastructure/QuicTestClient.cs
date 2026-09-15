using System.Globalization;
using System.Net;
using System.Text;
using Tedd.Quicly.Transport.MsQuic;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>
/// A real MsQuic client connection offering one ALPN. It connects to the listener's loopback address while presenting a
/// server name (the test identifier, which does not resolve) as SNI, applies the configured certificate validation, and can
/// echo a payload on a bidirectional stream.
/// </summary>
internal sealed class QuicTestClient : IMsQuicConnectionEvents, IAsyncDisposable
{
    private readonly TaskCompletionSource<string> _connected = E2eTimeouts.NewTcs<string>();
    private readonly TaskCompletionSource<int> _transportShutdown = E2eTimeouts.NewTcs<int>();
    private readonly TaskCompletionSource<ulong> _peerShutdown = E2eTimeouts.NewTcs<ulong>();
    private readonly TaskCompletionSource<bool> _shutdownComplete = E2eTimeouts.NewTcs<bool>();
    private readonly Func<PresentedCertificate, bool>? _validator;
    private MsQuicConnection? _connection;
    private PresentedCertificate? _presented;
    private Exception? _validatorFailure;
    private IPEndPoint? _localEndPoint;
    private int _disposed;

    private QuicTestClient(string serverName, Func<PresentedCertificate, bool>? validator)
    {
        ServerName = serverName;
        _validator = validator;
    }

    /// <summary>Starts a connection to <paramref name="server"/> with <paramref name="serverName"/> as SNI.</summary>
    public static QuicTestClient Start(MsQuicRegistration registration, MsQuicConfiguration configuration, IPEndPoint server, string serverName, Func<PresentedCertificate, bool>? validator)
    {
        QuicTestClient client = new(serverName, validator);
        MsQuicConnection connection = new(registration, client);
        client._connection = connection;
        try
        {
            // Connect to the listener's loopback address but present serverName as SNI: with the remote address set, MsQuic
            // uses the server name only for SNI and certificate validation (it would otherwise resolve it), as System.Net.Quic does.
            QUIC_ADDR remote = QUIC_ADDR.FromIPEndPoint(server);
            MsQuicException.ThrowIfFailed(connection.SetParam(MsQuicParam.QUIC_PARAM_CONN_REMOTE_ADDRESS, in remote), "SetParam(QUIC_PARAM_CONN_REMOTE_ADDRESS)");
            MsQuicException.ThrowIfFailed(connection.Start(configuration, serverName, (ushort)server.Port, QuicAddressFamily.INET), "ConnectionStart");
        }
        catch
        {
            connection.Close();
            throw;
        }

        return client;
    }

    public MsQuicConnection Connection => _connection ?? throw new InvalidOperationException("Not started.");

    /// <summary>The server name sent as SNI.</summary>
    public string ServerName { get; }

    /// <summary>What the server presented, when the client validates in the callback mode.</summary>
    public PresentedCertificate? PresentedCertificate => Volatile.Read(ref _presented);

    /// <summary>An exception the validator threw (the certificate was then rejected).</summary>
    public Exception? ValidatorFailure => Volatile.Read(ref _validatorFailure);

    /// <summary>The client's local UDP end point, once MsQuic has bound one.</summary>
    public IPEndPoint? LocalEndPoint
    {
        get
        {
            if (Volatile.Read(ref _localEndPoint) is { } known)
            {
                return known;
            }

            return Volatile.Read(ref _disposed) == 0 && _connection is { IsClosed: false } connection ? connection.LocalEndPoint : null;
        }
    }

    /// <summary>Completes at SHUTDOWN_COMPLETE with whether the handshake had completed.</summary>
    public Task<bool> WhenShutdownComplete => _shutdownComplete.Task;

    /// <summary>Whether the connection has begun shutting down, for whatever reason.</summary>
    public bool IsShuttingDown => _transportShutdown.Task.IsCompleted || _peerShutdown.Task.IsCompleted || _shutdownComplete.Task.IsCompleted;

    /// <summary>Completes with the negotiated ALPN once the handshake succeeded; throws when the connection shut down without completing it.</summary>
    public async Task<string> HandshakeAsync(TimeSpan timeout)
    {
        await Task.WhenAny(_connected.Task, _shutdownComplete.Task).Within(timeout, "the QUIC handshake with " + ServerName);
        if (_connected.Task.IsCompletedSuccessfully)
        {
            return _connected.Task.Result;
        }

        throw new InvalidOperationException("The QUIC handshake with " + ServerName + " failed: " + DescribeShutdown() + ".");
    }

    /// <summary>Waits for a handshake that must fail and returns the status the transport closed the connection with.</summary>
    public async Task<int> HandshakeFailureAsync(TimeSpan timeout)
    {
        bool handshakeCompleted = await _shutdownComplete.Task.Within(timeout, "the rejected QUIC handshake with " + ServerName + " to end");
        if (handshakeCompleted || _connected.Task.IsCompleted)
        {
            throw new InvalidOperationException("The QUIC handshake with " + ServerName + " succeeded, but it was expected to fail.");
        }

        return _transportShutdown.Task.IsCompleted
            ? _transportShutdown.Task.Result
            : throw new InvalidOperationException("The handshake failed without a transport status: " + DescribeShutdown() + ".");
    }

    /// <summary>The negotiated TLS parameters (valid after the handshake).</summary>
    public QUIC_HANDSHAKE_INFO HandshakeInfo()
    {
        MsQuicException.ThrowIfFailed(Connection.GetParam(MsQuicParam.QUIC_PARAM_TLS_HANDSHAKE_INFO, out QUIC_HANDSHAKE_INFO info), "GetParam(QUIC_PARAM_TLS_HANDSHAKE_INFO)");
        return info;
    }

    /// <summary>Sends <paramref name="payload"/> on a new bidirectional stream with FIN and returns what the server echoed.</summary>
    public async Task<byte[]> EchoAsync(byte[] payload, TimeSpan timeout)
    {
        ClientStream events = new();
        MsQuicException.ThrowIfFailed(Connection.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, events, out MsQuicStream? opened), "StreamOpen");
        MsQuicStream stream = opened!;
        try
        {
            MsQuicException.ThrowIfFailed(stream.Start(QUIC_STREAM_START_FLAGS.NONE), "StreamStart");
            MsQuicException.ThrowIfFailed(NativePayload.Send(stream, payload, QUIC_SEND_FLAGS.FIN), "StreamSend");
            byte[] echoed = await events.WhenReceived.Within(timeout, "the echo of " + payload.Length.ToString(CultureInfo.InvariantCulture) + " bytes");
            await events.WhenShutdownComplete.Within(timeout, "the echo stream to shut down");
            return echoed;
        }
        finally
        {
            stream.Close();
        }
    }

    public void Connected(MsQuicConnection connection, ReadOnlySpan<byte> negotiatedAlpn, bool sessionResumed)
    {
        CaptureLocalEndPoint(connection);
        _connected.TrySetResult(Encoding.ASCII.GetString(negotiatedAlpn));
    }

    public void ShutdownInitiatedByTransport(MsQuicConnection connection, int status, ulong errorCode) => _transportShutdown.TrySetResult(status);

    public void ShutdownInitiatedByPeer(MsQuicConnection connection, ulong errorCode) => _peerShutdown.TrySetResult(errorCode);

    public void ShutdownComplete(MsQuicConnection connection, bool handshakeCompleted, bool peerAcknowledgedShutdown, bool appCloseInProgress)
    {
        CaptureLocalEndPoint(connection);
        _shutdownComplete.TrySetResult(handshakeCompleted);
    }

    public MsQuicCertificateDecision PeerCertificateReceived(MsQuicConnection connection, in MsQuicPeerCertificateInfo info)
    {
        CaptureLocalEndPoint(connection);
        PresentedCertificate presented = new(info.CertificateDer.ToArray(), info.ChainPkcs7.ToArray(), info.DeferredStatus, info.DeferredErrorFlags);
        Volatile.Write(ref _presented, presented);
        try
        {
            return _validator?.Invoke(presented) == true ? MsQuicCertificateDecision.Accept : MsQuicCertificateDecision.Reject;
        }
        catch (Exception e)
        {
            Volatile.Write(ref _validatorFailure, e);
            return MsQuicCertificateDecision.Reject;
        }
    }

    /// <summary>Shuts the connection down (unless it already is) and closes it from this thread.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0 || _connection is null)
        {
            return;
        }

        if (!_shutdownComplete.Task.IsCompleted)
        {
            _connection.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
            try
            {
                await _shutdownComplete.Task.WaitAsync(E2eTimeouts.Step).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // Closed abortively below.
            }
        }

        _connection.Close();
    }

    private void CaptureLocalEndPoint(MsQuicConnection connection)
    {
        if (Volatile.Read(ref _localEndPoint) is null && connection.LocalEndPoint is { } local)
        {
            Volatile.Write(ref _localEndPoint, local);
        }
    }

    private string DescribeShutdown()
    {
        List<string> parts = [];
        if (_transportShutdown.Task.IsCompleted)
        {
            parts.Add("transport status " + MsQuicStatus.GetName(_transportShutdown.Task.Result));
        }

        if (_peerShutdown.Task.IsCompleted)
        {
            parts.Add("peer error code " + _peerShutdown.Task.Result.ToString(CultureInfo.InvariantCulture));
        }

        if (ValidatorFailure is { } failure)
        {
            parts.Add("the validator threw " + failure.GetType().Name + ": " + failure.Message);
        }

        return parts.Count == 0 ? "no shutdown reason reported" : string.Join(", ", parts);
    }

    /// <summary>Collects the echoed bytes of one client stream.</summary>
    private sealed unsafe class ClientStream : IMsQuicStreamEvents
    {
        private readonly MemoryStream _received = new();
        private readonly TaskCompletionSource<byte[]> _receivedAll = E2eTimeouts.NewTcs<byte[]>();
        private readonly TaskCompletionSource _shutdownComplete = E2eTimeouts.NewTcs();

        public Task<byte[]> WhenReceived => _receivedAll.Task;

        public Task WhenShutdownComplete => _shutdownComplete.Task;

        public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
        {
            for (uint i = 0; i < bufferCount; i++)
            {
                _received.Write(buffers[i].Span);
            }

            if ((flags & QUIC_RECEIVE_FLAGS.FIN) != 0)
            {
                _receivedAll.TrySetResult(_received.ToArray());
            }

            return MsQuicReceiveResult.Consumed(totalLength);
        }

        public void SendComplete(MsQuicStream stream, void* clientContext, bool canceled) => NativePayload.Free(clientContext);

        public void PeerSendAborted(MsQuicStream stream, ulong errorCode)
        {
            _receivedAll.TrySetException(new IOException("The server aborted the echo stream with error code " + errorCode.ToString(CultureInfo.InvariantCulture) + "."));
        }

        public void ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info)
        {
            _receivedAll.TrySetException(new IOException("The echo stream shut down before the echo arrived (connection shut down: " + info.ConnectionShutdown + ")."));
            _shutdownComplete.TrySetResult();
        }
    }
}
