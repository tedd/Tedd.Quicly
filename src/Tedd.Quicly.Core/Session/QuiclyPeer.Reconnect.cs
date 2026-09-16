using System.Net;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

// Game thread: in-place session resume over a new transport (PROTOCOL.md §4.1, docs/design/session-layer.md §4.8).
public sealed unsafe partial class QuiclyPeer
{
    /// <summary>
    /// Resumes this client peer's session over a new transport after the connection was lost (transport close, link loss or
    /// heartbeat timeout): the peer keeps its identity — <see cref="Index"/>, <see cref="Tag"/>, the registered
    /// <see cref="MessageHandler"/>s, the <see cref="StateChanged"/> subscribers, every counter in
    /// <see cref="GetStatistics"/> and its channel table — moves to <see cref="PeerState.Reconnecting"/>, attaches the
    /// transport <paramref name="connector"/> creates, and sends Hello with the session token of its last HelloAck and
    /// <c>LastEpoch</c> = the current <see cref="Epoch"/>. The server answers with a higher epoch when it accepts the
    /// resume, or with epoch 1 (a fresh session) when the token is unusable (PROTOCOL.md §4.1).
    /// </summary>
    /// <remarks>
    /// <para><b>What is reset.</b> Everything bound to the lost transport: the control stream, the stream table, the
    /// hand-off rings (receive, completion, pended streams, pong samples), the datagram capabilities and the session message
    /// cap, the ping clock and its RTT window, the close reason, the admission deadline, the container packer, the segment
    /// arena and every engine's streams (<see cref="ChannelEngine.OnReconnecting"/>). Sends the lost connection still held
    /// complete <see cref="Threading.DeliveryStatus.Disconnected"/> — queued and in flight alike — and every receive lease
    /// goes back to the pool, so a <c>SendShared</c> payload's reference is dropped here too. Waiting
    /// <see cref="SendAsync"/> calls answer <see cref="SendStatus.NotConnected"/> and <see cref="FlushAsync"/> calls
    /// complete. When the host has already polled the peer to <see cref="PeerState.Closed"/> that work is done and this call
    /// only re-arms the peer.</para>
    /// <para><b>Channel rules on acceptance</b> (PROTOCOL.md §4.1, applied from <see cref="ChannelEngine.OnEpochReset"/>
    /// with <c>resumed</c> true when the epoch advanced): <see cref="Channels.ChannelMode.UnreliableSequenced"/> sequence
    /// and key tables start over; in-flight <see cref="Channels.ChannelMode.ReliableOrdered"/> sends already completed
    /// <c>Disconnected</c> and are the application's responsibility; live <see cref="Channels.ChannelMode.ReliableLatest"/>
    /// keys are re-queued at their current version and resumable <see cref="Channels.ChannelMode.Bulk"/> transfers are
    /// re-requested by their engines (wave C2; the placeholder engines do nothing).</para>
    /// <para><b>Server side.</b> A server cannot do this: the listener's <see cref="AcceptCallback"/> has to return an
    /// <see cref="ITransportSink"/> synchronously, before a single QUICLY byte was read, so the server does not yet know
    /// which session the new connection belongs to — the token only arrives in the Hello, which is parsed by the sink it
    /// already had to hand out. A resumed connection therefore always gets a <em>new</em> server peer
    /// (<see cref="CreateServerPeer"/>), and the host carries the session identity over when
    /// <see cref="IPeerAdmission.Admit"/> matches the token: it copies <see cref="Index"/> and <see cref="Tag"/> and its own
    /// per-session application state onto the new peer, registers the same handlers, answers
    /// <see cref="AdmissionResult.Accept"/> with the session's id and the next epoch, and closes the peer of the replaced
    /// connection with <see cref="QuiclyErrorCode.SessionReplaced"/>. Per-peer statistics do not carry over (they belong to
    /// the peer object); a host that reports per session aggregates them itself.</para>
    /// <para>May run the continuations of sends the lost connection left behind, exactly as the Closed step of
    /// <see cref="Poll"/> does.</para>
    /// </remarks>
    /// <param name="connector">Creates the new transport (callbacks may arrive before this method returns).</param>
    /// <param name="endpoint">The server to connect to (it may differ from the original one).</param>
    /// <param name="serverName">TLS server name, or <see langword="null"/>.</param>
    /// <param name="authToken">
    /// Authentication token for the resumed Hello (copied). A resume must present one the admission policy accepts: the
    /// session token is a locator, not a credential (PROTOCOL.md §4.1). It replaces the token of the previous attempt.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="connector"/> or <paramref name="endpoint"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="authToken"/> is longer than 4 096 bytes.</exception>
    /// <exception cref="InvalidOperationException">
    /// The peer is a server, the call is made from inside <see cref="Poll"/> or <see cref="Flush"/>, or the lost transport
    /// has not reported its close yet (poll until <see cref="PeerState.Closed"/> first).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The peer is disposed.</exception>
    public void Reconnect(ITransportConnector connector, EndPoint endpoint, string? serverName, ReadOnlySpan<byte> authToken)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(connector);
        ArgumentNullException.ThrowIfNull(endpoint);
        if (_role != PeerRole.Client)
        {
            throw new InvalidOperationException(
                "Only a client peer can reconnect: a server gets a new peer for a resumed connection, because its accept callback must return a sink before the Hello is read (PROTOCOL.md §4.1).");
        }

        if (authToken.Length > ControlCodec.MaxTokenLength)
        {
            throw new ArgumentException($"An auth token is at most {ControlCodec.MaxTokenLength} bytes.", nameof(authToken));
        }

        if (_inPoll || _inFlush)
        {
            throw new InvalidOperationException("Reconnect cannot run inside Poll or Flush.");
        }

        if (!_core.IsTransportClosed)
        {
            throw new InvalidOperationException("Reconnect needs the lost connection's transport to have reported its close: poll until PeerState.Closed.");
        }

        EnterCall();
        try
        {
            SettleLostConnection();
            long now = _clock.NowMicros;
            _authToken = authToken.ToArray();
            // The token of the last HelloAck is what locates the session; the epoch we reached is informational.
            _resumeToken = _sessionToken.ToArray();
            _lastEpoch = _core.Epoch;
            ResetForReconnect(now);
            SetState(PeerState.Reconnecting);
            _remoteEndPoint = endpoint as IPEndPoint;
            ITransport transport;
            try
            {
                transport = connector.Connect(endpoint, serverName, _sink);
            }
            catch
            {
                // No transport means no close callback, so the peer must not be left waiting in Reconnecting.
                _transitionCount = 0;
                _state = PeerState.Closed;
                _closedRaised = true;
                throw;
            }

            AttachTransport(transport);
        }
        finally
        {
            ExitCall();
        }
    }

    /// <summary>
    /// Finishes what the lost connection left behind, exactly as the Closed step of <see cref="Poll"/> does: in-flight and
    /// queued sends complete <see cref="Threading.DeliveryStatus.Disconnected"/>, every receive lease and every send queued
    /// by another thread goes back, and waiting <see cref="SendAsync"/> and <see cref="FlushAsync"/> calls complete. Does
    /// nothing when the host already polled the peer to <see cref="PeerState.Closed"/>.
    /// </summary>
    private void SettleLostConnection()
    {
        if (_closedRaised)
        {
            return;
        }

        DrainCompletions();
        ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
        for (int i = 0; i < engines.Length; i++)
        {
            engines[i].OnPeerClosed();
        }

        DrainCompletions();
        ReleaseForeignSends();
        ReleaseAllReceived();
        CompleteSendWaiters(exception: null);
        CompleteFlushWaiters(all: true);
    }

    /// <summary>
    /// Puts the peer back into its pre-handshake shape for a resumed session (game thread): the lost transport is disposed,
    /// the shared state is cleared (<see cref="PeerCore.ResetForReconnect"/>), and every per-connection field of the peer
    /// itself — signals, handshake bodies, the control stream, the close reason, the ping clock, the deadlines — starts
    /// over. The rate limiters keep their state on purpose: they are per peer, not per connection, and refill with time.
    /// </summary>
    /// <param name="now">Clock micros of this call.</param>
    private void ResetForReconnect(long now)
    {
        ITransport? old = _transport;
        _transport = null;
        old?.Dispose();

        // The new transport will call back again, so the old close must not let Dispose free the native memory early.
        Interlocked.And(ref _lifetime, ~LifetimeClosedSeen);
        _core.ResetForReconnect();

        _signals = 0;
        _helloBody = null;
        _helloAckBody = null;
        Volatile.Write(ref _tableInfoBody, null);
        _peerCloseCode = QuiclyErrorCode.NoError;
        _peerCloseReason = null;
        _transportCloseCode = 0;
        _transportCloseStatus = 0;
        _handshakeMessageSeen = false;
        _controlAssemblyFill = 0;
        _controlPreambleSent = false;
        SetControlStreamId(TransportStreamId.None);
        _ignoreIncoming = false;
        _controlPool.Reset();
        while (_pongs.TryDequeue(out _))
        {
        }

        while (_streamPings.TryDequeue(out _))
        {
        }

        _recentPingValid = 0;
        _recentPingNext = 0;
        _ping = default;
        _closeReason = default;
        _closeCode = QuiclyErrorCode.NoError;
        _closeReasonUtf8 = null;
        _terminalSlot = -1;
        _lingerDeadline = 0;
        _transportCloseCalled = false;
        _admissionPending = false;
        _handshakeStatus = HelloStatus.Accepted;
        _remoteTable = null;
        _closedRaised = false;
        _transitionCount = 0;
        _queuedWithHandler = 0;
        _lastTick = 0;
        _connectedAtMicros = 0;
        _nextPingMicros = 0;
        _fastLockEndMicros = 0;
        _engineDeadline = long.MaxValue;
        _admissionDeadline = now + _admissionTimeoutMicros;
        _timerDeadline = _admissionDeadline;
        _nextDeadlineMicros = _admissionDeadline;
        Volatile.Write(ref _lastReceiveMicros, now);
        _core.ConnectionStartMicros = now;
        _core.NotePass(now);
        Volatile.Write(ref _workSignalled, 0);
    }
}
