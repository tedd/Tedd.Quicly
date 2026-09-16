using System.Net;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Client;

/// <summary>
/// A QUICLY client (ARCHITECTURE.md §6.1): connects to a server over an <see cref="ITransportConnector"/> (the MsQuic
/// connector, or a simulated one in tests), authenticates, and reconnects after a lost connection according to a
/// <see cref="ReconnectPolicy"/>, resuming the session with its last token.
/// </summary>
/// <remarks>
/// <para><b>Use.</b> <c>await ConnectAsync(endpoint, options)</c> returns the connected <see cref="QuiclyPeer"/>. From then
/// on call <see cref="Poll"/> and <see cref="Flush"/> on the game thread instead of the peer's own methods: they poll the
/// current peer and drive reconnects. Send through <see cref="Peer"/>.</para>
/// <para><b>Reconnect.</b> After a lost connection the client resumes the session on the <em>same</em> peer
/// (<see cref="QuiclyPeer.Reconnect"/>): the peer keeps its message handlers, <see cref="QuiclyPeer.Index"/>,
/// <see cref="QuiclyPeer.Tag"/>, <see cref="QuiclyPeer.StateChanged"/> subscribers, channel table and statistics, presents
/// the session token of its last HelloAck, and the server answers with the same <see cref="QuiclyPeer.SessionId"/> and
/// epoch + 1 (PROTOCOL.md §4.1). The peer's own states are <see cref="PeerState.Closed"/> →
/// <see cref="PeerState.Reconnecting"/> → <see cref="PeerState.Handshaking"/> → <see cref="PeerState.Connected"/>. When the
/// server refuses the resume and <see cref="ReconnectPolicy.FallBackToNewSession"/> is set (or
/// <see cref="ReconnectPolicy.ResumeSession"/> is off), the attempt starts a fresh session with a <em>new</em> peer instead
/// — <see cref="ClientOptions.PeerCreated"/> runs for it and <see cref="ReconnectedInfo.PreviousPeer"/> is the peer it
/// replaced. The same happens when the <see cref="ITransportConnector"/> itself fails while the peer is being re-armed: that
/// peer cannot take another in-place resume, so the next attempt presents its session token on a new peer (the session is
/// still resumed). While reconnecting, <see cref="State"/> is <see cref="PeerState.Reconnecting"/> and <see cref="Peer"/> is
/// the peer being resumed (sends answer <see cref="SendStatus.NotConnected"/> until it is connected again).</para>
/// <para><b>Threads.</b> Not thread-safe: one thread at a time (the game thread, or <see cref="ConnectAsync"/>'s continuation
/// while it runs). Events are raised on that thread. <see cref="Dispose"/> is the exception: it may be called while
/// <see cref="ConnectAsync"/> waits (from another thread too), which ends that connect with an
/// <see cref="ObjectDisposedException"/> and disposes its peer.</para>
/// <para><b>Dispatch timing.</b> A peer dispatches received messages to its handlers whenever it is polled, and the client
/// polls a connecting peer itself: handlers registered in <see cref="ClientOptions.PeerCreated"/> can run before
/// <see cref="ConnectAsync"/> returns (the server's first messages right after its HelloAck), and a reconnect attempt's
/// handlers can run before <see cref="Reconnected"/> is raised.</para>
/// </remarks>
public sealed class QuiclyClient : IDisposable
{
    /// <summary>Longest time <see cref="ConnectAsync"/> sleeps without a transport callback before it polls again.</summary>
    public static readonly TimeSpan MaxConnectWait = TimeSpan.FromMilliseconds(100);

    private readonly ITransportConnector _connector;
    private readonly WorkSignal _signal = new();
    private readonly CancellationTokenSource _disposeCts = new();
    private ChannelTable? _table;
    private PeerOptions? _peerOptions;
    private ReconnectPolicy? _policy;
    private Action<QuiclyPeer>? _peerCreated;
    private string? _serverName;
    private EndPoint? _endpoint;
    private ReadOnlyMemory<byte> _authToken;
    private IClock _clock = MonotonicClock.Instance;
    private QuiclyPeer? _peer;
    private QuiclyPeer? _attempt;     // an attempt's own peer; null while a resume reconnects _peer in place
    private bool _attemptInPlace;     // the attempt is _peer itself (QuiclyPeer.Reconnect)
    private bool _inPlaceUnusable;    // that peer cannot take another in-place resume (a connector failure left it reset)
    private bool _attemptResumes;
    private ReadOnlyMemory<byte> _resumeToken;
    private uint _resumeEpoch;
    private ulong _resumeSessionId;
    private PeerState _state = PeerState.Closed;
    private int _attemptNumber;
    private long _nextAttemptMicros;
    private long _autoFlushMicros;
    private long _nextAutoFlush;
    private CloseReason _lastReason;
    private bool _closeRequested;
    private bool _connecting;
    private volatile bool _disposed; // Dispose may run while ConnectAsync waits

    /// <summary>Creates a client that connects through <paramref name="connector"/>.</summary>
    /// <param name="connector">Creates the transports.</param>
    /// <exception cref="ArgumentNullException"><paramref name="connector"/> is null.</exception>
    public QuiclyClient(ITransportConnector connector)
    {
        ArgumentNullException.ThrowIfNull(connector);
        _connector = connector;
    }

    /// <summary>
    /// The client's state (<see cref="PeerState.Handshaking"/> is not used): <see cref="PeerState.Closed"/> before a connect
    /// and after the client gave up or was closed, <see cref="PeerState.Connecting"/> during <see cref="ConnectAsync"/>,
    /// <see cref="PeerState.Connected"/>, <see cref="PeerState.Reconnecting"/> while a reconnect is scheduled or running, and
    /// <see cref="PeerState.Closing"/> while a close is in progress.
    /// </summary>
    public event Action<QuiclyClient, PeerState, PeerState>? StateChanged;

    /// <summary>The connection was lost and a reconnect attempt was scheduled (raised from <see cref="Poll"/>).</summary>
    public event Action<QuiclyClient, ReconnectingInfo>? Reconnecting;

    /// <summary>A reconnect succeeded (raised from <see cref="Poll"/>); <see cref="Peer"/> is the new peer.</summary>
    public event Action<QuiclyClient, ReconnectedInfo>? Reconnected;

    /// <summary>
    /// The client is disconnected for good (raised from <see cref="Poll"/> or <see cref="Close"/>): a deliberate or
    /// non-reconnectable close, the reconnect attempts ran out, or the server refused the session.
    /// </summary>
    public event Action<QuiclyClient, CloseReason>? Disconnected;

    /// <summary>The connector.</summary>
    public ITransportConnector Connector => _connector;

    /// <summary>
    /// The current peer: the connected one, or while reconnecting and after a disconnect the last one. A resumed reconnect
    /// keeps this object (handlers, <see cref="QuiclyPeer.Index"/>, <see cref="QuiclyPeer.Tag"/> and statistics survive);
    /// only a fallback to a fresh session replaces it.
    /// </summary>
    public QuiclyPeer? Peer => _peer;

    /// <summary>The client's state (see <see cref="StateChanged"/>).</summary>
    public PeerState State => _state;

    /// <summary>The server's endpoint of the last <see cref="ConnectAsync"/>.</summary>
    public EndPoint? RemoteEndPoint => _endpoint;

    /// <summary>The attempt number of the reconnect in progress (0 when not reconnecting).</summary>
    public int ReconnectAttempt => _attemptNumber;

    /// <summary>The auth token presented by later reconnect attempts (starts as <see cref="ClientOptions.AuthToken"/>; copied).</summary>
    /// <exception cref="ArgumentException">Longer than 4 096 bytes.</exception>
    public ReadOnlyMemory<byte> AuthToken
    {
        get => _authToken;
        set
        {
            if (value.Length > ControlCodec.MaxTokenLength)
            {
                throw new ArgumentException("An auth token is at most " + ControlCodec.MaxTokenLength + " bytes.", nameof(value));
            }

            _authToken = value.ToArray();
        }
    }

    /// <summary>Test seam: replaces the wait for a transport callback in <see cref="ConnectAsync"/> (a simulated network advances here).</summary>
    internal Func<TimeSpan, CancellationToken, ValueTask>? WaitOverride { get; set; }

    /// <summary>
    /// Connects to <paramref name="endpoint"/> and completes once the server admitted the session (or resumed the one named
    /// by <see cref="PeerOptions.SessionToken"/>). The peer is polled while the call waits, so its handlers may already run
    /// before the call returns; the wait ends on transport callbacks and deadlines, bounded by
    /// <see cref="PeerOptions.AdmissionTimeout"/>.
    /// </summary>
    /// <param name="endpoint">The server.</param>
    /// <param name="options">Channels, peer options, auth token, server name, reconnect policy.</param>
    /// <param name="cancellationToken">Cancels the connect (the peer is disposed).</param>
    /// <returns>The connected peer (also <see cref="Peer"/>).</returns>
    /// <exception cref="ArgumentException">An option is invalid.</exception>
    /// <exception cref="InvalidOperationException">The client is connected, connecting or reconnecting.</exception>
    /// <exception cref="QuiclyConnectException">The transport failed or the server refused the session.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> fired.</exception>
    /// <exception cref="ObjectDisposedException">The client is disposed, or was disposed while the connect ran (its peer is disposed too).</exception>
    public async ValueTask<QuiclyPeer> ConnectAsync(EndPoint endpoint, ClientOptions options, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        if (_connecting || _state != PeerState.Closed)
        {
            throw new InvalidOperationException("The client is connected or connecting; close it first.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        _peer?.Dispose();
        _peer = null;
        _table = options.Channels;
        PeerOptions peerOptions = options.PeerOptions.Clone();
        _autoFlushMicros = peerOptions.AutoFlushInterval.Ticks / TimeSpan.TicksPerMicrosecond;
        peerOptions.AutoFlushInterval = TimeSpan.Zero; // implemented by Poll, not by the peer
        peerOptions.WorkSignal = _signal;              // a transport callback wakes the wait inside ConnectAsync
        peerOptions.Validate();                        // the peer's own checks, before a transport is created
        _peerOptions = peerOptions;
        _clock = peerOptions.Clock ?? MonotonicClock.Instance;
        _policy = options.Reconnect?.Clone();
        _peerCreated = options.PeerCreated;
        _serverName = options.ServerName;
        _endpoint = endpoint;
        _authToken = options.AuthToken.ToArray();
        _closeRequested = false;
        _attemptNumber = 0;
        _inPlaceUnusable = false;
        _connecting = true;
        SetState(PeerState.Connecting);
        QuiclyPeer peer;
        try
        {
            peer = CreatePeer(peerOptions.SessionToken, peerOptions.LastEpoch);
        }
        catch
        {
            _connecting = false;
            SetState(PeerState.Closed);
            throw;
        }

        // Dispose cancels the wait through this token; the connect then ends with ObjectDisposedException.
        using CancellationTokenSource waitCancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token);
        try
        {
            while (true)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                cancellationToken.ThrowIfCancellationRequested();
                peer.Poll();
                peer.Flush();
                ObjectDisposedException.ThrowIf(_disposed, this); // a handler disposed the client
                if (peer.State == PeerState.Connected)
                {
                    _peer = peer;
                    _connecting = false;
                    _nextAutoFlush = _clock.NowMicros + _autoFlushMicros;
                    SetState(PeerState.Connected);
                    return peer;
                }

                if (peer.State == PeerState.Closed)
                {
                    throw Failure(peer);
                }

                try
                {
                    await WaitAsync(peer, waitCancel.Token).ConfigureAwait(false);
                }
                catch (Exception) when (_disposed)
                {
                    // Dispose cancelled the wait (or disposed the signal under it): the check above ends the connect.
                }
            }
        }
        catch
        {
            _connecting = false;
            peer.Dispose();
            SetState(PeerState.Closed);
            throw;
        }
    }

    /// <summary>
    /// Runs the client on the game thread: polls the current peer (see <see cref="QuiclyPeer.Poll"/>) and notices a lost
    /// connection, or drives the scheduled reconnect attempts (polling the attempt's peer, whose handlers may run before
    /// <see cref="Reconnected"/> is raised). Returns 0 while <see cref="ConnectAsync"/> runs.
    /// </summary>
    /// <param name="maxItems">Most messages to dispatch to handlers (the current peer's, or the reconnect attempt's).</param>
    /// <returns>Messages dispatched.</returns>
    /// <exception cref="ObjectDisposedException">The client is disposed.</exception>
    public int Poll(int maxItems = int.MaxValue)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_connecting)
        {
            return 0;
        }

        long now = _clock.NowMicros;
        switch (_state)
        {
            case PeerState.Connected:
            case PeerState.Closing:
                QuiclyPeer peer = _peer!;
                int dispatched = peer.Poll(maxItems);
                if (peer.State == PeerState.Closed)
                {
                    OnConnectionLost(peer, now);
                }
                else
                {
                    if (peer.State == PeerState.Closing)
                    {
                        SetState(PeerState.Closing);
                    }

                    if (_autoFlushMicros > 0 && now >= _nextAutoFlush)
                    {
                        _nextAutoFlush = now + _autoFlushMicros;
                        peer.Flush();
                    }
                }

                return dispatched;
            case PeerState.Reconnecting:
                return DriveReconnect(now, maxItems);
            default:
                return 0;
        }
    }

    /// <summary>Flushes the current peer (or the reconnect attempt in progress) on the game thread.</summary>
    /// <param name="tick">The simulation tick carried in packed containers; 0 = none.</param>
    /// <exception cref="ObjectDisposedException">The client is disposed.</exception>
    public void Flush(uint tick = 0)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_connecting)
        {
            return;
        }

        if (_state is PeerState.Connected or PeerState.Closing)
        {
            _peer!.Flush(tick);
        }
        else if (_state == PeerState.Reconnecting)
        {
            (_attemptInPlace ? _peer : _attempt)?.Flush(tick);
        }
    }

    /// <summary>
    /// Closes the connection and stops reconnecting: the peer sends Close (<see cref="Disconnected"/> follows from
    /// <see cref="Poll"/> once it closed), or a scheduled reconnect is abandoned (<see cref="Disconnected"/> at once).
    /// Ignored when already closed or closing. Cancel <see cref="ConnectAsync"/> with its token instead.
    /// </summary>
    /// <param name="reason">The close code and reason; <see langword="null"/> for <see cref="CloseReason.Normal"/>.</param>
    /// <exception cref="InvalidOperationException"><see cref="ConnectAsync"/> is running.</exception>
    /// <exception cref="ObjectDisposedException">The client is disposed.</exception>
    public void Close(CloseReason? reason = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_connecting)
        {
            throw new InvalidOperationException("ConnectAsync is running; cancel it with its token.");
        }

        CloseReason close = reason ?? CloseReason.Normal;
        switch (_state)
        {
            case PeerState.Connected:
                _closeRequested = true;
                _peer!.Close(close);
                SetState(PeerState.Closing);
                break;
            case PeerState.Reconnecting:
                _closeRequested = true;
                if (_attemptInPlace)
                {
                    _attemptInPlace = false;
                    _peer!.Close(close); // the resume in progress gives up with the client; the peer stays the application's
                }
                else
                {
                    _attempt?.Dispose();
                    _attempt = null;
                }

                _attemptNumber = 0;
                SetState(PeerState.Closed);
                Disconnected?.Invoke(this, close);
                break;
        }
    }

    /// <summary>
    /// Disposes the peers (closing their transports) and the client. A <see cref="ConnectAsync"/> that is waiting ends with an
    /// <see cref="ObjectDisposedException"/> and disposes the peer it drives, so no connection outlives the client.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _disposeCts.Cancel(); // wakes a waiting ConnectAsync
        _attempt?.Dispose();
        _peer?.Dispose();
        _attempt = null;
        _signal.Dispose();
    }

    private QuiclyPeer CreatePeer(ReadOnlyMemory<byte> sessionToken, uint lastEpoch)
    {
        PeerOptions options = _peerOptions!;
        options.SessionToken = sessionToken;
        options.LastEpoch = sessionToken.IsEmpty ? 0 : lastEpoch;
        QuiclyPeer peer = QuiclyPeer.Connect(_connector, _endpoint!, _serverName, _table!, options, _authToken.Span);
        try
        {
            _peerCreated?.Invoke(peer);
        }
        catch
        {
            peer.Dispose();
            throw;
        }

        return peer;
    }

    private ValueTask WaitAsync(QuiclyPeer peer, CancellationToken cancellationToken)
    {
        TimeSpan wait = peer.NextDeadline;
        if (wait == Timeout.InfiniteTimeSpan || wait > MaxConnectWait)
        {
            wait = MaxConnectWait;
        }

        return WaitOverride is { } hook ? hook(wait, cancellationToken) : _signal.WaitAsync(wait, cancellationToken);
    }

    private static QuiclyConnectException Failure(QuiclyPeer peer)
    {
        CloseReason reason = peer.CloseReason;
        HelloStatus status = peer.HandshakeStatus;
        string message = status != HelloStatus.Accepted
            ? "The server refused the session (" + status + (reason.Reason is null ? ")." : ": " + reason.Reason + ").")
            : "The connection closed before the session was admitted (" + reason.Code + ", " + reason.Source + (reason.Reason is null ? ")." : ": " + reason.Reason + ").");
        return new QuiclyConnectException(message, reason, status);
    }

    private void OnConnectionLost(QuiclyPeer peer, long now)
    {
        CloseReason reason = peer.CloseReason;
        _lastReason = reason;
        if (_closeRequested || _policy is null || _policy.MaxAttempts == 0 || !_policy.Allows(reason))
        {
            SetState(PeerState.Closed);
            Disconnected?.Invoke(this, reason);
            return;
        }

        _resumeToken = _policy.ResumeSession ? peer.SessionToken : default;
        _resumeEpoch = peer.Epoch;
        _resumeSessionId = peer.SessionId;
        _attemptNumber = 0;
        SetState(PeerState.Reconnecting);
        ScheduleAttempt(now);
    }

    private void ScheduleAttempt(long now)
    {
        _attemptNumber++;
        ReconnectPolicy policy = _policy!;
        TimeSpan delay = policy.GetDelay(_attemptNumber, (policy.Random ?? Random.Shared).NextDouble());
        _nextAttemptMicros = now + (delay.Ticks / TimeSpan.TicksPerMicrosecond);
        Reconnecting?.Invoke(this, new ReconnectingInfo(_attemptNumber, delay, _lastReason));
    }

    private int DriveReconnect(long now, int maxItems)
    {
        QuiclyPeer? attempt = _attemptInPlace ? _peer : _attempt;
        if (attempt is null)
        {
            if (now < _nextAttemptMicros)
            {
                return 0;
            }

            try
            {
                attempt = StartAttempt();
            }
            catch (Exception exception)
            {
                // The connector failed synchronously (no transport): count it like a failed attempt.
                _lastReason = new CloseReason(QuiclyErrorCode.InternalError, Truncate(exception.Message)) { Source = CloseSource.Transport };
                AttemptFailed(now, HelloStatus.Accepted);
                return 0;
            }
        }

        int dispatched = attempt.Poll(maxItems);
        attempt.Flush();
        if (attempt.State == PeerState.Connected)
        {
            CompleteReconnect(attempt);
            return dispatched;
        }

        if (attempt.State != PeerState.Closed)
        {
            return dispatched;
        }

        HelloStatus status = attempt.HandshakeStatus;
        _lastReason = attempt.CloseReason;
        if (_attemptInPlace)
        {
            // The peer stays (its handlers and statistics are the application's): the next attempt reconnects it again, or a
            // fresh session replaces it.
            _attemptInPlace = false;
        }
        else
        {
            _attempt = null;
            attempt.Dispose();
        }

        AttemptFailed(now, status);
        return dispatched;
    }

    /// <summary>
    /// Starts the next attempt (game thread): a resume reconnects the current peer in place
    /// (<see cref="QuiclyPeer.Reconnect"/>, which keeps its handlers, <see cref="QuiclyPeer.Index"/>,
    /// <see cref="QuiclyPeer.Tag"/> and statistics and presents the session token of its last HelloAck), while a fresh
    /// session gets a new peer through <see cref="ClientOptions.PeerCreated"/>.
    /// </summary>
    /// <returns>The peer the attempt runs on.</returns>
    private QuiclyPeer StartAttempt()
    {
        _attemptResumes = !_resumeToken.IsEmpty;
        if (_attemptResumes && !_inPlaceUnusable)
        {
            QuiclyPeer peer = _peer!;
            try
            {
                peer.Reconnect(_connector, _endpoint!, _serverName, _authToken.Span);
            }
            catch
            {
                // The connector failed while the peer was being re-armed, which leaves that peer unable to take another
                // in-place resume: the attempts that follow present the session token on a new peer instead, so the session
                // can still be resumed.
                _inPlaceUnusable = true;
                throw;
            }

            _attemptInPlace = true;
            return peer;
        }

        // A fresh session (no resume token), or a resume the current peer can no longer make in place.
        _attempt = CreatePeer(_resumeToken, _resumeEpoch);
        return _attempt;
    }

    private void AttemptFailed(long now, HelloStatus status)
    {
        ReconnectPolicy policy = _policy!;
        if (status == HelloStatus.Rejected && _attemptResumes && policy.FallBackToNewSession)
        {
            _resumeToken = default; // the session is gone: the next attempt starts a fresh one
        }
        else if (status is HelloStatus.Rejected or HelloStatus.VersionMismatch or HelloStatus.ChannelTableMismatch or HelloStatus.DatagramsRequired)
        {
            GiveUp(); // retrying cannot change the answer
            return;
        }

        if (_attemptNumber >= policy.MaxAttempts)
        {
            GiveUp();
            return;
        }

        ScheduleAttempt(now);
    }

    private void GiveUp()
    {
        _attemptNumber = 0;
        SetState(PeerState.Closed);
        Disconnected?.Invoke(this, _lastReason);
    }

    private void CompleteReconnect(QuiclyPeer attempt)
    {
        QuiclyPeer previous = _peer!;
        bool inPlace = _attemptInPlace;
        _peer = attempt;
        _attempt = null;
        _attemptInPlace = false;
        _inPlaceUnusable = false; // whichever peer is current now can be resumed in place again
        bool resumed = _attemptResumes && attempt.SessionId == _resumeSessionId;
        int attempts = _attemptNumber;
        _attemptNumber = 0;
        _nextAutoFlush = _clock.NowMicros + _autoFlushMicros;
        SetState(PeerState.Connected);
        try
        {
            Reconnected?.Invoke(this, new ReconnectedInfo(previous, attempt, resumed, attempts));
        }
        finally
        {
            if (!inPlace)
            {
                previous.Dispose(); // a fresh session replaced the lost connection's peer
            }
        }
    }

    private void SetState(PeerState next)
    {
        PeerState previous = _state;
        if (previous == next)
        {
            return;
        }

        _state = next;
        StateChanged?.Invoke(this, previous, next);
    }

    private static string Truncate(string text) => text.Length <= 200 ? text : text[..200];
}
