using System.Net;
using System.Security.Cryptography;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Server.Certificates;

namespace Tedd.Quicly.Server;

/// <summary>
/// A QUICLY game server (ARCHITECTURE.md §6.1): accepts connections from an <see cref="ITransportListener"/>, admits
/// sessions (<see cref="IAdmissionPolicy"/>, ADR 0009), keeps every peer in a dense, generation-tagged slot table
/// (<see cref="Peers"/>), polls them from the game thread (<see cref="PollAll"/>), sends one serialisation to many peers
/// (<see cref="SendShared"/>), mints and resumes session tokens (PROTOCOL.md §4.1), and optionally provisions its certificate
/// and runs a plain-HTTP side endpoint.
/// </summary>
/// <remarks>
/// <para><b>Threads.</b> The game thread (whichever thread calls <see cref="PollAll"/>, <see cref="FlushAll"/>,
/// <see cref="SendShared"/>, <see cref="BeginShutdown"/> and the peers' own members, one at a time) owns the slot table,
/// the sessions and the events: every event is raised on it, from <see cref="PollAll"/> (or <see cref="StopAsync"/>). The
/// listener's callbacks run on transport threads; they reserve a slot and create the peer, which the next
/// <see cref="PollAll"/> activates, and queue the failures they find. <see cref="CompleteAdmission(QuiclyPeer, bool, string?)"/>
/// may be called from any thread; the decision is applied by the next <see cref="PollAll"/>.</para>
/// <para><b>Work tracking.</b> The server hands each transport a sink that forwards to the peer and then sets the peer's
/// bit in an atomic work bitset, and it keeps every peer's next deadline in a dense array. <see cref="PollAll"/> polls only
/// peers with a set bit or a due deadline, so idle peers cost a bit test and a (vectorised) deadline comparison.</para>
/// <para><b>Ownership.</b> The server owns the listener (stopped by <see cref="StopAsync"/>, disposed by
/// <see cref="DisposeAsync"/>), every peer it accepts (disposed right after <see cref="PeerClosed"/>: close peers with
/// <see cref="QuiclyPeer.Close(CloseReason)"/>, never dispose them), and the shared buffer pool it created
/// (<see cref="Allocator"/>, disposed once every peer freed its memory). Release shared leases before disposing the server.</para>
/// </remarks>
public sealed partial class QuiclyServer : IAsyncDisposable
{
    private const int StateCreated = 0;
    private const int StateStarting = 1;
    private const int StateRunning = 2;
    private const int StateStopping = 3;
    private const int StateStopped = 4;

    /// <summary>Most <see cref="AdmissionFailed"/> or <see cref="CertificateConsumerFailed"/> events waiting for <see cref="PollAll"/>.</summary>
    internal const int MaxQueuedEvents = 4096;

    /// <summary>Default replay-cache entries per expected peer (a consumed token is held until it expires).</summary>
    internal const int ReplayEntriesPerExpectedPeer = 32;

    private readonly ITransportListener _listener;
    private readonly ChannelTable _table;
    private readonly PeerOptions _peerOptions;
    private readonly IClock _clock;
    private readonly SlabAllocator _allocator;
    private readonly bool _ownsAllocator;
    private readonly SharedLeaseTable _sharedLeases;
    private readonly SessionTokenAuthority _tokens;
    private readonly AuthFailureRateLimiter _rateLimiter;
    private readonly SessionRegistry _sessions;
    private readonly DefaultAdmissionPolicy _defaultPolicy;
    private readonly HelloAdmission _helloAdmission;
    private readonly Action<QuiclyPeer, PeerState, PeerState> _onStateChanged;
    private readonly PreHandshakeCallback _preHandshake;
    private readonly AcceptCallback _accept;
    private readonly int _maxPeers;
    private readonly int _maxUnadmitted;
    private readonly int _maxPerAddress;
    private readonly int _ipv6PrefixLength;
    private readonly long _graceMicros;
    private readonly long _tokenLifetimeMicros;
    private readonly long _autoFlushMicros;
    private readonly TimeSpan _shutdownTimeout;
    private readonly CloseReason _shutdownReason;
    private readonly ServerCertificateOptions? _certificateOptions;
    private readonly ICertificateConsumer[] _certificateConsumers;
    private readonly CertificateBinderOptions? _binderOptions;
    private readonly bool _waitForCertificate;
    private readonly HttpSideOptions? _httpSide;
    private readonly EventQueue<AdmissionFailure> _failures = new(MaxQueuedEvents);
    private readonly EventQueue<CertificateConsumerFailure> _consumerFailures = new(MaxQueuedEvents);
    private readonly Action<AdmissionFailure> _raiseFailure;
    private readonly Action<CertificateConsumerFailure> _raiseConsumerFailure;
    private IAdmissionPolicy _policy;
    private int _state;
    private int _disposing;
    private bool _disposed;

    /// <summary>Creates a server; nothing is accepted until <see cref="StartAsync"/>.</summary>
    /// <param name="options">The configuration (read once, here).</param>
    /// <param name="listener">Accepts the connections (the MsQuic listener, or a simulated one in tests); owned by the server from now on.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    /// <exception cref="ArgumentException">An option is missing or out of range, including the peer options template.</exception>
    public QuiclyServer(ServerOptions options, ITransportListener listener)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(listener);
        options.Validate();
        _listener = listener;
        _raiseFailure = RaiseFailure;
        _raiseConsumerFailure = RaiseConsumerFailure;
        _table = options.Channels!;
        PeerOptions template = options.PeerOptions;
        _clock = template.Clock ?? throw new ArgumentException("PeerOptions.Clock is required.", nameof(options));
        Sizing = ServerSizing.Compute(options, out SlabAllocatorOptions? poolOptions);
        ServerAdmissionOptions admission = options.Admission;
        _maxPeers = options.MaxPeers;
        _maxUnadmitted = admission.MaxUnadmittedConnections;
        _maxPerAddress = admission.MaxConnectionsPerAddress;
        _ipv6PrefixLength = admission.IPv6PrefixLength;
        _graceMicros = ToMicros(options.Sessions.Grace);
        _tokenLifetimeMicros = ToMicros(options.Sessions.TokenLifetime);
        _autoFlushMicros = ToMicros(template.AutoFlushInterval);
        _shutdownTimeout = options.ShutdownTimeout;
        _shutdownReason = options.ShutdownReason;
        _certificateOptions = options.Certificate;
        _certificateConsumers = [.. options.CertificateConsumers];
        _binderOptions = options.CertificateBinding;
        _waitForCertificate = options.WaitForCertificate;
        _httpSide = options.Http is { } http ? new HttpSideOptions(http, options.ListenEndPoint.Port) : null;

        if (template.Allocator is { } supplied)
        {
            _allocator = supplied;
        }
        else
        {
            _allocator = new SlabAllocator(poolOptions);
            _ownsAllocator = true;
        }

        try
        {
            _peerOptions = PeerOptionsCopier.Copy(template);
            _peerOptions.Allocator = _allocator;
            _peerOptions.AllocatorOptions = null;
            _peerOptions.SendTableCapacity = Sizing.SendTableCapacity;
            _peerOptions.ReceiveRingCapacity = Sizing.ReceiveRingCapacity;
            _peerOptions.SegmentArenaCapacity = Sizing.SegmentArenaCapacity;
            _peerOptions.SendBudgetBytes = Sizing.SendBudgetBytes;
            _peerOptions.ReceiveBudgetBytes = Sizing.ReceiveBudgetBytes;
            _peerOptions.SessionGrace = options.Sessions.Grace;
            _peerOptions.AutoFlushInterval = TimeSpan.Zero;
            _peerOptions.SessionToken = default;
            _peerOptions.LastEpoch = 0;
            _peerOptions.RequestChannelTable = false;
            ProbeTransport.Validate(_peerOptions, _table);

            _sharedLeases = new SharedLeaseTable(_allocator);
            int replay = options.Sessions.ReplayCacheCapacity != 0
                ? options.Sessions.ReplayCacheCapacity
                : (int)Math.Clamp((long)options.ExpectedPeers * ReplayEntriesPerExpectedPeer, SessionTokenAuthority.DefaultReplayCacheCapacity, 1 << 22);
            byte[] key = options.Sessions.Key.IsEmpty ? RandomNumberGenerator.GetBytes(SessionTokenAuthority.KeyLength) : options.Sessions.Key.ToArray();
            try
            {
                _tokens = new SessionTokenAuthority(key, _clock, replay);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(key);
            }

            _rateLimiter = new AuthFailureRateLimiter(_clock, admission.AuthFailureTrackedAddresses, admission.AuthFailureBurst,
                ToMicros(admission.AuthFailureRefillInterval), admission.IPv6PrefixLength);
            _sessions = new SessionRegistry(Math.Min(options.ExpectedPeers, 1 << 16));

            int capacity = Sizing.SlotCapacity;
            _slots = new PeerSlot[capacity];
            _info = new SlotInfo?[capacity];
            _deadlines = new long[capacity];
            Array.Fill(_deadlines, long.MaxValue);
            _workBits = new long[(capacity + 63) >> 6];
            _generations = new uint[capacity];
            Array.Fill(_generations, 1u);
            _freeStack = new int[capacity];
            for (int i = 0; i < capacity; i++)
            {
                _slots[i] = new PeerSlot(null, 1, PeerSlotState.Free);
                _freeStack[i] = capacity - 1 - i;
            }

            _freeTop = capacity;
            _perAddress = new Dictionary<AddressKey, int>(Math.Min(capacity, 4096));
            _admittedSet = new PeerSet(capacity);
            _admittedSet.MakeReadOnly();
            _sharedAdmitted = new PeerSet(capacity);
            _sharedAdmitted.MakeReadOnly();
            _sharedRejected = new PeerSet(capacity);
            _sharedRejected.MakeReadOnly();
            _sharedStatus = new byte[capacity];
            _sharedHead = new int[capacity];
            Array.Fill(_sharedHead, -1);

            _defaultPolicy = new DefaultAdmissionPolicy(this, admission);
            _policy = _defaultPolicy;
            _helloAdmission = new HelloAdmission(this);
            _onStateChanged = OnPeerStateChanged;
            _preHandshake = OnPreHandshake;
            _accept = OnAccept;
        }
        catch
        {
            if (_ownsAllocator)
            {
                _allocator.Dispose();
            }

            throw;
        }
    }

    /// <summary>A peer's session was admitted (raised from <see cref="PollAll"/>, before any of its messages are dispatched, so handlers can be registered here).</summary>
    public event Action<QuiclyPeer>? PeerAdmitted;

    /// <summary>
    /// An admitted peer closed (raised from <see cref="PollAll"/>, or from <see cref="StopAsync"/>). The peer is disposed and
    /// its slot released right after the handlers return. Its session may still be resumed within the grace period
    /// (<see cref="SessionEnded"/> says when it cannot); a resumed session arrives as a new peer through
    /// <see cref="PeerAdmitted"/> with a higher <see cref="QuiclyPeer.Epoch"/> and the same <see cref="QuiclyPeer.SessionId"/>.
    /// When a resume replaces a connection that was still open, <see cref="PeerAdmitted"/> of the new peer can come first: the
    /// replaced peer's <see cref="PeerClosed"/> (with <see cref="QuiclyErrorCode.SessionReplaced"/>) follows once its close
    /// completed, so key per-session state by <see cref="QuiclyPeer.SessionId"/> and check the <see cref="QuiclyPeer.Epoch"/>.
    /// </summary>
    public event Action<QuiclyPeer, CloseReason>? PeerClosed;

    /// <summary>
    /// A session can no longer be resumed: its grace period ran out, it ended with a deliberate close, or the server stopped
    /// while it waited for a resume. Raised once per session, from <see cref="PollAll"/> (or <see cref="StopAsync"/>).
    /// </summary>
    public event Action<SessionEndInfo>? SessionEnded;

    /// <summary>
    /// An admission was refused, with the real cause (the client only sees a HelloAck status). Raised on the game thread:
    /// at once for a Hello, from the next <see cref="PollAll"/> for a refusal before the handshake (found on a transport
    /// thread and queued; at most 4 096 wait, more are dropped and counted in <see cref="ServerStatistics.EventsDropped"/>).
    /// Exceptions handlers throw are swallowed and counted (<see cref="ServerStatistics.EventHandlerFaults"/>).
    /// </summary>
    public event Action<AdmissionFailure>? AdmissionFailed;

    /// <summary>
    /// A certificate consumer threw while switching to a new certificate (it keeps presenting its previous one). Found on the
    /// thread that applied the certificate and raised from the next <see cref="PollAll"/>, like <see cref="AdmissionFailed"/>.
    /// </summary>
    public event Action<CertificateConsumerFailure>? CertificateConsumerFailed;

    /// <summary>The sizes the server derived from <see cref="ServerOptions.ExpectedPeers"/>.</summary>
    public ServerSizing Sizing { get; }

    /// <summary>The listener (owned by the server).</summary>
    public ITransportListener Listener => _listener;

    /// <summary>The listener's bound endpoint.</summary>
    public IPEndPoint LocalEndPoint => _listener.LocalEndPoint;

    /// <summary>The channel table.</summary>
    public ChannelTable Channels => _table;

    /// <summary>The buffer pool every peer's leases come from; rent shared payloads for <see cref="SendShared"/> here.</summary>
    public SlabAllocator Allocator => _allocator;

    /// <summary>Reference counts over <see cref="Allocator"/> for <see cref="SendShared"/>.</summary>
    public SharedLeaseTable SharedLeases => _sharedLeases;

    /// <summary>Number of peer slots (the capacity of <see cref="CreateSet"/>'s sets).</summary>
    public int Capacity => _slots.Length;

    /// <summary>The slot table up to the highest slot in use. Game thread; valid until the next <see cref="PollAll"/>.</summary>
    public ReadOnlySpan<PeerSlot> Peers => _slots.AsSpan(0, _highWater);

    /// <summary>Connections holding a slot (including ones the next <see cref="PollAll"/> activates).</summary>
    public int PeerCount => Volatile.Read(ref _connections);

    /// <summary>Admitted peers.</summary>
    public int AdmittedCount => _admittedSet.Count;

    /// <summary>The admitted peers, maintained by the server (read-only); a ready target for <see cref="SendShared"/>.</summary>
    public PeerSet AdmittedPeers => _admittedSet;

    /// <summary>True between a successful <see cref="StartAsync"/> and the start of a shutdown.</summary>
    public bool IsRunning => Volatile.Read(ref _state) == StateRunning;

    /// <summary>
    /// The admission policy. Starts as <see cref="DefaultAdmissionPolicy"/>; may be replaced before <see cref="StartAsync"/>
    /// (typically with a policy that adds rules and delegates to the default).
    /// </summary>
    /// <exception cref="InvalidOperationException">The server has been started.</exception>
    public IAdmissionPolicy AdmissionPolicy
    {
        get => Volatile.Read(ref _policy);
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            if (Volatile.Read(ref _state) != StateCreated)
            {
                throw new InvalidOperationException("The admission policy can only be replaced before the server starts.");
            }

            Volatile.Write(ref _policy, value);
        }
    }

    /// <summary>The server's built-in policy (sessions, tokens, limits, auth validator), for delegation from a custom policy.</summary>
    public DefaultAdmissionPolicy DefaultAdmissionPolicy => _defaultPolicy;

    /// <summary>The certificate provisioner while the server runs with <see cref="ServerOptions.Certificate"/>, otherwise <see langword="null"/>.</summary>
    public CertificateProvisioner? Provisioner => _provisioner;

    /// <summary>The binder of the provisioner to <see cref="ServerOptions.CertificateConsumers"/>, while the server runs with certificates.</summary>
    public CertificateBinder? Binder => _binder;

    /// <summary>The bound endpoint of the HTTP side endpoint while it runs, otherwise <see langword="null"/>.</summary>
    public IPEndPoint? HttpEndPoint => _http is { } http && http.BoundEndpoints.Count > 0 ? http.BoundEndpoints[0] : null;

    internal IClock Clock => _clock;

    internal AuthFailureRateLimiter RateLimiter => _rateLimiter;

    internal SessionTokenAuthority Tokens => _tokens;

    internal SessionRegistry Sessions => _sessions;

    internal bool IsAccepting => Volatile.Read(ref _accepting);

    /// <summary>The peer in slot <paramref name="index"/>, or <see langword="null"/>. Game thread.</summary>
    /// <param name="index">A slot index (<see cref="QuiclyPeer.Index"/>).</param>
    /// <returns>The peer, or <see langword="null"/> for a free slot or an index out of range.</returns>
    public QuiclyPeer? GetPeer(int index) => (uint)index < (uint)_slots.Length ? _slots[index].Peer : null;

    /// <summary>The peer in slot <paramref name="index"/> if the slot still has <paramref name="generation"/>. Game thread.</summary>
    /// <param name="index">A slot index.</param>
    /// <param name="generation">The generation read from <see cref="Peers"/> when the peer was stored.</param>
    /// <returns>The peer, or <see langword="null"/> when the slot was released since.</returns>
    public QuiclyPeer? GetPeer(int index, uint generation) =>
        (uint)index < (uint)_slots.Length && _slots[index].Generation == generation ? _slots[index].Peer : null;

    /// <summary>
    /// Creates an empty <see cref="PeerSet"/> sized to the slot table. The server removes an index from it when its peer is
    /// released (after <see cref="PeerClosed"/>), so a reused slot never inherits a membership. Game thread.
    /// </summary>
    /// <returns>The set.</returns>
    public PeerSet CreateSet()
    {
        ThrowIfDisposed();
        PeerSet set = new(_slots.Length);
        PruneTrackedSets();
        _trackedSets.Add(new WeakReference<PeerSet>(set));
        return set;
    }

    /// <summary>
    /// Makes <paramref name="newKey"/> the session-token signing key. Tokens signed with the previous key keep verifying for
    /// the token lifetime, their maximum age (so connected clients can still resume); the key before that stops verifying at
    /// once.
    /// </summary>
    /// <param name="newKey">The new 32-byte key (copied).</param>
    /// <exception cref="ArgumentException">The key is not 32 bytes.</exception>
    public void RotateSessionKey(ReadOnlySpan<byte> newKey)
    {
        ThrowIfDisposed();
        _tokens.RotateKey(newKey, _clock.NowMicros + _tokenLifetimeMicros);
    }

    /// <summary>Copies the server's counters into <paramref name="statistics"/>. Game thread; allocation-free.</summary>
    /// <param name="statistics">Receives the snapshot.</param>
    public void GetStatistics(out ServerStatistics statistics)
    {
        statistics = default;
        lock (_gate)
        {
            statistics.Connections = _connections;
            statistics.UnadmittedConnections = _unadmitted;
        }

        statistics.AdmittedPeers = _admittedSet.Count;
        statistics.Sessions = _sessions.Count;
        statistics.SharedSendsOutstanding = _sharedOutstanding + Volatile.Read(ref _sharedHeld);
        statistics.ConnectionsAccepted = Interlocked.Read(ref _connectionsAccepted);
        statistics.ConnectionsRefused = Interlocked.Read(ref _connectionsRefused);
        statistics.SessionsCreated = _sessionsCreated;
        statistics.SessionsResumed = _sessionsResumed;
        statistics.SessionsReplaced = _sessionsReplaced;
        statistics.SessionsExpired = _sessionsExpired;
        statistics.AdmissionsRejected = _admissionsRejected;
        statistics.AdmissionsPending = _admissionsPending;
        statistics.PollAllCalls = _pollAllCalls;
        statistics.PeersPolled = _peersPolled;
        statistics.EventHandlerFaults = Interlocked.Read(ref _eventHandlerFaults);
        statistics.EventsDropped = _failures.Dropped + _consumerFailures.Dropped;
    }

    internal static long ToMicros(TimeSpan value) => value.Ticks / TimeSpan.TicksPerMicrosecond;

    /// <summary>Reports a refused admission now (game thread); handler exceptions are swallowed and counted.</summary>
    internal void ReportFailure(in AdmissionFailure failure) => RaiseFailure(failure);

    /// <summary>Reports a refused admission found on another thread: queued for the next <see cref="PollAll"/>.</summary>
    internal void QueueFailure(in AdmissionFailure failure)
    {
        if (AdmissionFailed is not null)
        {
            _failures.Enqueue(in failure);
        }
    }

    /// <summary>Mints a session token for (<paramref name="sessionId"/>, <paramref name="epoch"/>) whose expiry is one token lifetime (its maximum age) from now.</summary>
    internal byte[] MintToken(ulong sessionId, uint epoch, long nowMicros)
    {
        byte[] token = new byte[SessionTokenAuthority.TokenLength];
        _tokens.Mint(sessionId, epoch, nowMicros + _tokenLifetimeMicros, token);
        return token;
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    /// <summary>Raises the events other threads queued (game thread).</summary>
    private void RaiseQueuedEvents()
    {
        if (_failures.HasItems)
        {
            _failures.Drain(_raiseFailure);
        }

        if (_consumerFailures.HasItems)
        {
            _consumerFailures.Drain(_raiseConsumerFailure);
        }
    }

    private void RaiseFailure(AdmissionFailure failure)
    {
        try
        {
            AdmissionFailed?.Invoke(failure);
        }
        catch (Exception)
        {
            Interlocked.Increment(ref _eventHandlerFaults);
        }
    }

    private void RaiseConsumerFailure(CertificateConsumerFailure failure)
    {
        try
        {
            CertificateConsumerFailed?.Invoke(failure);
        }
        catch (Exception)
        {
            Interlocked.Increment(ref _eventHandlerFaults);
        }
    }

    private void PruneTrackedSets()
    {
        for (int i = _trackedSets.Count - 1; i >= 0; i--)
        {
            if (!_trackedSets[i].TryGetTarget(out _))
            {
                _trackedSets[i] = _trackedSets[^1];
                _trackedSets.RemoveAt(_trackedSets.Count - 1);
            }
        }
    }
}
