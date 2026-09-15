using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Server;

// Connection lifecycle: the listener callbacks (transport threads), activation of accepted peers, the per-address counts
// and the release of a closed peer's slot (game thread).
public sealed partial class QuiclyServer
{
    // Guarded by _gate (touched by the listener's threads and the game thread).
    private readonly Lock _gate = new();
    private readonly uint[] _generations;
    private readonly int[] _freeStack;
    private readonly Dictionary<AddressKey, int> _perAddress;
    private List<Activation> _activations = [];
    private List<Activation> _activationsSpare = [];
    private int _activationCount;
    private int _freeTop;
    private int _unadmitted;
    private int _connections;
    private bool _accepting;

    // Game thread.
    private readonly PeerSlot[] _slots;
    private readonly SlotInfo?[] _info;
    private readonly long[] _deadlines;
    private readonly long[] _workBits;
    private readonly PeerSet _admittedSet;
    private readonly List<WeakReference<PeerSet>> _trackedSets = [];
    private int _highWater;
    private long _earliestDeadline = long.MaxValue;
    private int _reserved;

    // Any thread (Interlocked).
    private int _alivePeers;
    private int _allocatorReleaseRequested;
    private int _allocatorReleased;
    private long _connectionsAccepted;
    private long _connectionsRefused;
    private long _eventHandlerFaults;

    /// <summary>Marks <paramref name="slot"/> as having work for the next <see cref="PollAll"/> (any thread, lock-free).</summary>
    internal void MarkWork(int slot)
    {
        if ((uint)slot >= (uint)_slots.Length)
        {
            return;
        }

        ref long word = ref _workBits[slot >> 6];
        long bit = 1L << (slot & 63);
        if ((Volatile.Read(ref word) & bit) == 0)
        {
            Interlocked.Or(ref word, bit);
        }
    }

    /// <summary>The first connection limit a new connection from <paramref name="remote"/> would break, or <see langword="null"/>.</summary>
    internal AdmissionFailureReason? CheckConnectionLimits(IPEndPoint? remote)
    {
        bool hasAddress = AddressKey.TryCreate(remote, _ipv6PrefixLength, out AddressKey key);
        lock (_gate)
        {
            return CheckLimitsLocked(key, hasAddress);
        }
    }

    /// <summary>A peer's transport moved to another address: move its count and flag it when the new address is over the limit.</summary>
    internal void OnAddressChanged(WorkSignalSink sink, IPEndPoint? endPoint)
    {
        bool hasAddress = AddressKey.TryCreate(endPoint, _ipv6PrefixLength, out AddressKey key);
        bool exceeded = false;
        lock (_gate)
        {
            if (sink.IsRetired || (sink.HasAddress == hasAddress && (!hasAddress || sink.AddressKey == key)))
            {
                return;
            }

            if (sink.HasAddress)
            {
                DecrementAddressLocked(sink.AddressKey);
            }

            sink.AddressKey = key;
            sink.HasAddress = hasAddress;
            if (hasAddress)
            {
                ref int count = ref CollectionsMarshal.GetValueRefOrAddDefault(_perAddress, key, out _);
                count++;
                exceeded = count > _maxPerAddress;
            }
        }

        if (exceeded)
        {
            sink.RequestLimitClose();
        }
    }

    /// <summary>A peer's memory is free (its transport closed and the server disposed it).</summary>
    internal void OnPeerFreed()
    {
        if (Interlocked.Decrement(ref _alivePeers) == 0)
        {
            TryReleaseAllocator();
        }
    }

    private PreHandshakeDecision OnPreHandshake(in NewConnectionInfo info)
    {
        PreHandshakeDecision decision;
        try
        {
            decision = Volatile.Read(ref _policy).PreHandshake(in info);
        }
        catch (Exception exception)
        {
            ReportFailure(new AdmissionFailure(AdmissionStage.PreHandshake, AdmissionFailureReason.PolicyFault, info.RemoteEndPoint, Exception: exception));
            decision = PreHandshakeDecision.Reject;
        }

        if (decision != PreHandshakeDecision.Accept)
        {
            Interlocked.Increment(ref _connectionsRefused);
        }

        return decision;
    }

    private ITransportSink? OnAccept(ITransport transport, in NewConnectionInfo info)
    {
        IPEndPoint? remote = info.RemoteEndPoint;
        bool hasAddress = AddressKey.TryCreate(remote, _ipv6PrefixLength, out AddressKey key);
        int slot = -1;
        uint generation = 0;
        AdmissionFailureReason? refusal;
        lock (_gate)
        {
            // The authoritative, atomic check: the policy's pre-handshake check may have raced with other connections.
            refusal = CheckLimitsLocked(key, hasAddress);
            if (refusal is null)
            {
                slot = _freeStack[--_freeTop];
                generation = _generations[slot];
                _unadmitted++;
                _connections++;

                // Counted under the gate that shutdown flips _accepting under, so the shared pool can never be disposed
                // between this reservation and the peer's creation.
                Interlocked.Increment(ref _alivePeers);
                if (hasAddress)
                {
                    CollectionsMarshal.GetValueRefOrAddDefault(_perAddress, key, out _)++;
                }
            }
        }

        if (refusal is { } reason)
        {
            Interlocked.Increment(ref _connectionsRefused);
            ReportFailure(new AdmissionFailure(AdmissionStage.PreHandshake, reason, remote));
            return null;
        }

        QuiclyPeer peer;
        try
        {
            peer = QuiclyPeer.CreateServerPeer(transport, in info, _table, _peerOptions, _helloAdmission);
        }
        catch (Exception exception)
        {
            lock (_gate)
            {
                _freeStack[_freeTop++] = slot;
                _unadmitted--;
                _connections--;
                if (hasAddress)
                {
                    DecrementAddressLocked(key);
                }
            }

            Interlocked.Decrement(ref _alivePeers);
            Interlocked.Increment(ref _connectionsRefused);
            ReportFailure(new AdmissionFailure(AdmissionStage.PreHandshake, AdmissionFailureReason.PeerCreationFailed, remote, Exception: exception));
            return null;
        }

        peer.Index = slot;
        peer.StateChanged += _onStateChanged;
        WorkSignalSink sink = new(this, slot, peer.TransportSink);
        lock (_gate)
        {
            sink.AddressKey = key;
            sink.HasAddress = hasAddress;
            _activations.Add(new Activation(slot, generation, peer, sink));
            Volatile.Write(ref _activationCount, _activations.Count);
        }

        Interlocked.Increment(ref _connectionsAccepted);
        MarkWork(slot);
        return sink;
    }

    private AdmissionFailureReason? CheckLimitsLocked(AddressKey key, bool hasAddress)
    {
        if (!_accepting)
        {
            return AdmissionFailureReason.ServerStopping;
        }

        if (_freeTop == 0)
        {
            return AdmissionFailureReason.ServerAtCapacity;
        }

        if (_unadmitted >= _maxUnadmitted)
        {
            return AdmissionFailureReason.TooManyUnadmittedConnections;
        }

        if (hasAddress && _perAddress.TryGetValue(key, out int count) && count >= _maxPerAddress)
        {
            return AdmissionFailureReason.TooManyConnectionsFromAddress;
        }

        return null;
    }

    private void DecrementAddressLocked(AddressKey key)
    {
        ref int count = ref CollectionsMarshal.GetValueRefOrNullRef(_perAddress, key);
        if (Unsafe.IsNullRef(ref count))
        {
            return;
        }

        if (--count <= 0)
        {
            _perAddress.Remove(key);
        }
    }

    /// <summary>Moves peers accepted by the listener's threads into the slot table (game thread).</summary>
    private void ActivatePending()
    {
        List<Activation> batch;
        lock (_gate)
        {
            batch = _activations;
            _activations = _activationsSpare;
            _activationsSpare = batch;
            Volatile.Write(ref _activationCount, 0);
        }

        foreach (Activation activation in batch)
        {
            int slot = activation.Slot;
            SlotInfo info = _info[slot] ??= new SlotInfo();
            info.Sink = activation.Sink;
            info.Unadmitted = true;
            _slots[slot] = new PeerSlot(activation.Peer, activation.Generation, PeerSlotState.Handshaking);
            long deadline = activation.Peer.NextDeadlineMicros;
            _deadlines[slot] = deadline;
            if (deadline < _earliestDeadline)
            {
                _earliestDeadline = deadline;
            }

            if (slot >= _highWater)
            {
                _highWater = slot + 1;
            }
        }

        batch.Clear();
    }

    /// <summary>The server-side state of <paramref name="peer"/>, or <see langword="null"/> when it is not (or no longer) in the slot table.</summary>
    private SlotInfo? InfoOf(QuiclyPeer peer)
    {
        int slot = peer.Index;
        return (uint)slot < (uint)_slots.Length && ReferenceEquals(_slots[slot].Peer, peer) ? _info[slot] : null;
    }

    /// <summary>
    /// A peer reached <see cref="PeerState.Closed"/> (or is force-closed): updates its session, drops its shared sends, raises
    /// <see cref="PeerClosed"/> for an admitted peer, then disposes it and releases its slot (game thread).
    /// </summary>
    private void FinalizeSlot(int slot, QuiclyPeer peer)
    {
        SlotInfo info = _info[slot]!;
        long now = _clock.NowMicros;
        bool admitted = info.Admitted;
        CloseReason reason = peer.CloseReason;
        SessionEndInfo? ended = null;
        SessionRecord? record = info.Session;
        info.Session = null;
        if (record is not null && ReferenceEquals(record.Peer, peer))
        {
            record.Tag = peer.Tag;
            if (_graceMicros > 0 && IsResumable(in reason))
            {
                _sessions.Disconnect(record, now + _graceMicros);
            }
            else
            {
                _sessions.Remove(record);
                ended = new SessionEndInfo(record.SessionId, record.Tag, Expired: false);
            }
        }

        ReleaseAllShared(slot, peer);
        try
        {
            if (admitted)
            {
                PeerClosed?.Invoke(peer, reason);
            }
        }
        finally
        {
            Release(slot, peer, info);
            if (ended is { } end)
            {
                SessionEnded?.Invoke(end);
            }
        }
    }

    /// <summary>
    /// Whether a session survives this close for its grace period: lost connections and timeouts do; a deliberate close does
    /// not (the client said goodbye, or the server closed the peer itself, for example to kick a player).
    /// </summary>
    private static bool IsResumable(in CloseReason reason) =>
        reason.Source == CloseSource.Transport
        || reason.Code == QuiclyErrorCode.Timeout
        || (reason.Source == CloseSource.Peer && reason.Code != QuiclyErrorCode.NoError);

    private void Release(int slot, QuiclyPeer peer, SlotInfo info)
    {
        peer.StateChanged -= _onStateChanged;
        _admittedSet.RemoveCore(slot);
        ClearTrackedSets(slot);
        if (info.Reserved)
        {
            _reserved--;
        }

        WorkSignalSink? sink = info.Sink;
        try
        {
            peer.Dispose();
        }
        finally
        {
            uint generation;
            lock (_gate)
            {
                sink?.RetireLocked();
                if (sink is { HasAddress: true })
                {
                    DecrementAddressLocked(sink.AddressKey);
                }

                if (info.Unadmitted)
                {
                    _unadmitted--;
                }

                _connections--;
                generation = _generations[slot] = _generations[slot] == uint.MaxValue ? 1 : _generations[slot] + 1;
                _freeStack[_freeTop++] = slot;
            }

            _slots[slot] = new PeerSlot(null, generation, PeerSlotState.Free);
            _deadlines[slot] = long.MaxValue;
            info.Reset();
            while (_highWater > 0 && _slots[_highWater - 1].Peer is null)
            {
                _highWater--;
            }

            sink?.MarkReleased();
        }
    }

    private void ClearTrackedSets(int slot)
    {
        for (int i = _trackedSets.Count - 1; i >= 0; i--)
        {
            if (_trackedSets[i].TryGetTarget(out PeerSet? set))
            {
                set.RemoveCore(slot);
            }
            else
            {
                _trackedSets[i] = _trackedSets[^1];
                _trackedSets.RemoveAt(_trackedSets.Count - 1);
            }
        }
    }

    private void TryReleaseAllocator()
    {
        if (_ownsAllocator && Volatile.Read(ref _allocatorReleaseRequested) != 0 && Volatile.Read(ref _alivePeers) == 0
            && Interlocked.Exchange(ref _allocatorReleased, 1) == 0)
        {
            _allocator.Dispose();
        }
    }

    private readonly record struct Activation(int Slot, uint Generation, QuiclyPeer Peer, WorkSignalSink Sink);

    /// <summary>Server-side state of one slot (reused across the connections the slot holds).</summary>
    private sealed class SlotInfo
    {
        public WorkSignalSink? Sink;
        public SessionRecord? Session;
        public PendingAdmission? Pending;
        public bool Unadmitted;
        public bool Reserved;
        public bool Admitted;
        public bool AwaitingDecision;

        public void Reset()
        {
            Sink = null;
            Session = null;
            Pending = null;
            Unadmitted = false;
            Reserved = false;
            Admitted = false;
            AwaitingDecision = false;
        }
    }
}
