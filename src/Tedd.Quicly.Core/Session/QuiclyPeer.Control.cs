using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

// Game thread: the handshake state machine (PROTOCOL.md §3.4), admission, ping/pong and clock sync (§4.6), heartbeat,
// admission timeout, close (§6) and the state transitions reported by StateChanged.
public sealed unsafe partial class QuiclyPeer
{
    private readonly StateTransition[] _transitions = new StateTransition[16];
    private readonly uint[] _recentPings = new uint[8];
    private PeerState _state;
    private int _transitionCount;
    private bool _closedRaised;
    private bool _transportCloseCalled;
    private CloseReason _closeReason;
    private QuiclyErrorCode _closeCode;
    private byte[]? _closeReasonUtf8;
    private int _terminalSlot = -1;
    private long _lingerDeadline;
    private long _admissionDeadline;
    private bool _admissionPending;
    private HelloFlags _helloFlags;
    private ulong _sessionId;
    private ReadOnlyMemory<byte> _sessionToken;
    private HelloStatus _handshakeStatus;
    private ChannelTableDescription? _remoteTable;
    private bool _controlPreambleSent;
    private long _connectedAtMicros;
    private long _nextPingMicros;
    private long _fastLockEndMicros;
    private PingClock _ping;
    private int _recentPingNext;
    private int _recentPingValid;
    private long _timerDeadline = long.MaxValue;

    /// <summary>
    /// Raised from <see cref="Poll"/> on the game thread for every state change: (peer, previous state, new state). After the
    /// change to <see cref="PeerState.Closed"/> no further event or handler runs.
    /// </summary>
    public event Action<QuiclyPeer, PeerState, PeerState>? StateChanged;

    private enum CloseMode : byte
    {
        /// <summary>Send Close on the control stream, then close the transport when it was delivered or the linger elapsed.</summary>
        SendClose,

        /// <summary>A terminal message (a refusing HelloAck) was sent: close the transport when it was delivered or the linger elapsed.</summary>
        Linger,

        /// <summary>Close the transport now.</summary>
        Immediate,
    }

    // ------------------------------------------------------------------ public control API

    /// <summary>
    /// Closes the session gracefully: sends Close (code and reason) on the control stream, then closes the transport with
    /// the same code once the message was delivered (or after <see cref="PeerOptions.CloseLinger"/>). The peer moves to
    /// <see cref="PeerState.Closing"/> now and to <see cref="PeerState.Closed"/> when the transport reports its close; both
    /// changes are raised from <see cref="Poll"/>. Ignored when the session is already closing.
    /// </summary>
    /// <param name="reason">The PROTOCOL.md §6 code and an optional reason (at most 512 bytes of UTF-8).</param>
    /// <exception cref="ArgumentException">The reason is longer than 512 bytes of UTF-8 or the code does not fit 32 bits.</exception>
    /// <exception cref="ObjectDisposedException">The peer is disposed.</exception>
    public void Close(CloseReason reason)
    {
        ThrowIfDisposed();
        if ((ulong)reason.Code > uint.MaxValue)
        {
            throw new ArgumentException("Close codes are 32-bit values.", nameof(reason));
        }

        if (reason.Reason is { } text && Encoding.UTF8.GetByteCount(text) > ControlCodec.MaxReasonLength)
        {
            throw new ArgumentException($"A close reason is at most {ControlCodec.MaxReasonLength} bytes of UTF-8.", nameof(reason));
        }

        BeginClose(reason.Code, reason.Reason, CloseSource.Local, CloseMode.SendClose);
    }

    /// <summary>Closes the session with <see cref="QuiclyErrorCode.NoError"/> (see <see cref="Close(Session.CloseReason)"/>).</summary>
    public void Close() => Close(CloseReason.Normal);

    /// <summary>
    /// Completes an admission that <see cref="IPeerAdmission.Admit"/> answered with <see cref="AdmissionResult.Pending"/>
    /// (server, game thread). Must happen within <see cref="PeerOptions.AdmissionTimeout"/> of the connection; a decision
    /// for a connection that already closed is ignored.
    /// </summary>
    /// <param name="result">Accept or Reject.</param>
    /// <exception cref="ArgumentException"><paramref name="result"/> is Pending.</exception>
    /// <exception cref="InvalidOperationException">No admission decision is pending.</exception>
    /// <exception cref="ObjectDisposedException">The peer is disposed.</exception>
    public void CompleteAdmission(AdmissionResult result)
    {
        ThrowIfDisposed();
        if (result.Decision == AdmissionDecision.Pending)
        {
            throw new ArgumentException("An admission cannot be completed with Pending.", nameof(result));
        }

        if (_state is PeerState.Closing or PeerState.Closed)
        {
            return;
        }

        if (!_admissionPending)
        {
            throw new InvalidOperationException("No admission decision is pending.");
        }

        _admissionPending = false;
        ApplyAdmission(in result, _clock.NowMicros);
    }

    // ------------------------------------------------------------------ signals from the transport thread

    private void ProcessSignals(long now)
    {
        int signals = Interlocked.Exchange(ref _signals, 0);
        if (signals != 0)
        {
            if ((signals & SignalConnected) != 0)
            {
                OnTransportConnected(now);
            }

            // Closes first: a connection that violated the protocol or was closed by the peer is not admitted afterwards.
            if ((signals & SignalCloseRequest) != 0 && _core.TryGetCloseRequest(out QuiclyErrorCode code))
            {
                BeginClose(code, DescribeLocalClose(code), CloseSource.Local, CloseMode.SendClose);
            }

            if ((signals & SignalPeerClose) != 0)
            {
                OnPeerCloseReceived();
            }

            if ((signals & SignalHello) != 0)
            {
                OnHelloReceived(now);
            }

            if ((signals & SignalHelloAck) != 0)
            {
                OnHelloAckReceived(now);
            }

            if ((signals & SignalTableInfo) != 0)
            {
                OnTableInfoReceived();
            }

            if ((signals & SignalTableRequest) != 0)
            {
                SendTableAnswer();
            }

            if ((signals & SignalTransportClosed) != 0)
            {
                OnTransportClosed();
            }
        }

        DrainPongs(now);
    }

    private void OnTransportConnected(long now)
    {
        if (_state != PeerState.Connecting)
        {
            return;
        }

        SetState(PeerState.Handshaking);
        if (_role == PeerRole.Client)
        {
            StartClientHandshake(now);
        }
    }

    private void StartClientHandshake(long now)
    {
        if (_core.OpenStream(StreamKind.Bidirectional, PeerCore.ControlStreamContext, ushort.MaxValue, out TransportStreamId id) != TransportStatus.Success)
        {
            BeginClose(QuiclyErrorCode.InternalError, "the control stream could not be opened", CloseSource.Local, CloseMode.Immediate);
            return;
        }

        SetControlStreamId(id);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(ControlCodec.MaxEncodedStreamFrameLength);
        try
        {
            Hello hello = new()
            {
                Flags = _requestTable ? HelloFlags.RequestChannelTable : HelloFlags.None,
                TableHash = _core.Table.Hash,
                LastEpoch = _lastEpoch,
                SessionToken = _resumeToken,
                AuthToken = _authToken,
                MaxReceiveDatagram = _maxReceiveDatagram,
                Caps = LocalCaps(),
            };
            ControlCodec.TryWrite(buffer, in hello, out int written);
            if (!SendControlStream(buffer.AsSpan(0, written), out _))
            {
                BeginClose(QuiclyErrorCode.InternalError, "the Hello could not be sent", CloseSource.Local, CloseMode.Immediate);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void OnHelloReceived(long now)
    {
        if (_state != PeerState.Handshaking || _helloBody is null)
        {
            return;
        }

        ControlParseStatus status = ControlCodec.TryParse(_helloBody, out Hello hello);
        if (status == ControlParseStatus.UnsupportedVersion)
        {
            RejectHello(HelloStatus.VersionMismatch, "unsupported protocol version");
            return;
        }

        _helloFlags = hello.Flags;
        if (hello.TableHash != _core.Table.Hash)
        {
            RejectHello(HelloStatus.ChannelTableMismatch, "channel table mismatch");
            return;
        }

        if (_needsDatagrams && ((hello.Caps & PeerCaps.Datagrams) == 0 || !_core.DatagramsEnabled))
        {
            RejectHello(HelloStatus.DatagramsRequired, "the channel table requires datagrams");
            return;
        }

        HelloInfo info = new()
        {
            Version = hello.Version,
            Flags = hello.Flags,
            TableHash = hello.TableHash,
            LastEpoch = hello.LastEpoch,
            SessionToken = hello.SessionToken,
            AuthToken = hello.AuthToken,
            Caps = hello.Caps,
            MaxReceiveDatagram = hello.MaxReceiveDatagram,
            RemoteEndPoint = RemoteEndPoint,
        };
        AdmissionResult result;
        try
        {
            result = _admission!.Admit(in info, this);
        }
        catch
        {
            RejectHello(HelloStatus.InternalError, null);
            throw;
        }

        ApplyAdmission(in result, now);
    }

    private void ApplyAdmission(in AdmissionResult result, long now)
    {
        switch (result.Decision)
        {
            case AdmissionDecision.Pending:
                _admissionPending = true;
                break;
            case AdmissionDecision.Reject:
                RejectHello(result.Status, result.Reason);
                break;
            default:
                AcceptHello(in result, now);
                break;
        }
    }

    private void AcceptHello(in AdmissionResult result, long now)
    {
        _core.Epoch = result.Epoch;
        _sessionId = result.SessionId != 0 ? result.SessionId : NewSessionId();
        _sessionToken = result.SessionToken;
        // Publish admission before the HelloAck can reach the client: its traffic may follow the HelloAck immediately.
        _core.IsAdmitted = true;
        _transport!.UpdatePeerStreamLimits(1, (ushort)_core.PeerUnidirectionalStreamLimit);
        if (!SendHelloAck(HelloStatus.Accepted, null, out _))
        {
            BeginClose(QuiclyErrorCode.InternalError, "the HelloAck could not be sent", CloseSource.Local, CloseMode.Immediate);
            return;
        }

        EnterConnected(now);
    }

    private void RejectHello(HelloStatus status, string? reason)
    {
        _handshakeStatus = status;
        bool sent = SendHelloAck(status, reason, out int slot);
        BeginClose(QuiclyErrorCode.AdmissionRejected, reason, CloseSource.Local, sent ? CloseMode.Linger : CloseMode.Immediate, slot);
    }

    private bool SendHelloAck(HelloStatus status, string? reasonText, out int slot)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(ControlCodec.MaxEncodedStreamFrameLength);
        try
        {
            byte[]? reason = EncodeReason(reasonText);
            bool accepted = status == HelloStatus.Accepted;
            ReadOnlySpan<byte> token = accepted ? _sessionToken.Span : default;
            byte[]? table = null;
            if ((_helloFlags & HelloFlags.RequestChannelTable) != 0)
            {
                table = TryEncodeTable(token.Length, reason?.Length ?? 0);
            }

            HelloAck ack = new()
            {
                Status = status,
                SessionId = accepted ? _sessionId : 0,
                Epoch = accepted ? _core.Epoch : 0,
                MaxReceiveDatagram = _maxReceiveDatagram,
                Caps = LocalCaps(),
                MaxMessageSize = (ulong)_maxMessageSizeOption,
                HeartbeatMicros = (ulong)_heartbeatMicros,
                GraceMicros = (ulong)_sessionGraceMicros,
                SessionToken = token,
                Table = table,
                Reason = reason,
            };
            ControlCodec.TryWrite(buffer, in ack, out int written);
            return SendControlStream(buffer.AsSpan(0, written), out slot);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private byte[]? TryEncodeTable(int tokenLength, int reasonLength)
    {
        int length = ChannelTableCodec.GetLengthWithNames(_core.Table);
        if (length > ControlCodec.GetMaxHelloAckTableLength(tokenLength, reasonLength))
        {
            // PROTOCOL.md §8: a table section that does not fit the frame is not sent (tableIncluded = 0).
            return null;
        }

        byte[] table = new byte[length];
        ChannelTableCodec.WriteWithNames(_core.Table, table);
        return table;
    }

    private void SendTableAnswer()
    {
        if (_role != PeerRole.Server || _state != PeerState.Connected)
        {
            return;
        }

        byte[] buffer = ArrayPool<byte>.Shared.Rent(ControlCodec.MaxEncodedStreamFrameLength);
        try
        {
            HelloAck ack = new()
            {
                Status = HelloStatus.Informational,
                Caps = LocalCaps(),
                MaxMessageSize = (ulong)_maxMessageSizeOption,
                Table = TryEncodeTable(0, 0),
            };
            ControlCodec.TryWrite(buffer, in ack, out int written);
            SendControlStream(buffer.AsSpan(0, written), out _);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void OnHelloAckReceived(long now)
    {
        if (_state != PeerState.Handshaking || _helloAckBody is null)
        {
            return;
        }

        ControlCodec.TryParse(_helloAckBody, out HelloAck ack);
        if (ack.HasTable && ChannelTableCodec.TryParseWithNames(ack.Table, out ChannelTableDescription? description, out _) == ChannelTableParseStatus.Ok)
        {
            _remoteTable = description;
        }

        if (ack.Status != HelloStatus.Accepted)
        {
            _handshakeStatus = ack.Status;
            BeginClose(QuiclyErrorCode.AdmissionRejected, SanitizeReason(ack.Reason), CloseSource.Peer, CloseMode.Immediate);
            return;
        }

        _core.Epoch = ack.Epoch;
        _sessionId = ack.SessionId;
        _sessionToken = ack.SessionToken.ToArray();
        // Let the server open its streams toward us now (PROTOCOL.md §3.4: limits are raised only after admission).
        _transport!.UpdatePeerStreamLimits(0, (ushort)_core.PeerUnidirectionalStreamLimit);
        EnterConnected(now);
    }

    private void OnTableInfoReceived()
    {
        byte[]? body = Volatile.Read(ref _tableInfoBody);
        if (body is null || ControlCodec.TryParse(body, out HelloAck ack) != ControlParseStatus.Ok || !ack.HasTable)
        {
            return;
        }

        if (ChannelTableCodec.TryParseWithNames(ack.Table, out ChannelTableDescription? description, out _) == ChannelTableParseStatus.Ok)
        {
            _remoteTable = description;
        }
    }

    private void EnterConnected(long now)
    {
        _handshakeStatus = HelloStatus.Accepted;
        _connectedAtMicros = now;
        _fastLockEndMicros = now + _fastLockMicros;
        _nextPingMicros = now;
        _ping = default;
        SetState(PeerState.Connected);
        bool resumed = _core.Epoch > 1;
        ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
        for (int i = 0; i < engines.Length; i++)
        {
            engines[i].OnEpochReset(resumed);
        }
    }

    private void OnPeerCloseReceived()
    {
        string? reason = SanitizeReason(_peerCloseReason);
        if (_state == PeerState.Closing)
        {
            // Both ends closed: ours is delivered or about to be; no need to wait for the linger.
            CloseTransportNow();
            return;
        }

        // PROTOCOL.md §6: the QUIC connection is closed with the same code.
        BeginClose(_peerCloseCode, reason, CloseSource.Peer, CloseMode.Immediate);
    }

    private void OnTransportClosed()
    {
        if (_closeReason.Source == CloseSource.None)
        {
            _closeReason = _transportCloseReason switch
            {
                TransportCloseReason.Peer => new CloseReason((QuiclyErrorCode)_transportCloseCode) { Source = CloseSource.Peer },
                TransportCloseReason.Local => new CloseReason((QuiclyErrorCode)_transportCloseCode) { Source = CloseSource.Local },
                _ => new CloseReason((QuiclyErrorCode)_transportCloseCode) { Source = CloseSource.Transport, TransportStatus = _transportCloseStatus },
            };
        }

        _transportCloseCalled = true;
        _admissionPending = false;
        _terminalSlot = -1;
        _lingerDeadline = 0;
        SetState(PeerState.Closed);
    }

    // ------------------------------------------------------------------ close

    private void BeginClose(QuiclyErrorCode code, string? reason, CloseSource source, CloseMode mode, int terminalSlot = -1)
    {
        if (_state is PeerState.Closing or PeerState.Closed)
        {
            return;
        }

        _closeReason = new CloseReason(code, reason) { Source = source };
        _closeCode = code;
        _closeReasonUtf8 = source == CloseSource.Local ? EncodeReason(reason) : null;
        _ignoreIncoming = true;
        _admissionPending = false;
        SetState(PeerState.Closing);
        if (mode == CloseMode.SendClose)
        {
            terminalSlot = -1;
            if (ControlStreamId.IsValid && !_core.IsTransportClosing)
            {
                Span<byte> frame = stackalloc byte[ControlCodec.MaxReasonLength + 16];
                ControlCodec.TryWrite(frame, new Tedd.Quicly.Core.Control.Close(code, _closeReasonUtf8), out int written);
                if (SendControlStream(frame.Slice(0, written), out int slot))
                {
                    terminalSlot = slot;
                }
            }

            mode = terminalSlot >= 0 ? CloseMode.Linger : CloseMode.Immediate;
        }

        if (mode == CloseMode.Linger && terminalSlot >= 0 && _closeLingerMicros > 0)
        {
            _terminalSlot = terminalSlot;
            _lingerDeadline = _clock.NowMicros + _closeLingerMicros;
            return;
        }

        CloseTransportNow();
    }

    private void CloseTransportNow()
    {
        _terminalSlot = -1;
        _lingerDeadline = 0;
        if (_transportCloseCalled)
        {
            return;
        }

        _transportCloseCalled = true;
        _core.MarkTransportClosing();
        _transport?.Close((ulong)_closeCode, _closeReasonUtf8);
    }

    private static string DescribeLocalClose(QuiclyErrorCode code) => code switch
    {
        QuiclyErrorCode.ProtocolViolation => "protocol violation",
        QuiclyErrorCode.LimitExceeded => "limit exceeded",
        QuiclyErrorCode.Timeout => "timeout",
        QuiclyErrorCode.InternalError => "internal error",
        _ => "closed",
    };

    /// <summary>
    /// UTF-8 of a local close reason. Every local reason is at most 512 bytes: <see cref="Close(Session.CloseReason)"/> and
    /// <see cref="AdmissionResult.Reject"/> validate theirs, the peer's own are short constants.
    /// </summary>
    private static byte[]? EncodeReason(string? text) => string.IsNullOrEmpty(text) ? null : Encoding.UTF8.GetBytes(text);

    private static string? SanitizeReason(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty)
        {
            return null;
        }

        char[] chars = new char[utf8.Length];
        int written = ControlCodec.CopySanitizedReason(utf8, chars);
        return new string(chars, 0, written);
    }

    private static ulong NewSessionId()
    {
        Span<byte> bytes = stackalloc byte[8];
        RandomNumberGenerator.Fill(bytes);
        ulong id = BinaryPrimitives.ReadUInt64LittleEndian(bytes);
        return id == 0 ? 1 : id;
    }

    private PeerCaps LocalCaps()
    {
        PeerCaps caps = PeerCaps.Lz4;
        if (_core.DatagramsEnabled)
        {
            caps |= PeerCaps.Datagrams;
        }

        if (_core.DatagramStatesReported)
        {
            caps |= PeerCaps.DatagramSendState;
        }

        return caps;
    }

    // ------------------------------------------------------------------ timers (Poll and Flush)

    /// <summary>Runs the peer's time-driven work and returns its next deadline (game thread, <paramref name="now"/> read once).</summary>
    private long RunTimers(long now)
    {
        long next = long.MaxValue;
        switch (_state)
        {
            case PeerState.Connecting:
            case PeerState.Handshaking:
                if (now >= _admissionDeadline)
                {
                    BeginClose(QuiclyErrorCode.Timeout, "admission timeout", CloseSource.Local, CloseMode.SendClose);
                    next = LingerDeadline(next);
                }
                else
                {
                    next = _admissionDeadline;
                }

                break;
            case PeerState.Connected:
                if (now >= _nextPingMicros)
                {
                    SendPing(now);
                    _nextPingMicros = now + (now < _fastLockEndMicros ? _fastPingIntervalMicros : _pingIntervalMicros);
                }

                next = _nextPingMicros;
                _ping.Advance(now, now >= _fastLockEndMicros);
                if (!_ping.Locked && _fastLockEndMicros > now && _fastLockEndMicros < next)
                {
                    next = _fastLockEndMicros;
                }

                if (_heartbeatMicros > 0)
                {
                    long due = Math.Max(Volatile.Read(ref _lastReceiveMicros), _connectedAtMicros) + _heartbeatMicros;
                    if (now >= due)
                    {
                        BeginClose(QuiclyErrorCode.Timeout, "heartbeat timeout", CloseSource.Local, CloseMode.SendClose);
                        next = LingerDeadline(long.MaxValue);
                    }
                    else if (due < next)
                    {
                        next = due;
                    }
                }

                next = SweepIdleStreams(now, next);
                break;
            case PeerState.Closing:
                if (_lingerDeadline != 0 && now >= _lingerDeadline)
                {
                    CloseTransportNow();
                }

                next = LingerDeadline(next);
                break;
        }

        _timerDeadline = next;
        return next;
    }

    private long LingerDeadline(long next) => _lingerDeadline != 0 && _lingerDeadline < next ? _lingerDeadline : next;

    /// <summary>
    /// PROTOCOL.md §7 "stream idle mid-message | 30 s | stream reset Timeout": resets every peer stream that has made no
    /// progress on a half-received message for <see cref="PeerOptions.StreamIdleTimeout"/>, so a peer that starts a
    /// message and stops cannot pin the receive budget (its staging lease) and a receive-ring slot for the life of the
    /// connection. The reset is stream-level — the connection survives — and the transport reports the stream closed,
    /// where the owning engine releases the lease and cancels the reservation (the same path as a peer-initiated reset).
    /// Game thread, <paramref name="now"/> read once by <see cref="RunTimers"/>; returns the next deadline.
    /// </summary>
    /// <param name="now">Clock micros of this pass.</param>
    /// <param name="next">The deadline so far.</param>
    /// <returns>The deadline, lowered to the oldest watched stream's expiry.</returns>
    private long SweepIdleStreams(long now, long next)
    {
        if (_streamIdleMicros <= 0)
        {
            return next;
        }

        long cutoff = now - _streamIdleMicros;
        while (_core.Streams.TryTakeIdle(cutoff, out TransportStreamId stalled))
        {
            _core.Counters.StreamIdleTimeouts++;
            _transport?.AbortStream(stalled, (ulong)QuiclyErrorCode.Timeout, StreamAbortDirection.Both);
        }

        long earliest = _core.Streams.EarliestMidMessage();
        if (earliest == long.MaxValue)
        {
            return next;
        }

        long due = earliest + _streamIdleMicros;
        return due < next ? due : next;
    }

    // ------------------------------------------------------------------ ping / pong (PROTOCOL.md §2.3, §4.6)

    private void SendPing(long now)
    {
        uint t = _core.ToWireMicros(now);
        _recentPings[_recentPingNext] = t;
        _recentPingValid |= 1 << _recentPingNext;
        _recentPingNext = (_recentPingNext + 1) & (_recentPings.Length - 1);
        Span<byte> frame = stackalloc byte[16];
        bool sent;
        if (_core.DatagramsEnabled)
        {
            ControlCodec.TryWrite(frame, new Ping(t), ControlCarrier.Datagram, out int written);
            sent = SendControlDatagram(frame.Slice(0, written));
        }
        else
        {
            ControlCodec.TryWrite(frame, new Ping(t), ControlCarrier.Stream, out int written);
            sent = SendControlStream(frame.Slice(0, written), out _);
        }

        if (sent)
        {
            _core.Counters.PingsSent++;
        }
    }

    private void DrainPongs(long now)
    {
        while (_pongs.TryDequeue(out PongSample sample))
        {
            if (_state == PeerState.Connected && MatchPing(sample.Echo))
            {
                _ping.AddSample(in sample);
            }
            else
            {
                _core.Counters.UnmatchedPongs++;
            }
        }

        Span<byte> frame = stackalloc byte[24];
        while (_streamPings.TryDequeue(out PongSample request))
        {
            if (_state != PeerState.Connected)
            {
                continue;
            }

            ControlCodec.TryWrite(frame, new Pong(request.Echo, request.RemoteReceive, _core.ToWireMicros(now)), ControlCarrier.Stream, out int written);
            if (SendControlStream(frame.Slice(0, written), out _))
            {
                _core.Counters.StreamPongsSent++;
            }
        }
    }

    private bool MatchPing(uint echo)
    {
        for (int i = 0; i < _recentPings.Length; i++)
        {
            int bit = 1 << i;
            if ((_recentPingValid & bit) != 0 && _recentPings[i] == echo)
            {
                _recentPingValid &= ~bit;
                return true;
            }
        }

        return false;
    }

    // ------------------------------------------------------------------ control sends

    /// <summary>Sends one control datagram from a send entry's header block (game thread; Priority flag, PROTOCOL.md §2.3).</summary>
    private bool SendControlDatagram(ReadOnlySpan<byte> frame)
    {
        if (!_core.TryAllocateEntry(PeerCore.ControlChannelId, SendEntryFlags.None, out int slot))
        {
            _core.Counters.ControlSendFailures++;
            return false;
        }

        _core.Entries.SetHeader(slot, frame);
        if (_core.SubmitDatagram(slot, TransportSendFlags.Priority) != TransportStatus.Success)
        {
            _core.DiscardEntry(slot);
            _core.Counters.ControlSendFailures++;
            return false;
        }

        return true;
    }

    /// <summary>
    /// Sends one framed control message on the control stream (game thread). The first send in each direction carries the
    /// preamble <c>0x00</c>; the client's first send also starts the stream. Messages that fit (with the preamble) in the
    /// entry's 32-byte header block need no lease.
    /// </summary>
    private bool SendControlStream(ReadOnlySpan<byte> frame, out int slot)
    {
        slot = -1;
        TransportStreamId id = ControlStreamId;
        if (!id.IsValid)
        {
            return false;
        }

        if (!_core.TryAllocateEntry(PeerCore.ControlChannelId, SendEntryFlags.None, out slot))
        {
            _core.Counters.ControlSendFailures++;
            return false;
        }

        SendEntryTable entries = _core.Entries;
        int preamble = _controlPreambleSent ? 0 : 1;
        TransportSegment* segments = entries.GetSegments(slot);
        int count;
        Span<byte> block = entries.GetHeaderBlock(slot);
        if (preamble + frame.Length <= SendEntryTable.HeaderBlockSize)
        {
            if (preamble == 1)
            {
                block[0] = ControlCodec.StreamPreamble;
            }

            frame.CopyTo(block.Slice(preamble));
            entries.SetHeaderLength(slot, preamble + frame.Length);
            count = 1;
        }
        else
        {
            if (!_core.TryRentSend(frame.Length, out BufferLease lease))
            {
                _core.DiscardEntry(slot);
                slot = -1;
                _core.Counters.ControlSendFailures++;
                return false;
            }

            frame.CopyTo(_core.GetSpan(in lease));
            _core.AttachLease(slot, in lease, frame.Length);
            if (preamble == 1)
            {
                block[0] = ControlCodec.StreamPreamble;
                entries.SetHeaderLength(slot, 1);
                count = 2;
            }
            else
            {
                segments++;
                count = 1;
            }
        }

        TransportSendFlags flags = TransportSendFlags.Priority;
        if (_role == PeerRole.Client && preamble == 1)
        {
            flags |= TransportSendFlags.Start;
        }

        if (_core.SubmitStream(id, segments, count, slot, flags) != TransportStatus.Success)
        {
            _core.DiscardEntry(slot);
            slot = -1;
            _core.Counters.ControlSendFailures++;
            return false;
        }

        _controlPreambleSent = true;
        return true;
    }

    private void OnControlEntryCompleted(in CompletionEntry completion)
    {
        if (!completion.Final)
        {
            return;
        }

        bool terminal = completion.Slot == _terminalSlot;
        _core.CompleteEntry(completion.Slot, completion.Canceled ? DeliveryStatus.Failed : DeliveryStatus.Delivered);
        if (terminal)
        {
            CloseTransportNow();
        }
    }

    // ------------------------------------------------------------------ state transitions

    private void SetState(PeerState next)
    {
        PeerState previous = _state;
        if (previous == next)
        {
            return;
        }

        _state = next;
        if (_transitionCount < _transitions.Length)
        {
            _transitions[_transitionCount++] = new StateTransition(previous, next);
        }
        else
        {
            // Unreachable (at most five changes per connection); keep the newest.
            _transitions[_transitions.Length - 1] = new StateTransition(previous, next);
        }
    }

    /// <summary>Raises queued <see cref="StateChanged"/> events in order (game thread). With <paramref name="holdClosed"/> the change to Closed stays queued.</summary>
    private void RaiseTransitions(bool holdClosed)
    {
        int raised = 0;
        while (raised < _transitionCount)
        {
            StateTransition transition = _transitions[raised];
            if (holdClosed && transition.To == PeerState.Closed)
            {
                break;
            }

            raised++;
            StateChanged?.Invoke(this, transition.From, transition.To);
        }

        if (raised > 0)
        {
            Array.Copy(_transitions, raised, _transitions, 0, _transitionCount - raised);
            _transitionCount -= raised;
        }
    }
}
