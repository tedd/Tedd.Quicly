using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>
/// The receive half of the Bulk engine (PROTOCOL.md §3.3; docs/design/session-layer.md §7.7): the direct-mode receive
/// router, the progressive write into the application's <see cref="IBulkSink"/>, the per-range checksum and the
/// <c>BulkProgress</c> this end owes.
/// </summary>
/// <remarks>
/// <para><b>No ring entry, no reservation.</b> A bulk transfer's bytes never become a <c>ReceiveEntry</c>: they go
/// straight to the application's target as they arrive, so the <see cref="PeerCore.TryReserveReceive"/> /
/// <see cref="PeerCore.PublishReserved"/> protocol does not apply here — like a coalescing channel's mailbox, and unlike
/// every other stream mode (ADR 0008 invariant 6). A pooled lease is taken only to stage a <em>compressed</em> chunk and
/// the block it decodes into, which is what bounds a bulk transfer's receive footprint to two chunks rather than the
/// declared object size: <c>TotalLength</c>, <c>Offset</c> and <c>Length</c> are untrusted, so nothing is ever allocated
/// at the declared size.</para>
/// <para><b>Sizing rule.</b> Staging a compressed chunk needs a pooled block of the chunk's wire length and a second one
/// of its decoded length, so a peer that accepts chunked bulk transfers needs
/// <c>min(PeerOptions.BulkMaxChunk, the pool's largest block)</c> ≥ the largest chunk the peer sends, and a receive budget
/// of at least twice that. A chunk above that limit resets its stream with
/// <see cref="QuiclyErrorCode.LimitExceeded"/> (the transfer fails, the connection survives); a chunk that merely finds
/// the budget exhausted is <see cref="StreamConsume.Pend"/>ed and resumed from <see cref="QuiclyPeer.Poll"/>. This end
/// never sends a chunk larger than <see cref="PeerOptions.BulkChunkBytes"/> (default 64 KiB, one pooled block).</para>
/// <para><b>Checking a range.</b> The checksum trailer of PROTOCOL.md §3.3 covers exactly the bytes one transfer
/// carries, so every range is checkable on its own — whole object or resumed fragment, in order or not. A range whose
/// bytes do not match ends <see cref="BulkStatus.Failed"/> with <see cref="QuiclyErrorCode.BulkChecksumFailed"/>, and a
/// <c>BulkCancel</c> carrying that code tells its sender, whose object driver sends the range again. The progress this
/// end reports deliberately stops one byte short of the range until the trailer verifies, so the sender cannot complete
/// the transfer underneath that cancel.</para>
/// </remarks>
internal sealed unsafe partial class BulkEngine
{
    // Flags of BulkRecv.Flags (transport thread).
    private const byte RecvInUse = 1;
    private const byte RecvAccepted = 2;
    private const byte RecvChunked = 4;
    private const byte RecvChecksum = 8;
    private const byte RecvStaging = 32;
    private const byte RecvFinished = 64;

    /// <summary>
    /// Bytes the transport asked to keep outstanding per send stream, written by the transport thread
    /// (<see cref="OnIdealSendBufferSize"/>) and read by the game thread when it fills that stream. Advisory: a lost
    /// update only means one pass uses the previous window.
    /// </summary>
    private long[] _idealSendBuffer = [];

    private NativeArray<BulkRecv> _recv = null!;
    private IBulkSink?[] _sinks = [];

    /// <summary>
    /// The code <see cref="QuiclyPeer.CancelBulk"/> stopped a transfer with, or 0. Written by the game thread and read by
    /// the transport thread when the stream ends: stopping the peer's sending side with STOP_SENDING comes back as an
    /// ordinary shutdown (<c>aborted: false</c>), so without this the transfer we cancelled ourselves would end
    /// <see cref="BulkStatus.Failed"/>.
    /// </summary>
    private int[] _recvCancelCode = [];
    private XxHash64Builder[] _recvChecksums = [];
    private int[] _openStreams = [];
    private SpscRing<int> _retired = null!;
    private SpscRing<int> _recycle = null!;
    private int _freeRecv = -1;
    private int _recvLive;
    private int _retiredPending = -1;
    private int _maxStage;
    private int _resetReceive;

    /// <summary>
    /// Receive records per transfer this end accepts at once (<see cref="PeerOptions.BulkTransfersPerDirection"/>): the live
    /// ones, and the ones that ended and wait for the game thread to report them (<see cref="InitializeReceive"/>).
    /// </summary>
    internal const int ReceiveRecordsPerTransfer = 4;

    /// <summary>Peer bulk streams accepted on a channel right now (transport thread's count; tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>The number of accepted peer streams.</returns>
    internal int ReceiveStreamsOf(int channelIndex) => _openStreams[_localOf[channelIndex]];

    /// <summary>Receive transfers this end is holding (tests).</summary>
    internal int ReceiveTransfers => Volatile.Read(ref _recvLive);

    private void InitializeReceive(PeerCore core, int channelCount, int transfers)
    {
        _idealSendBuffer = new long[transfers];

        // More records than transfers may be live: a transfer that ended keeps its record until the game thread has sent its
        // final progress, and a sender whose transfer was canceled gets its slot back from the stream's close alone — on
        // this end's transport thread, whether or not its game thread is polling. So a sender that keeps the limit can
        // start transfers while earlier ones still wait to be reported; the live limit is _recvLive. Past
        // (ReceiveRecordsPerTransfer - 1) × transfers ended during one hitch of this end's host, the next is refused
        // (LimitExceeded) until the host polls again.
        int records = transfers * ReceiveRecordsPerTransfer;
        _recv = new NativeArray<BulkRecv>(records);
        _sinks = new IBulkSink?[records];
        _recvCancelCode = new int[records];
        _recvChecksums = new XxHash64Builder[records];
        _openStreams = new int[Math.Max(channelCount, 1)];
        _retired = new SpscRing<int>(records + 8);
        _recycle = new SpscRing<int>(records + 8);
        for (int record = records - 1; record >= 0; record--)
        {
            ref BulkRecv recv = ref _recv[record];
            recv = default;
            recv.Lease = BufferLease.Empty;
            recv.Next = _freeRecv;
            _freeRecv = record;
        }

        // A compressed chunk is staged whole and decoded into a second block, so neither the option nor the pool may be
        // exceeded; this end's own chunks are bounded by BulkChunkBytes, which fits a pooled block by default.
        _maxStage = Math.Min(core.BulkMaxChunk, core.Allocator.MaxBlockSize);
    }

    // ------------------------------------------------------------------ peer streams (transport thread)

    /// <inheritdoc/>
    public override StreamAccept OnStreamOpened(TransportStreamId id, ushort channel, ulong groupId)
    {
        ConsumeEpochReset();
        int dense = _core.ChannelIndexOf(channel);
        int local = dense >= 0 ? _localOf[dense] : -1;
        if (local < 0)
        {
            return StreamAccept.Reject(QuiclyErrorCode.UnsupportedChannel);
        }

        // Records the game thread finished reporting on are handed back here, so a long-lived session reuses them.
        while (_recycle.TryDequeue(out int returned))
        {
            ref BulkRecv recycled = ref _recv[returned];
            recycled = default;
            recycled.Lease = BufferLease.Empty;
            recycled.Next = _freeRecv;
            _freeRecv = returned;
        }

        // PROTOCOL.md §7: two transfers per direction per peer, and at most MaxGroups streams per channel.
        int record = _freeRecv;
        if (record < 0 || _recvLive >= _core.BulkTransfersPerDirection || _openStreams[local] >= _maxStreams[local])
        {
            return StreamAccept.Reject(QuiclyErrorCode.LimitExceeded);
        }

        ref BulkRecv recv = ref _recv[record];
        _freeRecv = recv.Next;
        recv = default;
        recv.Lease = BufferLease.Empty;
        recv.Next = -1;
        recv.Local = local;
        recv.Stream = id;
        recv.Flags = RecvInUse;
        _openStreams[local]++;
        Volatile.Write(ref _recvLive, _recvLive + 1);
        return StreamAccept.Accept(record);
    }

    /// <inheritdoc/>
    public override StreamConsume OnStreamMessage(ref StreamMessageContext message)
    {
        ConsumeEpochReset();
        int record = (int)message.Cookie;
        if ((uint)record >= (uint)_recv.Length)
        {
            return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
        }

        ref BulkRecv recv = ref _recv[record];
        return message.Phase switch
        {
            StreamMessagePhase.BulkHeader => OnBulkHeader(record, ref recv, ref message),
            StreamMessagePhase.BulkChecksum => OnBulkChecksum(record, ref recv, ref message),
            StreamMessagePhase.Start => OnBodyStart(ref recv, ref message),
            StreamMessagePhase.Chunk => OnBodyChunk(record, ref recv, ref message),
            StreamMessagePhase.End => OnBodyEnd(record, ref recv, ref message),
            _ => StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation),
        };
    }

    /// <summary>
    /// The transfer's header arrived and the framing layer has already validated every bound of PROTOCOL.md §3.3
    /// (<c>Length &gt; 0</c>, <c>Offset + Length ≤ TotalLength ≤ 2^62−1</c>, <c>Length ≤</c> the channel's
    /// <c>MaxMessageSize</c>, reserved flag bits, 32 hash bytes). What is left is this session's own rules — the transfer
    /// id must be free and the application's router must accept the descriptor — and both are checked <em>before</em> any
    /// state is created for the transfer.
    /// </summary>
    private StreamConsume OnBulkHeader(int record, ref BulkRecv recv, ref StreamMessageContext message)
    {
        if ((recv.Flags & RecvAccepted) != 0)
        {
            // One header per bulk stream; the parser produces no second one, so this is defence in depth.
            return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
        }

        BulkHeader header = message.Bulk;
        if (FindReceive(header.TransferId, record) >= 0)
        {
            // PROTOCOL.md §3.3: a transfer id is unique per peer and direction.
            _core.RecvCounters(message.ChannelIndex).Dropped++;
            return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
        }

        BulkTransferInfo info = new()
        {
            Channel = message.Channel,
            TransferId = header.TransferId,
            ObjectId = header.ObjectId,
            ObjectVersion = header.ObjectVersion,
            TotalLength = (long)header.TotalLength,
            Offset = (long)header.Offset,
            Length = (long)header.Length,
            HasChecksum = header.HasChecksum,
            Chunked = header.IsChunked,
            PeerIndex = _core.Peer.Index,
        };

        // PROTOCOL.md §3.3: the application's receive router must accept the descriptor, and the default is to reject.
        BulkReceiveDecision decision = _core.BulkRouter is { } router ? router.SelectTarget(in info) : BulkReceiveDecision.Reject();
        if (!decision.Accepted || decision.Sink is null)
        {
            _core.RecvCounters(message.ChannelIndex).Dropped++;
            return StreamConsume.ResetStream(decision.RejectCode == QuiclyErrorCode.NoError ? QuiclyErrorCode.BulkRejected : decision.RejectCode);
        }

        recv.TransferId = header.TransferId;
        recv.ObjectId = header.ObjectId;
        recv.ObjectVersion = header.ObjectVersion;
        recv.TotalLength = info.TotalLength;
        recv.Offset = info.Offset;
        recv.Length = info.Length;
        recv.Channel = message.Channel;
        recv.ChannelIndex = message.ChannelIndex;
        recv.Flags |= RecvAccepted;
        if (header.IsChunked)
        {
            recv.Flags |= RecvChunked;
        }

        _sinks[record] = decision.Sink;
        if (header.HasChecksum)
        {
            // Every range is checkable, whole object or not: the trailer covers exactly the bytes this transfer carries.
            recv.Flags |= RecvChecksum;
            _recvChecksums[record].Reset();
        }

        // LastReportMicros has one owner, the game thread (ADR 0008 invariant 4), so the window is not started here: the
        // first pass that sees the record stamps it (see FlushProgress).
        return StreamConsume.Continue;
    }

    /// <summary>
    /// A body piece begins: the whole body for an unchunked transfer, or one chunk. Only a <em>compressed</em> chunk is
    /// staged in a pooled block; everything else is written straight through as it arrives, so nothing is ever buffered at
    /// the declared size.
    /// </summary>
    private StreamConsume OnBodyStart(ref BulkRecv recv, ref StreamMessageContext message)
    {
        if ((recv.Flags & RecvAccepted) == 0)
        {
            return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
        }

        recv.ChunkLength = message.Header.Length;
        recv.ChunkRawLength = message.Header.RawLength;
        recv.ChunkFilled = 0;
        if (message.Header.RawLength <= 0)
        {
            return StreamConsume.Continue;
        }

        if (message.Header.Length > _maxStage || message.Header.RawLength > _maxStage)
        {
            // Within BulkMaxChunk but above what this peer's pool can stage: pending would stall the transfer for good, so
            // its stream is reset and the connection survives (PROTOCOL.md §6, the §7.7 sizing rule).
            _core.RecvCounters(message.ChannelIndex).TooLarge++;
            return StreamConsume.ResetStream(QuiclyErrorCode.LimitExceeded);
        }

        if (!_core.TryRentReceive(message.Header.Length, out BufferLease lease))
        {
            // Genuine back-pressure: the budget frees up when the game thread polls, and the peer un-reads this event.
            _core.Counters.OutOfReceiveBuffers++;
            _core.RecvCounters(message.ChannelIndex).OutOfBuffers++;
            return StreamConsume.Pend;
        }

        recv.Lease = lease;
        recv.Flags |= RecvStaging;
        return StreamConsume.Continue;
    }

    private StreamConsume OnBodyChunk(int record, ref BulkRecv recv, ref StreamMessageContext message)
    {
        if ((recv.Flags & RecvAccepted) == 0)
        {
            return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
        }

        ReadOnlySpan<byte> chunk = message.Chunk;
        if ((recv.Flags & RecvStaging) != 0)
        {
            if (chunk.Length > recv.ChunkLength - recv.ChunkFilled)
            {
                // The parser never produces more bytes than the chunk header promised; defence in depth.
                return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
            }

            chunk.CopyTo(new Span<byte>(_core.GetPointer(in recv.Lease) + recv.ChunkFilled, recv.ChunkLength - recv.ChunkFilled));
            recv.ChunkFilled += chunk.Length;
            return StreamConsume.Continue;
        }

        return Deliver(record, ref recv, chunk, ref message);
    }

    private StreamConsume OnBodyEnd(int record, ref BulkRecv recv, ref StreamMessageContext message)
    {
        if ((recv.Flags & RecvStaging) != 0)
        {
            int raw = recv.ChunkRawLength;
            if (!_core.TryRentReceive(raw, out BufferLease decoded))
            {
                // The staged bytes are already consumed, so this cannot be pended: the transfer's stream is reset instead.
                _core.Counters.OutOfReceiveBuffers++;
                _core.RecvCounters(message.ChannelIndex).OutOfBuffers++;
                return StreamConsume.ResetStream(QuiclyErrorCode.LimitExceeded);
            }

            ReadOnlySpan<byte> source = new(_core.GetPointer(in recv.Lease), recv.ChunkFilled);
            Span<byte> target = new(_core.GetPointer(in decoded), raw);
            bool ok = Lz4Block.DecompressExact(source, target);
            ReleaseStaging(ref recv);
            StreamConsume result = ok
                ? Deliver(record, ref recv, target, ref message)
                : StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
            _core.ReturnReceive(in decoded);
            if (!ok)
            {
                _core.RecvCounters(message.ChannelIndex).Dropped++;
            }

            return result;
        }

        return StreamConsume.Continue;
    }

    /// <summary>
    /// The range's checksum trailer arrived (PROTOCOL.md §3.3), which is what completes a checksummed transfer: every
    /// byte is in, and now this end knows whether they are the bytes that were read.
    /// </summary>
    /// <remarks>
    /// A mismatch fails <em>this range</em> and nothing more. The range is the unit of recovery — the whole point of
    /// checksumming per range rather than per object — so the peer is told with a <c>BulkCancel</c> carrying
    /// <see cref="QuiclyErrorCode.BulkChecksumFailed"/> (receiver to sender, PROTOCOL.md §3.4) and its object driver
    /// sends that range again. The bytes already written to the application are overwritten by the retry.
    /// </remarks>
    private StreamConsume OnBulkChecksum(int record, ref BulkRecv recv, ref StreamMessageContext message)
    {
        if ((recv.Flags & RecvAccepted) == 0 || (recv.Flags & RecvChecksum) == 0)
        {
            // The parser produces this only for a transfer whose header promised it; defence in depth.
            return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
        }

        if (_recvChecksums[record].Digest() != message.BulkChecksum)
        {
            _core.RecvCounters(message.ChannelIndex).Dropped++;
            _core.Counters.BulkChecksumFailures++;

            // BytesAccepted stays short of the range, so the peer's transfer cannot complete on a final progress frame
            // that races the cancel.
            FinishReceive(record, ref recv, BulkStatus.Failed, QuiclyErrorCode.BulkChecksumFailed);
            return StreamConsume.Continue;
        }

        Volatile.Write(ref recv.BytesAccepted, recv.Length);
        FinishReceive(record, ref recv, BulkStatus.Completed, QuiclyErrorCode.NoError);
        return StreamConsume.Continue;
    }

    /// <summary>Writes decoded object bytes to the application's target, checksums them and completes the transfer at its last byte.</summary>
    private StreamConsume Deliver(int record, ref BulkRecv recv, ReadOnlySpan<byte> data, ref StreamMessageContext message)
    {
        if (data.IsEmpty)
        {
            return StreamConsume.Continue;
        }

        long accepted = recv.BytesAccepted;
        if (data.Length > recv.Length - accepted)
        {
            // More bytes than the header promised; the parser bounds this, so this is defence in depth.
            return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
        }

        _sinks[record]!.Write(recv.Offset + accepted, data);
        bool checksummed = (recv.Flags & RecvChecksum) != 0;
        if (checksummed)
        {
            _recvChecksums[record].Append(data);
        }

        accepted += data.Length;

        // With a trailer coming, the range is not *accepted* until it verifies. Reporting the whole range now would
        // complete the sender's transfer, and it would never learn that it has to send this range again.
        Volatile.Write(ref recv.BytesAccepted, checksummed && accepted >= recv.Length ? recv.Length - 1 : accepted);
        ref ChannelRecvCounters counters = ref _core.RecvCounters(message.ChannelIndex);
        counters.Received++;
        counters.Bytes += data.Length;
        _core.NoteTransportWork();
        if (accepted >= recv.Length && !checksummed)
        {
            FinishReceive(record, ref recv, BulkStatus.Completed, QuiclyErrorCode.NoError);
        }

        return StreamConsume.Continue;
    }

    /// <summary>
    /// Finishes a receive transfer exactly once: the hash is checked when this end saw the whole object, the application's
    /// <see cref="IBulkSink.Finish"/> is called, and the record is handed to the game thread so the final
    /// <c>BulkProgress</c> goes out (PROTOCOL.md §2.3: progress is sent on completion).
    /// </summary>
    private void FinishReceive(int record, ref BulkRecv recv, BulkStatus status, QuiclyErrorCode code)
    {
        if ((recv.Flags & RecvFinished) != 0)
        {
            return;
        }

        recv.Flags |= RecvFinished;
        recv.Status = (byte)status;
        recv.Code = (uint)code;
        ReleaseStaging(ref recv);
        _sinks[record]?.Finish(new BulkResult(status, recv.BytesAccepted, code));
    }

    /// <inheritdoc/>
    public override void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode)
    {
        for (int record = 0; record < _recv.Length; record++)
        {
            ref BulkRecv recv = ref _recv[record];
            if ((recv.Flags & RecvInUse) == 0 || recv.Stream != id)
            {
                continue;
            }

            if ((recv.Flags & RecvAccepted) != 0)
            {
                // A completed transfer was finished at its last byte and FinishReceive ignores it. For an unfinished one: a
                // FIN before the last byte is a parser error, which resets the stream, so a stream that shuts down with no
                // reset in either direction and no cancel of ours was taken down by the connection itself — the transport
                // shuts every stream down before it reports its own close, so IsTransportClosing is not set yet.
                int cancelled = Volatile.Read(ref _recvCancelCode[record]);
                QuiclyErrorCode code = cancelled != 0 ? (QuiclyErrorCode)cancelled : (QuiclyErrorCode)errorCode;
                BulkStatus status = _core.IsTransportClosing ? BulkStatus.Disconnected
                    : cancelled != 0 || (aborted && code == QuiclyErrorCode.BulkCanceled) ? BulkStatus.Canceled
                    : aborted ? BulkStatus.Failed
                    : BulkStatus.Disconnected;
                FinishReceive(record, ref recv, status, aborted ? code : QuiclyErrorCode.NoError);
            }

            ReleaseStaging(ref recv);
            _openStreams[recv.Local]--;
            recv.Stream = default;
            recv.Flags = (byte)(recv.Flags & ~RecvInUse);
            Volatile.Write(ref _recvLive, _recvLive - 1);

            // The game thread sends the final progress and then recycles the record; nothing touches it here again.
            if (!_retired.TryEnqueue(in record))
            {
                _core.Counters.CallbackFaults++;
                RecycleDirect(record);
            }

            _core.NoteTransportWork();
            return;
        }

        if (!id.IsValid)
        {
            return;
        }

        for (int record = 0; record < _txStreams.Length; record++)
        {
            if (_txStreams[record] == id)
            {
                Post(record, _txSerials[record], aborted ? NoticeKind.Stopped : NoticeKind.ShutDown, errorCode);

                // The stream is gone either way, so the transport-side entries go with it and no later event of that id
                // resolves to this transfer again.
                _txStreams[record] = default;
                _txSerials[record] = 0;
                return;
            }
        }
    }

    private void RecycleDirect(int record)
    {
        ref BulkRecv recv = ref _recv[record];
        _sinks[record] = null;
        Volatile.Write(ref _recvCancelCode[record], 0);
        recv = default;
        recv.Lease = BufferLease.Empty;
        recv.Next = _freeRecv;
        _freeRecv = record;
    }

    private void ReleaseStaging(ref BulkRecv recv)
    {
        if ((recv.Flags & RecvStaging) != 0)
        {
            recv.Flags = (byte)(recv.Flags & ~RecvStaging);
        }

        if (!recv.Lease.IsEmpty)
        {
            _core.ReturnReceive(in recv.Lease);
            recv.Lease = BufferLease.Empty;
        }
    }

    /// <summary>
    /// The record holding <paramref name="transferId"/>, ignoring <paramref name="except"/> (transport thread). A transfer
    /// that has <em>finished</em> but whose record is still on its way back through the retire/recycle rings counts: a
    /// transfer id is unique per peer and direction for the epoch (PROTOCOL.md §3.3), and it is enforced over every transfer
    /// this end still holds any trace of. Once the record is recycled the id is free again — the tolerance PROTOCOL.md §8
    /// documents, because remembering every id an epoch has seen would be unbounded state the peer controls.
    /// </summary>
    /// <param name="transferId">The id to look for.</param>
    /// <param name="except">A record to ignore (the one being opened).</param>
    /// <returns>The record, or −1.</returns>
    private int FindReceive(ulong transferId, int except)
    {
        for (int record = 0; record < _recv.Length; record++)
        {
            ref BulkRecv recv = ref _recv[record];
            if (record != except && (recv.Flags & RecvAccepted) != 0 && recv.TransferId == transferId)
            {
                return record;
            }
        }

        return -1;
    }

    // ------------------------------------------------------------------ progress this end owes (game thread)

    /// <summary>
    /// Sends the <c>BulkProgress</c> frames that are due (PROTOCOL.md §2.3: at most every 64 KiB or 100 ms per transfer,
    /// and on completion). The window advances only when a frame really went out, so a pass that could send nothing does
    /// not silently skip it.
    /// </summary>
    private void FlushProgress(ref FlushContext flush)
    {
        // A retired transfer's final progress goes first and is never dropped: its record is recycled only once it is out, and
        // one whose frame could not go out stays pending for the next pass. The ring is dequeued into a local, never into the
        // field: a ring's out value is undefined when TryDequeue returns false (ADR 0008 invariant 5), and the next pass would
        // then "retire" whatever record that turned out to name, null its sink and recycle it underneath the transport thread.
        int retired = _retiredPending;
        while (retired >= 0 || _retired.TryDequeue(out retired))
        {
            _retiredPending = retired;
            ref BulkRecv recv = ref _recv[retired];
            if (!SendProgress(recv.TransferId, recv.BytesAccepted))
            {
                return;
            }

            if (recv.Code == (uint)QuiclyErrorCode.BulkChecksumFailed)
            {
                // Telling the peer is what lets its object driver send this range again. BulkCancel is receiver-to-sender
                // (PROTOCOL.md §3.4), which is exactly this direction, and the progress above stopped short of the range
                // so the peer's transfer has not completed underneath it.
                SendBulkCancel(recv.TransferId, QuiclyErrorCode.BulkChecksumFailed);
            }

            _retiredPending = -1;
            _sinks[retired] = null;
            SettleRequest(ref recv);
            if (!_recycle.TryEnqueue(in retired))
            {
                _core.Counters.CallbackFaults++;
            }

            retired = -1;
        }

        for (int record = 0; record < _recv.Length; record++)
        {
            ref BulkRecv recv = ref _recv[record];
            if (!TryReadProgress(ref recv, out ulong transferId, out long accepted))
            {
                continue;
            }

            if (recv.LastReportMicros == 0)
            {
                // First pass that sees this transfer: the game thread owns the field, so this is where its 100 ms window
                // starts (the transport thread stamped nothing when the header arrived).
                recv.LastReportMicros = flush.NowMicros;
            }

            long unreported = accepted - recv.ReportedBytes;
            if (unreported <= 0)
            {
                continue;
            }

            if (unreported < ProgressBytes && flush.NowMicros - recv.LastReportMicros < ProgressMicros)
            {
                continue;
            }

            if (!SendProgress(transferId, accepted))
            {
                return;
            }

            recv.ReportedBytes = accepted;
            recv.LastReportMicros = flush.NowMicros;
        }
    }

    /// <summary>
    /// Reads a live transfer's id and accepted bytes (game thread). The record is owned by the transport thread, which may
    /// retire and reuse it at any moment, so the id is read on both sides of the byte count: a record that changed hands
    /// is skipped, and the transfer's own final progress comes from <see cref="_retired"/> instead, which is exact.
    /// </summary>
    private bool TryReadProgress(ref BulkRecv recv, out ulong transferId, out long accepted)
    {
        transferId = recv.TransferId;
        accepted = Volatile.Read(ref recv.BytesAccepted);
        byte flags = recv.Flags;
        return (flags & RecvInUse) != 0 && (flags & RecvAccepted) != 0 && transferId != 0 && recv.TransferId == transferId;
    }

    private bool SendProgress(ulong transferId, long bytesAccepted)
    {
        if (transferId == 0)
        {
            return true;
        }

        // The carrier is chosen before the frame is encoded: the two framings differ (PROTOCOL.md §2.3, §3.4).
        ControlCarrier carrier = _core.DatagramsEnabled && _core.MaxDatagramPayload >= 32 ? ControlCarrier.Datagram : ControlCarrier.Stream;
        Span<byte> frame = stackalloc byte[32];
        BulkProgress progress = new(transferId, (ulong)bytesAccepted);
        return ControlCodec.TryWrite(frame, in progress, carrier, out int written) && _core.SendControlFrame(frame.Slice(0, written), carrier);
    }

    /// <summary>
    /// Whether this end owes the peer progress: a retired transfer's final <c>BulkProgress</c>, or bytes a live transfer
    /// accepted since it last reported (the records <see cref="TickProgress"/> gives a deadline). Game thread; advisory from
    /// another thread.
    /// </summary>
    private bool OwesProgress()
    {
        if (Volatile.Read(ref _recvLive) != 0)
        {
            for (int record = 0; record < _recv.Length; record++)
            {
                ref BulkRecv recv = ref _recv[record];
                if (TryReadProgress(ref recv, out _, out long accepted) && accepted > recv.ReportedBytes)
                {
                    return true;
                }
            }
        }

        // A transfer leaves the live records before it is retired (OnStreamClosed); a probe that falls between the two is not
        // lost work, because the retirement raises the signal after it is published.
        return _retiredPending >= 0 || !_retired.IsEmpty;
    }

    /// <summary>Lowers the flush deadline to the next progress frame a transfer owes (never to a time at or before now).</summary>
    private void TickProgress(long nowMicros, ref long nextDeadline)
    {
        if (_retiredPending >= 0 || !_retired.IsEmpty)
        {
            LowerDeadline(nowMicros, nowMicros + 1_000, ref nextDeadline);
            return;
        }

        for (int record = 0; record < _recv.Length; record++)
        {
            ref BulkRecv recv = ref _recv[record];
            if (TryReadProgress(ref recv, out _, out long accepted) && accepted > recv.ReportedBytes)
            {
                // A record the game thread has not stamped yet is due at the next pass, which is where its window starts.
                long stamped = recv.LastReportMicros;
                LowerDeadline(nowMicros, stamped == 0 ? nowMicros : stamped + ProgressMicros, ref nextDeadline);
            }
        }
    }

    /// <summary>Bytes of progress that force a <c>BulkProgress</c> frame (PROTOCOL.md §2.3).</summary>
    private const int ProgressBytes = 64 * 1024;

    /// <summary>Micros of progress that force a <c>BulkProgress</c> frame (PROTOCOL.md §2.3).</summary>
    private const long ProgressMicros = 100_000;

    // ------------------------------------------------------------------ cancel, epoch, teardown

    /// <summary>
    /// Cancels a transfer the peer is sending to this end (game thread, <see cref="QuiclyPeer.CancelBulk"/>): the stream is
    /// stopped with STOP_SENDING and a <c>BulkCancel</c> tells the peer to stop reading its source. The transport thread's
    /// close notice then finishes the application's sink <see cref="BulkStatus.Canceled"/>.
    /// </summary>
    /// <param name="channelIndex">Dense index of the Bulk channel.</param>
    /// <param name="transferId">The peer's transfer id.</param>
    /// <param name="code">The code sent to the peer.</param>
    /// <returns><see langword="false"/> when no such transfer is running.</returns>
    internal bool CancelReceive(int channelIndex, ulong transferId, QuiclyErrorCode code)
    {
        int local = (uint)channelIndex < (uint)_localOf.Length ? _localOf[channelIndex] : -1;
        if (local < 0)
        {
            return false;
        }

        for (int record = 0; record < _recv.Length; record++)
        {
            ref BulkRecv recv = ref _recv[record];
            byte flags = recv.Flags;
            if ((flags & RecvInUse) == 0 || (flags & RecvAccepted) == 0 || (flags & RecvFinished) != 0
                || recv.Local != local || recv.TransferId != transferId)
            {
                continue;
            }

            // Recorded before the abort: the shutdown it causes is what reads it.
            Volatile.Write(ref _recvCancelCode[record], (int)(code == QuiclyErrorCode.NoError ? QuiclyErrorCode.BulkCanceled : code));
            TransportStreamId stream = recv.Stream;
            if (stream.IsValid)
            {
                _core.Transport?.AbortStream(stream, (ulong)code, StreamAbortDirection.Receive);
                _core.Counters.StreamsReset++;
            }

            SendBulkCancel(transferId, code);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Asks the transport thread to forget the transfer ids and records of the epoch that just ended, and drops the control
    /// messages of that epoch (game thread, <see cref="OnEpochReset"/>). Transfer ids are scoped to the epoch like every
    /// other counter (PROTOCOL.md §1, §4.1).
    /// </summary>
    private void ResetReceiveState()
    {
        while (_control.TryDequeue(out _))
        {
        }

        Volatile.Write(ref _resetReceive, 1);
    }

    /// <summary>
    /// Forgets the previous epoch's receive state once, if a new epoch asked for it. Every receive entry point calls it —
    /// a stream's open, its messages and a control message — because the first thing a resumed session sees may be any of
    /// the three. Transport thread.
    /// </summary>
    private void ConsumeEpochReset()
    {
        if (Volatile.Read(ref _resetReceive) == 0 || Interlocked.Exchange(ref _resetReceive, 0) == 0)
        {
            return;
        }

        for (int record = 0; record < _recv.Length; record++)
        {
            ref BulkRecv recv = ref _recv[record];
            if ((recv.Flags & RecvInUse) == 0 && recv.TransferId == 0)
            {
                continue;
            }

            if ((recv.Flags & RecvAccepted) != 0)
            {
                FinishReceive(record, ref recv, BulkStatus.Disconnected, QuiclyErrorCode.NoError);
            }

            ReleaseStaging(ref recv);
            if ((recv.Flags & RecvInUse) != 0)
            {
                _openStreams[recv.Local]--;
                Volatile.Write(ref _recvLive, _recvLive - 1);
            }

            RecycleDirect(record);
        }
    }

    private void OnPeerClosedReceive()
    {
        for (int record = 0; record < _recv.Length; record++)
        {
            ref BulkRecv recv = ref _recv[record];
            if ((recv.Flags & RecvAccepted) != 0)
            {
                // The transport can no longer call back, so the game thread finishes what the connection left half done,
                // and settles the range request each finished transfer answered — including one retired but not yet
                // reported, whose final progress will never go out now.
                FinishReceive(record, ref recv, BulkStatus.Disconnected, QuiclyErrorCode.NoError);
                SettleRequest(ref recv);
            }

            ReleaseStaging(ref recv);
        }
    }

    /// <summary>
    /// Settles the range request a finished receive transfer answered (game thread, once per transfer). A transfer answers a
    /// pending <see cref="QuiclyPeer.RequestBulk"/> when its channel, object identity and first byte are the request's — a
    /// provider may shorten the range, never move its start (<see cref="IBulkProvider.TryGetObject"/>). The request is then
    /// released, so the table of PROTOCOL.md §7 holds only requests still waiting for an answer; the application learns the
    /// outcome from its sink. The one exception is a <em>resumable</em> request whose transfer the connection cut: it keeps
    /// the part still missing, which <see cref="OnEpochReset"/> asks for on the resumed session (PROTOCOL.md §4.1).
    /// </summary>
    /// <param name="recv">A finished, accepted receive transfer whose record the transport thread no longer touches.</param>
    private void SettleRequest(ref BulkRecv recv)
    {
        if (recv.Settled != 0 || (recv.Flags & RecvAccepted) == 0)
        {
            return;
        }

        recv.Settled = 1;
        for (int i = 0; i < _requests.Length; i++)
        {
            ref PendingRequest request = ref _requests[i];
            BulkRangeRequest range = request.Range;
            if (!request.InUse || range.Channel != recv.Channel || range.ObjectId != recv.ObjectId
                || range.ObjectVersion != recv.ObjectVersion || range.Offset != recv.Offset)
            {
                continue;
            }

            long accepted = Volatile.Read(ref recv.BytesAccepted);
            if ((BulkStatus)recv.Status == BulkStatus.Disconnected && range.Resumable && accepted < range.Length)
            {
                request.Range = range with { Offset = range.Offset + accepted, Length = range.Length - accepted };
            }
            else
            {
                request = default;
            }

            return;
        }
    }

    private void OnReconnectingReceive()
    {
        while (_retired.TryDequeue(out _))
        {
        }

        while (_recycle.TryDequeue(out _))
        {
        }

        _retiredPending = -1;
        _freeRecv = -1;
        for (int record = _recv.Length - 1; record >= 0; record--)
        {
            ref BulkRecv recv = ref _recv[record];
            ReleaseStaging(ref recv);
            _sinks[record] = null;
            recv = default;
            recv.Lease = BufferLease.Empty;
            recv.Next = _freeRecv;
            _freeRecv = record;
        }

        Array.Clear(_openStreams);
        Array.Clear(_recvCancelCode);
        Volatile.Write(ref _recvLive, 0);
        Volatile.Write(ref _resetReceive, 0);
        Array.Clear(_idealSendBuffer);
    }

    /// <summary>
    /// Finishes what the transport left half received and returns its staging leases. The peer frees its memory only once
    /// the transport has reported its close and no Poll or Flush is running, so nothing else can touch a receive record now:
    /// this is the one place a <em>disposed</em> peer can keep <see cref="IBulkSink.Finish"/>'s "exactly once per accepted
    /// transfer" promise, because the dispose hook itself runs while the transport thread may still own these records
    /// (<see cref="OnDisposing"/>). On the close path <see cref="OnPeerClosed"/> has finished them already and this finds
    /// nothing left to do.
    /// </summary>
    private void DisposeReceive()
    {
        if (_recv is not null && !_recv.IsDisposed)
        {
            for (int record = 0; record < _recv.Length; record++)
            {
                ref BulkRecv recv = ref _recv[record];
                if ((recv.Flags & RecvAccepted) != 0)
                {
                    FinishReceive(record, ref recv, BulkStatus.Disconnected, QuiclyErrorCode.NoError);
                }

                ReleaseStaging(ref recv);
            }

            _recv.Dispose();
        }

        _retired?.Dispose();
        _recycle?.Dispose();
    }

    /// <summary>
    /// One transfer the peer is sending to this end. The first two cache lines are the transport thread's (it parses and
    /// writes through to the application); the third is the game thread's (it sends the progress frames), so the two
    /// owners never share a line (ADR 0008 invariant 4). Every field is laid out at its natural alignment and inside
    /// <c>Size</c>: an explicit layout that overflows its size, or misaligns an 8-byte field, fails to load the type at
    /// all — and it would do so inside a transport callback.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 192)]
    private struct BulkRecv
    {
        /// <summary>The peer's transfer id.</summary>
        [FieldOffset(0)] public ulong TransferId;

        /// <summary>Object bytes written to the application's sink (published for the game thread).</summary>
        [FieldOffset(8)] public long BytesAccepted;

        /// <summary>Object bytes the range carries.</summary>
        [FieldOffset(16)] public long Length;

        /// <summary>First object byte of the range.</summary>
        [FieldOffset(24)] public long Offset;

        /// <summary>Lease staging a compressed chunk, or empty.</summary>
        [FieldOffset(32)] public BufferLease Lease;

        /// <summary>The peer's stream.</summary>
        [FieldOffset(48)] public TransportStreamId Stream;

        /// <summary>Wire bytes of the chunk being staged.</summary>
        [FieldOffset(56)] public int ChunkLength;

        /// <summary>Wire bytes of it copied so far.</summary>
        [FieldOffset(60)] public int ChunkFilled;

        /// <summary>Declared size of the whole object.</summary>
        [FieldOffset(64)] public long TotalLength;

        /// <summary>Object identity.</summary>
        [FieldOffset(72)] public ulong ObjectId;

        /// <summary>Object version.</summary>
        [FieldOffset(80)] public ulong ObjectVersion;

        /// <summary>Decoded size of the chunk being staged, or 0 when it is stored uncompressed.</summary>
        [FieldOffset(88)] public int ChunkRawLength;

        /// <summary>Engine-local index of the transfer's channel.</summary>
        [FieldOffset(92)] public int Local;

        /// <summary>Dense channel index (counters).</summary>
        [FieldOffset(96)] public int ChannelIndex;

        /// <summary>Next free record (-1 = none).</summary>
        [FieldOffset(100)] public int Next;

        /// <summary>The error code the transfer ended with.</summary>
        [FieldOffset(104)] public uint Code;

        /// <summary>The channel id.</summary>
        [FieldOffset(108)] public ushort Channel;

        /// <summary><see cref="RecvInUse"/>, <see cref="RecvAccepted"/>, <see cref="RecvChunked"/>, <see cref="RecvChecksum"/>, <see cref="RecvStaging"/>, <see cref="RecvFinished"/>.</summary>
        [FieldOffset(110)] public byte Flags;

        /// <summary>The <see cref="BulkStatus"/> the transfer ended with.</summary>
        [FieldOffset(111)] public byte Status;

        // ---- game thread from here (its own cache line)

        /// <summary>Bytes the last <c>BulkProgress</c> reported.</summary>
        [FieldOffset(128)] public long ReportedBytes;

        /// <summary>Clock micros of the last <c>BulkProgress</c>, or 0 until the first pass that sees the transfer stamps it.</summary>
        [FieldOffset(136)] public long LastReportMicros;

        /// <summary>1 once the game thread matched the finished transfer against this end's range requests (<see cref="SettleRequest"/>).</summary>
        [FieldOffset(144)] public byte Settled;
    }
}
