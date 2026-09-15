using System.Globalization;
using System.Net;
using System.Text;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Testing.Simulation;

/// <summary>Which <see cref="ITransportSink"/> callback a <see cref="RecordedEvent"/> came from.</summary>
public enum RecordedEventKind : byte
{
    /// <summary><see cref="ITransportSink.OnConnected"/>.</summary>
    Connected,
    /// <summary><see cref="ITransportSink.OnDatagramReceived"/>.</summary>
    DatagramReceived,
    /// <summary><see cref="ITransportSink.OnPeerStreamStarted"/>.</summary>
    PeerStreamStarted,
    /// <summary><see cref="ITransportSink.OnStreamStarted"/>.</summary>
    StreamStarted,
    /// <summary><see cref="ITransportSink.OnStreamReceived"/>.</summary>
    StreamReceived,
    /// <summary><see cref="ITransportSink.OnStreamSendCompleted"/>.</summary>
    StreamSendCompleted,
    /// <summary><see cref="ITransportSink.OnDatagramSendStateChanged"/>.</summary>
    DatagramSendStateChanged,
    /// <summary><see cref="ITransportSink.OnStreamAborted"/>.</summary>
    StreamAborted,
    /// <summary><see cref="ITransportSink.OnStreamPeerSendShutdown"/>.</summary>
    StreamPeerSendShutdown,
    /// <summary><see cref="ITransportSink.OnStreamShutdownComplete"/>.</summary>
    StreamShutdownComplete,
    /// <summary><see cref="ITransportSink.OnDatagramCapabilityChanged"/>.</summary>
    DatagramCapabilityChanged,
    /// <summary><see cref="ITransportSink.OnIdealSendBufferSize"/>.</summary>
    IdealSendBufferSize,
    /// <summary><see cref="ITransportSink.OnStreamsAvailable"/>.</summary>
    StreamsAvailable,
    /// <summary><see cref="ITransportSink.OnPeerAddressChanged"/>.</summary>
    PeerAddressChanged,
    /// <summary><see cref="ITransportSink.OnClosed"/>.</summary>
    Closed,
}

/// <summary>One recorded callback. Only the properties relevant to <see cref="Kind"/> are set; payloads are copies.</summary>
public sealed class RecordedEvent
{
    /// <summary>The callback.</summary>
    public RecordedEventKind Kind { get; init; }

    /// <summary>Clock time when the callback ran (0 when the sink has no clock).</summary>
    public long TimeMicros { get; init; }

    /// <summary>Stream the callback refers to.</summary>
    public TransportStreamId StreamId { get; init; }

    /// <summary>Send or open context.</summary>
    public ulong Context { get; init; }

    /// <summary>Status of <see cref="RecordedEventKind.StreamStarted"/>.</summary>
    public TransportStatus Status { get; init; }

    /// <summary>State of <see cref="RecordedEventKind.DatagramSendStateChanged"/>.</summary>
    public DatagramSendState DatagramState { get; init; }

    /// <summary>Datagram payload, or every byte delivered by a stream receive (all segments, consumed or not).</summary>
    public byte[] Data { get; init; } = [];

    /// <summary>Number of segments of a stream receive.</summary>
    public int SegmentCount { get; init; }

    /// <summary>Absolute stream offset of a stream receive.</summary>
    public ulong Offset { get; init; }

    /// <summary>FIN flag of a stream receive.</summary>
    public bool Fin { get; init; }

    /// <summary>What the sink returned from a stream receive.</summary>
    public ReceiveResult Result { get; init; }

    /// <summary>Canceled flag of a stream send completion.</summary>
    public bool Canceled { get; init; }

    /// <summary>Error code of an abort or close.</summary>
    public ulong ErrorCode { get; init; }

    /// <summary>Direction of an abort.</summary>
    public StreamAbortDirection Direction { get; init; }

    /// <summary>Kind of a peer-started stream.</summary>
    public StreamKind StreamKind { get; init; }

    /// <summary>Enabled flag of a datagram capability change.</summary>
    public bool Enabled { get; init; }

    /// <summary>Maximum payload of a datagram capability change.</summary>
    public int MaxPayload { get; init; }

    /// <summary>Bidirectional count of <see cref="RecordedEventKind.StreamsAvailable"/>.</summary>
    public ushort Bidirectional { get; init; }

    /// <summary>Unidirectional count of <see cref="RecordedEventKind.StreamsAvailable"/>.</summary>
    public ushort Unidirectional { get; init; }

    /// <summary>Byte count of <see cref="RecordedEventKind.IdealSendBufferSize"/>.</summary>
    public ulong Bytes { get; init; }

    /// <summary>Reason of <see cref="RecordedEventKind.Closed"/>.</summary>
    public TransportCloseReason CloseReason { get; init; }

    /// <summary>Transport status of <see cref="RecordedEventKind.Closed"/>.</summary>
    public int TransportStatus { get; init; }

    /// <summary>Capabilities reported by <see cref="RecordedEventKind.Connected"/>.</summary>
    public TransportCapabilities Capabilities { get; init; }

    /// <summary>Remote address of <see cref="RecordedEventKind.Connected"/> / <see cref="RecordedEventKind.PeerAddressChanged"/>.</summary>
    public IPEndPoint? RemoteEndPoint { get; init; }

    /// <summary>Local address of <see cref="RecordedEventKind.Connected"/> / <see cref="RecordedEventKind.PeerAddressChanged"/>.</summary>
    public IPEndPoint? LocalEndPoint { get; init; }

    /// <summary>Negotiated ALPN of <see cref="RecordedEventKind.Connected"/>.</summary>
    public string? Alpn { get; init; }

    /// <summary>A compact, culture-invariant description of every field (useful to compare runs).</summary>
    public override string ToString()
    {
        StringBuilder sb = new();
        sb.Append(CultureInfo.InvariantCulture, $"{TimeMicros} {Kind}");
        switch (Kind)
        {
            case RecordedEventKind.Connected:
                sb.Append(CultureInfo.InvariantCulture, $" alpn={Alpn} dg={Capabilities.Datagrams} max={Capabilities.MaxDatagramPayload}");
                break;
            case RecordedEventKind.DatagramReceived:
                sb.Append(CultureInfo.InvariantCulture, $" len={Data.Length} data={Convert.ToHexString(Data.AsSpan(0, Math.Min(16, Data.Length)))}");
                break;
            case RecordedEventKind.PeerStreamStarted:
                sb.Append(CultureInfo.InvariantCulture, $" {StreamId} {StreamKind}");
                break;
            case RecordedEventKind.StreamStarted:
                sb.Append(CultureInfo.InvariantCulture, $" {StreamId} ctx={Context} {Status}");
                break;
            case RecordedEventKind.StreamReceived:
                sb.Append(CultureInfo.InvariantCulture, $" {StreamId} off={Offset} len={Data.Length} segs={SegmentCount} fin={Fin} -> {Result.BytesConsumed}{(Result.Pending ? " pending" : "")}");
                break;
            case RecordedEventKind.StreamSendCompleted:
                sb.Append(CultureInfo.InvariantCulture, $" {StreamId} ctx={Context} canceled={Canceled}");
                break;
            case RecordedEventKind.DatagramSendStateChanged:
                sb.Append(CultureInfo.InvariantCulture, $" ctx={Context} {DatagramState}");
                break;
            case RecordedEventKind.StreamAborted:
                sb.Append(CultureInfo.InvariantCulture, $" {StreamId} code={ErrorCode} {Direction}");
                break;
            case RecordedEventKind.StreamPeerSendShutdown:
            case RecordedEventKind.StreamShutdownComplete:
                sb.Append(CultureInfo.InvariantCulture, $" {StreamId}");
                break;
            case RecordedEventKind.DatagramCapabilityChanged:
                sb.Append(CultureInfo.InvariantCulture, $" enabled={Enabled} max={MaxPayload}");
                break;
            case RecordedEventKind.IdealSendBufferSize:
                sb.Append(CultureInfo.InvariantCulture, $" {StreamId} bytes={Bytes}");
                break;
            case RecordedEventKind.StreamsAvailable:
                sb.Append(CultureInfo.InvariantCulture, $" bidi={Bidirectional} uni={Unidirectional}");
                break;
            case RecordedEventKind.PeerAddressChanged:
                sb.Append(CultureInfo.InvariantCulture, $" remote={RemoteEndPoint}");
                break;
            case RecordedEventKind.Closed:
                sb.Append(CultureInfo.InvariantCulture, $" {CloseReason} code={ErrorCode} status={TransportStatus}");
                break;
        }
        return sb.ToString();
    }
}

/// <summary>
/// An <see cref="ITransportSink"/> that records every callback (with payload copies) and optionally forwards it to an
/// inner sink. Thread-safe: callbacks may arrive on one thread while another reads or waits. Allocates per callback, so
/// it is a test tool, not something to measure allocations through.
/// </summary>
public sealed class RecordingSink : ITransportSink
{
    /// <summary>Decides the result of a stream receive when there is no inner sink.</summary>
    public delegate ReceiveResult StreamReceiveHandler(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin);

    private readonly List<RecordedEvent> _events = new();
    private readonly Dictionary<TransportStreamId, List<byte>> _streamData = new();
    private readonly IClock? _clock;
    private readonly ITransportSink? _inner;

    /// <summary>Creates a recorder.</summary>
    /// <param name="clock">Clock used to time-stamp events; <c>null</c> stamps 0.</param>
    /// <param name="inner">Sink every callback is forwarded to after recording; its receive results are recorded.</param>
    public RecordingSink(IClock? clock = null, ITransportSink? inner = null)
    {
        _clock = clock;
        _inner = inner;
    }

    /// <summary>Transport used to close streams automatically (see <see cref="AutoCloseStreams"/>).</summary>
    public ITransport? Transport { get; set; }

    /// <summary>
    /// When true (the default), there is no inner sink and <see cref="Transport"/> is set, the sink calls
    /// <see cref="ITransport.CloseStream"/> from <see cref="OnStreamShutdownComplete"/>, as the contract asks.
    /// </summary>
    public bool AutoCloseStreams { get; set; } = true;

    /// <summary>Receive policy when there is no inner sink; <c>null</c> consumes every byte.</summary>
    public StreamReceiveHandler? ReceiveHandler { get; set; }

    /// <summary>Snapshot of every event recorded so far, in order.</summary>
    public IReadOnlyList<RecordedEvent> Events
    {
        get
        {
            lock (_events)
                return _events.ToArray();
        }
    }

    /// <summary>Number of events recorded so far.</summary>
    public int Count
    {
        get
        {
            lock (_events)
                return _events.Count;
        }
    }

    /// <summary>True once <see cref="OnClosed"/> has been recorded.</summary>
    public bool IsClosed => CountOf(RecordedEventKind.Closed) > 0;

    /// <summary>Snapshot of the events of one kind, in order.</summary>
    public IReadOnlyList<RecordedEvent> OfKind(RecordedEventKind kind)
    {
        List<RecordedEvent> result = new();
        lock (_events)
        {
            foreach (RecordedEvent e in _events)
            {
                if (e.Kind == kind)
                    result.Add(e);
            }
        }
        return result;
    }

    /// <summary>Number of events of one kind.</summary>
    public int CountOf(RecordedEventKind kind)
    {
        int count = 0;
        lock (_events)
        {
            foreach (RecordedEvent e in _events)
            {
                if (e.Kind == kind)
                    count++;
            }
        }
        return count;
    }

    /// <summary>Every byte consumed on <paramref name="id"/> so far, in stream order.</summary>
    public byte[] GetStreamData(TransportStreamId id)
    {
        lock (_events)
            return _streamData.TryGetValue(id, out List<byte>? data) ? data.ToArray() : [];
    }

    /// <summary>Forgets every recorded event and stream byte.</summary>
    public void Clear()
    {
        lock (_events)
        {
            _events.Clear();
            _streamData.Clear();
        }
    }

    /// <summary>Waits until a recorded event (past or future) matches <paramref name="predicate"/>.</summary>
    /// <returns><c>false</c> on timeout.</returns>
    public bool WaitFor(Func<RecordedEvent, bool> predicate, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        long deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
        int checkedCount = 0;
        lock (_events)
        {
            while (true)
            {
                for (; checkedCount < _events.Count; checkedCount++)
                {
                    if (predicate(_events[checkedCount]))
                        return true;
                }
                long remaining = deadline - Environment.TickCount64;
                if (remaining <= 0)
                    return false;
                Monitor.Wait(_events, TimeSpan.FromMilliseconds(remaining));
            }
        }
    }

    /// <summary>Waits until at least <paramref name="count"/> events of <paramref name="kind"/> have been recorded.</summary>
    /// <returns><c>false</c> on timeout.</returns>
    public bool WaitForCount(RecordedEventKind kind, int count, TimeSpan timeout)
    {
        int seen = 0;
        return count <= 0 || WaitFor(e => e.Kind == kind && ++seen >= count, timeout);
    }

    private long Now => _clock?.NowMicros ?? 0;

    private void Add(RecordedEvent e)
    {
        lock (_events)
        {
            _events.Add(e);
            Monitor.PulseAll(_events);
        }
    }

    /// <inheritdoc/>
    public void OnConnected(in TransportConnectedInfo info)
    {
        ReadOnlySpan<byte> alpn = info.Alpn;
        Add(new RecordedEvent
        {
            Kind = RecordedEventKind.Connected, TimeMicros = Now, Capabilities = info.Capabilities,
            RemoteEndPoint = info.RemoteEndPoint, LocalEndPoint = info.LocalEndPoint,
            Alpn = Encoding.ASCII.GetString(alpn[..info.AlpnLength]),
        });
        _inner?.OnConnected(in info);
    }

    /// <inheritdoc/>
    public void OnDatagramReceived(ReadOnlySpan<byte> payload)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.DatagramReceived, TimeMicros = Now, Data = payload.ToArray() });
        _inner?.OnDatagramReceived(payload);
    }

    /// <inheritdoc/>
    public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.PeerStreamStarted, TimeMicros = Now, StreamId = id, StreamKind = kind });
        _inner?.OnPeerStreamStarted(id, kind);
    }

    /// <inheritdoc/>
    public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.StreamStarted, TimeMicros = Now, StreamId = id, Context = context, Status = status });
        _inner?.OnStreamStarted(id, context, status);
    }

    /// <inheritdoc/>
    public unsafe ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
    {
        int total = 0;
        foreach (TransportSegment segment in segments)
            total += (int)segment.Length;
        byte[] data = new byte[total];
        int offset = 0;
        foreach (TransportSegment segment in segments)
        {
            segment.AsSpan().CopyTo(data.AsSpan(offset));
            offset += (int)segment.Length;
        }

        ReceiveResult result = _inner is not null
            ? _inner.OnStreamReceived(id, segments, absoluteOffset, fin)
            : ReceiveHandler?.Invoke(id, segments, absoluteOffset, fin) ?? ReceiveResult.Consumed(total);

        lock (_events)
        {
            if (!_streamData.TryGetValue(id, out List<byte>? stream))
                _streamData[id] = stream = new List<byte>();
            int consumed = Math.Clamp(result.BytesConsumed, 0, total);
            for (int i = 0; i < consumed; i++)
                stream.Add(data[i]);
        }
        Add(new RecordedEvent
        {
            Kind = RecordedEventKind.StreamReceived, TimeMicros = Now, StreamId = id, Data = data, SegmentCount = segments.Length,
            Offset = absoluteOffset, Fin = fin, Result = result,
        });
        return result;
    }

    /// <inheritdoc/>
    public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.StreamSendCompleted, TimeMicros = Now, StreamId = id, Context = context, Canceled = canceled });
        _inner?.OnStreamSendCompleted(id, context, canceled);
    }

    /// <inheritdoc/>
    public void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.DatagramSendStateChanged, TimeMicros = Now, Context = context, DatagramState = state });
        _inner?.OnDatagramSendStateChanged(context, state);
    }

    /// <inheritdoc/>
    public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.StreamAborted, TimeMicros = Now, StreamId = id, ErrorCode = errorCode, Direction = direction });
        _inner?.OnStreamAborted(id, errorCode, direction);
    }

    /// <inheritdoc/>
    public void OnStreamPeerSendShutdown(TransportStreamId id)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.StreamPeerSendShutdown, TimeMicros = Now, StreamId = id });
        _inner?.OnStreamPeerSendShutdown(id);
    }

    /// <inheritdoc/>
    public void OnStreamShutdownComplete(TransportStreamId id)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.StreamShutdownComplete, TimeMicros = Now, StreamId = id });
        if (_inner is not null)
            _inner.OnStreamShutdownComplete(id);
        else if (AutoCloseStreams)
            Transport?.CloseStream(id);
    }

    /// <inheritdoc/>
    public void OnDatagramCapabilityChanged(bool enabled, int maxPayload)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.DatagramCapabilityChanged, TimeMicros = Now, Enabled = enabled, MaxPayload = maxPayload });
        _inner?.OnDatagramCapabilityChanged(enabled, maxPayload);
    }

    /// <inheritdoc/>
    public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.IdealSendBufferSize, TimeMicros = Now, StreamId = id, Bytes = bytes });
        _inner?.OnIdealSendBufferSize(id, bytes);
    }

    /// <inheritdoc/>
    public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.StreamsAvailable, TimeMicros = Now, Bidirectional = bidirectional, Unidirectional = unidirectional });
        _inner?.OnStreamsAvailable(bidirectional, unidirectional);
    }

    /// <inheritdoc/>
    public void OnPeerAddressChanged(in TransportConnectedInfo info)
    {
        Add(new RecordedEvent
        {
            Kind = RecordedEventKind.PeerAddressChanged, TimeMicros = Now, RemoteEndPoint = info.RemoteEndPoint, LocalEndPoint = info.LocalEndPoint,
        });
        _inner?.OnPeerAddressChanged(in info);
    }

    /// <inheritdoc/>
    public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
    {
        Add(new RecordedEvent { Kind = RecordedEventKind.Closed, TimeMicros = Now, CloseReason = reason, ErrorCode = errorCode, TransportStatus = transportStatus });
        _inner?.OnClosed(reason, errorCode, transportStatus);
    }
}
