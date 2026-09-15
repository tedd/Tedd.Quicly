using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Conformance;

/// <summary>Native memory whose address never changes: payloads handed to a transport live here until their completion.</summary>
internal sealed unsafe class NativeBuffer : IDisposable
{
    public NativeBuffer(int length)
    {
        Length = length;
        Pointer = (byte*)NativeMemory.AllocZeroed((nuint)Math.Max(1, length));
    }

    public byte* Pointer { get; private set; }

    public int Length { get; }

    public static byte PatternAt(long index, int seed) => (byte)((index * 31) + (seed * 17) + (index >> 9));

    public void Fill(int seed)
    {
        for (int i = 0; i < Length; i++) Pointer[i] = PatternAt(i, seed);
    }

    public TransportSegment Segment(int offset, int length) => new(Pointer + offset, length);

    public byte[] ToArray(int offset, int length) => new ReadOnlySpan<byte>(Pointer + offset, length).ToArray();

    public void Dispose()
    {
        if (Pointer == null) return;
        NativeMemory.Free(Pointer);
        Pointer = null;
    }
}

/// <summary>A native array of segments (the contract requires the array itself to stay valid until the completion too).</summary>
internal sealed unsafe class NativeSegments : IDisposable
{
    public NativeSegments(int count)
    {
        Count = count;
        Pointer = (TransportSegment*)NativeMemory.AllocZeroed((nuint)(Math.Max(1, count) * sizeof(TransportSegment)));
    }

    public TransportSegment* Pointer { get; private set; }

    public int Count { get; }

    public TransportSegment* At(int index) => Pointer + index;

    public void Set(int index, TransportSegment segment) => Pointer[index] = segment;

    public void Dispose()
    {
        if (Pointer == null) return;
        NativeMemory.Free(Pointer);
        Pointer = null;
    }
}

/// <summary>An <see cref="ITransportSink"/> whose callbacks do nothing (receives consume everything); scenarios override what they need.</summary>
internal abstract class SinkBase : ITransportSink
{
    public virtual void OnConnected(in TransportConnectedInfo info)
    {
    }

    public virtual void OnDatagramReceived(ReadOnlySpan<byte> payload)
    {
    }

    public virtual void OnPeerStreamStarted(TransportStreamId id, StreamKind kind)
    {
    }

    public virtual void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
    }

    public virtual ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
    {
        int total = 0;
        foreach (TransportSegment segment in segments) total += (int)segment.Length;
        return ReceiveResult.Consumed(total);
    }

    public virtual void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled)
    {
    }

    public virtual void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
    {
    }

    public virtual void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
    }

    public virtual void OnStreamPeerSendShutdown(TransportStreamId id)
    {
    }

    public virtual void OnStreamShutdownComplete(TransportStreamId id)
    {
    }

    public virtual void OnDatagramCapabilityChanged(bool enabled, int maxPayload)
    {
    }

    public virtual void OnIdealSendBufferSize(TransportStreamId id, ulong bytes)
    {
    }

    public virtual void OnStreamsAvailable(ushort bidirectional, ushort unidirectional)
    {
    }

    public virtual void OnPeerAddressChanged(in TransportConnectedInfo info)
    {
    }

    public virtual void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
    {
    }
}

/// <summary>
/// One pair under test: recording sinks on both ends (optionally forwarding to scenario sinks), scenario memory that is
/// freed only once both ends reported <see cref="ITransportSink.OnClosed"/>, and failure reporting with event dumps.
/// </summary>
internal sealed class Session : IDisposable
{
    private readonly List<IDisposable> _memory = [];

    public Session(ITransportTestHarness harness, ConformancePairOptions? options = null, ITransportSink? clientInner = null, ITransportSink? serverInner = null)
    {
        Harness = harness;
        ClientSink = new RecordingSink(null, clientInner);
        ServerSink = new RecordingSink(null, serverInner);
        Pair = harness.CreatePair(ClientSink, ServerSink, options);
        ClientSink.Transport = Pair.Client;
        ServerSink.Transport = Pair.Server;
    }

    public ITransportTestHarness Harness { get; }

    public RecordingSink ClientSink { get; }

    public RecordingSink ServerSink { get; }

    public ConformancePair Pair { get; }

    public ITransport Client => Pair.Client;

    public ITransport Server => Pair.Server;

    public NativeBuffer Rent(int length, int seed)
    {
        var buffer = new NativeBuffer(length);
        buffer.Fill(seed);
        _memory.Add(buffer);
        return buffer;
    }

    public NativeSegments RentSegments(int count)
    {
        var segments = new NativeSegments(count);
        _memory.Add(segments);
        return segments;
    }

    public void Wait(Func<bool> condition, string what)
    {
        if (!Harness.Pump(condition, Harness.DefaultTimeout)) throw Failure("timed out waiting for " + what + ".");
    }

    /// <summary>Lets the transports run for <paramref name="duration"/> (to observe that nothing else happens).</summary>
    public void Settle(TimeSpan duration) => Harness.Pump(static () => false, duration);

    public void WaitConnected() => Wait(() => ClientSink.CountOf(RecordedEventKind.Connected) > 0 && ServerSink.CountOf(RecordedEventKind.Connected) > 0, "OnConnected on both ends");

    public void WaitDatagrams() => Wait(() => Client.Capabilities.Datagrams && Server.Capabilities.Datagrams, "datagram support on both ends");

    public bool BothClosed => ClientSink.IsClosed && ServerSink.IsClosed;

    public void CloseAndWait(ulong errorCode = 0)
    {
        Client.Close(errorCode, default);
        Wait(() => BothClosed, "OnClosed on both ends");
    }

    public void Require(bool condition, string message)
    {
        if (!condition) throw Failure(message);
    }

    public ConformanceException Failure(string message) => new($"[{Harness.Name}] {message}{Environment.NewLine}{Dump()}");

    public string Dump() => "client events:" + Environment.NewLine + Describe(ClientSink) + "server events:" + Environment.NewLine + Describe(ServerSink);

    private static string Describe(RecordingSink sink)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        var text = new StringBuilder();
        int start = Math.Max(0, events.Count - 60);
        if (start > 0) text.Append("  ... ").Append(start).AppendLine(" earlier events");
        for (int i = start; i < events.Count; i++) text.Append("  ").AppendLine(events[i].ToString());
        return text.ToString();
    }

    /// <summary>
    /// Closes both ends if the scenario did not (and waits for it), then frees the scenario memory, but only once both ends
    /// reported OnClosed: until then a transport may still read it.
    /// </summary>
    public void Dispose()
    {
        bool closed = BothClosed;
        if (!closed)
        {
            try
            {
                Client.Close(0, default);
                Server.Close(0, default);
                closed = Harness.Pump(() => BothClosed, Harness.DefaultTimeout);
            }
            catch (Exception)
            {
                // The scenario already failed; leaking the memory is the safe outcome.
            }
        }
        if (!closed) return;
        foreach (IDisposable memory in _memory) memory.Dispose();
    }

    /// <summary>True when two end points denote the same address and port (IPv4-mapped IPv6 normalised; an unspecified address matches any).</summary>
    public static bool SameEndPoint(IPEndPoint? a, IPEndPoint? b)
    {
        if (a is null || b is null || a.Port != b.Port) return false;
        IPAddress x = a.Address.IsIPv4MappedToIPv6 ? a.Address.MapToIPv4() : a.Address;
        IPAddress y = b.Address.IsIPv4MappedToIPv6 ? b.Address.MapToIPv4() : b.Address;
        if (x.Equals(IPAddress.Any) || x.Equals(IPAddress.IPv6Any) || y.Equals(IPAddress.Any) || y.Equals(IPAddress.IPv6Any)) return true;
        return x.Equals(y);
    }
}
