using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>An <see cref="ITransportSink"/> whose callbacks do nothing (receives consume everything); tests override what they need.</summary>
internal class NullTransportSink : ITransportSink
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
        for (int i = 0; i < segments.Length; i++) total += (int)segments[i].Length;
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
/// Counts callbacks and samples the allocations of each callback thread between consecutive callbacks
/// (<see cref="CallbackAllocationProbe"/>); never allocates itself, so steady-state allocation can be measured through it.
/// </summary>
internal sealed class CountingTransportSink : NullTransportSink
{
    public readonly CallbackAllocationProbe Probe = new();
    public int Connected;
    public int Closed;
    public long DatagramsReceived;
    public long FinalStates;
    public long SendCompletions;
    public long CanceledCompletions;
    public long StreamBytes;
    public volatile bool DatagramsEnabled;
    public ITransport? Transport;

    public override void OnConnected(in TransportConnectedInfo info) => Interlocked.Increment(ref Connected);

    public override void OnDatagramCapabilityChanged(bool enabled, int maxPayload)
    {
        if (enabled) DatagramsEnabled = true;
    }

    public override void OnDatagramReceived(ReadOnlySpan<byte> payload)
    {
        Probe.Sample();
        Interlocked.Increment(ref DatagramsReceived);
    }

    public override void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
    {
        Probe.Sample();
        if (state.IsFinal()) Interlocked.Increment(ref FinalStates);
    }

    public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
    {
        Probe.Sample();
        int total = 0;
        for (int i = 0; i < segments.Length; i++) total += (int)segments[i].Length;
        Interlocked.Add(ref StreamBytes, total);
        return ReceiveResult.Consumed(total);
    }

    public override void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled)
    {
        Probe.Sample();
        Interlocked.Increment(ref SendCompletions);
        if (canceled) Interlocked.Increment(ref CanceledCompletions);
    }

    public override void OnStreamShutdownComplete(TransportStreamId id) => Transport?.CloseStream(id);

    public override void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus) => Interlocked.Increment(ref Closed);
}

internal static class Spin
{
    /// <summary>Waits without allocating (no delegates, no tasks) until <paramref name="value"/> reaches <paramref name="target"/>.</summary>
    public static bool UntilAtLeast(ref long value, long target, TimeSpan timeout)
    {
        long deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
        while (Volatile.Read(ref value) < target)
        {
            if (Environment.TickCount64 >= deadline) return false;
            Thread.Sleep(1);
        }
        return true;
    }

    /// <summary>Waits (allocation-free) until <paramref name="value"/> reaches <paramref name="target"/>.</summary>
    public static bool UntilAtLeast(ref int value, int target, TimeSpan timeout)
    {
        long deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
        while (Volatile.Read(ref value) < target)
        {
            if (Environment.TickCount64 >= deadline) return false;
            Thread.Sleep(1);
        }
        return true;
    }

    /// <summary>Waits until <paramref name="condition"/> holds (allocates the delegate once; not for allocation measurements).</summary>
    public static bool Until(Func<bool> condition, TimeSpan timeout)
    {
        long deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
        while (!condition())
        {
            if (Environment.TickCount64 >= deadline) return false;
            Thread.Sleep(1);
        }
        return true;
    }
}
