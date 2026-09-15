using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>Result of <see cref="IMsQuicStreamEvents.Receive"/>.</summary>
public readonly struct MsQuicReceiveResult
{
    /// <summary>Bytes consumed (ignored when <see cref="IsPending"/>).</summary>
    public readonly ulong BytesConsumed;

    /// <summary>
    /// True to return <c>QUIC_STATUS_PENDING</c>: MsQuic keeps the buffers alive and delivers no further RECEIVE
    /// events until <see cref="MsQuicStream.ReceiveComplete"/> is called.
    /// </summary>
    public readonly bool IsPending;

    private MsQuicReceiveResult(ulong bytesConsumed, bool pending)
    {
        BytesConsumed = bytesConsumed;
        IsPending = pending;
    }

    /// <summary>The handler consumed <paramref name="bytes"/> bytes synchronously.</summary>
    public static MsQuicReceiveResult Consumed(ulong bytes) => new(bytes, false);

    /// <summary>The handler will call <see cref="MsQuicStream.ReceiveComplete"/> later (backpressure).</summary>
    public static MsQuicReceiveResult Pending => new(0, true);
}

/// <summary>Details of a stream's SHUTDOWN_COMPLETE event.</summary>
public readonly struct MsQuicStreamShutdownInfo
{
    /// <summary>True when the stream closed because the whole connection did.</summary>
    public readonly bool ConnectionShutdown;
    public readonly bool AppCloseInProgress;
    public readonly bool ConnectionShutdownByApp;
    public readonly bool ConnectionClosedRemotely;
    public readonly ulong ConnectionErrorCode;
    public readonly int ConnectionCloseStatus;

    internal MsQuicStreamShutdownInfo(in QUIC_STREAM_EVENT._Anonymous_e__Union._SHUTDOWN_COMPLETE_e__Struct e)
    {
        ConnectionShutdown = e.ConnectionShutdown != 0;
        AppCloseInProgress = e.AppCloseInProgress;
        ConnectionShutdownByApp = e.ConnectionShutdownByApp;
        ConnectionClosedRemotely = e.ConnectionClosedRemotely;
        ConnectionErrorCode = e.ConnectionErrorCode;
        ConnectionCloseStatus = e.ConnectionCloseStatus;
    }
}

/// <summary>
/// Stream event sink. Every method has a default so implementations override only what they need.
/// </summary>
/// <remarks>
/// Threading: invoked on MsQuic worker threads, serialised with all other callbacks of the owning connection.
/// Buffers passed to <see cref="Receive"/> are valid until consumed (synchronously, or via
/// <see cref="MsQuicStream.ReceiveComplete"/> after returning <see cref="MsQuicReceiveResult.Pending"/>).
/// </remarks>
public unsafe interface IMsQuicStreamEvents
{
    /// <summary>Result of <see cref="MsQuicStream.Start"/>; <paramref name="id"/> is the QUIC stream id.</summary>
    void StartComplete(MsQuicStream stream, int status, ulong id, bool peerAccepted)
    {
    }

    /// <summary>
    /// Data arrived in <paramref name="bufferCount"/> gathered buffers totalling <paramref name="totalLength"/>
    /// bytes starting at stream offset <paramref name="absoluteOffset"/>. <paramref name="flags"/> carries FIN.
    /// Return how many bytes were consumed, or <see cref="MsQuicReceiveResult.Pending"/> to hold the data.
    /// Consuming fewer bytes than indicated pauses further RECEIVE events until <see cref="MsQuicStream.ReceiveSetEnabled"/>
    /// re-enables them (MsQuic semantics). Default: consume everything.
    /// </summary>
    MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
        => MsQuicReceiveResult.Consumed(totalLength);

    /// <summary>A <see cref="MsQuicStream.Send"/> finished; its buffers may be reused. <paramref name="canceled"/> means the data was not sent.</summary>
    void SendComplete(MsQuicStream stream, void* clientContext, bool canceled)
    {
    }

    /// <summary>The peer sent FIN; no more data will arrive.</summary>
    void PeerSendShutdown(MsQuicStream stream)
    {
    }

    /// <summary>The peer aborted its send direction (RESET_STREAM) with an application error code.</summary>
    void PeerSendAborted(MsQuicStream stream, ulong errorCode)
    {
    }

    /// <summary>The peer aborted its receive direction (STOP_SENDING); our sends will be cancelled.</summary>
    void PeerReceiveAborted(MsQuicStream stream, ulong errorCode)
    {
    }

    /// <summary>Our send direction is fully shut down (FIN acknowledged when <paramref name="graceful"/>).</summary>
    void SendShutdownComplete(MsQuicStream stream, bool graceful)
    {
    }

    /// <summary>Last event; after it the handle may be closed (calling <see cref="MsQuicStream.Close"/> inline is allowed).</summary>
    void ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info)
    {
    }

    /// <summary>MsQuic's suggestion for how many bytes to keep queued for sending.</summary>
    void IdealSendBufferSize(MsQuicStream stream, ulong byteCount)
    {
    }

    /// <summary>The peer accepted a stream started with <see cref="QUIC_STREAM_START_FLAGS.INDICATE_PEER_ACCEPT"/>.</summary>
    void PeerAccepted(MsQuicStream stream)
    {
    }
}

/// <summary>Sink that ignores every event and consumes all received data; used when no handler is attached.</summary>
internal sealed class NoOpStreamEvents : IMsQuicStreamEvents
{
    public static readonly NoOpStreamEvents Instance = new();
}
