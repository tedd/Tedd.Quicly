using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

public sealed unsafe partial class MsQuicTransport
{
    /// <summary><see cref="TransportSendFlags"/> (6 bits) → MsQuic flags for a datagram send, before the library-version mask.</summary>
    private static readonly QUIC_SEND_FLAGS[] s_datagramFlagMap = BuildFlagMap(datagram: true);

    /// <summary><see cref="TransportSendFlags"/> (6 bits) → MsQuic flags for a stream send (<c>Start</c> is handled separately).</summary>
    private static readonly QUIC_SEND_FLAGS[] s_streamFlagMap = BuildFlagMap(datagram: false);

    /// <inheritdoc/>
    /// <remarks>
    /// Passes <paramref name="segments"/> to <c>DatagramSend</c> as <c>QUIC_BUFFER*</c> (no copy; the layout is identical)
    /// and <paramref name="context"/> as the client context, which MsQuic reports with every
    /// <see cref="ITransportSink.OnDatagramSendStateChanged"/> until a final state. <see cref="TransportSendFlags.Priority"/> →
    /// <c>DGRAM_PRIORITY</c>, <see cref="TransportSendFlags.DelaySend"/> → <c>DELAY_SEND</c>,
    /// <see cref="TransportSendFlags.CancelOnBlocked"/> → <c>CANCEL_ON_BLOCKED</c> when the library supports it (else
    /// ignored; see <see cref="TransportCapabilities.CancelOnBlocked"/>); other flags are ignored.
    /// <see cref="TransportStatus.InvalidState"/> unless connected with datagrams enabled;
    /// <see cref="TransportStatus.TooLarge"/> when the gathered payload exceeds <see cref="TransportCapabilities.MaxDatagramPayload"/>
    /// (checked before the call, and MsQuic's <c>INVALID_PARAMETER</c> after a path MTU drop maps to it too).
    /// </remarks>
    public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count > 0 && segments == null) throw new ArgumentNullException(nameof(segments));
        int datagrams = _datagramState;
        if (Volatile.Read(ref _state) != StateConnected || (datagrams & DatagramEnabledBit) == 0) return TransportStatus.InvalidState;
        long total = 0;
        for (int i = 0; i < count; i++) total += segments[i].Length;
        if (total > (datagrams & 0xFFFF)) return TransportStatus.TooLarge;
        QUIC_SEND_FLAGS sendFlags = s_datagramFlagMap[(int)flags & 63] & _supportedSendFlags;
        int status = _connection.SendDatagram((QUIC_BUFFER*)segments, (uint)count, sendFlags, (void*)context);
        return MsQuicStatus.Succeeded(status) ? TransportStatus.Success : MapStatus(status, datagramSend: true);
    }

    /// <summary>The MsQuic send flags a datagram send with <paramref name="flags"/> uses (before the library-version mask).</summary>
    internal static QUIC_SEND_FLAGS MapDatagramFlags(TransportSendFlags flags) => s_datagramFlagMap[(int)flags & 63];

    /// <summary>The MsQuic send flags a stream send with <paramref name="flags"/> uses (before the library-version mask; <c>Start</c> excluded).</summary>
    internal static QUIC_SEND_FLAGS MapStreamFlags(TransportSendFlags flags) => s_streamFlagMap[(int)flags & 63];

    /// <summary>
    /// Maps a <c>QUIC_STATUS</c> returned by an MsQuic call to a <see cref="TransportStatus"/>: accepted (SUCCESS or
    /// PENDING) → Success, INVALID_STATE → InvalidState, INVALID_PARAMETER → TooLarge for a datagram send (else Failed),
    /// OUT_OF_MEMORY → OutOfMemory, STREAM_LIMIT_REACHED → StreamLimitReached, NOT_SUPPORTED → NotSupported, else Failed.
    /// </summary>
    internal static TransportStatus MapStatus(int status, bool datagramSend)
    {
        if (MsQuicStatus.Succeeded(status)) return TransportStatus.Success;
        if (status == MsQuicStatus.QUIC_STATUS_INVALID_STATE) return TransportStatus.InvalidState;
        if (status == MsQuicStatus.QUIC_STATUS_INVALID_PARAMETER) return datagramSend ? TransportStatus.TooLarge : TransportStatus.Failed;
        if (status == MsQuicStatus.QUIC_STATUS_OUT_OF_MEMORY) return TransportStatus.OutOfMemory;
        if (status == MsQuicStatus.QUIC_STATUS_STREAM_LIMIT_REACHED) return TransportStatus.StreamLimitReached;
        if (status == MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED) return TransportStatus.NotSupported;
        return TransportStatus.Failed;
    }

    /// <summary>MsQuic's datagram send state → <see cref="DatagramSendState"/> (same values); unknown future states → Unknown.</summary>
    internal static DatagramSendState MapDatagramSendState(QUIC_DATAGRAM_SEND_STATE state)
        => (uint)state <= (uint)QUIC_DATAGRAM_SEND_STATE.CANCELED ? (DatagramSendState)(byte)state : DatagramSendState.Unknown;

    private static QUIC_SEND_FLAGS[] BuildFlagMap(bool datagram)
    {
        var map = new QUIC_SEND_FLAGS[64];
        for (int i = 0; i < map.Length; i++)
        {
            var flags = (TransportSendFlags)i;
            QUIC_SEND_FLAGS mapped = QUIC_SEND_FLAGS.NONE;
            if ((flags & TransportSendFlags.DelaySend) != 0) mapped |= QUIC_SEND_FLAGS.DELAY_SEND;
            if (datagram)
            {
                if ((flags & TransportSendFlags.Priority) != 0) mapped |= QUIC_SEND_FLAGS.DGRAM_PRIORITY;
                if ((flags & TransportSendFlags.CancelOnBlocked) != 0) mapped |= QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED;
            }
            else
            {
                if ((flags & TransportSendFlags.Fin) != 0) mapped |= QUIC_SEND_FLAGS.FIN;
                if ((flags & TransportSendFlags.Priority) != 0) mapped |= QUIC_SEND_FLAGS.PRIORITY_WORK;
                if ((flags & TransportSendFlags.CancelOnLoss) != 0) mapped |= QUIC_SEND_FLAGS.CANCEL_ON_LOSS;
            }
            map[i] = mapped;
        }
        return map;
    }

    // ------------------------------------------------------------------ datagram events (MsQuic worker thread)

    void IMsQuicConnectionEvents.DatagramStateChanged(MsQuicConnection connection, bool sendEnabled, ushort maxSendLength)
    {
        _datagramState = (sendEnabled ? DatagramEnabledBit : 0) | maxSendLength;
        try
        {
            LiveSink?.OnDatagramCapabilityChanged(sendEnabled, sendEnabled ? maxSendLength : 0);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }

    void IMsQuicConnectionEvents.DatagramReceived(MsQuicConnection connection, ReadOnlySpan<byte> data, QUIC_RECEIVE_FLAGS flags)
    {
        try
        {
            LiveSink?.OnDatagramReceived(data);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }

    void IMsQuicConnectionEvents.DatagramSendStateChanged(MsQuicConnection connection, void* clientContext, QUIC_DATAGRAM_SEND_STATE state)
    {
        DatagramSendState mapped = MapDatagramSendState(state);
        if (mapped == DatagramSendState.Unknown) return;
        try
        {
            LiveSink?.OnDatagramSendStateChanged((ulong)clientContext, mapped);
        }
        catch (Exception ex)
        {
            OnHandlerException(ex);
        }
    }
}
