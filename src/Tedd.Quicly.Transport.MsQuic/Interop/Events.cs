using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>Event delivered to a listener callback.</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_LISTENER_EVENT
{
    public QUIC_LISTENER_EVENT_TYPE Type;
    public _Anonymous_e__Union Anonymous;

    [StructLayout(LayoutKind.Explicit)]
    public struct _Anonymous_e__Union
    {
        [FieldOffset(0)] public _NEW_CONNECTION_e__Struct NEW_CONNECTION;
        [FieldOffset(0)] public _STOP_COMPLETE_e__Struct STOP_COMPLETE;
        [FieldOffset(0)] public _DOS_MODE_CHANGED_e__Struct DOS_MODE_CHANGED;

        [StructLayout(LayoutKind.Sequential)]
        public struct _NEW_CONNECTION_e__Struct
        {
            public QUIC_NEW_CONNECTION_INFO* Info;
            public QUIC_HANDLE* Connection;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _STOP_COMPLETE_e__Struct
        {
            public byte _bitfield;
            public readonly bool AppCloseInProgress => (_bitfield & 0x01) != 0;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _DOS_MODE_CHANGED_e__Struct
        {
            public byte _bitfield;
            public readonly bool DosModeEnabled => (_bitfield & 0x01) != 0;
        }
    }

    [UnscopedRef] public ref _Anonymous_e__Union._NEW_CONNECTION_e__Struct NEW_CONNECTION => ref Anonymous.NEW_CONNECTION;
    [UnscopedRef] public ref _Anonymous_e__Union._STOP_COMPLETE_e__Struct STOP_COMPLETE => ref Anonymous.STOP_COMPLETE;
    [UnscopedRef] public ref _Anonymous_e__Union._DOS_MODE_CHANGED_e__Struct DOS_MODE_CHANGED => ref Anonymous.DOS_MODE_CHANGED;
}

/// <summary>Event delivered to a connection callback.</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_CONNECTION_EVENT
{
    public QUIC_CONNECTION_EVENT_TYPE Type;
    public _Anonymous_e__Union Anonymous;

    [StructLayout(LayoutKind.Explicit)]
    public struct _Anonymous_e__Union
    {
        [FieldOffset(0)] public _CONNECTED_e__Struct CONNECTED;
        [FieldOffset(0)] public _SHUTDOWN_INITIATED_BY_TRANSPORT_e__Struct SHUTDOWN_INITIATED_BY_TRANSPORT;
        [FieldOffset(0)] public _SHUTDOWN_INITIATED_BY_PEER_e__Struct SHUTDOWN_INITIATED_BY_PEER;
        [FieldOffset(0)] public _SHUTDOWN_COMPLETE_e__Struct SHUTDOWN_COMPLETE;
        [FieldOffset(0)] public _LOCAL_ADDRESS_CHANGED_e__Struct LOCAL_ADDRESS_CHANGED;
        [FieldOffset(0)] public _PEER_ADDRESS_CHANGED_e__Struct PEER_ADDRESS_CHANGED;
        [FieldOffset(0)] public _PEER_STREAM_STARTED_e__Struct PEER_STREAM_STARTED;
        [FieldOffset(0)] public _STREAMS_AVAILABLE_e__Struct STREAMS_AVAILABLE;
        [FieldOffset(0)] public _PEER_NEEDS_STREAMS_e__Struct PEER_NEEDS_STREAMS;
        [FieldOffset(0)] public _IDEAL_PROCESSOR_CHANGED_e__Struct IDEAL_PROCESSOR_CHANGED;
        [FieldOffset(0)] public _DATAGRAM_STATE_CHANGED_e__Struct DATAGRAM_STATE_CHANGED;
        [FieldOffset(0)] public _DATAGRAM_RECEIVED_e__Struct DATAGRAM_RECEIVED;
        [FieldOffset(0)] public _DATAGRAM_SEND_STATE_CHANGED_e__Struct DATAGRAM_SEND_STATE_CHANGED;
        [FieldOffset(0)] public _RESUMED_e__Struct RESUMED;
        [FieldOffset(0)] public _RESUMPTION_TICKET_RECEIVED_e__Struct RESUMPTION_TICKET_RECEIVED;
        [FieldOffset(0)] public _PEER_CERTIFICATE_RECEIVED_e__Struct PEER_CERTIFICATE_RECEIVED;
        [FieldOffset(0)] public _RELIABLE_RESET_NEGOTIATED_e__Struct RELIABLE_RESET_NEGOTIATED;
        [FieldOffset(0)] public _ONE_WAY_DELAY_NEGOTIATED_e__Struct ONE_WAY_DELAY_NEGOTIATED;

        [StructLayout(LayoutKind.Sequential)]
        public struct _CONNECTED_e__Struct
        {
            public byte SessionResumed;
            public byte NegotiatedAlpnLength;
            public byte* NegotiatedAlpn;
            public readonly ReadOnlySpan<byte> NegotiatedAlpnSpan => new(NegotiatedAlpn, NegotiatedAlpnLength);
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _SHUTDOWN_INITIATED_BY_TRANSPORT_e__Struct
        {
            public int Status;
            public ulong ErrorCode;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _SHUTDOWN_INITIATED_BY_PEER_e__Struct
        {
            public ulong ErrorCode;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _SHUTDOWN_COMPLETE_e__Struct
        {
            public byte _bitfield;
            public readonly bool HandshakeCompleted => (_bitfield & 0x01) != 0;
            public readonly bool PeerAcknowledgedShutdown => (_bitfield & 0x02) != 0;
            public readonly bool AppCloseInProgress => (_bitfield & 0x04) != 0;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _LOCAL_ADDRESS_CHANGED_e__Struct
        {
            public QUIC_ADDR* Address;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _PEER_ADDRESS_CHANGED_e__Struct
        {
            public QUIC_ADDR* Address;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _PEER_STREAM_STARTED_e__Struct
        {
            public QUIC_HANDLE* Stream;
            public QUIC_STREAM_OPEN_FLAGS Flags;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _STREAMS_AVAILABLE_e__Struct
        {
            public ushort BidirectionalCount;
            public ushort UnidirectionalCount;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _PEER_NEEDS_STREAMS_e__Struct
        {
            public byte Bidirectional;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _IDEAL_PROCESSOR_CHANGED_e__Struct
        {
            public ushort IdealProcessor;
            public ushort PartitionIndex;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _DATAGRAM_STATE_CHANGED_e__Struct
        {
            public byte SendEnabled;
            public ushort MaxSendLength;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _DATAGRAM_RECEIVED_e__Struct
        {
            public QUIC_BUFFER* Buffer;
            public QUIC_RECEIVE_FLAGS Flags;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _DATAGRAM_SEND_STATE_CHANGED_e__Struct
        {
            public void* ClientContext;
            public QUIC_DATAGRAM_SEND_STATE State;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _RESUMED_e__Struct
        {
            public ushort ResumptionStateLength;
            public byte* ResumptionState;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _RESUMPTION_TICKET_RECEIVED_e__Struct
        {
            public uint ResumptionTicketLength;
            public byte* ResumptionTicket;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _PEER_CERTIFICATE_RECEIVED_e__Struct
        {
            /// <summary>Platform certificate, or a <see cref="QUIC_BUFFER"/>* of DER bytes with <see cref="QUIC_CREDENTIAL_FLAGS.USE_PORTABLE_CERTIFICATES"/>.</summary>
            public void* Certificate;
            public uint DeferredErrorFlags;
            public int DeferredStatus;
            /// <summary>Platform chain, or a <see cref="QUIC_BUFFER"/>* of PKCS#7 bytes with portable certificates.</summary>
            public void* Chain;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _RELIABLE_RESET_NEGOTIATED_e__Struct
        {
            public byte IsNegotiated;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _ONE_WAY_DELAY_NEGOTIATED_e__Struct
        {
            public byte SendNegotiated;
            public byte ReceiveNegotiated;
        }
    }

    [UnscopedRef] public ref _Anonymous_e__Union._CONNECTED_e__Struct CONNECTED => ref Anonymous.CONNECTED;
    [UnscopedRef] public ref _Anonymous_e__Union._SHUTDOWN_INITIATED_BY_TRANSPORT_e__Struct SHUTDOWN_INITIATED_BY_TRANSPORT => ref Anonymous.SHUTDOWN_INITIATED_BY_TRANSPORT;
    [UnscopedRef] public ref _Anonymous_e__Union._SHUTDOWN_INITIATED_BY_PEER_e__Struct SHUTDOWN_INITIATED_BY_PEER => ref Anonymous.SHUTDOWN_INITIATED_BY_PEER;
    [UnscopedRef] public ref _Anonymous_e__Union._SHUTDOWN_COMPLETE_e__Struct SHUTDOWN_COMPLETE => ref Anonymous.SHUTDOWN_COMPLETE;
    [UnscopedRef] public ref _Anonymous_e__Union._LOCAL_ADDRESS_CHANGED_e__Struct LOCAL_ADDRESS_CHANGED => ref Anonymous.LOCAL_ADDRESS_CHANGED;
    [UnscopedRef] public ref _Anonymous_e__Union._PEER_ADDRESS_CHANGED_e__Struct PEER_ADDRESS_CHANGED => ref Anonymous.PEER_ADDRESS_CHANGED;
    [UnscopedRef] public ref _Anonymous_e__Union._PEER_STREAM_STARTED_e__Struct PEER_STREAM_STARTED => ref Anonymous.PEER_STREAM_STARTED;
    [UnscopedRef] public ref _Anonymous_e__Union._STREAMS_AVAILABLE_e__Struct STREAMS_AVAILABLE => ref Anonymous.STREAMS_AVAILABLE;
    [UnscopedRef] public ref _Anonymous_e__Union._PEER_NEEDS_STREAMS_e__Struct PEER_NEEDS_STREAMS => ref Anonymous.PEER_NEEDS_STREAMS;
    [UnscopedRef] public ref _Anonymous_e__Union._IDEAL_PROCESSOR_CHANGED_e__Struct IDEAL_PROCESSOR_CHANGED => ref Anonymous.IDEAL_PROCESSOR_CHANGED;
    [UnscopedRef] public ref _Anonymous_e__Union._DATAGRAM_STATE_CHANGED_e__Struct DATAGRAM_STATE_CHANGED => ref Anonymous.DATAGRAM_STATE_CHANGED;
    [UnscopedRef] public ref _Anonymous_e__Union._DATAGRAM_RECEIVED_e__Struct DATAGRAM_RECEIVED => ref Anonymous.DATAGRAM_RECEIVED;
    [UnscopedRef] public ref _Anonymous_e__Union._DATAGRAM_SEND_STATE_CHANGED_e__Struct DATAGRAM_SEND_STATE_CHANGED => ref Anonymous.DATAGRAM_SEND_STATE_CHANGED;
    [UnscopedRef] public ref _Anonymous_e__Union._RESUMED_e__Struct RESUMED => ref Anonymous.RESUMED;
    [UnscopedRef] public ref _Anonymous_e__Union._RESUMPTION_TICKET_RECEIVED_e__Struct RESUMPTION_TICKET_RECEIVED => ref Anonymous.RESUMPTION_TICKET_RECEIVED;
    [UnscopedRef] public ref _Anonymous_e__Union._PEER_CERTIFICATE_RECEIVED_e__Struct PEER_CERTIFICATE_RECEIVED => ref Anonymous.PEER_CERTIFICATE_RECEIVED;
    [UnscopedRef] public ref _Anonymous_e__Union._RELIABLE_RESET_NEGOTIATED_e__Struct RELIABLE_RESET_NEGOTIATED => ref Anonymous.RELIABLE_RESET_NEGOTIATED;
    [UnscopedRef] public ref _Anonymous_e__Union._ONE_WAY_DELAY_NEGOTIATED_e__Struct ONE_WAY_DELAY_NEGOTIATED => ref Anonymous.ONE_WAY_DELAY_NEGOTIATED;
}

/// <summary>Event delivered to a stream callback.</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_STREAM_EVENT
{
    public QUIC_STREAM_EVENT_TYPE Type;
    public _Anonymous_e__Union Anonymous;

    [StructLayout(LayoutKind.Explicit)]
    public struct _Anonymous_e__Union
    {
        [FieldOffset(0)] public _START_COMPLETE_e__Struct START_COMPLETE;
        [FieldOffset(0)] public _RECEIVE_e__Struct RECEIVE;
        [FieldOffset(0)] public _SEND_COMPLETE_e__Struct SEND_COMPLETE;
        [FieldOffset(0)] public _PEER_SEND_ABORTED_e__Struct PEER_SEND_ABORTED;
        [FieldOffset(0)] public _PEER_RECEIVE_ABORTED_e__Struct PEER_RECEIVE_ABORTED;
        [FieldOffset(0)] public _SEND_SHUTDOWN_COMPLETE_e__Struct SEND_SHUTDOWN_COMPLETE;
        [FieldOffset(0)] public _SHUTDOWN_COMPLETE_e__Struct SHUTDOWN_COMPLETE;
        [FieldOffset(0)] public _IDEAL_SEND_BUFFER_SIZE_e__Struct IDEAL_SEND_BUFFER_SIZE;
        [FieldOffset(0)] public _CANCEL_ON_LOSS_e__Struct CANCEL_ON_LOSS;

        [StructLayout(LayoutKind.Sequential)]
        public struct _START_COMPLETE_e__Struct
        {
            public int Status;
            public ulong ID;
            public byte _bitfield;
            public readonly bool PeerAccepted => (_bitfield & 0x01) != 0;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _RECEIVE_e__Struct
        {
            public ulong AbsoluteOffset;
            /// <summary>In: bytes indicated. Out: bytes consumed when the callback returns success.</summary>
            public ulong TotalBufferLength;
            public QUIC_BUFFER* Buffers;
            public uint BufferCount;
            public QUIC_RECEIVE_FLAGS Flags;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _SEND_COMPLETE_e__Struct
        {
            public byte Canceled;
            public void* ClientContext;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _PEER_SEND_ABORTED_e__Struct
        {
            public ulong ErrorCode;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _PEER_RECEIVE_ABORTED_e__Struct
        {
            public ulong ErrorCode;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _SEND_SHUTDOWN_COMPLETE_e__Struct
        {
            public byte Graceful;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _SHUTDOWN_COMPLETE_e__Struct
        {
            public byte ConnectionShutdown;
            public byte _bitfield;
            public ulong ConnectionErrorCode;
            public int ConnectionCloseStatus;
            public readonly bool AppCloseInProgress => (_bitfield & 0x01) != 0;
            public readonly bool ConnectionShutdownByApp => (_bitfield & 0x02) != 0;
            public readonly bool ConnectionClosedRemotely => (_bitfield & 0x04) != 0;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _IDEAL_SEND_BUFFER_SIZE_e__Struct
        {
            public ulong ByteCount;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct _CANCEL_ON_LOSS_e__Struct
        {
            public ulong ErrorCode;
        }
    }

    [UnscopedRef] public ref _Anonymous_e__Union._START_COMPLETE_e__Struct START_COMPLETE => ref Anonymous.START_COMPLETE;
    [UnscopedRef] public ref _Anonymous_e__Union._RECEIVE_e__Struct RECEIVE => ref Anonymous.RECEIVE;
    [UnscopedRef] public ref _Anonymous_e__Union._SEND_COMPLETE_e__Struct SEND_COMPLETE => ref Anonymous.SEND_COMPLETE;
    [UnscopedRef] public ref _Anonymous_e__Union._PEER_SEND_ABORTED_e__Struct PEER_SEND_ABORTED => ref Anonymous.PEER_SEND_ABORTED;
    [UnscopedRef] public ref _Anonymous_e__Union._PEER_RECEIVE_ABORTED_e__Struct PEER_RECEIVE_ABORTED => ref Anonymous.PEER_RECEIVE_ABORTED;
    [UnscopedRef] public ref _Anonymous_e__Union._SEND_SHUTDOWN_COMPLETE_e__Struct SEND_SHUTDOWN_COMPLETE => ref Anonymous.SEND_SHUTDOWN_COMPLETE;
    [UnscopedRef] public ref _Anonymous_e__Union._SHUTDOWN_COMPLETE_e__Struct SHUTDOWN_COMPLETE => ref Anonymous.SHUTDOWN_COMPLETE;
    [UnscopedRef] public ref _Anonymous_e__Union._IDEAL_SEND_BUFFER_SIZE_e__Struct IDEAL_SEND_BUFFER_SIZE => ref Anonymous.IDEAL_SEND_BUFFER_SIZE;
    [UnscopedRef] public ref _Anonymous_e__Union._CANCEL_ON_LOSS_e__Struct CANCEL_ON_LOSS => ref Anonymous.CANCEL_ON_LOSS;
}
