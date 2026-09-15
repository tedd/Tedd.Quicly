using System.Runtime.InteropServices;

namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>
/// The MsQuic v2 function table returned by <c>MsQuicOpenVersion(2, ...)</c>. Field order and signatures are
/// exactly those of msquic.h up to <c>ConnectionCertificateValidationComplete</c> (MsQuic 2.2); later entries
/// (partition-aware open, app-owned receive buffers, connection pools, execution contexts) are not bound.
/// Every call is an indirect <c>cdecl</c> call with no marshalling (ADR 0002).
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_API_TABLE
{
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*, void> SetContext;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*> GetContext;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*, void*, void> SetCallbackHandler;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, uint, uint, void*, int> SetParam;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, uint, uint*, void*, int> GetParam;
    public delegate* unmanaged[Cdecl]<QUIC_REGISTRATION_CONFIG*, QUIC_HANDLE**, int> RegistrationOpen;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void> RegistrationClose;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_CONNECTION_SHUTDOWN_FLAGS, ulong, void> RegistrationShutdown;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_BUFFER*, uint, QUIC_SETTINGS*, uint, void*, QUIC_HANDLE**, int> ConfigurationOpen;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void> ConfigurationClose;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_CREDENTIAL_CONFIG*, int> ConfigurationLoadCredential;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*, QUIC_LISTENER_EVENT*, int>, void*, QUIC_HANDLE**, int> ListenerOpen;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void> ListenerClose;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_BUFFER*, uint, QuicAddr*, int> ListenerStart;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void> ListenerStop;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*, QUIC_CONNECTION_EVENT*, int>, void*, QUIC_HANDLE**, int> ConnectionOpen;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void> ConnectionClose;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_CONNECTION_SHUTDOWN_FLAGS, ulong, void> ConnectionShutdown;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_HANDLE*, ushort, sbyte*, ushort, int> ConnectionStart;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_HANDLE*, int> ConnectionSetConfiguration;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_SEND_RESUMPTION_FLAGS, ushort, byte*, int> ConnectionSendResumptionTicket;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_STREAM_OPEN_FLAGS, delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*, QUIC_STREAM_EVENT*, int>, void*, QUIC_HANDLE**, int> StreamOpen;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void> StreamClose;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_STREAM_START_FLAGS, int> StreamStart;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_STREAM_SHUTDOWN_FLAGS, ulong, int> StreamShutdown;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_BUFFER*, uint, QUIC_SEND_FLAGS, void*, int> StreamSend;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, ulong, void> StreamReceiveComplete;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, byte, int> StreamReceiveSetEnabled;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_BUFFER*, uint, QUIC_SEND_FLAGS, void*, int> DatagramSend;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, byte, int> ConnectionResumptionTicketValidationComplete;
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, byte, QUIC_TLS_ALERT_CODES, int> ConnectionCertificateValidationComplete;
}
