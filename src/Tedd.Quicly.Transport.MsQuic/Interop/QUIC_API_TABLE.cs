using System.Runtime.InteropServices;

namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>
/// The MsQuic v2 function table returned by <c>MsQuicOpenVersion(2, ...)</c>. Exactly the 31 entries (248 bytes)
/// of msquic.h up to <c>ConnectionCertificateValidationComplete</c> (MsQuic 2.2), which is also what the
/// runtime's own bindings declare. Entries added in later versions are never appended here; they live in
/// <see cref="QUIC_API_TABLE_EXT_2_5"/> / <see cref="QUIC_API_TABLE_PREVIEW_2_5"/> and are reachable only through
/// the version-gated accessors on <c>MsQuicApi</c>. Every call is an indirect <c>cdecl</c> call with no marshalling (ADR 0002).
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
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, QUIC_BUFFER*, uint, QUIC_ADDR*, int> ListenerStart;
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

/// <summary>
/// The non-preview entry MsQuic 2.5 appended to the API table, located immediately after
/// <see cref="QUIC_API_TABLE"/> (offset 248). Only valid when the loaded library reports version 2.5 or later;
/// obtain it through <c>MsQuicApi.TryGetExtension25</c>, never by widening <see cref="QUIC_API_TABLE"/>.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_API_TABLE_EXT_2_5
{
    /// <summary><c>ConnectionOpenInPartition(Registration, PartitionIndex, Handler, Context, out Connection)</c>. Available from v2.5.</summary>
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, ushort, delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*, QUIC_CONNECTION_EVENT*, int>, void*, QUIC_HANDLE**, int> ConnectionOpenInPartition;
}

/// <summary>
/// The preview-feature entries MsQuic 2.5 appends after <see cref="QUIC_API_TABLE_EXT_2_5"/> (offset 256). They
/// exist only in a library built with <c>QUIC_API_ENABLE_PREVIEW_FEATURES</c>; a library built without it has
/// nothing at these offsets. Reachable only through <c>MsQuicApi.TryGetPreviewExtension25</c>, which
/// requires the explicit <c>MsQuicApi.AllowPreviewFeatures</c> opt-in (ARCHITECTURE §7). Types the binding does
/// not model (<c>QUIC_CONNECTION_POOL_CONFIG</c>, <c>QUIC_EXECUTION_CONFIG</c>, <c>QUIC_EXECUTION</c>) are <c>void*</c>.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_API_TABLE_PREVIEW_2_5
{
    /// <summary><c>StreamProvideReceiveBuffers(Stream, BufferCount, Buffers)</c> for app-owned receive buffers. Available from v2.5.</summary>
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, uint, QUIC_BUFFER*, int> StreamProvideReceiveBuffers;
    /// <summary><c>ConnectionPoolCreate(QUIC_CONNECTION_POOL_CONFIG* Config, HQUIC* ConnectionPool)</c>. Available from v2.5.</summary>
    public delegate* unmanaged[Cdecl]<void*, QUIC_HANDLE**, int> ConnectionPoolCreate;
    /// <summary><c>ExecutionCreate(Flags, PollingIdleTimeoutUs, Count, QUIC_EXECUTION_CONFIG* Configs, QUIC_EXECUTION** Executions)</c>. Available from v2.5.</summary>
    public delegate* unmanaged[Cdecl]<int, uint, uint, void*, void**, int> ExecutionCreate;
    /// <summary><c>ExecutionDelete(Count, QUIC_EXECUTION** Executions)</c>. Available from v2.5.</summary>
    public delegate* unmanaged[Cdecl]<uint, void**, void> ExecutionDelete;
    /// <summary><c>ExecutionPoll(QUIC_EXECUTION* Execution)</c>; returns milliseconds until the next timer. Available from v2.5.</summary>
    public delegate* unmanaged[Cdecl]<void*, uint> ExecutionPoll;
}
