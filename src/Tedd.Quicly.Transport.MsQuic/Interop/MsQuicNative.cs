using System.Runtime.InteropServices;

namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>
/// The two exported entry points of the MsQuic library. <c>msquic</c> resolves to <c>msquic.dll</c> shipped in
/// the .NET shared framework on Windows and to <c>libmsquic.so</c> / <c>libmsquic.dylib</c> elsewhere.
/// </summary>
public static unsafe partial class MsQuicNative
{
    /// <summary>Library name passed to the loader.</summary>
    public const string LibraryName = "msquic";

    /// <summary>Opens the API table for the requested major version (2). Returns a <c>QUIC_STATUS</c>.</summary>
    [LibraryImport(LibraryName)]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial int MsQuicOpenVersion(uint version, void** quicApi);

    /// <summary>Releases an API table obtained from <see cref="MsQuicOpenVersion"/>.</summary>
    [LibraryImport(LibraryName)]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial void MsQuicClose(void* quicApi);
}
