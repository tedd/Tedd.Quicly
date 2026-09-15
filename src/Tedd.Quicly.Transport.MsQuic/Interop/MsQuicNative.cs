using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>
/// The two exported entry points of the MsQuic library. <c>msquic</c> resolves to <c>msquic.dll</c> shipped in
/// the .NET shared framework on Windows and to <c>libmsquic.so</c> / <c>libmsquic.dylib</c> elsewhere.
/// </summary>
/// <remarks>
/// A <see cref="DllImportResolver"/> is registered before the first call. It first probes the directory of the
/// running runtime (<c>Path.GetDirectoryName(typeof(object).Assembly.Location)</c>, where the shared framework
/// keeps its copy of msquic on Windows) and then falls back to the default probing rules
/// (application directory, <c>PATH</c> / <c>LD_LIBRARY_PATH</c>, system directories).
/// </remarks>
public static unsafe partial class MsQuicNative
{
    /// <summary>Library name passed to the loader.</summary>
    public const string LibraryName = "msquic";

    static MsQuicNative()
    {
        NativeLibrary.SetDllImportResolver(typeof(MsQuicNative).Assembly, ResolveLibrary);
    }

    /// <summary>Runs the static constructor (which registers the resolver); calling any static member does the same.</summary>
    internal static void EnsureInitialized()
    {
    }

    /// <summary>Opens the API table for the requested major version (2). Returns a <c>QUIC_STATUS</c>.</summary>
    [LibraryImport(LibraryName)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    public static partial int MsQuicOpenVersion(uint version, void** quicApi);

    /// <summary>Releases an API table obtained from <see cref="MsQuicOpenVersion"/>.</summary>
    [LibraryImport(LibraryName)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    public static partial void MsQuicClose(void* quicApi);

    /// <summary>The platform file name the resolver probes for in the runtime directory.</summary>
    internal static string PlatformFileName =>
        OperatingSystem.IsWindows() ? "msquic.dll" : OperatingSystem.IsMacOS() ? "libmsquic.dylib" : "libmsquic.so";

    /// <summary>The directory of the running runtime (null when it cannot be determined, e.g. single-file publish).</summary>
    internal static string? RuntimeDirectory
    {
        get
        {
#pragma warning disable IL3000 // Single-file apps return an empty Location; that case falls through to default probing.
            string location = typeof(object).Assembly.Location;
#pragma warning restore IL3000
            return location.Length == 0 ? null : Path.GetDirectoryName(location);
        }
    }

    /// <summary>
    /// Resolver for <see cref="LibraryName"/>: runtime directory first, then default probing (returning
    /// <see cref="IntPtr.Zero"/> hands the decision back to the runtime). Other names are never touched.
    /// </summary>
    internal static IntPtr ResolveLibrary(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
        => ResolveLibrary(libraryName, assembly, searchPath, RuntimeDirectory);

    /// <summary>Resolver body with the runtime directory as a parameter (null or missing file: default probing only).</summary>
    internal static IntPtr ResolveLibrary(string libraryName, Assembly assembly, DllImportSearchPath? searchPath, string? runtimeDirectory)
    {
        if (!string.Equals(libraryName, LibraryName, StringComparison.Ordinal))
        {
            return IntPtr.Zero;
        }
        if (runtimeDirectory is not null)
        {
            string candidate = Path.Combine(runtimeDirectory, PlatformFileName);
            if (File.Exists(candidate) && NativeLibrary.TryLoad(candidate, out IntPtr handle))
            {
                return handle;
            }
        }
        return NativeLibrary.TryLoad(libraryName, assembly, searchPath, out IntPtr fallback) ? fallback : IntPtr.Zero;
    }
}
