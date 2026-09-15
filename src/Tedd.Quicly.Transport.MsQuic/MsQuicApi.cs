using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// Process-wide access to the MsQuic v2 API table. The table is opened lazily on first use and kept open for
/// the life of the process (MsQuic reference-counts <c>MsQuicOpenVersion</c>/<c>MsQuicClose</c>; closing it while
/// registrations exist would be an error, so we never close it).
/// </summary>
public sealed unsafe class MsQuicApi
{
    private static readonly object s_lock = new();
    private static MsQuicApi? s_instance;
    private static bool s_attempted;
    private static int s_openStatus;

    /// <summary>The raw function table for advanced use. Never null on a constructed instance.</summary>
    public QUIC_API_TABLE* Table { get; }

    /// <summary>Library version from <c>QUIC_PARAM_GLOBAL_LIBRARY_VERSION</c> (major.minor.patch.build).</summary>
    public Version Version { get; }

    /// <summary>The TLS provider the library was built with.</summary>
    public QUIC_TLS_PROVIDER TlsProvider { get; }

    private MsQuicApi(QUIC_API_TABLE* table)
    {
        Table = table;
        uint* v = stackalloc uint[4];
        uint length = 16;
        int status = table->GetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_LIBRARY_VERSION, &length, v);
        Version = MsQuicStatus.Succeeded(status) ? new Version((int)v[0], (int)v[1], (int)v[2], (int)v[3]) : new Version(0, 0);
        QUIC_TLS_PROVIDER provider = default;
        length = sizeof(QUIC_TLS_PROVIDER);
        status = table->GetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_TLS_PROVIDER, &length, &provider);
        TlsProvider = MsQuicStatus.Succeeded(status) ? provider : (OperatingSystem.IsWindows() ? QUIC_TLS_PROVIDER.SCHANNEL : QUIC_TLS_PROVIDER.OPENSSL);
    }

    /// <summary>True when the native library could be loaded and the v2 API table opened.</summary>
    public static bool IsSupported => TryGetInstance(out _, out _);

    /// <summary>The shared instance; throws <see cref="MsQuicException"/> when MsQuic is unavailable.</summary>
    public static MsQuicApi Instance
    {
        get
        {
            if (!TryGetInstance(out MsQuicApi? api, out int status))
            {
                throw new MsQuicException(status, nameof(MsQuicNative.MsQuicOpenVersion));
            }
            return api;
        }
    }

    /// <summary>Opens (once) and returns the shared instance, or the status that made opening fail.</summary>
    public static bool TryGetInstance([NotNullWhen(true)] out MsQuicApi? api, out int status)
    {
        lock (s_lock)
        {
            if (!s_attempted)
            {
                s_attempted = true;
                void* table = null;
                try
                {
                    s_openStatus = MsQuicNative.MsQuicOpenVersion(2, &table);
                }
                catch (DllNotFoundException)
                {
                    s_openStatus = MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED;
                }
                catch (EntryPointNotFoundException)
                {
                    s_openStatus = MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED;
                }
                catch (BadImageFormatException)
                {
                    s_openStatus = MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED;
                }
                if (MsQuicStatus.Succeeded(s_openStatus) && table != null)
                {
                    s_instance = new MsQuicApi((QUIC_API_TABLE*)table);
                }
                else if (MsQuicStatus.Succeeded(s_openStatus))
                {
                    s_openStatus = MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR;
                }
            }
            api = s_instance;
            status = s_openStatus;
            return api is not null;
        }
    }

    /// <summary>Raw <c>GetParam</c>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetParam(QUIC_HANDLE* handle, uint param, uint* bufferLength, void* buffer) => Table->GetParam(handle, param, bufferLength, buffer);

    /// <summary>Raw <c>SetParam</c>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int SetParam(QUIC_HANDLE* handle, uint param, uint bufferLength, void* buffer) => Table->SetParam(handle, param, bufferLength, buffer);

    /// <summary>Typed <c>GetParam</c> for a fixed-size value.</summary>
    public int GetParam<T>(QUIC_HANDLE* handle, uint param, out T value) where T : unmanaged
    {
        value = default;
        uint length = (uint)sizeof(T);
        fixed (T* p = &value)
        {
            return Table->GetParam(handle, param, &length, p);
        }
    }

    /// <summary>Typed <c>SetParam</c> for a fixed-size value.</summary>
    public int SetParam<T>(QUIC_HANDLE* handle, uint param, in T value) where T : unmanaged
    {
        fixed (T* p = &value)
        {
            return Table->SetParam(handle, param, (uint)sizeof(T), p);
        }
    }
}
