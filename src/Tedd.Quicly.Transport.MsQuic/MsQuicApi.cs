using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// Process-wide access to the MsQuic v2 API table. The table is opened lazily on first use and kept open for
/// the life of the process (MsQuic reference-counts <c>MsQuicOpenVersion</c>/<c>MsQuicClose</c>; closing it while
/// registrations exist would be an error, so we never close it). Libraries older than
/// <see cref="MsQuicFeatureGate.MinimumVersion"/> are refused (<see cref="IsSupported"/> is false).
/// </summary>
public sealed unsafe class MsQuicApi
{
    private static readonly object s_lock = new();
    private static MsQuicApi? s_instance;
    private static bool s_attempted;
    private static int s_openStatus;

    /// <summary>The raw function table (31 entries, see <see cref="QUIC_API_TABLE"/>). Never null on a constructed instance.</summary>
    public QUIC_API_TABLE* Table { get; }

    /// <summary>Library version from <c>QUIC_PARAM_GLOBAL_LIBRARY_VERSION</c> (major.minor.patch.build).</summary>
    public Version Version { get; }

    /// <summary>The TLS provider the library was built with.</summary>
    public QUIC_TLS_PROVIDER TlsProvider { get; }

    /// <summary>
    /// The <c>QUIC_STATISTICS_V2</c> sizes the library knows (<c>QUIC_PARAM_GLOBAL_STATISTICS_V2_SIZES</c>), oldest
    /// first; empty when the library predates that parameter. The last entry is the size the library fills.
    /// </summary>
    public uint[] StatisticsV2Sizes { get; }

    /// <summary>
    /// The number of <see cref="QUIC_STATISTICS_V2"/> bytes the loaded library fills, capped to our struct size
    /// (a preview-feature build reports more bytes than the binding models).
    /// </summary>
    public uint StatisticsV2Size { get; }

    /// <summary>Send flags (<see cref="QUIC_SEND_FLAGS"/>) the loaded library accepts.</summary>
    public QUIC_SEND_FLAGS SupportedSendFlags { get; }

    /// <summary>Start flags (<see cref="QUIC_STREAM_START_FLAGS"/>) the loaded library accepts.</summary>
    public QUIC_STREAM_START_FLAGS SupportedStartFlags { get; }

    /// <summary>
    /// Explicit opt-in for the preview-feature table entries (<see cref="QUIC_API_TABLE_PREVIEW_2_5"/>). They exist
    /// only in libraries built with <c>QUIC_API_ENABLE_PREVIEW_FEATURES</c>, which the binding cannot detect; the
    /// caller takes responsibility by setting this to true before calling <see cref="TryGetPreviewExtension25"/>.
    /// </summary>
    public static bool AllowPreviewFeatures { get; set; }

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

        StatisticsV2Sizes = ReadStatisticsSizes(table);
        uint librarySize = StatisticsV2Sizes.Length == 0 ? QUIC_STATISTICS_V2.SIZE_3 : StatisticsV2Sizes[^1];
        StatisticsV2Size = Math.Min(librarySize, (uint)sizeof(QUIC_STATISTICS_V2));

        SupportedSendFlags = MsQuicFeatureGate.SupportedSendFlags(Version);
        SupportedStartFlags = MsQuicFeatureGate.SupportedStartFlags(Version);
    }

    private static uint[] ReadStatisticsSizes(QUIC_API_TABLE* table)
    {
        uint length = 0;
        int status = table->GetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_STATISTICS_V2_SIZES, &length, null);
        if (status != MsQuicStatus.QUIC_STATUS_BUFFER_TOO_SMALL || length == 0 || length > 64 * sizeof(uint))
        {
            return [];
        }
        uint count = length / sizeof(uint);
        uint* sizes = stackalloc uint[(int)count];
        status = table->GetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_STATISTICS_V2_SIZES, &length, sizes);
        if (MsQuicStatus.Failed(status))
        {
            return [];
        }
        return new ReadOnlySpan<uint>(sizes, (int)(length / sizeof(uint))).ToArray();
    }

    /// <summary>True when the native library could be loaded, the v2 API table opened and the version is supported.</summary>
    public static bool IsSupported => TryGetInstance(out _, out _);

    /// <summary>The shared instance; throws <see cref="MsQuicException"/> when MsQuic is unavailable or too old.</summary>
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
                s_openStatus = OpenOnce(out s_instance);
            }
            api = s_instance;
            status = s_openStatus;
            return api is not null;
        }
    }

    private static int OpenOnce(out MsQuicApi? instance)
    {
        instance = null;
        void* table = null;
        int status;
        try
        {
            status = MsQuicNative.MsQuicOpenVersion(2, &table);
        }
        catch (DllNotFoundException)
        {
            return MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED;
        }
        catch (EntryPointNotFoundException)
        {
            return MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED;
        }
        catch (BadImageFormatException)
        {
            return MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED;
        }
        if (MsQuicStatus.Failed(status))
        {
            return status;
        }
        if (table == null)
        {
            return MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR;
        }
        var api = new MsQuicApi((QUIC_API_TABLE*)table);
        if (!MsQuicFeatureGate.IsSupported(api.Version))
        {
            MsQuicNative.MsQuicClose(table);
            return MsQuicStatus.QUIC_STATUS_VER_NEG_ERROR;
        }
        instance = api;
        return MsQuicStatus.QUIC_STATUS_SUCCESS;
    }

    /// <summary>The 2.5 table extension (<c>ConnectionOpenInPartition</c>), or false when the library is older.</summary>
    public bool TryGetExtension25(out QUIC_API_TABLE_EXT_2_5* extension)
    {
        if (MsQuicFeatureGate.HasExtension25(Version))
        {
            extension = (QUIC_API_TABLE_EXT_2_5*)((byte*)Table + sizeof(QUIC_API_TABLE));
            return true;
        }
        extension = null;
        return false;
    }

    /// <summary>
    /// The 2.5 preview-feature block. Requires <see cref="AllowPreviewFeatures"/> and a 2.5+ library; the caller
    /// must know the library was built with preview features enabled.
    /// </summary>
    public bool TryGetPreviewExtension25(out QUIC_API_TABLE_PREVIEW_2_5* extension)
    {
        if (AllowPreviewFeatures && MsQuicFeatureGate.HasExtension25(Version))
        {
            extension = (QUIC_API_TABLE_PREVIEW_2_5*)((byte*)Table + sizeof(QUIC_API_TABLE) + sizeof(QUIC_API_TABLE_EXT_2_5));
            return true;
        }
        extension = null;
        return false;
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
