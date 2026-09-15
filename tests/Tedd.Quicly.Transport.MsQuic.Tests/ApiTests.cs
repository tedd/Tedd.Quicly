using System.Runtime.InteropServices;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

[Collection(MsQuicCollection.Name)]
public unsafe class ApiTests
{
    [Fact]
    public void Api_opens_and_reports_version_2_4_or_later()
    {
        Assert.True(MsQuicApi.IsSupported);
        MsQuicApi api = MsQuicApi.Instance;
        Assert.True(api.Table != null);
        Assert.True(api.Version >= new Version(2, 4), $"version {api.Version}");
        Assert.True(MsQuicFeatureGate.IsSupported(api.Version));
    }

    [Fact]
    public void Instance_is_a_singleton()
    {
        Assert.True(MsQuicApi.TryGetInstance(out MsQuicApi? a, out int statusA));
        Assert.True(MsQuicApi.TryGetInstance(out MsQuicApi? b, out int statusB));
        Assert.Same(a, b);
        Assert.Same(a, MsQuicApi.Instance);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, statusA);
        Assert.Equal(statusA, statusB);
    }

    [Fact]
    public void Tls_provider_matches_platform()
    {
        QUIC_TLS_PROVIDER expected = OperatingSystem.IsWindows() ? QUIC_TLS_PROVIDER.SCHANNEL : QUIC_TLS_PROVIDER.OPENSSL;
        Assert.Equal(expected, MsQuicApi.Instance.TlsProvider);
    }

    [Fact]
    public void Statistics_v2_sizes_are_queried_from_the_library()
    {
        MsQuicApi api = MsQuicApi.Instance;
        Assert.NotEmpty(api.StatisticsV2Sizes);
        Assert.Equal(QUIC_STATISTICS_V2.SIZE_1, api.StatisticsV2Sizes[0]);
        Assert.Contains(QUIC_STATISTICS_V2.SIZE_2, api.StatisticsV2Sizes);
        Assert.Contains(QUIC_STATISTICS_V2.SIZE_3, api.StatisticsV2Sizes);
        Assert.True(api.StatisticsV2Sizes[^1] >= QUIC_STATISTICS_V2.SIZE_3);
        Assert.True(api.StatisticsV2Size >= QUIC_STATISTICS_V2.SIZE_3 && api.StatisticsV2Size <= sizeof(QUIC_STATISTICS_V2));
        if (api.Version >= new Version(2, 5))
        {
            Assert.Equal(QUIC_STATISTICS_V2.SIZE_4, api.StatisticsV2Size);
        }
    }

    [Fact]
    public void Supported_flags_follow_the_library_version()
    {
        MsQuicApi api = MsQuicApi.Instance;
        Assert.Equal(MsQuicFeatureGate.SupportedSendFlags(api.Version), api.SupportedSendFlags);
        Assert.Equal(MsQuicFeatureGate.SupportedStartFlags(api.Version), api.SupportedStartFlags);
        // Minimum version is 2.4, so every documented flag is available on an accepted library.
        Assert.True((api.SupportedSendFlags & QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED) != 0);
        Assert.True((api.SupportedStartFlags & QUIC_STREAM_START_FLAGS.PRIORITY_WORK) != 0);
    }

    [Fact]
    public void Extension_2_5_is_gated_on_version_and_works_when_present()
    {
        MsQuicApi api = MsQuicApi.Instance;
        bool has = api.TryGetExtension25(out QUIC_API_TABLE_EXT_2_5* ext);
        Assert.Equal(api.Version >= new Version(2, 5), has);
        if (!has)
        {
            Assert.True(ext == null);
            return;
        }
        Assert.True(ext != null);
        Assert.Equal(sizeof(QUIC_API_TABLE), (int)((byte*)ext - (byte*)api.Table));
        Assert.True(ext->ConnectionOpenInPartition != null);

        // Use the entry for real: open a connection on partition 0 and close it again.
        using var registration = new MsQuicRegistration("ext25");
        QUIC_HANDLE* handle = null;
        int status = ext->ConnectionOpenInPartition(registration.Handle, 0, &NoOpConnectionCallback, null, &handle);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, status);
        Assert.True(handle != null);
        api.Table->ConnectionClose(handle);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    private static int NoOpConnectionCallback(QUIC_HANDLE* handle, void* context, QUIC_CONNECTION_EVENT* evt) => MsQuicStatus.QUIC_STATUS_SUCCESS;

    [Fact]
    public void Preview_extension_requires_explicit_opt_in()
    {
        MsQuicApi api = MsQuicApi.Instance;
        bool previous = MsQuicApi.AllowPreviewFeatures;
        try
        {
            MsQuicApi.AllowPreviewFeatures = false;
            Assert.False(api.TryGetPreviewExtension25(out QUIC_API_TABLE_PREVIEW_2_5* none));
            Assert.True(none == null);

            MsQuicApi.AllowPreviewFeatures = true;
            bool has = api.TryGetPreviewExtension25(out QUIC_API_TABLE_PREVIEW_2_5* preview);
            Assert.Equal(api.Version >= new Version(2, 5), has);
            if (has)
            {
                Assert.Equal(sizeof(QUIC_API_TABLE) + sizeof(QUIC_API_TABLE_EXT_2_5), (int)((byte*)preview - (byte*)api.Table));
            }
        }
        finally
        {
            MsQuicApi.AllowPreviewFeatures = previous;
        }
    }

    [Fact]
    public void Global_get_param_typed_and_raw_agree()
    {
        MsQuicApi api = MsQuicApi.Instance;
        uint* raw = stackalloc uint[4];
        uint length = 16;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, api.GetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_LIBRARY_VERSION, &length, raw));
        Assert.Equal(16u, length);
        Assert.Equal((int)raw[0], api.Version.Major);
        Assert.Equal((int)raw[1], api.Version.Minor);

        ulong* counters = stackalloc ulong[(int)QUIC_PERFORMANCE_COUNTERS.MAX];
        length = (uint)(sizeof(ulong) * (int)QUIC_PERFORMANCE_COUNTERS.MAX);
        Assert.True(MsQuicStatus.Succeeded(api.GetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_PERF_COUNTERS, &length, counters)));
        Assert.True(length > 0);
    }

    [Fact]
    public void Global_get_param_reports_buffer_too_small()
    {
        MsQuicApi api = MsQuicApi.Instance;
        uint length = 0;
        int status = api.GetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_LIBRARY_VERSION, &length, null);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_BUFFER_TOO_SMALL, status);
        Assert.Equal(16u, length);
    }

    [Fact]
    public void Typed_set_and_get_param_round_trip_on_global()
    {
        MsQuicApi api = MsQuicApi.Instance;
        Assert.True(MsQuicStatus.Succeeded(api.GetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_RETRY_MEMORY_PERCENT, out ushort percent)));
        Assert.True(MsQuicStatus.Succeeded(api.SetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_RETRY_MEMORY_PERCENT, in percent)));
        Assert.True(MsQuicStatus.Succeeded(api.GetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_RETRY_MEMORY_PERCENT, out ushort again)));
        Assert.Equal(percent, again);
    }

    [Fact]
    public void Exception_reports_status_and_operation()
    {
        var ex = new MsQuicException(MsQuicStatus.QUIC_STATUS_INVALID_PARAMETER, "Op");
        Assert.Equal(MsQuicStatus.QUIC_STATUS_INVALID_PARAMETER, ex.Status);
        Assert.Equal("Op", ex.Operation);
        Assert.Contains("QUIC_STATUS_INVALID_PARAMETER", ex.Message, StringComparison.Ordinal);
        MsQuicException.ThrowIfFailed(MsQuicStatus.QUIC_STATUS_SUCCESS, "ok");
        MsQuicException.ThrowIfFailed(MsQuicStatus.QUIC_STATUS_PENDING, "ok");
        Assert.Throws<MsQuicException>(() => MsQuicException.ThrowIfFailed(MsQuicStatus.QUIC_STATUS_ABORTED, "bad"));
    }

    [Fact]
    public void Callback_scope_is_clear_outside_callbacks()
    {
        Assert.False(MsQuicCallbackScope.IsInsideCallback);
        Assert.False(MsQuicCallbackScope.FailFastOnCallbackException);
    }

    [Fact]
    public void Resolver_finds_the_runtime_copy_and_ignores_other_names()
    {
        Assert.Equal(OperatingSystem.IsWindows() ? "msquic.dll" : OperatingSystem.IsMacOS() ? "libmsquic.dylib" : "libmsquic.so", MsQuicNative.PlatformFileName);
        Assert.Equal(IntPtr.Zero, MsQuicNative.ResolveLibrary("not-msquic", typeof(MsQuicNative).Assembly, null));
        IntPtr handle = MsQuicNative.ResolveLibrary(MsQuicNative.LibraryName, typeof(MsQuicNative).Assembly, null);
        Assert.NotEqual(IntPtr.Zero, handle);
        if (OperatingSystem.IsWindows())
        {
            string? runtimeDirectory = MsQuicNative.RuntimeDirectory;
            Assert.NotNull(runtimeDirectory);
            Assert.True(File.Exists(Path.Combine(runtimeDirectory, "msquic.dll")), "the shared framework ships msquic.dll");
        }
    }
}

[Collection(MsQuicCollection.Name)]
public class FeatureGateTests
{
    [Fact]
    public void Minimum_version_is_2_4()
    {
        Assert.Equal(new Version(2, 4), MsQuicFeatureGate.MinimumVersion);
        Assert.False(MsQuicFeatureGate.IsSupported(new Version(2, 3, 9)));
        Assert.True(MsQuicFeatureGate.IsSupported(new Version(2, 4, 0)));
        Assert.True(MsQuicFeatureGate.IsSupported(new Version(3, 0)));
        Assert.Throws<ArgumentNullException>(() => MsQuicFeatureGate.IsSupported(null!));
    }

    [Fact]
    public void Extension_2_5_gate()
    {
        Assert.False(MsQuicFeatureGate.HasExtension25(new Version(2, 4, 9)));
        Assert.True(MsQuicFeatureGate.HasExtension25(new Version(2, 5, 0)));
        Assert.Throws<ArgumentNullException>(() => MsQuicFeatureGate.HasExtension25(null!));
    }

    [Fact]
    public void Send_flags_are_gated_by_version()
    {
        Assert.Equal(MsQuicFeatureGate.BaseSendFlags, MsQuicFeatureGate.SupportedSendFlags(new Version(2, 1)));
        Assert.Equal(MsQuicFeatureGate.BaseSendFlags | MsQuicFeatureGate.SendFlags22, MsQuicFeatureGate.SupportedSendFlags(new Version(2, 3)));
        Assert.Equal(MsQuicFeatureGate.BaseSendFlags | MsQuicFeatureGate.SendFlags22 | MsQuicFeatureGate.SendFlags24, MsQuicFeatureGate.SupportedSendFlags(new Version(2, 5)));
        Assert.True(MsQuicFeatureGate.AreSendFlagsSupported(new Version(2, 4), QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED | QUIC_SEND_FLAGS.DELAY_SEND));
        Assert.False(MsQuicFeatureGate.AreSendFlagsSupported(new Version(2, 3), QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED));
        Assert.False(MsQuicFeatureGate.AreSendFlagsSupported(new Version(2, 1), QUIC_SEND_FLAGS.CANCEL_ON_LOSS));
        Assert.True(MsQuicFeatureGate.AreSendFlagsSupported(new Version(2, 1), QUIC_SEND_FLAGS.FIN | QUIC_SEND_FLAGS.START));
        Assert.Throws<ArgumentNullException>(() => MsQuicFeatureGate.SupportedSendFlags(null!));
    }

    [Fact]
    public void Start_flags_are_gated_by_version()
    {
        Assert.Equal(MsQuicFeatureGate.BaseStartFlags, MsQuicFeatureGate.SupportedStartFlags(new Version(2, 1)));
        Assert.Equal(MsQuicFeatureGate.BaseStartFlags | MsQuicFeatureGate.StartFlags22, MsQuicFeatureGate.SupportedStartFlags(new Version(2, 2)));
        Assert.True(MsQuicFeatureGate.AreStartFlagsSupported(new Version(2, 2), QUIC_STREAM_START_FLAGS.PRIORITY_WORK | QUIC_STREAM_START_FLAGS.FAIL_BLOCKED));
        Assert.False(MsQuicFeatureGate.AreStartFlagsSupported(new Version(2, 1), QUIC_STREAM_START_FLAGS.PRIORITY_WORK));
        Assert.Throws<ArgumentNullException>(() => MsQuicFeatureGate.SupportedStartFlags(null!));
    }

    [Fact]
    public void Flag_values_match_msquic_h()
    {
        Assert.Equal(0x20, (int)QUIC_SEND_FLAGS.CANCEL_ON_LOSS);
        Assert.Equal(0x40, (int)QUIC_SEND_FLAGS.PRIORITY_WORK);
        Assert.Equal(0x80, (int)QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED);
        Assert.Equal(0x10, (int)QUIC_STREAM_START_FLAGS.PRIORITY_WORK);
    }
}
