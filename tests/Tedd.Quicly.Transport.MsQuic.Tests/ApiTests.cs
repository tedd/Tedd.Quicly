using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

public unsafe class ApiTests
{
    [Fact]
    public void Api_opens_and_reports_version_2_or_later()
    {
        Assert.True(MsQuicApi.IsSupported);
        MsQuicApi api = MsQuicApi.Instance;
        Assert.True(api.Table != null);
        Assert.True(api.Version >= new Version(2, 0), $"version {api.Version}");
        Assert.True(api.Version.Major >= 2);
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
    public void Global_get_param_typed_and_raw_agree()
    {
        MsQuicApi api = MsQuicApi.Instance;
        uint* raw = stackalloc uint[4];
        uint length = 16;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, api.GetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_LIBRARY_VERSION, &length, raw));
        Assert.Equal(16u, length);
        Assert.Equal((int)raw[0], api.Version.Major);
        Assert.Equal((int)raw[1], api.Version.Minor);

        // Perf counters: an array of ulong, at least QUIC_PERFORMANCE_COUNTERS.MAX entries.
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
        // Retry memory percent is a harmless global knob; read, write the same value, read again.
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
}
