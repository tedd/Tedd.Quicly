using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Interop;

[Collection(MsQuicCollection.Name)]
public class StatusTests
{
    [Fact]
    public void Succeeded_and_failed_follow_platform_rules()
    {
        Assert.True(MsQuicStatus.Succeeded(MsQuicStatus.QUIC_STATUS_SUCCESS));
        Assert.True(MsQuicStatus.Succeeded(MsQuicStatus.QUIC_STATUS_PENDING));
        Assert.True(MsQuicStatus.Succeeded(MsQuicStatus.QUIC_STATUS_CONTINUE));
        Assert.False(MsQuicStatus.Failed(MsQuicStatus.QUIC_STATUS_SUCCESS));
        Assert.True(MsQuicStatus.Failed(MsQuicStatus.QUIC_STATUS_INVALID_PARAMETER));
        Assert.True(MsQuicStatus.Failed(MsQuicStatus.QUIC_STATUS_ABORTED));
        Assert.True(MsQuicStatus.Failed(MsQuicStatus.QUIC_STATUS_BAD_CERTIFICATE));
        Assert.True(MsQuicStatus.Failed(MsQuicStatus.QUIC_STATUS_CERT_NO_CERT));
        Assert.False(MsQuicStatus.Succeeded(MsQuicStatus.QUIC_STATUS_ALPN_NEG_FAILURE));
    }

    [Fact]
    public void Windows_values_are_hresults()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Windows HRESULT layout");
        Assert.Equal(0x703E5, MsQuicStatus.QUIC_STATUS_PENDING);
        Assert.Equal(unchecked((int)0x80410007), MsQuicStatus.QUIC_STATUS_ALPN_NEG_FAILURE);
        Assert.Equal(unchecked((int)0x8041012A), MsQuicStatus.QUIC_STATUS_BAD_CERTIFICATE);
        Assert.Equal(unchecked((int)0x80410100), MsQuicStatus.QUIC_STATUS_CLOSE_NOTIFY);
        Assert.Equal(unchecked((int)0x80410178), MsQuicStatus.TlsAlert(120));
        Assert.Equal(unchecked((int)0x80410178), MsQuicStatus.TlsAlert(0x178));
        Assert.Equal(unchecked((int)0x80070002), MsQuicStatus.QUIC_STATUS_FILE_NOT_FOUND);
    }

    /// <summary>
    /// msquic_posix.h (UNVERIFIED here, the header is not in docs/reference): ERROR_BASE = 200000000,
    /// TLS_ERROR_BASE = 256 + ERROR_BASE, CERT_ERROR_BASE = 512 + ERROR_BASE, QUIC_STATUS_TLS_ALERT(a) =
    /// TLS_ERROR_BASE + (a &amp; 0xFF), QUIC_STATUS_CERT_* = CERT_ERROR_BASE + 1..3. Pinned on every OS through the
    /// internal posix helpers; on posix the public fields must agree.
    /// </summary>
    [Fact]
    public void Posix_tls_and_certificate_statuses_follow_msquic_posix_h()
    {
        Assert.Equal(0xBEBC200, MsQuicStatus.PosixErrorBase);
        Assert.Equal(0xBEBC300, MsQuicStatus.PosixTlsErrorBase);
        Assert.Equal(0xBEBC400, MsQuicStatus.PosixCertErrorBase);
        Assert.Equal(0xBEBC300 + 42, MsQuicStatus.PosixTlsAlert(42));
        Assert.Equal(0xBEBC300 + 0x78, MsQuicStatus.PosixTlsAlert(0x178));
        if (OperatingSystem.IsWindows()) return;
        Assert.Equal(MsQuicStatus.PosixTlsAlert(42), MsQuicStatus.QUIC_STATUS_BAD_CERTIFICATE);
        Assert.Equal(MsQuicStatus.PosixTlsErrorBase, MsQuicStatus.QUIC_STATUS_CLOSE_NOTIFY);
        Assert.Equal(0xBEBC401, MsQuicStatus.QUIC_STATUS_CERT_EXPIRED);
        Assert.Equal(0xBEBC402, MsQuicStatus.QUIC_STATUS_CERT_UNTRUSTED_ROOT);
        Assert.Equal(0xBEBC403, MsQuicStatus.QUIC_STATUS_CERT_NO_CERT);
    }

    [Fact]
    public void Get_name_knows_every_status_and_formats_unknown_ones()
    {
        Assert.Equal("QUIC_STATUS_SUCCESS", MsQuicStatus.GetName(MsQuicStatus.QUIC_STATUS_SUCCESS));
        Assert.Equal("QUIC_STATUS_PENDING", MsQuicStatus.GetName(MsQuicStatus.QUIC_STATUS_PENDING));
        Assert.Equal("QUIC_STATUS_CONTINUE", MsQuicStatus.GetName(MsQuicStatus.QUIC_STATUS_CONTINUE));
        foreach (System.Reflection.FieldInfo field in typeof(MsQuicStatus).GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
        {
            int value = (int)field.GetValue(null)!;
            string name = MsQuicStatus.GetName(value);
            Assert.StartsWith("QUIC_STATUS_", name, StringComparison.Ordinal);
            // Aliases (NOT_FOUND / FILE_NOT_FOUND share ENOENT on posix) may resolve to the first match; the value must agree.
            Assert.Equal(value, (int)typeof(MsQuicStatus).GetField(name)!.GetValue(null)!);
        }
        Assert.Equal("0x12345678", MsQuicStatus.GetName(0x12345678));
    }

    [Fact]
    public void Param_helpers()
    {
        Assert.True(MsQuicParam.IsGlobal(MsQuicParam.QUIC_PARAM_GLOBAL_LIBRARY_VERSION));
        Assert.True(MsQuicParam.IsGlobal(MsQuicParam.QUIC_PARAM_GLOBAL_LIBRARY_VERSION | MsQuicParam.QUIC_PARAM_HIGH_PRIORITY));
        Assert.False(MsQuicParam.IsGlobal(MsQuicParam.QUIC_PARAM_CONN_STATISTICS_V2));
        Assert.Equal(0x05000016u, MsQuicParam.QUIC_PARAM_CONN_STATISTICS_V2);
        Assert.Equal(0x08000003u, MsQuicParam.QUIC_PARAM_STREAM_PRIORITY);
        Assert.Equal(0x04000000u, MsQuicParam.QUIC_PARAM_LISTENER_LOCAL_ADDRESS);
        Assert.Equal(0x0100000Cu, MsQuicParam.QUIC_PARAM_GLOBAL_STATISTICS_V2_SIZES);
    }
}
