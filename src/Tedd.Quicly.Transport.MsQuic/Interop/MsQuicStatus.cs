namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>
/// <c>QUIC_STATUS_*</c> values. They are platform specific: on Windows they are HRESULTs (verified against the
/// runtime's bindings, see the layout dump); on Linux/macOS they are errno-based values from msquic_posix.h,
/// transcribed from that header without a live verification on those platforms (marked UNVERIFIED).
/// The fields are <c>static readonly</c> (not <c>const</c>) because the value depends on the OS at run time.
/// </summary>
public static class MsQuicStatus
{
    private static readonly bool s_windows = OperatingSystem.IsWindows();
    private static readonly bool s_bsd = QuicAddressFamily.IsBsdLayout;

    private static int Pick(int windows, int linux, int bsd) => s_windows ? windows : s_bsd ? bsd : linux;

    public const int QUIC_STATUS_SUCCESS = 0;
    public static readonly int QUIC_STATUS_PENDING = Pick(0x703E5, -2, -2);
    public static readonly int QUIC_STATUS_CONTINUE = Pick(0x704DE, -1, -1);
    public static readonly int QUIC_STATUS_OUT_OF_MEMORY = Pick(unchecked((int)0x8007000E), 12, 12);            // ENOMEM
    public static readonly int QUIC_STATUS_INVALID_PARAMETER = Pick(unchecked((int)0x80070057), 22, 22);        // EINVAL
    public static readonly int QUIC_STATUS_INVALID_STATE = Pick(unchecked((int)0x8007139F), 1, 1);              // EPERM
    public static readonly int QUIC_STATUS_NOT_SUPPORTED = Pick(unchecked((int)0x80004002), 95, 102);           // EOPNOTSUPP (UNVERIFIED on posix)
    public static readonly int QUIC_STATUS_NOT_FOUND = Pick(unchecked((int)0x80070490), 2, 2);                  // ENOENT
    public static readonly int QUIC_STATUS_FILE_NOT_FOUND = Pick(unchecked((int)0x80070002), 2, 2);             // ENOENT
    public static readonly int QUIC_STATUS_BUFFER_TOO_SMALL = Pick(unchecked((int)0x8007007A), 75, 84);         // EOVERFLOW
    public static readonly int QUIC_STATUS_HANDSHAKE_FAILURE = Pick(unchecked((int)0x80410000), 103, 53);       // ECONNABORTED
    public static readonly int QUIC_STATUS_ABORTED = Pick(unchecked((int)0x80004004), 125, 89);                 // ECANCELED
    public static readonly int QUIC_STATUS_ADDRESS_IN_USE = Pick(unchecked((int)0x80072740), 98, 48);           // EADDRINUSE
    public static readonly int QUIC_STATUS_INVALID_ADDRESS = Pick(unchecked((int)0x80072741), 97, 47);          // EAFNOSUPPORT
    public static readonly int QUIC_STATUS_CONNECTION_TIMEOUT = Pick(unchecked((int)0x80410006), 110, 60);      // ETIMEDOUT
    public static readonly int QUIC_STATUS_CONNECTION_IDLE = Pick(unchecked((int)0x80410005), 62, 101);         // ETIME
    public static readonly int QUIC_STATUS_UNREACHABLE = Pick(unchecked((int)0x800704D0), 113, 65);             // EHOSTUNREACH
    public static readonly int QUIC_STATUS_INTERNAL_ERROR = Pick(unchecked((int)0x80410003), 5, 5);             // EIO
    public static readonly int QUIC_STATUS_CONNECTION_REFUSED = Pick(unchecked((int)0x800704C9), 111, 61);      // ECONNREFUSED
    public static readonly int QUIC_STATUS_PROTOCOL_ERROR = Pick(unchecked((int)0x80410004), 71, 100);          // EPROTO
    public static readonly int QUIC_STATUS_VER_NEG_ERROR = Pick(unchecked((int)0x80410001), 93, 43);            // EPROTONOSUPPORT
    public static readonly int QUIC_STATUS_TLS_ERROR = Pick(unchecked((int)0x80072B18), 126, 126);              // ENOKEY (UNVERIFIED on posix)
    public static readonly int QUIC_STATUS_USER_CANCELED = Pick(unchecked((int)0x80410002), 130, 105);          // EOWNERDEAD
    public static readonly int QUIC_STATUS_ALPN_NEG_FAILURE = Pick(unchecked((int)0x80410007), 92, 42);         // ENOPROTOOPT
    public static readonly int QUIC_STATUS_STREAM_LIMIT_REACHED = Pick(unchecked((int)0x80410008), 86, 86);     // ESTRPIPE (UNVERIFIED on posix)
    public static readonly int QUIC_STATUS_ALPN_IN_USE = Pick(unchecked((int)0x80410009), 91, 41);              // EPROTOTYPE
    public static readonly int QUIC_STATUS_ADDRESS_NOT_AVAILABLE = Pick(unchecked((int)0x8007273F), 99, 49);    // EADDRNOTAVAIL

    /// <summary><c>ERROR_BASE</c> of msquic_posix.h (200 000 000 = 0xBEBC200). UNVERIFIED: the header is not in docs/reference.</summary>
    internal const int PosixErrorBase = 200000000;

    /// <summary><c>TLS_ERROR_BASE</c> of msquic_posix.h (<c>256 + ERROR_BASE</c> = 0xBEBC300). UNVERIFIED.</summary>
    internal const int PosixTlsErrorBase = 256 + PosixErrorBase;

    /// <summary><c>CERT_ERROR_BASE</c> of msquic_posix.h (<c>512 + ERROR_BASE</c> = 0xBEBC400). UNVERIFIED.</summary>
    internal const int PosixCertErrorBase = 512 + PosixErrorBase;

    /// <summary>
    /// Builds the status for a TLS alert (<c>QUIC_STATUS_TLS_ALERT(Alert)</c>): <c>0x80410100 | alert</c> on Windows
    /// (verified), <c>TLS_ERROR_BASE + (alert &amp; 0xFF)</c> on posix (UNVERIFIED, transcribed from msquic_posix.h).
    /// </summary>
    public static int TlsAlert(int alert) => s_windows ? unchecked((int)0x80410100) | (alert & 0xFF) : PosixTlsAlert(alert);

    /// <summary>The posix form of <see cref="TlsAlert"/>, available on every OS so it can be pinned by tests.</summary>
    internal static int PosixTlsAlert(int alert) => PosixTlsErrorBase + (alert & 0xFF);

    public static readonly int QUIC_STATUS_CLOSE_NOTIFY = TlsAlert(0);
    public static readonly int QUIC_STATUS_BAD_CERTIFICATE = TlsAlert(42);
    public static readonly int QUIC_STATUS_UNSUPPORTED_CERTIFICATE = TlsAlert(43);
    public static readonly int QUIC_STATUS_REVOKED_CERTIFICATE = TlsAlert(44);
    public static readonly int QUIC_STATUS_EXPIRED_CERTIFICATE = TlsAlert(45);
    public static readonly int QUIC_STATUS_UNKNOWN_CERTIFICATE = TlsAlert(46);
    public static readonly int QUIC_STATUS_REQUIRED_CERTIFICATE = TlsAlert(116);

    public static readonly int QUIC_STATUS_CERT_EXPIRED = Pick(unchecked((int)0x800B0101), PosixCertErrorBase + 1, PosixCertErrorBase + 1);        // CERT_ERROR_BASE + 1 (UNVERIFIED on posix)
    public static readonly int QUIC_STATUS_CERT_UNTRUSTED_ROOT = Pick(unchecked((int)0x800B0109), PosixCertErrorBase + 2, PosixCertErrorBase + 2); // CERT_ERROR_BASE + 2 (UNVERIFIED on posix)
    public static readonly int QUIC_STATUS_CERT_NO_CERT = Pick(unchecked((int)0x8009030E), PosixCertErrorBase + 3, PosixCertErrorBase + 3);        // CERT_ERROR_BASE + 3 (UNVERIFIED on posix)

    /// <summary><c>QUIC_SUCCEEDED</c>: HRESULT sign test on Windows, <c>status &lt;= 0</c> on posix (where PENDING/CONTINUE are negative).</summary>
    public static bool Succeeded(int status) => s_windows ? status >= 0 : status <= 0;

    /// <summary><c>QUIC_FAILED</c>.</summary>
    public static bool Failed(int status) => !Succeeded(status);

    /// <summary>Returns the symbolic name of a known status, or the hex value for unknown ones. For diagnostics only (allocates).</summary>
    public static string GetName(int status)
    {
        if (status == QUIC_STATUS_SUCCESS) return nameof(QUIC_STATUS_SUCCESS);
        if (status == QUIC_STATUS_PENDING) return nameof(QUIC_STATUS_PENDING);
        if (status == QUIC_STATUS_CONTINUE) return nameof(QUIC_STATUS_CONTINUE);
        if (status == QUIC_STATUS_OUT_OF_MEMORY) return nameof(QUIC_STATUS_OUT_OF_MEMORY);
        if (status == QUIC_STATUS_INVALID_PARAMETER) return nameof(QUIC_STATUS_INVALID_PARAMETER);
        if (status == QUIC_STATUS_INVALID_STATE) return nameof(QUIC_STATUS_INVALID_STATE);
        if (status == QUIC_STATUS_NOT_SUPPORTED) return nameof(QUIC_STATUS_NOT_SUPPORTED);
        if (status == QUIC_STATUS_NOT_FOUND) return nameof(QUIC_STATUS_NOT_FOUND);
        if (status == QUIC_STATUS_FILE_NOT_FOUND) return nameof(QUIC_STATUS_FILE_NOT_FOUND);
        if (status == QUIC_STATUS_BUFFER_TOO_SMALL) return nameof(QUIC_STATUS_BUFFER_TOO_SMALL);
        if (status == QUIC_STATUS_HANDSHAKE_FAILURE) return nameof(QUIC_STATUS_HANDSHAKE_FAILURE);
        if (status == QUIC_STATUS_ABORTED) return nameof(QUIC_STATUS_ABORTED);
        if (status == QUIC_STATUS_ADDRESS_IN_USE) return nameof(QUIC_STATUS_ADDRESS_IN_USE);
        if (status == QUIC_STATUS_INVALID_ADDRESS) return nameof(QUIC_STATUS_INVALID_ADDRESS);
        if (status == QUIC_STATUS_CONNECTION_TIMEOUT) return nameof(QUIC_STATUS_CONNECTION_TIMEOUT);
        if (status == QUIC_STATUS_CONNECTION_IDLE) return nameof(QUIC_STATUS_CONNECTION_IDLE);
        if (status == QUIC_STATUS_UNREACHABLE) return nameof(QUIC_STATUS_UNREACHABLE);
        if (status == QUIC_STATUS_INTERNAL_ERROR) return nameof(QUIC_STATUS_INTERNAL_ERROR);
        if (status == QUIC_STATUS_CONNECTION_REFUSED) return nameof(QUIC_STATUS_CONNECTION_REFUSED);
        if (status == QUIC_STATUS_PROTOCOL_ERROR) return nameof(QUIC_STATUS_PROTOCOL_ERROR);
        if (status == QUIC_STATUS_VER_NEG_ERROR) return nameof(QUIC_STATUS_VER_NEG_ERROR);
        if (status == QUIC_STATUS_TLS_ERROR) return nameof(QUIC_STATUS_TLS_ERROR);
        if (status == QUIC_STATUS_USER_CANCELED) return nameof(QUIC_STATUS_USER_CANCELED);
        if (status == QUIC_STATUS_ALPN_NEG_FAILURE) return nameof(QUIC_STATUS_ALPN_NEG_FAILURE);
        if (status == QUIC_STATUS_STREAM_LIMIT_REACHED) return nameof(QUIC_STATUS_STREAM_LIMIT_REACHED);
        if (status == QUIC_STATUS_ALPN_IN_USE) return nameof(QUIC_STATUS_ALPN_IN_USE);
        if (status == QUIC_STATUS_ADDRESS_NOT_AVAILABLE) return nameof(QUIC_STATUS_ADDRESS_NOT_AVAILABLE);
        if (status == QUIC_STATUS_CLOSE_NOTIFY) return nameof(QUIC_STATUS_CLOSE_NOTIFY);
        if (status == QUIC_STATUS_BAD_CERTIFICATE) return nameof(QUIC_STATUS_BAD_CERTIFICATE);
        if (status == QUIC_STATUS_UNSUPPORTED_CERTIFICATE) return nameof(QUIC_STATUS_UNSUPPORTED_CERTIFICATE);
        if (status == QUIC_STATUS_REVOKED_CERTIFICATE) return nameof(QUIC_STATUS_REVOKED_CERTIFICATE);
        if (status == QUIC_STATUS_EXPIRED_CERTIFICATE) return nameof(QUIC_STATUS_EXPIRED_CERTIFICATE);
        if (status == QUIC_STATUS_UNKNOWN_CERTIFICATE) return nameof(QUIC_STATUS_UNKNOWN_CERTIFICATE);
        if (status == QUIC_STATUS_REQUIRED_CERTIFICATE) return nameof(QUIC_STATUS_REQUIRED_CERTIFICATE);
        if (status == QUIC_STATUS_CERT_EXPIRED) return nameof(QUIC_STATUS_CERT_EXPIRED);
        if (status == QUIC_STATUS_CERT_UNTRUSTED_ROOT) return nameof(QUIC_STATUS_CERT_UNTRUSTED_ROOT);
        if (status == QUIC_STATUS_CERT_NO_CERT) return nameof(QUIC_STATUS_CERT_NO_CERT);
        return "0x" + status.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);
    }
}
