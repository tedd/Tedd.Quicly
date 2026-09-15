using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>TLS alerts as MsQuic statuses (QUIC_STATUS_TLS_ALERT), for the alerts msquic.h does not name.</summary>
internal static class TlsAlerts
{
    /// <summary>
    /// QUIC_STATUS_TLS_ALERT(48), unknown_ca: a chain was received but ends in no trusted anchor (RFC 8446 §6.2). This is how
    /// the Schannel msquic.dll fails a SystemRoots client that faces a private CA. msquic.h names no constant for it; the TLS
    /// alert statuses are the alert number added to one base, so it is derived from QUIC_STATUS_BAD_CERTIFICATE (alert 42).
    /// </summary>
    public static int UnknownCa => MsQuicStatus.QUIC_STATUS_BAD_CERTIFICATE + (48 - 42);
}
