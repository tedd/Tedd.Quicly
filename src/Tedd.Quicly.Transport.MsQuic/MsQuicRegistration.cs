using System.Text;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// An MsQuic registration: the root object that owns worker threads and every configuration, listener and
/// connection created from it.
/// </summary>
/// <remarks>
/// Lifetime: <see cref="Close"/> calls <c>RegistrationClose</c>, which blocks until every child object has been
/// closed; close configurations, listeners, connections and streams first. Never use the registration after
/// <see cref="Close"/>.
/// </remarks>
public sealed unsafe class MsQuicRegistration : IDisposable
{
    private QUIC_HANDLE* _handle;

    /// <summary>The API instance this registration was opened on.</summary>
    public MsQuicApi Api { get; }

    /// <summary>The native handle (null after <see cref="Close"/>).</summary>
    public QUIC_HANDLE* Handle => _handle;

    /// <summary>True once <see cref="Close"/> has run.</summary>
    public bool IsClosed => _handle == null;

    /// <summary>Opens a registration.</summary>
    /// <param name="appName">Optional name shown in MsQuic traces.</param>
    /// <param name="executionProfile">Worker thread profile; <see cref="QUIC_EXECUTION_PROFILE.LOW_LATENCY"/> is MsQuic's default.</param>
    public MsQuicRegistration(string? appName = null, QUIC_EXECUTION_PROFILE executionProfile = QUIC_EXECUTION_PROFILE.LOW_LATENCY)
    {
        Api = MsQuicApi.Instance;
        byte[]? nameBytes = appName is null ? null : Encoding.UTF8.GetBytes(appName + '\0');
        fixed (byte* name = nameBytes)
        {
            QUIC_REGISTRATION_CONFIG config = new() { AppName = (sbyte*)name, ExecutionProfile = executionProfile };
            QUIC_HANDLE* handle = null;
            int status = Api.Table->RegistrationOpen(&config, &handle);
            MsQuicException.ThrowIfFailed(status, "RegistrationOpen");
            _handle = handle;
        }
    }

    /// <summary>Shuts down every connection in this registration (<c>RegistrationShutdown</c>).</summary>
    public void Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS flags, ulong errorCode)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        Api.Table->RegistrationShutdown(_handle, flags, errorCode);
    }

    /// <summary>Closes the registration. Idempotent. Blocks until all child objects are closed.</summary>
    public void Close()
    {
        QUIC_HANDLE* handle = _handle;
        if (handle == null) return;
        _handle = null;
        Api.Table->RegistrationClose(handle);
    }

    /// <inheritdoc cref="Close"/>
    public void Dispose() => Close();
}
