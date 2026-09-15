using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>Raised by wrapper operations that cannot report a <c>QUIC_STATUS</c> as a return value (object construction, Start).</summary>
public sealed class MsQuicException : Exception
{
    /// <summary>The <c>QUIC_STATUS</c> that caused the failure.</summary>
    public int Status { get; }

    /// <summary>The MsQuic operation that failed (for example <c>ConfigurationLoadCredential</c>).</summary>
    public string Operation { get; }

    /// <summary>Creates an exception for a failed MsQuic call.</summary>
    public MsQuicException(int status, string operation)
        : base($"{operation} failed with {MsQuicStatus.GetName(status)}")
    {
        Status = status;
        Operation = operation;
    }

    /// <summary>Throws when <paramref name="status"/> is a failure code.</summary>
    public static void ThrowIfFailed(int status, string operation)
    {
        if (MsQuicStatus.Failed(status))
        {
            throw new MsQuicException(status, operation);
        }
    }
}
