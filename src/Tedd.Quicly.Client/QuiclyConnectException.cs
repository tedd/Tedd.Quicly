using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Client;

/// <summary><see cref="QuiclyClient.ConnectAsync"/> failed: the transport could not connect, or the server refused the session.</summary>
public sealed class QuiclyConnectException : Exception
{
    /// <summary>Creates an exception with a default message.</summary>
    public QuiclyConnectException()
        : this("The connection failed.")
    {
    }

    /// <summary>Creates an exception with <paramref name="message"/>.</summary>
    /// <param name="message">What went wrong.</param>
    public QuiclyConnectException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an exception with <paramref name="message"/> and the exception behind it.</summary>
    /// <param name="message">What went wrong.</param>
    /// <param name="innerException">The cause.</param>
    public QuiclyConnectException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Creates an exception describing a closed connection.</summary>
    /// <param name="message">What went wrong.</param>
    /// <param name="closeReason">Why the connection closed.</param>
    /// <param name="handshakeStatus">The HelloAck status the server sent (<see cref="HelloStatus.Accepted"/> when it sent none).</param>
    public QuiclyConnectException(string message, CloseReason closeReason, HelloStatus handshakeStatus)
        : base(message)
    {
        CloseReason = closeReason;
        HandshakeStatus = handshakeStatus;
    }

    /// <summary>Why the connection closed (for a refusal: <see cref="QuiclyErrorCode.AdmissionRejected"/>).</summary>
    public CloseReason CloseReason { get; }

    /// <summary>The server's refusal status, or <see cref="HelloStatus.Accepted"/> when the connection failed before any answer.</summary>
    public HelloStatus HandshakeStatus { get; }
}
