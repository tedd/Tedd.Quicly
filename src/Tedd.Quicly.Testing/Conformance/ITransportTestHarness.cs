using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Testing.Conformance;

/// <summary>
/// Creates connected transport pairs for the <see cref="TransportConformance"/> scenarios and lets them make progress.
/// One implementation per carrier: <see cref="SimulatedTransportHarness"/> (virtual time) and one over real MsQuic loopback
/// in the MsQuic test project.
/// </summary>
/// <remarks>
/// Scenarios call transport APIs from the thread that calls <see cref="Pump"/> (the "game thread"). Sink callbacks run
/// inside <see cref="Pump"/> for a simulated transport and on the transport's own threads for a real one, so the scenarios
/// only use thread-safe sinks and never assume that a callback has or has not happened when an API call returns.
/// </remarks>
public interface ITransportTestHarness : IDisposable
{
    /// <summary>Name used in failure messages.</summary>
    string Name { get; }

    /// <summary>How long a scenario waits for one expected event (virtual time for a simulator, wall-clock time otherwise).</summary>
    TimeSpan DefaultTimeout { get; }

    /// <summary>
    /// Starts a connection from a new client transport (callbacks to <paramref name="clientSink"/>) to a listener whose
    /// accept callback returns <paramref name="serverSink"/>. Returns once the server side exists; the handshake may still
    /// be running. The harness owns both transports and disposes them in <see cref="IDisposable.Dispose"/>.
    /// </summary>
    ConformancePair CreatePair(ITransportSink clientSink, ITransportSink serverSink, ConformancePairOptions? options = null);

    /// <summary>
    /// Lets the transports make progress until <paramref name="condition"/> is true or <paramref name="timeout"/> has
    /// elapsed: a simulator advances virtual time, a real transport waits. Returns whether the condition became true.
    /// </summary>
    bool Pump(Func<bool> condition, TimeSpan timeout);
}

/// <summary>A client transport and the server transport a listener accepted for it.</summary>
/// <param name="Client">The connecting end.</param>
/// <param name="Server">The accepted end.</param>
public sealed record ConformancePair(ITransport Client, ITransport Server);

/// <summary>Initial stream limits of a pair created by <see cref="ITransportTestHarness.CreatePair"/>.</summary>
public sealed class ConformancePairOptions
{
    /// <summary>Bidirectional streams the server lets the client have open (the pre-admission default is 1). Default 16.</summary>
    public ushort ServerPeerBidiStreams { get; set; } = 16;

    /// <summary>Unidirectional streams the server lets the client have open (the pre-admission default is 0). Default 16.</summary>
    public ushort ServerPeerUnidiStreams { get; set; } = 16;

    /// <summary>Bidirectional streams the client lets the server have open. Default 16.</summary>
    public ushort ClientPeerBidiStreams { get; set; } = 16;

    /// <summary>Unidirectional streams the client lets the server have open. Default 16.</summary>
    public ushort ClientPeerUnidiStreams { get; set; } = 16;
}

/// <summary>Thrown by a <see cref="TransportConformance"/> scenario when a transport breaks the <see cref="ITransport"/> contract.</summary>
public sealed class ConformanceException : Exception
{
    /// <summary>Creates an exception without a message.</summary>
    public ConformanceException()
    {
    }

    /// <summary>Creates an exception describing the violation.</summary>
    public ConformanceException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an exception describing the violation, with the exception that revealed it.</summary>
    public ConformanceException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
