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
/// <para>
/// Three scenarios make a transport call from a second thread, because the timing of that call is what they test:
/// <see cref="ITransport.ResumeStreamReceive"/> against the receive callback (<c>ResumeRacingTheReceiveCallbackIsApplied</c>,
/// <c>HeldStreamIsIndicatedAgainAfterEveryResume</c>) and <see cref="ITransport.Close"/> against datagrams in flight
/// (<c>DatagramsInFlightAtCloseReachAFinalStateBeforeOnClosed</c>). The second thread is woken by an event the callback or
/// the sink sets, and while its call is owed the pumping thread blocks inside its <see cref="Pump"/> condition until that
/// call has been made (with a real-time bound of 30 seconds, after which the scenario fails). A harness on virtual time
/// therefore does not advance its clock while the call is owed: the scenario takes the same virtual time in every run, its
/// timeout cannot run out merely because the second thread had to wait for a core, and because neither thread spins
/// against the other a hand-off costs a thread wake-up even when every core is busy. A harness must therefore evaluate the
/// condition outside any lock its transports take in <see cref="ITransport.ResumeStreamReceive"/> and
/// <see cref="ITransport.Close"/>. One receive callback blocks too: the held-stream scenario's callback waits (busy for
/// 20 microseconds, then blocked) until the resuming thread runs, so that its resume races the callback's return; that
/// thread signals before it calls the transport, so the wait never depends on a lock the callback's thread holds.
/// </para>
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
    /// be running (or fail, with <see cref="ConformancePairOptions.FailHandshake"/>). The harness owns both transports and
    /// disposes them in <see cref="IDisposable.Dispose"/>.
    /// </summary>
    ConformancePair CreatePair(ITransportSink clientSink, ITransportSink serverSink, ConformancePairOptions? options = null);

    /// <summary>
    /// Starts a client connection (callbacks to <paramref name="clientSink"/>) to a new listener that runs
    /// <paramref name="preHandshake"/> and then <paramref name="accept"/> for it, and returns the client transport at once:
    /// the handshake runs in the background (driven by <see cref="Pump"/>), so the client is still connecting when the call
    /// returns. The callbacks run where the transport runs them (inside <see cref="Pump"/> for a simulator, on transport
    /// threads otherwise). The harness owns the client, the listener and every transport handed to <paramref name="accept"/>
    /// (refused ones included) and disposes them.
    /// </summary>
    ITransport Connect(ITransportSink clientSink, PreHandshakeCallback preHandshake, AcceptCallback accept, ConformancePairOptions? options = null);

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

/// <summary>Initial stream limits and connection behaviour of a pair created by <see cref="ITransportTestHarness.CreatePair"/>.</summary>
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

    /// <summary>
    /// When set, the transports close on their own (<see cref="TransportCloseReason.Transport"/>) about this long after the
    /// connection was made, provided the scenario leaves it idle: the MsQuic harness uses it as the idle timeout of both
    /// ends with keep-alives off, the simulator cuts the link that long after it was created
    /// (<see cref="Simulation.LinkOptions.DisconnectAtMicros"/>). Default null (never).
    /// </summary>
    public TimeSpan? TransportCloseAfter { get; set; }

    /// <summary>
    /// When true, the handshake fails after the listener accepted the connection: neither end connects and both report
    /// <see cref="TransportCloseReason.Transport"/> (MsQuic: the client rejects the server certificate; the simulator:
    /// <see cref="Simulation.LinkOptions.FailHandshake"/>). Default false.
    /// </summary>
    public bool FailHandshake { get; set; }
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
