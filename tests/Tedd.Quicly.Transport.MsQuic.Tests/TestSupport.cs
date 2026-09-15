using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

/// <summary>
/// Every test class joins this collection so the loopback tests run one at a time: they share MsQuic worker
/// threads and the per-thread allocation probes and timing assertions need a quiet machine.
/// </summary>
[CollectionDefinition(Name)]
public class MsQuicCollection
{
    public const string Name = "MsQuic";
}

internal static class TestTimeouts
{
    public static readonly TimeSpan Default = TimeSpan.FromSeconds(10);

    public static Task<T> Within<T>(this Task<T> task) => task.WaitAsync(Default);
    public static Task<T> Within<T>(this TaskCompletionSource<T> tcs) => tcs.Task.WaitAsync(Default);

    public static TaskCompletionSource<T> NewTcs<T>() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Waits up to <paramref name="timeout"/> for <paramref name="condition"/>; returns whether it became true.</summary>
    public static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) return false;
            await Task.Delay(10);
        }
        return true;
    }
}


internal delegate void SpanHandler(ReadOnlySpan<byte> data);

/// <summary>Records every connection event into awaitable slots.</summary>
internal sealed unsafe class ConnectionRecorder : IMsQuicConnectionEvents
{
    public readonly TaskCompletionSource<(string Alpn, bool Resumed)> ConnectedTcs = TestTimeouts.NewTcs<(string, bool)>();
    public readonly TaskCompletionSource<(int Status, ulong ErrorCode)> TransportShutdownTcs = TestTimeouts.NewTcs<(int, ulong)>();
    public readonly TaskCompletionSource<ulong> PeerShutdownTcs = TestTimeouts.NewTcs<ulong>();
    public readonly TaskCompletionSource<(bool HandshakeCompleted, bool PeerAcknowledged, bool AppCloseInProgress)> ShutdownCompleteTcs = TestTimeouts.NewTcs<(bool, bool, bool)>();
    public readonly TaskCompletionSource<(bool Enabled, ushort MaxLength)> DatagramSendEnabledTcs = TestTimeouts.NewTcs<(bool, ushort)>();
    public readonly TaskCompletionSource<MsQuicStream> FirstPeerStreamTcs = TestTimeouts.NewTcs<MsQuicStream>();
    public readonly TaskCompletionSource<(ushort Bidi, ushort Unidi)> StreamsAvailableTcs = TestTimeouts.NewTcs<(ushort, ushort)>();
    public readonly TaskCompletionSource<bool> PeerNeedsStreamsTcs = TestTimeouts.NewTcs<bool>();
    public readonly TaskCompletionSource<bool> CertificateReceivedTcs = TestTimeouts.NewTcs<bool>();
    public readonly ConcurrentBag<MsQuicStream> PeerStreams = [];
    public readonly ConcurrentQueue<byte[]> Datagrams = [];
    public readonly ConcurrentQueue<(ushort Bidi, ushort Unidi)> StreamsAvailableHistory = [];

    public int DatagramsReceived;
    public int FinalSendStates;
    public QUIC_DATAGRAM_SEND_STATE[]? SendStates;
    public byte[]? CertificateDer;
    public byte[]? ChainPkcs7;
    public int CertificateDeferredStatus;
    public bool CertificateCanDefer;
    public bool CertificateIsPortable;
    public MsQuicCertificateDecision CertificateDecision = MsQuicCertificateDecision.Accept;
    public bool AcceptPeerStreams = true;
    public bool KeepDatagrams = true;
    public bool InsideCallbackSeen;
    public Func<MsQuicStream, IMsQuicStreamEvents>? PeerStreamEventsFactory;
    public SpanHandler? OnDatagram;
    public Action<MsQuicConnection>? OnShutdownComplete;
    public MsQuicConnection? Connection;

    public void Connected(MsQuicConnection connection, ReadOnlySpan<byte> negotiatedAlpn, bool sessionResumed)
    {
        Connection = connection;
        InsideCallbackSeen = MsQuicCallbackScope.IsInsideCallback;
        ConnectedTcs.TrySetResult((Encoding.ASCII.GetString(negotiatedAlpn), sessionResumed));
    }

    public void ShutdownInitiatedByTransport(MsQuicConnection connection, int status, ulong errorCode) => TransportShutdownTcs.TrySetResult((status, errorCode));

    public void ShutdownInitiatedByPeer(MsQuicConnection connection, ulong errorCode) => PeerShutdownTcs.TrySetResult(errorCode);

    public void ShutdownComplete(MsQuicConnection connection, bool handshakeCompleted, bool peerAcknowledgedShutdown, bool appCloseInProgress)
    {
        // Run the hook first so a test awaiting the TCS observes its effects.
        OnShutdownComplete?.Invoke(connection);
        ShutdownCompleteTcs.TrySetResult((handshakeCompleted, peerAcknowledgedShutdown, appCloseInProgress));
    }

    public bool ThrowOnPeerStream;
    private readonly Lock _streamsLock = new();
    private bool _closing;

    public bool PeerStreamStarted(MsQuicConnection connection, MsQuicStream stream, QUIC_STREAM_OPEN_FLAGS flags)
    {
        if (ThrowOnPeerStream) throw new InvalidOperationException("no streams for you");
        lock (_streamsLock)
        {
            // Once cleanup has begun, late peer streams (e.g. the RESET_STREAM of a stream the other side just closed)
            // are rejected, which makes the wrapper close them; accepting one now would leak it past cleanup.
            if (!AcceptPeerStreams || _closing) return false;
            stream.Events = PeerStreamEventsFactory?.Invoke(stream) ?? new StreamRecorder();
            PeerStreams.Add(stream);
        }
        FirstPeerStreamTcs.TrySetResult(stream);
        return true;
    }

    /// <summary>Stops accepting peer streams and returns every stream accepted so far (the set is final afterwards).</summary>
    public MsQuicStream[] BeginClosing()
    {
        lock (_streamsLock)
        {
            _closing = true;
            return PeerStreams.ToArray();
        }
    }

    public void StreamsAvailable(MsQuicConnection connection, ushort bidirectionalCount, ushort unidirectionalCount)
    {
        StreamsAvailableHistory.Enqueue((bidirectionalCount, unidirectionalCount));
        StreamsAvailableTcs.TrySetResult((bidirectionalCount, unidirectionalCount));
    }

    public void PeerNeedsStreams(MsQuicConnection connection, bool bidirectional) => PeerNeedsStreamsTcs.TrySetResult(bidirectional);

    public void DatagramStateChanged(MsQuicConnection connection, bool sendEnabled, ushort maxSendLength)
    {
        if (sendEnabled) DatagramSendEnabledTcs.TrySetResult((sendEnabled, maxSendLength));
    }

    public void DatagramReceived(MsQuicConnection connection, ReadOnlySpan<byte> data, QUIC_RECEIVE_FLAGS flags)
    {
        Interlocked.Increment(ref DatagramsReceived);
        OnDatagram?.Invoke(data);
        if (KeepDatagrams) Datagrams.Enqueue(data.ToArray());
    }

    public void DatagramSendStateChanged(MsQuicConnection connection, void* clientContext, QUIC_DATAGRAM_SEND_STATE state)
    {
        if (!QuicDatagramSendState.IsFinal(state)) return;
        nint index = (nint)clientContext - 1;
        if (SendStates is not null && index >= 0 && index < SendStates.Length && SendStates[index] == QUIC_DATAGRAM_SEND_STATE.UNKNOWN)
        {
            SendStates[index] = state;
            Interlocked.Increment(ref FinalSendStates);
        }
    }

    public MsQuicCertificateDecision PeerCertificateReceived(MsQuicConnection connection, in MsQuicPeerCertificateInfo info)
    {
        CertificateDer = info.CertificateDer.ToArray();
        ChainPkcs7 = info.ChainPkcs7.ToArray();
        CertificateDeferredStatus = info.DeferredStatus;
        CertificateCanDefer = info.CanDefer;
        CertificateIsPortable = info.IsPortable;
        CertificateReceivedTcs.TrySetResult(true);
        return CertificateDecision;
    }
}

/// <summary>Records stream events; by default accumulates received bytes and consumes everything.</summary>
internal sealed unsafe class StreamRecorder : IMsQuicStreamEvents
{
    public readonly TaskCompletionSource<(int Status, ulong Id, bool PeerAccepted)> StartCompleteTcs = TestTimeouts.NewTcs<(int, ulong, bool)>();
    public readonly TaskCompletionSource<bool> FinTcs = TestTimeouts.NewTcs<bool>();
    public readonly TaskCompletionSource<bool> PeerSendShutdownTcs = TestTimeouts.NewTcs<bool>();
    public readonly TaskCompletionSource<ulong> PeerSendAbortedTcs = TestTimeouts.NewTcs<ulong>();
    public readonly TaskCompletionSource<ulong> PeerReceiveAbortedTcs = TestTimeouts.NewTcs<ulong>();
    public readonly TaskCompletionSource<bool> SendShutdownCompleteTcs = TestTimeouts.NewTcs<bool>();
    public readonly TaskCompletionSource<MsQuicStreamShutdownInfo> ShutdownCompleteTcs = TestTimeouts.NewTcs<MsQuicStreamShutdownInfo>();
    public readonly TaskCompletionSource<ulong> IdealSendBufferSizeTcs = TestTimeouts.NewTcs<ulong>();
    public readonly TaskCompletionSource<bool> PeerAcceptedTcs = TestTimeouts.NewTcs<bool>();
    public readonly MemoryStream Received = new();
    public readonly ConcurrentQueue<(nint Context, bool Canceled)> SendCompletes = [];
    public readonly TaskCompletionSource<bool> AllSendsCompleteTcs = TestTimeouts.NewTcs<bool>();
    public int ExpectedSends = 1;
    public int ReceiveCount;
    public long ReceivedBytes;
    public Action<MsQuicStream>? OnShutdownComplete;

    public void StartComplete(MsQuicStream stream, int status, ulong id, bool peerAccepted) => StartCompleteTcs.TrySetResult((status, id, peerAccepted));

    public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
    {
        Interlocked.Increment(ref ReceiveCount);
        lock (Received)
        {
            for (uint i = 0; i < bufferCount; i++) Received.Write(buffers[i].Span);
        }
        Interlocked.Add(ref ReceivedBytes, (long)totalLength);
        if ((flags & QUIC_RECEIVE_FLAGS.FIN) != 0) FinTcs.TrySetResult(true);
        return MsQuicReceiveResult.Consumed(totalLength);
    }

    public void SendComplete(MsQuicStream stream, void* clientContext, bool canceled)
    {
        SendCompletes.Enqueue(((nint)clientContext, canceled));
        if (SendCompletes.Count >= ExpectedSends) AllSendsCompleteTcs.TrySetResult(true);
    }

    public void PeerSendShutdown(MsQuicStream stream) => PeerSendShutdownTcs.TrySetResult(true);
    public void PeerSendAborted(MsQuicStream stream, ulong errorCode) => PeerSendAbortedTcs.TrySetResult(errorCode);
    public void PeerReceiveAborted(MsQuicStream stream, ulong errorCode) => PeerReceiveAbortedTcs.TrySetResult(errorCode);
    public void SendShutdownComplete(MsQuicStream stream, bool graceful) => SendShutdownCompleteTcs.TrySetResult(graceful);

    public void ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info)
    {
        OnShutdownComplete?.Invoke(stream);
        ShutdownCompleteTcs.TrySetResult(info);
    }

    public void IdealSendBufferSize(MsQuicStream stream, ulong byteCount) => IdealSendBufferSizeTcs.TrySetResult(byteCount);
    public void PeerAccepted(MsQuicStream stream) => PeerAcceptedTcs.TrySetResult(true);
}

/// <summary>A registration + server configuration + listener on 127.0.0.1:ephemeral, with tracking so Dispose never hangs.</summary>
internal sealed class Loopback : IMsQuicListenerEvents, IDisposable
{
    public const string Alpn = "quicly-test";

    public readonly MsQuicRegistration Registration;
    public readonly X509Certificate2 Certificate;
    public readonly MsQuicConfiguration ServerConfiguration;
    public readonly MsQuicListener Listener;
    public readonly IPEndPoint EndPoint;
    public readonly ConcurrentBag<MsQuicConnection> Accepted = [];
    public readonly TaskCompletionSource<MsQuicConnection> FirstAcceptedTcs = TestTimeouts.NewTcs<MsQuicConnection>();
    public readonly TaskCompletionSource<bool> StopCompleteTcs = TestTimeouts.NewTcs<bool>();
    public readonly List<string> LastClientAlpns = [];
    public string? LastServerName;
    public string? LastNegotiatedAlpn;
    public IPEndPoint? LastRemoteEndPoint;
    public uint LastQuicVersion;
    public IPEndPoint? LastLocalEndPoint;
    public int LastCryptoBufferLength;
    public bool LastRawInfoAvailable;
    public bool LastNegotiatedDefaultAlpn;
    public bool RejectConnections;
    public Func<MsQuicConnection, IMsQuicConnectionEvents> ServerEventsFactory = static _ => new ConnectionRecorder();
    /// <summary>Picks the configuration for a negotiated ALPN; null falls back to <see cref="ServerConfiguration"/>.</summary>
    public Func<string, MsQuicConfiguration?>? SelectConfiguration;

    private readonly List<MsQuicConfiguration> _configurations = [];
    private readonly List<MsQuicConnection> _clientConnections = [];
    private readonly ConcurrentBag<MsQuicStream> _streams = [];

    /// <summary>Test posture: generous stream limits on both sides so the tests can open streams freely.</summary>
    public static MsQuicSettings TestServerSettings() => new() { PeerBidiStreamCount = 16, PeerUnidiStreamCount = 16 };

    public static MsQuicSettings TestClientSettings() => new() { PeerBidiStreamCount = 16, PeerUnidiStreamCount = 16, KeepAliveInterval = TimeSpan.FromSeconds(10) };

    public Loopback(MsQuicSettings? serverSettings = null, MsQuicServerCredentialMode credentialMode = MsQuicServerCredentialMode.Auto, string[]? listenerAlpns = null)
    {
        Registration = new MsQuicRegistration("quicly-tests");
        Certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        MsQuicConfiguration? configuration = null;
        MsQuicListener? listener = null;
        try
        {
            configuration = MsQuicConfiguration.CreateServer(Registration, [Alpn], Certificate, serverSettings ?? TestServerSettings(), credentialMode);
            listener = new MsQuicListener(Registration, this);
            listener.Start(new IPEndPoint(IPAddress.Loopback, 0), listenerAlpns ?? [Alpn]);
            EndPoint = listener.LocalEndPoint;
        }
        catch
        {
            listener?.Close();
            configuration?.Close();
            TestRegistration.CloseWithDeadline(Registration);
            Certificate.Dispose();
            throw;
        }
        ServerConfiguration = configuration;
        Listener = listener;
    }

    public MsQuicConfiguration CreateServerConfiguration(string alpn, MsQuicSettings? settings = null)
    {
        var config = MsQuicConfiguration.CreateServer(Registration, [alpn], Certificate, settings ?? TestServerSettings());
        _configurations.Add(config);
        return config;
    }

    public MsQuicConfiguration CreateClientConfiguration(MsQuicCertificateValidation validation = MsQuicCertificateValidation.InsecureSkipValidation, string alpn = Alpn, MsQuicSettings? settings = null)
    {
        var config = MsQuicConfiguration.CreateClient(Registration, [alpn], validation, settings ?? TestClientSettings());
        _configurations.Add(config);
        return config;
    }

    /// <summary>
    /// Starts a client connection to the listener. <paramref name="serverName"/> defaults to the IP literal (no SNI
    /// is sent for IP literals, RFC 6066); pass "localhost" to exercise SNI (INET keeps the resolution on IPv4).
    /// </summary>
    public MsQuicConnection Connect(IMsQuicConnectionEvents events, MsQuicConfiguration? configuration = null, string? serverName = null)
    {
        configuration ??= CreateClientConfiguration();
        var connection = new MsQuicConnection(Registration, events);
        _clientConnections.Add(connection);
        MsQuicException.ThrowIfFailed(connection.Start(configuration, serverName ?? EndPoint.Address.ToString(), (ushort)EndPoint.Port, QuicAddressFamily.INET), "ConnectionStart");
        return connection;
    }

    public async Task<(MsQuicConnection Client, ConnectionRecorder ClientEvents, MsQuicConnection Server, ConnectionRecorder ServerEvents)> ConnectPairAsync(MsQuicConfiguration? clientConfiguration = null, string expectedAlpn = Alpn, string? serverName = null)
    {
        var clientEvents = new ConnectionRecorder();
        MsQuicConnection client = Connect(clientEvents, clientConfiguration, serverName);
        (string alpn, _) = await clientEvents.ConnectedTcs.Within();
        Assert.Equal(expectedAlpn, alpn);
        MsQuicConnection server = await FirstAcceptedTcs.Within();
        var serverEvents = (ConnectionRecorder)server.Events;
        (alpn, _) = await serverEvents.ConnectedTcs.Within();
        Assert.Equal(expectedAlpn, alpn);
        return (client, clientEvents, server, serverEvents);
    }

    public MsQuicStream Track(MsQuicStream stream)
    {
        _streams.Add(stream);
        return stream;
    }

    public unsafe MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, in MsQuicNewConnectionInfo info)
    {
        LastServerName = Encoding.UTF8.GetString(info.ServerName);
        LastNegotiatedAlpn = Encoding.ASCII.GetString(info.NegotiatedAlpn);
        LastRemoteEndPoint = info.RemoteAddress.ToIPEndPoint();
        LastLocalEndPoint = info.LocalAddress.ToIPEndPoint();
        LastQuicVersion = info.QuicVersion;
        LastCryptoBufferLength = info.CryptoBuffer.Length;
        LastRawInfoAvailable = info.Raw != null;
        LastNegotiatedDefaultAlpn = info.NegotiatedAlpnIs("quicly-test"u8);
        lock (LastClientAlpns)
        {
            LastClientAlpns.Clear();
            ReadOnlySpan<byte> list = info.ClientAlpnList;
            while (!list.IsEmpty)
            {
                int len = list[0];
                LastClientAlpns.Add(Encoding.ASCII.GetString(list.Slice(1, len)));
                list = list[(1 + len)..];
            }
        }
        if (RejectConnections) return null;
        MsQuicConfiguration? configuration = SelectConfiguration?.Invoke(LastNegotiatedAlpn) ?? ServerConfiguration;
        IMsQuicConnectionEvents events = ServerEventsFactory(connection);
        connection.Events = events;
        Accepted.Add(connection);
        FirstAcceptedTcs.TrySetResult(connection);
        return configuration;
    }

    public void StopComplete(MsQuicListener listener, bool appCloseInProgress) => StopCompleteTcs.TrySetResult(true);

    public void Dispose()
    {
        // Freeze the peer-stream sets on every connection first, then close streams, then connections.
        var peerStreams = new List<MsQuicStream>();
        foreach (MsQuicConnection c in Accepted.Concat(_clientConnections))
        {
            if (c.Events is ConnectionRecorder r) peerStreams.AddRange(r.BeginClosing());
        }
        foreach (MsQuicStream s in peerStreams) s.Close();
        foreach (MsQuicStream s in _streams) s.Close();
        foreach (MsQuicConnection c in _clientConnections) c.Close();
        foreach (MsQuicConnection c in Accepted) c.Close();
        Listener.Close();
        foreach (MsQuicConfiguration c in _configurations) c.Close();
        ServerConfiguration.Close();
        TestRegistration.CloseWithDeadline(Registration);
        Certificate.Dispose();
    }
}

/// <summary>
/// A registration for a test. <c>RegistrationClose</c> blocks until every child handle is closed, so a test that
/// fails before closing a listener or connection would otherwise hang the whole run; this closes it on a
/// dedicated thread with a deadline and turns a leak into a test failure instead.
/// </summary>
internal sealed class TestRegistration : IDisposable
{
    public static readonly TimeSpan CloseDeadline = TimeSpan.FromSeconds(20);

    public MsQuicRegistration Registration { get; }

    public TestRegistration(string? appName = null, QUIC_EXECUTION_PROFILE profile = QUIC_EXECUTION_PROFILE.LOW_LATENCY) => Registration = new MsQuicRegistration(appName, profile);

    public static void CloseWithDeadline(MsQuicRegistration registration)
    {
        if (registration.IsClosed) return;
        var thread = new Thread(registration.Close) { IsBackground = true, Name = "RegistrationClose" };
        thread.Start();
        if (!thread.Join(CloseDeadline))
        {
            throw new TimeoutException("RegistrationClose did not return: a listener, connection, stream or configuration handle was leaked by the test. " + PerfCounterSummary());
        }
    }

    /// <summary>Process-wide MsQuic object counters, for diagnosing which kind of handle leaked.</summary>
    public static unsafe string PerfCounterSummary()
    {
        const int count = (int)QUIC_PERFORMANCE_COUNTERS.MAX;
        long* counters = stackalloc long[count];
        uint length = (uint)(count * sizeof(long));
        int status = MsQuicApi.Instance.GetParam(null, MsQuicParam.QUIC_PARAM_GLOBAL_PERF_COUNTERS, &length, counters);
        if (MsQuicStatus.Failed(status)) return "perf counters unavailable: " + MsQuicStatus.GetName(status);
        return $"CONN_ACTIVE={counters[(int)QUIC_PERFORMANCE_COUNTERS.CONN_ACTIVE]} STRM_ACTIVE={counters[(int)QUIC_PERFORMANCE_COUNTERS.STRM_ACTIVE]} CONN_CREATED={counters[(int)QUIC_PERFORMANCE_COUNTERS.CONN_CREATED]} CONN_QUEUE_DEPTH={counters[(int)QUIC_PERFORMANCE_COUNTERS.CONN_QUEUE_DEPTH]} CONN_OPER_QUEUE_DEPTH={counters[(int)QUIC_PERFORMANCE_COUNTERS.CONN_OPER_QUEUE_DEPTH]}";
    }

    public void Dispose() => CloseWithDeadline(Registration);
}

/// <summary>Native scratch memory with a deterministic byte pattern, for gathered sends.</summary>
internal sealed unsafe class NativeBlock : IDisposable
{
    public byte* Pointer { get; private set; }
    public int Length { get; }

    public NativeBlock(int length)
    {
        Length = length;
        Pointer = (byte*)System.Runtime.InteropServices.NativeMemory.Alloc((nuint)length);
    }

    public Span<byte> Span => new(Pointer, Length);

    public static byte PatternAt(long offset) => (byte)((offset * 31 + 7) & 0xFF);

    public void FillPattern(long baseOffset)
    {
        for (int i = 0; i < Length; i++) Pointer[i] = PatternAt(baseOffset + i);
    }

    public void CopyFrom(ReadOnlySpan<byte> data) => data.CopyTo(Span);

    public string AsciiString() => Encoding.ASCII.GetString(Span);

    public void WriteInt32(int offset, int value) => System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(Span[offset..], value);

    public void Dispose()
    {
        if (Pointer != null)
        {
            System.Runtime.InteropServices.NativeMemory.Free(Pointer);
            Pointer = null;
        }
    }
}

/// <summary>
/// A native <c>QUIC_BUFFER[]</c> so that async tests never hold pointers across awaits; the array stays valid
/// until the matching completion, as MsQuic requires.
/// </summary>
internal sealed unsafe class NativeBuffers : IDisposable
{
    public QUIC_BUFFER* Buffers { get; private set; }
    public uint Count { get; }

    public NativeBuffers(int count)
    {
        Count = (uint)count;
        Buffers = (QUIC_BUFFER*)System.Runtime.InteropServices.NativeMemory.AllocZeroed((nuint)(count * sizeof(QUIC_BUFFER)));
    }

    /// <summary>One buffer covering the whole block.</summary>
    public static NativeBuffers Single(NativeBlock block)
    {
        var buffers = new NativeBuffers(1);
        buffers.Set(0, block, 0, block.Length);
        return buffers;
    }

    public void Set(int index, NativeBlock block, int offset, int length) => Buffers[index] = new QUIC_BUFFER(block.Pointer + offset, (uint)length);

    public int SendOn(MsQuicStream stream, QUIC_SEND_FLAGS flags, nint context) => stream.Send(Buffers, Count, flags, (void*)context);

    public int SendDatagramOn(MsQuicConnection connection, QUIC_SEND_FLAGS flags, nint context) => connection.SendDatagram(Buffers, Count, flags, (void*)context);

    /// <summary>Sends entry <paramref name="index"/> as its own datagram.</summary>
    public int SendDatagramOn(MsQuicConnection connection, int index, QUIC_SEND_FLAGS flags, nint context) => connection.SendDatagram(Buffers + index, 1, flags, (void*)context);

    public void Dispose()
    {
        if (Buffers != null)
        {
            System.Runtime.InteropServices.NativeMemory.Free(Buffers);
            Buffers = null;
        }
    }
}

internal static unsafe class RawCredentials
{
    /// <summary>Loads a raw client credential with exactly <paramref name="flags"/>.</summary>
    public static int LoadClient(MsQuicConfiguration configuration, QUIC_CREDENTIAL_FLAGS flags)
    {
        QUIC_CREDENTIAL_CONFIG cred = default;
        cred.Type = QUIC_CREDENTIAL_TYPE.NONE;
        cred.Flags = flags;
        return configuration.LoadCredential(&cred);
    }

    /// <summary>Loads a credential of an invalid type (MsQuic must refuse it).</summary>
    public static int LoadInvalid(MsQuicConfiguration configuration)
    {
        QUIC_CREDENTIAL_CONFIG cred = default;
        cred.Type = (QUIC_CREDENTIAL_TYPE)999;
        return configuration.LoadCredential(&cred);
    }
}

/// <summary>
/// Measures managed allocations attributed to the MsQuic worker thread between two consecutive samples taken on
/// that same thread. Any allocation in the wrapper's callback path (or the handler) shows up as a positive delta.
/// </summary>
internal sealed class CallbackAllocationProbe
{
    private int _lastThread;
    private long _lastAllocated;
    private bool _armed;
    public long Allocated;
    public int Samples;

    public void Arm()
    {
        _armed = true;
        _lastThread = 0;
    }

    public void Sample()
    {
        long now = GC.GetAllocatedBytesForCurrentThread();
        int thread = Environment.CurrentManagedThreadId;
        if (_armed && thread == _lastThread)
        {
            Allocated += now - _lastAllocated;
            Samples++;
        }
        _lastThread = thread;
        _lastAllocated = now;
    }
}

internal static class TestStatus
{
    /// <summary>
    /// StreamStart, StreamSend and DatagramSend answer QUIC_STATUS_PENDING when they queue the work (the normal case
    /// off the worker thread) and QUIC_STATUS_SUCCESS when they complete inline; both mean "accepted".
    /// </summary>
    public static void AssertAccepted(int status) =>
        Assert.True(status == MsQuicStatus.QUIC_STATUS_SUCCESS || status == MsQuicStatus.QUIC_STATUS_PENDING, MsQuicStatus.GetName(status));
}
