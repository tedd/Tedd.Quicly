using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

// Loopback tests share MsQuic worker threads; run them serially so per-thread allocation probes and timing stay deterministic.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Tedd.Quicly.Transport.MsQuic.Tests;

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

internal unsafe delegate MsQuicReceiveResult ReceiveHandler(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags);

internal delegate void SpanHandler(ReadOnlySpan<byte> data);

/// <summary>Records every connection event into awaitable slots.</summary>
internal sealed unsafe class ConnectionRecorder : IMsQuicConnectionEvents
{
    public readonly TaskCompletionSource<(string Alpn, bool Resumed)> Connected = TestTimeouts.NewTcs<(string, bool)>();
    public readonly TaskCompletionSource<(int Status, ulong ErrorCode)> TransportShutdown = TestTimeouts.NewTcs<(int, ulong)>();
    public readonly TaskCompletionSource<ulong> PeerShutdown = TestTimeouts.NewTcs<ulong>();
    public readonly TaskCompletionSource<(bool HandshakeCompleted, bool PeerAcknowledged, bool AppCloseInProgress)> ShutdownComplete = TestTimeouts.NewTcs<(bool, bool, bool)>();
    public readonly TaskCompletionSource<(bool Enabled, ushort MaxLength)> DatagramSendEnabled = TestTimeouts.NewTcs<(bool, ushort)>();
    public readonly TaskCompletionSource<MsQuicStream> FirstPeerStream = TestTimeouts.NewTcs<MsQuicStream>();
    public readonly TaskCompletionSource<(ushort Bidi, ushort Unidi)> StreamsAvailable = TestTimeouts.NewTcs<(ushort, ushort)>();
    public readonly ConcurrentBag<MsQuicStream> PeerStreams = [];
    public readonly ConcurrentQueue<byte[]> Datagrams = [];

    public int DatagramsReceived;
    public int FinalSendStates;
    public QUIC_DATAGRAM_SEND_STATE[]? SendStates;
    public X509Certificate2? ReceivedCertificate;
    public bool AcceptPeerStreams = true;
    public bool AcceptCertificate = true;
    public bool KeepDatagrams = true;
    public Func<MsQuicStream, IMsQuicStreamEvents>? PeerStreamEventsFactory;
    public SpanHandler? OnDatagram;
    public MsQuicConnection? Connection;

    public void Connected(MsQuicConnection connection, ReadOnlySpan<byte> negotiatedAlpn, bool sessionResumed)
    {
        Connection = connection;
        this.Connected.TrySetResult((Encoding.ASCII.GetString(negotiatedAlpn), sessionResumed));
    }

    public void ShutdownInitiatedByTransport(MsQuicConnection connection, int status, ulong errorCode) => TransportShutdown.TrySetResult((status, errorCode));

    public void ShutdownInitiatedByPeer(MsQuicConnection connection, ulong errorCode) => PeerShutdown.TrySetResult(errorCode);

    public void ShutdownComplete(MsQuicConnection connection, bool handshakeCompleted, bool peerAcknowledgedShutdown, bool appCloseInProgress)
        => this.ShutdownComplete.TrySetResult((handshakeCompleted, peerAcknowledgedShutdown, appCloseInProgress));

    public bool PeerStreamStarted(MsQuicConnection connection, MsQuicStream stream, QUIC_STREAM_OPEN_FLAGS flags)
    {
        if (!AcceptPeerStreams) return false;
        stream.Events = PeerStreamEventsFactory?.Invoke(stream) ?? new StreamRecorder();
        PeerStreams.Add(stream);
        FirstPeerStream.TrySetResult(stream);
        return true;
    }

    public void StreamsAvailable(MsQuicConnection connection, ushort bidirectionalCount, ushort unidirectionalCount) => this.StreamsAvailable.TrySetResult((bidirectionalCount, unidirectionalCount));

    public void DatagramStateChanged(MsQuicConnection connection, bool sendEnabled, ushort maxSendLength)
    {
        if (sendEnabled) DatagramSendEnabled.TrySetResult((sendEnabled, maxSendLength));
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

    public bool PeerCertificateReceived(MsQuicConnection connection, X509Certificate2? certificate, uint deferredErrorFlags, int deferredStatus)
    {
        ReceivedCertificate = certificate;
        return AcceptCertificate;
    }
}

/// <summary>Records stream events; by default accumulates received bytes and consumes everything.</summary>
internal sealed unsafe class StreamRecorder : IMsQuicStreamEvents
{
    public readonly TaskCompletionSource<(int Status, ulong Id, bool PeerAccepted)> StartComplete = TestTimeouts.NewTcs<(int, ulong, bool)>();
    public readonly TaskCompletionSource<bool> Fin = TestTimeouts.NewTcs<bool>();
    public readonly TaskCompletionSource<bool> PeerSendShutdown = TestTimeouts.NewTcs<bool>();
    public readonly TaskCompletionSource<ulong> PeerSendAborted = TestTimeouts.NewTcs<ulong>();
    public readonly TaskCompletionSource<ulong> PeerReceiveAborted = TestTimeouts.NewTcs<ulong>();
    public readonly TaskCompletionSource<bool> SendShutdownComplete = TestTimeouts.NewTcs<bool>();
    public readonly TaskCompletionSource<MsQuicStreamShutdownInfo> ShutdownComplete = TestTimeouts.NewTcs<MsQuicStreamShutdownInfo>();
    public readonly TaskCompletionSource<ulong> IdealSendBufferSize = TestTimeouts.NewTcs<ulong>();
    public readonly TaskCompletionSource<bool> PeerAccepted = TestTimeouts.NewTcs<bool>();
    public readonly MemoryStream Received = new();
    public readonly ConcurrentQueue<(nint Context, bool Canceled)> SendCompletes = [];
    public readonly TaskCompletionSource<bool> AllSendsComplete = TestTimeouts.NewTcs<bool>();
    public int ExpectedSends = 1;
    public int ReceiveCount;
    public long ReceivedBytes;
    public ReceiveHandler? OnReceive;
    public Action<MsQuicStream, nint, bool>? OnSendComplete;

    public void StartComplete(MsQuicStream stream, int status, ulong id, bool peerAccepted) => this.StartComplete.TrySetResult((status, id, peerAccepted));

    public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
    {
        Interlocked.Increment(ref ReceiveCount);
        if (OnReceive is not null) return OnReceive(stream, buffers, bufferCount, absoluteOffset, totalLength, flags);
        lock (Received)
        {
            for (uint i = 0; i < bufferCount; i++) Received.Write(buffers[i].Span);
        }
        Interlocked.Add(ref ReceivedBytes, (long)totalLength);
        if ((flags & QUIC_RECEIVE_FLAGS.FIN) != 0) Fin.TrySetResult(true);
        return MsQuicReceiveResult.Consumed(totalLength);
    }

    public void SendComplete(MsQuicStream stream, void* clientContext, bool canceled)
    {
        SendCompletes.Enqueue(((nint)clientContext, canceled));
        OnSendComplete?.Invoke(stream, (nint)clientContext, canceled);
        if (SendCompletes.Count >= ExpectedSends) AllSendsComplete.TrySetResult(true);
    }

    public void PeerSendShutdown(MsQuicStream stream) => this.PeerSendShutdown.TrySetResult(true);
    public void PeerSendAborted(MsQuicStream stream, ulong errorCode) => this.PeerSendAborted.TrySetResult(errorCode);
    public void PeerReceiveAborted(MsQuicStream stream, ulong errorCode) => this.PeerReceiveAborted.TrySetResult(errorCode);
    public void SendShutdownComplete(MsQuicStream stream, bool graceful) => this.SendShutdownComplete.TrySetResult(graceful);
    public void ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info) => this.ShutdownComplete.TrySetResult(info);
    public void IdealSendBufferSize(MsQuicStream stream, ulong byteCount) => this.IdealSendBufferSize.TrySetResult(byteCount);
    public void PeerAccepted(MsQuicStream stream) => this.PeerAccepted.TrySetResult(true);
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
    public readonly TaskCompletionSource<MsQuicConnection> FirstAccepted = TestTimeouts.NewTcs<MsQuicConnection>();
    public readonly TaskCompletionSource<bool> StopComplete = TestTimeouts.NewTcs<bool>();
    public readonly List<string> LastClientAlpns = [];
    public string? LastServerName;
    public bool RejectConnections;
    public Func<MsQuicConnection, IMsQuicConnectionEvents> ServerEventsFactory = static _ => new ConnectionRecorder();

    private readonly List<MsQuicConfiguration> _clientConfigurations = [];
    private readonly List<MsQuicConnection> _clientConnections = [];
    private readonly ConcurrentBag<MsQuicStream> _streams = [];
    private readonly ConcurrentBag<ConnectionRecorder> _recorders = [];

    public Loopback(MsQuicSettings? serverSettings = null, MsQuicServerCredentialMode credentialMode = MsQuicServerCredentialMode.Auto)
    {
        Registration = new MsQuicRegistration("quicly-tests");
        Certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        ServerConfiguration = MsQuicConfiguration.CreateServer(Registration, [Alpn], Certificate, serverSettings, credentialMode);
        Listener = new MsQuicListener(Registration, this);
        Listener.Start(new IPEndPoint(IPAddress.Loopback, 0), [Alpn]);
        EndPoint = Listener.LocalEndPoint;
    }

    public MsQuicConfiguration CreateClientConfiguration(MsQuicCertificateValidation validation = MsQuicCertificateValidation.InsecureSkipValidation, string alpn = Alpn, MsQuicSettings? settings = null)
    {
        var config = MsQuicConfiguration.CreateClient(Registration, [alpn], validation, settings);
        _clientConfigurations.Add(config);
        return config;
    }

    public MsQuicConnection Connect(IMsQuicConnectionEvents events, MsQuicConfiguration? configuration = null)
    {
        configuration ??= CreateClientConfiguration();
        var connection = new MsQuicConnection(Registration, events);
        _clientConnections.Add(connection);
        if (events is ConnectionRecorder recorder) _recorders.Add(recorder);
        MsQuicException.ThrowIfFailed(connection.Start(configuration, EndPoint.Address.ToString(), (ushort)EndPoint.Port, QuicAddressFamily.INET), "ConnectionStart");
        return connection;
    }

    public async Task<(MsQuicConnection Client, ConnectionRecorder ClientEvents, MsQuicConnection Server, ConnectionRecorder ServerEvents)> ConnectPairAsync(MsQuicConfiguration? clientConfiguration = null)
    {
        var clientEvents = new ConnectionRecorder();
        MsQuicConnection client = Connect(clientEvents, clientConfiguration);
        (string alpn, _) = await clientEvents.Connected.Within();
        Assert.Equal(Alpn, alpn);
        MsQuicConnection server = await FirstAccepted.Within();
        var serverEvents = (ConnectionRecorder)server.Events;
        (alpn, _) = await serverEvents.Connected.Within();
        Assert.Equal(Alpn, alpn);
        return (client, clientEvents, server, serverEvents);
    }

    public MsQuicStream Track(MsQuicStream stream)
    {
        _streams.Add(stream);
        return stream;
    }

    public unsafe MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, ref QUIC_NEW_CONNECTION_INFO info)
    {
        LastServerName = Encoding.UTF8.GetString(info.ServerNameSpan);
        lock (LastClientAlpns)
        {
            LastClientAlpns.Clear();
            ReadOnlySpan<byte> list = new(info.ClientAlpnList, info.ClientAlpnListLength);
            while (!list.IsEmpty)
            {
                int len = list[0];
                LastClientAlpns.Add(Encoding.ASCII.GetString(list.Slice(1, len)));
                list = list[(1 + len)..];
            }
        }
        if (RejectConnections) return null;
        IMsQuicConnectionEvents events = ServerEventsFactory(connection);
        connection.Events = events;
        if (events is ConnectionRecorder recorder) _recorders.Add(recorder);
        Accepted.Add(connection);
        FirstAccepted.TrySetResult(connection);
        return ServerConfiguration;
    }

    public void StopComplete(MsQuicListener listener, bool appCloseInProgress) => this.StopComplete.TrySetResult(true);

    public void Dispose()
    {
        foreach (ConnectionRecorder r in _recorders)
        {
            foreach (MsQuicStream s in r.PeerStreams) s.Close();
        }
        foreach (MsQuicStream s in _streams) s.Close();
        foreach (MsQuicConnection c in _clientConnections) c.Close();
        foreach (MsQuicConnection c in Accepted) c.Close();
        Listener.Close();
        foreach (MsQuicConfiguration c in _clientConfigurations) c.Close();
        ServerConfiguration.Close();
        Registration.Close();
        Certificate.Dispose();
    }
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

    public void Dispose()
    {
        if (Pointer != null)
        {
            System.Runtime.InteropServices.NativeMemory.Free(Pointer);
            Pointer = null;
        }
    }
}
