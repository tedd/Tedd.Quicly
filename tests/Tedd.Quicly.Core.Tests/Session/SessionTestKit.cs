using System.Buffers.Binary;
using System.Net;
using System.Text;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Channel tables shared by the session tests (immutable, so safe across parallel tests).</summary>
internal static class TestTables
{
    public static ChannelTable Default { get; } = ChannelTable.Create()
        .Add(2, "fx", ChannelMode.UnreliableUnordered)
        .Add(3, "move", ChannelMode.UnreliableSequenced, o => o.Keyed = true)
        .Add(4, "chat", ChannelMode.ReliableOrdered)
        .Add(5, "world", ChannelMode.Bulk)
        .Build();

    public static ChannelTable Other { get; } = ChannelTable.Create()
        .Add(2, "fx", ChannelMode.UnreliableUnordered)
        .Build();

    public static ChannelTable StreamsOnly { get; } = ChannelTable.Create()
        .Add(4, "chat", ChannelMode.ReliableOrdered)
        .Build();

    public static ChannelTable AllModes { get; } = ChannelTable.Create()
        .Add(2, "unordered", ChannelMode.UnreliableUnordered)
        .Add(3, "sequenced", ChannelMode.UnreliableSequenced)
        .Add(4, "ordered", ChannelMode.ReliableOrdered)
        .Add(5, "reliable-unordered", ChannelMode.ReliableUnordered)
        .Add(6, "latest", ChannelMode.ReliableLatest)
        .Add(7, "bulk", ChannelMode.Bulk)
        .Build();

    public static ChannelTable Plumbing { get; } = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
        .Add(3, "b", ChannelMode.UnreliableUnordered)
        .Add(6, "packed", ChannelMode.UnreliableUnordered, o =>
        {
            o.Compression = ChannelCompression.Lz4;
            o.MinCompressSize = 1;
        })
        .Add(8, "keyed", ChannelMode.UnreliableSequenced, o => o.Keyed = true)
        .Add(10, "stream", ChannelMode.ReliableOrdered)
        .Add(11, "group", ChannelMode.ReliableUnordered)
        .Build();
}

internal delegate AdmissionResult AdmitHandler(in HelloInfo hello, QuiclyPeer peer);

/// <summary>Admission policy under test control; accepts by default.</summary>
internal sealed class TestAdmission : IPeerAdmission
{
    public AdmitHandler? Handler;
    public int Calls;
    public byte[] LastAuthToken = [];
    public byte[] LastSessionToken = [];
    public HelloFlags LastFlags;
    public PeerCaps LastCaps;
    public ushort LastMaxReceiveDatagram;

    public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer)
    {
        Calls++;
        LastAuthToken = hello.AuthToken.ToArray();
        LastSessionToken = hello.SessionToken.ToArray();
        LastFlags = hello.Flags;
        LastCaps = hello.Caps;
        LastMaxReceiveDatagram = hello.MaxReceiveDatagram;
        return Handler is null ? AdmissionResult.Accept() : Handler(in hello, peer);
    }
}

/// <summary>A clock skewed (and optionally drifting) against another one.</summary>
internal sealed class OffsetClock(IClock inner, long offsetMicros, double rate = 1.0) : IClock
{
    public long NowMicros => (long)(inner.NowMicros * rate) + offsetMicros;
}

/// <summary>A simulated network with a listener; the derived fixtures add the endpoints.</summary>
internal abstract class SimFixture : IDisposable
{
    protected SimFixture(int seed)
    {
        Network = new SimulatedNetwork(Clock, seed);
        Listener = new SimulatedListener(Network);
    }

    public VirtualClock Clock { get; } = new();

    public SimulatedNetwork Network { get; }

    public SimulatedListener Listener { get; }

    public abstract void Pump();

    public void Run(long micros, long step = 1_000)
    {
        long end = Network.NowMicros + micros;
        Pump();
        while (Network.NowMicros < end)
        {
            Network.AdvanceTo(Math.Min(end, Network.NowMicros + step));
            Pump();
        }
    }

    public bool RunUntil(Func<bool> condition, long maxMicros = 10_000_000, long step = 1_000)
    {
        long end = Network.NowMicros + maxMicros;
        Pump();
        while (!condition())
        {
            if (Network.NowMicros >= end)
            {
                return false;
            }

            Network.AdvanceTo(Math.Min(end, Network.NowMicros + step));
            Pump();
        }

        return true;
    }

    protected abstract void DisposeEndpoints();

    public void Dispose()
    {
        DisposeEndpoints();
        // Deliver the transports' close callbacks so the peers free their native memory.
        Network.RunUntilIdle(60_000_000);
        Network.Dispose();
        Listener.Dispose();
    }
}

/// <summary>A client peer and a server peer over one simulated link.</summary>
internal sealed class SessionHarness : SimFixture
{
    private bool _clientDisposed;
    private bool _serverDisposed;

    public SessionHarness(LinkOptions? link = null, ChannelTable? table = null, ChannelTable? serverTable = null,
        Action<PeerOptions>? client = null, Action<PeerOptions>? server = null, byte[]? authToken = null, int seed = 1, bool connect = true,
        Func<ITransportConnector, ITransportConnector>? connector = null)
        : base(seed)
    {
        Table = table ?? TestTables.Default;
        ChannelTable tableOfServer = serverTable ?? Table;
        ServerOptions = new PeerOptions { Clock = Clock };
        server?.Invoke(ServerOptions);
        Listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo info) =>
        {
            QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, tableOfServer, ServerOptions, Admission);
            peer.StateChanged += (_, from, to) => ServerEvents.Add((from, to));
            Server = peer;
            return peer.TransportSink;
        });
        ClientOptions = new PeerOptions { Clock = Clock };
        client?.Invoke(ClientOptions);
        Connector = new SimulatedConnector(Network, link);
        Client = QuiclyPeer.Connect(connector?.Invoke(Connector) ?? Connector, Listener.LocalEndPoint, "test", Table, ClientOptions, authToken);
        Client.StateChanged += (_, from, to) => ClientEvents.Add((from, to));
        if (connect)
        {
            Assert.True(RunUntilConnected(), "The handshake did not complete.");
        }
    }

    public ChannelTable Table { get; }

    public PeerOptions ClientOptions { get; }

    public PeerOptions ServerOptions { get; }

    public SimulatedConnector Connector { get; }

    public TestAdmission Admission { get; } = new();

    public QuiclyPeer Client { get; }

    public QuiclyPeer? Server { get; private set; }

    public List<(PeerState From, PeerState To)> ClientEvents { get; } = [];

    public List<(PeerState From, PeerState To)> ServerEvents { get; } = [];

    public bool RunUntilConnected(long maxMicros = 10_000_000) =>
        RunUntil(() => Client.State == PeerState.Connected && Server?.State == PeerState.Connected, maxMicros);

    public bool RunUntilClosed(long maxMicros = 10_000_000) =>
        RunUntil(() => Client.State == PeerState.Closed && (Server is null || Server.State == PeerState.Closed), maxMicros);

    public override void Pump()
    {
        if (!_clientDisposed)
        {
            Client.Poll();
            Client.Flush();
        }

        if (Server is { } server && !_serverDisposed)
        {
            server.Poll();
            server.Flush();
        }
    }

    public void DisposeClient()
    {
        _clientDisposed = true;
        Client.Dispose();
    }

    public void DisposeServer()
    {
        _serverDisposed = true;
        Server?.Dispose();
    }

    /// <summary>Stops pumping the server (a test that disposes it from inside a handler drives it by hand).</summary>
    public void StopPumpingServer() => _serverDisposed = true;

    protected override void DisposeEndpoints()
    {
        if (!_clientDisposed)
        {
            Client.Dispose();
        }

        if (!_serverDisposed)
        {
            Server?.Dispose();
        }
    }
}

/// <summary>A bare transport client that speaks QUICLY by hand (for protocol-violation tests of the server).</summary>
internal sealed unsafe class RawClient
{
    private bool _controlPreambleSent;

    public RawClient(SimulatedNetwork network, EndPoint server, LinkOptions? link = null)
    {
        Sink = new RecordingSink(network.Clock);
        Transport = (SimulatedTransport)new SimulatedConnector(network, link).Connect(server, null, Sink);
        Sink.Transport = Transport;
    }

    public RecordingSink Sink { get; }

    public SimulatedTransport Transport { get; }

    public TransportStreamId Control { get; private set; }

    public bool IsConnected => Sink.CountOf(RecordedEventKind.Connected) > 0;

    public bool IsClosed => Sink.IsClosed;

    public ulong CloseCode => Sink.OfKind(RecordedEventKind.Closed)[0].ErrorCode;

    public void SendControl(ReadOnlySpan<byte> frame)
    {
        TransportSendFlags flags = TransportSendFlags.None;
        if (!Control.IsValid)
        {
            Assert.Equal(TransportStatus.Success, Transport.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id));
            Control = id;
            flags = TransportSendFlags.Start;
        }

        byte[] data = _controlPreambleSent ? frame.ToArray() : [0x00, .. frame];
        _controlPreambleSent = true;
        Assert.Equal(TransportStatus.Success, SendStream(Transport, Control, data, flags));
    }

    public TransportStatus SendDatagram(ReadOnlySpan<byte> datagram)
    {
        fixed (byte* p = datagram)
        {
            TransportSegment segment = new(p, datagram.Length);
            return Transport.SendDatagram(&segment, 1, 0, TransportSendFlags.None);
        }
    }

    /// <summary>Opens a unidirectional stream and sends <paramref name="data"/> on it (with FIN when asked).</summary>
    public TransportStatus OpenUni(ReadOnlySpan<byte> data, out TransportStreamId id, bool fin = false)
    {
        TransportStatus status = Transport.OpenStream(StreamKind.Unidirectional, 2, 32767, out id);
        if (status != TransportStatus.Success)
        {
            return status;
        }

        return SendStream(Transport, id, data, TransportSendFlags.Start | (fin ? TransportSendFlags.Fin : TransportSendFlags.None));
    }

    public static TransportStatus SendStream(ITransport transport, TransportStreamId id, ReadOnlySpan<byte> data, TransportSendFlags flags)
    {
        fixed (byte* p = data)
        {
            TransportSegment segment = new(p, data.Length);
            return transport.SendStream(id, &segment, 1, 0, flags);
        }
    }

    public List<(ControlType Type, byte[] Body)> ServerFrames() => Control.IsValid ? Frames.ParseStream(Sink.GetStreamData(Control)) : [];

    public int DatagramsOfType(ControlType type)
    {
        int count = 0;
        foreach (RecordedEvent e in Sink.OfKind(RecordedEventKind.DatagramReceived))
        {
            if (e.Data.Length >= 2 && e.Data[0] == 0 && e.Data[1] == (byte)type)
            {
                count++;
            }
        }

        return count;
    }
}

/// <summary>A server peer and a <see cref="RawClient"/>.</summary>
internal sealed class ServerHarness : SimFixture
{
    private bool _disposed;

    public ServerHarness(LinkOptions? link = null, ChannelTable? table = null, Action<PeerOptions>? server = null, int seed = 1)
        : base(seed)
    {
        Table = table ?? TestTables.Default;
        Options = new PeerOptions { Clock = Clock };
        server?.Invoke(Options);
        Listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo info) =>
        {
            QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, Table, Options, Admission);
            peer.StateChanged += (_, from, to) => Events.Add((from, to));
            Server = peer;
            return peer.TransportSink;
        });
        Raw = new RawClient(Network, Listener.LocalEndPoint, link);
        Assert.True(RunUntil(() => Raw.IsConnected && Server?.State == PeerState.Handshaking), "The raw client did not connect.");
    }

    public ChannelTable Table { get; }

    public PeerOptions Options { get; }

    public TestAdmission Admission { get; } = new();

    public RawClient Raw { get; }

    public QuiclyPeer? Server { get; private set; }

    public List<(PeerState From, PeerState To)> Events { get; } = [];

    /// <summary>Sends a valid Hello and runs until the server accepted it and the client saw the HelloAck and the raised stream limits.</summary>
    public bool Admit(HelloFlags flags = HelloFlags.None)
    {
        Raw.SendControl(Frames.HelloFrame(Table.Hash, flags: flags));
        return RunUntil(() => Server!.State == PeerState.Connected && Raw.ServerFrames().Count > 0
            && Raw.Sink.CountOf(RecordedEventKind.StreamsAvailable) > 0);
    }

    public PeerStatistics Statistics()
    {
        Server!.GetStatistics(out PeerStatistics statistics);
        return statistics;
    }

    public override void Pump()
    {
        if (Server is { } server && !_disposed)
        {
            server.Poll();
            server.Flush();
        }
    }

    /// <summary>Disposes the server peer mid-test and stops pumping it; the raw client and the network keep running.</summary>
    public void DisposeServer()
    {
        _disposed = true;
        Server?.Dispose();
    }

    protected override void DisposeEndpoints()
    {
        _disposed = true;
        Server?.Dispose();
        Raw.Transport.Dispose();
    }
}

/// <summary>A client peer and a raw server transport that answers by hand.</summary>
internal sealed class ClientHarness : SimFixture
{
    private bool _serverPreambleSent;
    private bool _disposed;

    public ClientHarness(LinkOptions? link = null, ChannelTable? table = null, Action<PeerOptions>? client = null, int seed = 1)
        : base(seed)
    {
        Table = table ?? TestTables.Default;
        Sink = new RecordingSink(Clock);
        Listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo info) =>
        {
            ServerTransport = (SimulatedTransport)transport;
            Sink.Transport = transport;
            return Sink;
        });
        Options = new PeerOptions { Clock = Clock };
        client?.Invoke(Options);
        Client = QuiclyPeer.Connect(new SimulatedConnector(Network, link), Listener.LocalEndPoint, null, Table, Options, "auth"u8);
        Client.StateChanged += (_, from, to) => Events.Add((from, to));
        Assert.True(RunUntil(() => ClientFrames().Count > 0), "No Hello arrived.");
    }

    public ChannelTable Table { get; }

    public PeerOptions Options { get; }

    public RecordingSink Sink { get; }

    public SimulatedTransport? ServerTransport { get; private set; }

    public QuiclyPeer Client { get; }

    public List<(PeerState From, PeerState To)> Events { get; } = [];

    public TransportStreamId ControlStream =>
        Sink.CountOf(RecordedEventKind.PeerStreamStarted) > 0 ? Sink.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId : default;

    public bool IsServerClosed => Sink.IsClosed;

    public ulong ServerCloseCode => Sink.OfKind(RecordedEventKind.Closed)[0].ErrorCode;

    /// <summary>Frames the client sent on the control stream.</summary>
    public List<(ControlType Type, byte[] Body)> ClientFrames() =>
        ControlStream.IsValid ? Frames.ParseStream(Sink.GetStreamData(ControlStream)) : [];

    public void SendToClient(ReadOnlySpan<byte> frame)
    {
        byte[] data = _serverPreambleSent ? frame.ToArray() : [0x00, .. frame];
        _serverPreambleSent = true;
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(ServerTransport!, ControlStream, data, TransportSendFlags.None));
    }

    public bool Accept(uint epoch = 1, ulong sessionId = 9, byte[]? token = null)
    {
        SendToClient(Frames.HelloAckFrame(HelloStatus.Accepted, epoch, sessionId, token: token));
        return RunUntil(() => Client.State == PeerState.Connected);
    }

    public override void Pump()
    {
        if (!_disposed)
        {
            Client.Poll();
            Client.Flush();
        }
    }

    protected override void DisposeEndpoints()
    {
        _disposed = true;
        Client.Dispose();
        ServerTransport?.Dispose();
    }
}

/// <summary>Builders and parsers of control frames for the raw endpoints.</summary>
internal static class Frames
{
    public const PeerCaps AllCaps = PeerCaps.Datagrams | PeerCaps.DatagramSendState | PeerCaps.Lz4;

    public static byte[] HelloFrame(ulong tableHash, ReadOnlySpan<byte> auth = default, HelloFlags flags = HelloFlags.None, PeerCaps caps = AllCaps,
        ushort version = ControlCodec.ProtocolVersion, bool badMagic = false)
    {
        byte[] buffer = new byte[ControlCodec.MaxEncodedStreamFrameLength];
        Hello hello = new() { TableHash = tableHash, AuthToken = auth, Flags = flags, Caps = caps };
        Assert.True(ControlCodec.TryWrite(buffer, in hello, out int written));
        byte[] frame = buffer.AsSpan(0, written).ToArray();
        int body = VarInt.PeekLength(frame[0]) + 1;
        if (version != ControlCodec.ProtocolVersion)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(body + 4), version);
        }

        if (badMagic)
        {
            frame[body] = (byte)'X';
        }

        return frame;
    }

    public static byte[] HelloAckFrame(HelloStatus status, uint epoch = 1, ulong sessionId = 9, string? reason = null, byte[]? token = null,
        ulong maxMessageSize = 1 << 20, byte[]? table = null)
    {
        byte[] buffer = new byte[ControlCodec.MaxEncodedStreamFrameLength];
        HelloAck ack = new()
        {
            Status = status,
            Epoch = epoch,
            SessionId = sessionId,
            Reason = reason is null ? default : Encoding.UTF8.GetBytes(reason),
            SessionToken = token,
            MaxMessageSize = maxMessageSize,
            Caps = AllCaps,
            Table = table,
        };
        Assert.True(ControlCodec.TryWrite(buffer, in ack, out int written));
        return buffer.AsSpan(0, written).ToArray();
    }

    public static byte[] TableSection(ChannelTable table)
    {
        byte[] section = new byte[ChannelTableCodec.GetLengthWithNames(table)];
        ChannelTableCodec.WriteWithNames(table, section);
        return section;
    }

    public static byte[] PingFrame(uint time, ControlCarrier carrier)
    {
        byte[] buffer = new byte[32];
        Assert.True(ControlCodec.TryWrite(buffer, new Ping(time), carrier, out int written));
        return buffer.AsSpan(0, written).ToArray();
    }

    public static byte[] PongFrame(uint echo, uint received, uint sent, ControlCarrier carrier)
    {
        byte[] buffer = new byte[32];
        Assert.True(ControlCodec.TryWrite(buffer, new Pong(echo, received, sent), carrier, out int written));
        return buffer.AsSpan(0, written).ToArray();
    }

    public static byte[] CloseFrame(QuiclyErrorCode code, string reason = "")
    {
        byte[] buffer = new byte[600];
        Assert.True(ControlCodec.TryWrite(buffer, new Close(code, Encoding.UTF8.GetBytes(reason)), out int written));
        return buffer.AsSpan(0, written).ToArray();
    }

    public static byte[] KeyRetiredFrame(ushort channel, ulong key)
    {
        byte[] buffer = new byte[32];
        Assert.True(ControlCodec.TryWrite(buffer, new KeyRetired(channel, key), out int written));
        return buffer.AsSpan(0, written).ToArray();
    }

    public static byte[] TableRequestFrame()
    {
        byte[] buffer = new byte[8];
        Assert.True(ControlCodec.TryWriteChannelTableRequest(buffer, out int written));
        return buffer.AsSpan(0, written).ToArray();
    }

    /// <summary>A control-stream frame with any type byte and body (for malformed-input tests).</summary>
    public static byte[] RawFrame(byte type, ReadOnlySpan<byte> body)
    {
        byte[] frame = new byte[8 + body.Length];
        int length = VarInt.Write(frame, (ulong)body.Length + 1);
        frame[length++] = type;
        body.CopyTo(frame.AsSpan(length));
        return frame.AsSpan(0, length + body.Length).ToArray();
    }

    /// <summary>Parses a control stream's bytes (preamble <c>0x00</c>, then framed messages); a trailing partial frame is ignored.</summary>
    public static List<(ControlType Type, byte[] Body)> ParseStream(byte[] data)
    {
        List<(ControlType Type, byte[] Body)> frames = [];
        if (data.Length == 0)
        {
            return frames;
        }

        Assert.Equal(0, data[0]);
        ReadOnlySpan<byte> rest = data.AsSpan(1);
        while (!rest.IsEmpty && ControlCodec.TryReadStream(rest, out ControlType type, out ReadOnlySpan<byte> body, out int consumed) == ControlParseStatus.Ok)
        {
            frames.Add((type, body.ToArray()));
            rest = rest.Slice(consumed);
        }

        return frames;
    }

    public static HelloStatus AckStatus(byte[] body)
    {
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out HelloAck ack));
        return ack.Status;
    }

    public static QuiclyErrorCode CloseCode(byte[] body)
    {
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out Close close));
        return close.Code;
    }
}

/// <summary>
/// A minimal engine that exercises the engine boundary the way the real engines will: datagram channels send loose
/// datagrams from <see cref="Admit"/> and publish received messages to the receive ring (or a mailbox); stream channels
/// open one unidirectional stream and publish complete stream messages through ring reservations.
/// </summary>
internal sealed unsafe class TestEngine : ChannelEngine
{
    private readonly bool _useMailbox;
    private readonly Dictionary<TransportStreamId, StreamState> _streams = [];
    private uint _sequence;
    private TransportStreamId _sendStream;
    private bool _sendStarted;

    public TestEngine(ChannelMode mode, bool useMailbox = false)
    {
        Mode = mode;
        _useMailbox = useMailbox;
    }

    public override ChannelMode Mode { get; }

    public PeerCore Core { get; private set; } = null!;

    public ReceiveMailbox? Mailbox { get; private set; }

    // knobs
    public bool ThrowOnDatagram;
    public int PendStarts;
    public StreamConsume? StartResult;
    public StreamAccept? OpenResult;
    public bool RejectControl;
    public bool ThrowOnOpen;
    public bool ThrowOnClosed;
    public int BulkHeaders;

    // observations
    public int Admitted;
    public int Received;
    public int Completed;
    public int SentNotices;
    public int Opened;
    public int Starts;
    public int Chunks;
    public int Ends;
    public int Pended;
    public int StreamsClosed;
    public int LocalStreamEvents;
    public int EpochResets;
    public int Ticks;
    public int Flushes;
    public int ControlMessages;
    public int PeerClosedCalls;
    public bool LastStreamAborted;
    public ulong LastStreamCode;
    public uint LastSenderTick;
    public DeliveryStatus LastStatus = DeliveryStatus.Pending;

    public override void Initialize(PeerCore core, ReadOnlySpan<ChannelDefinition> channelsOfMode)
    {
        Core = core;
        if (_useMailbox && channelsOfMode.Length > 0)
        {
            Mailbox = core.CreateMailbox(core.ChannelIndexOf(channelsOfMode[0].Id), 16);
        }
    }

    public override SendStatus Admit(ref SendRequest request)
    {
        ChannelDefinition channel = request.Channel;
        int length = request.Kind == SendPayloadKind.Copy ? request.Source.Length : request.Length;
        if (length > Core.EffectiveMaxMessageSize(channel))
        {
            return SendStatus.TooLarge;
        }

        if (!Core.TryAllocateEntry(channel.Id, SendEntryFlags.None, out int slot))
        {
            return SendStatus.QueueFull;
        }

        int payloadLength = length;
        int rawLength = 0;
        if (request.Kind == SendPayloadKind.Owned)
        {
            Core.AttachLease(slot, in request.Lease, length);
        }
        else
        {
            if (!Core.TryRentSend(Math.Max(Lz4Block.GetMaxCompressedLength(length), 1), out BufferLease lease))
            {
                Core.DiscardEntry(slot);
                return SendStatus.OutOfBuffers;
            }

            Span<byte> target = Core.GetSpan(in lease);
            if (channel.Compression == ChannelCompression.Lz4 && length >= channel.MinCompressSize && length > 0)
            {
                int compressed = Lz4Block.Compress(request.Source, target);
                if (compressed > 0 && compressed < length)
                {
                    payloadLength = compressed;
                    rawLength = length;
                }
                else
                {
                    request.Source.CopyTo(target);
                }
            }
            else
            {
                request.Source.CopyTo(target);
            }

            Core.AttachLease(slot, in lease, payloadLength);
        }

        if (request.Options.Track && !Core.TryTrack(slot, request.Options.Context, out request.Token))
        {
            Core.DiscardEntry(slot);
            return SendStatus.QueueFull;
        }

        if (!channel.IsDatagramMode)
        {
            return SubmitStreamFrame(channel, slot, payloadLength, request.Key);
        }

        MessageHeader header = default;
        header.Sequence = ++_sequence;
        header.Key = request.Key;
        header.FragCount = 1;
        header.RawLength = rawLength;
        int headerLength = DatagramFraming.WriteHeader(Core.Entries.GetHeaderBlock(slot), channel, in header);
        Core.Entries.SetHeaderLength(slot, headerLength);
        if (Core.SubmitDatagram(slot, TransportSendFlags.None) != TransportStatus.Success)
        {
            Core.DiscardEntry(slot);
            return SendStatus.NotConnected;
        }

        Admitted++;
        return SendStatus.Admitted;
    }

    private SendStatus SubmitStreamFrame(ChannelDefinition channel, int slot, int payloadLength, ulong key)
    {
        if (!_sendStream.IsValid && Core.OpenStream(StreamKind.Unidirectional, 42, 32767, out _sendStream) != TransportStatus.Success)
        {
            Core.DiscardEntry(slot);
            return SendStatus.ChannelClosed;
        }

        Span<byte> block = Core.Entries.GetHeaderBlock(slot);
        int position = 0;
        TransportSendFlags flags = TransportSendFlags.None;
        if (!_sendStarted)
        {
            position = StreamFraming.WritePreamble(block, channel.Id);
            flags = TransportSendFlags.Start;
        }

        StreamMessageHeader header = default;
        header.Length = payloadLength;
        header.Key = key;
        position += StreamFraming.WriteFrameHeader(block.Slice(position), channel, in header);
        Core.Entries.SetHeaderLength(slot, position);
        TransportStatus status = Core.SubmitStream(_sendStream, Core.Entries.GetSegments(slot), payloadLength == 0 ? 1 : 2, slot, flags);
        if (status != TransportStatus.Success)
        {
            Core.DiscardEntry(slot);
            if (!_sendStarted)
            {
                // Never started (no accepted send carrying Start): releasing the slot is enough, no callback follows for it.
                Core.Transport?.CloseStream(_sendStream);
                _sendStream = default;
            }

            return status == TransportStatus.StreamLimitReached ? SendStatus.QueueFull : SendStatus.ChannelClosed;
        }

        _sendStarted = true;
        Admitted++;
        return SendStatus.Admitted;
    }

    public override void Flush(ref FlushContext flush) => Flushes++;

    public override void Tick(long nowMicros, ref long nextDeadline) => Ticks++;

    public override void OnSendCompleted(int entrySlot, in CompletionEntry completion)
    {
        if (!completion.Final)
        {
            SentNotices++;
            Core.ReleasePayload(entrySlot);
            Core.CompleteStage(entrySlot, CompletionStage.BufferReleased, DeliveryStatus.Pending);
            return;
        }

        Completed++;
        LastStatus = completion.Kind == CompletionKind.Datagram
            ? Core.MapDatagramState(completion.DatagramState)
            : Core.MapStreamCompletion(completion.Canceled);
        Core.CompleteEntry(entrySlot, LastStatus);
    }

    public override void OnEpochReset(bool resumed) => EpochResets++;

    public override void OnPeerClosed() => PeerClosedCalls++;

    public override bool OnControl(ControlType type, ReadOnlySpan<byte> body, bool onStream, long nowMicros)
    {
        ControlMessages++;
        return !RejectControl;
    }

    public override void OnDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros)
    {
        if (ThrowOnDatagram)
        {
            throw new InvalidOperationException("test engine fault");
        }

        Received++;
        LastSenderTick = Core.CurrentSenderTick;
        int index = Core.ChannelIndexOf(header.Channel);
        if (!Core.TryRentReceive(Math.Max(payload.Length, 1), out BufferLease lease))
        {
            Core.Counters.OutOfReceiveBuffers++;
            Core.RecvCounters(index).OutOfBuffers++;
            return;
        }

        payload.CopyTo(Core.GetSpan(in lease));
        ReceiveEntry entry = default;
        entry.Channel = header.Channel;
        entry.Sequence = header.Sequence;
        entry.Key = header.Key;
        entry.Lease = lease;
        entry.Length = payload.Length;
        entry.RawLength = header.RawLength;
        entry.ReceivedMicrosDelta = PeerCore.StampReceive(nowMicros);
        entry.SenderTick = Core.CurrentSenderTick;
        if (header.Compressed)
        {
            entry.Flags |= ReceiveFlags.Compressed;
        }

        Core.RecvCounters(index).Received++;
        if (Mailbox is not null)
        {
            if (Mailbox.TryPost((int)(header.Key % (ulong)Mailbox.KeySlots), in entry, out BufferLease displaced) && !displaced.IsEmpty)
            {
                Core.ReturnReceive(in displaced);
                Core.RecvCounters(index).Superseded++;
            }

            return;
        }

        if (!Core.TryEnqueueReceive(in entry))
        {
            Core.ReturnReceive(in lease);
            Core.RecvCounters(index).RingDrops++;
        }
    }

    public override StreamAccept OnStreamOpened(TransportStreamId id, ushort channel, ulong groupId)
    {
        Opened++;
        if (ThrowOnOpen)
        {
            throw new InvalidOperationException("test engine open fault");
        }

        if (OpenResult is { } forced)
        {
            return forced;
        }

        _streams[id] = new StreamState();
        return StreamAccept.Accept(id.Slot);
    }

    public override StreamConsume OnStreamMessage(ref StreamMessageContext message)
    {
        StreamState state = _streams[message.Id];
        switch (message.Phase)
        {
            case StreamMessagePhase.Start:
                if (PendStarts > 0)
                {
                    PendStarts--;
                    Pended++;
                    return StreamConsume.Pend;
                }

                if (StartResult is { } forced)
                {
                    return forced;
                }

                if (!Core.TryReserveReceive())
                {
                    Pended++;
                    return StreamConsume.Pend;
                }

                Starts++;
                if (!Core.TryRentReceive(Math.Max(message.Header.Length, 1), out state.Lease))
                {
                    Core.CancelReservation();
                    return StreamConsume.ResetStream(QuiclyErrorCode.LimitExceeded);
                }

                state.Reserved = true;
                state.Filled = 0;
                state.Length = message.Header.Length;
                return StreamConsume.Continue;
            case StreamMessagePhase.Chunk:
                Chunks++;
                message.Chunk.CopyTo(Core.GetSpan(in state.Lease).Slice(state.Filled));
                state.Filled += message.Chunk.Length;
                return StreamConsume.Continue;
            case StreamMessagePhase.End:
            {
                Ends++;
                ReceiveEntry entry = default;
                entry.Channel = message.Channel;
                entry.Key = message.Header.Key;
                entry.Lease = state.Lease;
                entry.Length = state.Length;
                entry.ReceivedMicrosDelta = PeerCore.StampReceive(message.NowMicros);
                Core.PublishReserved(in entry);
                state.Reserved = false;
                state.Lease = default;
                return StreamConsume.Continue;
            }

            case StreamMessagePhase.BulkHeader:
                BulkHeaders++;
                return StreamConsume.Continue;
            default:
                return StreamConsume.Continue;
        }
    }

    public override void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode)
    {
        if (ThrowOnClosed)
        {
            throw new InvalidOperationException("test engine close fault");
        }

        if (_streams.Remove(id, out StreamState? state))
        {
            StreamsClosed++;
            LastStreamAborted = aborted;
            LastStreamCode = errorCode;
            if (state.Reserved)
            {
                Core.CancelReservation();
                Core.ReturnReceive(in state.Lease);
            }
        }
        else
        {
            LocalStreamEvents++;
        }
    }

    private sealed class StreamState
    {
        public BufferLease Lease;
        public int Filled;
        public int Length;
        public bool Reserved;
    }
}

/// <summary>Creates peer pairs whose engine of one mode is a <see cref="TestEngine"/>.</summary>
internal static class TestEngines
{
    public static SessionHarness Create(out TestEngine client, out TestEngine server, ChannelMode mode = ChannelMode.UnreliableUnordered,
        bool mailbox = false, Action<PeerOptions>? both = null, LinkOptions? link = null, ChannelTable? table = null)
    {
        TestEngine? c = null;
        TestEngine? s = null;
        SessionHarness harness = new(link: link, table: table ?? TestTables.Plumbing,
            client: o =>
            {
                both?.Invoke(o);
                o.EngineFactory = m => m == mode ? c = new TestEngine(m, mailbox) : null;
            },
            server: o =>
            {
                both?.Invoke(o);
                o.EngineFactory = m => m == mode ? s = new TestEngine(m, mailbox) : null;
            });
        client = c!;
        server = s!;
        return harness;
    }

    public static ServerHarness CreateServer(out Func<TestEngine> engine, ChannelMode mode = ChannelMode.UnreliableUnordered, LinkOptions? link = null,
        ChannelTable? table = null, Action<PeerOptions>? server = null)
    {
        TestEngine? s = null;
        ServerHarness harness = new(link: link, table: table ?? TestTables.Plumbing, server: o =>
        {
            server?.Invoke(o);
            o.EngineFactory = m => m == mode ? s = new TestEngine(m) : null;
        });
        engine = () => s!;
        return harness;
    }
}

/// <summary>A bulk source with nothing to read.</summary>
internal sealed class EmptySource : IBulkSource
{
    public int Read(long offset, Span<byte> destination) => 0;
}

/// <summary>Collects handler calls.</summary>
internal static class Handlers
{
    public static MessageHandler Collect(List<(ReceiveHeader Header, byte[] Payload)> into) =>
        (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) => into.Add((header, payload.ToArray()));
}

/// <summary>Zero-allocation assertion over windows of calls (the pattern of the other zero-allocation tests).</summary>
internal static class AllocationAssert
{
    public static void NoAllocations(Action body, int warmup = 1_000, int iterations = 2_000, int windows = 5)
    {
        for (int i = 0; i < warmup; i++)
        {
            body();
        }

        long[] deltas = new long[windows];
        int allocating = 0;
        for (int window = 0; window < windows; window++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < iterations; i++)
            {
                body();
            }

            deltas[window] = GC.GetAllocatedBytesForCurrentThread() - before;
            if (deltas[window] != 0)
            {
                allocating++;
            }
        }

        Assert.True(allocating <= 1, $"Allocated in {allocating} of {windows} windows: {string.Join(", ", deltas)} bytes per {iterations} calls.");
    }
}
