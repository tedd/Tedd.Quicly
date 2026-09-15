using System.Collections.Concurrent;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

[Collection(MsQuicCollection.Name)]
public class ExplorationTests
{
    private static readonly string LogPath = Path.Combine(Environment.GetEnvironmentVariable("QUICLY_EXPLORE_LOG") ?? Path.GetTempPath(), "quicly-explore.txt");

    private static void Log(string s) => File.AppendAllText(LogPath, s + Environment.NewLine);

    private sealed unsafe class ScriptedStream : IMsQuicStreamEvents
    {
        public readonly ConcurrentQueue<string> Events = new();
        public Func<int, ulong, bool, MsQuicReceiveResult> Script = (_, total, _) => MsQuicReceiveResult.Consumed(total);
        public int Calls;
        public MsQuicStream? Stream;

        public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
        {
            int call = Interlocked.Increment(ref Calls);
            bool fin = (flags & QUIC_RECEIVE_FLAGS.FIN) != 0;
            MsQuicReceiveResult r = Script(call, totalLength, fin);
            Events.Enqueue($"RECV#{call} off={absoluteOffset} len={totalLength} bufs={bufferCount} fin={fin} -> {(r.IsPending ? "PENDING" : r.BytesConsumed.ToString())}");
            return r;
        }

        public void StartComplete(MsQuicStream stream, int status, ulong id, bool peerAccepted) => Events.Enqueue($"START_COMPLETE {MsQuicStatus.GetName(status)} id={id}");
        public void SendComplete(MsQuicStream stream, void* clientContext, bool canceled) => Events.Enqueue($"SEND_COMPLETE ctx={(nint)clientContext} canceled={canceled}");
        public void PeerSendShutdown(MsQuicStream stream) => Events.Enqueue("PEER_SEND_SHUTDOWN");
        public void PeerSendAborted(MsQuicStream stream, ulong errorCode) => Events.Enqueue($"PEER_SEND_ABORTED {errorCode}");
        public void PeerReceiveAborted(MsQuicStream stream, ulong errorCode) => Events.Enqueue($"PEER_RECEIVE_ABORTED {errorCode}");
        public void SendShutdownComplete(MsQuicStream stream, bool graceful) => Events.Enqueue($"SEND_SHUTDOWN_COMPLETE graceful={graceful}");
        public void ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info) => Events.Enqueue($"SHUTDOWN_COMPLETE conn={info.ConnectionShutdown}");
        public void IdealSendBufferSize(MsQuicStream stream, ulong byteCount) => Events.Enqueue($"IDEAL {byteCount}");
    }

    private static void Dump(string title, ScriptedStream s)
    {
        Log("== " + title);
        foreach (string e in s.Events) Log("   " + e);
    }

    [Fact]
    public async Task E1_partial_sync_consume()
    {
        ScriptedStream? server = null;
        using var loopback = new Loopback { ServerEventsFactory = _ => new ConnectionRecorder { PeerStreamEventsFactory = st => server = new ScriptedStream { Stream = st, Script = (call, total, fin) => call == 1 ? MsQuicReceiveResult.Consumed(10) : MsQuicReceiveResult.Consumed(total) } } };
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();
        var cs = new ScriptedStream();
        client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, cs, out MsQuicStream? stream);
        loopback.Track(stream!);
        using var block = new NativeBlock(1000);
        using NativeBuffers buf = NativeBuffers.Single(block);
        buf.SendOn(stream!, QUIC_SEND_FLAGS.START, 1);
        MsQuicStream ss = await serverEvents.FirstPeerStreamTcs.Within();
        await Task.Delay(400);
        server!.Events.Enqueue("--- after 400ms idle");
        buf.SendOn(stream!, QUIC_SEND_FLAGS.NONE, 2);
        await Task.Delay(400);
        server.Events.Enqueue("--- after second send + 400ms");
        server.Events.Enqueue("ReceiveSetEnabled(true) -> " + MsQuicStatus.GetName(ss.ReceiveSetEnabled(true)));
        await Task.Delay(400);
        server.Events.Enqueue("--- after enable + 400ms");
        buf.SendOn(stream!, QUIC_SEND_FLAGS.FIN, 3);
        await Task.Delay(400);
        Dump("E1 partial sync consume (10 of first)", server);
        stream!.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0);
        await Task.Delay(100);
    }

    [Fact]
    public async Task E2_pending_then_partial_receive_complete()
    {
        ScriptedStream? server = null;
        using var loopback = new Loopback { ServerEventsFactory = _ => new ConnectionRecorder { PeerStreamEventsFactory = st => server = new ScriptedStream { Stream = st, Script = (call, total, fin) => call == 1 ? MsQuicReceiveResult.Pending : MsQuicReceiveResult.Consumed(total) } } };
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();
        var cs = new ScriptedStream();
        client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, cs, out MsQuicStream? stream);
        loopback.Track(stream!);
        using var block = new NativeBlock(1000);
        using NativeBuffers buf = NativeBuffers.Single(block);
        buf.SendOn(stream!, QUIC_SEND_FLAGS.START, 1);
        MsQuicStream ss = await serverEvents.FirstPeerStreamTcs.Within();
        await Task.Delay(300);
        ss.ReceiveComplete(10);
        server!.Events.Enqueue("--- ReceiveComplete(10)");
        await Task.Delay(400);
        server.Events.Enqueue("--- after 400ms idle");
        buf.SendOn(stream!, QUIC_SEND_FLAGS.NONE, 2);
        await Task.Delay(400);
        server.Events.Enqueue("--- after second send + 400ms");
        server.Events.Enqueue("ReceiveSetEnabled(true) -> " + MsQuicStatus.GetName(ss.ReceiveSetEnabled(true)));
        await Task.Delay(400);
        server.Events.Enqueue("--- after enable + 400ms");
        Dump("E2 pending then ReceiveComplete(10)", server);
        stream!.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0);
        await Task.Delay(100);
    }

    [Fact]
    public async Task E3_pending_then_full_receive_complete()
    {
        ScriptedStream? server = null;
        using var loopback = new Loopback { ServerEventsFactory = _ => new ConnectionRecorder { PeerStreamEventsFactory = st => server = new ScriptedStream { Stream = st, Script = (call, total, fin) => call == 1 ? MsQuicReceiveResult.Pending : MsQuicReceiveResult.Consumed(total) } } };
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();
        var cs = new ScriptedStream();
        client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, cs, out MsQuicStream? stream);
        loopback.Track(stream!);
        using var block = new NativeBlock(1000);
        using NativeBuffers buf = NativeBuffers.Single(block);
        buf.SendOn(stream!, QUIC_SEND_FLAGS.START, 1);
        MsQuicStream ss = await serverEvents.FirstPeerStreamTcs.Within();
        await Task.Delay(300);
        buf.SendOn(stream!, QUIC_SEND_FLAGS.NONE, 2);
        await Task.Delay(300);
        ss.ReceiveComplete(1000);
        server!.Events.Enqueue("--- ReceiveComplete(1000) (the first indication fully)");
        await Task.Delay(400);
        server.Events.Enqueue("--- after 400ms");
        buf.SendOn(stream!, QUIC_SEND_FLAGS.FIN, 3);
        await Task.Delay(400);
        Dump("E3 pending then ReceiveComplete(full)", server);
        stream!.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0);
        await Task.Delay(100);
    }

    [Fact]
    public async Task E4_fail_blocked_start_off_thread()
    {
        MsQuicSettings serverSettings = Loopback.TestServerSettings();
        serverSettings.PeerUnidiStreamCount = 0;
        using var loopback = new Loopback(serverSettings);
        (MsQuicConnection client, _, _, _) = await loopback.ConnectPairAsync();
        var cs = new ScriptedStream();
        client.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, cs, out MsQuicStream? stream);
        loopback.Track(stream!);
        using var block = new NativeBlock(100);
        using NativeBuffers buf = NativeBuffers.Single(block);
        int st = stream!.Start(QUIC_STREAM_START_FLAGS.FAIL_BLOCKED | QUIC_STREAM_START_FLAGS.SHUTDOWN_ON_FAIL);
        cs.Events.Enqueue("Start -> " + MsQuicStatus.GetName(st));
        int sd = buf.SendOn(stream!, QUIC_SEND_FLAGS.NONE, 5);
        cs.Events.Enqueue("Send -> " + MsQuicStatus.GetName(sd));
        await Task.Delay(500);
        int st2 = stream.Start(QUIC_STREAM_START_FLAGS.FAIL_BLOCKED | QUIC_STREAM_START_FLAGS.SHUTDOWN_ON_FAIL);
        cs.Events.Enqueue("Start again -> " + MsQuicStatus.GetName(st2));
        await Task.Delay(300);
        Dump("E4 FAIL_BLOCKED|SHUTDOWN_ON_FAIL start off-thread + queued send (peer uni=0)", cs);

        // Without SHUTDOWN_ON_FAIL
        var cs2 = new ScriptedStream();
        client.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, cs2, out MsQuicStream? stream2);
        loopback.Track(stream2!);
        cs2.Events.Enqueue("Start -> " + MsQuicStatus.GetName(stream2!.Start(QUIC_STREAM_START_FLAGS.FAIL_BLOCKED)));
        await Task.Delay(300);
        cs2.Events.Enqueue("Start again -> " + MsQuicStatus.GetName(stream2.Start(QUIC_STREAM_START_FLAGS.FAIL_BLOCKED)));
        await Task.Delay(300);
        Dump("E4b FAIL_BLOCKED only", cs2);
    }

    [Fact]
    public async Task E5_send_before_start_and_datagram_status()
    {
        using var loopback = new Loopback();
        var clientEvents = new ConnectionRecorder();
        MsQuicConnection client = loopback.Connect(clientEvents);
        using var dblock = new NativeBlock(64);
        using NativeBuffers dbuf = NativeBuffers.Single(dblock);
        Log("== E5 datagram before connected -> " + MsQuicStatus.GetName(dbuf.SendDatagramOn(client, QUIC_SEND_FLAGS.NONE, 1)));
        await clientEvents.ConnectedTcs.Within();
        (_, ushort max) = await clientEvents.DatagramSendEnabledTcs.Within();
        using var big = new NativeBlock(max + 1);
        using NativeBuffers bbuf = NativeBuffers.Single(big);
        Log("   datagram oversized (max+1) -> " + MsQuicStatus.GetName(bbuf.SendDatagramOn(client, QUIC_SEND_FLAGS.NONE, 2)) + " max=" + max);
        using var exact = new NativeBlock(max);
        using NativeBuffers ebuf = NativeBuffers.Single(exact);
        Log("   datagram exactly max -> " + MsQuicStatus.GetName(ebuf.SendDatagramOn(client, QUIC_SEND_FLAGS.NONE, 3)));

        var cs = new ScriptedStream();
        client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, cs, out MsQuicStream? stream);
        loopback.Track(stream!);
        using var block = new NativeBlock(100);
        using NativeBuffers buf = NativeBuffers.Single(block);
        cs.Events.Enqueue("Send before start (no START flag) -> " + MsQuicStatus.GetName(buf.SendOn(stream!, QUIC_SEND_FLAGS.NONE, 7)));
        await Task.Delay(300);
        cs.Events.Enqueue("Start -> " + MsQuicStatus.GetName(stream!.Start(QUIC_STREAM_START_FLAGS.FAIL_BLOCKED | QUIC_STREAM_START_FLAGS.SHUTDOWN_ON_FAIL)));
        await Task.Delay(300);
        unsafe
        {
            byte[] reason = "bye"u8.ToArray();
            fixed (byte* p = reason)
            {
                cs.Events.Enqueue("CLOSE_REASON_PHRASE -> " + MsQuicStatus.GetName(client.SetParam(MsQuicParam.QUIC_PARAM_CONN_CLOSE_REASON_PHRASE, (uint)reason.Length, p)));
            }
            byte[] reasonZ = "bye\0"u8.ToArray();
            fixed (byte* p = reasonZ)
            {
                cs.Events.Enqueue("CLOSE_REASON_PHRASE (nul) -> " + MsQuicStatus.GetName(client.SetParam(MsQuicParam.QUIC_PARAM_CONN_CLOSE_REASON_PHRASE, (uint)reasonZ.Length, p)));
            }
        }
        Dump("E5 send before start", cs);
        stream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0);
        await Task.Delay(100);
    }

    [Fact]
    public async Task E6_partial_consume_at_fin()
    {
        ScriptedStream? server = null;
        using var loopback = new Loopback { ServerEventsFactory = _ => new ConnectionRecorder { PeerStreamEventsFactory = st => server = new ScriptedStream { Stream = st, Script = (call, total, fin) => call == 1 ? MsQuicReceiveResult.Consumed(10) : MsQuicReceiveResult.Consumed(total) } } };
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();
        var cs = new ScriptedStream();
        client.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, cs, out MsQuicStream? stream);
        loopback.Track(stream!);
        using var block = new NativeBlock(1000);
        using NativeBuffers buf = NativeBuffers.Single(block);
        buf.SendOn(stream!, QUIC_SEND_FLAGS.START | QUIC_SEND_FLAGS.FIN, 1);
        MsQuicStream ss = await serverEvents.FirstPeerStreamTcs.Within();
        await Task.Delay(400);
        server!.Events.Enqueue("--- after 400ms idle");
        server.Events.Enqueue("ReceiveSetEnabled(true) -> " + MsQuicStatus.GetName(ss.ReceiveSetEnabled(true)));
        await Task.Delay(400);
        Dump("E6 partial consume (10) of a FIN indication", server);
        Dump("E6 client", cs);
    }
}

[Collection(MsQuicCollection.Name)]
public class ExplorationTests2
{
    private static readonly string LogPath = Path.Combine(Environment.GetEnvironmentVariable("QUICLY_EXPLORE_LOG") ?? Path.GetTempPath(), "quicly-explore.txt");

    private sealed unsafe class InlineEnable : IMsQuicStreamEvents
    {
        public readonly ConcurrentQueue<string> Events = new();
        public int Calls;
        public int EnableFirstN = 1;
        public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
        {
            int call = Interlocked.Increment(ref Calls);
            bool fin = (flags & QUIC_RECEIVE_FLAGS.FIN) != 0;
            if (call <= EnableFirstN && totalLength > 10)
            {
                int st = stream.ReceiveSetEnabled(true);
                Events.Enqueue($"RECV#{call} off={absoluteOffset} len={totalLength} fin={fin} inline enable -> {MsQuicStatus.GetName(st)} ; consume 10");
                return MsQuicReceiveResult.Consumed(10);
            }
            Events.Enqueue($"RECV#{call} off={absoluteOffset} len={totalLength} fin={fin} -> all");
            return MsQuicReceiveResult.Consumed(totalLength);
        }
        public void PeerSendShutdown(MsQuicStream stream) => Events.Enqueue("PEER_SEND_SHUTDOWN");
    }

    [Fact]
    public async Task E7_inline_enable_during_receive()
    {
        InlineEnable? server = null;
        using var loopback = new Loopback { ServerEventsFactory = _ => new ConnectionRecorder { PeerStreamEventsFactory = st => server = new InlineEnable { EnableFirstN = 3 } } };
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();
        var cs = new StreamRecorder();
        client.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, cs, out MsQuicStream? stream);
        loopback.Track(stream!);
        using var block = new NativeBlock(1000);
        using NativeBuffers buf = NativeBuffers.Single(block);
        buf.SendOn(stream!, QUIC_SEND_FLAGS.START, 1);
        await serverEvents.FirstPeerStreamTcs.Within();
        await Task.Delay(400);
        server!.Events.Enqueue("--- after 400ms idle");
        buf.SendOn(stream!, QUIC_SEND_FLAGS.FIN, 2);
        await Task.Delay(400);
        File.AppendAllText(LogPath, "== E7 inline ReceiveSetEnabled(true) in RECEIVE then Consumed(10)" + Environment.NewLine);
        foreach (string e in server.Events) File.AppendAllText(LogPath, "   " + e + Environment.NewLine);
    }
}
