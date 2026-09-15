using System.Runtime.InteropServices;
using System.Text;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

public unsafe class StreamTests
{
    private const int OneMiB = 1024 * 1024;
    private const int ChunkSize = 64 * 1024;

    /// <summary>Echoes everything back; copies each receive into a native block that is freed on SEND_COMPLETE.</summary>
    private sealed class EchoStreamEvents : IMsQuicStreamEvents
    {
        public readonly TaskCompletionSource<MsQuicStreamShutdownInfo> ShutdownComplete = TestTimeouts.NewTcs<MsQuicStreamShutdownInfo>();
        public long Received;
        public MsQuicStream? Stream;

        public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
        {
            Stream = stream;
            Received += (long)totalLength;
            if (totalLength > 0)
            {
                // One block: [QUIC_BUFFER header][payload]; the header pointer doubles as the send context.
                var block = (QUIC_BUFFER*)NativeMemory.Alloc((nuint)(sizeof(QUIC_BUFFER) + (int)totalLength));
                byte* payload = (byte*)(block + 1);
                block->Buffer = payload;
                block->Length = (uint)totalLength;
                for (uint i = 0; i < bufferCount; i++)
                {
                    buffers[i].Span.CopyTo(new Span<byte>(payload, (int)buffers[i].Length));
                    payload += buffers[i].Length;
                }
                int status = stream.Send(block, 1, QUIC_SEND_FLAGS.NONE, block);
                if (MsQuicStatus.Failed(status)) NativeMemory.Free(block);
            }
            if ((flags & QUIC_RECEIVE_FLAGS.FIN) != 0)
            {
                stream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.GRACEFUL, 0);
            }
            return MsQuicReceiveResult.Consumed(totalLength);
        }

        public void SendComplete(MsQuicStream stream, void* clientContext, bool canceled) => NativeMemory.Free(clientContext);

        public void ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info) => this.ShutdownComplete.TrySetResult(info);
    }

    [Fact]
    public async Task Bidirectional_stream_echoes_one_mebibyte_sent_as_gathered_buffers()
    {
        EchoStreamEvents? echo = null;
        using var loopback = new Loopback
        {
            ServerEventsFactory = _ => new ConnectionRecorder { PeerStreamEventsFactory = _ => echo = new EchoStreamEvents() },
        };
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        long verified = 0;
        long mismatches = 0;
        var clientStreamEvents = new StreamRecorder
        {
            OnReceive = (stream, buffers, count, offset, total, flags) =>
            {
                long pos = (long)offset;
                for (uint i = 0; i < count; i++)
                {
                    Span<byte> span = buffers[i].Span;
                    for (int j = 0; j < span.Length; j++)
                    {
                        if (span[j] != NativeBlock.PatternAt(pos + j)) mismatches++;
                    }
                    pos += span.Length;
                }
                verified += (long)total;
                return MsQuicReceiveResult.Consumed(total);
            },
        };
        // The recorder's Fin slot is only set by the default handler; watch PeerSendShutdown instead.
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, clientStreamEvents, out MsQuicStream? clientStream));
        Assert.NotNull(clientStream);
        loopback.Track(clientStream);
        Assert.Equal(ulong.MaxValue, clientStream.Id);
        Assert.False(clientStream.IsPeerStarted);
        Assert.False(clientStream.IsUnidirectional);
        Assert.Same(client, clientStream.Connection);

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream.Start(QUIC_STREAM_START_FLAGS.NONE));
        (int startStatus, ulong id, _) = await clientStreamEvents.StartComplete.Within();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, startStatus);
        Assert.Equal(id, clientStream.Id);

        int chunks = OneMiB / ChunkSize;
        var blocks = new NativeBlock[chunks];
        QUIC_BUFFER* buffers = stackalloc QUIC_BUFFER[chunks];
        for (int i = 0; i < chunks; i++)
        {
            blocks[i] = new NativeBlock(ChunkSize);
            blocks[i].FillPattern((long)i * ChunkSize);
            buffers[i] = new QUIC_BUFFER(blocks[i].Pointer, ChunkSize);
        }
        try
        {
            Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream.Send(buffers, (uint)chunks, QUIC_SEND_FLAGS.FIN, (void*)42));

            await clientStreamEvents.AllSendsComplete.Within();
            Assert.True(clientStreamEvents.SendCompletes.TryDequeue(out (nint Context, bool Canceled) sc));
            Assert.Equal(42, sc.Context);
            Assert.False(sc.Canceled);

            await clientStreamEvents.PeerSendShutdown.Within();
            Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref verified) == OneMiB, TestTimeouts.Default), $"verified {verified}");
            Assert.Equal(0, mismatches);

            Assert.True(await clientStreamEvents.SendShutdownComplete.Within());
            MsQuicStreamShutdownInfo clientInfo = await clientStreamEvents.ShutdownComplete.Within();
            Assert.False(clientInfo.ConnectionShutdown);
            Assert.False(clientInfo.AppCloseInProgress);

            Assert.NotNull(echo);
            MsQuicStreamShutdownInfo serverInfo = await echo.ShutdownComplete.Within();
            Assert.False(serverInfo.ConnectionShutdown);
            Assert.Equal(OneMiB, echo.Received);
            MsQuicStream serverStream = await serverEvents.FirstPeerStream.Within();
            Assert.True(serverStream.IsPeerStarted);
            Assert.Equal(clientStream.Id, serverStream.Id);
            Assert.Equal(0UL, clientStream.Id); // first client-initiated bidirectional stream

            serverStream.Close();
            clientStream.Close();
            Assert.True(clientStream.IsClosed);
        }
        finally
        {
            foreach (NativeBlock b in blocks) b.Dispose();
        }
    }

    [Fact]
    public async Task Server_initiated_unidirectional_stream_reaches_client()
    {
        using var loopback = new Loopback();
        (_, ConnectionRecorder clientEvents, MsQuicConnection server, _) = await loopback.ConnectPairAsync();

        var serverStreamEvents = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, server.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, serverStreamEvents, out MsQuicStream? serverStream));
        loopback.Track(serverStream!);
        Assert.True(serverStream!.IsUnidirectional);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, serverStream.Start(QUIC_STREAM_START_FLAGS.IMMEDIATE));
        (int status, ulong id, _) = await serverStreamEvents.StartComplete.Within();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, status);
        Assert.Equal(3UL, id); // first server-initiated unidirectional stream

        byte[] hello = Encoding.ASCII.GetBytes("hello from server");
        using var block = new NativeBlock(hello.Length);
        hello.CopyTo(block.Span);
        QUIC_BUFFER buffer = new(block.Pointer, (uint)hello.Length);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, serverStream.Send(&buffer, 1, QUIC_SEND_FLAGS.FIN, null));

        MsQuicStream clientStream = await clientEvents.FirstPeerStream.Within();
        Assert.True(clientStream.IsPeerStarted);
        Assert.True(clientStream.IsUnidirectional);
        Assert.Equal(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, clientStream.OpenFlags & QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL);
        Assert.Equal(id, clientStream.Id);
        var clientStreamEvents = (StreamRecorder)clientStream.Events;
        await clientStreamEvents.Fin.Within();
        Assert.Equal("hello from server", Encoding.ASCII.GetString(clientStreamEvents.Received.ToArray()));
        await clientStreamEvents.PeerSendShutdown.Within();
        await clientStreamEvents.ShutdownComplete.Within();
        await serverStreamEvents.AllSendsComplete.Within();
        Assert.True(await serverStreamEvents.SendShutdownComplete.Within());
        await serverStreamEvents.ShutdownComplete.Within();
    }

    private sealed class BackpressureStreamEvents : IMsQuicStreamEvents
    {
        public readonly TaskCompletionSource<ulong> FirstReceive = TestTimeouts.NewTcs<ulong>();
        public readonly TaskCompletionSource<bool> Fin = TestTimeouts.NewTcs<bool>();
        public int Calls;
        public long BytesAfterFirst;

        public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
        {
            int call = Interlocked.Increment(ref Calls);
            if (call == 1)
            {
                FirstReceive.TrySetResult(totalLength);
                return MsQuicReceiveResult.Pending;
            }
            Interlocked.Add(ref BytesAfterFirst, (long)totalLength);
            if ((flags & QUIC_RECEIVE_FLAGS.FIN) != 0) Fin.TrySetResult(true);
            return MsQuicReceiveResult.Consumed(totalLength);
        }
    }

    [Fact]
    public async Task Pending_receive_pauses_delivery_until_receive_complete()
    {
        BackpressureStreamEvents? serverStreamEvents = null;
        using var loopback = new Loopback
        {
            ServerEventsFactory = _ => new ConnectionRecorder { PeerStreamEventsFactory = _ => serverStreamEvents = new BackpressureStreamEvents() },
        };
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        var clientStreamEvents = new StreamRecorder { ExpectedSends = 2 };
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, clientStreamEvents, out MsQuicStream? clientStream));
        loopback.Track(clientStream!);
        using var a = new NativeBlock(ChunkSize);
        using var b = new NativeBlock(ChunkSize);
        a.FillPattern(0);
        b.FillPattern(ChunkSize);
        QUIC_BUFFER bufA = new(a.Pointer, ChunkSize);
        QUIC_BUFFER bufB = new(b.Pointer, ChunkSize);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream!.Send(&bufA, 1, QUIC_SEND_FLAGS.START, (void*)1));

        MsQuicStream serverStream = await serverEvents.FirstPeerStream.Within();
        Assert.NotNull(serverStreamEvents);
        ulong held = await serverStreamEvents.FirstReceive.Within();
        Assert.True(held > 0);

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream.Send(&bufB, 1, QUIC_SEND_FLAGS.FIN, (void*)2));
        await Task.Delay(300);
        Assert.Equal(1, Volatile.Read(ref serverStreamEvents.Calls));
        Assert.False(serverStreamEvents.Fin.Task.IsCompleted);

        serverStream.ReceiveComplete(held);
        await serverStreamEvents.Fin.Within();
        Assert.Equal(2L * ChunkSize, (long)held + Volatile.Read(ref serverStreamEvents.BytesAfterFirst));
        Assert.True(serverStreamEvents.Calls >= 2);

        await clientStreamEvents.AllSendsComplete.Within();
        serverStream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.GRACEFUL, 0);
        await clientStreamEvents.PeerSendShutdown.Within();
        await clientStreamEvents.ShutdownComplete.Within();
    }

    [Fact]
    public async Task Receive_set_enabled_false_pauses_and_true_resumes()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        var clientStreamEvents = new StreamRecorder { ExpectedSends = 2 };
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, clientStreamEvents, out MsQuicStream? clientStream));
        loopback.Track(clientStream!);
        using var a = new NativeBlock(1000);
        a.FillPattern(0);
        QUIC_BUFFER buf = new(a.Pointer, 1000);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream!.Send(&buf, 1, QUIC_SEND_FLAGS.START, (void*)1));

        MsQuicStream serverStream = await serverEvents.FirstPeerStream.Within();
        var serverStreamEvents = (StreamRecorder)serverStream.Events;
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref serverStreamEvents.ReceivedBytes) == 1000, TestTimeouts.Default));

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, serverStream.ReceiveSetEnabled(false));
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream.Send(&buf, 1, QUIC_SEND_FLAGS.FIN, (void*)2));
        await Task.Delay(300);
        Assert.Equal(1000, Volatile.Read(ref serverStreamEvents.ReceivedBytes));

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, serverStream.ReceiveSetEnabled(true));
        await serverStreamEvents.Fin.Within();
        Assert.Equal(2000, Volatile.Read(ref serverStreamEvents.ReceivedBytes));
        await clientStreamEvents.AllSendsComplete.Within();
        serverStream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.GRACEFUL, 0);
        await clientStreamEvents.ShutdownComplete.Within();
        await serverStreamEvents.ShutdownComplete.Within();
    }

    [Fact]
    public async Task Abortive_shutdown_propagates_error_codes_in_both_directions()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        // Stream 1: the client resets its send direction; the server then resets its own.
        var clientEvents1 = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, clientEvents1, out MsQuicStream? clientStream1));
        loopback.Track(clientStream1!);
        using var block = new NativeBlock(100);
        QUIC_BUFFER buf = new(block.Pointer, 100);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream1!.Send(&buf, 1, QUIC_SEND_FLAGS.START, null));
        MsQuicStream serverStream1 = await serverEvents.FirstPeerStream.Within();
        var serverEvents1 = (StreamRecorder)serverStream1.Events;
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref serverEvents1.ReceivedBytes) == 100, TestTimeouts.Default));

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream1.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT_SEND, 0x77));
        Assert.Equal(0x77UL, await serverEvents1.PeerSendAborted.Within());
        Assert.False(await clientEvents1.SendShutdownComplete.Within()); // not graceful

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, serverStream1.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT_SEND, 0x99));
        Assert.Equal(0x99UL, await clientEvents1.PeerSendAborted.Within());
        await clientEvents1.ShutdownComplete.Within();
        await serverEvents1.ShutdownComplete.Within();

        // Stream 2: the server aborts its receive direction (STOP_SENDING) with a code the client observes.
        var clientEvents2 = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, clientEvents2, out MsQuicStream? clientStream2));
        loopback.Track(clientStream2!);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream2!.Send(&buf, 1, QUIC_SEND_FLAGS.START, null));
        MsQuicStream serverStream2 = await TestTimeouts.WaitUntilAsync(() => serverEvents.PeerStreams.Count == 2, TestTimeouts.Default)
            ? serverEvents.PeerStreams.Single(s => s != serverStream1)
            : throw new TimeoutException("second peer stream");
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, serverStream2.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT_RECEIVE, 0x88));
        Assert.Equal(0x88UL, await clientEvents2.PeerReceiveAborted.Within());
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream2.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0x11));
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, serverStream2.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0x22));
        await clientEvents2.ShutdownComplete.Within();
        await ((StreamRecorder)serverStream2.Events).ShutdownComplete.Within();
    }

    [Fact]
    public async Task Rejected_peer_stream_is_closed_by_the_wrapper()
    {
        using var loopback = new Loopback { ServerEventsFactory = _ => new ConnectionRecorder { AcceptPeerStreams = false } };
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        var clientStreamEvents = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, clientStreamEvents, out MsQuicStream? clientStream));
        loopback.Track(clientStream!);
        using var block = new NativeBlock(10);
        QUIC_BUFFER buf = new(block.Pointer, 10);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream!.Send(&buf, 1, QUIC_SEND_FLAGS.START, null));

        Task aborted = Task.WhenAny(clientStreamEvents.PeerReceiveAborted.Task, clientStreamEvents.PeerSendAborted.Task);
        await aborted.WaitAsync(TestTimeouts.Default);
        Assert.Empty(serverEvents.PeerStreams);
        Assert.False(serverEvents.FirstPeerStream.Task.IsCompleted);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0));
        await clientStreamEvents.ShutdownComplete.Within();
    }

    [Fact]
    public async Task Stream_parameters_and_lifetime_rules()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, _, _, _) = await loopback.ConnectPairAsync();

        var events = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, events, out MsQuicStream? stream));
        Assert.NotNull(stream);
        Assert.Same(events, stream.Events);
        stream.Events = null!;
        Assert.NotNull(stream.Events);
        stream.Events = events;
        stream.Tag = 5;
        Assert.Equal(5, stream.Tag);
        Assert.Null(stream.LastCallbackException);

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, stream.SetPriority(0xFFFF));
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, stream.GetParam(MsQuicParam.QUIC_PARAM_STREAM_PRIORITY, out ushort priority));
        Assert.Equal(0xFFFF, priority);
        ushort mid = 0x1000;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, stream.SetParam(MsQuicParam.QUIC_PARAM_STREAM_PRIORITY, in mid));
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, stream.GetParam(MsQuicParam.QUIC_PARAM_STREAM_PRIORITY, out priority));
        Assert.Equal(mid, priority);

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, stream.Start(QUIC_STREAM_START_FLAGS.NONE));
        await events.StartComplete.Within();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, stream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 1));
        await events.ShutdownComplete.Within();

        // Closing a never-started stream is fine too.
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, null, out MsQuicStream? unstarted));
        unstarted!.Close();
        Assert.True(unstarted.IsClosed);

        stream.Close();
        stream.Close();
        Assert.True(stream.IsClosed);
        Assert.Throws<ObjectDisposedException>(() => stream.Start(QUIC_STREAM_START_FLAGS.NONE));
        Assert.Throws<ObjectDisposedException>(() => stream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0));
        Assert.Throws<ObjectDisposedException>(() => stream.ReceiveComplete(0));
        Assert.Throws<ObjectDisposedException>(() => stream.ReceiveSetEnabled(true));
        Assert.Throws<ObjectDisposedException>(() => stream.SetPriority(1));
        Assert.Throws<ObjectDisposedException>(() => stream.SetParam(MsQuicParam.QUIC_PARAM_STREAM_PRIORITY, in mid));
        Assert.Throws<ObjectDisposedException>(() => stream.GetParam(MsQuicParam.QUIC_PARAM_STREAM_PRIORITY, out ushort _));
    }

    [Fact]
    public async Task Stream_handler_exceptions_are_recorded()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, _, _, _) = await loopback.ConnectPairAsync();
        var events = new ThrowingStreamEvents();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, events, out MsQuicStream? stream));
        loopback.Track(stream!);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, stream!.Start(QUIC_STREAM_START_FLAGS.NONE));
        Assert.True(await TestTimeouts.WaitUntilAsync(() => stream.LastCallbackException is not null, TestTimeouts.Default));
        Assert.IsType<InvalidOperationException>(stream.LastCallbackException);
        stream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => events.ShutdownCompleted, TestTimeouts.Default));
    }

    private sealed class ThrowingStreamEvents : IMsQuicStreamEvents
    {
        public volatile bool ShutdownCompleted;
        public void StartComplete(MsQuicStream stream, int status, ulong id, bool peerAccepted) => throw new InvalidOperationException("boom");
        public void ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info) => ShutdownCompleted = true;
    }

    /// <summary>Counts events and samples per-thread GC allocation between consecutive callbacks on the same worker thread.</summary>
    private sealed class ProbeStreamEvents : IMsQuicStreamEvents
    {
        public readonly CallbackAllocationProbe Probe = new();
        public int SendCompletes;
        public long ReceivedBytes;
        public volatile bool ShutdownCompleted;

        public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
        {
            Probe.Sample();
            Interlocked.Add(ref ReceivedBytes, (long)totalLength);
            return MsQuicReceiveResult.Consumed(totalLength);
        }

        public void SendComplete(MsQuicStream stream, void* clientContext, bool canceled)
        {
            Probe.Sample();
            Interlocked.Increment(ref SendCompletes);
        }

        public void ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info) => ShutdownCompleted = true;
    }

    [Fact]
    public async Task Stream_send_and_receive_paths_do_not_allocate_in_steady_state()
    {
        ProbeStreamEvents? serverProbe = null;
        using var loopback = new Loopback
        {
            ServerEventsFactory = _ => new ConnectionRecorder { PeerStreamEventsFactory = _ => serverProbe = new ProbeStreamEvents() },
        };
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        var clientProbe = new ProbeStreamEvents();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, clientProbe, out MsQuicStream? stream));
        loopback.Track(stream!);
        using var block = new NativeBlock(1024);
        block.FillPattern(0);
        QUIC_BUFFER buf = new(block.Pointer, 1024);

        const int warmup = 200;
        const int measured = 2000;
        for (int i = 0; i < warmup; i++)
        {
            Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, stream!.Send(&buf, 1, i == 0 ? QUIC_SEND_FLAGS.START : QUIC_SEND_FLAGS.NONE, (void*)(i + 1)));
        }
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref clientProbe.SendCompletes) == warmup, TestTimeouts.Default));
        Assert.NotNull(serverProbe);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref serverProbe.ReceivedBytes) == warmup * 1024L, TestTimeouts.Default));

        clientProbe.Probe.Arm();
        serverProbe.Probe.Arm();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < measured; i++)
        {
            int status = stream!.Send(&buf, 1, QUIC_SEND_FLAGS.NONE, (void*)(warmup + i + 1));
            if (status != MsQuicStatus.QUIC_STATUS_SUCCESS) throw new MsQuicException(status, "StreamSend");
        }
        long senderAllocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref clientProbe.SendCompletes) == warmup + measured, TestTimeouts.Default));
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref serverProbe.ReceivedBytes) == (warmup + measured) * 1024L, TestTimeouts.Default));

        Assert.Equal(0, senderAllocated);
        Assert.Equal(0, clientProbe.Probe.Allocated);
        Assert.Equal(0, serverProbe.Probe.Allocated);
        Assert.True(clientProbe.Probe.Samples > measured / 2, $"client samples {clientProbe.Probe.Samples}");
        Assert.True(serverProbe.Probe.Samples > 0, $"server samples {serverProbe.Probe.Samples}");

        stream!.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.GRACEFUL, 0);
        MsQuicStream serverStream = await serverEvents.FirstPeerStream.Within();
        serverStream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.GRACEFUL, 0);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => clientProbe.ShutdownCompleted && serverProbe.ShutdownCompleted, TestTimeouts.Default));
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
