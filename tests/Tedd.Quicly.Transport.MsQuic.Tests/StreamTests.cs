using System.Runtime.InteropServices;
using System.Text;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

[Collection(MsQuicCollection.Name)]
public class StreamTests
{
    private const int OneMiB = 1024 * 1024;
    private const int ChunkSize = 64 * 1024;

    /// <summary>Echoes everything back; copies each receive into a native block that is freed on SEND_COMPLETE.</summary>
    private sealed unsafe class EchoStreamEvents : IMsQuicStreamEvents
    {
        public readonly TaskCompletionSource<MsQuicStreamShutdownInfo> ShutdownCompleteTcs = TestTimeouts.NewTcs<MsQuicStreamShutdownInfo>();
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

        public void ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info) => ShutdownCompleteTcs.TrySetResult(info);
    }

    /// <summary>Verifies received bytes against <see cref="NativeBlock.PatternAt"/> by absolute stream offset.</summary>
    private sealed unsafe class PatternVerifyingStreamEvents : IMsQuicStreamEvents
    {
        public readonly TaskCompletionSource<(int Status, ulong Id, bool PeerAccepted)> StartCompleteTcs = TestTimeouts.NewTcs<(int, ulong, bool)>();
        public readonly TaskCompletionSource<(nint Context, bool Canceled)> SendCompleteTcs = TestTimeouts.NewTcs<(nint, bool)>();
        public readonly TaskCompletionSource<bool> PeerSendShutdownTcs = TestTimeouts.NewTcs<bool>();
        public readonly TaskCompletionSource<bool> SendShutdownCompleteTcs = TestTimeouts.NewTcs<bool>();
        public readonly TaskCompletionSource<MsQuicStreamShutdownInfo> ShutdownCompleteTcs = TestTimeouts.NewTcs<MsQuicStreamShutdownInfo>();
        public long Verified;
        public long Mismatches;

        public void StartComplete(MsQuicStream stream, int status, ulong id, bool peerAccepted) => StartCompleteTcs.TrySetResult((status, id, peerAccepted));

        public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
        {
            long pos = (long)absoluteOffset;
            for (uint i = 0; i < bufferCount; i++)
            {
                Span<byte> span = buffers[i].Span;
                for (int j = 0; j < span.Length; j++)
                {
                    if (span[j] != NativeBlock.PatternAt(pos + j)) Mismatches++;
                }
                pos += span.Length;
            }
            Interlocked.Add(ref Verified, (long)totalLength);
            return MsQuicReceiveResult.Consumed(totalLength);
        }

        public void SendComplete(MsQuicStream stream, void* clientContext, bool canceled) => SendCompleteTcs.TrySetResult(((nint)clientContext, canceled));
        public void PeerSendShutdown(MsQuicStream stream) => PeerSendShutdownTcs.TrySetResult(true);
        public void SendShutdownComplete(MsQuicStream stream, bool graceful) => SendShutdownCompleteTcs.TrySetResult(graceful);
        public void ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info) => ShutdownCompleteTcs.TrySetResult(info);
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

        var clientStreamEvents = new PatternVerifyingStreamEvents();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, clientStreamEvents, out MsQuicStream? clientStream));
        Assert.NotNull(clientStream);
        loopback.Track(clientStream);
        Assert.Equal(ulong.MaxValue, clientStream.Id);
        Assert.True(MsQuicStatus.Failed(clientStream.QueryId(out _)));
        Assert.False(clientStream.IsPeerStarted);
        Assert.False(clientStream.IsUnidirectional);
        Assert.Same(client, clientStream.Connection);

        TestStatus.AssertAccepted(clientStream.Start(QUIC_STREAM_START_FLAGS.NONE));
        (int startStatus, ulong id, _) = await clientStreamEvents.StartCompleteTcs.Within();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, startStatus);
        Assert.Equal(id, clientStream.Id);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, clientStream.QueryId(out ulong queried));
        Assert.Equal(id, queried);

        int chunks = OneMiB / ChunkSize;
        var blocks = new NativeBlock[chunks];
        using var buffers = new NativeBuffers(chunks);
        for (int i = 0; i < chunks; i++)
        {
            blocks[i] = new NativeBlock(ChunkSize);
            blocks[i].FillPattern((long)i * ChunkSize);
            buffers.Set(i, blocks[i], 0, ChunkSize);
        }
        try
        {
            TestStatus.AssertAccepted(buffers.SendOn(clientStream, QUIC_SEND_FLAGS.FIN, 42));

            (nint context, bool canceled) = await clientStreamEvents.SendCompleteTcs.Within();
            Assert.Equal(42, context);
            Assert.False(canceled);

            await clientStreamEvents.PeerSendShutdownTcs.Within();
            Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref clientStreamEvents.Verified) == OneMiB, TestTimeouts.Default), $"verified {clientStreamEvents.Verified}");
            Assert.Equal(0, clientStreamEvents.Mismatches);

            Assert.True(await clientStreamEvents.SendShutdownCompleteTcs.Within());
            MsQuicStreamShutdownInfo clientInfo = await clientStreamEvents.ShutdownCompleteTcs.Within();
            Assert.False(clientInfo.ConnectionShutdown);
            Assert.False(clientInfo.AppCloseInProgress);

            Assert.NotNull(echo);
            MsQuicStreamShutdownInfo serverInfo = await echo.ShutdownCompleteTcs.Within();
            Assert.False(serverInfo.ConnectionShutdown);
            Assert.Equal(OneMiB, echo.Received);
            MsQuicStream serverStream = await serverEvents.FirstPeerStreamTcs.Within();
            Assert.True(serverStream.IsPeerStarted);
            Assert.Equal(clientStream.Id, serverStream.Id);
            Assert.Equal(0UL, clientStream.Id);

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
        TestStatus.AssertAccepted(serverStream.Start(QUIC_STREAM_START_FLAGS.IMMEDIATE | QUIC_STREAM_START_FLAGS.PRIORITY_WORK));
        (int status, ulong id, _) = await serverStreamEvents.StartCompleteTcs.Within();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, status);
        Assert.Equal(3UL, id);

        byte[] hello = Encoding.ASCII.GetBytes("hello from server");
        using var block = new NativeBlock(hello.Length);
        block.CopyFrom(hello);
        using NativeBuffers buffer = NativeBuffers.Single(block);
        TestStatus.AssertAccepted(buffer.SendOn(serverStream, QUIC_SEND_FLAGS.FIN | QUIC_SEND_FLAGS.DELAY_SEND, 0));

        MsQuicStream clientStream = await clientEvents.FirstPeerStreamTcs.Within();
        Assert.True(clientStream.IsPeerStarted);
        Assert.True(clientStream.IsUnidirectional);
        Assert.Equal(id, clientStream.Id);
        var clientStreamEvents = (StreamRecorder)clientStream.Events;
        await clientStreamEvents.FinTcs.Within();
        Assert.Equal("hello from server", Encoding.ASCII.GetString(clientStreamEvents.Received.ToArray()));
        await clientStreamEvents.PeerSendShutdownTcs.Within();
        await clientStreamEvents.ShutdownCompleteTcs.Within();
        await serverStreamEvents.AllSendsCompleteTcs.Within();
        Assert.True(await serverStreamEvents.SendShutdownCompleteTcs.Within());
        await serverStreamEvents.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public async Task Stream_limit_is_reported_with_fail_blocked_and_lifted_by_update_settings()
    {
        MsQuicSettings serverSettings = Loopback.TestServerSettings();
        serverSettings.PeerUnidiStreamCount = 1;
        using var loopback = new Loopback(serverSettings);
        (MsQuicConnection client, ConnectionRecorder clientEvents, MsQuicConnection server, _) = await loopback.ConnectPairAsync();
        (_, ushort unidi) = await clientEvents.StreamsAvailableTcs.Within();
        Assert.Equal(1, unidi);

        var first = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, first, out MsQuicStream? firstStream));
        loopback.Track(firstStream!);
        TestStatus.AssertAccepted(firstStream!.Start(QUIC_STREAM_START_FLAGS.FAIL_BLOCKED | QUIC_STREAM_START_FLAGS.SHUTDOWN_ON_FAIL));
        (int status, _, _) = await first.StartCompleteTcs.Within();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, status);

        var second = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, second, out MsQuicStream? secondStream));
        loopback.Track(secondStream!);
        status = secondStream!.Start(QUIC_STREAM_START_FLAGS.FAIL_BLOCKED | QUIC_STREAM_START_FLAGS.SHUTDOWN_ON_FAIL);
        // Off the worker thread the start is queued (PENDING); inline it fails straight away. Either way StartComplete reports the limit.
        Assert.True(status == MsQuicStatus.QUIC_STATUS_STREAM_LIMIT_REACHED || status == MsQuicStatus.QUIC_STATUS_PENDING, MsQuicStatus.GetName(status));
        (status, _, _) = await second.StartCompleteTcs.Within();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_STREAM_LIMIT_REACHED, status);
        await second.ShutdownCompleteTcs.Within();

        // Raise the limit on the server; the client sees STREAMS_AVAILABLE and can start another stream.
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, server.UpdatePeerStreamLimits(16, 2));
        Assert.True(await TestTimeouts.WaitUntilAsync(() => clientEvents.StreamsAvailableHistory.Count >= 2, TestTimeouts.Default));

        var third = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, third, out MsQuicStream? thirdStream));
        loopback.Track(thirdStream!);
        TestStatus.AssertAccepted(thirdStream!.Start(QUIC_STREAM_START_FLAGS.FAIL_BLOCKED | QUIC_STREAM_START_FLAGS.SHUTDOWN_ON_FAIL));
        (status, _, _) = await third.StartCompleteTcs.Within();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, status);

        firstStream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0);
        thirdStream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0);
        await first.ShutdownCompleteTcs.Within();
        await third.ShutdownCompleteTcs.Within();
    }

    private sealed unsafe class BackpressureStreamEvents : IMsQuicStreamEvents
    {
        public readonly TaskCompletionSource<ulong> FirstReceiveTcs = TestTimeouts.NewTcs<ulong>();
        public readonly TaskCompletionSource<bool> FinTcs = TestTimeouts.NewTcs<bool>();
        public int Calls;
        public long BytesAfterFirst;

        public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
        {
            int call = Interlocked.Increment(ref Calls);
            if (call == 1)
            {
                FirstReceiveTcs.TrySetResult(totalLength);
                return MsQuicReceiveResult.Pending;
            }
            Interlocked.Add(ref BytesAfterFirst, (long)totalLength);
            if ((flags & QUIC_RECEIVE_FLAGS.FIN) != 0) FinTcs.TrySetResult(true);
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
        using NativeBuffers bufA = NativeBuffers.Single(a);
        using NativeBuffers bufB = NativeBuffers.Single(b);
        TestStatus.AssertAccepted(bufA.SendOn(clientStream!, QUIC_SEND_FLAGS.START, 1));

        MsQuicStream serverStream = await serverEvents.FirstPeerStreamTcs.Within();
        Assert.NotNull(serverStreamEvents);
        ulong held = await serverStreamEvents.FirstReceiveTcs.Within();
        Assert.True(held > 0);

        TestStatus.AssertAccepted(bufB.SendOn(clientStream!, QUIC_SEND_FLAGS.FIN, 2));
        await Task.Delay(300);
        Assert.Equal(1, Volatile.Read(ref serverStreamEvents.Calls));
        Assert.False(serverStreamEvents.FinTcs.Task.IsCompleted);

        serverStream.ReceiveComplete(held);
        await serverStreamEvents.FinTcs.Within();
        Assert.Equal(2L * ChunkSize, (long)held + Volatile.Read(ref serverStreamEvents.BytesAfterFirst));
        Assert.True(serverStreamEvents.Calls >= 2);

        await clientStreamEvents.AllSendsCompleteTcs.Within();
        serverStream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.GRACEFUL, 0);
        await clientStreamEvents.PeerSendShutdownTcs.Within();
        await clientStreamEvents.ShutdownCompleteTcs.Within();
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
        using NativeBuffers buf = NativeBuffers.Single(a);
        TestStatus.AssertAccepted(buf.SendOn(clientStream!, QUIC_SEND_FLAGS.START, 1));

        MsQuicStream serverStream = await serverEvents.FirstPeerStreamTcs.Within();
        var serverStreamEvents = (StreamRecorder)serverStream.Events;
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref serverStreamEvents.ReceivedBytes) == 1000, TestTimeouts.Default));

        TestStatus.AssertAccepted(serverStream.ReceiveSetEnabled(false));
        TestStatus.AssertAccepted(buf.SendOn(clientStream!, QUIC_SEND_FLAGS.FIN, 2));
        await Task.Delay(300);
        Assert.Equal(1000, Volatile.Read(ref serverStreamEvents.ReceivedBytes));

        TestStatus.AssertAccepted(serverStream.ReceiveSetEnabled(true));
        await serverStreamEvents.FinTcs.Within();
        Assert.Equal(2000, Volatile.Read(ref serverStreamEvents.ReceivedBytes));
        await clientStreamEvents.AllSendsCompleteTcs.Within();
        serverStream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.GRACEFUL, 0);
        await clientStreamEvents.ShutdownCompleteTcs.Within();
        await serverStreamEvents.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public async Task Abortive_shutdown_propagates_error_codes_in_both_directions()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, _, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        var clientEvents1 = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, clientEvents1, out MsQuicStream? clientStream1));
        loopback.Track(clientStream1!);
        using var block = new NativeBlock(100);
        using NativeBuffers buf = NativeBuffers.Single(block);
        TestStatus.AssertAccepted(buf.SendOn(clientStream1!, QUIC_SEND_FLAGS.START, 0));
        MsQuicStream serverStream1 = await serverEvents.FirstPeerStreamTcs.Within();
        var serverEvents1 = (StreamRecorder)serverStream1.Events;
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref serverEvents1.ReceivedBytes) == 100, TestTimeouts.Default));

        TestStatus.AssertAccepted(clientStream1!.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT_SEND, 0x77));
        Assert.Equal(0x77UL, await serverEvents1.PeerSendAbortedTcs.Within());
        Assert.False(await clientEvents1.SendShutdownCompleteTcs.Within());

        TestStatus.AssertAccepted(serverStream1.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT_SEND, 0x99));
        Assert.Equal(0x99UL, await clientEvents1.PeerSendAbortedTcs.Within());
        await clientEvents1.ShutdownCompleteTcs.Within();
        await serverEvents1.ShutdownCompleteTcs.Within();

        var clientEvents2 = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, clientEvents2, out MsQuicStream? clientStream2));
        loopback.Track(clientStream2!);
        TestStatus.AssertAccepted(buf.SendOn(clientStream2!, QUIC_SEND_FLAGS.START, 0));
        MsQuicStream serverStream2 = await TestTimeouts.WaitUntilAsync(() => serverEvents.PeerStreams.Count == 2, TestTimeouts.Default)
            ? serverEvents.PeerStreams.Single(s => s != serverStream1)
            : throw new TimeoutException("second peer stream");
        TestStatus.AssertAccepted(serverStream2.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT_RECEIVE, 0x88));
        Assert.Equal(0x88UL, await clientEvents2.PeerReceiveAbortedTcs.Within());
        TestStatus.AssertAccepted(clientStream2!.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0x11));
        TestStatus.AssertAccepted(serverStream2.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0x22));
        await clientEvents2.ShutdownCompleteTcs.Within();
        await ((StreamRecorder)serverStream2.Events).ShutdownCompleteTcs.Within();
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
        using NativeBuffers buf = NativeBuffers.Single(block);
        TestStatus.AssertAccepted(buf.SendOn(clientStream!, QUIC_SEND_FLAGS.START, 0));

        Task aborted = Task.WhenAny(clientStreamEvents.PeerReceiveAbortedTcs.Task, clientStreamEvents.PeerSendAbortedTcs.Task);
        await aborted.WaitAsync(TestTimeouts.Default);
        Assert.Empty(serverEvents.PeerStreams);
        Assert.False(serverEvents.FirstPeerStreamTcs.Task.IsCompleted);
        TestStatus.AssertAccepted(clientStream!.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0));
        await clientStreamEvents.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public async Task Peer_stream_handler_exception_rejects_the_stream_and_poisons_the_connection()
    {
        using var loopback = new Loopback { ServerEventsFactory = _ => new ConnectionRecorder { ThrowOnPeerStream = true } };
        (MsQuicConnection client, ConnectionRecorder clientEvents, MsQuicConnection server, _) = await loopback.ConnectPairAsync();
        var clientStreamEvents = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, clientStreamEvents, out MsQuicStream? clientStream));
        loopback.Track(clientStream!);
        using var block = new NativeBlock(10);
        using NativeBuffers buf = NativeBuffers.Single(block);
        TestStatus.AssertAccepted(buf.SendOn(clientStream!, QUIC_SEND_FLAGS.START, 0));

        Assert.True(await TestTimeouts.WaitUntilAsync(() => server.IsPoisoned, TestTimeouts.Default));
        Assert.IsType<InvalidOperationException>(server.LastCallbackException);
        Assert.Equal(MsQuicConnection.CallbackFailureErrorCode, await clientEvents.PeerShutdownTcs.Within());
        await clientEvents.ShutdownCompleteTcs.Within();
        await clientStreamEvents.ShutdownCompleteTcs.Within();
    }


    [Fact]
    public async Task Stream_parameters_and_lifetime_rules()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, _, _, _) = await loopback.ConnectPairAsync();

        var events = new StreamRecorder();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, events, out MsQuicStream? stream));
        Assert.NotNull(stream);
        loopback.Track(stream);
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
        Assert.Throws<ArgumentException>(() => stream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT | QUIC_STREAM_SHUTDOWN_FLAGS.INLINE, 0));

        TestStatus.AssertAccepted(stream.Start(QUIC_STREAM_START_FLAGS.NONE));
        await events.StartCompleteTcs.Within();
        TestStatus.AssertAccepted(stream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 1));
        await events.ShutdownCompleteTcs.Within();

        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, null, out MsQuicStream? unstarted));
        unstarted!.Close();
        Assert.True(unstarted.IsClosed);

        stream.Close();
        stream.Close();
        Assert.True(stream.IsClosed);
        AssertClosedStreamThrows(stream);
    }

    private static unsafe void AssertClosedStreamThrows(MsQuicStream stream)
    {
        ushort mid = 0x1000;
        Assert.Throws<ObjectDisposedException>(() => stream.Start(QUIC_STREAM_START_FLAGS.NONE));
        Assert.Throws<ObjectDisposedException>(() => stream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0));
        Assert.Throws<ObjectDisposedException>(() => stream.ReceiveComplete(0));
        Assert.Throws<ObjectDisposedException>(() => stream.ReceiveSetEnabled(true));
        Assert.Throws<ObjectDisposedException>(() => stream.SetPriority(1));
        Assert.Throws<ObjectDisposedException>(() => stream.QueryId(out _));
        Assert.Throws<ObjectDisposedException>(() => stream.Send(null, 0, QUIC_SEND_FLAGS.NONE, null));
        Assert.Throws<ObjectDisposedException>(() => stream.SetParam(MsQuicParam.QUIC_PARAM_STREAM_PRIORITY, in mid));
        Assert.Throws<ObjectDisposedException>(() => stream.GetParam(MsQuicParam.QUIC_PARAM_STREAM_PRIORITY, out ushort _));
    }

    [Fact]
    public async Task Stream_handler_exceptions_are_recorded_and_poison_the_connection()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, _, _) = await loopback.ConnectPairAsync();
        var events = new ThrowingStreamEvents();
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, events, out MsQuicStream? stream));
        loopback.Track(stream!);
        TestStatus.AssertAccepted(stream!.Start(QUIC_STREAM_START_FLAGS.NONE));
        Assert.True(await TestTimeouts.WaitUntilAsync(() => stream.LastCallbackException is not null, TestTimeouts.Default));
        Assert.IsType<InvalidOperationException>(stream.LastCallbackException);
        Assert.Same(stream.LastCallbackException, client.LastCallbackException);
        Assert.True(client.IsPoisoned);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => events.ShutdownCompleted, TestTimeouts.Default));
        await clientEvents.ShutdownCompleteTcs.Within();
    }

    private sealed class ThrowingStreamEvents : IMsQuicStreamEvents
    {
        public volatile bool ShutdownCompleted;
        public void StartComplete(MsQuicStream stream, int status, ulong id, bool peerAccepted) => throw new InvalidOperationException("boom");
        public void ShutdownComplete(MsQuicStream stream, in MsQuicStreamShutdownInfo info) => ShutdownCompleted = true;
    }

    [Fact]
    public async Task Stream_close_from_a_callback_thread_is_refused()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, _, _, _) = await loopback.ConnectPairAsync();
        Exception? caught = null;
        var events = new StreamRecorder
        {
            OnShutdownComplete = stream =>
            {
                try
                {
                    stream.Close();
                }
                catch (Exception ex)
                {
                    caught = ex;
                }
            },
        };
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, events, out MsQuicStream? stream));
        loopback.Track(stream!);
        TestStatus.AssertAccepted(stream!.Start(QUIC_STREAM_START_FLAGS.NONE));
        await events.StartCompleteTcs.Within();
        TestStatus.AssertAccepted(stream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.ABORT, 0));
        await events.ShutdownCompleteTcs.Within();
        Assert.IsType<InvalidOperationException>(caught);
        Assert.False(stream.IsClosed);
        Assert.Null(stream.LastCallbackException);
    }

    /// <summary>Counts events and samples per-thread GC allocation between consecutive callbacks on the same worker thread.</summary>
    private sealed unsafe class ProbeStreamEvents : IMsQuicStreamEvents
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

    private static long SendMeasured(MsQuicStream stream, NativeBuffers buffers, int first, int count)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < count; i++)
        {
            int status = buffers.SendOn(stream, first + i == 0 ? QUIC_SEND_FLAGS.START : QUIC_SEND_FLAGS.NONE, first + i + 1);
            if (MsQuicStatus.Failed(status)) throw new MsQuicException(status, "StreamSend");
        }
        return GC.GetAllocatedBytesForCurrentThread() - before;
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
        using NativeBuffers buf = NativeBuffers.Single(block);

        const int warmup = 200;
        const int measured = 2000;
        SendMeasured(stream!, buf, 0, warmup);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref clientProbe.SendCompletes) == warmup, TestTimeouts.Default));
        Assert.NotNull(serverProbe);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref serverProbe.ReceivedBytes) == warmup * 1024L, TestTimeouts.Default));

        clientProbe.Probe.Arm();
        serverProbe.Probe.Arm();
        long senderAllocated = SendMeasured(stream!, buf, warmup, measured);

        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref clientProbe.SendCompletes) == warmup + measured, TestTimeouts.Default));
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref serverProbe.ReceivedBytes) == (warmup + measured) * 1024L, TestTimeouts.Default));

        Assert.Equal(0, senderAllocated);
        Assert.Equal(0, clientProbe.Probe.Allocated);
        Assert.Equal(0, serverProbe.Probe.Allocated);
        Assert.True(clientProbe.Probe.Samples > measured / 2, $"client samples {clientProbe.Probe.Samples}");
        Assert.True(serverProbe.Probe.Samples > 0, $"server samples {serverProbe.Probe.Samples}");

        stream!.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.GRACEFUL, 0);
        MsQuicStream serverStream = await serverEvents.FirstPeerStreamTcs.Within();
        serverStream.Shutdown(QUIC_STREAM_SHUTDOWN_FLAGS.GRACEFUL, 0);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => clientProbe.ShutdownCompleted && serverProbe.ShutdownCompleted, TestTimeouts.Default));
    }
}
