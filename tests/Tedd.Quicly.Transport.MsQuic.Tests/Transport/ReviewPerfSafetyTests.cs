using System.Reflection;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>
/// Review findings (performance and safety lens): races between the game thread and MsQuic worker callbacks in the
/// stream table and its close protocol. The race windows are real but only a few instructions wide; each test holds one of
/// the transport's private locks from a helper thread (reflection) at exactly the point where production code holds it
/// (a thread-pool drain holds <c>_cleanupLock</c>, an allocation holds <c>_tableLock</c>), which keeps the window open long
/// enough for the other thread to walk into it.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class ReviewPerfSafetyTests
{
    private static readonly TimeSpan Timeout = TestTimeouts.Default;

    /// <summary>
    /// The game thread closes a stream that was never started while the connection reaches SHUTDOWN_COMPLETE.
    /// <c>CloseStream</c> sets AppClosed and, because the stream never started, decides to close the slot itself
    /// (<c>CloseOrDefer</c> → <c>CloseSlotNative</c>). Before it sets GuardClosing, the connection's shutdown sweep
    /// (<c>ShutdownRemainingStreams</c>) pins the same slot, sees AppClosed without NativeShutdown and queues a deferred
    /// close too. Both closers run <c>CloseSlotNative</c> → <c>FreeSlot</c>: the slot index is pushed onto the free list
    /// twice (and <c>FreeSlot</c> resets <c>DeferredNext</c> of a slot that may still be linked in the deferred stack).
    /// </summary>
    [Fact]
    public void Closing_a_never_started_stream_while_the_connection_shuts_down_frees_its_slot_exactly_once()
    {
        var harness = new MsQuicTransportHarness();
        LockHolder? holder = null;
        try
        {
            (MsQuicTransport client, MsQuicTransport server, RecordingSink clientSink) = Connect(harness);
            Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id));
            Assert.Equal(1, client.OpenStreamCount);

            // A running thread-pool drain holds _cleanupLock; the game thread's CloseStream queues up behind it.
            Lock cleanupLock = PrivateField<Lock>(client, "_cleanupLock");
            holder = new LockHolder(cleanupLock);
            var closer = new Thread(() => client.CloseStream(id)) { IsBackground = true, Name = "game thread" };
            closer.Start();
            Assert.True(Spin.Until(() => (SlotField<int>(client, id.Slot, "CloseFlags") & 1) != 0, Timeout), "CloseStream never marked the slot app-closed");

            // The peer closes the connection: the client's SHUTDOWN_COMPLETE sweeps the slot while CloseStream is in flight.
            server.Close(7, default);
            Assert.True(clientSink.WaitFor(static e => e.Kind == RecordedEventKind.Closed, Timeout), "client OnClosed");

            holder.Release();
            Assert.True(closer.Join(Timeout), "CloseStream did not return");
            // Wait for the deferred-close work item (if one was queued) to take the list, then for it to leave the lock.
            Assert.True(Spin.Until(() => PrivateField<int>(client, "_deferredHead") < 0, Timeout), "the deferred-close list was never drained");
            cleanupLock.Enter();
            cleanupLock.Exit();

            uint generation = SlotField<uint>(client, id.Slot, "Generation");
            Assert.True(
                client.OpenStreamCount == 0 && generation == id.Generation + 1,
                $"the slot was freed more than once: OpenStreamCount {client.OpenStreamCount} (expected 0), generation {generation} (expected {id.Generation + 1})");
        }
        finally
        {
            holder?.Dispose();
            harness.Dispose();
        }
        Assert.Null(harness.CleanupError);
    }

    /// <summary>
    /// <c>OpenStream</c> passes its state check just before the peer closes the connection and grows the stream table
    /// (the 17th stream) while the connection's SHUTDOWN_COMPLETE runs. <c>ShutdownRemainingStreams</c> reads
    /// <c>_slots</c> outside <c>_tableLock</c> and <c>_slotHighWater</c> inside it, so it can pair the pre-growth array
    /// (16 entries) with the post-growth high-water mark (17): <c>slots[16]</c> throws <see cref="IndexOutOfRangeException"/>
    /// out of the SHUTDOWN_COMPLETE handler (outside its per-slot try), so <c>OnClosed</c> is never delivered,
    /// <c>_shutdownComplete</c> never becomes true and the handles are never released (RegistrationClose then hangs).
    /// The helper thread lines both threads up on <c>_tableLock</c>; the opener has waited longer and is normally woken first.
    /// </summary>
    [Fact]
    public void Growing_the_stream_table_while_the_connection_shuts_down_still_delivers_OnClosed()
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            (bool closed, string detail) = GrowTableDuringShutdown();
            Assert.True(closed, $"attempt {attempt}: {detail}");
        }
    }

    private static (bool Closed, string Detail) GrowTableDuringShutdown()
    {
        var harness = new MsQuicTransportHarness();
        LockHolder? holder = null;
        MsQuicTransport? client = null;
        bool closed = false;
        string detail = "";
        try
        {
            (MsQuicTransport c, MsQuicTransport server, RecordingSink clientSink) = Connect(harness);
            client = c;
            // Fill the initial table (16 slots) with streams that are never started.
            for (int i = 0; i < 16; i++) Assert.Equal(TransportStatus.Success, c.OpenStream(StreamKind.Bidirectional, (ulong)i, 32767, out _));

            holder = new LockHolder(PrivateField<Lock>(c, "_tableLock"));
            var opener = new Thread(() => c.OpenStream(StreamKind.Bidirectional, 99, 32767, out _)) { IsBackground = true, Name = "game thread" };
            opener.Start();
            // The 17th OpenStream has passed its state check (still connected) and waits for _tableLock to grow the table.
            Assert.True(Spin.Until(() => (opener.ThreadState & ThreadState.WaitSleepJoin) != 0, Timeout), "OpenStream never blocked on _tableLock");
            Thread.Sleep(50);

            server.Close(7, default);
            // The client's worker runs SHUTDOWN_COMPLETE into ShutdownRemainingStreams, which read _slots and waits for _tableLock.
            Assert.True(Spin.Until(() => c.State == TransportState.Closing, Timeout), "the client never started closing");
            Thread.Sleep(500);
            holder.Release();
            Assert.True(opener.Join(Timeout), "OpenStream did not return");

            closed = clientSink.WaitFor(static e => e.Kind == RecordedEventKind.Closed, TimeSpan.FromSeconds(5));
            Exception? escaped = c.Connection.LastCallbackException;
            detail = "the client never reported OnClosed and never released its handles; the exception that escaped its SHUTDOWN_COMPLETE handler: "
                + (escaped is null ? "none" : $"{escaped.GetType().Name}: {escaped.Message}");
        }
        finally
        {
            holder?.Dispose();
            if (client is not null && !closed) ReleaseLostHandles(client);
            harness.Dispose();
        }
        if (closed) Assert.Null(harness.CleanupError);
        return (closed, detail);
    }

    // ------------------------------------------------------------------ helpers

    private static (MsQuicTransport Client, MsQuicTransport Server, RecordingSink ClientSink) Connect(MsQuicTransportHarness harness)
    {
        var clientSink = new RecordingSink();
        var serverSink = new RecordingSink();
        ConformancePair pair = harness.CreatePair(clientSink, serverSink);
        clientSink.Transport = pair.Client;
        serverSink.Transport = pair.Server;
        Assert.True(clientSink.WaitFor(static e => e.Kind == RecordedEventKind.Connected, Timeout), "client OnConnected");
        Assert.True(serverSink.WaitFor(static e => e.Kind == RecordedEventKind.Connected, Timeout), "server OnConnected");
        return ((MsQuicTransport)pair.Client, (MsQuicTransport)pair.Server, clientSink);
    }

    private static T PrivateField<T>(MsQuicTransport transport, string name)
        => (T)typeof(MsQuicTransport).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(transport)!;

    private static T SlotField<T>(MsQuicTransport transport, int index, string name)
    {
        object slot = PrivateField<Array>(transport, "_slots").GetValue(index)!;
        return (T)slot.GetType().GetField(name)!.GetValue(slot)!;
    }

    /// <summary>The transport lost its SHUTDOWN_COMPLETE: close its handles so the test registration can still close.</summary>
    private static void ReleaseLostHandles(MsQuicTransport transport)
    {
        if (transport.HandlesClosed) return;
        typeof(MsQuicTransport).GetMethod("ReleaseHandlesNow", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(transport, null);
    }

    /// <summary>Holds a <see cref="Lock"/> on a dedicated thread until released (a lock is exited by the thread that entered it).</summary>
    private sealed class LockHolder : IDisposable
    {
        private readonly ManualResetEventSlim _held = new();
        private readonly ManualResetEventSlim _release = new();
        private readonly Thread _thread;

        public LockHolder(Lock gate)
        {
            _thread = new Thread(() =>
            {
                gate.Enter();
                _held.Set();
                _release.Wait();
                gate.Exit();
            }) { IsBackground = true, Name = "review lock holder" };
            _thread.Start();
            if (!_held.Wait(Timeout)) throw new TimeoutException("the lock holder never got the lock");
        }

        public void Release()
        {
            _release.Set();
            _thread.Join(Timeout);
        }

        public void Dispose() => Release();
    }
}
