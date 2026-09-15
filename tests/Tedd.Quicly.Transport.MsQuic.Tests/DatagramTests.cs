using System.Buffers.Binary;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

public unsafe class DatagramTests
{
    private const int Count = 100;

    private static async Task SendBurstAndVerifyAsync(MsQuicConnection sender, ConnectionRecorder senderEvents, ConnectionRecorder receiverEvents, int payloadLength)
    {
        var received = new bool[Count];
        int receivedCount = 0;
        int badLength = 0;
        receiverEvents.KeepDatagrams = false;
        receiverEvents.OnDatagram = data =>
        {
            if (data.Length != payloadLength)
            {
                Interlocked.Increment(ref badLength);
                return;
            }
            int index = BinaryPrimitives.ReadInt32LittleEndian(data);
            if ((uint)index < Count && !received[index])
            {
                received[index] = true;
                Interlocked.Increment(ref receivedCount);
            }
        };
        senderEvents.SendStates = new QUIC_DATAGRAM_SEND_STATE[Count];

        using var block = new NativeBlock(payloadLength * Count);
        block.FillPattern(0);
        QUIC_BUFFER* buffers = stackalloc QUIC_BUFFER[Count];
        for (int i = 0; i < Count; i++)
        {
            byte* p = block.Pointer + i * payloadLength;
            BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>(p, 4), i);
            buffers[i] = new QUIC_BUFFER(p, (uint)payloadLength);
        }
        for (int i = 0; i < Count; i++)
        {
            int status = sender.SendDatagram(&buffers[i], 1, QUIC_SEND_FLAGS.NONE, (void*)(i + 1));
            Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, status);
        }

        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref receivedCount) >= Count * 9 / 10, TimeSpan.FromSeconds(2)), $"received {receivedCount}/{Count} in 2 s");
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref senderEvents.FinalSendStates) == Count, TimeSpan.FromSeconds(5)), $"final states {senderEvents.FinalSendStates}/{Count}");
        Assert.Equal(0, badLength);
        foreach (QUIC_DATAGRAM_SEND_STATE state in senderEvents.SendStates)
        {
            Assert.True(QuicDatagramSendState.IsFinal(state), state.ToString());
        }
        Assert.Contains(senderEvents.SendStates, s => s is QUIC_DATAGRAM_SEND_STATE.ACKNOWLEDGED or QUIC_DATAGRAM_SEND_STATE.ACKNOWLEDGED_SPURIOUS);
    }

    [Fact]
    public async Task Datagrams_flow_both_ways_with_send_state_tracking()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, MsQuicConnection server, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

        (bool clientEnabled, ushort clientMax) = await clientEvents.DatagramSendEnabled.Within();
        (bool serverEnabled, ushort serverMax) = await serverEvents.DatagramSendEnabled.Within();
        Assert.True(clientEnabled);
        Assert.True(serverEnabled);
        Assert.True(clientMax > 100, $"client max {clientMax}");
        Assert.True(serverMax > 100, $"server max {serverMax}");

        await SendBurstAndVerifyAsync(client, clientEvents, serverEvents, clientMax - 16);
        await SendBurstAndVerifyAsync(server, serverEvents, clientEvents, serverMax - 16);

        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents.ShutdownComplete.Within();
        await serverEvents.ShutdownComplete.Within();
    }

    [Fact]
    public async Task Oversized_datagram_is_rejected_synchronously()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, _, _) = await loopback.ConnectPairAsync();
        (_, ushort max) = await clientEvents.DatagramSendEnabled.Within();
        using var block = new NativeBlock(max + 100);
        QUIC_BUFFER buffer = new(block.Pointer, (uint)(max + 100));
        int status = client.SendDatagram(&buffer, 1, QUIC_SEND_FLAGS.NONE, null);
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
    }

    [Fact]
    public async Task Datagram_receive_disabled_by_settings_means_peer_cannot_send()
    {
        using var loopback = new Loopback(new MsQuicSettings { DatagramReceiveEnabled = false });
        (MsQuicConnection client, ConnectionRecorder clientEvents, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();
        // The server does not advertise datagram support, so the client never gets send-enabled...
        await Task.Delay(200);
        Assert.False(clientEvents.DatagramSendEnabled.Task.IsCompleted);
        // ...while the client (receive enabled by default) lets the server send.
        (bool enabled, _) = await serverEvents.DatagramSendEnabled.Within();
        Assert.True(enabled);
        using var block = new NativeBlock(16);
        QUIC_BUFFER buffer = new(block.Pointer, 16);
        Assert.True(MsQuicStatus.Failed(client.SendDatagram(&buffer, 1, QUIC_SEND_FLAGS.NONE, null)));
    }

    /// <summary>Receiver that only counts and samples allocations (never allocates itself).</summary>
    private sealed class ProbeConnectionEvents : IMsQuicConnectionEvents
    {
        public readonly CallbackAllocationProbe Probe = new();
        public int Received;
        public int FinalStates;
        public volatile bool DatagramsEnabled;
        public ushort MaxSendLength;

        public void DatagramStateChanged(MsQuicConnection connection, bool sendEnabled, ushort maxSendLength)
        {
            if (sendEnabled)
            {
                MaxSendLength = maxSendLength;
                DatagramsEnabled = true;
            }
        }

        public void DatagramReceived(MsQuicConnection connection, ReadOnlySpan<byte> data, QUIC_RECEIVE_FLAGS flags)
        {
            Probe.Sample();
            Interlocked.Increment(ref Received);
        }

        public void DatagramSendStateChanged(MsQuicConnection connection, void* clientContext, QUIC_DATAGRAM_SEND_STATE state)
        {
            Probe.Sample();
            if (QuicDatagramSendState.IsFinal(state)) Interlocked.Increment(ref FinalStates);
        }
    }

    [Fact]
    public async Task Datagram_send_and_receive_paths_do_not_allocate_in_steady_state()
    {
        ProbeConnectionEvents? serverProbe = null;
        using var loopback = new Loopback { ServerEventsFactory = _ => serverProbe = new ProbeConnectionEvents() };
        var clientProbe = new ProbeConnectionEvents();
        MsQuicConnection client = loopback.Connect(clientProbe);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => clientProbe.DatagramsEnabled, TestTimeouts.Default));
        Assert.NotNull(serverProbe);

        int payload = Math.Min(clientProbe.MaxSendLength - 16, 1000);
        using var block = new NativeBlock(payload);
        block.FillPattern(0);
        QUIC_BUFFER buffer = new(block.Pointer, (uint)payload);

        const int warmup = 200;
        const int measured = 2000;
        for (int i = 0; i < warmup; i++)
        {
            Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, client.SendDatagram(&buffer, 1, QUIC_SEND_FLAGS.NONE, (void*)(i + 1)));
        }
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref clientProbe.FinalStates) == warmup, TestTimeouts.Default));

        clientProbe.Probe.Arm();
        serverProbe.Probe.Arm();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < measured; i++)
        {
            int status = client.SendDatagram(&buffer, 1, QUIC_SEND_FLAGS.NONE, (void*)(warmup + i + 1));
            if (status != MsQuicStatus.QUIC_STATUS_SUCCESS) throw new MsQuicException(status, "DatagramSend");
        }
        long senderAllocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref clientProbe.FinalStates) == warmup + measured, TestTimeouts.Default), $"final {clientProbe.FinalStates}");
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref serverProbe.Received) >= (warmup + measured) * 9 / 10, TestTimeouts.Default), $"received {serverProbe.Received}");

        Assert.Equal(0, senderAllocated);
        Assert.Equal(0, clientProbe.Probe.Allocated);
        Assert.Equal(0, serverProbe.Probe.Allocated);
        Assert.True(clientProbe.Probe.Samples > measured / 2, $"client samples {clientProbe.Probe.Samples}");
        Assert.True(serverProbe.Probe.Samples > measured / 2, $"server samples {serverProbe.Probe.Samples}");
    }
}
