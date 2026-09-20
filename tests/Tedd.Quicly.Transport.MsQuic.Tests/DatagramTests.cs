using System.Buffers.Binary;
using Tedd.Quicly.Transport.MsQuic.Interop;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

[Collection(MsQuicCollection.Name)]
public class DatagramTests
{
    private const int Count = 100;

    private sealed class Burst : IDisposable
    {
        public readonly NativeBlock Block;
        public readonly NativeBuffers Buffers;

        public Burst(int payloadLength)
        {
            Block = new NativeBlock(payloadLength * Count);
            Block.FillPattern(0);
            Buffers = new NativeBuffers(Count);
            for (int i = 0; i < Count; i++)
            {
                Block.WriteInt32(i * payloadLength, i);
                Buffers.Set(i, Block, i * payloadLength, payloadLength);
            }
        }

        public void SendAll(MsQuicConnection sender)
        {
            for (int i = 0; i < Count; i++)
            {
                TestStatus.AssertAccepted(Buffers.SendDatagramOn(sender, i, QUIC_SEND_FLAGS.NONE, i + 1));
            }
        }

        public void Dispose()
        {
            Buffers.Dispose();
            Block.Dispose();
        }
    }

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

        using var burst = new Burst(payloadLength);
        burst.SendAll(sender);

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

        (bool clientEnabled, ushort clientMax) = await clientEvents.DatagramSendEnabledTcs.Within();
        (bool serverEnabled, ushort serverMax) = await serverEvents.DatagramSendEnabledTcs.Within();
        Assert.True(clientEnabled);
        Assert.True(serverEnabled);
        Assert.True(clientMax > 100, $"client max {clientMax}");
        Assert.True(serverMax > 100, $"server max {serverMax}");

        await SendBurstAndVerifyAsync(client, clientEvents, serverEvents, clientMax - 16);
        await SendBurstAndVerifyAsync(server, serverEvents, clientEvents, serverMax - 16);

        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents.ShutdownCompleteTcs.Within();
        await serverEvents.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public async Task Newer_send_flags_are_accepted_by_the_library()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();
        (_, ushort max) = await clientEvents.DatagramSendEnabledTcs.Within();
        Assert.True(64 < max);
        using var block = new NativeBlock(64);
        block.FillPattern(0);
        using NativeBuffers buffers = NativeBuffers.Single(block);
        QUIC_SEND_FLAGS flags = QUIC_SEND_FLAGS.DGRAM_PRIORITY | QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED | QUIC_SEND_FLAGS.PRIORITY_WORK;
        Assert.True(MsQuicFeatureGate.AreSendFlagsSupported(client.Api.Version, flags));
        TestStatus.AssertAccepted(buffers.SendDatagramOn(client, flags, 0));
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref serverEvents.DatagramsReceived) >= 1, TestTimeouts.Default));
        Assert.True(serverEvents.Datagrams.TryDequeue(out byte[]? payload));
        Assert.Equal(block.Span.ToArray(), payload);
        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents.ShutdownCompleteTcs.Within();
    }

    [Fact]
    public async Task Oversized_datagram_is_rejected_synchronously()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, _, _) = await loopback.ConnectPairAsync();
        (_, ushort max) = await clientEvents.DatagramSendEnabledTcs.Within();
        using var block = new NativeBlock(max + 100);
        using NativeBuffers buffers = NativeBuffers.Single(block);
        int status = buffers.SendDatagramOn(client, QUIC_SEND_FLAGS.NONE, 0);
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
    }

    [Fact]
    public async Task Datagram_receive_disabled_by_settings_means_peer_cannot_send()
    {
        MsQuicSettings serverSettings = Loopback.TestServerSettings();
        serverSettings.DatagramReceiveEnabled = false;
        using var loopback = new Loopback(serverSettings);
        (MsQuicConnection client, ConnectionRecorder clientEvents, _, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();
        await Task.Delay(200);
        Assert.False(clientEvents.DatagramSendEnabledTcs.Task.IsCompleted);
        (bool enabled, _) = await serverEvents.DatagramSendEnabledTcs.Within();
        Assert.True(enabled);
        using var block = new NativeBlock(16);
        using NativeBuffers buffers = NativeBuffers.Single(block);
        Assert.True(MsQuicStatus.Failed(buffers.SendDatagramOn(client, QUIC_SEND_FLAGS.NONE, 0)));
    }

    /// <summary>Receiver that only counts and samples allocations (never allocates itself).</summary>
    private sealed unsafe class ProbeConnectionEvents : IMsQuicConnectionEvents
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

    private static void Send(MsQuicConnection client, NativeBuffers buffers, int first, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int status = buffers.SendDatagramOn(client, QUIC_SEND_FLAGS.NONE, first + i + 1);
            if (MsQuicStatus.Failed(status)) throw new MsQuicException(status, "DatagramSend");
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
        using NativeBuffers buffers = NativeBuffers.Single(block);

        const int warmup = 200;
        const int perWindow = 2000;
        Send(client, buffers, 0, warmup);
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref clientProbe.FinalStates) == warmup, TestTimeouts.Default));

        clientProbe.Probe.Arm();
        serverProbe.Probe.Arm();
        // The sending thread is measured over rounds of windows (see WindowedAllocation); a window waits for its own sends
        // to reach a final state, so a repeated round leaves no more outstanding than one window does. The probes sample the
        // worker threads, which the test host's transient does not touch, so they stay one measurement over every round run.
        int sent = warmup;
        int windows = WindowedAllocation.AssertNone(() =>
        {
            Send(client, buffers, sent, perWindow);
            sent += perWindow;
            if (!Spin.UntilAtLeast(ref clientProbe.FinalStates, sent, TestTimeouts.Default))
                throw new InvalidOperationException($"final states {clientProbe.FinalStates} of {sent}");
        });
        int measured = windows * perWindow;

        Assert.Equal(warmup + measured, Volatile.Read(ref clientProbe.FinalStates));
        Assert.True(await TestTimeouts.WaitUntilAsync(() => Volatile.Read(ref serverProbe.Received) >= (warmup + measured) * 9 / 10, TestTimeouts.Default), $"received {serverProbe.Received}");

        Assert.Equal(0, clientProbe.Probe.Allocated);
        Assert.Equal(0, serverProbe.Probe.Allocated);
        Assert.True(clientProbe.Probe.Samples > measured / 2, $"client samples {clientProbe.Probe.Samples}");
        Assert.True(serverProbe.Probe.Samples > measured / 2, $"server samples {serverProbe.Probe.Samples}");
    }
}
