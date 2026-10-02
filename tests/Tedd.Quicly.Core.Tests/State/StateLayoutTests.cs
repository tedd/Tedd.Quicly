using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.State;

public unsafe class StateLayoutTests
{
    [Fact]
    public void ReceiveEntry_Fits_One_Cache_Line_With_Sequential_Fields()
    {
        Assert.True(sizeof(ReceiveEntry) <= 64);
        Assert.Equal(ReceiveEntry.Size, sizeof(ReceiveEntry));

        ReceiveEntry e = default;
        byte* b = (byte*)&e;
        Assert.Equal(0, (int)((byte*)&e.Channel - b));
        Assert.Equal(2, (int)((byte*)&e.Flags - b));
        Assert.Equal(4, (int)((byte*)&e.Sequence - b));
        Assert.Equal(8, (int)((byte*)&e.Key - b));
        Assert.Equal(16, (int)((byte*)&e.Lease - b));
        Assert.Equal(32, (int)((byte*)&e.Length - b));
        Assert.Equal(36, (int)((byte*)&e.RawLength - b));
        Assert.Equal(40, (int)((byte*)&e.ReceivedMicrosDelta - b));
        Assert.Equal(44, (int)((byte*)&e.SenderTick - b));
        Assert.Equal(48, (int)((byte*)&e.RequestId - b));

        // The receive credit's tag (RC2-2) sits in what was padding: the entry is still one cache line.
        Assert.Equal(52, (int)((byte*)&e.CreditShared - b));
        Assert.Equal(2, sizeof(ReceiveFlags));
    }

    [Fact]
    public void ReceiveFlags_Values_Are_Fixed()
    {
        Assert.Equal(0, (ushort)ReceiveFlags.None);
        Assert.Equal(1, (ushort)ReceiveFlags.Fragmented);
        Assert.Equal(2, (ushort)ReceiveFlags.Compressed);
        Assert.Equal(4, (ushort)ReceiveFlags.Superseded);
        Assert.Equal(8, (ushort)ReceiveFlags.IsRequest);
        Assert.Equal(16, (ushort)ReceiveFlags.IsResponse);
        Assert.Equal(32, (ushort)ReceiveFlags.KeyRetired);
        Assert.Equal(64, (ushort)ReceiveFlags.Control);
    }

    [Fact]
    public void ReceiveEntry_Round_Trips_Its_Fields()
    {
        var lease = new BufferLease(2, 1, 9, 17, 4096, 1536);
        var e = new ReceiveEntry
        {
            Channel = 12,
            Flags = ReceiveFlags.Compressed | ReceiveFlags.IsResponse,
            Sequence = uint.MaxValue,
            Key = ulong.MaxValue,
            Lease = lease,
            Length = 900,
            RawLength = 1400,
            ReceivedMicrosDelta = 5,
            SenderTick = 6,
            RequestId = 7,
        };

        ReceiveEntry copy = e;
        Assert.Equal(lease, copy.Lease);
        Assert.Equal(ReceiveFlags.Compressed | ReceiveFlags.IsResponse, copy.Flags);
        Assert.Equal(ulong.MaxValue, copy.Key);
        Assert.Equal(1400, copy.RawLength);
    }

    [Fact]
    public void Channel_And_Key_State_Structs_Are_Exactly_One_Cache_Line()
    {
        Assert.Equal(64, sizeof(ChannelSendState));
        Assert.Equal(64, sizeof(ChannelRecvState));
        Assert.Equal(64, sizeof(ChannelSendCounters));
        Assert.Equal(64, sizeof(ChannelRecvCounters));
        Assert.Equal(64, sizeof(KeySendSlot));
        Assert.Equal(64, sizeof(KeyRecvSlot));
        Assert.Equal(ChannelSendState.Size, sizeof(ChannelSendState));
        Assert.Equal(ChannelRecvState.Size, sizeof(ChannelRecvState));
        Assert.Equal(ChannelSendCounters.Size, sizeof(ChannelSendCounters));
        Assert.Equal(ChannelRecvCounters.Size, sizeof(ChannelRecvCounters));
        Assert.Equal(KeySendSlot.Size, sizeof(KeySendSlot));
        Assert.Equal(KeyRecvSlot.Size, sizeof(KeyRecvSlot));
    }

    [Fact]
    public void Channel_State_Offsets_Match_The_Documented_Layout()
    {
        ChannelSendState s = default;
        byte* b = (byte*)&s;
        Assert.Equal(0, (int)((byte*)&s.NextSequence - b));
        Assert.Equal(4, (int)((byte*)&s.QueueHead - b));
        Assert.Equal(8, (int)((byte*)&s.QueueTail - b));
        Assert.Equal(12, (int)((byte*)&s.QueueCount - b));
        Assert.Equal(16, (int)((byte*)&s.QueueBytes - b));
        Assert.Equal(24, (int)((byte*)&s.Stream - b));
        Assert.Equal(32, (int)((byte*)&s.CurrentGroup - b));
        Assert.Equal(36, (int)((byte*)&s.InFlight - b));
        Assert.Equal(40, (int)((byte*)&s.Flags - b));
        Assert.Equal(44, (int)((byte*)&s.RetryPending - b));
        Assert.Equal(48, (int)((byte*)&s.NextDeadlineMicros - b));
        Assert.Equal(56, (int)((byte*)&s.BudgetBytes - b));

        ChannelRecvState r = default;
        b = (byte*)&r;
        Assert.Equal(0, (int)((byte*)&r.LastAccepted - b));
        Assert.Equal(4, (int)((byte*)&r.HighestSeen - b));
        Assert.Equal(8, (int)((byte*)&r.Stream - b));
        Assert.Equal(16, (int)((byte*)&r.Reassemblies - b));
        Assert.Equal(20, (int)((byte*)&r.ActiveGroups - b));
        Assert.Equal(24, (int)((byte*)&r.LastReceiveMicros - b));
        Assert.Equal(32, (int)((byte*)&r.Parser - b));
        Assert.Equal(36, (int)((byte*)&r.Flags - b));
        Assert.Equal(40, (int)((byte*)&r.PendingAcks - b));
        Assert.Equal(44, (int)((byte*)&r.PendingAckVersion - b));
        Assert.Equal(48, (int)((byte*)&r.StagedBytes - b));
        Assert.Equal(56, (int)((byte*)&r.NewestSequence - b));    }

    [Fact]
    public void Key_Slot_Offsets_Match_The_Documented_Layout()
    {
        KeySendSlot s = default;
        byte* b = (byte*)&s;
        Assert.Equal(0, (int)((byte*)&s.CurrentVersion - b));
        Assert.Equal(4, (int)((byte*)&s.AckedVersion - b));
        Assert.Equal(8, (int)((byte*)&s.CurrentLease - b));
        Assert.Equal(24, (int)((byte*)&s.RetryDeadline - b));
        Assert.Equal(32, (int)((byte*)&s.InFlightEntry - b));
        Assert.Equal(36, (int)((byte*)&s.LargeValueStream - b));
        Assert.Equal(44, (int)((byte*)&s.Attempts - b));
        Assert.Equal(45, (int)((byte*)&s.Flags - b));
        Assert.Equal(48, (int)((byte*)&s.Key - b));
        Assert.Equal(56, (int)((byte*)&s.LastSentMicros - b));

        KeyRecvSlot r = default;
        b = (byte*)&r;
        Assert.Equal(0, (int)((byte*)&r.LastAccepted - b));
        Assert.Equal(4, (int)((byte*)&r.Mailbox - b));
        Assert.Equal(8, (int)((byte*)&r.Reassembly - b));
        Assert.Equal(12, (int)((byte*)&r.Flags - b));
        Assert.Equal(16, (int)((byte*)&r.LastUpdateMicros - b));
        Assert.Equal(24, (int)((byte*)&r.Key - b));
        Assert.Equal(32, (int)((byte*)&r.PendingAckVersion - b));
        Assert.Equal(36, (int)((byte*)&r.Updates - b));
        Assert.Equal(40, (int)((byte*)&r.LastAcceptedExtended - b));
    }

    [Fact]
    public void Counters_Are_Eight_Longs()
    {
        var send = new ChannelSendCounters { Sent = 1, Bytes = 2, Superseded = 3, Expired = 4, QueueFull = 5, TooLarge = 6, Retries = 7, KeyTableFull = 8 };
        long* p = (long*)&send;
        for (int i = 0; i < 8; i++)
            Assert.Equal(i + 1, p[i]);

        var recv = new ChannelRecvCounters { Received = 1, Bytes = 2, Dropped = 3, Superseded = 4, RingDrops = 5, KeyTableFull = 6, TooLarge = 7, OutOfBuffers = 8 };
        p = (long*)&recv;
        for (int i = 0; i < 8; i++)
            Assert.Equal(i + 1, p[i]);
    }

    [Fact]
    public void Send_Outcome_Counters_Are_Two_Longs_Apart_From_The_Send_Counters()
    {
        // The transport outcome counters have an array of their own: the send counters stay one full line of eight longs.
        Assert.Equal(16, sizeof(ChannelSendOutcomeCounters));
        Assert.Equal(ChannelSendOutcomeCounters.Size, sizeof(ChannelSendOutcomeCounters));
        ChannelSendOutcomeCounters outcomes = default;
        byte* b = (byte*)&outcomes;
        Assert.Equal(0, (int)((byte*)&outcomes.TransportCanceled - b));
        Assert.Equal(8, (int)((byte*)&outcomes.TransportLost - b));
        Assert.Equal(64, sizeof(ChannelSendCounters));
    }

    [Fact]
    public void State_Arrays_Put_Every_Element_On_Its_Own_Cache_Line()
    {
        using var send = new NativeArray<ChannelSendState>(5);
        using var keys = new NativeArray<KeyRecvSlot>(5);
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(0, (nint)Unsafe.AsPointer(ref send[i]) % 64);
            Assert.Equal(0, (nint)Unsafe.AsPointer(ref keys[i]) % 64);
            Assert.Equal(0u, send[i].NextSequence);
            Assert.False(send[i].Stream.IsValid);
        }

        send[2].Stream = new TransportStreamId(3, 9);
        send[2].Flags = ChannelSendFlags.StreamOpen | ChannelSendFlags.StreamStartPending;
        keys[4].Flags = KeyRecvFlags.HasAccepted | KeyRecvFlags.AckPending;
        Assert.True(send[2].Stream.IsValid);
        Assert.Equal(ChannelSendFlags.StreamOpen | ChannelSendFlags.StreamStartPending, send[2].Flags);
        Assert.Equal(KeyRecvFlags.HasAccepted | KeyRecvFlags.AckPending, keys[4].Flags);
    }

    [Fact]
    public void Flag_Enums_Have_Distinct_Bits()
    {
        AssertBits([(ulong)ChannelSendFlags.StreamOpen, (ulong)ChannelSendFlags.StreamStartPending, (ulong)ChannelSendFlags.FlushRequested, (ulong)ChannelSendFlags.Blocked]);
        AssertBits([(ulong)ChannelRecvFlags.StreamOpen, (ulong)ChannelRecvFlags.Pended, (ulong)ChannelRecvFlags.HasAccepted]);
        AssertBits([(ulong)KeySendFlags.Pending, (ulong)KeySendFlags.LargeValue, (ulong)KeySendFlags.RetireRequested, (ulong)KeySendFlags.RetryArmed]);
        AssertBits([(ulong)KeyRecvFlags.AckPending, (ulong)KeyRecvFlags.Reassembling, (ulong)KeyRecvFlags.Retired, (ulong)KeyRecvFlags.HasAccepted]);
        Assert.Equal(0, (int)ChannelSendFlags.None + (int)ChannelRecvFlags.None + (int)KeySendFlags.None + (int)KeyRecvFlags.None);

        static void AssertBits(ulong[] values)
        {
            ulong seen = 0;
            foreach (ulong v in values)
            {
                Assert.Equal(1, System.Numerics.BitOperations.PopCount(v));
                Assert.Equal(0UL, seen & v);
                seen |= v;
            }
        }
    }
}
