using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Channel tables of the ReliableLatest tests (immutable, shared across parallel tests).</summary>
internal static class LatestTables
{
    /// <summary>
    /// 2 latest (hashed keys) · 3 unordered (movement, so the latest channel is not alone on the link) · 4 latest with a
    /// dense key space 0 … 1023 · 5 latest LZ4 (MinCompressSize 16) · 6 latest with 4 keys · 7 latest priority 250 ·
    /// 8 latest with MaxGroups 1 (one large value at a time) · 9 ordered (a stream channel next to the latest ones).
    /// </summary>
    public static ChannelTable Main { get; } = ChannelTable.Create()
        .Add(2, "state", ChannelMode.ReliableLatest)
        .Add(3, "moves", ChannelMode.UnreliableUnordered)
        .Add(4, "dense", ChannelMode.ReliableLatest, o => { o.KeySpace = KeySpace.Dense(1023); o.MaxKeys = 1024; })
        .Add(5, "packed", ChannelMode.ReliableLatest, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; })
        .Add(6, "few-keys", ChannelMode.ReliableLatest, o => o.MaxKeys = 4)
        .Add(7, "urgent", ChannelMode.ReliableLatest, o => o.Priority = 250)
        .Add(8, "one-group", ChannelMode.ReliableLatest, o => o.MaxGroups = 1)
        .Add(9, "chat", ChannelMode.ReliableOrdered)
        .Add(10, "limited", ChannelMode.ReliableLatest, o => o.QueueLimitBytes = 200)
        .Build();

    /// <summary>One latest channel whose values are tiny; used where a test wants the whole link to itself.</summary>
    public static ChannelTable Single { get; } = ChannelTable.Create()
        .Add(2, "state", ChannelMode.ReliableLatest)
        .Build();
}

/// <summary>Helpers of the ReliableLatest tests.</summary>
internal static class LatestKit
{
    /// <summary>No pings after the first one and no heartbeat, so datagram counts are exact.</summary>
    public static void Quiet(PeerOptions options)
    {
        options.PingInterval = TimeSpan.FromHours(1);
        options.FastPingInterval = TimeSpan.FromHours(1);
        options.FastLockDuration = TimeSpan.Zero;
        options.HeartbeatTimeout = TimeSpan.Zero;
    }

    /// <summary>
    /// Budgets and a pool with room for many large values in flight. The control-message rate is left at its default: one
    /// coalesced LatestAck datagram carries about 170 keys, so even the 1 000-key 60 Hz workload needs only some 360 control
    /// messages per second in each direction, well inside the 2 000/s default of PROTOCOL.md §7 (which is what that default
    /// was sized for).
    /// </summary>
    public static void Roomy(PeerOptions options)
    {
        options.SendBudgetBytes = 4 * 1024 * 1024;
        options.ReceiveBudgetBytes = 4 * 1024 * 1024;
        options.SendTableCapacity = 4096;
        options.AllocatorOptions = new SlabAllocatorOptions
        {
            FreeListShards = 2,
            SizeClasses =
            [
                new(64, 8192),
                new(256, 4096),
                new(1536, 1024),
                new(4096, 512),
                new(16384, 128),
                new(65536, 64),
                new(262144, 4),
            ],
        };
    }

    public static ReliableLatestEngine Engine(QuiclyPeer peer) => (ReliableLatestEngine)peer.Core.GetEngine(ChannelMode.ReliableLatest)!;

    public static uint AckedVersion(QuiclyPeer peer, ushort channel, ulong key) =>
        Engine(peer).AckedVersionOf(peer.Core.ChannelIndexOf(channel), key);

    public static int LiveKeys(QuiclyPeer peer, ushort channel) => Engine(peer).LiveKeys(peer.Core.ChannelIndexOf(channel));

    public static uint NextVersion(QuiclyPeer peer, ushort channel) => Engine(peer).NextVersionOf(peer.Core.ChannelIndexOf(channel));

    public static void SetNextVersion(QuiclyPeer peer, ushort channel, uint version) =>
        Engine(peer).SetNextVersion(peer.Core.ChannelIndexOf(channel), version);

    /// <summary>A recognisable payload of any length (byte i depends on the id and i).</summary>
    public static byte[] Payload(int id, int length)
    {
        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++)
        {
            payload[i] = (byte)((id * 131) + (i * 7) + (i >> 8));
        }

        return payload;
    }

    public static bool Matches(ReadOnlySpan<byte> payload, int id, int length)
    {
        if (payload.Length != length)
        {
            return false;
        }

        for (int i = 0; i < length; i++)
        {
            if (payload[i] != (byte)((id * 131) + (i * 7) + (i >> 8)))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Control datagrams of one type a raw endpoint received (they all start with 0x00, then the type byte).</summary>
    public static int ControlDatagrams(RecordingSink sink, ControlType type)
    {
        int count = 0;
        foreach (RecordedEvent e in sink.OfKind(RecordedEventKind.DatagramReceived))
        {
            if (e.Data.Length >= 2 && e.Data[0] == 0 && e.Data[1] == (byte)type)
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>Collects the values of a latest channel: (key, version, payload) in dispatch order.</summary>
    public static MessageHandler Collect(List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> into) =>
        (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
            into.Add((header.Key, header.Sequence, payload.ToArray(), header.Flags));

    /// <summary>One ReliableLatest datagram value written by hand (for raw endpoints).</summary>
    public static byte[] Frame(ChannelTable table, ushort channel, uint version, ulong key, ReadOnlySpan<byte> payload, int rawLength = 0)
    {
        MessageHeader header = default;
        header.Channel = channel;
        header.Sequence = version;
        header.Key = key;
        header.FragCount = 1;
        header.RawLength = rawLength;
        byte[] frame = new byte[DatagramFraming.MaxHeaderLength + payload.Length];
        int written = DatagramFraming.WriteHeader(frame, table[channel]!, in header);
        payload.CopyTo(frame.AsSpan(written));
        return frame.AsSpan(0, written + payload.Length).ToArray();
    }

    /// <summary>
    /// A one-message ReliableLatest group stream written by hand (PROTOCOL.md §3.2 and §8 item 8): preamble
    /// <c>ChannelId, GroupId = version</c>, then <c>Length, Sequence = version, Key, RawLength</c> and the payload.
    /// </summary>
    public static byte[] GroupStream(ChannelTable table, ushort channel, uint version, ulong key, ReadOnlySpan<byte> payload, int rawLength = 0)
    {
        ChannelDefinition definition = table[channel]!;
        StreamMessageHeader header = default;
        header.Length = payload.Length;
        header.Sequence = version;
        header.Key = key;
        header.RawLength = rawLength;
        byte[] stream = new byte[32 + StreamFraming.MaxFrameHeaderLength + payload.Length];
        int written = StreamFraming.WriteGroupPreamble(stream, channel, version);
        written += StreamFraming.WriteFrameHeader(stream.AsSpan(written), definition, in header);
        payload.CopyTo(stream.AsSpan(written));
        return stream.AsSpan(0, written + payload.Length).ToArray();
    }

    /// <summary>The body of a LatestAck control message with one entry, as <see cref="ReliableLatestEngine.OnControl"/> takes it.</summary>
    public static byte[] AckBody(ushort channel, ulong key, uint version)
    {
        byte[] frame = new byte[64];
        LatestAckBatchWriter writer = new(frame, ControlCarrier.Datagram);
        Assert.True(writer.TryAdd(new LatestAckEntry(channel, key, version)));
        int length = writer.Finish();
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadDatagram(frame.AsSpan(0, length), out _, out ReadOnlySpan<byte> body, out _));
        return body.ToArray();
    }

    /// <summary>Every LatestAck entry a raw endpoint received, in arrival order.</summary>
    public static List<LatestAckEntry> AckEntries(RecordingSink sink)
    {
        List<LatestAckEntry> entries = [];
        foreach (RecordedEvent e in sink.OfKind(RecordedEventKind.DatagramReceived))
        {
            if (ControlCodec.TryReadDatagram(e.Data, out ControlType type, out ReadOnlySpan<byte> body, out _) != ControlParseStatus.Ok
                || type != ControlType.LatestAck
                || ControlCodec.TryParse(body, out LatestAckBatchReader reader) != ControlParseStatus.Ok)
            {
                continue;
            }

            foreach (LatestAckEntry entry in reader)
            {
                entries.Add(entry);
            }
        }

        return entries;
    }

    /// <summary>Every LatestReject entry a raw endpoint received, in arrival order.</summary>
    public static List<LatestRejectEntry> RejectEntries(RecordingSink sink)
    {
        List<LatestRejectEntry> entries = [];
        foreach (RecordedEvent e in sink.OfKind(RecordedEventKind.DatagramReceived))
        {
            if (ControlCodec.TryReadDatagram(e.Data, out ControlType type, out ReadOnlySpan<byte> body, out _) != ControlParseStatus.Ok
                || type != ControlType.LatestReject
                || ControlCodec.TryParse(body, out LatestRejectBatchReader reader) != ControlParseStatus.Ok)
            {
                continue;
            }

            foreach (LatestRejectEntry entry in reader)
            {
                entries.Add(entry);
            }
        }

        return entries;
    }

}
