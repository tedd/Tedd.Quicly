using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Channel tables of the group-stream (ReliableUnordered) tests (immutable, shared across parallel tests).</summary>
internal static class GroupTables
{
    /// <summary>
    /// 2 unordered datagrams · 4 ordered · 5 groups · 6 groups keyed · 7 groups LZ4 (MinCompressSize 16) · 8 groups with
    /// GroupMaxBytes 256 · 9 groups with MaxGroups 2 · 10 groups with a 64 KiB queue limit · 11 groups at priority 200 ·
    /// 12 groups with MaxMessageSize 1000.
    /// </summary>
    public static ChannelTable Main { get; } = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(4, "chat", ChannelMode.ReliableOrdered)
        .Add(5, "events", ChannelMode.ReliableUnordered)
        .Add(6, "keyed", ChannelMode.ReliableUnordered, o => o.Keyed = true)
        .Add(7, "packed", ChannelMode.ReliableUnordered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; })
        .Add(8, "small-groups", ChannelMode.ReliableUnordered, o => o.GroupMaxBytes = 256)
        .Add(9, "few-groups", ChannelMode.ReliableUnordered, o => o.MaxGroups = 2)
        .Add(10, "limited", ChannelMode.ReliableUnordered, o => o.QueueLimitBytes = 64 * 1024)
        .Add(11, "urgent", ChannelMode.ReliableUnordered, o => o.Priority = 200)
        .Add(12, "tiny", ChannelMode.ReliableUnordered, o => o.MaxMessageSize = 1000)
        .Build();

    /// <summary>
    /// One group channel with <c>MaxGroups = 1</c> and one datagram channel, so the peer grants exactly one unidirectional
    /// stream (<c>PeerCore.PeerUnidirectionalStreamLimit</c> = Σ max(MaxGroups, 1) over stream channels): a second group must
    /// wait for credit.
    /// </summary>
    public static ChannelTable OneStream { get; } = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(5, "events", ChannelMode.ReliableUnordered, o => o.MaxGroups = 1)
        .Build();

    /// <summary>
    /// The same channels as <see cref="OneStream"/> with room for four groups. <c>MaxGroups</c> is not part of the table hash
    /// (PROTOCOL.md §1), so a peer may hold this table while its peer holds <see cref="OneStream"/>: the sender then tries to
    /// open four group streams while the receiver grants one, which is what drives the refusal path.
    /// </summary>
    public static ChannelTable FourGroups { get; } = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(5, "events", ChannelMode.ReliableUnordered, o => o.MaxGroups = 4)
        .Build();
}

/// <summary>Helpers of the group-stream tests.</summary>
internal static class GroupKit
{
    /// <summary>Budgets and a pool large enough for many groups of 64 KiB in flight.</summary>
    public static void Roomy(PeerOptions options)
    {
        options.SendBudgetBytes = 4 * 1024 * 1024;
        options.ReceiveBudgetBytes = 4 * 1024 * 1024;
        options.SendTableCapacity = 4096;
        options.SegmentArenaCapacity = 4096;
        options.AllocatorOptions = new SlabAllocatorOptions
        {
            FreeListShards = 2,
            SizeClasses =
            [
                new(64, 8192),
                new(256, 2048),
                new(1536, 1024),
                new(4096, 512),
                new(16384, 128),
                new(65536, 64),
                new(262144, 2),
            ],
        };
    }

    /// <summary>Quiet options (no pings, no heartbeat) with groups sealed on every flush.</summary>
    public static void Prompt(PeerOptions options)
    {
        DatagramKit.Quiet(options);
        options.GroupMinInterval = TimeSpan.Zero;
    }

    public static GroupStreamEngine Engine(QuiclyPeer peer) => (GroupStreamEngine)peer.Core.GetEngine(ChannelMode.ReliableUnordered)!;

    /// <summary>Live groups of a channel: filling, waiting for credit, or with a stream.</summary>
    public static int Groups(QuiclyPeer peer, ushort channel) => Engine(peer).GroupsOf(peer.Core.ChannelIndexOf(channel));

    /// <summary>Phases of the channel's live groups, oldest first.</summary>
    public static List<GroupStreamEngine.GroupPhase> Phases(QuiclyPeer peer, ushort channel) =>
        Engine(peer).PhasesOf(peer.Core.ChannelIndexOf(channel));

    /// <summary>Group ids handed out on a channel so far (the id its next group will carry).</summary>
    public static ulong GroupsFormed(QuiclyPeer peer, ushort channel) => Engine(peer).NextGroupIdOf(peer.Core.ChannelIndexOf(channel));

    /// <summary>Peer group streams the receiving engine holds open on a channel.</summary>
    public static int OpenPeerGroups(QuiclyPeer peer, ushort channel) => Engine(peer).OpenPeerGroupsOf(peer.Core.ChannelIndexOf(channel));

    public static ChannelStatistics Stats(QuiclyPeer peer, ushort channel) => DatagramKit.ChannelStats(peer, channel);

    /// <summary>
    /// A group stream's bytes written by hand (PROTOCOL.md §3.2): the preamble <c>ChannelId, GroupId</c> followed by one
    /// <c>Length varint, payload</c> frame per message.
    /// </summary>
    public static byte[] GroupStream(ushort channel, ulong groupId, params byte[][] messages)
    {
        byte[] buffer = new byte[64 + Sum(messages)];
        int position = VarInt.Write(buffer, channel);
        position += VarInt.Write(buffer.AsSpan(position), groupId);
        foreach (byte[] message in messages)
        {
            position += VarInt.Write(buffer.AsSpan(position), (ulong)message.Length);
            message.CopyTo(buffer.AsSpan(position));
            position += message.Length;
        }

        return buffer.AsSpan(0, position).ToArray();
    }

    /// <summary>A group stream's preamble plus a frame header that promises <paramref name="length"/> bytes never sent.</summary>
    public static byte[] GroupStreamStart(ushort channel, ulong groupId, int length, ReadOnlySpan<byte> partial)
    {
        byte[] buffer = new byte[64 + partial.Length];
        int position = VarInt.Write(buffer, channel);
        position += VarInt.Write(buffer.AsSpan(position), groupId);
        position += VarInt.Write(buffer.AsSpan(position), (ulong)length);
        partial.CopyTo(buffer.AsSpan(position));
        return buffer.AsSpan(0, position + partial.Length).ToArray();
    }

    private static int Sum(byte[][] messages)
    {
        int total = 0;
        foreach (byte[] message in messages)
        {
            total += message.Length + 8;
        }

        return total;
    }
}
