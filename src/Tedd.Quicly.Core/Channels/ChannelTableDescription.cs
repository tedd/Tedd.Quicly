namespace Tedd.Quicly.Core.Channels;

/// <summary>Outcome of <see cref="ChannelTableCodec.TryParseWithNames"/>.</summary>
public enum ChannelTableParseStatus : byte
{
    /// <summary>The table section was parsed.</summary>
    Ok = 0,

    /// <summary>The input ended inside the table section.</summary>
    Truncated,

    /// <summary>A varint was not minimally encoded.</summary>
    NonMinimalVarint,

    /// <summary>The channel count exceeds 16 382.</summary>
    TooManyChannels,

    /// <summary>A channel id is outside 2…16383.</summary>
    BadChannelId,

    /// <summary>Channel ids are not strictly ascending (duplicate or out of order).</summary>
    ChannelsNotAscending,

    /// <summary>A mode byte is not a defined <see cref="ChannelMode"/>.</summary>
    BadMode,

    /// <summary>A flags byte has the reserved bit 7 set or names a reserved compression codec.</summary>
    BadFlags,

    /// <summary>A <c>maxMessageSize</c> is 0 or larger than 16 MiB.</summary>
    BadMaxMessageSize,

    /// <summary>A name is longer than 64 bytes.</summary>
    NameTooLong,

    /// <summary>A name is not valid UTF-8.</summary>
    InvalidUtf8,
}

/// <summary>
/// One channel of a peer's table as received in <c>HelloAck</c> (diagnostics only; never adopted).
/// </summary>
public readonly struct ChannelDescription
{
    internal ChannelDescription(ushort id, ChannelMode mode, byte flags, byte priority, int maxMessageSize, string name)
    {
        Id = id;
        Mode = mode;
        Flags = flags;
        Priority = priority;
        MaxMessageSize = maxMessageSize;
        Name = name;
    }

    /// <summary>Channel id.</summary>
    public ushort Id { get; }

    /// <summary>Delivery mode.</summary>
    public ChannelMode Mode { get; }

    /// <summary>The raw canonical flags byte.</summary>
    public byte Flags { get; }

    /// <summary>Scheduling priority.</summary>
    public byte Priority { get; }

    /// <summary>Largest raw payload.</summary>
    public int MaxMessageSize { get; }

    /// <summary>Diagnostic name.</summary>
    public string Name { get; }

    /// <summary>Flag bit 0.</summary>
    public bool Keyed => (Flags & 0x01) != 0;

    /// <summary>16 or 32 (flag bit 1).</summary>
    public byte SequenceBits => (Flags & 0x02) != 0 ? (byte)32 : (byte)16;

    /// <summary>Flag bit 2.</summary>
    public bool Fragmentation => (Flags & 0x04) != 0;

    /// <summary>Flag bit 3.</summary>
    public bool RequestResponse => (Flags & 0x08) != 0;

    /// <summary>Flag bit 4.</summary>
    public bool CoalesceOnReceive => (Flags & 0x10) != 0;

    /// <summary>Flag bits 5–6.</summary>
    public ChannelCompression Compression => (ChannelCompression)((Flags >> 5) & 0x03);

    /// <inheritdoc/>
    public override string ToString() => $"{Id} '{Name}' {Mode} flags=0x{Flags:X2} prio={Priority} max={MaxMessageSize}";
}

/// <summary>
/// A peer's channel table as described by the <c>HelloAck</c> table section: a read-only description for
/// diagnostics (for example explaining a table-hash mismatch). It is never turned into a <see cref="ChannelTable"/>.
/// </summary>
public sealed class ChannelTableDescription
{
    private readonly ChannelDescription[] _channels;

    internal ChannelTableDescription(ChannelDescription[] channels, ulong hash)
    {
        _channels = channels;
        Hash = hash;
    }

    /// <summary>Number of channels.</summary>
    public int Count => _channels.Length;

    /// <summary>XXH64 (seed 0) of the canonical part exactly as received; compare with <see cref="ChannelTable.Hash"/>.</summary>
    public ulong Hash { get; }

    /// <summary>The channels in ascending id order.</summary>
    public ReadOnlySpan<ChannelDescription> Channels => _channels;
}
