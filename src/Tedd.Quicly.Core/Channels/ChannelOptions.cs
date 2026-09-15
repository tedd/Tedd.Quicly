namespace Tedd.Quicly.Core.Channels;

/// <summary>
/// Optional settings of one channel, filled in by the <c>configure</c> callback of
/// <see cref="ChannelTableBuilder.Add(int, string, ChannelMode, Action{ChannelOptions}?)"/>. A property left at
/// <see langword="null"/> takes the mode-dependent default of PROTOCOL.md §1/§4.5/§7; everything is validated by
/// <see cref="ChannelTableBuilder.Build"/>.
/// </summary>
public sealed class ChannelOptions
{
    /// <summary>Messages carry a key. Default: <see langword="true"/> for <see cref="ChannelMode.ReliableLatest"/>, otherwise <see langword="false"/>.</summary>
    public bool? Keyed { get; set; }

    /// <summary>Sequence width, 16 or 32. Default: 32 for keyed channels (and always for ReliableLatest), 16 for unkeyed ones.</summary>
    public byte? SequenceBits { get; set; }

    /// <summary>Unreliable messages larger than one datagram are fragmented (modes 0 and 1 only).</summary>
    public bool Fragmentation { get; set; }

    /// <summary>Payload compression codec.</summary>
    public ChannelCompression Compression { get; set; }

    /// <summary>Frames carry a request id and a response flag (ReliableOrdered only).</summary>
    public bool RequestResponse { get; set; }

    /// <summary>Receiver delivers only the newest queued version per key. Default: on for ReliableLatest (mandatory), off otherwise.</summary>
    public bool? CoalesceOnReceive { get; set; }

    /// <summary>Scheduling priority, 0 (lowest) … 255 (highest). Default 128.</summary>
    public byte Priority { get; set; } = ChannelDefinition.DefaultPriority;

    /// <summary>Largest raw payload in bytes. Default 1 200 (unreliable), 64 KiB (reliable), 16 MiB (Bulk).</summary>
    public int? MaxMessageSize { get; set; }

    /// <summary>Per-channel send queue limit in bytes; 0 (the default) means only the peer's send budget applies.</summary>
    public int? QueueLimitBytes { get; set; }

    /// <summary>
    /// Send expiry in microseconds; 0 = never. Default 0 except UnreliableSequenced, whose default is
    /// <see cref="ChannelDefinition.ExpiryTwiceFlushInterval"/>.
    /// </summary>
    public long? ExpiryMicros { get; set; }

    /// <summary>Keys per channel per peer. Default 4 096 (or max key + 1 for a dense key space).</summary>
    public int? MaxKeys { get; set; }

    /// <summary>Concurrent reassemblies per channel. Default 16.</summary>
    public int? MaxReassemblies { get; set; }

    /// <summary>Concurrent peer streams per channel. Default 8 (ReliableUnordered), 4 (ReliableLatest), 2 (Bulk), 1 (ReliableOrdered), 0 otherwise.</summary>
    public int? MaxGroups { get; set; }

    /// <summary>Bytes admitted into one group stream. Default 64 KiB.</summary>
    public int? GroupMaxBytes { get; set; }

    /// <summary>How per-key state is indexed. Default <see cref="KeySpace.Hashed"/>.</summary>
    public KeySpace KeySpace { get; set; }

    /// <summary>Payloads shorter than this are always sent uncompressed. Default 64.</summary>
    public int? MinCompressSize { get; set; }
}
