namespace Tedd.Quicly.Core.Channels;

/// <summary>
/// One application channel of a <see cref="ChannelTable"/> (PROTOCOL.md §1). Immutable; created only by
/// <see cref="ChannelTableBuilder.Build"/>, which validates every field and precomputes the derived values the
/// framing hot path reads.
/// </summary>
public sealed class ChannelDefinition
{
    /// <summary>Smallest application channel id (0 is control, 1 is the packed container).</summary>
    public const int MinId = 2;

    /// <summary>Largest application channel id (the largest two-byte varint).</summary>
    public const int MaxId = 16383;

    /// <summary>Largest channel id encoded as a one-byte varint.</summary>
    public const int MaxOneByteId = 63;

    /// <summary>Largest channel name, in UTF-8 bytes.</summary>
    public const int MaxNameBytes = 64;

    /// <summary>Default scheduling priority.</summary>
    public const byte DefaultPriority = 128;

    /// <summary>Default and maximum <see cref="MaxMessageSize"/> of an unreliable channel without fragmentation.</summary>
    public const int UnreliableMaxMessageSize = 1200;

    /// <summary>Maximum <see cref="MaxMessageSize"/> of a fragmenting channel (8 fragments of 1 100 bytes).</summary>
    public const int FragmentedMaxMessageSize = 8 * 1100;

    /// <summary>Default <see cref="MaxMessageSize"/> of ReliableOrdered, ReliableUnordered and ReliableLatest channels.</summary>
    public const int ReliableDefaultMaxMessageSize = 64 * 1024;

    /// <summary>Maximum <see cref="MaxMessageSize"/> of ReliableOrdered, ReliableUnordered and ReliableLatest channels.</summary>
    public const int ReliableMaxMessageSize = 1024 * 1024;

    /// <summary>Default and maximum <see cref="MaxMessageSize"/> of a Bulk channel (bounds one transfer's <c>Length</c>).</summary>
    public const int BulkMaxMessageSize = 16 * 1024 * 1024;

    /// <summary>
    /// <see cref="ExpiryMicros"/> sentinel: expire after twice the peer's flush interval (the default of
    /// UnreliableSequenced channels, PROTOCOL.md §4.5). The peer resolves it; the table does not know the interval.
    /// </summary>
    public const long ExpiryTwiceFlushInterval = -1;

    internal ChannelDefinition(
        ushort id,
        string name,
        byte[] nameUtf8,
        ChannelMode mode,
        bool keyed,
        byte sequenceBits,
        bool fragmentation,
        ChannelCompression compression,
        bool requestResponse,
        bool coalesceOnReceive,
        byte priority,
        int maxMessageSize,
        int queueLimitBytes,
        long expiryMicros,
        int maxKeys,
        int maxReassemblies,
        int maxGroups,
        int groupMaxBytes,
        KeySpace keySpace,
        int minCompressSize)
    {
        Id = id;
        Name = name;
        NameUtf8 = nameUtf8;
        Mode = mode;
        Keyed = keyed;
        SequenceBits = sequenceBits;
        Fragmentation = fragmentation;
        Compression = compression;
        RequestResponse = requestResponse;
        CoalesceOnReceive = coalesceOnReceive;
        Priority = priority;
        MaxMessageSize = maxMessageSize;
        QueueLimitBytes = queueLimitBytes;
        ExpiryMicros = expiryMicros;
        MaxKeys = maxKeys;
        MaxReassemblies = maxReassemblies;
        MaxGroups = maxGroups;
        GroupMaxBytes = groupMaxBytes;
        KeySpace = keySpace;
        MinCompressSize = minCompressSize;

        HasSequence = mode is ChannelMode.UnreliableSequenced or ChannelMode.ReliableLatest || fragmentation;
        IsDatagramMode = mode is ChannelMode.UnreliableUnordered or ChannelMode.UnreliableSequenced or ChannelMode.ReliableLatest;
        IsStreamMode = mode is ChannelMode.ReliableOrdered or ChannelMode.ReliableUnordered or ChannelMode.ReliableLatest or ChannelMode.Bulk;
        ChannelIdLength = id <= MaxOneByteId ? 1 : 2;
        SequenceBytes = (byte)(HasSequence ? sequenceBits / 8 : 0);
        FixedHeaderBytesWithoutKey = ChannelIdLength + SequenceBytes + (fragmentation ? 1 : 0);
        StreamShape = (byte)((keyed ? Framing.StreamFraming.ShapeKeyed : 0)
            | (requestResponse ? Framing.StreamFraming.ShapeRequestId : 0)
            | (compression != ChannelCompression.None ? Framing.StreamFraming.ShapeCompressed : 0)
            | (mode == ChannelMode.ReliableLatest ? Framing.StreamFraming.ShapeSequence : 0));
        CanonicalFlags = (byte)((keyed ? 0x01 : 0)
            | (sequenceBits == 32 ? 0x02 : 0)
            | (fragmentation ? 0x04 : 0)
            | (requestResponse ? 0x08 : 0)
            | (coalesceOnReceive ? 0x10 : 0)
            | ((int)compression << 5));
    }

    /// <summary>Channel id, 2…16383.</summary>
    public ushort Id { get; }

    /// <summary>Diagnostic name (≤ 64 UTF-8 bytes); not part of the table hash.</summary>
    public string Name { get; }

    /// <summary>Delivery mode.</summary>
    public ChannelMode Mode { get; }

    /// <summary>Messages carry a key.</summary>
    public bool Keyed { get; }

    /// <summary>Sequence width in bits, 16 or 32 (written on the wire only when <see cref="HasSequence"/>).</summary>
    public byte SequenceBits { get; }

    /// <summary>Unreliable messages larger than one datagram are fragmented.</summary>
    public bool Fragmentation { get; }

    /// <summary>Payload compression codec; when not <see cref="ChannelCompression.None"/> every frame carries a <c>RawLength</c>.</summary>
    public ChannelCompression Compression { get; }

    /// <summary>ReliableOrdered frames carry a request id.</summary>
    public bool RequestResponse { get; }

    /// <summary>Receiver delivers only the newest queued version per key.</summary>
    public bool CoalesceOnReceive { get; }

    /// <summary>Scheduling priority, 0 (lowest) … 255 (highest).</summary>
    public byte Priority { get; }

    /// <summary>Largest raw (decoded) payload in bytes; for Bulk, the largest <c>Length</c> of one transfer.</summary>
    public int MaxMessageSize { get; }

    /// <summary>Per-channel send queue limit in bytes; 0 = only the peer budget applies.</summary>
    public int QueueLimitBytes { get; }

    /// <summary>Send expiry in microseconds; 0 = never; <see cref="ExpiryTwiceFlushInterval"/> = twice the flush interval.</summary>
    public long ExpiryMicros { get; }

    /// <summary>Keys per channel per peer.</summary>
    public int MaxKeys { get; }

    /// <summary>Concurrent reassemblies per channel.</summary>
    public int MaxReassemblies { get; }

    /// <summary>Concurrent peer streams per channel (0 for datagram-only modes).</summary>
    public int MaxGroups { get; }

    /// <summary>Bytes admitted into one group stream.</summary>
    public int GroupMaxBytes { get; }

    /// <summary>How per-key state is indexed.</summary>
    public KeySpace KeySpace { get; }

    /// <summary>Payloads shorter than this are sent uncompressed.</summary>
    public int MinCompressSize { get; }

    /// <summary>Datagram frames carry a sequence field (UnreliableSequenced, ReliableLatest, or any fragmenting channel).</summary>
    public bool HasSequence { get; }

    /// <summary>Messages may be carried in DATAGRAM frames (modes 0, 1 and 4).</summary>
    public bool IsDatagramMode { get; }

    /// <summary>
    /// Messages may be carried on a unidirectional stream whose preamble names this channel (modes 2, 3, 4 and 5).
    /// Both this and <see cref="IsDatagramMode"/> are true for ReliableLatest (large values use group streams).
    /// </summary>
    public bool IsStreamMode { get; }

    /// <summary>
    /// Datagram header bytes that do not depend on the message: channel id + sequence + <c>FragCount</c>.
    /// The key varint, <c>FragIndex</c> (fragmented messages only) and the <c>RawLength</c> varint come on top.
    /// </summary>
    public int FixedHeaderBytesWithoutKey { get; }

    /// <summary>Encoded size of the channel id varint: 1 for ids ≤ 63, otherwise 2.</summary>
    public int ChannelIdLength { get; }

    /// <summary>Bytes of the datagram sequence field: 0, 2 or 4.</summary>
    internal byte SequenceBytes { get; }

    /// <summary>Which optional fields a stream message frame of this channel carries (<c>StreamFraming.Shape*</c> bits).</summary>
    internal byte StreamShape { get; }

    /// <summary>The <c>flags</c> byte of the canonical table encoding.</summary>
    internal byte CanonicalFlags { get; }

    /// <summary>UTF-8 bytes of <see cref="Name"/>.</summary>
    internal byte[] NameUtf8 { get; }

    /// <inheritdoc/>
    public override string ToString() => $"{Id} '{Name}' {Mode}";
}
