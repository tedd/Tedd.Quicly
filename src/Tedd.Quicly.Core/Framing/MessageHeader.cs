namespace Tedd.Quicly.Core.Framing;

/// <summary>
/// Header fields of an application datagram message (PROTOCOL.md §2.1). Fields the channel does not carry are 0
/// after parsing and ignored when writing (for example <see cref="Key"/> on an unkeyed channel).
/// </summary>
public struct MessageHeader
{
    /// <summary>Channel id.</summary>
    public ushort Channel;

    /// <summary>Sequence number (the low 16 bits only on a 16-bit channel).</summary>
    public uint Sequence;

    /// <summary>Key (keyed channels), ≤ 2^62−1.</summary>
    public ulong Key;

    /// <summary>Fragment count, 1…8, on fragmenting channels (writers treat 0 as 1); 1 after parsing a non-fragmenting channel.</summary>
    public byte FragCount;

    /// <summary>Fragment index, &lt; <see cref="FragCount"/>; present on the wire only when <see cref="FragCount"/> &gt; 1.</summary>
    public byte FragIndex;

    /// <summary>Decoded size of a compressed payload; 0 = the payload is not compressed.</summary>
    public int RawLength;

    /// <summary><see langword="true"/> when the payload is an LZ4 block of <see cref="RawLength"/> decoded bytes.</summary>
    public readonly bool Compressed => RawLength > 0;

    /// <summary><see langword="true"/> when this datagram carries one fragment of a larger message.</summary>
    public readonly bool IsFragment => FragCount > 1;
}
