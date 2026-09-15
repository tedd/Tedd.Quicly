using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.State;

/// <summary>Lifecycle of a <see cref="SendEntry"/> slot. Stored in <see cref="SendEntry.State"/> as an <see cref="int"/>.</summary>
public enum SendEntryState : int
{
    /// <summary>On the free list; owned by the game thread.</summary>
    Free = 0,
    /// <summary>Allocated and being filled by the game thread; not yet visible to the transport thread.</summary>
    Filling = 1,
    /// <summary>Published: the transport may hold pointers into the entry. Only CAS transitions from here on.</summary>
    InFlight = 2,
    /// <summary>The game thread asked for cancellation; the transport thread decides the outcome.</summary>
    Cancelling = 3,
    /// <summary>The transport is done with the entry; the game thread may release its resources and free the slot.</summary>
    Completed = 4,
}

/// <summary>Per-entry flags stored in <see cref="SendEntry.Flags"/>.</summary>
[Flags]
public enum SendEntryFlags : byte
{
    /// <summary>No flags.</summary>
    None = 0,
    /// <summary>The send has a <see cref="Threading.SendToken"/>; completion must be reported to the <see cref="Threading.CompletionTable"/>.</summary>
    Tracked = 1,
    /// <summary>Sent as a datagram (completion arrives as datagram send states) rather than on a stream.</summary>
    Datagram = 2,
    /// <summary>The entry is a packed container whose completion fans out to its members (see <see cref="SendEntryTable.GetBatch"/>).</summary>
    Container = 4,
    /// <summary>Flush the channel at the end of the call that queued the entry.</summary>
    Immediate = 8,
    /// <summary>The stream send carries FIN.</summary>
    Fin = 16,
    /// <summary>The entry is a retransmission (ReliableLatest retry); counted separately.</summary>
    Retry = 32,
    /// <summary>The payload is caller memory pinned through a handle in <see cref="SendEntryTable.PinHandles"/> that must be released on completion.</summary>
    Pinned = 64,
    /// <summary>
    /// The owning engine decides the delivery status on the game thread (for example ReliableLatest, whose Delivered comes
    /// from a LatestAck): in <c>CompletionMode.ThreadPool</c> the transport thread never signals this entry's token directly.
    /// </summary>
    EngineCompletes = 128,
}

/// <summary>
/// The hot part of one queued or in-flight send: exactly 64 bytes (one cache line) in native memory, laid out so
/// that <see cref="Header"/> and <see cref="Payload"/> form a contiguous <c>QUIC_BUFFER[2]</c> the transport can be
/// pointed at directly.
/// </summary>
/// <remarks>
/// <para>Layout (explicit, byte offsets):</para>
/// <list type="table">
/// <item><term>0</term><description><see cref="State"/> (<see cref="int"/>, <see cref="SendEntryState"/>) — the only field written by both threads, via CAS.</description></item>
/// <item><term>4</term><description><see cref="Generation"/> (<see cref="uint"/>, odd, bumped per allocation).</description></item>
/// <item><term>8</term><description><see cref="Channel"/> (<see cref="ushort"/>).</description></item>
/// <item><term>10</term><description><see cref="Flags"/> (<see cref="SendEntryFlags"/>, 1 byte).</description></item>
/// <item><term>11</term><description><see cref="HeaderLength"/> (1 byte; valid bytes in the slot's header block).</description></item>
/// <item><term>12</term><description>reserved (4 bytes, zero).</description></item>
/// <item><term>16</term><description><see cref="Header"/> (<see cref="TransportSegment"/>, 16 bytes) — normally points at the slot's 32-byte header block (<see cref="SendEntryTable.GetHeaderBlock"/>).</description></item>
/// <item><term>32</term><description><see cref="Payload"/> (<see cref="TransportSegment"/>, 16 bytes) — the message bytes.</description></item>
/// <item><term>48</term><description><see cref="Aux0"/>, <see cref="Aux1"/> (8 bytes each) — scratch words owned by the engine that queued the entry.</description></item>
/// </list>
/// <para>The encoded header does not live in the entry: the largest datagram header is 24 bytes, so header bytes are
/// kept in a cold native array of 32-byte blocks indexed by slot (ADR 0008 invariant 1 as amended). A block stays
/// reserved with its slot until the completion is observed.</para>
/// <para>Ownership: the game thread writes every field while the entry is <see cref="SendEntryState.Filling"/>
/// and never again until it has observed the completion; the transport thread only reads
/// <see cref="Generation"/>, <see cref="Channel"/>, <see cref="Flags"/> and the segments, and only changes
/// <see cref="State"/> through <see cref="SendEntryTable.TryTransition"/>. Cold per-entry data lives in the
/// structure-of-arrays side tables of <see cref="SendEntryTable"/>.</para>
/// </remarks>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public unsafe struct SendEntry
{
    /// <summary>Size of the struct in bytes: one cache line.</summary>
    public const int Size = 64;

    /// <summary>Byte offset of <see cref="State"/>.</summary>
    public const int StateOffset = 0;
    /// <summary>Byte offset of <see cref="Generation"/>.</summary>
    public const int GenerationOffset = 4;
    /// <summary>Byte offset of <see cref="Channel"/>.</summary>
    public const int ChannelOffset = 8;
    /// <summary>Byte offset of <see cref="Flags"/>.</summary>
    public const int FlagsOffset = 10;
    /// <summary>Byte offset of <see cref="HeaderLength"/>.</summary>
    public const int HeaderLengthOffset = 11;
    /// <summary>Byte offset of <see cref="Header"/>; <see cref="Payload"/> follows immediately.</summary>
    public const int HeaderOffset = 16;
    /// <summary>Byte offset of <see cref="Payload"/>.</summary>
    public const int PayloadOffset = 32;
    /// <summary>Byte offset of <see cref="Aux0"/>; <see cref="Aux1"/> follows immediately.</summary>
    public const int AuxOffset = 48;

    /// <summary>Current <see cref="SendEntryState"/>. Read with <see cref="Volatile"/>, changed with CAS once published.</summary>
    [FieldOffset(StateOffset)] public int State;

    /// <summary>Generation of the current occupant (always odd); part of the context handed to the transport.</summary>
    [FieldOffset(GenerationOffset)] public uint Generation;

    /// <summary>Channel id the message belongs to.</summary>
    [FieldOffset(ChannelOffset)] public ushort Channel;

    /// <summary>Entry flags.</summary>
    [FieldOffset(FlagsOffset)] public SendEntryFlags Flags;

    /// <summary>Number of valid bytes in the slot's header block (0 … <see cref="SendEntryTable.HeaderBlockSize"/>).</summary>
    [FieldOffset(HeaderLengthOffset)] public byte HeaderLength;

    /// <summary>First of the two contiguous transport segments (<c>QUIC_BUFFER[0]</c>): the encoded header.</summary>
    [FieldOffset(HeaderOffset)] public TransportSegment Header;

    /// <summary>Second transport segment (<c>QUIC_BUFFER[1]</c>): the payload.</summary>
    [FieldOffset(PayloadOffset)] public TransportSegment Payload;

    /// <summary>Engine-owned scratch word (game thread), zeroed at allocation.</summary>
    [FieldOffset(AuxOffset)] public long Aux0;

    /// <summary>Engine-owned scratch word (game thread), zeroed at allocation.</summary>
    [FieldOffset(AuxOffset + 8)] public long Aux1;
}
