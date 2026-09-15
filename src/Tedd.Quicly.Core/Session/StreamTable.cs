using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

/// <summary>What a <see cref="StreamRecord"/> is used for.</summary>
internal enum StreamTag : byte
{
    /// <summary>Unused slot.</summary>
    Free = 0,

    /// <summary>The control stream (parsed by the peer).</summary>
    Control = 1,

    /// <summary>A peer-opened unidirectional stream whose preamble has not been parsed yet.</summary>
    Preamble = 2,

    /// <summary>A peer-opened unidirectional stream accepted by the engine of its channel's mode.</summary>
    Engine = 3,

    /// <summary>A stream this end reset or refused (or whose engine already saw it end): remaining data is consumed and dropped.</summary>
    Discard = 4,
}

/// <summary>
/// Receive-side state of one transport stream: role, channel, owning engine and the resumable frame parser.
/// Reference-free; owned by the transport thread.
/// </summary>
internal struct StreamRecord
{
    /// <summary>The transport stream.</summary>
    public TransportStreamId Id;

    /// <summary>Use of the record.</summary>
    public StreamTag Tag;

    /// <summary>Mode of the preamble's channel (engine streams).</summary>
    public ChannelMode Mode;

    /// <summary>Preamble channel id (engine streams).</summary>
    public ushort Channel;

    /// <summary>Dense index of <see cref="Channel"/> (engine streams).</summary>
    public int ChannelIndex;

    /// <summary>Engine-owned per-stream value (<see cref="Engines.StreamAccept.Cookie"/>).</summary>
    public long Cookie;

    /// <summary>The stream's parser (a mutable struct: always accessed by reference).</summary>
    public StreamFrameParser Parser;
}

/// <summary>
/// The peer's stream table (docs/design/session-layer.md §4): receive-side records indexed by the transport's stream
/// slot and validated by generation. Transport thread only; grows (and allocates) only while the peak number of
/// concurrent streams rises during warm-up. Locally opened unidirectional send streams have no record: their events
/// are broadcast to the engines (<see cref="Engines.ChannelEngine.OnStreamClosed"/>).
/// </summary>
internal sealed class StreamTable
{
    private StreamRecord[] _records = new StreamRecord[8];

    /// <summary>Live records.</summary>
    public int Count { get; private set; }

    /// <summary>The record of <paramref name="id"/>, or a null reference (<see cref="Unsafe.IsNullRef{T}(ref readonly T)"/>).</summary>
    /// <param name="id">The stream.</param>
    /// <returns>A reference into the table; valid until the next <see cref="Add"/>.</returns>
    public ref StreamRecord Find(TransportStreamId id)
    {
        StreamRecord[] records = _records;
        if ((uint)id.Slot < (uint)records.Length)
        {
            ref StreamRecord r = ref records[id.Slot];
            if (r.Tag != StreamTag.Free && r.Id == id)
            {
                return ref r;
            }
        }

        return ref Unsafe.NullRef<StreamRecord>();
    }

    /// <summary>Creates (or replaces) the record of <paramref name="id"/>.</summary>
    /// <param name="id">The stream.</param>
    /// <param name="tag">Its use.</param>
    /// <returns>The zeroed record; valid until the next <see cref="Add"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The slot is negative.</exception>
    public ref StreamRecord Add(TransportStreamId id, StreamTag tag)
    {
        if (id.Slot < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(id), id, "Stream slots are non-negative.");
        }

        if (id.Slot >= _records.Length)
        {
            int size = _records.Length;
            while (size <= id.Slot)
            {
                size *= 2;
            }

            Array.Resize(ref _records, size);
        }

        ref StreamRecord r = ref _records[id.Slot];
        if (r.Tag == StreamTag.Free)
        {
            Count++;
        }

        r = default;
        r.Id = id;
        r.Tag = tag;
        return ref r;
    }

    /// <summary>Removes the record of <paramref name="id"/> if it is live.</summary>
    /// <param name="id">The stream.</param>
    public void Remove(TransportStreamId id)
    {
        ref StreamRecord r = ref Find(id);
        if (!Unsafe.IsNullRef(ref r))
        {
            r = default;
            Count--;
        }
    }
}
