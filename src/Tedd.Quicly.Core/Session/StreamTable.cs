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

    /// <summary>
    /// Clock micros of the last progress on a message this end is receiving, or 0 while no message is in flight
    /// (PROTOCOL.md §7 "stream idle mid-message"). The transport thread arms it when a message starts, refreshes it on
    /// every accepted event and clears it when the message ends or the stream is given up; the game thread reads it in
    /// <see cref="StreamTable.TryTakeIdle"/> / <see cref="StreamTable.EarliestMidMessage"/> and disarms an expired
    /// stamp with a compare-exchange (a refresh lost to that race only stops the reset stream from being watched).
    /// </summary>
    public long MidMessageMicros;

    /// <summary>
    /// <see cref="Id"/> packed into one 64-bit word, written with the record and read by the game thread's idle sweep
    /// (a single aligned 64-bit read, like the peer's control-stream id).
    /// </summary>
    public long IdWord;

    /// <summary>The stream's parser (a mutable struct: always accessed by reference).</summary>
    public StreamFrameParser Parser;
}

/// <summary>
/// The peer's stream table (docs/design/session-layer.md §4): receive-side records indexed by the transport's stream
/// slot and validated by generation. Transport thread only; grows (and allocates) only while the peak number of
/// concurrent streams rises during warm-up. Locally opened unidirectional send streams have no record: their events
/// are broadcast to the engines (<see cref="Engines.ChannelEngine.OnStreamClosed"/>).
/// </summary>
/// <remarks>
/// The one exception to "transport thread only" is the mid-message idle sweep (<see cref="TryTakeIdle"/>,
/// <see cref="EarliestMidMessage"/>): the game thread reads <see cref="StreamRecord.MidMessageMicros"/> and
/// <see cref="StreamRecord.IdWord"/> — one aligned 64-bit word each — from a snapshot of the record array, so a stream
/// added concurrently is simply seen by the next sweep. It touches nothing else and changes only that stamp, with a
/// compare-exchange from the value it read.
/// </remarks>
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

            StreamRecord[] grown = new StreamRecord[size];
            Array.Copy(_records, grown, _records.Length);
            // Published so the game thread's idle sweep either sees the old array or a fully copied new one.
            Volatile.Write(ref _records, grown);
        }

        ref StreamRecord r = ref _records[id.Slot];
        if (r.Tag == StreamTag.Free)
        {
            Count++;
        }

        r = default;
        r.Id = id;
        r.Tag = tag;
        r.IdWord = PackId(id);
        return ref r;
    }

    /// <summary>Packs a stream id into one 64-bit word.</summary>
    /// <param name="id">The stream.</param>
    public static long PackId(TransportStreamId id) => (long)(((ulong)id.Generation << 32) | (uint)id.Slot);

    /// <summary>Unpacks a word written by <see cref="PackId"/>.</summary>
    /// <param name="word">The word.</param>
    public static TransportStreamId UnpackId(long word) => new((int)(uint)word, (uint)((ulong)word >> 32));

    /// <summary>
    /// Records progress on a message being received on <paramref name="record"/> (transport thread): arms the
    /// mid-message idle watch of PROTOCOL.md §7 at <paramref name="nowMicros"/>, or clears it when the message is
    /// complete.
    /// </summary>
    /// <param name="record">The stream's record.</param>
    /// <param name="nowMicros">Clock micros of the callback, or 0 to stop watching.</param>
    public static void NoteProgress(ref StreamRecord record, long nowMicros) => Volatile.Write(ref record.MidMessageMicros, nowMicros);

    /// <summary>
    /// Takes the next stream whose message has made no progress since <paramref name="cutoff"/> and stops watching it
    /// (game thread). Call it until it returns <see langword="false"/>; the caller resets each stream it returns.
    /// </summary>
    /// <param name="cutoff">Clock micros: stamps at or below this are expired.</param>
    /// <param name="id">The stalled stream.</param>
    /// <returns><see langword="false"/> when no stream is overdue.</returns>
    public bool TryTakeIdle(long cutoff, out TransportStreamId id)
    {
        StreamRecord[] records = Volatile.Read(ref _records);
        for (int i = 0; i < records.Length; i++)
        {
            ref StreamRecord r = ref records[i];
            long stamp = Volatile.Read(ref r.MidMessageMicros);
            if (stamp == 0 || stamp > cutoff)
            {
                continue;
            }

            if (Interlocked.CompareExchange(ref r.MidMessageMicros, 0, stamp) != stamp)
            {
                continue; // the transport thread moved on: it is not idle after all
            }

            id = UnpackId(Volatile.Read(ref r.IdWord));
            return true;
        }

        id = default;
        return false;
    }

    /// <summary>
    /// The oldest mid-message progress stamp of any watched stream, or <see cref="long.MaxValue"/> when none is
    /// mid-message (game thread; feeds <see cref="QuiclyPeer.NextDeadline"/>).
    /// </summary>
    /// <returns>The oldest stamp.</returns>
    public long EarliestMidMessage()
    {
        StreamRecord[] records = Volatile.Read(ref _records);
        long earliest = long.MaxValue;
        for (int i = 0; i < records.Length; i++)
        {
            long stamp = Volatile.Read(ref records[i].MidMessageMicros);
            if (stamp != 0 && stamp < earliest)
            {
                earliest = stamp;
            }
        }

        return earliest;
    }

    /// <summary>
    /// Drops every record (<see cref="QuiclyPeer.Reconnect"/>): the lost transport's stream slots are about to be handed out
    /// again by a new transport, so a stale record must not be mistaken for one of the resumed connection's streams. Game
    /// thread, while no transport callback can arrive.
    /// </summary>
    public void Clear()
    {
        Array.Clear(_records);
        Count = 0;
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
