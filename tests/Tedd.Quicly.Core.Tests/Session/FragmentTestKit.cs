using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Channel tables of the fragmentation tests (immutable, shared across parallel tests).</summary>
internal static class FragmentTables
{
    /// <summary>
    /// 2 unordered fragmenting (8 800 B) · 3 sequenced keyed fragmenting (8 800 B) · 4 unordered fragmenting LZ4
    /// (MinCompressSize 64) · 5 sequenced keyed fragmenting with MaxReassemblies 2 · 6 unordered fragmenting, 2 400 B ·
    /// 7 unordered without fragmentation (the control) · 8 unordered keyed coalescing fragmenting (dense keys 0 … 3).
    /// </summary>
    public static ChannelTable Main { get; } = ChannelTable.Create()
        .Add(2, "chunks", ChannelMode.UnreliableUnordered, o => { o.Fragmentation = true; o.MaxMessageSize = ChannelDefinition.FragmentedMaxMessageSize; })
        .Add(3, "state", ChannelMode.UnreliableSequenced, o =>
        {
            o.Fragmentation = true;
            o.Keyed = true;
            o.MaxMessageSize = ChannelDefinition.FragmentedMaxMessageSize;
            o.ExpiryMicros = 0;
        })
        .Add(4, "packed", ChannelMode.UnreliableUnordered, o =>
        {
            o.Fragmentation = true;
            o.Compression = ChannelCompression.Lz4;
            o.MaxMessageSize = ChannelDefinition.FragmentedMaxMessageSize;
        })
        .Add(5, "tight", ChannelMode.UnreliableSequenced, o =>
        {
            o.Fragmentation = true;
            o.Keyed = true;
            o.MaxReassemblies = 2;
            o.MaxMessageSize = ChannelDefinition.FragmentedMaxMessageSize;
            o.ExpiryMicros = 0;
        })
        .Add(6, "small", ChannelMode.UnreliableUnordered, o => { o.Fragmentation = true; o.MaxMessageSize = 2400; })
        .Add(7, "plain", ChannelMode.UnreliableUnordered)
        .Add(8, "latest", ChannelMode.UnreliableUnordered, o =>
        {
            o.Fragmentation = true;
            o.Keyed = true;
            o.CoalesceOnReceive = true;
            o.KeySpace = KeySpace.Dense(3);
            o.MaxMessageSize = ChannelDefinition.FragmentedMaxMessageSize;
        })
        .Build();
}

/// <summary>Helpers of the fragmentation tests.</summary>
internal static class FragmentKit
{
    /// <summary>The datagram engine of a mode (for the reassembly accessors).</summary>
    public static DatagramEngine Engine(QuiclyPeer peer, ChannelMode mode) => (DatagramEngine)peer.Core.GetEngine(mode)!;

    /// <summary>Partial reassemblies a channel holds right now.</summary>
    public static int Reassemblies(QuiclyPeer peer, ushort channel)
    {
        ChannelDefinition definition = peer.Channels[channel]!;
        return Engine(peer, definition.Mode).Reassemblies(peer.Core.ChannelIndexOf(channel));
    }

    /// <summary>The reassembly expiry window the receiver uses (2 × RTT + 100 ms).</summary>
    public static long Window(QuiclyPeer peer, ChannelMode mode = ChannelMode.UnreliableUnordered) => Engine(peer, mode).ReassemblyWindowMicros;

    /// <summary>Payload bytes LZ4 cannot shrink (so a compressed channel sends them raw).</summary>
    public static byte[] Noise(int length, int seed)
    {
        byte[] data = new byte[length];
        new Random(seed).NextBytes(data);
        return data;
    }

    /// <summary>Payload bytes LZ4 shrinks a lot.</summary>
    public static byte[] Compressible(int length, byte seed)
    {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++)
        {
            data[i] = (byte)(seed + (i / 64));
        }

        return data;
    }

    /// <summary>
    /// Payload bytes one fragment of <paramref name="channel"/> carries on a path of <paramref name="maxDatagramPayload"/>
    /// bytes. Exact for a channel without compression; on a compressed one the real <c>RawLength</c> varint may be longer
    /// than the one byte assumed here.
    /// </summary>
    /// <param name="channel">A fragmenting channel.</param>
    /// <param name="maxDatagramPayload">The path's current datagram limit.</param>
    /// <returns>The capacity in bytes.</returns>
    public static int Capacity(ChannelDefinition channel, int maxDatagramPayload = 1200)
    {
        MessageHeader header = default;
        header.FragCount = 2;
        return maxDatagramPayload - DatagramFraming.GetHeaderLength(channel, in header);
    }

    /// <summary>A message length that needs exactly <paramref name="count"/> fragments on <paramref name="channel"/>.</summary>
    /// <param name="channel">A fragmenting channel without compression.</param>
    /// <param name="count">Wanted fragment count, 2 … 8.</param>
    /// <param name="maxDatagramPayload">The path's current datagram limit.</param>
    /// <returns>The length in bytes.</returns>
    public static int LengthFor(ChannelDefinition channel, int count, int maxDatagramPayload = 1200) =>
        ((count - 1) * Capacity(channel, maxDatagramPayload)) + 10;

    /// <summary>The fragments a message of <paramref name="length"/> bytes is split into on a channel (the sender's own layout).</summary>
    /// <param name="channel">A fragmenting channel.</param>
    /// <param name="maxDatagramPayload">The path's current datagram limit.</param>
    /// <param name="length">Payload bytes on the wire.</param>
    /// <returns>Fragment count, the size of every fragment but the last, and the last fragment's size.</returns>
    public static (int Count, int Size, int LastSize) Layout(ChannelDefinition channel, int maxDatagramPayload, int length)
    {
        int capacity = Capacity(channel, maxDatagramPayload);
        int count = (length + capacity - 1) / capacity;
        int size = (length + count - 1) / count;
        return (count, size, length - (size * (count - 1)));
    }

    /// <summary>
    /// One fragment frame written field by field, so a test can send values the writer would refuse (a <c>FragCount</c> of 0
    /// or 9, a <c>FragIndex</c> at or beyond the count, mismatched sizes, an empty fragment).
    /// </summary>
    /// <param name="channel">The channel (its id must be ≤ 63, so it is one byte).</param>
    /// <param name="sequenceBytes">2 or 4.</param>
    /// <param name="sequence">The message's sequence.</param>
    /// <param name="key">The key, or <see langword="null"/> on an unkeyed channel.</param>
    /// <param name="fragCount">The <c>FragCount</c> byte, as it should appear on the wire.</param>
    /// <param name="fragIndex">The <c>FragIndex</c> byte, or <see langword="null"/> to leave the field out.</param>
    /// <param name="rawLength">The <c>RawLength</c> varint, or <see langword="null"/> on a channel without compression.</param>
    /// <param name="payload">The fragment's payload.</param>
    /// <returns>The datagram.</returns>
    public static byte[] RawFragment(ushort channel, int sequenceBytes, uint sequence, ulong? key, byte fragCount, byte? fragIndex, int? rawLength, ReadOnlySpan<byte> payload)
    {
        byte[] frame = new byte[32 + payload.Length];
        int pos = 0;
        frame[pos++] = (byte)channel;
        if (sequenceBytes == 2)
        {
            frame[pos++] = (byte)sequence;
            frame[pos++] = (byte)(sequence >> 8);
        }
        else
        {
            frame[pos++] = (byte)sequence;
            frame[pos++] = (byte)(sequence >> 8);
            frame[pos++] = (byte)(sequence >> 16);
            frame[pos++] = (byte)(sequence >> 24);
        }

        if (key is { } value)
        {
            pos += VarInt.Write(frame.AsSpan(pos), value);
        }

        frame[pos++] = fragCount;
        if (fragIndex is { } index)
        {
            frame[pos++] = index;
        }

        if (rawLength is { } raw)
        {
            pos += VarInt.Write(frame.AsSpan(pos), (ulong)raw);
        }

        payload.CopyTo(frame.AsSpan(pos));
        return frame.AsSpan(0, pos + payload.Length).ToArray();
    }

    /// <summary>The fragments of one message as the sender would write them (used to reorder, duplicate or drop them).</summary>
    /// <param name="channel">A fragmenting channel.</param>
    /// <param name="sequence">The message's sequence.</param>
    /// <param name="key">The key (0 on unkeyed channels).</param>
    /// <param name="message">The whole payload.</param>
    /// <param name="count">Fragments to split it into (2 … 8).</param>
    /// <returns>The datagrams, in fragment order.</returns>
    public static List<byte[]> Split(ChannelDefinition channel, uint sequence, ulong key, ReadOnlySpan<byte> message, int count)
    {
        int size = (message.Length + count - 1) / count;
        List<byte[]> frames = new(count);
        for (int index = 0; index < count; index++)
        {
            int offset = index * size;
            int length = Math.Min(size, message.Length - offset);
            MessageHeader header = default;
            header.Sequence = sequence;
            header.Key = channel.Keyed ? key : 0;
            header.FragCount = (byte)count;
            header.FragIndex = (byte)index;
            byte[] frame = new byte[DatagramFraming.MaxHeaderLength + length];
            int written = DatagramFraming.WriteHeader(frame, channel, in header);
            message.Slice(offset, length).CopyTo(frame.AsSpan(written));
            frames.Add(frame.AsSpan(0, written + length).ToArray());
        }

        return frames;
    }
}
