using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>The payload of an admission in progress: what the send entry takes when admission commits (game thread).</summary>
internal unsafe struct PreparedPayload
{
    /// <summary>The lease the entry takes (rented here, or the caller's owned lease or single gathered page), or empty.</summary>
    public BufferLease Lease;

    /// <summary><see cref="Lease"/> was rented here (give it back if admission fails).</summary>
    public bool Rented;

    /// <summary>The caller's single gathered page is taken as it is (zero copy; see <see cref="EnginePayload.TryPrepare"/>).</summary>
    public bool TookPage;

    /// <summary>First payload byte.</summary>
    public byte* Pointer;

    /// <summary>Payload bytes on the wire (the compressed size when <see cref="RawLength"/> &gt; 0).</summary>
    public int Length;

    /// <summary>Decoded size of a compressed payload, else 0.</summary>
    public int RawLength;

    /// <summary>The GC handle pinning a borrowed array, or 0.</summary>
    public nint Pin;
}

/// <summary>
/// The send paths every engine shares (ARCHITECTURE.md §4.1; docs/design/session-layer.md §7.1): a copy into a send lease,
/// the caller's owned lease (zero copy), pinned or native memory (zero copy), borrowed memory (an array is pinned with a
/// <see cref="GCHandle"/>, other memory is copied), and gathered pages (copied into one lease, or, when the engine allows
/// it, a single page taken as it is). LZ4 applies when the channel enables it, the payload is at least
/// max(2, <see cref="ChannelDefinition.MinCompressSize"/>) bytes and the block is strictly shorter (PROTOCOL.md §2.1, §8);
/// otherwise <see cref="PreparedPayload.RawLength"/> is 0. Game thread; allocation-free. Protocol: <see cref="TryPrepare"/>,
/// then either <see cref="Commit"/> (the entry takes the payload) or <see cref="Release"/> (admission failed; the caller
/// keeps its lease or pages).
/// </summary>
internal static unsafe class EnginePayload
{
    /// <summary>Total length of gathered pages (each contributes its whole <see cref="BufferLease.Length"/>).</summary>
    /// <param name="pages">The pages.</param>
    /// <returns>The length in bytes.</returns>
    public static int GatherLength(ReadOnlySpan<BufferLease> pages)
    {
        int length = 0;
        foreach (BufferLease page in pages)
        {
            length += page.Length;
        }

        return length;
    }

    /// <summary>Whether a payload of <paramref name="length"/> bytes should be LZ4-compressed on <paramref name="channel"/>.</summary>
    /// <param name="channel">The channel.</param>
    /// <param name="length">Raw payload bytes.</param>
    /// <returns><see langword="true"/> to try compression.</returns>
    public static bool ShouldCompress(ChannelDefinition channel, int length) =>
        channel.Compression == ChannelCompression.Lz4 && length >= 2 && length >= channel.MinCompressSize;

    /// <summary>Prepares the payload of <paramref name="request"/> without committing it.</summary>
    /// <param name="core">The peer's shared state (send leases and budget).</param>
    /// <param name="request">The request.</param>
    /// <param name="channel">Its channel.</param>
    /// <param name="length">Raw payload bytes (for gathers: <see cref="GatherLength"/>).</param>
    /// <param name="takeSinglePage">A gather of exactly one page that is not compressed is taken as it is (zero copy).</param>
    /// <param name="payload">Receives the prepared payload.</param>
    /// <returns><see langword="false"/> when no send lease was available (answer <see cref="SendStatus.OutOfBuffers"/>).</returns>
    public static bool TryPrepare(PeerCore core, ref SendRequest request, ChannelDefinition channel, int length, bool takeSinglePage, ref PreparedPayload payload)
    {
        switch (request.Kind)
        {
            case SendPayloadKind.Copy:
                return TryCopyIn(core, request.Source, channel, compress: true, ref payload);
            case SendPayloadKind.Owned:
            {
                byte* data = length == 0 ? null : core.GetPointer(in request.Lease);
                if (!TryCompressFrom(core, new ReadOnlySpan<byte>(data, length), channel, ref payload))
                {
                    // Zero copy: the entry takes the caller's lease on commit (the caller keeps it if admission fails).
                    payload.Lease = request.Lease;
                    payload.Pointer = data;
                    payload.Length = length;
                }

                return true;
            }

            case SendPayloadKind.Pinned:
                if (!TryCompressFrom(core, new ReadOnlySpan<byte>(request.Pointer, length), channel, ref payload))
                {
                    payload.Pointer = request.Pointer;
                    payload.Length = length;
                }

                return true;
            case SendPayloadKind.Borrowed:
            {
                ReadOnlyMemory<byte> memory = request.Borrowed;
                if (length == 0 || TryCompressFrom(core, memory.Span, channel, ref payload))
                {
                    return true;
                }

                if (MemoryMarshal.TryGetArray(memory, out ArraySegment<byte> segment) && segment.Array is not null)
                {
                    GCHandle handle = GCHandle.Alloc(segment.Array, GCHandleType.Pinned);
                    payload.Pin = GCHandle.ToIntPtr(handle);
                    payload.Pointer = (byte*)handle.AddrOfPinnedObject() + segment.Offset;
                    payload.Length = length;
                    return true;
                }

                // Not array-backed: a copy is the safe convenience (compression was already tried).
                return TryCopyIn(core, memory.Span, channel, compress: false, ref payload);
            }

            default:
            {
                ReadOnlySpan<BufferLease> pages = request.Gather;
                if (takeSinglePage && pages.Length == 1 && !pages[0].IsEmpty && !ShouldCompress(channel, length))
                {
                    payload.Lease = pages[0];
                    payload.TookPage = true;
                    payload.Pointer = core.GetPointer(in pages[0]);
                    payload.Length = length;
                    return true;
                }

                return TryGatherIn(core, pages, length, channel, ref payload);
            }
        }
    }

    /// <summary>Gives back what <see cref="TryPrepare"/> took after admission failed (the caller keeps its own lease or pages).</summary>
    /// <param name="core">The peer's shared state.</param>
    /// <param name="payload">The prepared payload.</param>
    public static void Release(PeerCore core, in PreparedPayload payload)
    {
        if (payload.Rented)
        {
            core.ReturnSend(in payload.Lease);
        }

        if (payload.Pin != 0)
        {
            GCHandle.FromIntPtr(payload.Pin).Free();
        }
    }

    /// <summary>
    /// Hands the prepared payload to the entry: its lease (released with the entry), its payload segment and its pin handle
    /// (<see cref="SendEntryFlags.Pinned"/>). An owned lease or gathered pages whose bytes were copied or compressed away go
    /// back to the pool now.
    /// </summary>
    /// <param name="core">The peer's shared state.</param>
    /// <param name="slot">The <c>Filling</c> entry.</param>
    /// <param name="request">The request.</param>
    /// <param name="payload">The prepared payload.</param>
    public static void Commit(PeerCore core, int slot, ref SendRequest request, in PreparedPayload payload)
    {
        SendEntryTable entries = core.Entries;
        if (!payload.Lease.IsEmpty)
        {
            entries.Leases[slot] = payload.Lease;
        }

        core.SetPayload(slot, payload.Pointer, payload.Length);
        if (payload.Pin != 0)
        {
            entries.PinHandles[slot] = payload.Pin;
            entries[slot].Flags |= SendEntryFlags.Pinned;
        }

        if (request.Kind == SendPayloadKind.Owned && payload.Rented)
        {
            core.ReturnSend(in request.Lease);
        }
        else if (request.Kind == SendPayloadKind.Gather && !payload.TookPage)
        {
            foreach (BufferLease page in request.Gather)
            {
                core.ReturnSend(in page);
            }
        }
    }

    private static bool TryCopyIn(PeerCore core, ReadOnlySpan<byte> source, ChannelDefinition channel, bool compress, ref PreparedPayload payload)
    {
        int length = source.Length;
        if (length == 0)
        {
            return true;
        }

        if (!core.TryRentSend(length, out BufferLease lease))
        {
            return false;
        }

        Span<byte> target = core.GetSpan(in lease);
        payload.Lease = lease;
        payload.Rented = true;
        payload.Pointer = core.GetPointer(in lease);
        if (compress && ShouldCompress(channel, length))
        {
            // Compression must shrink the payload (PROTOCOL.md §2.1): a destination one byte short makes it fail otherwise.
            int compressed = Lz4Block.Compress(source, target.Slice(0, length - 1));
            if (compressed > 0)
            {
                payload.Length = compressed;
                payload.RawLength = length;
                return true;
            }
        }

        source.CopyTo(target);
        payload.Length = length;
        return true;
    }

    private static bool TryCompressFrom(PeerCore core, ReadOnlySpan<byte> source, ChannelDefinition channel, ref PreparedPayload payload)
    {
        int length = source.Length;
        if (!ShouldCompress(channel, length) || !core.TryRentSend(length - 1, out BufferLease lease))
        {
            return false;
        }

        int compressed = Lz4Block.Compress(source, core.GetSpan(in lease).Slice(0, length - 1));
        if (compressed <= 0)
        {
            core.ReturnSend(in lease);
            return false;
        }

        payload.Lease = lease;
        payload.Rented = true;
        payload.Pointer = core.GetPointer(in lease);
        payload.Length = compressed;
        payload.RawLength = length;
        return true;
    }

    private static bool TryGatherIn(PeerCore core, ReadOnlySpan<BufferLease> pages, int length, ChannelDefinition channel, ref PreparedPayload payload)
    {
        if (length == 0)
        {
            return true;
        }

        if (!core.TryRentSend(length, out BufferLease lease))
        {
            return false;
        }

        Span<byte> target = core.GetSpan(in lease);
        int offset = 0;
        foreach (BufferLease page in pages)
        {
            if (!page.IsEmpty)
            {
                core.GetSpan(in page).Slice(0, page.Length).CopyTo(target.Slice(offset));
                offset += page.Length;
            }
        }

        payload.Length = length;
        if (ShouldCompress(channel, length) && core.TryRentSend(length - 1, out BufferLease packed))
        {
            int compressed = Lz4Block.Compress(target.Slice(0, length), core.GetSpan(in packed).Slice(0, length - 1));
            if (compressed > 0)
            {
                core.ReturnSend(in lease);
                lease = packed;
                payload.Length = compressed;
                payload.RawLength = length;
            }
            else
            {
                core.ReturnSend(in packed);
            }
        }

        payload.Lease = lease;
        payload.Rented = true;
        payload.Pointer = core.GetPointer(in lease);
        return true;
    }
}
