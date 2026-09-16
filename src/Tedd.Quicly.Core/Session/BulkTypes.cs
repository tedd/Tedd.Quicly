namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Describes an object to send on a Bulk channel (<see cref="QuiclyPeer.BeginBulkSendAsync"/>). Placeholder shape:
/// the bulk engine (wave C2) may extend it.
/// </summary>
/// <param name="Channel">The Bulk channel.</param>
/// <param name="ObjectId">Object identity.</param>
/// <param name="ObjectVersion">Object version (identity is id + version).</param>
/// <param name="TotalLength">Total object length in bytes.</param>
/// <param name="Resumable">Re-request the remaining range after a reconnect.</param>
public readonly record struct BulkDescriptor(ushort Channel, ulong ObjectId, ulong ObjectVersion, long TotalLength, bool Resumable = false);

/// <summary>Supplies the bytes of a bulk object (placeholder contract; wave C2 defines the final shape).</summary>
public interface IBulkSource
{
    /// <summary>Copies object bytes starting at <paramref name="offset"/> into <paramref name="destination"/>.</summary>
    /// <param name="offset">Object offset.</param>
    /// <param name="destination">Receives the bytes.</param>
    /// <returns>Bytes copied (0 at the end of the object).</returns>
    int Read(long offset, Span<byte> destination);
}

/// <summary>A running bulk transfer (placeholder; wave C2 adds progress, cancel and completion).</summary>
public sealed class BulkTransfer
{
    internal BulkTransfer(in BulkDescriptor descriptor) => Descriptor = descriptor;

    /// <summary>What is being transferred.</summary>
    public BulkDescriptor Descriptor { get; }
}

/// <summary>
/// Asks the peer to send a range of a bulk object (<see cref="QuiclyPeer.RequestBulk"/>, control message BulkRequest).
/// Named to avoid a clash with <see cref="Control.BulkRequest"/>, the wire message.
/// </summary>
/// <param name="Channel">The Bulk channel.</param>
/// <param name="ObjectId">Object identity.</param>
/// <param name="ObjectVersion">Object version.</param>
/// <param name="Offset">First byte of the range.</param>
/// <param name="Length">Length of the range.</param>
public readonly record struct BulkRangeRequest(ushort Channel, ulong ObjectId, ulong ObjectVersion, long Offset, long Length);
