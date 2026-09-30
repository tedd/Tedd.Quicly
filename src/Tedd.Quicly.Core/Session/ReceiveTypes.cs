using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Describes one received message (ARCHITECTURE.md §4.2). Passed by reference to a <see cref="MessageHandler"/>, or
/// carried by a <see cref="ReceivedMessage"/> / <see cref="ReceiveLease"/>.
/// </summary>
public readonly struct ReceiveHeader
{
    /// <summary>Channel id.</summary>
    public ushort Channel { get; init; }

    /// <summary>Key on keyed channels, otherwise 0.</summary>
    public ulong Key { get; init; }

    /// <summary>Sequence (sequenced channels) or version (ReliableLatest) as received; 0 when the channel has none.</summary>
    public uint Sequence { get; init; }

    /// <summary>Payload bytes handed to the application (decoded size when the message arrived compressed).</summary>
    public int Length { get; init; }

    /// <summary>The wire <c>RawLength</c>: the decoded size when the message arrived compressed, 0 otherwise.</summary>
    public int RawLength { get; init; }

    /// <summary>Local clock (<see cref="PeerOptions.Clock"/>) microseconds when the message was completely received.</summary>
    public long ReceivedMicros { get; init; }

    /// <summary>The sender's flush tick from the packed container header, or 0.</summary>
    public uint SenderTick { get; init; }

    /// <summary>The session epoch the message belongs to.</summary>
    public uint Epoch { get; init; }

    /// <summary><see cref="QuiclyPeer.Index"/> of the receiving peer (lets one handler serve many peers).</summary>
    public int PeerIndex { get; init; }

    /// <summary>Request id on request/response channels (odd = request, even = response to <c>RequestId − 1</c>), else 0.</summary>
    public uint RequestId { get; init; }

    /// <summary>Flags (fragmented, compressed, superseded, request, response, key retired).</summary>
    public ReceiveFlags Flags { get; init; }
}

/// <summary>
/// Receives the messages of one channel inside <see cref="QuiclyPeer.Poll"/> on the game thread. The payload is valid
/// only during the call unless it is retained with <see cref="QuiclyPeer.Retain"/>.
/// </summary>
/// <param name="peer">The peer the message arrived on.</param>
/// <param name="header">The message header (pass it to <see cref="QuiclyPeer.Retain"/> to keep the payload).</param>
/// <param name="payload">The payload.</param>
public delegate void MessageHandler(QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> payload);

/// <summary>
/// A message taken with <see cref="QuiclyPeer.Drain"/>. Its payload stays valid until the message is passed to
/// <see cref="QuiclyPeer.Release(ReadOnlySpan{ReceivedMessage})"/>; every drained message must be released exactly once.
/// On a peer with its own private pool the payload also ends when the peer's memory is freed after
/// <see cref="QuiclyPeer.Dispose"/>; on a peer over a shared pool it must be released even after the peer was disposed.
/// </summary>
public readonly unsafe struct ReceivedMessage
{
    internal readonly BufferLease Lease;
    internal readonly byte* Data;

    internal ReceivedMessage(in ReceiveHeader header, in BufferLease lease, byte* data)
    {
        Header = header;
        Lease = lease;
        Data = data;
    }

    /// <summary>The message header.</summary>
    public ReceiveHeader Header { get; }

    /// <summary>The payload (<see cref="ReceiveHeader.Length"/> bytes). Valid until the message is released.</summary>
    public ReadOnlySpan<byte> Payload => Data is null ? default : new ReadOnlySpan<byte>(Data, Header.Length);
}

/// <summary>
/// A payload kept beyond its handler (<see cref="QuiclyPeer.Retain"/>). Valid until
/// <see cref="QuiclyPeer.Release(in ReceiveLease)"/>; must be released exactly once, on the game thread.
/// On a peer with its own private pool the payload also ends when the peer's memory is freed after
/// <see cref="QuiclyPeer.Dispose"/>; on a peer over a shared pool (<see cref="PeerOptions.Allocator"/>, every peer of a
/// server) it stays valid and must be released even after the peer was disposed, or the shared pool loses the block.
/// </summary>
public readonly unsafe struct ReceiveLease
{
    internal readonly BufferLease Lease;
    internal readonly byte* Data;

    internal ReceiveLease(in ReceiveHeader header, in BufferLease lease, byte* data)
    {
        Header = header;
        Lease = lease;
        Data = data;
    }

    /// <summary>The message header.</summary>
    public ReceiveHeader Header { get; }

    /// <summary>True for a lease produced by the library (the default value is invalid).</summary>
    public bool IsValid => Data is not null;

    /// <summary>The payload (<see cref="ReceiveHeader.Length"/> bytes). Valid until the lease is released.</summary>
    public ReadOnlySpan<byte> Payload => Data is null ? default : new ReadOnlySpan<byte>(Data, Header.Length);
}
