namespace Tedd.Quicly.Core.Threading;

/// <summary>
/// Handle to a tracked send: a <see cref="CompletionTable"/> slot plus the generation the slot had when it
/// was allocated. A token whose generation no longer matches the slot's is <em>stale</em>: its send has
/// finished and the slot has been reused or is free.
/// </summary>
/// <param name="Slot">Index of the completion slot.</param>
/// <param name="Generation">Generation of the slot at allocation time. Never zero for a real token.</param>
public readonly record struct SendToken(int Slot, uint Generation)
{
    /// <summary>
    /// True when this token was produced by <see cref="CompletionTable.TryAllocate"/>; <see langword="default"/>
    /// tokens are invalid. Says nothing about whether the token is stale.
    /// </summary>
    public bool IsValid => Generation != 0 && Slot >= 0;
}

/// <summary>The two independently completed stages of a tracked send.</summary>
public enum CompletionStage : byte
{
    /// <summary>The transport no longer references the payload; the caller may reuse or free its memory.</summary>
    BufferReleased = 0,

    /// <summary>The remote library accepted the complete message (requires receipts or latest-acks).</summary>
    RemoteAccepted = 1,
}

/// <summary>Final outcome of a tracked send.</summary>
public enum DeliveryStatus : byte
{
    /// <summary>The outcome is not known yet.</summary>
    Pending = 0,

    /// <summary>The message reached the remote peer's transport (an acknowledgement, or a LatestAck for ReliableLatest).</summary>
    Delivered,

    /// <summary>A newer version of the same key replaced this message before it was sent.</summary>
    Superseded,

    /// <summary>The transport rejected or lost the message and will not retry.</summary>
    Failed,

    /// <summary>The send (or its slot) was canceled or released by the application.</summary>
    Canceled,

    /// <summary>
    /// The message was held back for longer than its expiry before it could be sent (the expiry counts from the message's
    /// first scheduler pass, PROTOCOL.md §4.5), or the transport dropped its datagram before transmission: it could not
    /// send the datagram at once (<see cref="Session.PeerOptions.DropWhenBlocked"/>), or the datagram no longer fitted after
    /// the path's datagram limit shrank. Statistics tell the two apart: <see cref="Session.ChannelStatistics.Expired"/>
    /// counts the first, <see cref="Session.ChannelStatistics.TransportCanceled"/> the second.
    /// </summary>
    Expired,

    /// <summary>The session ended before the message was delivered.</summary>
    Disconnected,

    /// <summary>The transport declared the datagram lost (unreliable sends whose transport reports datagram send state).</summary>
    Lost,

    /// <summary>
    /// The datagram was handed to the network and this carrier cannot report acknowledgement (PROTOCOL.md §4.3:
    /// <c>Delivered</c> requires per-datagram send state, <see cref="Transport.TransportCapabilities.DatagramSendState"/>).
    /// A terminal outcome: nothing further will be reported for the send, and it is <em>not</em> a delivery claim.
    /// </summary>
    Sent,
}
