namespace Tedd.Quicly.Core.Control;

/// <summary>
/// <see cref="Hello.Flags"/> bits (PROTOCOL.md §3.4). Unknown bits are preserved by the codec and ignored by
/// version-1 endpoints.
/// </summary>
[Flags]
public enum HelloFlags : ushort
{
    /// <summary>No flags.</summary>
    None = 0,

    /// <summary>Ask the server to include its channel table (with names) in the HelloAck.</summary>
    RequestChannelTable = 1,
}
