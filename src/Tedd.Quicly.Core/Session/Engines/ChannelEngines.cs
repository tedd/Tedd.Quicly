using Tedd.Quicly.Core.Channels;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>
/// Per-mode engine registry: the one place that maps a <see cref="ChannelMode"/> to its engine class. Implementing a
/// mode means adding its engine file and changing its case here; the peer's files stay untouched.
/// </summary>
internal static class ChannelEngines
{
    /// <summary>Number of delivery modes (the size of <see cref="PeerCore.Engines"/>).</summary>
    public const int ModeCount = 6;

    /// <summary>Creates the engine of <paramref name="mode"/> for one peer.</summary>
    /// <param name="mode">The mode.</param>
    /// <returns>A new, uninitialised engine.</returns>
    public static ChannelEngine Create(ChannelMode mode) => mode switch
    {
        // Wave C1 (next step): UnreliableUnordered, UnreliableSequenced and ReliableOrdered engines.
        ChannelMode.UnreliableUnordered => new PlaceholderEngine(mode),
        ChannelMode.UnreliableSequenced => new PlaceholderEngine(mode),
        ChannelMode.ReliableOrdered => new PlaceholderEngine(mode),
        // Wave C2: GroupStreamEngine, ReliableLatestEngine, BulkEngine.
        ChannelMode.ReliableUnordered => new PlaceholderEngine(mode),
        ChannelMode.ReliableLatest => new PlaceholderEngine(mode),
        ChannelMode.Bulk => new PlaceholderEngine(mode),
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Undefined channel mode."),
    };
}
