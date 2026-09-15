using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.Channels;

/// <summary>
/// The immutable set of application channels both peers agree on (PROTOCOL.md §1). Lookup by id is a single
/// bounds-checked array load; <see cref="Hash"/> is the <c>TableHash</c> exchanged in the handshake.
/// </summary>
public sealed class ChannelTable
{
    private readonly ChannelDefinition?[] _byId;
    private readonly ChannelDefinition[] _all;

    internal ChannelTable(ChannelDefinition[] sortedById)
    {
        _all = sortedById;
        int maxId = sortedById.Length == 0 ? 0 : sortedById[^1].Id;
        _byId = new ChannelDefinition?[maxId + 1];
        foreach (ChannelDefinition definition in sortedById)
        {
            _byId[definition.Id] = definition;
        }

        Hash = ChannelTableCodec.ComputeHash(this);
    }

    /// <summary>Starts building a table.</summary>
    public static ChannelTableBuilder Create() => new();

    /// <summary>Number of application channels.</summary>
    public int Count => _all.Length;

    /// <summary>Largest channel id in the table, or 0 when the table is empty.</summary>
    public int MaxId => _byId.Length - 1;

    /// <summary><c>TableHash</c>: XXH64 (seed 0) of the canonical encoding (<see cref="ChannelTableCodec.WriteCanonical"/>).</summary>
    public ulong Hash { get; }

    /// <summary>Every channel, in ascending id order.</summary>
    public ReadOnlySpan<ChannelDefinition> All => _all;

    /// <summary>Returns the channel with id <paramref name="id"/>, or <see langword="null"/> when there is none (any int is accepted).</summary>
    /// <param name="id">The channel id.</param>
    public ChannelDefinition? this[int id]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            ChannelDefinition?[] byId = _byId;
            return (uint)id < (uint)byId.Length ? byId[id] : null;
        }
    }

    /// <summary>Looks up the channel with id <paramref name="id"/>.</summary>
    /// <param name="id">The channel id.</param>
    /// <param name="definition">The channel, or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the table has the channel.</returns>
    public bool TryGet(int id, [NotNullWhen(true)] out ChannelDefinition? definition)
    {
        definition = this[id];
        return definition is not null;
    }
}
