using System.Text;

namespace Tedd.Quicly.Core.Channels;

/// <summary>
/// Collects channel declarations and validates them into a <see cref="ChannelTable"/>. Not thread-safe; build once
/// at startup. Every rule violation is reported by <see cref="Build"/> as an <see cref="ArgumentException"/> naming
/// the channel and the rule.
/// </summary>
public sealed class ChannelTableBuilder
{
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly List<Declaration> _declarations = new();

    internal ChannelTableBuilder()
    {
    }

    /// <summary>Declares a channel.</summary>
    /// <param name="id">Channel id, 2…16383 (ids ≤ 63 cost one header byte, larger ids two).</param>
    /// <param name="name">Diagnostic name, at most 64 UTF-8 bytes.</param>
    /// <param name="mode">Delivery mode.</param>
    /// <param name="configure">Optional callback that sets non-default options; invoked immediately.</param>
    /// <returns>This builder.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public ChannelTableBuilder Add(int id, string name, ChannelMode mode, Action<ChannelOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(name);
        ChannelOptions options = new();
        configure?.Invoke(options);
        _declarations.Add(new Declaration(id, name, mode, options));
        return this;
    }

    /// <summary>Validates every declaration and creates the table.</summary>
    /// <returns>The immutable table.</returns>
    /// <exception cref="ArgumentException">A declaration violates a rule of PROTOCOL.md §1 or the session-layer design.</exception>
    public ChannelTable Build()
    {
        ChannelDefinition[] definitions = new ChannelDefinition[_declarations.Count];
        for (int i = 0; i < definitions.Length; i++)
        {
            Declaration d = _declarations[i];
            definitions[i] = Resolve(d.Id, d.Name, d.Mode, d.Options);
        }

        Array.Sort(definitions, static (a, b) => a.Id.CompareTo(b.Id));
        for (int i = 1; i < definitions.Length; i++)
        {
            if (definitions[i].Id == definitions[i - 1].Id)
            {
                throw new ArgumentException(
                    $"Channel id {definitions[i].Id} is declared more than once ('{definitions[i - 1].Name}' and '{definitions[i].Name}').");
            }
        }

        return new ChannelTable(definitions);
    }

    private static ChannelDefinition Resolve(int id, string name, ChannelMode mode, ChannelOptions o)
    {
        string who = $"Channel {id} ('{name}')";
        if (id < ChannelDefinition.MinId || id > ChannelDefinition.MaxId)
        {
            throw Error(who,$"id must be in {ChannelDefinition.MinId}..{ChannelDefinition.MaxId} (0 is the control channel, 1 the packed container).");
        }

        if ((byte)mode > (byte)ChannelMode.Bulk)
        {
            throw Error(who,$"mode {(byte)mode} is not a defined ChannelMode.");
        }

        byte[] nameUtf8;
        try
        {
            nameUtf8 = StrictUtf8.GetBytes(name);
        }
        catch (EncoderFallbackException)
        {
            throw Error(who, "name is not valid Unicode (unpaired surrogate).");
        }

        if (nameUtf8.Length > ChannelDefinition.MaxNameBytes)
        {
            throw Error(who,$"name is {nameUtf8.Length} UTF-8 bytes; the maximum is {ChannelDefinition.MaxNameBytes}.");
        }

        bool latest = mode == ChannelMode.ReliableLatest;
        bool unreliable = mode is ChannelMode.UnreliableUnordered or ChannelMode.UnreliableSequenced;

        bool keyed = o.Keyed ?? latest;
        if (latest && !keyed)
        {
            throw Error(who,"ReliableLatest channels must be keyed.");
        }

        byte sequenceBits = o.SequenceBits ?? (byte)(keyed ? 32 : 16);
        if (sequenceBits is not (16 or 32))
        {
            throw Error(who,$"SequenceBits must be 16 or 32 (was {sequenceBits}).");
        }

        if (latest && sequenceBits != 32)
        {
            throw Error(who,"ReliableLatest channels must use 32-bit sequences.");
        }

        bool coalesce = o.CoalesceOnReceive ?? latest;
        if (latest && !coalesce)
        {
            throw Error(who,"ReliableLatest channels always coalesce on receive; CoalesceOnReceive cannot be turned off.");
        }

        if (coalesce && !keyed)
        {
            throw Error(who,"CoalesceOnReceive requires a keyed channel.");
        }

        if (o.RequestResponse && mode != ChannelMode.ReliableOrdered)
        {
            throw Error(who,$"RequestResponse requires mode ReliableOrdered (was {mode}).");
        }

        if (o.Fragmentation && !unreliable)
        {
            throw Error(who,$"Fragmentation requires mode UnreliableUnordered or UnreliableSequenced (was {mode}).");
        }

        if ((byte)o.Compression > (byte)ChannelCompression.Lz4)
        {
            throw Error(who,$"compression codec {(byte)o.Compression} is reserved.");
        }

        int maxMessageSizeLimit;
        int defaultMaxMessageSize;
        string limitReason;
        if (unreliable)
        {
            maxMessageSizeLimit = o.Fragmentation ? ChannelDefinition.FragmentedMaxMessageSize : ChannelDefinition.UnreliableMaxMessageSize;
            defaultMaxMessageSize = ChannelDefinition.UnreliableMaxMessageSize;
            limitReason = o.Fragmentation ? "a fragmenting channel (8 fragments x 1 100)" : "an unreliable channel without fragmentation";
        }
        else if (mode == ChannelMode.Bulk)
        {
            maxMessageSizeLimit = ChannelDefinition.BulkMaxMessageSize;
            defaultMaxMessageSize = ChannelDefinition.BulkMaxMessageSize;
            limitReason = "a Bulk channel";
        }
        else
        {
            maxMessageSizeLimit = ChannelDefinition.ReliableMaxMessageSize;
            defaultMaxMessageSize = ChannelDefinition.ReliableDefaultMaxMessageSize;
            limitReason = $"a {mode} channel";
        }

        int maxMessageSize = o.MaxMessageSize ?? defaultMaxMessageSize;
        if (maxMessageSize < 1 || maxMessageSize > maxMessageSizeLimit)
        {
            throw Error(who,$"MaxMessageSize must be in 1..{maxMessageSizeLimit} for {limitReason} (was {maxMessageSize}).");
        }

        int queueLimitBytes = o.QueueLimitBytes ?? 0;
        if (queueLimitBytes < 0)
        {
            throw Error(who,$"QueueLimitBytes must not be negative (was {queueLimitBytes}).");
        }

        long expiryMicros = o.ExpiryMicros ?? (mode == ChannelMode.UnreliableSequenced ? ChannelDefinition.ExpiryTwiceFlushInterval : 0);
        if (expiryMicros < ChannelDefinition.ExpiryTwiceFlushInterval)
        {
            throw Error(who,$"ExpiryMicros must be 0 (never), a positive duration or ExpiryTwiceFlushInterval (was {expiryMicros}).");
        }

        KeySpace keySpace = o.KeySpace;
        if (keySpace.IsDense && !keyed)
        {
            throw Error(who,"a dense KeySpace requires a keyed channel.");
        }

        const int MaxKeysLimit = 1 << 24;
        int maxKeys = o.MaxKeys ?? (keySpace.IsDense ? keySpace.MaxKey + 1 : 4096);
        if (maxKeys < 1 || maxKeys > MaxKeysLimit)
        {
            throw Error(who,$"MaxKeys must be in 1..{MaxKeysLimit} (was {maxKeys}).");
        }

        if (keySpace.IsDense && keySpace.MaxKey >= maxKeys)
        {
            throw Error(who,$"KeySpace {keySpace} needs MaxKeys >= {(long)keySpace.MaxKey + 1} (was {maxKeys}).");
        }

        int maxReassemblies = o.MaxReassemblies ?? 16;
        if (maxReassemblies < 1 || maxReassemblies > 1024)
        {
            throw Error(who,$"MaxReassemblies must be in 1..1024 (was {maxReassemblies}).");
        }

        int defaultGroups = mode switch
        {
            ChannelMode.ReliableUnordered => 8,
            ChannelMode.ReliableLatest => 4,
            ChannelMode.Bulk => 2,
            ChannelMode.ReliableOrdered => 1,
            _ => 0,
        };
        int maxGroups = o.MaxGroups ?? defaultGroups;
        int minGroups = defaultGroups == 0 ? 0 : 1;
        if (maxGroups < minGroups || maxGroups > 1024)
        {
            throw Error(who,$"MaxGroups must be in {minGroups}..1024 for a {mode} channel (was {maxGroups}).");
        }

        int groupMaxBytes = o.GroupMaxBytes ?? 64 * 1024;
        if (groupMaxBytes < 1 || groupMaxBytes > ChannelDefinition.BulkMaxMessageSize)
        {
            throw Error(who,$"GroupMaxBytes must be in 1..{ChannelDefinition.BulkMaxMessageSize} (was {groupMaxBytes}).");
        }

        int minCompressSize = o.MinCompressSize ?? 64;
        if (minCompressSize < 0)
        {
            throw Error(who,$"MinCompressSize must not be negative (was {minCompressSize}).");
        }

        return new ChannelDefinition(
            (ushort)id, name, nameUtf8, mode, keyed, sequenceBits, o.Fragmentation, o.Compression, o.RequestResponse, coalesce,
            o.Priority, maxMessageSize, queueLimitBytes, expiryMicros, maxKeys, maxReassemblies, maxGroups, groupMaxBytes,
            keySpace, minCompressSize);
    }

    private static ArgumentException Error(string who, string rule) => new($"{who}: {rule}");

    private readonly record struct Declaration(int Id, string Name, ChannelMode Mode, ChannelOptions Options);
}
