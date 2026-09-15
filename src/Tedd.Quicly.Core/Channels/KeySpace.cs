namespace Tedd.Quicly.Core.Channels;

/// <summary>
/// How per-key state of a keyed channel is indexed: <see cref="Hashed"/> (any 62-bit key, open-addressing table
/// bounded by <see cref="ChannelDefinition.MaxKeys"/>) or <see cref="Dense(int)"/> (keys 0…max, direct index).
/// </summary>
/// <remarks>Local implementation choice only; not part of the wire format or the table hash.</remarks>
public readonly struct KeySpace
{
    private readonly int _maxKeyPlusOne;

    private KeySpace(int maxKeyPlusOne) => _maxKeyPlusOne = maxKeyPlusOne;

    /// <summary>Keys are hashed (the default).</summary>
    public static KeySpace Hashed => default;

    /// <summary>Keys are small integers 0…<paramref name="maxKey"/> used as a direct index.</summary>
    /// <param name="maxKey">The largest key the application will use; must be non-negative.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxKey"/> is negative or <see cref="int.MaxValue"/>.</exception>
    public static KeySpace Dense(int maxKey)
    {
        if (maxKey < 0 || maxKey == int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(maxKey), maxKey, "A dense key space needs a maximum key in 0..int.MaxValue-1.");
        }

        return new KeySpace(maxKey + 1);
    }

    /// <summary><see langword="true"/> for a <see cref="Dense(int)"/> key space.</summary>
    public bool IsDense => _maxKeyPlusOne != 0;

    /// <summary>The largest key of a dense key space, or -1 for <see cref="Hashed"/>.</summary>
    public int MaxKey => _maxKeyPlusOne - 1;

    /// <inheritdoc/>
    public override string ToString() => IsDense ? $"Dense({MaxKey})" : "Hashed";
}
