namespace Tedd.Quicly.Core.State;

/// <summary>
/// Maps 64-bit message keys to dense slot indices in <c>[0, MaxKeys)</c>. A slot is stable while its key lives and
/// is recycled after <see cref="Remove"/>. Implemented by <see cref="KeyTable"/> (hashed) and
/// <see cref="DenseKeyTable"/> (identity). Call sites hold the concrete type; the interface only fixes the surface.
/// </summary>
public interface IKeyTable
{
    /// <summary>Number of live keys.</summary>
    int Count { get; }

    /// <summary>Upper bound on <see cref="Count"/>; <see cref="TryAdd"/> fails once it is reached.</summary>
    int MaxKeys { get; }

    /// <summary>Finds the slot of <paramref name="key"/>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="slot">Its slot, or -1.</param>
    /// <returns><see langword="false"/> when the key is not present.</returns>
    bool TryGetSlot(ulong key, out int slot);

    /// <summary>Adds <paramref name="key"/> and allocates a slot for it.</summary>
    /// <param name="key">The key.</param>
    /// <param name="slot">The new slot; the existing slot when the key was already present; -1 when the table is full or the key is invalid.</param>
    /// <returns><see langword="true"/> only when the key was added by this call.</returns>
    bool TryAdd(ulong key, out int slot);

    /// <summary>Finds the slot of <paramref name="key"/>, adding the key when absent.</summary>
    /// <param name="key">The key.</param>
    /// <param name="slot">The slot, or -1 when the key is absent and cannot be added.</param>
    /// <returns><see langword="false"/> when the key is absent and the table is full (or the key is invalid).</returns>
    bool TryGetOrAdd(ulong key, out int slot);

    /// <summary>Removes <paramref name="key"/> and recycles its slot.</summary>
    /// <param name="key">The key.</param>
    /// <param name="slot">The slot the key had, or -1.</param>
    /// <returns><see langword="false"/> when the key was not present.</returns>
    bool Remove(ulong key, out int slot);

    /// <summary>Removes every key.</summary>
    void Clear();
}
