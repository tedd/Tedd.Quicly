using System.Numerics;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server;

/// <summary>
/// A set of peers as a bitset over peer indices (<see cref="QuiclyPeer.Index"/>, the slot in <see cref="QuiclyServer.Peers"/>).
/// Add, remove, contains and iteration never allocate, so sets can be rebuilt every tick; <see cref="QuiclyServer.SendShared"/>
/// sends to one.
/// </summary>
/// <remarks>
/// A set holds indices, not peers, and a slot is reused after its peer closed. Sets created by
/// <see cref="QuiclyServer.CreateSet"/> lose an index when its peer is released (right after
/// <see cref="QuiclyServer.PeerClosed"/>); sets created with the public constructor do not. Iterate with <c>foreach</c>
/// (a struct enumerator); do not modify a set while iterating it. Not thread-safe: use it on the game thread.
/// </remarks>
public sealed class PeerSet
{
    /// <summary>Largest capacity.</summary>
    public const int MaxCapacity = 1 << 22;

    private readonly ulong[] _words;
    private int _count;
    private bool _readOnly;

    /// <summary>Creates an empty set for indices 0 … <paramref name="capacity"/> − 1.</summary>
    /// <param name="capacity">Number of indices (a server's <see cref="QuiclyServer.Capacity"/>).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative or larger than <see cref="MaxCapacity"/>.</exception>
    public PeerSet(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(capacity, MaxCapacity);
        Capacity = capacity;
        _words = new ulong[(capacity + 63) >> 6];
    }

    /// <summary>Number of indices the set can hold.</summary>
    public int Capacity { get; }

    /// <summary>Number of indices in the set.</summary>
    public int Count => _count;

    /// <summary>True when the set is empty.</summary>
    public bool IsEmpty => _count == 0;

    /// <summary>True for sets maintained by the server (<see cref="QuiclyServer.AdmittedPeers"/>, <see cref="SharedSendResult"/> masks); mutating them throws.</summary>
    public bool IsReadOnly => _readOnly;

    /// <summary>The raw bits, 64 indices per word (index <c>i</c> is bit <c>i % 64</c> of word <c>i / 64</c>), for vectorised consumers.</summary>
    public ReadOnlySpan<ulong> Words => _words;

    /// <summary>Whether <paramref name="index"/> is in the set (false for indices outside the capacity).</summary>
    /// <param name="index">A peer index.</param>
    /// <returns><see langword="true"/> when the index is in the set.</returns>
    public bool Contains(int index) => (uint)index < (uint)Capacity && (_words[index >> 6] & (1UL << (index & 63))) != 0;

    /// <summary>Whether the index of <paramref name="peer"/> is in the set.</summary>
    /// <param name="peer">A peer of the server.</param>
    /// <returns><see langword="true"/> when the peer's index is in the set.</returns>
    public bool Contains(QuiclyPeer peer)
    {
        ArgumentNullException.ThrowIfNull(peer);
        return Contains(peer.Index);
    }

    /// <summary>Adds an index.</summary>
    /// <param name="index">A peer index in [0, <see cref="Capacity"/>).</param>
    /// <returns><see langword="true"/> when the index was not in the set.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The index is outside the capacity.</exception>
    /// <exception cref="InvalidOperationException">The set is read-only.</exception>
    public bool Add(int index)
    {
        ThrowIfReadOnly();
        return AddCore(index);
    }

    /// <summary>Adds the index of <paramref name="peer"/>.</summary>
    /// <param name="peer">A peer of the server.</param>
    /// <returns><see langword="true"/> when the index was not in the set.</returns>
    public bool Add(QuiclyPeer peer)
    {
        ArgumentNullException.ThrowIfNull(peer);
        return Add(peer.Index);
    }

    /// <summary>Removes an index.</summary>
    /// <param name="index">A peer index.</param>
    /// <returns><see langword="true"/> when the index was in the set.</returns>
    /// <exception cref="InvalidOperationException">The set is read-only.</exception>
    public bool Remove(int index)
    {
        ThrowIfReadOnly();
        return RemoveCore(index);
    }

    /// <summary>Removes the index of <paramref name="peer"/>.</summary>
    /// <param name="peer">A peer of the server.</param>
    /// <returns><see langword="true"/> when the index was in the set.</returns>
    public bool Remove(QuiclyPeer peer)
    {
        ArgumentNullException.ThrowIfNull(peer);
        return Remove(peer.Index);
    }

    /// <summary>Removes every index.</summary>
    /// <exception cref="InvalidOperationException">The set is read-only.</exception>
    public void Clear()
    {
        ThrowIfReadOnly();
        ClearCore();
    }

    /// <summary>Makes this set equal to <paramref name="other"/>.</summary>
    /// <param name="other">A set whose capacity is at most this set's.</param>
    /// <exception cref="ArgumentException"><paramref name="other"/> is larger than this set.</exception>
    public void CopyFrom(PeerSet other)
    {
        ThrowIfReadOnly();
        CheckOther(other);
        other._words.AsSpan().CopyTo(_words);
        _words.AsSpan(other._words.Length).Clear();
        _count = other._count;
    }

    /// <summary>Adds every index of <paramref name="other"/>.</summary>
    /// <param name="other">A set whose capacity is at most this set's.</param>
    /// <exception cref="ArgumentException"><paramref name="other"/> is larger than this set.</exception>
    public void UnionWith(PeerSet other)
    {
        ThrowIfReadOnly();
        CheckOther(other);
        ulong[] source = other._words;
        for (int i = 0; i < source.Length; i++)
        {
            _words[i] |= source[i];
        }

        Recount();
    }

    /// <summary>Keeps only the indices that are also in <paramref name="other"/>.</summary>
    /// <param name="other">A set whose capacity is at most this set's.</param>
    /// <exception cref="ArgumentException"><paramref name="other"/> is larger than this set.</exception>
    public void IntersectWith(PeerSet other)
    {
        ThrowIfReadOnly();
        CheckOther(other);
        ulong[] source = other._words;
        for (int i = 0; i < source.Length; i++)
        {
            _words[i] &= source[i];
        }

        _words.AsSpan(source.Length).Clear();
        Recount();
    }

    /// <summary>Removes every index of <paramref name="other"/>.</summary>
    /// <param name="other">A set whose capacity is at most this set's.</param>
    /// <exception cref="ArgumentException"><paramref name="other"/> is larger than this set.</exception>
    public void ExceptWith(PeerSet other)
    {
        ThrowIfReadOnly();
        CheckOther(other);
        ulong[] source = other._words;
        for (int i = 0; i < source.Length; i++)
        {
            _words[i] &= ~source[i];
        }

        Recount();
    }

    /// <summary>Enumerates the indices in ascending order without allocating.</summary>
    /// <returns>A struct enumerator.</returns>
    public Enumerator GetEnumerator() => new(_words);

    internal void MakeReadOnly() => _readOnly = true;

    internal bool AddCore(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, Capacity);
        ref ulong word = ref _words[index >> 6];
        ulong bit = 1UL << (index & 63);
        if ((word & bit) != 0)
        {
            return false;
        }

        word |= bit;
        _count++;
        return true;
    }

    internal bool RemoveCore(int index)
    {
        if ((uint)index >= (uint)Capacity)
        {
            return false;
        }

        ref ulong word = ref _words[index >> 6];
        ulong bit = 1UL << (index & 63);
        if ((word & bit) == 0)
        {
            return false;
        }

        word &= ~bit;
        _count--;
        return true;
    }

    internal void ClearCore()
    {
        if (_count != 0)
        {
            Array.Clear(_words);
            _count = 0;
        }
    }

    private void CheckOther(PeerSet other)
    {
        ArgumentNullException.ThrowIfNull(other);
        if (other.Capacity > Capacity)
        {
            throw new ArgumentException("The other set has a larger capacity than this one.", nameof(other));
        }
    }

    private void ThrowIfReadOnly()
    {
        if (_readOnly)
        {
            throw new InvalidOperationException("This peer set is maintained by the server and is read-only.");
        }
    }

    private void Recount()
    {
        int count = 0;
        foreach (ulong word in _words)
        {
            count += BitOperations.PopCount(word);
        }

        _count = count;
    }

    /// <summary>Enumerates the indices of a <see cref="PeerSet"/> in ascending order.</summary>
    public struct Enumerator
    {
        private readonly ulong[] _words;
        private int _word;
        private ulong _bits;
        private int _current;

        internal Enumerator(ulong[] words)
        {
            _words = words;
            _word = -1;
            _bits = 0;
            _current = -1;
        }

        /// <summary>The current index.</summary>
        public readonly int Current => _current;

        /// <summary>Moves to the next index.</summary>
        /// <returns><see langword="false"/> after the last index.</returns>
        public bool MoveNext()
        {
            while (_bits == 0)
            {
                if (_word + 1 >= _words.Length)
                {
                    _word = _words.Length;
                    _current = -1;
                    return false;
                }

                _bits = _words[++_word];
            }

            _current = (_word << 6) + BitOperations.TrailingZeroCount(_bits);
            _bits &= _bits - 1;
            return true;
        }
    }
}
