using System.Numerics;

namespace Tedd.Quicly.Replication;

/// <summary>Classification of an event id by <see cref="DedupWindow"/>.</summary>
public enum DedupStatus
{
    /// <summary>Not seen before (inside the window or ahead of it).</summary>
    New = 0,

    /// <summary>Already accepted.</summary>
    Duplicate = 1,

    /// <summary>Behind the window: can no longer be told apart from a duplicate, so it is rejected.</summary>
    TooOld = 2,
}

/// <summary>
/// Sliding-window replay filter over 64-bit event ids, in the style of the IPsec/DTLS anti-replay window
/// (RFC 4303 §3.4.3, RFC 6347 §4.1.2.6, with the block-clearing bitmap of RFC 6479).
/// </summary>
/// <remarks>
/// <para>
/// With window size W and highest accepted id H: an id greater than H is accepted and slides the window (an id W
/// or more ahead clears it entirely); an id in (H − W, H] is accepted once and then reported as a duplicate; an id
/// at or below H − W is rejected as <see cref="DedupStatus.TooOld"/>. The first id ever seen anchors the window.
/// </para>
/// <para>
/// Use it where the same event can arrive more than once — for example a game event sent redundantly on an
/// unreliable channel, or re-sent after a reconnect. The ids come from the peer, so a peer can slide the window
/// arbitrarily far ahead and make its own later events look too old; that only harms the sender (QUICLY traffic
/// is authenticated by TLS). O(1), allocation-free after construction; not thread-safe.
/// </para>
/// </remarks>
public sealed class DedupWindow
{
    /// <summary>Default window size in ids.</summary>
    public const int DefaultWindowBits = 1024;

    private readonly ulong[] _blocks;
    private readonly int _blockMask;
    private readonly ulong _window;
    private ulong _highest;
    private bool _started;

    /// <summary>Creates a window.</summary>
    /// <param name="windowBits">Window size in ids: a positive multiple of 64, at most 2^24.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="windowBits"/> is invalid.</exception>
    public DedupWindow(int windowBits = DefaultWindowBits)
    {
        if (windowBits < 64 || windowBits > (1 << 24) || (windowBits & 63) != 0)
        {
            throw new ArgumentOutOfRangeException(nameof(windowBits), windowBits, "Window size must be a positive multiple of 64, at most 2^24.");
        }

        // One block more than the window so that clearing whole blocks while sliding never erases an id still inside it.
        int blocks = (int)BitOperations.RoundUpToPowerOf2((uint)(windowBits / 64 + 1));
        _blocks = new ulong[blocks];
        _blockMask = blocks - 1;
        _window = (ulong)windowBits;
    }

    /// <summary>Window size in ids.</summary>
    public int WindowBits => (int)_window;

    /// <summary>Highest id accepted so far (meaningful when <see cref="HasAccepted"/>).</summary>
    public ulong Highest => _highest;

    /// <summary><see langword="true"/> once an id has been accepted.</summary>
    public bool HasAccepted => _started;

    /// <summary>Classifies <paramref name="id"/> without recording it.</summary>
    /// <param name="id">The event id.</param>
    /// <returns>What <see cref="Accept"/> would report.</returns>
    public DedupStatus Check(ulong id)
    {
        if (!_started || id > _highest)
        {
            return DedupStatus.New;
        }

        if (_highest - id >= _window)
        {
            return DedupStatus.TooOld;
        }

        return (_blocks[(int)(id >> 6) & _blockMask] & (1UL << (int)(id & 63))) != 0 ? DedupStatus.Duplicate : DedupStatus.New;
    }

    /// <summary>Records <paramref name="id"/> if it is new.</summary>
    /// <param name="id">The event id.</param>
    /// <returns><see langword="true"/> exactly once per id inside the window; <see langword="false"/> for duplicates and ids that are too old.</returns>
    public bool TryAccept(ulong id) => Accept(id) == DedupStatus.New;

    /// <summary>Classifies <paramref name="id"/> and records it when it is new (sliding the window if it is ahead).</summary>
    /// <param name="id">The event id.</param>
    /// <returns>The classification; only <see cref="DedupStatus.New"/> changes state.</returns>
    public DedupStatus Accept(ulong id)
    {
        if (!_started)
        {
            _started = true;
            _highest = id;
        }
        else if (id > _highest)
        {
            ulong currentBlock = _highest >> 6;
            ulong blocksAhead = (id >> 6) - currentBlock;
            if (blocksAhead >= (ulong)_blocks.Length)
            {
                Array.Clear(_blocks);
            }
            else
            {
                for (ulong k = 1; k <= blocksAhead; k++)
                {
                    _blocks[(int)(currentBlock + k) & _blockMask] = 0;
                }
            }

            _highest = id;
        }
        else if (_highest - id >= _window)
        {
            return DedupStatus.TooOld;
        }

        ref ulong block = ref _blocks[(int)(id >> 6) & _blockMask];
        ulong bit = 1UL << (int)(id & 63);
        if ((block & bit) != 0)
        {
            return DedupStatus.Duplicate;
        }

        block |= bit;
        return DedupStatus.New;
    }

    /// <summary>Forgets every id; the next id anchors the window again.</summary>
    public void Reset()
    {
        Array.Clear(_blocks);
        _started = false;
        _highest = 0;
    }
}
