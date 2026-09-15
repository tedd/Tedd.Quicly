using System.Numerics;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.State;

/// <summary>
/// Latest-wins hand-off of one lease index per key slot from the transport thread to the game thread, for keyed
/// channels with <c>CoalesceOnReceive</c> (ADR 0008 invariant 6): an <see cref="int"/> mailbox per key slot
/// (-1 = empty) plus a dirty bitset that tells the game thread which keys changed. Bounded memory, no
/// back-pressure, no ring entries.
/// </summary>
/// <remarks>
/// <para><b>Transport thread:</b> <see cref="Exchange"/> the new lease index into the key's mailbox, then
/// <see cref="SetDirty"/> the key (or <see cref="Post"/>, which does both). A non-negative previous value is a
/// lease the game thread never saw; the transport thread frees it immediately.</para>
/// <para><b>Game thread:</b> <see cref="PopDirty"/> to collect dirty keys (their bits are cleared inside the call,
/// before it returns), then <see cref="Take"/> each key's mailbox. <see cref="Take"/> can return -1 for a key
/// <see cref="PopDirty"/> just reported: the producer may have posted a newer value between an earlier
/// <see cref="PopDirty"/> and its <see cref="Take"/>, which then claimed the newer value and left a dirty bit
/// with nothing behind it. Callers skip such keys.</para>
/// <para><b>Memory ordering.</b> Every operation on a mailbox word and on a dirty word is an interlocked
/// read-modify-write (full fence, sequentially consistent across all such operations) or a volatile read. For a
/// producer that performs <c>E</c> = <see cref="Exchange"/>(value) then <c>S</c> = <see cref="SetDirty"/>, and a
/// consumer that performs <c>C</c> = clear-bit (in <see cref="PopDirty"/>) then <c>T</c> = <see cref="Take"/>:
/// (1) if the consumer observed the bit set by <c>S</c>, the release in <c>S</c> and the acquire in the
/// consumer's read order <c>E</c> before <c>T</c>, so <c>T</c> returns <c>value</c> or something newer, never an
/// older value; (2) if instead <c>E</c> is ordered after <c>T</c> in the mailbox's modification order, then
/// <c>S</c> (after <c>E</c>) is after <c>C</c> (before <c>T</c>) on the dirty word, so the bit is set when
/// <c>C</c> has already run and the next <see cref="PopDirty"/> reports the key. Either way no posted value is
/// left in a mailbox without a dirty bit, and every lease index posted is returned exactly once: by
/// <see cref="Exchange"/> to the producer (as the displaced previous value) or by <see cref="Take"/> to the
/// consumer. The order clear-then-take is what makes (2) hold; take-then-clear would lose a post that lands
/// between the two.</para>
/// <para>Any value written by the producer before its <see cref="Exchange"/> (for example the bytes of the
/// lease) is visible to the consumer after the <see cref="Take"/> that returns that index, by the same
/// release/acquire pairing.</para>
/// </remarks>
public sealed unsafe class Mailboxes : IDisposable
{
    private readonly NativeArray<int> _mailbox;
    private readonly NativeArray<ulong> _dirty;
    private bool _disposed;

    /// <summary>Creates mailboxes for <paramref name="keySlots"/> key slots, all empty and clean.</summary>
    /// <param name="keySlots">Number of key slots (0 … 2^30).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="keySlots"/> is outside its range.</exception>
    public Mailboxes(int keySlots)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(keySlots);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(keySlots, 1 << 30);
        _mailbox = new NativeArray<int>(keySlots);
        _mailbox.Fill(-1);
        _dirty = new NativeArray<ulong>((keySlots + 63) >> 6);
    }

    /// <summary>Number of key slots.</summary>
    public int Capacity => _mailbox.Length;

    /// <summary>Number of 64-bit words in the dirty bitset.</summary>
    public int WordCount => _dirty.Length;

    /// <summary>
    /// Stores <paramref name="leaseIndex"/> in the mailbox of <paramref name="keySlot"/> and returns the previous
    /// value (-1 when it was empty). Transport thread. A non-negative result is a lease the game thread never saw;
    /// the caller frees it. Follow with <see cref="SetDirty"/>.
    /// </summary>
    /// <param name="keySlot">Key slot.</param>
    /// <param name="leaseIndex">Non-negative lease index to post.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Exchange(int keySlot, int leaseIndex) => Interlocked.Exchange(ref _mailbox[keySlot], leaseIndex);

    /// <summary>Marks <paramref name="keySlot"/> dirty. Transport thread, after <see cref="Exchange"/>.</summary>
    /// <param name="keySlot">Key slot.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetDirty(int keySlot) => Interlocked.Or(ref _dirty[keySlot >> 6], 1UL << (keySlot & 63));

    /// <summary><see cref="Exchange"/> followed by <see cref="SetDirty"/>; returns the displaced value. Transport thread.</summary>
    /// <param name="keySlot">Key slot.</param>
    /// <param name="leaseIndex">Non-negative lease index to post.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Post(int keySlot, int leaseIndex)
    {
        int previous = Interlocked.Exchange(ref _mailbox[keySlot], leaseIndex);
        Interlocked.Or(ref _dirty[keySlot >> 6], 1UL << (keySlot & 63));
        return previous;
    }

    /// <summary>
    /// Collects dirty key slots into <paramref name="keySlots"/>, clearing their bits, and returns how many were
    /// written. Game thread. Stops when the span is full; remaining dirty keys stay dirty for the next call.
    /// </summary>
    /// <param name="keySlots">Receives dirty key slots in increasing order.</param>
    public int PopDirty(Span<int> keySlots)
    {
        int n = 0;
        ulong* words = _dirty.Pointer;
        int wordCount = _dirty.Length;
        for (int w = 0; w < wordCount && n < keySlots.Length; w++)
        {
            ulong bits = Volatile.Read(ref words[w]);
            if (bits == 0)
                continue;

            ulong consumed = 0;
            int baseSlot = w << 6;
            while (bits != 0 && n < keySlots.Length)
            {
                int tz = BitOperations.TrailingZeroCount(bits);
                consumed |= 1UL << tz;
                bits &= bits - 1;
                keySlots[n++] = baseSlot + tz;
            }

            // Clear only the bits handed out; bits the producer set since the read above survive.
            Interlocked.And(ref words[w], ~consumed);
        }

        return n;
    }

    /// <summary>Empties the mailbox of <paramref name="keySlot"/> and returns its value (-1 when empty). Game thread, after <see cref="PopDirty"/>.</summary>
    /// <param name="keySlot">Key slot.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Take(int keySlot) => Interlocked.Exchange(ref _mailbox[keySlot], -1);

    /// <summary>Current mailbox value without taking it (-1 when empty). Diagnostics; any thread.</summary>
    /// <param name="keySlot">Key slot.</param>
    public int Peek(int keySlot) => Volatile.Read(ref _mailbox[keySlot]);

    /// <summary>Whether <paramref name="keySlot"/>'s dirty bit is set right now. Diagnostics; any thread.</summary>
    /// <param name="keySlot">Key slot.</param>
    public bool IsDirty(int keySlot) => (Volatile.Read(ref _dirty[keySlot >> 6]) & (1UL << (keySlot & 63))) != 0;

    /// <summary>Number of dirty bits, as a snapshot that may be stale by the time it returns. Any thread.</summary>
    public int DirtyCountEstimate
    {
        get
        {
            int count = 0;
            ulong* words = _dirty.Pointer;
            for (int w = 0; w < _dirty.Length; w++)
                count += BitOperations.PopCount(Volatile.Read(ref words[w]));
            return count;
        }
    }

    /// <summary>Frees the native memory. Idempotent; must not race with other calls.</summary>
    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        _mailbox.Dispose();
        _dirty.Dispose();
    }
}
