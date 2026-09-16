using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace Tedd.Quicly.Core.State;

/// <summary>
/// Latest-wins hand-off of one lease index per key slot from the transport thread to the game thread, for keyed
/// channels with <c>CoalesceOnReceive</c> (ADR 0008 invariant 6): an <see cref="int"/> mailbox per key slot
/// (-1 = empty) plus a dirty bitset that tells the game thread which keys changed. Bounded memory, no
/// back-pressure, no ring entries. One instance per channel.
/// </summary>
/// <remarks>
/// <para><b>Transport thread (single producer):</b> <see cref="Exchange"/> the new lease index into the key's
/// mailbox, then <see cref="SetDirty"/> the key (or <see cref="Post"/>, which does both). A non-negative previous
/// value is a lease the game thread never saw; the transport thread frees it immediately.</para>
/// <para><b>Game thread (single consumer):</b> <see cref="PopDirty"/> collects dirty keys (their bits are cleared
/// inside the call), then <see cref="Take"/> claims each key's mailbox. <see cref="Take"/> can return -1 for a key
/// that <see cref="PopDirty"/> just reported: when the producer posts after the consumer's clear but before its
/// take, that take claims the new value and the bit set by the post stays behind with nothing under it. Callers
/// skip such keys.</para>
/// <para><b>Memory ordering.</b> Every access to a mailbox word is an interlocked exchange, and every write to a
/// dirty word is an interlocked OR/AND. .NET interlocked operations are full fences, so all of them fall into one
/// total order consistent with each thread's program order. Take a producer post <c>E</c> (exchange in value
/// <c>v</c>) followed by <c>S</c> (set the bit), and a consumer pass <c>C</c> (clear the bit) followed by <c>T</c>
/// (take):</para>
/// <list type="number">
/// <item><description><b>Exactly once.</b> A mailbox is a single atomic cell, so the value written by each
/// exchange is returned by the next exchange on that cell: the producer's next <see cref="Exchange"/> (it
/// frees a lease the consumer never saw) or the consumer's <see cref="Take"/>. Nothing is returned twice or
/// dropped silently, except a value still sitting in the mailbox.</description></item>
/// <item><description><b>Never stranded.</b> Suppose <c>v</c> is still in the mailbox when the threads go
/// quiet. If some clear <c>C</c> came after <c>S</c>, its take <c>T</c> (after <c>C</c>) would also come after
/// <c>E</c> (before <c>S</c>) and would have claimed <c>v</c>, a contradiction. So every clear of that bit came
/// before <c>S</c>, the bit is still set, and the next <see cref="PopDirty"/> reports the key. This depends on
/// the consumer clearing <em>before</em> taking; take-then-clear would lose a post that lands between the
/// two. <see cref="PopDirty"/> clears only the bits it read and handed out (<c>AND ~consumed</c>), so a bit
/// set after its read survives.</description></item>
/// <item><description><b>Payload visibility.</b> Everything the producer wrote before <c>E</c> (the lease
/// bytes) is visible to the consumer after a <see cref="Take"/> that returns <c>v</c>. <c>E</c> is a release
/// and <c>T</c> an acquire on the same cell.</description></item>
/// </list>
/// <para>The bitset scan first tests each whole cache line (eight words) with one plain vector read and skips a
/// clean line; a non-clean line is then read word by word with <see cref="Volatile.Read(ref readonly ulong)"/>.
/// Both reads are only hints about where to look: clearing is always the interlocked AND of the bits handed out.
/// A bit set after either read is reported by the next call, as the argument above requires.</para>
/// <para>After <see cref="Dispose"/> every per-key call throws <see cref="ArgumentOutOfRangeException"/> and
/// <see cref="PopDirty"/> returns 0.</para>
/// </remarks>
public sealed unsafe class Mailboxes : IDisposable
{
    /// <summary>Largest number of key slots.</summary>
    public const int MaxKeySlots = 1 << 30;

    /// <summary>Value of an empty mailbox.</summary>
    public const int Empty = -1;

    private const int WordsPerLine = 8;

    private readonly NativeArray<int> _mailbox;
    private readonly NativeArray<ulong> _dirty;

    /// <summary>Creates mailboxes for <paramref name="keySlots"/> key slots, all empty and clean.</summary>
    /// <param name="keySlots">Number of key slots (0 … <see cref="MaxKeySlots"/>).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="keySlots"/> is outside its range.</exception>
    public Mailboxes(int keySlots)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(keySlots);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(keySlots, MaxKeySlots);
        _mailbox = new NativeArray<int>(keySlots);
        _mailbox.Fill(Empty);
        _dirty = new NativeArray<ulong>((keySlots + 63) >> 6);
    }

    /// <summary>Number of key slots. Zero after <see cref="Dispose"/>.</summary>
    public int Capacity => _mailbox.Length;

    /// <summary>
    /// Stores <paramref name="leaseIndex"/> in the mailbox of <paramref name="keySlot"/> and returns the previous
    /// value (<see cref="Empty"/> when there was none). Transport thread. A non-negative result is a lease the game
    /// thread never saw, and the caller frees it. Follow with <see cref="SetDirty"/>.
    /// </summary>
    /// <param name="keySlot">Key slot.</param>
    /// <param name="leaseIndex">Non-negative lease index to post.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="keySlot"/> is outside <c>[0, Capacity)</c>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Exchange(int keySlot, int leaseIndex)
    {
        Debug.Assert(leaseIndex >= 0, "posting a negative lease index would look like an empty mailbox");
        return Interlocked.Exchange(ref _mailbox[keySlot], leaseIndex);
    }

    /// <summary>Marks <paramref name="keySlot"/> dirty. Transport thread, after <see cref="Exchange"/>.</summary>
    /// <param name="keySlot">Key slot.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="keySlot"/> is outside <c>[0, Capacity)</c>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetDirty(int keySlot)
    {
        if ((uint)keySlot >= (uint)_mailbox.Length)
            ThrowSlotOutOfRange(keySlot);
        Interlocked.Or(ref _dirty.Pointer[keySlot >> 6], 1UL << keySlot);
    }

    /// <summary><see cref="Exchange"/> followed by <see cref="SetDirty"/>; returns the displaced value. Transport thread.</summary>
    /// <param name="keySlot">Key slot.</param>
    /// <param name="leaseIndex">Non-negative lease index to post.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="keySlot"/> is outside <c>[0, Capacity)</c>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Post(int keySlot, int leaseIndex)
    {
        int previous = Exchange(keySlot, leaseIndex);
        Interlocked.Or(ref _dirty.Pointer[keySlot >> 6], 1UL << keySlot);
        return previous;
    }

    /// <summary>
    /// Writes dirty key slots into <paramref name="keySlots"/> in increasing order, clears their bits, and returns
    /// how many were written. Game thread. Stops when the span is full; the remaining dirty keys stay dirty for the
    /// next call.
    /// </summary>
    /// <param name="keySlots">Receives dirty key slots.</param>
    public int PopDirty(Span<int> keySlots)
    {
        int n = 0;
        ulong* words = _dirty.Pointer;
        int wordCount = _dirty.Length;
        int w = 0;
        while (w < wordCount && n < keySlots.Length)
        {
            // Skip a clean cache line with one test (the array is 64-byte aligned, so w % 8 == 0 is a line start).
            if ((w & (WordsPerLine - 1)) == 0 && wordCount - w >= WordsPerLine && IsLineClean(words + w))
            {
                w += WordsPerLine;
                continue;
            }

            ulong bits = Volatile.Read(ref words[w]);
            if (bits != 0)
            {
                ulong consumed = 0;
                int baseSlot = w << 6;
                do
                {
                    int tz = BitOperations.TrailingZeroCount(bits);
                    consumed |= 1UL << tz;
                    bits &= bits - 1;
                    keySlots[n++] = baseSlot + tz;
                }
                while (bits != 0 && n < keySlots.Length);

                // Clear only the bits handed out; bits the producer set since the read above survive.
                Interlocked.And(ref words[w], ~consumed);
            }

            w++;
        }

        return n;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsLineClean(ulong* line) =>
        (Vector128.Load(line) | Vector128.Load(line + 2) | Vector128.Load(line + 4) | Vector128.Load(line + 6)) == Vector128<ulong>.Zero;

    /// <summary>Empties the mailbox of <paramref name="keySlot"/> and returns its value (<see cref="Empty"/> when there was none). Game thread, after <see cref="PopDirty"/>.</summary>
    /// <param name="keySlot">Key slot.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="keySlot"/> is outside <c>[0, Capacity)</c>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Take(int keySlot) => Interlocked.Exchange(ref _mailbox[keySlot], Empty);

    /// <summary>
    /// Whether any key slot is dirty right now (any thread; <see cref="Session.QuiclyPeer.HasPendingWork"/>). One acquire
    /// read per bitset word, so a post that lands during the scan is simply reported by the next call.
    /// </summary>
    internal bool HasDirty
    {
        get
        {
            ulong* words = _dirty.Pointer;
            int wordCount = _dirty.Length;
            for (int w = 0; w < wordCount; w++)
            {
                if (Volatile.Read(ref words[w]) != 0)
                    return true;
            }

            return false;
        }
    }

    /// <summary>Current mailbox value without taking it. Diagnostics; any thread.</summary>
    internal int Peek(int keySlot) => Volatile.Read(ref _mailbox[keySlot]);

    /// <summary>Whether <paramref name="keySlot"/>'s dirty bit is set right now. Diagnostics; any thread.</summary>
    internal bool IsDirty(int keySlot)
    {
        if ((uint)keySlot >= (uint)_mailbox.Length)
            ThrowSlotOutOfRange(keySlot);
        return (Volatile.Read(ref _dirty.Pointer[keySlot >> 6]) & (1UL << keySlot)) != 0;
    }

    /// <summary>Frees the native memory. Idempotent; must not race with other calls.</summary>
    public void Dispose()
    {
        _mailbox.Dispose();
        _dirty.Dispose();
    }

    private static void ThrowSlotOutOfRange(int keySlot) =>
        throw new ArgumentOutOfRangeException(nameof(keySlot), keySlot, "Key slot is outside the mailboxes.");
}
