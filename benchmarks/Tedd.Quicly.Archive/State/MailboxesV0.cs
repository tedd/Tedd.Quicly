using System.Numerics;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Archive.State;

/// <summary>
/// V0 of <c>Tedd.Quicly.Core.State.Mailboxes</c>: identical protocol, but <c>PopDirty</c> reads every dirty word
/// with a scalar volatile load and branch, so an idle channel still costs one load and branch per 64 key slots
/// per poll. Superseded by the version that skips clean cache lines with one vector test; kept for the comparison
/// in docs/benchmarks/state.md.
/// </summary>
public sealed unsafe class MailboxesV0 : IDisposable
{
    private readonly NativeArray<int> _mailbox;
    private readonly NativeArray<ulong> _dirty;

    public MailboxesV0(int keySlots)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(keySlots);
        _mailbox = new NativeArray<int>(keySlots);
        _mailbox.Fill(-1);
        _dirty = new NativeArray<ulong>((keySlots + 63) >> 6);
    }

    public int Post(int keySlot, int leaseIndex)
    {
        int previous = Interlocked.Exchange(ref _mailbox[keySlot], leaseIndex);
        Interlocked.Or(ref _dirty.Pointer[keySlot >> 6], 1UL << keySlot);
        return previous;
    }

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
            do
            {
                int tz = BitOperations.TrailingZeroCount(bits);
                consumed |= 1UL << tz;
                bits &= bits - 1;
                keySlots[n++] = baseSlot + tz;
            }
            while (bits != 0 && n < keySlots.Length);

            Interlocked.And(ref words[w], ~consumed);
        }

        return n;
    }

    public int Take(int keySlot) => Interlocked.Exchange(ref _mailbox[keySlot], -1);

    public void Dispose()
    {
        _mailbox.Dispose();
        _dirty.Dispose();
    }
}
