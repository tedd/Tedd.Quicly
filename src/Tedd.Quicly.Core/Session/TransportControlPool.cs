using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// A few fixed native buffers owned by the transport thread for control datagrams it must send from inside a callback
/// (Pong, PROTOCOL.md §2.3: answered at once so the responder's timestamps are as close as possible to the wire).
/// Allocation, send and release all happen on the transport thread, so no hand-off is needed.
/// </summary>
/// <remarks>
/// Contexts are <c>(generation &lt;&lt; 32) | 0x8000_0000 | slot</c>; the tag bit keeps them apart from send-entry contexts
/// (whose slot part is below the send table capacity). Buffers and segments stay reserved until the final datagram
/// state (or <c>Sent</c> when the transport does not report states), and a completion delivered re-entrantly inside the
/// send call is handled idempotently (ADR 0008 invariants 1, 2 and 7).
/// </remarks>
internal sealed unsafe class TransportControlPool : IDisposable
{
    /// <summary>Number of buffers.</summary>
    public const int SlotCount = 8;

    /// <summary>Bytes per buffer.</summary>
    public const int SlotBytes = 32;

    /// <summary>Tag bit in the low half of a pool context.</summary>
    public const uint ContextTag = 0x8000_0000;

    private readonly NativeArray<byte> _buffers = new(SlotCount * SlotBytes);
    private readonly NativeArray<TransportSegment> _segments = new(SlotCount);
    private readonly uint[] _generation = new uint[SlotCount];
    private readonly bool[] _busy = new bool[SlotCount];

    /// <summary>Buffers in flight.</summary>
    public int InUse { get; private set; }

    /// <summary>True for a context produced by this pool.</summary>
    /// <param name="context">A transport context.</param>
    public static bool IsPoolContext(ulong context) => ((uint)context & ContextTag) != 0;

    /// <summary>Sends <paramref name="datagram"/> from a free buffer. Transport thread.</summary>
    /// <param name="transport">The transport.</param>
    /// <param name="datagram">At most <see cref="SlotBytes"/> bytes.</param>
    /// <param name="flags">Send flags.</param>
    /// <returns><see langword="false"/> when no buffer is free or the transport refused the send.</returns>
    public bool TrySend(ITransport transport, ReadOnlySpan<byte> datagram, TransportSendFlags flags)
    {
        if (datagram.Length > SlotBytes)
        {
            return false;
        }

        for (int i = 0; i < SlotCount; i++)
        {
            if (_busy[i])
            {
                continue;
            }

            uint generation = _generation[i] + 1;
            if (generation == 0)
            {
                generation = 1;
            }

            _generation[i] = generation;
            _busy[i] = true;
            InUse++;
            byte* buffer = _buffers.Pointer + (i * SlotBytes);
            datagram.CopyTo(new Span<byte>(buffer, SlotBytes));
            _segments.Pointer[i] = new TransportSegment(buffer, datagram.Length);
            ulong context = ((ulong)generation << 32) | ContextTag | (uint)i;
            if (transport.SendDatagram(_segments.Pointer + i, 1, context, flags) == TransportStatus.Success)
            {
                return true;
            }

            // No completion follows a failed send.
            Release(context);
            return false;
        }

        return false;
    }

    /// <summary>Frees the buffer of <paramref name="context"/> if it is still the live occupant. Transport thread; idempotent.</summary>
    /// <param name="context">A pool context.</param>
    public void Release(ulong context)
    {
        int slot = (int)((uint)context & ~ContextTag);
        if ((uint)slot < SlotCount && _busy[slot] && _generation[slot] == (uint)(context >> 32))
        {
            _busy[slot] = false;
            InUse--;
        }
    }

    /// <summary>Frees the native memory.</summary>
    public void Dispose()
    {
        _buffers.Dispose();
        _segments.Dispose();
    }
}
