using System.Runtime.InteropServices;
using Tedd.Quicly.Transport.MsQuic;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>
/// Stream sends for the echo checks. MsQuic sends straight from the caller's buffers (send buffering is off), so each payload
/// is copied into one native block holding its <see cref="QUIC_BUFFER"/> and its bytes. The block is the send context and is
/// freed at SendComplete, or at once when MsQuic refuses the send (no completion follows a failure).
/// </summary>
internal static unsafe class NativePayload
{
    public static int Send(MsQuicStream stream, ReadOnlySpan<byte> data, QUIC_SEND_FLAGS flags)
    {
        byte* block = (byte*)NativeMemory.Alloc((nuint)(sizeof(QUIC_BUFFER) + data.Length));
        QUIC_BUFFER* buffer = (QUIC_BUFFER*)block;
        byte* payload = block + sizeof(QUIC_BUFFER);
        data.CopyTo(new Span<byte>(payload, data.Length));
        *buffer = new QUIC_BUFFER(payload, (uint)data.Length);
        int status = stream.Send(buffer, 1, flags, block);
        if (MsQuicStatus.Failed(status))
        {
            NativeMemory.Free(block);
        }

        return status;
    }

    public static void Free(void* context)
    {
        if (context != null)
        {
            NativeMemory.Free(context);
        }
    }
}
