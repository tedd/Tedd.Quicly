using System.Runtime.InteropServices;

namespace Tedd.Quicly.Core.Transport;

/// <summary>
/// One contiguous native memory segment handed to or received from a transport.
/// In a 64-bit process the layout is bit-identical to MsQuic's <c>QUIC_BUFFER</c> (<c>uint32_t Length; uint8_t* Buffer;</c>,
/// 16 bytes) so arrays of segments can be passed to the native library without translation. In a 32-bit process
/// <c>QUIC_BUFFER</c> is 8 bytes while this struct stays 16, so a transport that reinterprets segment arrays (the MsQuic
/// transport) refuses to run there.
/// </summary>
[StructLayout(LayoutKind.Sequential, Size = 16)]
public unsafe struct TransportSegment
{
    /// <summary>Number of valid bytes at <see cref="Buffer"/>.</summary>
    public uint Length;

    private uint _padding;

    /// <summary>Pointer to the first byte. Must stay valid (pinned or native) until the matching completion.</summary>
    public byte* Buffer;

    /// <summary>Creates a segment over native or pinned memory.</summary>
    public TransportSegment(byte* buffer, int length)
    {
        Buffer = buffer;
        Length = (uint)length;
        _padding = 0;
    }

    /// <summary>The segment as a span. Valid only while the underlying memory is.</summary>
    public readonly ReadOnlySpan<byte> AsSpan() => new(Buffer, (int)Length);
}
