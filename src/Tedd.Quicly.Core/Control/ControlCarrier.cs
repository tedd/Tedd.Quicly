namespace Tedd.Quicly.Core.Control;

/// <summary>Which of the two control framings a message is written in.</summary>
public enum ControlCarrier : byte
{
    /// <summary>Control datagram (PROTOCOL.md §2.3): <c>0x00</c> (channel 0), <c>Type</c>, body = rest of the datagram.</summary>
    Datagram = 0,

    /// <summary>Control-stream message (PROTOCOL.md §3.4): <c>Length</c> varint in [1, 16384], <c>Type</c>, body of <c>Length − 1</c> bytes.</summary>
    Stream = 1,
}
