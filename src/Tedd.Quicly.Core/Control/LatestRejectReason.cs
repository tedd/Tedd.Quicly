namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Why a receiver dropped a ReliableLatest version locally (<see cref="LatestRejectEntry.Reason"/>,
/// PROTOCOL.md §2.3). Other values are rejected by the parser with <see cref="ControlParseStatus.InvalidValue"/>.
/// </summary>
public enum LatestRejectReason : byte
{
    /// <summary>The receive ring was full.</summary>
    RingFull = 1,

    /// <summary>The value exceeded the receiver's size limit.</summary>
    TooLarge = 2,

    /// <summary>The value could not be decoded (for example a decompression failure).</summary>
    DecodeError = 3,

    /// <summary>The receiver's per-channel key table was full (PROTOCOL.md §7 <c>MaxKeys</c>).</summary>
    KeyTableFull = 4,
}
