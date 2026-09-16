using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;

namespace Tedd.Quicly.Archive.Session;

/// <summary>
/// V0 of the peer's engine-stream receive loop (<c>QuiclyPeer.ReceiveEngineStream</c>, wave C1 steps 1 to 3): before every
/// parser event it copies the whole <see cref="StreamFrameParser"/> (about 190 bytes: the bulk header, the 32-byte
/// partial-header buffer and the configuration) so that an engine's Pend can restore it. Superseded by
/// <see cref="StreamFrameParser.Mark"/>, which saves only the state a message event changes (docs/benchmarks/session.md).
/// The engine call is reduced to a consumer that sums what it sees, identical in both versions.
/// </summary>
public static class StreamReceiveLoopV0
{
    /// <summary>Parses one received segment the way V0 did.</summary>
    /// <param name="parser">The stream's parser.</param>
    /// <param name="table">The channel table.</param>
    /// <param name="segment">The segment.</param>
    /// <param name="pendAt">Event number (counted in <paramref name="events"/>) that the consumer pends; -1 = none.</param>
    /// <param name="events">Message events seen so far (across calls).</param>
    /// <returns>A checksum of what the consumer saw, or -1 when it pended (the event was un-read).</returns>
    public static long Run(ref StreamFrameParser parser, ChannelTable table, ReadOnlySpan<byte> segment, int pendAt, ref int events)
    {
        ReadOnlySpan<byte> input = segment;
        long sum = 0;
        while (true)
        {
            StreamFrameParser snapshot = parser;
            StreamEvent streamEvent = parser.Read(table, ref input, out ReadOnlySpan<byte> payload);
            if (streamEvent is StreamEvent.NeedMore or StreamEvent.Error)
            {
                return sum;
            }

            if (streamEvent == StreamEvent.Preamble)
            {
                continue;
            }

            if (events++ == pendAt)
            {
                parser = snapshot;
                return -1;
            }

            sum += (long)streamEvent + parser.Message.Length + payload.Length;
        }
    }
}
