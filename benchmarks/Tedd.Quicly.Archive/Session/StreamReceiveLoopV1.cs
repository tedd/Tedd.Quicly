using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;

namespace Tedd.Quicly.Archive.Session;

/// <summary>
/// V1 of the peer's engine-stream receive loop (<c>QuiclyPeer.ReceiveEngineStream</c>, wave C1 to the stream-receive pass):
/// a <see cref="StreamFrameParser.Mark"/> per event, a whole parser copy only on bulk streams, and every message handed to
/// the engine as three events (<c>Start</c>, <c>Chunk</c>, <c>End</c>) even when its payload lies wholly in the segment.
/// Superseded by V2, which hands such a message over in one event (<c>StreamMessagePhase.Whole</c>,
/// docs/benchmarks/session.md). The engine call is reduced to a consumer that sums what it sees, as in V0.
/// </summary>
public static class StreamReceiveLoopV1
{
    /// <summary>Parses one received segment the way V1 did.</summary>
    /// <param name="parser">The stream's parser.</param>
    /// <param name="table">The channel table.</param>
    /// <param name="segment">The segment.</param>
    /// <param name="pendAt">Event number (counted in <paramref name="events"/>) that the consumer pends; -1 = none.</param>
    /// <param name="events">Message events seen so far (across calls).</param>
    /// <returns>A checksum of what the consumer saw, or -1 when it pended (the event was un-read).</returns>
    public static long Run(ref StreamFrameParser parser, ChannelTable table, ReadOnlySpan<byte> segment, int pendAt, ref int events)
    {
        ReadOnlySpan<byte> input = segment;
        StreamFrameParser snapshot = default;
        long sum = 0;
        while (true)
        {
            bool copy = parser.Role == StreamRole.Bulk;
            parser.GetMark(out StreamFrameParser.Mark mark);
            if (copy)
            {
                snapshot = parser;
            }

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
                if (copy)
                {
                    parser = snapshot;
                }
                else
                {
                    parser.Rewind(in mark);
                }

                return -1;
            }

            sum += (long)streamEvent + parser.Message.Length + payload.Length;
        }
    }
}
