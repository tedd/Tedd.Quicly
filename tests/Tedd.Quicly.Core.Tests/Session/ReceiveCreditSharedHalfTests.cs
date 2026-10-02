using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// RC2-2: the half of the receive budget the reliable channels no handler reads share is counted peer-wide, by the messages
/// taken while their channel was limited (a tag each message carries to its return), so its check is constant time
/// whatever the number of channels. These pin the counting against a brute-force model and across the 32-bit wrap.
/// </summary>
public unsafe class ReceiveCreditSharedHalfTests
{
    private struct Message
    {
        public int Bytes;
        public bool Shared;
    }

    /// <summary>
    /// Random takes, returns, give-backs of staged messages, state changes and reconnects. The peer-wide count always
    /// equals the bytes of the tagged messages that wait; against the per-channel scan it replaced (every channel that is
    /// not handled, all its waiting messages) it differs by exactly the two documented terms — tagged messages of a
    /// channel that has a handler by now, untagged messages of a channel that is limited by now — and is equal whenever
    /// neither exists.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(8)]
    [InlineData(32)]
    [InlineData(128)]
    public void The_Shared_Half_Matches_A_Brute_Force_Model(int channels)
    {
        using ReceiveCredit credit = new(channels, streams: 64, countLimit: 64, byteLimit: 1_000_000, drainedCountLimit: 1_024, drainedByteLimit: 4_000_000);
        for (int c = 0; c < channels; c++)
        {
            credit.Enable(c);
        }

        Random random = new(channels * 7919);
        Queue<Message>[] waiting = new Queue<Message>[channels];
        for (int c = 0; c < channels; c++)
        {
            waiting[c] = new Queue<Message>();
        }

        int equalSteps = 0;
        for (int step = 0; step < 40_000; step++)
        {
            int channel = random.Next(channels);
            int op = random.Next(100);
            if (op < 45)
            {
                CreditTake take = credit.TryTake(channel, 0, 0);
                if (take != CreditTake.Blocked)
                {
                    Message message = new() { Bytes = 64 << random.Next(0, 8), Shared = take == CreditTake.Limited };
                    credit.NoteTaken(channel, message.Bytes, message.Shared);
                    if (random.Next(20) == 0)
                    {
                        // A stream that ends in the middle of the message: the engine gives the staged credit back.
                        credit.Untake(channel, message.Bytes, message.Shared);
                    }
                    else
                    {
                        waiting[channel].Enqueue(message);
                    }
                }
            }
            else if (op < 90)
            {
                if (waiting[channel].TryDequeue(out Message message))
                {
                    credit.NoteReturned(channel, message.Bytes, message.Shared);
                }
            }
            else if (op < 99)
            {
                credit.SetState(channel, (CreditState)random.Next(3));
            }
            else if (random.Next(10) == 0)
            {
                credit.Reset();
                foreach (Queue<Message> queue in waiting)
                {
                    queue.Clear();
                }
            }

            long tagged = 0;
            long scan = 0;
            long taggedHandled = 0;
            long untaggedLimited = 0;
            for (int c = 0; c < channels; c++)
            {
                bool limited = credit.IsLimited(c);
                foreach (Message message in waiting[c])
                {
                    tagged += message.Shared ? message.Bytes : 0;
                    scan += limited ? message.Bytes : 0;
                    taggedHandled += message.Shared && !limited ? message.Bytes : 0;
                    untaggedLimited += !message.Shared && limited ? message.Bytes : 0;
                }
            }

            Assert.Equal(tagged, credit.SharedWaitingBytes);
            Assert.Equal(scan - untaggedLimited + taggedHandled, credit.SharedWaitingBytes);
            if (taggedHandled == 0 && untaggedLimited == 0)
            {
                Assert.Equal(scan, credit.SharedWaitingBytes);
                equalSteps++;
            }
        }

        Assert.True(equalSteps > 1_000, $"only {equalSteps} steps without the documented slack");
    }

    [Fact]
    public void The_Shared_Half_Holds_Back_A_Drained_Channel_Across_The_Counter_Wrap()
    {
        using ReceiveCredit credit = new(channels: 2, streams: 4, countLimit: 100, byteLimit: 500, drainedCountLimit: 100, drainedByteLimit: 1_000);
        credit.Enable(0);
        credit.Enable(1);
        credit.SetState(0, CreditState.Drained);
        credit.SetState(1, CreditState.Drained);
        credit.SetCountersForTest(0, 0xFFFF_FFFE, 0xFFFF_FF00);
        credit.SetCountersForTest(1, 0xFFFF_FFFF, 0xFFFF_FE00);
        credit.SetSharedCountersForTest(0xFFFF_FD00);

        // Each channel starts one message on its empty queue; together they fill the half (1 200 of 1 000).
        Assert.Equal(CreditTake.Limited, credit.TryTake(0, 0, 600));
        credit.NoteTaken(0, 600, shared: true);
        Assert.Equal(CreditTake.Limited, credit.TryTake(1, 0, 600));
        credit.NoteTaken(1, 600, shared: true);
        Assert.Equal(1_200, credit.SharedWaitingBytes);

        // The half is full: neither starts a second message.
        Assert.Equal(CreditTake.Blocked, credit.TryTake(0, 0, 600));
        Assert.Equal(CreditTake.Blocked, credit.TryTake(1, 0, 600));

        // Channel 1's message comes back: channel 0 may go on (it refreshes its copy of what came back).
        credit.NoteReturned(1, 600, shared: true);
        Assert.Equal(600, credit.SharedWaitingBytes);
        Assert.Equal(CreditTake.Limited, credit.TryTake(0, 0, 600));
        credit.NoteTaken(0, 600, shared: true);
        credit.NoteReturned(0, 600, shared: true);
        credit.NoteReturned(0, 600, shared: true);
        Assert.Equal(0, credit.SharedWaitingBytes);
        Assert.Equal(0, credit.WaitingBytes(0));
        Assert.Equal(0, credit.WaitingBytes(1));
    }

    [Fact]
    public void A_Tagged_Message_Returned_On_A_Handled_Channel_Asks_For_A_Look_At_The_Other_Channels()
    {
        // Channel 1 holds a stream back on the full half; channel 0 gets a handler over its tagged backlog. When that
        // backlog is dispatched the half gains room, so the held stream must be looked at although channel 0 is handled.
        using ReceiveCredit credit = new(channels: 2, streams: 4, countLimit: 100, byteLimit: 500, drainedCountLimit: 100, drainedByteLimit: 1_000);
        credit.Enable(0);
        credit.Enable(1);
        credit.SetState(0, CreditState.Drained);
        credit.SetState(1, CreditState.Drained);
        Assert.Equal(CreditTake.Limited, credit.TryTake(0, 0, 0));
        credit.NoteTaken(0, 1_200, shared: true);
        Assert.Equal(CreditTake.Limited, credit.TryTake(1, 0, 0));
        credit.NoteTaken(1, 100, shared: true);
        Assert.Equal(CreditTake.Blocked, credit.TryTake(1, 0, 0));
        TransportStreamId held = new(1, 1);
        Assert.True(credit.NotePended(held, 1));
        ResumeRecorder transport = new();
        credit.Resume(transport);
        Assert.Empty(transport.Resumed);

        credit.SetState(0, CreditState.Handled);
        credit.Resume(transport);
        Assert.Empty(transport.Resumed); // the backlog is still counted: nothing came back yet
        Assert.False(credit.HasWork);

        credit.NoteReturned(0, 1_200, shared: true);
        Assert.True(credit.HasWork);
        credit.Resume(transport);
        Assert.Equal([held], transport.Resumed);
    }

    /// <summary>An <see cref="ITransport"/> that records the streams it is asked to resume and does nothing else.</summary>
    private sealed class ResumeRecorder : ITransport
    {
        public List<TransportStreamId> Resumed { get; } = [];

        public TransportCapabilities Capabilities => default;

        public TransportState State => default;

        public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed) => Resumed.Add(id);

        public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags) => throw new NotSupportedException();

        public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id) => throw new NotSupportedException();

        public TransportStatus StartStream(TransportStreamId id) => throw new NotSupportedException();

        public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags) => throw new NotSupportedException();

        public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => throw new NotSupportedException();

        public void SetStreamPriority(TransportStreamId id, ushort priority) => throw new NotSupportedException();

        public long GetQuicStreamId(TransportStreamId id) => throw new NotSupportedException();

        public void CloseStream(TransportStreamId id) => throw new NotSupportedException();

        public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional) => throw new NotSupportedException();

        public void Close(ulong errorCode, ReadOnlySpan<byte> reason) => throw new NotSupportedException();

        public void GetStatistics(out TransportStatistics statistics) => throw new NotSupportedException();

        public void Dispose()
        {
        }
    }
}
