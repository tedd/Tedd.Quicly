using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Tests.State;

public class MailboxesTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(Mailboxes.MaxKeySlots + 1)]
    public void Constructor_Rejects_Invalid_Size(int keySlots)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new Mailboxes(keySlots));
    }

    [Fact]
    public void New_Mailboxes_Are_Empty_And_Clean()
    {
        using var mailboxes = new Mailboxes(130);
        Assert.Equal(130, mailboxes.Capacity);
        for (int k = 0; k < 130; k++)
        {
            Assert.Equal(Mailboxes.Empty, mailboxes.Peek(k));
            Assert.False(mailboxes.IsDirty(k));
            Assert.Equal(Mailboxes.Empty, mailboxes.Take(k));
        }

        Assert.Equal(0, mailboxes.PopDirty(new int[8]));
    }

    [Fact]
    public void Zero_Slots_Is_Allowed()
    {
        using var mailboxes = new Mailboxes(0);
        Assert.Equal(0, mailboxes.Capacity);
        Assert.Equal(0, mailboxes.PopDirty(new int[4]));
        Assert.Throws<ArgumentOutOfRangeException>(() => mailboxes.Post(0, 1));
    }

    [Fact]
    public void Exchange_Returns_The_Displaced_Value_And_Take_Empties()
    {
        using var mailboxes = new Mailboxes(8);
        Assert.Equal(Mailboxes.Empty, mailboxes.Exchange(3, 10));
        Assert.False(mailboxes.IsDirty(3)); // Exchange alone does not mark
        mailboxes.SetDirty(3);
        Assert.True(mailboxes.IsDirty(3));
        Assert.Equal(10, mailboxes.Exchange(3, 11));   // the producer frees 10
        Assert.Equal(11, mailboxes.Post(3, 12));       // the producer frees 11
        Assert.Equal(12, mailboxes.Peek(3));

        Span<int> dirty = stackalloc int[4];
        Assert.Equal(1, mailboxes.PopDirty(dirty));
        Assert.Equal(3, dirty[0]);
        Assert.False(mailboxes.IsDirty(3));
        Assert.Equal(12, mailboxes.Take(3));
        Assert.Equal(Mailboxes.Empty, mailboxes.Take(3));
        Assert.Equal(0, mailboxes.PopDirty(dirty));
    }

    [Fact]
    public void PopDirty_Reports_In_Increasing_Order_Across_Words_And_Resumes_When_The_Span_Is_Full()
    {
        using var mailboxes = new Mailboxes(300);
        int[] keys = [299, 0, 64, 63, 1, 128, 200, 255, 256];
        foreach (int k in keys)
            mailboxes.Post(k, k + 1000);

        var span = new int[4];
        var seen = new List<int>();
        int n;
        while ((n = mailboxes.PopDirty(span)) > 0)
        {
            Assert.InRange(n, 1, 4);
            for (int i = 0; i < n; i++)
            {
                seen.Add(span[i]);
                Assert.Equal(span[i] + 1000, mailboxes.Take(span[i]));
            }
        }

        Array.Sort(keys);
        Assert.Equal(keys, seen);
        foreach (int k in keys)
            Assert.False(mailboxes.IsDirty(k));

        // A span that fills mid-word leaves the rest of that word dirty.
        for (int k = 0; k < 10; k++)
            mailboxes.Post(k, k);
        Assert.Equal(3, mailboxes.PopDirty(span.AsSpan(0, 3)));
        Assert.Equal(new[] { 0, 1, 2 }, span[..3]);
        for (int k = 3; k < 10; k++)
            Assert.True(mailboxes.IsDirty(k));
        Assert.Equal(0, mailboxes.PopDirty(Span<int>.Empty));
    }

    [Fact]
    public void PopDirty_Finds_Keys_At_Line_Boundaries_And_In_The_Partial_Tail()
    {
        // 27 words: three full 8-word lines (skipped by the vector test when clean) and a 3-word tail.
        const int Slots = 27 * 64;
        using var mailboxes = new Mailboxes(Slots);
        var buffer = new int[64];
        Assert.Equal(0, mailboxes.PopDirty(buffer));

        int[] keys = [0, 511, 512, 1000, 1535, 1536, 1600, Slots - 1];
        foreach (int k in keys)
            mailboxes.Post(k, k);
        Assert.Equal(keys.Length, mailboxes.PopDirty(buffer));
        Assert.Equal(keys, buffer[..keys.Length]);
        foreach (int k in keys)
            Assert.Equal(k, mailboxes.Take(k));
        Assert.Equal(0, mailboxes.PopDirty(buffer));

        // One dirty key in the last full line only: the first two lines are skipped, the rest scanned.
        mailboxes.Post(1100, 5);
        Assert.Equal(1, mailboxes.PopDirty(buffer));
        Assert.Equal(1100, buffer[0]);

        // A span that fills inside a line resumes at the right word on the next call.
        for (int k = 64; k < 64 + 70; k++)
            mailboxes.Post(k, k);
        Assert.Equal(64, mailboxes.PopDirty(buffer));
        Assert.Equal(64, buffer[0]);
        Assert.Equal(127, buffer[63]);
        Assert.Equal(6, mailboxes.PopDirty(buffer));
        Assert.Equal(new[] { 128, 129, 130, 131, 132, 133 }, buffer[..6]);
    }

    [Fact]
    public void A_Dirty_Bit_Without_A_Value_Takes_Empty()
    {
        using var mailboxes = new Mailboxes(64);
        mailboxes.Post(5, 1);
        Assert.Equal(1, mailboxes.Take(5)); // claimed before its bit was popped
        Span<int> dirty = stackalloc int[2];
        Assert.Equal(1, mailboxes.PopDirty(dirty));
        Assert.Equal(Mailboxes.Empty, mailboxes.Take(dirty[0]));
    }

    [Fact]
    public void Key_Slots_Are_Bounds_Checked()
    {
        using var mailboxes = new Mailboxes(64);
        Assert.Throws<ArgumentOutOfRangeException>(() => mailboxes.Exchange(64, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => mailboxes.Exchange(-1, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => mailboxes.Post(64, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => mailboxes.SetDirty(64));
        Assert.Throws<ArgumentOutOfRangeException>(() => mailboxes.SetDirty(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => mailboxes.Take(64));
        Assert.Throws<ArgumentOutOfRangeException>(() => mailboxes.Peek(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => mailboxes.IsDirty(64));
    }

    [Fact]
    public void Dispose_Is_Idempotent_And_Disables_The_Mailboxes()
    {
        var mailboxes = new Mailboxes(64);
        mailboxes.Post(1, 1);
        mailboxes.Dispose();
        mailboxes.Dispose();
        Assert.Equal(0, mailboxes.Capacity);
        Assert.Equal(0, mailboxes.PopDirty(new int[4]));
        Assert.Throws<ArgumentOutOfRangeException>(() => mailboxes.Take(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => mailboxes.Post(1, 2));
    }

    [Fact]
    public void Post_PopDirty_Take_Does_Not_Allocate()
    {
        using var mailboxes = new Mailboxes(4096);
        int[] buffer = new int[256];
        Run(mailboxes, buffer, 1_000);
        WindowedAllocation.AssertNone(() => Run(mailboxes, buffer, 20_000));

        static int Run(Mailboxes mailboxes, int[] buffer, int iterations)
        {
            long sum = 0;
            for (int i = 0; i < iterations; i++)
            {
                int slot = i * 13 & 4095;
                sum += mailboxes.Post(slot, i);
                if ((i & 63) == 63)
                {
                    int n = mailboxes.PopDirty(buffer);
                    for (int j = 0; j < n; j++)
                        sum += mailboxes.Take(buffer[j]);
                }
            }

            return (int)sum;
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(200)]
    public void Spsc_Stress_Every_Lease_Is_Seen_Exactly_Once_By_Exactly_One_Side(int keySlots)
    {
        // The producer (transport thread) posts lease indices 0..N-1 in increasing order to random keys and frees
        // every displaced value; the consumer (game thread) pops dirty keys and takes their mailboxes. Each lease must
        // be returned exactly once, to exactly one side; per key the consumer must see increasing values (latest
        // wins, nothing older resurfaces); the payload written before the post must be visible after the take; and
        // once the producer stops, one final drain must leave every mailbox empty (no value stranded without a bit).
        const int N = 400_000;
        using var mailboxes = new Mailboxes(keySlots);
        int[] seenBy = new int[N];          // 1 = producer (displaced), 2 = consumer (taken)
        int[] seenCount = new int[N];
        long[] payload = new long[N];
        bool producerDone = false;
        int payloadErrors = 0;
        int orderErrors = 0;
        int emptyTakes = 0;

        var producer = new Thread(() =>
        {
            var rng = new Random(keySlots);
            for (int lease = 0; lease < N; lease++)
            {
                payload[lease] = lease * 7L + 3;
                int previous = mailboxes.Post(rng.Next(keySlots), lease);
                if (previous >= 0)
                {
                    Interlocked.Increment(ref seenCount[previous]);
                    seenBy[previous] = 1;
                }
            }

            Volatile.Write(ref producerDone, true);
        })
        { IsBackground = true };

        int[] lastTaken = new int[keySlots];
        Array.Fill(lastTaken, -1);
        int[] dirty = new int[Math.Max(1, keySlots / 3)];
        producer.Start();

        bool finalPass = false;
        while (true)
        {
            bool done = Volatile.Read(ref producerDone);
            int n;
            while ((n = mailboxes.PopDirty(dirty)) > 0)
            {
                for (int i = 0; i < n; i++)
                {
                    int key = dirty[i];
                    int lease = mailboxes.Take(key);
                    if (lease < 0)
                    {
                        emptyTakes++;
                        continue;
                    }

                    Interlocked.Increment(ref seenCount[lease]);
                    seenBy[lease] = 2;
                    if (payload[lease] != lease * 7L + 3)
                        payloadErrors++;
                    if (lease <= lastTaken[key])
                        orderErrors++;
                    lastTaken[key] = lease;
                }
            }

            if (finalPass)
                break;
            if (done)
                finalPass = true; // one more full drain after observing the producer's completion
        }

        producer.Join();
        for (int k = 0; k < keySlots; k++)
        {
            Assert.Equal(Mailboxes.Empty, mailboxes.Peek(k));
            Assert.False(mailboxes.IsDirty(k));
        }

        Assert.Equal(0, payloadErrors);
        Assert.Equal(0, orderErrors);
        int consumerCount = 0;
        for (int lease = 0; lease < N; lease++)
        {
            Assert.Equal(1, seenCount[lease]);
            if (seenBy[lease] == 2)
                consumerCount++;
        }

        // The consumer saw at least the last value of each key; the producer displaced everything else.
        Assert.InRange(consumerCount, Math.Min(keySlots, N), N);
        Assert.InRange(emptyTakes, 0, N);
    }
}
