using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Review (perf-threading lens) of fix/localhost-drops: the pending-work probe after the drain-queue change. The branch
/// documents, on <see cref="QuiclyPeer.HasPendingWork"/>, that messages queued for a channel without a handler are not
/// work ("they wait for the application's Drain"), and fixed the probe for unreliable ring channels. A channel whose
/// values wait in a mailbox — a coalescing channel, or ReliableLatest — still reports work for as long as nobody drains
/// it, although no Poll can consume it: a host that polls while the probe is set never sleeps.
/// </summary>
/// <remarks>
/// Fixed since: the probe counts a mailbox only while its channel has a handler. The description above is what the tests
/// found; they now pin the fix.
/// </remarks>
public class ReviewPerfthreadingWorkProbeTests
{
    /// <summary>2 unordered · 9 keyed sequenced, coalescing (a mailbox) · 12 ReliableLatest (always a mailbox).</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered, o => o.ExpiryMicros = 0)
        .Add(9, "latest", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.CoalesceOnReceive = true; o.MaxKeys = 64; o.ExpiryMicros = 0; })
        .Add(12, "state", ChannelMode.ReliableLatest, o => o.MaxKeys = 64)
        .Build();

    private static byte[] Payload(int index)
    {
        byte[] payload = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static void AssertTheHostCanSleep(SessionHarness h, string what)
    {
        QuiclyPeer server = h.Server!;
        for (int i = 0; i < 200; i++)
        {
            h.Network.Advance(1_000);
            h.Client.Poll();
            h.Client.Flush();
            server.Poll();
            server.Flush();
        }

        // Settled: nothing is in flight, every ack was sent. What is left is only what waits for the application's Drain.
        for (int i = 0; i < 20; i++)
        {
            server.Poll();
            server.Flush();
            Assert.False(server.HasPendingWork, $"{what} nobody drains keeps the peer reporting work (poll {i})");
            Assert.False(server.HasPendingPollWork, $"{what} nobody drains keeps the peer marked for polling (poll {i})");
            h.Network.Advance(1_000);
            h.Client.Poll();
            h.Client.Flush();
        }
    }

    [Fact]
    public void An_Undrained_Coalescing_Channel_Does_Not_Keep_The_Host_Busy()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        for (ulong key = 1; key <= 4; key++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(9, key), Payload((int)key)).Status);
        }

        AssertTheHostCanSleep(h, "a coalescing channel");
    }

    [Fact]
    public void An_Undrained_ReliableLatest_Channel_Does_Not_Keep_The_Host_Busy()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        for (ulong key = 1; key <= 4; key++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(12, key), Payload((int)key)).Status);
        }

        AssertTheHostCanSleep(h, "a ReliableLatest channel");
    }

    [Fact]
    public void Guard_An_Undrained_Unreliable_Ring_Channel_Lets_The_Host_Sleep()
    {
        using SessionHarness h = new(table: Table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        for (int i = 0; i < 4; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), Payload(i)).Status);
        }

        AssertTheHostCanSleep(h, "an unreliable channel");
    }
}
