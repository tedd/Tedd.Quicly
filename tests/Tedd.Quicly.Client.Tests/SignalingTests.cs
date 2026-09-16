using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Client.Tests;

public class SignalingTests
{
    /// <summary>
    /// The client's signal is the peers' own <see cref="IPeerWorkSignal"/> (<see cref="PeerOptions.WorkSignal"/>): the peer
    /// raises it when it publishes game-thread work, and the wait inside ConnectAsync wakes instead of sleeping its interval.
    /// </summary>
    [Fact]
    public async Task The_Work_Signal_Is_A_Peers_Work_Hook()
    {
        await using ClientFixture f = new();
        using WorkSignal signal = new();
        IPeerWorkSignal hook = signal;
        PeerOptions options = new()
        {
            Clock = f.Clock,
            AllocatorOptions = Pools.Small(),
            SendTableCapacity = 32,
            ReceiveRingCapacity = 32,
            SegmentArenaCapacity = 32,
            WorkSignal = signal,
        };
        using QuiclyPeer peer = QuiclyPeer.Connect(f.Connector, f.EndPoint, "game.test", Tables.Default, options);

        hook.OnWork(peer); // as the peer does from the thread that published the work

        await signal.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.True(signal.LastWaitSignaled);
        f.Step(1_000);
    }

    [Fact]
    public async Task Work_Signal_Wakes_A_Waiter_And_Tolerates_Disposal()
    {
        WorkSignal signal = new();
        ValueTask waiting = signal.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        signal.Set();
        await waiting;
        Assert.True(signal.LastWaitSignaled);
        signal.Set();
        signal.Set(); // already signalled: no-op
        await signal.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.True(signal.LastWaitSignaled); // the first of the two Sets released it (a lost wake-up would sleep 30 s here)
        await signal.WaitAsync(TimeSpan.FromMilliseconds(5), TestContext.Current.CancellationToken);
        Assert.False(signal.LastWaitSignaled); // the second Set released nothing: this wait timed out quietly
        signal.Dispose();
        signal.Set(); // a late transport callback after the client was disposed
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await signal.WaitAsync(TimeSpan.FromMilliseconds(5), TestContext.Current.CancellationToken));
    }
}
