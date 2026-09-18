using System.Reflection;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server.Tests;

/// <summary>
/// Work a peer publishes before the game thread activates it must not be lost. The listener thread queues the activation
/// and marks the slot; the peer's own work signal (connected, Hello) marks it again, once, until its first Poll. A PollAll
/// that read the activation count just before the listener queued the peer, and then took the slot's bit, found an empty
/// slot and dropped the bit, so the peer was polled only at its admission deadline and the client timed out first.
/// </summary>
public class ActivationWorkTests
{
    private static readonly FieldInfo ActivationCount =
        typeof(QuiclyServer).GetField("_activationCount", BindingFlags.Instance | BindingFlags.NonPublic)!;

    [Fact]
    public async Task Work_Published_Before_Activation_Survives_A_PollAll_That_Took_The_Bit_Early()
    {
        await using ServerFixture f = new();
        f.ConnectAdmitted(); // slot 0 is live, so PollAll scans the bit word the next peer's slot shares with it
        QuiclyPeer client = f.Connect();

        // Run the network (and the client) only, until the listener accepted the connection and the peer received its
        // connection and the Hello: its work signal has fired and will not fire again before a Poll.
        for (int i = 0; i < 100 && (int)ActivationCount.GetValue(f.Server)! == 0; i++)
        {
            f.Network.Advance(1_000);
            f.PumpClients();
        }

        Assert.NotEqual(0, (int)ActivationCount.GetValue(f.Server)!);
        for (int i = 0; i < 20; i++)
        {
            f.Network.Advance(1_000);
            f.PumpClients();
        }

        // The interleaving of the race: PollAll sees no activation queued, then takes the slot's bit.
        int queued = (int)ActivationCount.GetValue(f.Server)!;
        ActivationCount.SetValue(f.Server, 0);
        f.Server.PollAll();
        ActivationCount.SetValue(f.Server, queued);

        Assert.True(f.RunUntil(() => client.State == PeerState.Connected, maxMicros: 1_000_000),
            "the client was not admitted within a second: " + client.State + " " + client.CloseReason);
    }
}
