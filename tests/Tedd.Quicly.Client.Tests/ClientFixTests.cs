using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Client.Tests;

/// <summary>Fixes of the client review follow-up that the review tests do not pin down on their own.</summary>
public class ClientFixTests
{
    [Fact]
    public async Task Disposing_The_Client_While_ConnectAsync_Sleeps_In_The_Work_Signal_Ends_The_Connect()
    {
        await using ClientFixture f = new();
        QuiclyClient client = new(f.Unreachable()); // the network is never advanced: no callback wakes the connect
        try
        {
            Task<QuiclyPeer> connecting = client.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken).AsTask();
            Assert.False(connecting.IsCompleted);
            client.Dispose(); // cancels the wait at once instead of letting it sleep out its timeout
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connecting);
            Assert.Equal(PeerState.Closed, client.State);
            Assert.Null(client.Peer);
        }
        finally
        {
            client.Dispose();
        }
    }
}
