using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Server;

namespace Tedd.Quicly.Client.Tests;

/// <summary>
/// Defects found in the review of the client layer (branch quicly/c3-server-client). Each test states the expected
/// behaviour and fails until the finding is fixed.
/// </summary>
public class ReviewClientTests
{
    /// <summary>
    /// Disposing the client while ConnectAsync is suspended (a scene change, an application shutdown, a connect timeout
    /// implemented with Task.WhenAny) must end that connect. Dispose only disposes <see cref="QuiclyClient.Peer"/> and the
    /// reconnect attempt; the peer ConnectAsync is still driving is a local variable, and the loop never checks the flag,
    /// so the connect completes and hands out a live connection that the disposed client will never close (with the real
    /// wait the next WaitAsync on the disposed semaphore throws instead, depending on timing).
    /// </summary>
    [Fact]
    public async Task Disposing_The_Client_During_ConnectAsync_Leaves_No_Live_Connection()
    {
        await using ClientFixture f = new();
        QuiclyClient client = f.CreateClient();
        int waits = 0;
        client.WaitOverride = (_, _) =>
        {
            if (++waits == 1)
            {
                client.Dispose();
            }

            f.Step(1_000);
            return ValueTask.CompletedTask;
        };

        QuiclyPeer? peer = null;
        Exception? failure = null;
        try
        {
            peer = await client.ConnectAsync(f.EndPoint, f.Options(), TestContext.Current.CancellationToken);
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        try
        {
            Assert.IsType<ObjectDisposedException>(failure);
            for (int i = 0; i < 200 && f.Server.PeerCount != 0; i++)
            {
                f.Step(1_000);
            }

            Assert.Equal(0, f.Server.PeerCount);
        }
        finally
        {
            peer?.Dispose();
        }
    }

    /// <summary>
    /// With both sides at their defaults (the server's MinResumeInterval of 1 s and the client's first reconnect after
    /// 250 ms ± 20 %), a connection lost within a second of its admission is never resumed: the first attempt is refused
    /// as ResumeTooSoon, which is answered like a bad token (Rejected, and charged to the address), and the client's
    /// FallBackToNewSession then starts a fresh session. The session is lost although the loss happened well within the
    /// grace period (the same holds for a second loss within a second of a resume).
    /// </summary>
    [Fact]
    public async Task A_Connection_Lost_Soon_After_Admission_Is_Resumed_With_The_Default_Policies()
    {
        await using ClientFixture f = new(o => o.Admission.MinResumeInterval = new ServerAdmissionOptions().MinResumeInterval);
        ScriptedConnector connector = new ScriptedConnector(f.Connector, f.Clock).Then(f.Cutting(200_000));
        QuiclyClient client = f.CreateClient(connector);
        List<ReconnectedInfo> reconnected = [];
        client.Reconnected += (_, info) => reconnected.Add(info);
        QuiclyPeer first = await client.ConnectAsync(f.EndPoint, f.Options("alice", new ReconnectPolicy { Random = new Random(7) }), TestContext.Current.CancellationToken);
        ulong session = first.SessionId;

        Assert.True(f.RunUntil(client, () => reconnected.Count == 1 || client.State == PeerState.Closed, maxMicros: 10_000_000));
        ReconnectedInfo info = Assert.Single(reconnected);
        Assert.True(info.Resumed, "The session was not resumed; the client got a fresh session after " + info.Attempts + " attempts.");
        Assert.Equal(session, info.Peer.SessionId);
    }
}
