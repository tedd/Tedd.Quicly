// Purpose: reproduce a stream-credit grant that arrives inside a synchronously refused bulk start.
// Responsibilities: require a retry after one grant and verify exact delivery without a second synthetic credit event.
// Design intent: the refusal must retain the generation observed before the attempt, rather than consume a newer grant.
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

public class BulkCreditDuringRefusalTests
{
    [Fact]
    public async Task A_Grant_During_A_Refused_Start_Retries_And_Delivers_Without_A_Second_Grant()
    {
        byte[] payload = Enumerable.Range(0, 128 * 1024).Select(i => (byte)(i >> 6)).ToArray();
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        BulkRefusalConnector? connector = null;
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router),
            connector: inner => connector = new BulkRefusalConnector(inner));

        BulkRefusalTransport transport = connector!.Transport!;
        transport.FailStarts = 1;
        transport.StartStatus = TransportStatus.StreamLimitReached;
        transport.GrantCreditOnFailedStart = true;
        int before = h.Client.Core.StreamCreditGeneration;

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(
            new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        Assert.True(h.RunUntil(() => transport.FailedStarts == 1, 100_000), "the first start was not refused");
        Assert.Equal(1, transport.InjectedCreditGrants);
        Assert.Equal(before + 1, h.Client.Core.StreamCreditGeneration);
        Assert.False(transfer.IsFinished);

        // No grant is made after the failed call returns: the grant inside that call is sufficient for the next attempt.
        h.Run(200_000);
        Assert.True(transport.BulkStartAttempts >= 2,
            $"only {transport.BulkStartAttempts} start attempt(s) after the sole credit grant; the queue remained parked");
        Assert.True(h.RunUntil(() => transfer.IsFinished, 30_000_000), "the granted transfer did not drain");
        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);
        Assert.Equal(1, transport.FailedStarts);
        Assert.Equal(1, transport.InjectedCreditGrants);
        Assert.Equal(PeerState.Connected, h.Client.State);
    }
}