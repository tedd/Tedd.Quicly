using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Adversarial review, RC2 round 1 of fix/stack-review (8f6c47c), lens: reconnect with compressed messages staged at their
/// decoded size and tagged in the shared half (design test 14, which the implementer left uncovered).
/// </summary>
public class ReviewRc2R1ReconnectTests
{
    private const ushort Packed = 6;
    private const ushort Groups = 7;

    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(Packed, "packed", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; })
        .Add(Groups, "groups", ChannelMode.ReliableUnordered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; })
        .Build();

    private static readonly byte[] ResumeToken = [9, 8, 7, 6];

    private static byte[] Payload(int index, int size, int noise)
    {
        byte[] payload = new byte[size];
        new Random(index + 1).NextBytes(payload.AsSpan(0, Math.Min(noise, size)));
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static List<int> DrainAll(QuiclyPeer peer, ushort channel, Dictionary<int, byte[]> sent)
    {
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        int taken;
        while ((taken = peer.Drain(channel, buffer)) > 0)
        {
            for (int i = 0; i < taken; i++)
            {
                int index = BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload);
                Assert.True(sent.TryGetValue(index, out byte[]? expected) && buffer[i].Payload.SequenceEqual(expected));
                got.Add(index);
            }

            peer.Release(buffer.AsSpan(0, taken));
        }

        return got;
    }

    /// <summary>
    /// GUARD (passes at 8f6c47c): a client with compressed messages staged at their decoded size and tagged in the shared
    /// half on two drained channels reconnects; the shared half and the receive budget are back at 0, and the resumed
    /// connection delivers forty more such messages intact and in order with nothing left counted.
    /// </summary>
    [Fact]
    public void GUARD_A_Reconnect_With_Tagged_Messages_Waiting_Leaves_No_Shared_Half_Or_Budget_Behind()
    {
        using SessionHarness h = new(connect: false, table: Table, server: o =>
        {
            GroupKit.Prompt(o);
            OrderedKit.Roomy(o);
        }, client: GroupKit.Prompt);
        h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
            AdmissionResult.Accept(ResumeToken, 4242, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer client = h.Client;
        QuiclyPeer oldServer = h.Server!;
        Dictionary<int, byte[]> sent = new();
        for (int i = 0; i < 2; i++)
        {
            h.Run(20_000);
            Assert.Empty(DrainAll(client, Packed, sent));
            Assert.Empty(DrainAll(client, Groups, sent));
        }

        for (int i = 0; i < 3; i++)
        {
            sent[i] = Payload(i, 12_000, 2_000);
            sent[100 + i] = Payload(100 + i, 12_000, 2_000);
            Assert.Equal(SendStatus.Admitted, oldServer.SendCopy(new SendHeader(Packed), sent[i]).Status);
            Assert.Equal(SendStatus.Admitted, oldServer.SendCopy(new SendHeader(Groups), sent[100 + i]).Status);
        }

        Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(client, Packed).Received == 3 && DatagramKit.ChannelStats(client, Groups).Received == 3));
        Assert.True(client.Core.Credit.SharedWaitingBytes > 0);

        oldServer.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);
        Assert.True(h.RunUntil(() => client.State == PeerState.Closed));
        client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.Equal(0, client.Core.Credit.SharedWaitingBytes);
        Assert.Equal(0, client.Core.ReceiveBytesOutstanding);
        Assert.True(h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();

        QuiclyPeer server = h.Server!;
        List<int> gotPacked = [];
        List<int> gotGroups = [];
        for (int i = 0; i < 20; i++)
        {
            sent[1_000 + i] = Payload(1_000 + i, 12_000, 2_000);
            sent[2_000 + i] = Payload(2_000 + i, 12_000, 2_000);
            Assert.Equal(SendStatus.Admitted, server.SendCopy(new SendHeader(Packed), sent[1_000 + i]).Status);
            Assert.Equal(SendStatus.Admitted, server.SendCopy(new SendHeader(Groups), sent[2_000 + i]).Status);
        }

        Assert.True(h.RunUntil(() =>
        {
            gotPacked.AddRange(DrainAll(client, Packed, sent));
            gotGroups.AddRange(DrainAll(client, Groups, sent));
            return gotPacked.Count == 20 && gotGroups.Count == 20;
        }), $"packed {gotPacked.Count}, groups {gotGroups.Count}");
        Assert.Equal(Enumerable.Range(1_000, 20), gotPacked);
        Assert.Equal(0, client.Core.Credit.SharedWaitingBytes);
        Assert.Equal(0, client.Core.ReceiveBytesOutstanding);
        Assert.Equal(0, DatagramKit.Statistics(client).DecodeFailures);
    }
}
