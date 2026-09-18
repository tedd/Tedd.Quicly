using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The packer releases the send budget of a container member's payload lease when it copies the member in, and returns
/// the blocks of the members together when the container closes (<see cref="DatagramPacker.ReturnCopied"/>,
/// <see cref="SlabAllocator.ReturnMany"/>): the send budget must look exactly as if each lease had been returned when it
/// was copied, and the pool too once the pass is over (the budget within a pass:
/// <c>ReviewPerfContractTests.A_Bulk_Piece_Fits_The_Send_Budget_Next_To_A_Container_Of_Datagrams_Sent_In_The_Same_Pass</c>).
/// </summary>
public class PackedPayloadReturnTests
{
    private static readonly ChannelTable Table = DatagramTables.Main;

    [Fact]
    public void Member_Payloads_Are_Back_In_The_Pool_When_The_Pass_Ends_And_The_Budget_Holds_Only_Containers()
    {
        SlabAllocatorOptions pool = new()
        {
            FreeListShards = 1,
            ValidateLeases = true,
            SizeClasses = [new(64, 512), new(256, 64), new(1536, 64), new(4096, 8)],
        };
        using SessionHarness h = new(table: Table, client: o =>
        {
            DatagramKit.Quiet(o);
            o.AllocatorOptions = pool;
        }, server: DatagramKit.Quiet);
        int received = 0;
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            int id = BitConverter.ToInt32(payload);
            Assert.True(payload.SequenceEqual(DatagramKit.Payload(id, payload.Length)));
            received++;
        });
        SlabAllocator allocator = h.Client.Core.Allocator;
        int next = 0;
        for (int round = 0; round < 5; round++)
        {
            for (int i = 0; i < 150; i++)
            {
                Assert.True(h.Client.SendCopy(new SendHeader(2), DatagramKit.Payload(next++, 20 + (i % 40))).IsAdmitted);
            }

            Assert.Equal(150, allocator.GetClassStatistics(0).Rented);
            h.Client.Flush();

            // Every member was copied into a container, so only the containers' leases are held.
            Assert.Equal(0, allocator.GetClassStatistics(0).Rented);
            int containers = allocator.GetClassStatistics(2).Rented;
            Assert.True(containers >= 3, $"{containers} containers");
            Assert.Equal(containers * 1536L, h.Client.Core.SendBytesOutstanding);
            Assert.True(h.RunUntil(() => received == next, 1_000_000));
        }

        Assert.True(h.RunUntil(() => h.Client.Core.SendBytesOutstanding == 0, 1_000_000));
        Assert.Equal(0, allocator.GetStatistics().TotalRentedBytes);
    }
}
