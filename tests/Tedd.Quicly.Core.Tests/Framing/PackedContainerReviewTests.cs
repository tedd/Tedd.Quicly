using Tedd.Quicly.Core.Framing;

namespace Tedd.Quicly.Core.Tests.Framing;

/// <summary>Review findings for <see cref="PackedContainerWriter"/> (expected to fail until fixed).</summary>
public class PackedContainerReviewTests
{
    // GetEntryLength(messageLength) = varint length + messageLength overflows int for the largest lengths, so
    // CanAppend returns true for a length that can never fit. TryReserve then writes an 8-byte length varint,
    // advances Length, and throws from Span.Slice instead of returning false (leaving a corrupt container).
    [Theory]
    [InlineData(int.MaxValue)]
    [InlineData(int.MaxValue - 7)]
    public void Review_Huge_Message_Length_Does_Not_Fit_And_Leaves_Writer_Untouched(int messageLength)
    {
        byte[] buffer = new byte[1200];
        PackedContainerWriter writer = new(buffer);

        Assert.False(writer.CanAppend(messageLength));

        bool reserved;
        try
        {
            reserved = writer.TryReserve(messageLength, out _);
        }
        catch (ArgumentOutOfRangeException)
        {
            reserved = true; // reported as a failure below together with the corrupted length
        }

        Assert.False(reserved);
        Assert.Equal(2, writer.Length);
        Assert.Equal(0, writer.Count);
    }
}
