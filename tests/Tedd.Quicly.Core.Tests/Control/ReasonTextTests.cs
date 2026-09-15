using System.Globalization;
using System.Text;
using Tedd.Quicly.Core.Control;
using static Tedd.Quicly.Core.Tests.Control.ControlTestData;

namespace Tedd.Quicly.Core.Tests.Control;

public class ReasonTextTests
{
    // Strings are assembled from code points so the source file stays pure ASCII (U+2028/U+2029 would end a literal).
    private const int Rep = 0xFFFD;

    /// <summary>Concatenates strings and code points (ints).</summary>
    private static string S(params object[] parts)
    {
        StringBuilder builder = new();
        foreach (object part in parts)
        {
            builder.Append(part is int codePoint ? char.ConvertFromUtf32(codePoint) : (string)part);
        }

        return builder.ToString();
    }

    private static string Sanitize(byte[] reason, int? capacity = null)
    {
        char[] destination = new char[capacity ?? reason.Length];
        int written = ControlCodec.CopySanitizedReason(reason, destination);
        return new string(destination, 0, written);
    }

    [Fact]
    public void Plain_Text_Passes_Through()
    {
        Assert.Equal("peer closed: ok", Sanitize(Utf8Bytes("peer closed: ok")));
        string international = S("bl", 0xE5, "b", 0xE6, "r ", 0x4E2D, 0x6587, " ", 0x1F600);
        Assert.Equal(international, Sanitize(Utf8Bytes(international)));
        Assert.Equal("", Sanitize([]));
    }

    public static TheoryData<string, string> UnsafeCharacters => new()
    {
        { S("a", 0x0A, "b"), S("a", Rep, "b") },
        { S(0x0D, 0x0A), S(Rep, Rep) },
        { S(0x09, 0x00, 0x1B, "[31m"), S(Rep, Rep, Rep, "[31m") },
        { S("x", 0x7F, "y"), S("x", Rep, "y") },
        { S("x", 0x85, "y"), S("x", Rep, "y") },
        { S("abc", 0x202E, "dcba"), S("abc", Rep, "dcba") },
        { S("a", 0x2066, "b", 0x2069), S("a", Rep, "b", Rep) },
        { S("line", 0x2028, "para", 0x2029), S("line", Rep, "para", Rep) },
        { S("zero", 0x200B, "width"), S("zero", Rep, "width") },
    };

    [Theory]
    [MemberData(nameof(UnsafeCharacters))]
    public void Control_Format_And_Separator_Characters_Are_Replaced(string input, string expected) =>
        Assert.Equal(expected, Sanitize(Utf8Bytes(input)));

    public static TheoryData<byte[], string> InvalidUtf8 => new()
    {
        { new byte[] { 0xFF }, S(Rep) },
        { new byte[] { 0x61, 0xC3 }, S("a", Rep) },
        { new byte[] { 0xE2, 0x82 }, S(Rep) },
        { new byte[] { 0xC0, 0x80, 0x62 }, S(Rep, Rep, "b") },
        { new byte[] { 0xED, 0xA0, 0x80 }, S(Rep, Rep, Rep) },
    };

    [Theory]
    [MemberData(nameof(InvalidUtf8))]
    public void Invalid_Utf8_Is_Replaced(byte[] input, string expected) => Assert.Equal(expected, Sanitize(input));

    [Fact]
    public void Output_Is_Truncated_At_A_Character_Boundary()
    {
        byte[] reason = Utf8Bytes(S("ab", 0x1F600, "c"));
        Assert.Equal("ab", Sanitize(reason, 2));
        Assert.Equal("ab", Sanitize(reason, 3)); // the surrogate pair does not fit in one char
        Assert.Equal(S("ab", 0x1F600), Sanitize(reason, 4));
        Assert.Equal("", Sanitize(reason, 0));
    }

    [Fact]
    public void Destination_Of_Reason_Length_Always_Suffices_And_Output_Is_Safe()
    {
        Random random = new(5);
        for (int i = 0; i < 5000; i++)
        {
            byte[] reason = new byte[random.Next(0, 64)];
            random.NextBytes(reason);
            char[] destination = new char[reason.Length + 8];
            int written = ControlCodec.CopySanitizedReason(reason, destination);
            Assert.InRange(written, 0, reason.Length);
            Assert.Equal(written, ControlCodec.CopySanitizedReason(reason, destination.AsSpan(0, reason.Length)));
            string text = new(destination, 0, written);
            foreach (Rune rune in text.EnumerateRunes())
            {
                UnicodeCategory category = Rune.GetUnicodeCategory(rune);
                Assert.False(category is UnicodeCategory.Control or UnicodeCategory.Format or UnicodeCategory.LineSeparator or UnicodeCategory.ParagraphSeparator);
            }

            if (ControlCodec.IsValidReason(reason))
            {
                Assert.Equal(Encoding.UTF8.GetString(reason).Length, written);
            }
        }
    }

    [Fact]
    public void IsValidReason_Bounds()
    {
        string twoByte = new((char)0xE6, 256); // 512 bytes of 2-byte UTF-8
        Assert.True(ControlCodec.IsValidReason([]));
        Assert.True(ControlCodec.IsValidReason(Bytes(512, (byte)'a')));
        Assert.False(ControlCodec.IsValidReason(Bytes(513, (byte)'a')));
        Assert.False(ControlCodec.IsValidReason([0xC3]));
        Assert.True(ControlCodec.IsValidReason(Utf8Bytes(twoByte)));
        Assert.False(ControlCodec.IsValidReason(Utf8Bytes(twoByte + "a")));
    }
}
