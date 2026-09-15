using Tedd.Quicly.Http3.Qpack;

namespace Tedd.Quicly.Http3.Tests;

public class QpackDecoderTests
{
    private static Http3HeaderCollection NewHeaders() => new(1024, 16);

    private static QpackDecodeStatus Decode(string hex, Http3HeaderCollection h, int max = 0) => QpackDecoder.Decode(TestUtil.Hex(hex), h, max);

    [Fact]
    public void Indexed_Static_Field_Line()
    {
        var h = NewHeaders();
        // prefix 00 00, :method GET (17) = 0xD1, :path / (1) = 0xC1, :status 200 (25) = 0xD9
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 d1 c1 d9", h));
        Assert.Equal(3, h.Count);
        Assert.Equal("GET", TestUtil.AsciiString(h.GetValue(0)));
        Assert.Equal(":path", TestUtil.AsciiString(h.GetName(1)));
        Assert.Equal("/", TestUtil.AsciiString(h.GetValue(1)));
        Assert.Equal("200", TestUtil.AsciiString(h.GetValue(2)));
        // Index 98 uses a continuation byte: 0xFF, 0x23 (63 + 35).
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 ff23", h));
        Assert.Equal("x-frame-options", TestUtil.AsciiString(h.GetName(0)));
        Assert.Equal("sameorigin", TestUtil.AsciiString(h.GetValue(0)));
    }

    [Fact]
    public void Literal_With_Static_Name_Reference_Raw_And_Huffman()
    {
        var h = NewHeaders();
        // 0x51 = 0101 0001 -> N=0, T=1, index 1 (:path); raw value "/foo"
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 51 04 2f666f6f", h));
        Assert.Equal(1, h.Count);
        Assert.Equal(":path", TestUtil.AsciiString(h.GetName(0)));
        Assert.Equal("/foo", TestUtil.AsciiString(h.GetValue(0)));
        // Huffman value: 0x83 + 62539f
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 51 83 62539f", h));
        Assert.Equal("/foo", TestUtil.AsciiString(h.GetValue(0)));
        // N bit set (0x71) is tolerated.
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 71 04 2f666f6f", h));
        Assert.Equal("/foo", TestUtil.AsciiString(h.GetValue(0)));
        // Index needing a continuation byte: 0x5f 0x00 = 15 (:method CONNECT name) with value "PATCH".
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 5f00 05 5041544348", h));
        Assert.Equal(":method", TestUtil.AsciiString(h.GetName(0)));
        Assert.Equal("PATCH", TestUtil.AsciiString(h.GetValue(0)));
    }

    [Fact]
    public void Literal_With_Literal_Name_Raw_And_Huffman()
    {
        var h = NewHeaders();
        // 0x23 = 001 0 0 011 -> N=0 H=0 len 3 "x-a"; value raw len 1 "b"
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 23 782d61 01 62", h));
        Assert.Equal("x-a", TestUtil.AsciiString(h.GetName(0)));
        Assert.Equal("b", TestUtil.AsciiString(h.GetValue(0)));
        // Huffman name: "no-cache" = a8eb10649cbf (6 bytes): 0x2e = 001 0 1 110; Huffman value "custom-value" 25a849e95bb8e8b4bf (9 bytes): 0x89
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 2e a8eb10649cbf 89 25a849e95bb8e8b4bf", h));
        Assert.Equal("no-cache", TestUtil.AsciiString(h.GetName(0)));
        Assert.Equal("custom-value", TestUtil.AsciiString(h.GetValue(0)));
        // N bit set (0x33) tolerated; name length using continuation (7 + 1 = 8 bytes "x-custom").
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 37 01 782d637573746f6d 00", h));
        Assert.Equal("x-custom", TestUtil.AsciiString(h.GetName(0)));
        Assert.True(h.GetValue(0).IsEmpty);
    }

    [Theory]
    [InlineData("0000 80")]        // indexed, dynamic (T=0)
    [InlineData("0000 bf01")]      // indexed, dynamic with continuation
    [InlineData("0000 40 01 62")]  // literal with dynamic name reference
    [InlineData("0000 60 01 62")]  // literal with dynamic name reference, N bit
    [InlineData("0000 10")]        // indexed post-base
    [InlineData("0000 1f")]        // indexed post-base
    [InlineData("0000 00 01 62")]  // literal with post-base name reference
    [InlineData("0000 08 01 62")]  // literal with post-base name reference, N bit
    public void Dynamic_Table_References_Are_Rejected(string hex)
    {
        Assert.Equal(QpackDecodeStatus.DynamicTableReference, Decode(hex, NewHeaders()));
    }

    [Theory]
    [InlineData("0100", QpackDecodeStatus.RequiredInsertCountNotZero)]
    [InlineData("ff0100", QpackDecodeStatus.RequiredInsertCountNotZero)]
    [InlineData("0080", QpackDecodeStatus.InvalidBase)]
    [InlineData("0001", QpackDecodeStatus.InvalidBase)]
    [InlineData("0081", QpackDecodeStatus.InvalidBase)]
    public void Prefix_Must_Be_Zero(string hex, QpackDecodeStatus expected)
    {
        Assert.Equal(expected, Decode(hex, NewHeaders()));
    }

    [Theory]
    [InlineData("")]                   // no prefix
    [InlineData("00")]                 // no base
    [InlineData("00ff")]               // base integer continuation missing
    [InlineData("ff")]                 // RIC integer continuation missing
    [InlineData("0000 ff")]            // indexed: continuation missing
    [InlineData("0000 5f")]            // name ref: continuation missing
    [InlineData("0000 51")]            // name ref: value missing
    [InlineData("0000 51 7f")]         // name ref: value length continuation missing
    [InlineData("0000 51 04 2f66")]    // name ref: value bytes missing
    [InlineData("0000 27")]            // literal name: length continuation missing
    [InlineData("0000 23 782d")]       // literal name: name bytes missing
    [InlineData("0000 23 782d61")]     // literal name: value missing
    [InlineData("0000 23 782d61 05 6162")] // literal name: value bytes missing
    public void Truncated_Inputs(string hex)
    {
        Assert.Equal(QpackDecodeStatus.Truncated, Decode(hex, NewHeaders()));
    }

    [Theory]
    [InlineData("ff 80808080808080808001 00")]        // RIC overflow
    [InlineData("00 ff80808080808080808001")]         // base overflow
    [InlineData("0000 ff ffffffffffffffff7f")]        // indexed index overflow
    [InlineData("0000 5f ffffffffffffffff7f")]        // name ref index overflow
    [InlineData("0000 51 7f ffffffffffffffff7f")]     // value length overflow
    [InlineData("0000 27 ffffffffffffffff7f")]        // name length overflow
    public void Malformed_Integers(string hex)
    {
        Assert.Equal(QpackDecodeStatus.MalformedInteger, Decode(hex, NewHeaders()));
    }

    [Theory]
    [InlineData("0000 ff24")]    // 63 + 36 = 99
    [InlineData("0000 5f54 00")] // 15 + 84 = 99
    public void Invalid_Static_Index(string hex)
    {
        Assert.Equal(QpackDecodeStatus.InvalidStaticIndex, Decode(hex, NewHeaders()));
    }

    [Theory]
    [InlineData("0000 29 18 00")]       // Huffman name with bad padding
    [InlineData("0000 51 81 18")]       // Huffman value with bad padding
    [InlineData("0000 23 782d61 81 18")] // Huffman value after literal name
    [InlineData("0000 51 84 ffffffff")] // EOS in value
    public void Invalid_Huffman(string hex)
    {
        Assert.Equal(QpackDecodeStatus.InvalidHuffman, Decode(hex, NewHeaders()));
    }

    [Fact]
    public void Field_Section_Size_Limit()
    {
        var h = NewHeaders();
        // :method GET = 7 + 3 + 32 = 42
        Assert.Equal(QpackDecodeStatus.FieldSectionTooLarge, Decode("0000 d1", h, 41));
        Assert.Equal(0, h.Count);
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 d1", h, 42));
        Assert.Equal(QpackDecodeStatus.FieldSectionTooLarge, Decode("0000 d1 d1", h, 42));
        Assert.Equal(1, h.Count);
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 d1 d1", h, 84));
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 d1 d1", h, 0));
        Assert.Equal(84, h.FieldSectionSize);
    }

    [Fact]
    public void Insufficient_Buffer()
    {
        // :method GET needs 10 bytes.
        Assert.Equal(QpackDecodeStatus.InsufficientBuffer, Decode("0000 d1", new Http3HeaderCollection(9, 4)));
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000 d1", new Http3HeaderCollection(10, 4)));
        // name ref: :path (5) does not fit in 4.
        Assert.Equal(QpackDecodeStatus.InsufficientBuffer, Decode("0000 51 04 2f666f6f", new Http3HeaderCollection(4, 4)));
        // name fits, raw value does not.
        Assert.Equal(QpackDecodeStatus.InsufficientBuffer, Decode("0000 51 04 2f666f6f", new Http3HeaderCollection(8, 4)));
        // name fits, Huffman value does not.
        Assert.Equal(QpackDecodeStatus.InsufficientBuffer, Decode("0000 51 83 62539f", new Http3HeaderCollection(8, 4)));
        // literal name does not fit.
        Assert.Equal(QpackDecodeStatus.InsufficientBuffer, Decode("0000 23 782d61 01 62", new Http3HeaderCollection(2, 4)));
        // literal Huffman name does not fit.
        Assert.Equal(QpackDecodeStatus.InsufficientBuffer, Decode("0000 2e a8eb10649cbf 00", new Http3HeaderCollection(7, 4)));
    }

    [Fact]
    public void Too_Many_Headers()
    {
        var h = new Http3HeaderCollection(64, 1);
        Assert.Equal(QpackDecodeStatus.TooManyHeaders, Decode("0000 d1 d1", h));
        Assert.Equal(1, h.Count);
    }

    [Fact]
    public void Empty_Section_Is_Ok_And_Clears_Previous_Content()
    {
        var h = NewHeaders();
        Assert.True(h.TryAdd("a"u8, "b"u8));
        Assert.Equal(QpackDecodeStatus.Ok, Decode("0000", h));
        Assert.Equal(0, h.Count);
    }

    [Fact]
    public void Null_Headers_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => QpackDecoder.Decode(new byte[] { 0, 0 }, null!));
    }

    [Fact]
    public void Prefixes_Of_A_Valid_Block_Are_Ok_Or_Truncated()
    {
        Http3HeaderCollection src = TestUtil.ChromeConnectHeaders();
        byte[] block = new byte[1024];
        int n = QpackEncoder.Encode(src, block);
        Assert.True(n > 0);
        var h = NewHeaders();
        for (int len = 0; len < n; len++)
        {
            QpackDecodeStatus s = QpackDecoder.Decode(block.AsSpan(0, len), h);
            Assert.True(s == QpackDecodeStatus.Ok || s == QpackDecodeStatus.Truncated, $"len {len}: {s}");
            if (s == QpackDecodeStatus.Ok) Assert.True(h.Count < src.Count);
        }
        Assert.Equal(QpackDecodeStatus.Ok, QpackDecoder.Decode(block.AsSpan(0, n), h));
        TestUtil.AssertSameHeaders(src, h);
    }

    [Theory]
    [InlineData(QpackDecodeStatus.Ok, Http3ErrorCode.NoError)]
    [InlineData(QpackDecodeStatus.FieldSectionTooLarge, Http3ErrorCode.ExcessiveLoad)]
    [InlineData(QpackDecodeStatus.InsufficientBuffer, Http3ErrorCode.ExcessiveLoad)]
    [InlineData(QpackDecodeStatus.TooManyHeaders, Http3ErrorCode.ExcessiveLoad)]
    [InlineData(QpackDecodeStatus.Truncated, Http3ErrorCode.QpackDecompressionFailed)]
    [InlineData(QpackDecodeStatus.MalformedInteger, Http3ErrorCode.QpackDecompressionFailed)]
    [InlineData(QpackDecodeStatus.RequiredInsertCountNotZero, Http3ErrorCode.QpackDecompressionFailed)]
    [InlineData(QpackDecodeStatus.InvalidBase, Http3ErrorCode.QpackDecompressionFailed)]
    [InlineData(QpackDecodeStatus.DynamicTableReference, Http3ErrorCode.QpackDecompressionFailed)]
    [InlineData(QpackDecodeStatus.InvalidStaticIndex, Http3ErrorCode.QpackDecompressionFailed)]
    [InlineData(QpackDecodeStatus.InvalidHuffman, Http3ErrorCode.QpackDecompressionFailed)]
    public void Status_Maps_To_Error_Code(QpackDecodeStatus status, Http3ErrorCode expected)
    {
        Assert.Equal(expected, status.ToErrorCode());
    }
}
