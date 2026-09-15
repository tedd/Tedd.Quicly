using Tedd.Quicly.Http3.Qpack;

namespace Tedd.Quicly.Http3.Tests;

public class HpackHuffmanTests
{
    [Theory]
    [InlineData("www.example.com", "f1e3c2e5f23a6ba0ab90f4ff")]
    [InlineData("no-cache", "a8eb10649cbf")]
    [InlineData("custom-key", "25a849e95ba97d7f")]
    [InlineData("custom-value", "25a849e95bb8e8b4bf")]
    [InlineData("302", "6402")]
    [InlineData("private", "aec3771a4b")]
    [InlineData("Mon, 21 Oct 2013 20:13:21 GMT", "d07abe941054d444a8200595040b8166e082a62d1bff")]
    [InlineData("https://www.example.com", "9d29ad171863c78f0b97c8e9ae82ae43d3")]
    [InlineData("foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1", "94e7821dd7f2e6c7b335dfdfcd5b3960d5af27087f3672c1ab270fb5291f9587316065c003ed4ee5b1063d5007")]
    public void Rfc7541_Appendix_C_Vectors(string text, string hex)
    {
        byte[] plain = TestUtil.Ascii(text);
        byte[] expected = TestUtil.Hex(hex);

        Assert.Equal(expected.Length, HpackHuffman.GetEncodedLength(plain));
        Span<byte> enc = stackalloc byte[64];
        int n = HpackHuffman.Encode(plain, enc);
        Assert.Equal(expected.Length, n);
        Assert.Equal(hex, TestUtil.ToHex(enc.Slice(0, n)));

        Span<byte> dec = stackalloc byte[64];
        int m = HpackHuffman.Decode(expected, dec);
        Assert.Equal(plain.Length, m);
        Assert.Equal(text, TestUtil.AsciiString(dec.Slice(0, m)));
    }

    [Fact]
    public void Every_Symbol_Round_Trips_Alone_And_In_Sequence()
    {
        Span<byte> enc = stackalloc byte[8];
        Span<byte> dec = stackalloc byte[8];
        for (int s = 0; s < 256; s++)
        {
            byte[] one = [(byte)s];
            int n = HpackHuffman.Encode(one, enc);
            Assert.Equal(HpackHuffman.GetEncodedLength(one), n);
            Assert.Equal(1, HpackHuffman.Decode(enc.Slice(0, n), dec));
            Assert.Equal((byte)s, dec[0]);
        }

        byte[] all = new byte[256];
        for (int i = 0; i < 256; i++) all[i] = (byte)i;
        byte[] encAll = new byte[HpackHuffman.GetEncodedLength(all)];
        Assert.Equal(encAll.Length, HpackHuffman.Encode(all, encAll));
        byte[] decAll = new byte[256];
        Assert.Equal(256, HpackHuffman.Decode(encAll, decAll));
        Assert.Equal(all, decAll);
    }

    [Fact]
    public void Empty_Input_Round_Trips()
    {
        Assert.Equal(0, HpackHuffman.GetEncodedLength(ReadOnlySpan<byte>.Empty));
        Assert.Equal(0, HpackHuffman.Encode(ReadOnlySpan<byte>.Empty, Span<byte>.Empty));
        Assert.Equal(0, HpackHuffman.Decode(ReadOnlySpan<byte>.Empty, Span<byte>.Empty));
    }

    [Fact]
    public void Random_Round_Trips()
    {
        var rng = new Random(99);
        byte[] plain = new byte[512];
        byte[] enc = new byte[2048];
        byte[] dec = new byte[512];
        for (int iter = 0; iter < 2000; iter++)
        {
            int len = rng.Next(0, 512);
            Span<byte> p = plain.AsSpan(0, len);
            if (iter % 2 == 0)
            {
                rng.NextBytes(p);
            }
            else
            {
                for (int i = 0; i < len; i++) p[i] = (byte)rng.Next(0x20, 0x7f); // printable ASCII (short codes)
            }
            int n = HpackHuffman.Encode(p, enc);
            Assert.Equal(HpackHuffman.GetEncodedLength(p), n);
            int m = HpackHuffman.Decode(enc.AsSpan(0, n), dec);
            Assert.Equal(len, m);
            Assert.True(p.SequenceEqual(dec.AsSpan(0, m)));
        }
    }

    [Fact]
    public void Eos_In_String_Is_Invalid()
    {
        // 'a' (00011, 5 bits) followed by EOS (30 ones) = 35 bits -> 5 bytes: 00011111 11111111 11111111 11111111 111xxxxx
        byte[] withEos = [0x1f, 0xff, 0xff, 0xff, 0xff];
        Assert.Equal(HpackHuffman.InvalidInput, HpackHuffman.Decode(withEos, stackalloc byte[8]));
        // EOS alone.
        Assert.Equal(HpackHuffman.InvalidInput, HpackHuffman.Decode(new byte[] { 0xff, 0xff, 0xff, 0xff }, stackalloc byte[8]));
    }

    [Fact]
    public void Padding_Longer_Than_Seven_Bits_Is_Invalid()
    {
        // 'a' = 00011 + 3 bits pad = 0x1f; then a full byte of 1s = 11 bits of padding.
        Assert.Equal(HpackHuffman.InvalidInput, HpackHuffman.Decode(new byte[] { 0x1f, 0xff }, stackalloc byte[8]));
        // Exactly 8 padding bits ('0' = 00000 is 5 bits; 000 + 8 ones -> 2 bytes = 0x07, 0xff)
        Assert.Equal(HpackHuffman.InvalidInput, HpackHuffman.Decode(new byte[] { 0x07, 0xff }, stackalloc byte[8]));
        // Seven padding bits is fine: '0' (00000) + 0000000? No: 5 bits + 7 pad = 12 bits -> use "00" = 10 bits + 6 pad.
        Assert.Equal(2, HpackHuffman.Decode(new byte[] { 0x00, 0x3f }, stackalloc byte[8]));
    }

    [Fact]
    public void Padding_Not_All_Ones_Is_Invalid()
    {
        // 'a' = 00011, pad with 000 instead of 111.
        Assert.Equal(HpackHuffman.InvalidInput, HpackHuffman.Decode(new byte[] { 0x18 }, stackalloc byte[8]));
        // 'a' padded 110.
        Assert.Equal(HpackHuffman.InvalidInput, HpackHuffman.Decode(new byte[] { 0x1e }, stackalloc byte[8]));
        // 'a' padded 111 is valid.
        Assert.Equal(1, HpackHuffman.Decode(new byte[] { 0x1f }, stackalloc byte[8]));
    }

    [Fact]
    public void Truncated_Code_Is_Invalid()
    {
        // 11111110 is a prefix of the 10-bit code 1111111000 ('!'): the string ends inside a code, off the EOS path.
        Assert.Equal(HpackHuffman.InvalidInput, HpackHuffman.Decode(new byte[] { 0xfe }, stackalloc byte[8]));
        // 'w' (1111000) followed by a single 1 bit is valid padding.
        Assert.Equal(1, HpackHuffman.Decode(new byte[] { 0xf1 }, stackalloc byte[8]));
        // 'w' followed by a 0 bit is not.
        Assert.Equal(HpackHuffman.InvalidInput, HpackHuffman.Decode(new byte[] { 0xf0 }, stackalloc byte[8]));
        // 12 ones then 0000: '^' (11111111111100) followed by "00" padding -> invalid.
        Assert.Equal(HpackHuffman.InvalidInput, HpackHuffman.Decode(new byte[] { 0xff, 0xf0 }, stackalloc byte[8]));
    }

    [Fact]
    public void Destination_Too_Small()
    {
        byte[] enc = TestUtil.Hex("f1e3c2e5f23a6ba0ab90f4ff");
        for (int capacity = 0; capacity < 15; capacity++)
        {
            Assert.Equal(HpackHuffman.DestinationTooSmall, HpackHuffman.Decode(enc, new byte[capacity]));
        }
        Assert.Equal(15, HpackHuffman.Decode(enc, new byte[15]));

        byte[] plain = TestUtil.Ascii("www.example.com");
        Assert.Equal(HpackHuffman.DestinationTooSmall, HpackHuffman.Encode(plain, stackalloc byte[11]));
        Assert.Equal(HpackHuffman.DestinationTooSmall, HpackHuffman.Encode(plain, Span<byte>.Empty));
        // Final partial byte does not fit: 'a' needs 1 byte.
        Assert.Equal(HpackHuffman.DestinationTooSmall, HpackHuffman.Encode(TestUtil.Ascii("a"), Span<byte>.Empty));
    }

    [Fact]
    public void Long_Codes_Round_Trip_Across_Byte_Boundaries()
    {
        // Mix of 30-bit, 28-bit and 5-bit codes to exercise the accumulator.
        byte[] plain = [10, 13, 22, 0, 1, 2, 48, 49, 127, 255, 128, 200, 32, 10, 48];
        byte[] enc = new byte[64];
        int n = HpackHuffman.Encode(plain, enc);
        byte[] dec = new byte[64];
        int m = HpackHuffman.Decode(enc.AsSpan(0, n), dec);
        Assert.Equal(plain.Length, m);
        Assert.Equal(plain, dec.AsSpan(0, m).ToArray());
    }
}
