using Tedd.Quicly.Http3.Qpack;

namespace Tedd.Quicly.Http3.Tests;

public class QpackEncoderTests
{
    private static string EncodeOne(ReadOnlySpan<byte> name, ReadOnlySpan<byte> value, bool huffman = true, bool neverIndex = false)
    {
        Span<byte> dst = stackalloc byte[256];
        var e = new QpackEncoder(dst, huffman);
        Assert.True(e.TryWrite(name, value, neverIndex));
        Assert.False(e.HasFailed);
        Assert.Equal(huffman, e.UseHuffman);
        return TestUtil.ToHex(dst.Slice(0, e.BytesWritten));
    }

    [Fact]
    public void Prefix_Is_Zero_Zero()
    {
        Span<byte> dst = stackalloc byte[8];
        var e = new QpackEncoder(dst);
        Assert.Equal(2, e.BytesWritten);
        Assert.Equal(QpackEncoder.PrefixLength, e.BytesWritten);
        Assert.Equal(0, dst[0]);
        Assert.Equal(0, dst[1]);
        Assert.False(e.HasFailed);
    }

    [Fact]
    public void Exact_Static_Match_Is_Indexed()
    {
        Assert.Equal("0000d1", EncodeOne(":method"u8, "GET"u8));
        Assert.Equal("0000c1", EncodeOne(":path"u8, "/"u8));
        Assert.Equal("0000ff23", EncodeOne("x-frame-options"u8, "sameorigin"u8)); // index 98 = 63 + 35
        Assert.Equal("0000d1", EncodeOne(":method"u8, "GET"u8, neverIndex: true)); // N bit does not apply to indexed lines
    }

    [Fact]
    public void Name_Match_Uses_Static_Name_Reference()
    {
        Assert.Equal("00005183" + "62539f", EncodeOne(":path"u8, "/foo"u8));               // Huffman shorter
        Assert.Equal("00005104" + "2f666f6f", EncodeOne(":path"u8, "/foo"u8, huffman: false));
        Assert.Equal("00007104" + "2f666f6f", EncodeOne(":path"u8, "/foo"u8, huffman: false, neverIndex: true));
        Assert.Equal("00005101" + "21", EncodeOne(":path"u8, "!"u8)); // Huffman not shorter -> raw even when enabled
        Assert.Equal("00005f00" + "05" + "5041544348", EncodeOne(":method"u8, "PATCH"u8, huffman: false)); // index 15 needs continuation
    }

    [Fact]
    public void Unknown_Name_Uses_Literal_Name()
    {
        Assert.Equal("0000" + "23" + "782d61" + "01" + "62", EncodeOne("x-a"u8, "b"u8));
        Assert.Equal("0000" + "33" + "782d61" + "01" + "62", EncodeOne("x-a"u8, "b"u8, neverIndex: true));
        // Huffman name and value: "no-cache" is a static value but not a static name -> literal name.
        Assert.Equal("0000" + "2e" + "a8eb10649cbf" + "89" + "25a849e95bb8e8b4bf", EncodeOne("no-cache"u8, "custom-value"u8));
        // Name length continuation (8 bytes raw).
        Assert.Equal("0000" + "2701" + "782d637573746f6d" + "00", EncodeOne("x-custom"u8, ""u8, huffman: false));
    }

    [Fact]
    public void Round_Trips_Realistic_Header_Sets()
    {
        foreach (bool huffman in new[] { true, false })
        {
            foreach (Http3HeaderCollection src in new[] { TestUtil.ChromeConnectHeaders(), TestUtil.ResponseHeaders() })
            {
                byte[] block = new byte[2048];
                int n = QpackEncoder.Encode(src, block, huffman);
                Assert.True(n > 2);
                var dst = new Http3HeaderCollection(2048, 32);
                Assert.Equal(QpackDecodeStatus.Ok, QpackDecoder.Decode(block.AsSpan(0, n), dst));
                TestUtil.AssertSameHeaders(src, dst);
            }
        }
    }

    [Fact]
    public void Huffman_Output_Is_Never_Longer_Than_Raw()
    {
        Http3HeaderCollection src = TestUtil.ChromeConnectHeaders();
        byte[] a = new byte[2048];
        byte[] b = new byte[2048];
        int huff = QpackEncoder.Encode(src, a, true);
        int raw = QpackEncoder.Encode(src, b, false);
        Assert.True(huff < raw);
    }

    [Fact]
    public void Random_Round_Trips()
    {
        var rng = new Random(5);
        byte[] block = new byte[16384];
        var src = new Http3HeaderCollection(8192, 64);
        var dst = new Http3HeaderCollection(8192, 64);
        for (int iter = 0; iter < 300; iter++)
        {
            src.Clear();
            int count = rng.Next(0, 20);
            for (int i = 0; i < count; i++)
            {
                byte[] name;
                byte[] value;
                if (rng.Next(3) == 0)
                {
                    QpackStaticTable.TryGetEntry(rng.Next(QpackStaticTable.Count), out ReadOnlySpan<byte> sn, out ReadOnlySpan<byte> sv);
                    name = sn.ToArray();
                    value = rng.Next(2) == 0 ? sv.ToArray() : RandomBytes(rng, rng.Next(0, 40));
                }
                else
                {
                    name = RandomBytes(rng, rng.Next(1, 30));
                    value = RandomBytes(rng, rng.Next(0, 200));
                }
                Assert.True(src.TryAdd(name, value));
            }
            bool huffman = rng.Next(2) == 0;
            int n = QpackEncoder.Encode(src, block, huffman);
            Assert.True(n >= 2);
            Assert.Equal(QpackDecodeStatus.Ok, QpackDecoder.Decode(block.AsSpan(0, n), dst));
            TestUtil.AssertSameHeaders(src, dst);
        }

        static byte[] RandomBytes(Random rng, int len)
        {
            byte[] b = new byte[len];
            rng.NextBytes(b);
            return b;
        }
    }

    [Fact]
    public void Destination_Too_Small_Fails_And_Sticks()
    {
        // No room for the prefix.
        var e0 = new QpackEncoder(stackalloc byte[1]);
        Assert.True(e0.HasFailed);
        Assert.Equal(0, e0.BytesWritten);
        Assert.False(e0.TryWrite(":method"u8, "GET"u8));

        // Prefix only: indexed line does not fit.
        var e1 = new QpackEncoder(stackalloc byte[2]);
        Assert.False(e1.TryWrite(":method"u8, "GET"u8));
        Assert.True(e1.HasFailed);
        Assert.Equal(2, e1.BytesWritten);
        Assert.False(e1.TryWrite(":method"u8, "GET"u8));

        // Name reference: index byte fits, value does not (integer, then Huffman bytes, then raw bytes).
        Assert.False(new QpackEncoder(stackalloc byte[2]).TryWrite(":path"u8, "/foo"u8));
        Assert.False(new QpackEncoder(stackalloc byte[3]).TryWrite(":path"u8, "/foo"u8));
        Assert.False(new QpackEncoder(stackalloc byte[4]).TryWrite(":path"u8, "/foo"u8));
        Assert.False(new QpackEncoder(stackalloc byte[6]).TryWrite(":path"u8, "/foo"u8));
        Assert.True(new QpackEncoder(stackalloc byte[7]).TryWrite(":path"u8, "/foo"u8));
        Assert.False(new QpackEncoder(stackalloc byte[4], false).TryWrite(":path"u8, "/foo"u8));
        Assert.False(new QpackEncoder(stackalloc byte[7], false).TryWrite(":path"u8, "/foo"u8));
        Assert.True(new QpackEncoder(stackalloc byte[8], false).TryWrite(":path"u8, "/foo"u8));

        // Literal name: name does not fit, then value does not fit.
        Assert.False(new QpackEncoder(stackalloc byte[2]).TryWrite("x-a"u8, "b"u8));
        Assert.False(new QpackEncoder(stackalloc byte[5]).TryWrite("x-a"u8, "b"u8));
        Assert.False(new QpackEncoder(stackalloc byte[7]).TryWrite("x-a"u8, "b"u8));
        Assert.True(new QpackEncoder(stackalloc byte[8]).TryWrite("x-a"u8, "b"u8));
        // Huffman name that does not fit (integer ok, Huffman bytes not).
        Assert.False(new QpackEncoder(stackalloc byte[4]).TryWrite("no-cache"u8, "x"u8));
    }

    [Fact]
    public void Static_Encode_Helper()
    {
        Http3HeaderCollection src = TestUtil.ResponseHeaders();
        byte[] block = new byte[2048];
        int n = QpackEncoder.Encode(src, block);
        Assert.True(n > 2);
        for (int size = 0; size < n; size++)
        {
            Assert.Equal(-1, QpackEncoder.Encode(src, block.AsSpan(0, size)));
        }
        Assert.Equal(n, QpackEncoder.Encode(src, block.AsSpan(0, n)));

        var empty = new Http3HeaderCollection(16, 1);
        Assert.Equal(2, QpackEncoder.Encode(empty, block));
        Assert.Equal(-1, QpackEncoder.Encode(empty, block.AsSpan(0, 1)));
        Assert.Throws<ArgumentNullException>(() => QpackEncoder.Encode(null!, new byte[8]));
    }
}
