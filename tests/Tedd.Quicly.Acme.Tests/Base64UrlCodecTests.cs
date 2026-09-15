using System.Text;

namespace Tedd.Quicly.Acme.Tests;

public class Base64UrlCodecTests
{
    [Theory]
    [InlineData("", "")]
    [InlineData("f", "Zg")]
    [InlineData("fo", "Zm8")]
    [InlineData("foo", "Zm9v")]
    [InlineData("foob", "Zm9vYg")]
    [InlineData("fooba", "Zm9vYmE")]
    [InlineData("foobar", "Zm9vYmFy")]
    public void Encode_MatchesRfc4648Vectors_WithoutPadding(string input, string expected)
    {
        Assert.Equal(expected, Base64UrlCodec.Encode(Encoding.ASCII.GetBytes(input)));
        Assert.Equal(input, Encoding.ASCII.GetString(Base64UrlCodec.Decode(expected)));
    }

    [Fact]
    public void Encode_UsesUrlSafeAlphabet()
    {
        byte[] data = [0xfb, 0xff, 0xbf, 0x3e, 0xfe];
        string encoded = Base64UrlCodec.Encode(data);
        Assert.DoesNotContain('+', encoded);
        Assert.DoesNotContain('/', encoded);
        Assert.DoesNotContain('=', encoded);
        Assert.Equal(data, Base64UrlCodec.Decode(encoded));
    }

    [Fact]
    public void Decode_AcceptsPaddedInput()
    {
        Assert.Equal("foob", Encoding.ASCII.GetString(Base64UrlCodec.Decode("Zm9vYg==")));
    }

    [Fact]
    public void Decode_RejectsInvalidInput()
    {
        Assert.Throws<FormatException>(() => Base64UrlCodec.Decode("!!!"));
    }

    [Fact]
    public void SpanEncode_WritesExactLength()
    {
        byte[] data = new byte[37];
        Random.Shared.NextBytes(data);
        int length = Base64UrlCodec.GetEncodedLength(data.Length);
        char[] buffer = new char[length];
        int written = Base64UrlCodec.Encode(data, buffer);
        Assert.Equal(length, written);
        Assert.Equal(Base64UrlCodec.Encode(data), new string(buffer, 0, written));
    }

    [Fact]
    public void SpanEncode_SteadyState_AllocatesNothing()
    {
        byte[] data = new byte[64];
        Random.Shared.NextBytes(data);
        char[] buffer = new char[Base64UrlCodec.GetEncodedLength(data.Length)];

        for (int i = 0; i < 1_000; i++)
        {
            Base64UrlCodec.Encode(data, buffer);
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 10_000; i++)
        {
            Base64UrlCodec.Encode(data, buffer);
        }

        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);
    }
}
