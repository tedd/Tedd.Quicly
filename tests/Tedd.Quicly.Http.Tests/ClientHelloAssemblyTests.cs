using System.Buffers.Binary;
using System.Text;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Http.Tests;

/// <summary>Cross-record reassembly of a ClientHello (<see cref="ClientHelloParser.TryAssemble(ReadOnlySpan{byte}, Span{byte}, out int, out int)"/>).</summary>
public class ClientHelloAssemblyTests
{
    /// <summary>Builds the handshake message (type + length + body) for a ClientHello with the given SNI/ALPN.</summary>
    internal static byte[] BuildHandshake(string? sni, string[]? alpn, int padding = 0)
    {
        var body = new List<byte>();
        body.AddRange([0x03, 0x03]);
        body.AddRange(new byte[32]);
        body.Add(0);
        body.AddRange([0x00, 0x02, 0x13, 0x01]);
        body.AddRange([0x01, 0x00]);
        var ext = new List<byte>();
        if (sni is not null)
        {
            var name = Encoding.ASCII.GetBytes(sni);
            var entry = new List<byte> { 0 };
            entry.AddRange(U16(name.Length));
            entry.AddRange(name);
            var list = new List<byte>();
            list.AddRange(U16(entry.Count));
            list.AddRange(entry);
            ext.AddRange(U16(0));
            ext.AddRange(U16(list.Count));
            ext.AddRange(list);
        }
        if (alpn is not null)
        {
            var list = new List<byte>();
            foreach (var p in alpn)
            {
                var b = Encoding.ASCII.GetBytes(p);
                list.Add((byte)b.Length);
                list.AddRange(b);
            }
            var payload = new List<byte>();
            payload.AddRange(U16(list.Count));
            payload.AddRange(list);
            ext.AddRange(U16(16));
            ext.AddRange(U16(payload.Count));
            ext.AddRange(payload);
        }
        if (padding > 0)
        {
            // padding extension (type 21)
            ext.AddRange(U16(21));
            ext.AddRange(U16(padding));
            ext.AddRange(new byte[padding]);
        }
        body.AddRange(U16(ext.Count));
        body.AddRange(ext);
        var handshake = new List<byte> { 0x01, (byte)(body.Count >> 16), (byte)(body.Count >> 8), (byte)body.Count };
        handshake.AddRange(body);
        return [.. handshake];
    }

    /// <summary>Splits <paramref name="handshake"/> into TLS handshake records of at most <paramref name="fragment"/> bytes.</summary>
    internal static byte[] Fragment(byte[] handshake, int fragment)
    {
        var output = new List<byte>();
        for (int offset = 0; offset < handshake.Length; offset += fragment)
        {
            int len = Math.Min(fragment, handshake.Length - offset);
            output.AddRange([0x16, 0x03, 0x01]);
            output.AddRange(U16(len));
            output.AddRange(handshake.AsSpan(offset, len).ToArray());
        }
        return [.. output];
    }

    private static byte[] U16(int v)
    {
        var b = new byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(b, (ushort)v);
        return b;
    }

    private static ClientHelloAssembleStatus Assemble(byte[] raw, out byte[] handshake, out int recordBytes)
    {
        var scratch = new byte[ClientHelloParser.MaxClientHelloLength];
        var status = ClientHelloParser.TryAssemble(raw, scratch, out int length, out recordBytes);
        handshake = scratch[..length];
        return status;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(7)]
    [InlineData(64)]
    [InlineData(16384)]
    public void Assembles_fragmented_records(int fragment)
    {
        var handshake = BuildHandshake("frag.example", ["acme-tls/1", "http/1.1"]);
        var raw = Fragment(handshake, fragment);
        var trailing = raw.Concat(new byte[] { 0x17, 0x03, 0x03, 0x00, 0x01, 0xAA }).ToArray(); // an unrelated record after the hello
        Assert.Equal(ClientHelloAssembleStatus.Complete, Assemble(trailing, out var assembled, out int recordBytes));
        Assert.Equal(handshake, assembled);
        Assert.Equal(raw.Length, recordBytes);
        Assert.True(ClientHelloParser.TryParse(assembled, out var info));
        Assert.Equal("frag.example", info.ServerName);
        Assert.True(info.Offers("acme-tls/1"));

        // every proper prefix needs more
        for (int cut = 0; cut < raw.Length; cut += Math.Max(1, raw.Length / 50))
            Assert.Equal(ClientHelloAssembleStatus.NeedMore, Assemble(raw[..cut], out _, out _));
    }

    [Fact]
    public void Rejects_non_tls_and_oversized_input()
    {
        Assert.Equal(ClientHelloAssembleStatus.NotTls, Assemble("GET / HTTP/1.1\r\n"u8.ToArray(), out _, out _));
        Assert.Equal(ClientHelloAssembleStatus.NotTls, Assemble([0x16, 0x02, 0x01, 0x00, 0x05, 1, 2, 3, 4, 5], out _, out _)); // wrong major version
        Assert.Equal(ClientHelloAssembleStatus.NotTls, Assemble([0x16, 0x03, 0x01, 0x00, 0x00], out _, out _)); // empty record
        Assert.Equal(ClientHelloAssembleStatus.NotTls, Assemble([0x16, 0x03, 0x01, 0x40, 0x01], out _, out _)); // record > 16 KiB
        Assert.Equal(ClientHelloAssembleStatus.NotTls, Assemble([0x16, 0x03, 0x01, 0x00, 0x04, 0x02, 0x00, 0x00, 0x00], out _, out _)); // ServerHello, not ClientHello
        Assert.Equal(ClientHelloAssembleStatus.NotTls, Assemble([0x16, 0x03, 0x01, 0x00, 0x01, 0x01, 0x17, 0x03, 0x01, 0x00, 0x03, 0x00, 0x00, 0x00], out _, out _)); // second record is not a handshake record
        Assert.Equal(ClientHelloAssembleStatus.NeedMore, Assemble([0x16, 0x03, 0x01, 0x00, 0x01, 0x01, 0x16, 0x03], out _, out _)); // type byte only, then a partial header

        // declared handshake length beyond the assembly buffer
        Assert.Equal(ClientHelloAssembleStatus.TooLarge, Assemble([0x16, 0x03, 0x01, 0x00, 0x04, 0x01, 0x01, 0x00, 0x00], out _, out _));

        // many maximal records exceed the buffer before the length is known only if type is valid
        var big = new List<byte>();
        for (int i = 0; i < 5; i++)
        {
            big.AddRange([0x16, 0x03, 0x01, 0x40, 0x00]);
            var payload = new byte[16384];
            if (i == 0)
            {
                payload[0] = 0x01; payload[1] = 0x00; payload[2] = 0xFF; payload[3] = 0xFF; // 64 KiB - 1 body
            }
            big.AddRange(payload);
        }
        Assert.Equal(ClientHelloAssembleStatus.TooLarge, Assemble([.. big], out _, out _));

        // a declared length that fits exactly (64 KiB) but records that overshoot the assembly buffer before completing it
        var overshoot = new List<byte>();
        overshoot.AddRange([0x16, 0x03, 0x01, 0x3E, 0x80]); // 16000-byte record
        var first = new byte[16000];
        first[0] = 0x01; first[1] = 0x00; first[2] = 0xFF; first[3] = 0xFC; // body 65532 -> message 65536
        overshoot.AddRange(first);
        for (int i = 0; i < 4; i++)
        {
            overshoot.AddRange([0x16, 0x03, 0x01, 0x40, 0x00]);
            overshoot.AddRange(new byte[16384]);
        }
        Assert.Equal(ClientHelloAssembleStatus.TooLarge, Assemble([.. overshoot], out _, out _));

        // the record-length helper is unchanged
        Assert.Equal(0, ClientHelloParser.GetRecordLength([0x16]));
        Assert.Equal(5 + 1, ClientHelloParser.GetRecordLength([0x16, 0x03, 0x03, 0x00, 0x01]));
    }

    [Fact]
    public void Random_input_never_throws()
    {
        var random = new Random(7);
        var scratch = new byte[ClientHelloParser.MaxClientHelloLength];
        var good = Fragment(BuildHandshake("fuzz.example", ["h2", "http/1.1"], padding: 300), 100);
        for (int i = 0; i < 2000; i++)
        {
            byte[] input;
            if (i % 2 == 0)
            {
                input = new byte[random.Next(0, 400)];
                random.NextBytes(input);
                if (input.Length > 1)
                {
                    input[0] = 0x16;
                    input[1] = 0x03;
                }
            }
            else
            {
                input = (byte[])good.Clone();
                for (int m = 0; m < 4; m++)
                    input[random.Next(input.Length)] = (byte)random.Next(256);
            }
            var status = ClientHelloParser.TryAssemble(input, scratch, out int length, out int consumed);
            if (status == ClientHelloAssembleStatus.Complete)
            {
                Assert.True(consumed <= input.Length);
                ClientHelloParser.TryParse(scratch.AsSpan(0, length), out _);
            }
        }
    }

    [Fact]
    public void Assembly_does_not_allocate()
    {
        var raw = Fragment(BuildHandshake("alloc.example", ["http/1.1"]), 50);
        var scratch = new byte[ClientHelloParser.MaxClientHelloLength];
        for (int i = 0; i < 1000; i++)
            ClientHelloParser.TryAssemble(raw, scratch, out _, out _);
        int total = 0;
        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 10_000; i++)
            {
                if (ClientHelloParser.TryAssemble(raw, scratch, out int length, out _) == ClientHelloAssembleStatus.Complete)
                    total += length;
            }
        });
        Assert.True(total > 0);
    }
}
