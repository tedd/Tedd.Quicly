using System.Buffers.Binary;
using System.Text;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Http.Tests;

public class ClientHelloParserTests
{
    private static byte[] BuildClientHello(string? sni, string[]? alpn, bool withExtensionsBlock = true, byte[]? extraExtension = null)
    {
        var body = new List<byte>();
        body.AddRange([0x03, 0x03]);                 // legacy version
        body.AddRange(new byte[32]);                 // random
        body.Add(0);                                 // session id length
        body.AddRange([0x00, 0x02, 0x13, 0x01]);     // cipher suites: TLS_AES_128_GCM_SHA256
        body.AddRange([0x01, 0x00]);                 // compression: null

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
        if (extraExtension is not null)
            ext.AddRange(extraExtension);
        if (withExtensionsBlock)
        {
            body.AddRange(U16(ext.Count));
            body.AddRange(ext);
        }

        var handshake = new List<byte> { 0x01, (byte)(body.Count >> 16), (byte)(body.Count >> 8), (byte)body.Count };
        handshake.AddRange(body);
        var record = new List<byte> { 0x16, 0x03, 0x01 };
        record.AddRange(U16(handshake.Count));
        record.AddRange(handshake);
        return [.. record];
    }

    private static byte[] U16(int v)
    {
        var b = new byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(b, (ushort)v);
        return b;
    }

    [Fact]
    public void Parses_sni_and_alpn()
    {
        var record = BuildClientHello("example.com", ["h2", "http/1.1", "acme-tls/1"]);
        int length = ClientHelloParser.GetRecordLength(record);
        Assert.Equal(record.Length, length);
        Assert.True(ClientHelloParser.TryParse(record.AsSpan(5), out var info));
        Assert.Equal("example.com", info.ServerName);
        Assert.Equal(["h2", "http/1.1", "acme-tls/1"], info.ApplicationProtocols);
        Assert.True(info.Offers("acme-tls/1"));
        Assert.False(info.Offers("h3"));
    }

    [Fact]
    public void Parses_without_extensions()
    {
        var record = BuildClientHello(null, null, withExtensionsBlock: false);
        Assert.True(ClientHelloParser.TryParse(record.AsSpan(5), out var info));
        Assert.Null(info.ServerName);
        Assert.Empty(info.ApplicationProtocols);

        record = BuildClientHello(null, null);
        Assert.True(ClientHelloParser.TryParse(record.AsSpan(5), out info));
        Assert.Null(info.ServerName);

        // unknown extension types are skipped
        record = BuildClientHello("a.b", null, extraExtension: [0x00, 0x2b, 0x00, 0x03, 0x02, 0x03, 0x04]);
        Assert.True(ClientHelloParser.TryParse(record.AsSpan(5), out info));
        Assert.Equal("a.b", info.ServerName);
    }

    [Fact]
    public void Record_length_detection()
    {
        Assert.Equal(0, ClientHelloParser.GetRecordLength([0x16, 0x03]));
        Assert.Equal(-1, ClientHelloParser.GetRecordLength("GET /"u8.ToArray()));
        Assert.Equal(-1, ClientHelloParser.GetRecordLength([0x16, 0x03, 0x01, 0x00, 0x00]));
        Assert.Equal(-1, ClientHelloParser.GetRecordLength([0x16, 0x03, 0x01, 0x50, 0x00]));
        Assert.Equal(5 + 0x1234, ClientHelloParser.GetRecordLength([0x16, 0x03, 0x01, 0x12, 0x34]));
        Assert.Equal(-1, ClientHelloParser.GetRecordLength([0x17, 0x03, 0x01, 0x00, 0x10]));
    }

    [Fact]
    public void Rejects_truncated_and_malformed()
    {
        var good = BuildClientHello("example.com", ["http/1.1"]);
        var payload = good.AsSpan(5).ToArray();

        Assert.False(ClientHelloParser.TryParse([], out _));
        Assert.False(ClientHelloParser.TryParse([0x02, 0, 0, 0], out _)); // not client_hello
        Assert.False(ClientHelloParser.TryParse([0x01, 0x00, 0x10, 0x00], out _)); // declared length beyond buffer
        Assert.False(ClientHelloParser.TryParse([0x01, 0x00, 0x00, 0x02, 0x03, 0x03], out _)); // no random

        // truncate at every position inside the extensions: must never throw and must fail cleanly
        for (int cut = 4; cut < payload.Length; cut++)
        {
            var truncated = payload.AsSpan(0, cut).ToArray();
            truncated[1] = (byte)((cut - 4) >> 16);
            truncated[2] = (byte)((cut - 4) >> 8);
            truncated[3] = (byte)(cut - 4);
            ClientHelloParser.TryParse(truncated, out _);
        }

        // SNI with a non-ASCII host name
        var badSni = BuildClientHello(null, null, extraExtension: [0x00, 0x00, 0x00, 0x08, 0x00, 0x06, 0x00, 0x00, 0x03, (byte)'e', (byte)'x', 0xE4]);
        Assert.False(ClientHelloParser.TryParse(badSni.AsSpan(5), out _));

        // empty SNI host
        var emptySni = BuildClientHello(string.Empty, null);
        Assert.False(ClientHelloParser.TryParse(emptySni.AsSpan(5), out _));

        // ALPN with a zero-length protocol
        var badAlpn = BuildClientHello(null, [string.Empty]);
        Assert.False(ClientHelloParser.TryParse(badAlpn.AsSpan(5), out _));

        // SNI extension claiming a longer list than present
        var sniShort = BuildClientHello(null, null, extraExtension: [0x00, 0x00, 0x00, 0x02, 0x00, 0x10]);
        Assert.False(ClientHelloParser.TryParse(sniShort.AsSpan(5), out _));
        var sniTiny = BuildClientHello(null, null, extraExtension: [0x00, 0x00, 0x00, 0x01, 0x00]);
        Assert.False(ClientHelloParser.TryParse(sniTiny.AsSpan(5), out _));
        // SNI entry with a name length beyond the list
        var sniEntryShort = BuildClientHello(null, null, extraExtension: [0x00, 0x00, 0x00, 0x05, 0x00, 0x03, 0x00, 0x00, 0x10]);
        Assert.False(ClientHelloParser.TryParse(sniEntryShort.AsSpan(5), out _));
        // SNI list with trailing garbage
        var sniTrailing = BuildClientHello(null, null, extraExtension: [0x00, 0x00, 0x00, 0x04, 0x00, 0x02, 0x00, 0x00]);
        Assert.False(ClientHelloParser.TryParse(sniTrailing.AsSpan(5), out _));
        // second SNI entry of a different type is ignored
        var sniTwo = BuildClientHello(null, null, extraExtension: [0x00, 0x00, 0x00, 0x0a, 0x00, 0x08, 0x00, 0x00, 0x01, (byte)'a', 0x01, 0x00, 0x01, (byte)'b']);
        Assert.True(ClientHelloParser.TryParse(sniTwo.AsSpan(5), out var two));
        Assert.Equal("a", two.ServerName);

        // ALPN extension with bad list lengths
        var alpnShort = BuildClientHello(null, null, extraExtension: [0x00, 0x10, 0x00, 0x01, 0x00]);
        Assert.False(ClientHelloParser.TryParse(alpnShort.AsSpan(5), out _));
        var alpnLong = BuildClientHello(null, null, extraExtension: [0x00, 0x10, 0x00, 0x02, 0x00, 0x10]);
        Assert.False(ClientHelloParser.TryParse(alpnLong.AsSpan(5), out _));
        var alpnOverrun = BuildClientHello(null, null, extraExtension: [0x00, 0x10, 0x00, 0x04, 0x00, 0x02, 0x05, (byte)'a']);
        Assert.False(ClientHelloParser.TryParse(alpnOverrun.AsSpan(5), out _));
        var alpnEmptyList = BuildClientHello(null, null, extraExtension: [0x00, 0x10, 0x00, 0x02, 0x00, 0x00]);
        Assert.True(ClientHelloParser.TryParse(alpnEmptyList.AsSpan(5), out var empty));
        Assert.Empty(empty.ApplicationProtocols);

        // extension length overrunning the block
        var extOverrun = BuildClientHello(null, null, extraExtension: [0x00, 0x2b, 0x00, 0x10]);
        Assert.False(ClientHelloParser.TryParse(extOverrun.AsSpan(5), out _));
        // trailing bytes after the last extension
        var extTrailing = BuildClientHello(null, null, extraExtension: [0x00, 0x2b]);
        Assert.False(ClientHelloParser.TryParse(extTrailing.AsSpan(5), out _));
        // extensions block too short for its own length prefix
        var body = good.AsSpan(5).ToArray();
        var trimmed = body.AsSpan(0, 4 + 34 + 1 + 4 + 2 + 1).ToArray();
        trimmed[1] = 0; trimmed[2] = 0; trimmed[3] = (byte)(trimmed.Length - 4);
        Assert.False(ClientHelloParser.TryParse(trimmed, out _));
    }
}
