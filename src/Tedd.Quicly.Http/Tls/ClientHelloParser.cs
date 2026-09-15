using System.Buffers.Binary;
using System.Text;

namespace Tedd.Quicly.Http.Tls;

/// <summary>What we need from a TLS ClientHello before handing the connection to <see cref="System.Net.Security.SslStream"/>.</summary>
internal readonly struct ClientHelloInfo
{
    public ClientHelloInfo(string? serverName, string[] applicationProtocols)
    {
        ServerName = serverName;
        ApplicationProtocols = applicationProtocols;
    }

    /// <summary>SNI host name, or <see langword="null"/>.</summary>
    public string? ServerName { get; }

    /// <summary>Offered ALPN protocol ids in client preference order (empty when the extension is absent).</summary>
    public string[] ApplicationProtocols { get; }

    /// <summary>Whether <paramref name="protocol"/> was offered.</summary>
    public bool Offers(string protocol)
    {
        var list = ApplicationProtocols;
        for (int i = 0; i < list.Length; i++)
        {
            if (string.Equals(list[i], protocol, StringComparison.Ordinal))
                return true;
        }
        return false;
    }
}

/// <summary>Outcome of <see cref="ClientHelloParser.TryAssemble"/>.</summary>
internal enum ClientHelloAssembleStatus : byte
{
    /// <summary>More bytes are needed to complete the ClientHello.</summary>
    NeedMore,

    /// <summary>The complete ClientHello handshake message has been copied out.</summary>
    Complete,

    /// <summary>The bytes are not a TLS handshake (wrong record type/version, zero or oversized record, not a ClientHello).</summary>
    NotTls,

    /// <summary>The ClientHello is larger than the assembly buffer allows.</summary>
    TooLarge,
}

/// <summary>
/// Minimal TLS ClientHello parser: extracts <c>server_name</c> (type 0) and <c>application_layer_protocol_negotiation</c>
/// (type 16). <see cref="TryAssemble"/> reassembles a ClientHello that the client fragmented over several handshake
/// records; <see cref="TryParse"/> then reads the extensions from the assembled message.
/// </summary>
internal static class ClientHelloParser
{
    /// <summary>Largest TLS record payload (RFC 8446 §5.1).</summary>
    public const int MaxRecordLength = 16384;

    /// <summary>Record header size.</summary>
    public const int RecordHeaderLength = 5;

    /// <summary>Handshake message header size (type + 24-bit length).</summary>
    public const int HandshakeHeaderLength = 4;

    /// <summary>Largest ClientHello we are willing to reassemble (header included). Real ones are a few KiB even with post-quantum key shares.</summary>
    public const int MaxClientHelloLength = 64 * 1024;

    /// <summary>Upper bound on raw bytes buffered while peeking: every record carries a 5-byte header, so a maximal ClientHello spread over minimal records still fits.</summary>
    public const int MaxPeekBytes = MaxClientHelloLength + 8 * RecordHeaderLength + MaxRecordLength;

    /// <summary>
    /// Inspects a buffered record header. Returns the number of bytes the complete first record occupies
    /// (header included), 0 when more header bytes are needed, or -1 when this is not a TLS handshake record.
    /// </summary>
    public static int GetRecordLength(ReadOnlySpan<byte> buffered)
    {
        if (buffered.Length < RecordHeaderLength)
            return 0;
        if (buffered[0] != 0x16 || buffered[1] != 0x03)
            return -1;
        int length = BinaryPrimitives.ReadUInt16BigEndian(buffered[3..]);
        if (length == 0 || length > MaxRecordLength)
            return -1;
        return RecordHeaderLength + length;
    }

    /// <summary>
    /// Walks the handshake records at the start of <paramref name="buffered"/> and copies their payloads into
    /// <paramref name="handshake"/> until one complete ClientHello message (header included) has been assembled.
    /// </summary>
    /// <param name="buffered">Raw bytes received so far.</param>
    /// <param name="handshake">Scratch buffer of at least <see cref="MaxClientHelloLength"/> bytes.</param>
    /// <param name="handshakeLength">Length of the assembled message in <paramref name="handshake"/> on <see cref="ClientHelloAssembleStatus.Complete"/>.</param>
    /// <param name="recordBytes">Number of raw bytes belonging to the records that were consumed on <see cref="ClientHelloAssembleStatus.Complete"/>.</param>
    public static ClientHelloAssembleStatus TryAssemble(ReadOnlySpan<byte> buffered, Span<byte> handshake, out int handshakeLength, out int recordBytes)
    {
        handshakeLength = 0;
        recordBytes = 0;
        int pos = 0;
        int assembled = 0;
        int needed = -1;
        while (true)
        {
            int recordLength = GetRecordLength(buffered[pos..]);
            if (recordLength < 0)
                return ClientHelloAssembleStatus.NotTls;
            if (recordLength == 0 || buffered.Length - pos < recordLength)
                return ClientHelloAssembleStatus.NeedMore;

            var payload = buffered.Slice(pos + RecordHeaderLength, recordLength - RecordHeaderLength);
            if (assembled + payload.Length > handshake.Length)
                return ClientHelloAssembleStatus.TooLarge;
            payload.CopyTo(handshake[assembled..]);
            assembled += payload.Length;
            pos += recordLength;

            if (needed < 0 && assembled >= HandshakeHeaderLength)
            {
                if (handshake[0] != 0x01)
                    return ClientHelloAssembleStatus.NotTls;
                needed = HandshakeHeaderLength + ((handshake[1] << 16) | (handshake[2] << 8) | handshake[3]);
                if (needed > handshake.Length)
                    return ClientHelloAssembleStatus.TooLarge;
            }
            if (needed >= 0 && assembled >= needed)
            {
                handshakeLength = needed;
                recordBytes = pos;
                return ClientHelloAssembleStatus.Complete;
            }
        }
    }

    /// <summary>Parses an assembled ClientHello handshake message (starting at the handshake type byte).</summary>
    public static bool TryParse(ReadOnlySpan<byte> record, out ClientHelloInfo info)
    {
        info = default;
        if (record.Length < HandshakeHeaderLength || record[0] != 0x01)
            return false;
        int handshakeLength = (record[1] << 16) | (record[2] << 8) | record[3];
        record = record[HandshakeHeaderLength..];
        if (handshakeLength > record.Length)
            return false;
        record = record[..handshakeLength];

        // legacy_version(2) + random(32)
        if (record.Length < 34)
            return false;
        record = record[34..];

        if (!TrySkipVector(ref record, 1)) return false;   // session id
        if (!TrySkipVector(ref record, 2)) return false;   // cipher suites
        if (!TrySkipVector(ref record, 1)) return false;   // compression methods

        string? serverName = null;
        string[] alpn = [];
        if (record.Length == 0)
        {
            info = new ClientHelloInfo(null, alpn);
            return true;
        }
        if (record.Length < 2)
            return false;
        int extLength = BinaryPrimitives.ReadUInt16BigEndian(record);
        record = record[2..];
        if (extLength > record.Length)
            return false;
        var extensions = record[..extLength];

        while (extensions.Length >= 4)
        {
            int type = BinaryPrimitives.ReadUInt16BigEndian(extensions);
            int length = BinaryPrimitives.ReadUInt16BigEndian(extensions[2..]);
            extensions = extensions[4..];
            if (length > extensions.Length)
                return false;
            var body = extensions[..length];
            extensions = extensions[length..];

            if (type == 0)
            {
                if (!TryParseServerName(body, out serverName))
                    return false;
            }
            else if (type == 16)
            {
                if (!TryParseAlpn(body, out alpn))
                    return false;
            }
        }
        if (extensions.Length != 0)
            return false;

        info = new ClientHelloInfo(serverName, alpn);
        return true;
    }

    private static bool TrySkipVector(ref ReadOnlySpan<byte> s, int lengthBytes)
    {
        if (s.Length < lengthBytes)
            return false;
        int length = lengthBytes == 1 ? s[0] : BinaryPrimitives.ReadUInt16BigEndian(s);
        s = s[lengthBytes..];
        if (length > s.Length)
            return false;
        s = s[length..];
        return true;
    }

    private static bool TryParseServerName(ReadOnlySpan<byte> body, out string? serverName)
    {
        serverName = null;
        if (body.Length < 2)
            return false;
        int listLength = BinaryPrimitives.ReadUInt16BigEndian(body);
        body = body[2..];
        if (listLength > body.Length)
            return false;
        var list = body[..listLength];
        while (list.Length >= 3)
        {
            byte nameType = list[0];
            int nameLength = BinaryPrimitives.ReadUInt16BigEndian(list[1..]);
            list = list[3..];
            if (nameLength > list.Length)
                return false;
            if (nameType == 0 && serverName is null)
            {
                var name = list[..nameLength];
                if (name.Length == 0 || name.ContainsAnyExceptInRange((byte)0x21, (byte)0x7E))
                    return false;
                serverName = Encoding.ASCII.GetString(name);
            }
            list = list[nameLength..];
        }
        return list.Length == 0;
    }

    private static bool TryParseAlpn(ReadOnlySpan<byte> body, out string[] protocols)
    {
        protocols = [];
        if (body.Length < 2)
            return false;
        int listLength = BinaryPrimitives.ReadUInt16BigEndian(body);
        body = body[2..];
        if (listLength > body.Length)
            return false;
        var list = body[..listLength];

        int count = 0;
        var scan = list;
        while (scan.Length >= 1)
        {
            int len = scan[0];
            if (len == 0 || len > scan.Length - 1)
                return false;
            scan = scan[(1 + len)..];
            count++;
        }

        protocols = count == 0 ? [] : new string[count];
        for (int i = 0; i < count; i++)
        {
            int len = list[0];
            protocols[i] = Encoding.ASCII.GetString(list.Slice(1, len));
            list = list[(1 + len)..];
        }
        return true;
    }
}
