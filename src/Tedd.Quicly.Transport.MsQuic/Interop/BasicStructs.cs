using System.Runtime.InteropServices;

namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>Opaque MsQuic object handle (<c>HQUIC</c> is a <c>QUIC_HANDLE*</c>).</summary>
[StructLayout(LayoutKind.Sequential, Size = 1)]
public struct QUIC_HANDLE
{
}

/// <summary>A length-prefixed pointer to memory owned by the application (send) or MsQuic (receive).</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_BUFFER
{
    /// <summary>Number of valid bytes at <see cref="Buffer"/>.</summary>
    public uint Length;
    /// <summary>Start of the data.</summary>
    public byte* Buffer;

    /// <summary>Creates a buffer descriptor over native memory.</summary>
    public QUIC_BUFFER(byte* buffer, uint length)
    {
        Buffer = buffer;
        Length = length;
    }

    /// <summary>The data as a span (valid only while the underlying memory is).</summary>
    public readonly Span<byte> Span => new(Buffer, (int)Length);
}

/// <summary>Registration configuration passed to <c>RegistrationOpen</c>.</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_REGISTRATION_CONFIG
{
    /// <summary>Optional null-terminated UTF-8 application name (used in logging).</summary>
    public sbyte* AppName;
    /// <summary>Worker execution profile.</summary>
    public QUIC_EXECUTION_PROFILE ExecutionProfile;
}

/// <summary>Negotiated TLS parameters (<c>QUIC_PARAM_TLS_HANDSHAKE_INFO</c>).</summary>
[StructLayout(LayoutKind.Sequential)]
public struct QUIC_HANDSHAKE_INFO
{
    public QUIC_TLS_PROTOCOL_VERSION TlsProtocolVersion;
    public QUIC_CIPHER_ALGORITHM CipherAlgorithm;
    public int CipherStrength;
    public QUIC_HASH_ALGORITHM Hash;
    public int HashStrength;
    public QUIC_KEY_EXCHANGE_ALGORITHM KeyExchangeAlgorithm;
    public int KeyExchangeStrength;
    public QUIC_CIPHER_SUITE CipherSuite;
}

/// <summary>Information about an incoming connection (<c>QUIC_LISTENER_EVENT.NEW_CONNECTION.Info</c>).</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_NEW_CONNECTION_INFO
{
    public uint QuicVersion;
    public QuicAddr* LocalAddress;
    public QuicAddr* RemoteAddress;
    public uint CryptoBufferLength;
    public ushort ClientAlpnListLength;
    public ushort ServerNameLength;
    public byte NegotiatedAlpnLength;
    public byte* CryptoBuffer;
    /// <summary>The ALPN list offered by the client, wire format (length-prefixed entries).</summary>
    public byte* ClientAlpnList;
    public byte* NegotiatedAlpn;
    /// <summary>SNI (not null-terminated; see <see cref="ServerNameLength"/>).</summary>
    public sbyte* ServerName;

    /// <summary>The negotiated ALPN as a span (valid only during the callback).</summary>
    public readonly ReadOnlySpan<byte> NegotiatedAlpnSpan => new(NegotiatedAlpn, NegotiatedAlpnLength);

    /// <summary>The SNI bytes (UTF-8) as a span (valid only during the callback).</summary>
    public readonly ReadOnlySpan<byte> ServerNameSpan => new(ServerName, ServerNameLength);
}

/// <summary>Traffic secrets for SSLKEYLOGFILE support (<c>QUIC_PARAM_CONN_TLS_SECRETS</c>).</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_TLS_SECRETS
{
    public byte SecretLength;
    public _IsSet_e__Struct IsSet;
    public fixed byte ClientRandom[32];
    public fixed byte ClientEarlyTrafficSecret[64];
    public fixed byte ClientHandshakeTrafficSecret[64];
    public fixed byte ServerHandshakeTrafficSecret[64];
    public fixed byte ClientTrafficSecret0[64];
    public fixed byte ServerTrafficSecret0[64];

    /// <summary>Bitfield saying which secrets are populated.</summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct _IsSet_e__Struct
    {
        public byte _bitfield;

        public bool ClientRandom { readonly get => (_bitfield & 0x01) != 0; set => _bitfield = (byte)(value ? _bitfield | 0x01 : _bitfield & ~0x01); }
        public bool ClientEarlyTrafficSecret { readonly get => (_bitfield & 0x02) != 0; set => _bitfield = (byte)(value ? _bitfield | 0x02 : _bitfield & ~0x02); }
        public bool ClientHandshakeTrafficSecret { readonly get => (_bitfield & 0x04) != 0; set => _bitfield = (byte)(value ? _bitfield | 0x04 : _bitfield & ~0x04); }
        public bool ServerHandshakeTrafficSecret { readonly get => (_bitfield & 0x08) != 0; set => _bitfield = (byte)(value ? _bitfield | 0x08 : _bitfield & ~0x08); }
        public bool ClientTrafficSecret0 { readonly get => (_bitfield & 0x10) != 0; set => _bitfield = (byte)(value ? _bitfield | 0x10 : _bitfield & ~0x10); }
        public bool ServerTrafficSecret0 { readonly get => (_bitfield & 0x20) != 0; set => _bitfield = (byte)(value ? _bitfield | 0x20 : _bitfield & ~0x20); }
    }
}

/// <summary>
/// Connection statistics (<c>QUIC_PARAM_CONN_STATISTICS_V2</c>). Only the fields up to MsQuic 2.5
/// (<c>QUIC_STATISTICS_V2_SIZE_4</c>) are included; the preview-feature block is deliberately omitted.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public struct QUIC_STATISTICS_V2
{
    public ulong CorrelationId;
    /// <summary>Bitfield: see the bool properties.</summary>
    public uint _bitfield;
    /// <summary>Smoothed RTT in microseconds.</summary>
    public uint Rtt;
    public uint MinRtt;
    public uint MaxRtt;
    public ulong TimingStart;
    public ulong TimingInitialFlightEnd;
    public ulong TimingHandshakeFlightEnd;
    public uint HandshakeClientFlight1Bytes;
    public uint HandshakeServerFlight1Bytes;
    public uint HandshakeClientFlight2Bytes;
    public ushort SendPathMtu;
    public ulong SendTotalPackets;
    public ulong SendRetransmittablePackets;
    public ulong SendSuspectedLostPackets;
    public ulong SendSpuriousLostPackets;
    public ulong SendTotalBytes;
    public ulong SendTotalStreamBytes;
    public uint SendCongestionCount;
    public uint SendPersistentCongestionCount;
    public ulong RecvTotalPackets;
    public ulong RecvReorderedPackets;
    public ulong RecvDroppedPackets;
    public ulong RecvDuplicatePackets;
    public ulong RecvTotalBytes;
    public ulong RecvTotalStreamBytes;
    public ulong RecvDecryptionFailures;
    public ulong RecvValidAckFrames;
    public uint KeyUpdateCount;
    public uint SendCongestionWindow;
    public uint DestCidUpdateCount;
    public uint SendEcnCongestionCount;
    public byte HandshakeHopLimitTTL;
    /// <summary>RTT variance in microseconds.</summary>
    public uint RttVariance;

    public readonly bool VersionNegotiation => (_bitfield & 0x01) != 0;
    public readonly bool StatelessRetry => (_bitfield & 0x02) != 0;
    public readonly bool ResumptionAttempted => (_bitfield & 0x04) != 0;
    public readonly bool ResumptionSucceeded => (_bitfield & 0x08) != 0;
    public readonly bool GreaseBitNegotiated => (_bitfield & 0x10) != 0;
    public readonly bool EcnCapable => (_bitfield & 0x20) != 0;
    public readonly bool EncryptionOffloaded => (_bitfield & 0x40) != 0;
}
