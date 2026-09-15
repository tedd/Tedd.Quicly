using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>
/// Settings for a configuration / connection (<c>QUIC_PARAM_*_SETTINGS</c>). Every value field has a matching
/// bit in <see cref="IsSetFlags"/>; MsQuic only reads fields whose bit is set. Bit order follows msquic.h.
/// Use the <c>Set*</c> methods to assign a value and set its bit in one step.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public struct QUIC_SETTINGS
{
    public _Anonymous1_e__Union Anonymous1;
    public ulong MaxBytesPerKey;
    public ulong HandshakeIdleTimeoutMs;
    public ulong IdleTimeoutMs;
    public ulong MtuDiscoverySearchCompleteTimeoutUs;
    public uint TlsClientMaxSendBuffer;
    public uint TlsServerMaxSendBuffer;
    public uint StreamRecvWindowDefault;
    public uint StreamRecvBufferDefault;
    public uint ConnFlowControlWindow;
    public uint MaxWorkerQueueDelayUs;
    public uint MaxStatelessOperations;
    public uint InitialWindowPackets;
    public uint SendIdleTimeoutMs;
    public uint InitialRttMs;
    public uint MaxAckDelayMs;
    public uint DisconnectTimeoutMs;
    public uint KeepAliveIntervalMs;
    /// <summary>A <see cref="QUIC_CONGESTION_CONTROL_ALGORITHM"/> value.</summary>
    public ushort CongestionControlAlgorithm;
    public ushort PeerBidiStreamCount;
    public ushort PeerUnidiStreamCount;
    public ushort MaxBindingStatelessOperations;
    public ushort StatelessOperationExpirationMs;
    public ushort MinimumMtu;
    public ushort MaximumMtu;
    /// <summary>Packed booleans: see <see cref="SendBufferingEnabled"/> etc.</summary>
    public byte _bitfield;
    public byte MaxOperationsPerDrain;
    public byte MtuDiscoveryMissingProbeCount;
    public uint DestCidUpdateIdleTimeoutMs;
    public _Anonymous2_e__Union Anonymous2;
    public uint StreamRecvWindowBidiLocalDefault;
    public uint StreamRecvWindowBidiRemoteDefault;
    public uint StreamRecvWindowUnidiDefault;

    /// <summary>Union of the raw <c>IsSetFlags</c> and the named bitfield view.</summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct _Anonymous1_e__Union
    {
        [FieldOffset(0)] public ulong IsSetFlags;
        [FieldOffset(0)] public _IsSet_e__Struct IsSet;

        /// <summary>Named "is set" bits, one per settings field, in msquic.h order.</summary>
        [StructLayout(LayoutKind.Sequential)]
        public struct _IsSet_e__Struct
        {
            public ulong _bitfield;

            private readonly bool Get(int bit) => (_bitfield & (1UL << bit)) != 0;
            private void Set(int bit, bool value) => _bitfield = value ? _bitfield | (1UL << bit) : _bitfield & ~(1UL << bit);

            public bool MaxBytesPerKey { readonly get => Get(0); set => Set(0, value); }
            public bool HandshakeIdleTimeoutMs { readonly get => Get(1); set => Set(1, value); }
            public bool IdleTimeoutMs { readonly get => Get(2); set => Set(2, value); }
            public bool MtuDiscoverySearchCompleteTimeoutUs { readonly get => Get(3); set => Set(3, value); }
            public bool TlsClientMaxSendBuffer { readonly get => Get(4); set => Set(4, value); }
            public bool TlsServerMaxSendBuffer { readonly get => Get(5); set => Set(5, value); }
            public bool StreamRecvWindowDefault { readonly get => Get(6); set => Set(6, value); }
            public bool StreamRecvBufferDefault { readonly get => Get(7); set => Set(7, value); }
            public bool ConnFlowControlWindow { readonly get => Get(8); set => Set(8, value); }
            public bool MaxWorkerQueueDelayUs { readonly get => Get(9); set => Set(9, value); }
            public bool MaxStatelessOperations { readonly get => Get(10); set => Set(10, value); }
            public bool InitialWindowPackets { readonly get => Get(11); set => Set(11, value); }
            public bool SendIdleTimeoutMs { readonly get => Get(12); set => Set(12, value); }
            public bool InitialRttMs { readonly get => Get(13); set => Set(13, value); }
            public bool MaxAckDelayMs { readonly get => Get(14); set => Set(14, value); }
            public bool DisconnectTimeoutMs { readonly get => Get(15); set => Set(15, value); }
            public bool KeepAliveIntervalMs { readonly get => Get(16); set => Set(16, value); }
            public bool CongestionControlAlgorithm { readonly get => Get(17); set => Set(17, value); }
            public bool PeerBidiStreamCount { readonly get => Get(18); set => Set(18, value); }
            public bool PeerUnidiStreamCount { readonly get => Get(19); set => Set(19, value); }
            public bool MaxBindingStatelessOperations { readonly get => Get(20); set => Set(20, value); }
            public bool StatelessOperationExpirationMs { readonly get => Get(21); set => Set(21, value); }
            public bool MinimumMtu { readonly get => Get(22); set => Set(22, value); }
            public bool MaximumMtu { readonly get => Get(23); set => Set(23, value); }
            public bool SendBufferingEnabled { readonly get => Get(24); set => Set(24, value); }
            public bool PacingEnabled { readonly get => Get(25); set => Set(25, value); }
            public bool MigrationEnabled { readonly get => Get(26); set => Set(26, value); }
            public bool DatagramReceiveEnabled { readonly get => Get(27); set => Set(27, value); }
            public bool ServerResumptionLevel { readonly get => Get(28); set => Set(28, value); }
            public bool MaxOperationsPerDrain { readonly get => Get(29); set => Set(29, value); }
            public bool MtuDiscoveryMissingProbeCount { readonly get => Get(30); set => Set(30, value); }
            public bool DestCidUpdateIdleTimeoutMs { readonly get => Get(31); set => Set(31, value); }
            public bool GreaseQuicBitEnabled { readonly get => Get(32); set => Set(32, value); }
            public bool EcnEnabled { readonly get => Get(33); set => Set(33, value); }
            public bool HyStartEnabled { readonly get => Get(34); set => Set(34, value); }
            public bool StreamRecvWindowBidiLocalDefault { readonly get => Get(35); set => Set(35, value); }
            public bool StreamRecvWindowBidiRemoteDefault { readonly get => Get(36); set => Set(36, value); }
            public bool StreamRecvWindowUnidiDefault { readonly get => Get(37); set => Set(37, value); }
            // Preview-feature bits (only honoured by a library built with QUIC_API_ENABLE_PREVIEW_FEATURES).
            public bool EncryptionOffloadAllowed { readonly get => Get(38); set => Set(38, value); }
            public bool ReliableResetEnabled { readonly get => Get(39); set => Set(39, value); }
            public bool OneWayDelayEnabled { readonly get => Get(40); set => Set(40, value); }
            public bool NetStatsEventEnabled { readonly get => Get(41); set => Set(41, value); }
            public bool StreamMultiReceiveEnabled { readonly get => Get(42); set => Set(42, value); }
            public bool XdpEnabled { readonly get => Get(43); set => Set(43, value); }
            public bool QTIPEnabled { readonly get => Get(44); set => Set(44, value); }
            public bool ReservedRioEnabled { readonly get => Get(45); set => Set(45, value); }
        }
    }

    /// <summary>Union of the raw <c>Flags</c> and the named bitfield view.</summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct _Anonymous2_e__Union
    {
        [FieldOffset(0)] public ulong Flags;
        [FieldOffset(0)] public _Anonymous_e__Struct Anonymous;

        /// <summary>Named flag bits.</summary>
        [StructLayout(LayoutKind.Sequential)]
        public struct _Anonymous_e__Struct
        {
            public ulong _bitfield;

            private readonly bool Get(int bit) => (_bitfield & (1UL << bit)) != 0;
            private void Set(int bit, bool value) => _bitfield = value ? _bitfield | (1UL << bit) : _bitfield & ~(1UL << bit);

            public bool HyStartEnabled { readonly get => Get(0); set => Set(0, value); }
            public bool EncryptionOffloadAllowed { readonly get => Get(1); set => Set(1, value); }
            public bool ReliableResetEnabled { readonly get => Get(2); set => Set(2, value); }
            public bool OneWayDelayEnabled { readonly get => Get(3); set => Set(3, value); }
            public bool NetStatsEventEnabled { readonly get => Get(4); set => Set(4, value); }
            public bool StreamMultiReceiveEnabled { readonly get => Get(5); set => Set(5, value); }
            public bool XdpEnabled { readonly get => Get(6); set => Set(6, value); }
            public bool QTIPEnabled { readonly get => Get(7); set => Set(7, value); }
            public bool ReservedRioEnabled { readonly get => Get(8); set => Set(8, value); }
        }
    }

    /// <summary>Raw "is set" mask.</summary>
    public ulong IsSetFlags { readonly get => Anonymous1.IsSetFlags; set => Anonymous1.IsSetFlags = value; }

    /// <summary>Named "is set" bits.</summary>
    [UnscopedRef] public ref _Anonymous1_e__Union._IsSet_e__Struct IsSet => ref Anonymous1.IsSet;

    /// <summary>Raw flags word.</summary>
    public ulong Flags { readonly get => Anonymous2.Flags; set => Anonymous2.Flags = value; }

    private readonly bool GetBit(int bit) => (_bitfield & (1 << bit)) != 0;
    private void SetBit(int bit, bool value) => _bitfield = (byte)(value ? _bitfield | (1 << bit) : _bitfield & ~(1 << bit));

    /// <summary>When true MsQuic copies send buffers (buffered mode); when false the app must keep buffers alive until SEND_COMPLETE.</summary>
    public bool SendBufferingEnabled { readonly get => GetBit(0); set => SetBit(0, value); }
    public bool PacingEnabled { readonly get => GetBit(1); set => SetBit(1, value); }
    public bool MigrationEnabled { readonly get => GetBit(2); set => SetBit(2, value); }
    public bool DatagramReceiveEnabled { readonly get => GetBit(3); set => SetBit(3, value); }
    /// <summary>Two-bit <see cref="QUIC_SERVER_RESUMPTION_LEVEL"/>.</summary>
    public QUIC_SERVER_RESUMPTION_LEVEL ServerResumptionLevel
    {
        readonly get => (QUIC_SERVER_RESUMPTION_LEVEL)((_bitfield >> 4) & 0x3);
        set => _bitfield = (byte)((_bitfield & ~0x30) | (((int)value & 0x3) << 4));
    }
    public bool GreaseQuicBitEnabled { readonly get => GetBit(6); set => SetBit(6, value); }
    public bool EcnEnabled { readonly get => GetBit(7); set => SetBit(7, value); }
    public bool HyStartEnabled { readonly get => Anonymous2.Anonymous.HyStartEnabled; set => Anonymous2.Anonymous.HyStartEnabled = value; }

    // Value + IsSet bit setters. These are the intended way to populate the struct.

    public void SetMaxBytesPerKey(ulong v) { MaxBytesPerKey = v; IsSet.MaxBytesPerKey = true; }
    public void SetHandshakeIdleTimeoutMs(ulong v) { HandshakeIdleTimeoutMs = v; IsSet.HandshakeIdleTimeoutMs = true; }
    public void SetIdleTimeoutMs(ulong v) { IdleTimeoutMs = v; IsSet.IdleTimeoutMs = true; }
    public void SetMtuDiscoverySearchCompleteTimeoutUs(ulong v) { MtuDiscoverySearchCompleteTimeoutUs = v; IsSet.MtuDiscoverySearchCompleteTimeoutUs = true; }
    public void SetTlsClientMaxSendBuffer(uint v) { TlsClientMaxSendBuffer = v; IsSet.TlsClientMaxSendBuffer = true; }
    public void SetTlsServerMaxSendBuffer(uint v) { TlsServerMaxSendBuffer = v; IsSet.TlsServerMaxSendBuffer = true; }
    public void SetStreamRecvWindowDefault(uint v) { StreamRecvWindowDefault = v; IsSet.StreamRecvWindowDefault = true; }
    public void SetStreamRecvBufferDefault(uint v) { StreamRecvBufferDefault = v; IsSet.StreamRecvBufferDefault = true; }
    public void SetConnFlowControlWindow(uint v) { ConnFlowControlWindow = v; IsSet.ConnFlowControlWindow = true; }
    public void SetMaxWorkerQueueDelayUs(uint v) { MaxWorkerQueueDelayUs = v; IsSet.MaxWorkerQueueDelayUs = true; }
    public void SetMaxStatelessOperations(uint v) { MaxStatelessOperations = v; IsSet.MaxStatelessOperations = true; }
    public void SetInitialWindowPackets(uint v) { InitialWindowPackets = v; IsSet.InitialWindowPackets = true; }
    public void SetSendIdleTimeoutMs(uint v) { SendIdleTimeoutMs = v; IsSet.SendIdleTimeoutMs = true; }
    public void SetInitialRttMs(uint v) { InitialRttMs = v; IsSet.InitialRttMs = true; }
    public void SetMaxAckDelayMs(uint v) { MaxAckDelayMs = v; IsSet.MaxAckDelayMs = true; }
    public void SetDisconnectTimeoutMs(uint v) { DisconnectTimeoutMs = v; IsSet.DisconnectTimeoutMs = true; }
    public void SetKeepAliveIntervalMs(uint v) { KeepAliveIntervalMs = v; IsSet.KeepAliveIntervalMs = true; }
    public void SetCongestionControlAlgorithm(QUIC_CONGESTION_CONTROL_ALGORITHM v) { CongestionControlAlgorithm = (ushort)v; IsSet.CongestionControlAlgorithm = true; }
    public void SetPeerBidiStreamCount(ushort v) { PeerBidiStreamCount = v; IsSet.PeerBidiStreamCount = true; }
    public void SetPeerUnidiStreamCount(ushort v) { PeerUnidiStreamCount = v; IsSet.PeerUnidiStreamCount = true; }
    public void SetMaxBindingStatelessOperations(ushort v) { MaxBindingStatelessOperations = v; IsSet.MaxBindingStatelessOperations = true; }
    public void SetStatelessOperationExpirationMs(ushort v) { StatelessOperationExpirationMs = v; IsSet.StatelessOperationExpirationMs = true; }
    public void SetMinimumMtu(ushort v) { MinimumMtu = v; IsSet.MinimumMtu = true; }
    public void SetMaximumMtu(ushort v) { MaximumMtu = v; IsSet.MaximumMtu = true; }
    public void SetSendBufferingEnabled(bool v) { SendBufferingEnabled = v; IsSet.SendBufferingEnabled = true; }
    public void SetPacingEnabled(bool v) { PacingEnabled = v; IsSet.PacingEnabled = true; }
    public void SetMigrationEnabled(bool v) { MigrationEnabled = v; IsSet.MigrationEnabled = true; }
    public void SetDatagramReceiveEnabled(bool v) { DatagramReceiveEnabled = v; IsSet.DatagramReceiveEnabled = true; }
    public void SetServerResumptionLevel(QUIC_SERVER_RESUMPTION_LEVEL v) { ServerResumptionLevel = v; IsSet.ServerResumptionLevel = true; }
    public void SetMaxOperationsPerDrain(byte v) { MaxOperationsPerDrain = v; IsSet.MaxOperationsPerDrain = true; }
    public void SetMtuDiscoveryMissingProbeCount(byte v) { MtuDiscoveryMissingProbeCount = v; IsSet.MtuDiscoveryMissingProbeCount = true; }
    public void SetDestCidUpdateIdleTimeoutMs(uint v) { DestCidUpdateIdleTimeoutMs = v; IsSet.DestCidUpdateIdleTimeoutMs = true; }
    public void SetGreaseQuicBitEnabled(bool v) { GreaseQuicBitEnabled = v; IsSet.GreaseQuicBitEnabled = true; }
    public void SetEcnEnabled(bool v) { EcnEnabled = v; IsSet.EcnEnabled = true; }
    public void SetHyStartEnabled(bool v) { HyStartEnabled = v; IsSet.HyStartEnabled = true; }
    public void SetStreamRecvWindowBidiLocalDefault(uint v) { StreamRecvWindowBidiLocalDefault = v; IsSet.StreamRecvWindowBidiLocalDefault = true; }
    public void SetStreamRecvWindowBidiRemoteDefault(uint v) { StreamRecvWindowBidiRemoteDefault = v; IsSet.StreamRecvWindowBidiRemoteDefault = true; }
    public void SetStreamRecvWindowUnidiDefault(uint v) { StreamRecvWindowUnidiDefault = v; IsSet.StreamRecvWindowUnidiDefault = true; }
}
