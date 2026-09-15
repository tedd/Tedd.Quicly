using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Interop;

/// <summary>
/// Validates every struct size, field offset, enum member and (on Windows) status value of our hand-written
/// bindings against docs/reference/msquic/msquic_layout_dotnet11_preview7.txt, the layout dumped from the
/// runtime's own MsQuic bindings. This is the guarantee that the interop layer matches the native library.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class LayoutTests
{
    private const string DumpNamespace = "Microsoft.Quic.";
    private const string OurNamespace = "Tedd.Quicly.Transport.MsQuic.Interop.";

    private static readonly Assembly s_interop = typeof(QUIC_BUFFER).Assembly;

    private sealed record StructEntry(string Name, int Size, List<(int Offset, string Field)> Fields);
    private sealed record EnumEntry(string Name, List<(string Member, long Value)> Members);

    private sealed class Dump
    {
        public List<StructEntry> Structs { get; } = [];
        public List<EnumEntry> Enums { get; } = [];
        public List<(string Name, long Value)> StatusProps { get; } = [];
    }

    private static Dump ParseDump()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "msquic_layout_dotnet11_preview7.txt");
        Assert.True(File.Exists(path), $"layout dump not found at {path}");
        var dump = new Dump();
        StructEntry? currentStruct = null;
        EnumEntry? currentEnum = null;
        bool inMsQuicClass = false;
        foreach (string raw in File.ReadLines(path))
        {
            if (raw.StartsWith("//", StringComparison.Ordinal)) continue;
            if (!raw.StartsWith(' '))
            {
                currentStruct = null;
                currentEnum = null;
                inMsQuicClass = false;
                if (raw.StartsWith("STRUCT ", StringComparison.Ordinal))
                {
                    string[] parts = raw.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    string name = parts[1][DumpNamespace.Length..];
                    int size = int.Parse(parts[2]["size=".Length..], CultureInfo.InvariantCulture);
                    currentStruct = new StructEntry(name, size, []);
                    if (size >= 0) dump.Structs.Add(currentStruct);
                }
                else if (raw.StartsWith("ENUM ", StringComparison.Ordinal))
                {
                    string[] parts = raw.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    currentEnum = new EnumEntry(parts[1][DumpNamespace.Length..], []);
                    dump.Enums.Add(currentEnum);
                }
                else if (raw.StartsWith("CLASS Microsoft.Quic.MsQuic", StringComparison.Ordinal))
                {
                    inMsQuicClass = true;
                }
                continue;
            }

            string line = raw.Trim();
            if (currentStruct is not null && line.StartsWith('['))
            {
                int close = line.IndexOf(']');
                int offset = int.Parse(line.AsSpan(1, close - 1), CultureInfo.InvariantCulture);
                // "[off] <type> <name>" optionally followed by " (fixed Byte[20])" or " (FieldOffset 0)".
                // The type may contain spaces (delegate* unmanaged[]<A, B>) but never parentheses, so cut at the
                // first " (" and take the last whitespace-separated token as the field name.
                string rest = line[(close + 1)..].Trim();
                int paren = rest.IndexOf(" (", StringComparison.Ordinal);
                if (paren >= 0) rest = rest[..paren];
                string fieldName = rest[(rest.LastIndexOf(' ') + 1)..];
                currentStruct.Fields.Add((offset, fieldName));
            }
            else if (currentEnum is not null && line.Contains(" = ", StringComparison.Ordinal))
            {
                string[] kv = line.Split(" = ", 2, StringSplitOptions.None);
                currentEnum.Members.Add((kv[0].Trim(), long.Parse(kv[1], CultureInfo.InvariantCulture)));
            }
            else if (inMsQuicClass && line.StartsWith("prop System.Int32 QUIC_", StringComparison.Ordinal))
            {
                string[] kv = line["prop System.Int32 ".Length..].Split(" = ", 2, StringSplitOptions.None);
                dump.StatusProps.Add((kv[0].Trim(), long.Parse(kv[1], CultureInfo.InvariantCulture)));
            }
        }
        Assert.NotEmpty(dump.Structs);
        Assert.NotEmpty(dump.Enums);
        Assert.NotEmpty(dump.StatusProps);
        return dump;
    }

    /// <summary>
    /// Maps a dump type name (nested with '+') onto our type. Our nesting mirrors the dump exactly; the only
    /// renamed type is the address union, which the runtime calls <c>QuicAddr</c> and msquic.h <c>QUIC_ADDR</c>.
    /// </summary>
    private static Type? ResolveOurType(string dumpName)
    {
        if (dumpName == "QuicAddr") dumpName = "QUIC_ADDR";
        return s_interop.GetType(OurNamespace + dumpName, throwOnError: false);
    }

    private static int SizeOf(Type t) => (int)typeof(Unsafe).GetMethod(nameof(Unsafe.SizeOf))!.MakeGenericMethod(t).Invoke(null, null)!;

    public static TheoryData<string> StructNames()
    {
        var data = new TheoryData<string>();
        foreach (StructEntry s in ParseDump().Structs) data.Add(s.Name);
        return data;
    }

    public static TheoryData<string> EnumNames()
    {
        var data = new TheoryData<string>();
        foreach (EnumEntry e in ParseDump().Enums) data.Add(e.Name);
        return data;
    }

    [Theory]
    [MemberData(nameof(StructNames))]
    public void Struct_size_and_field_offsets_match_dump(string name)
    {
        StructEntry entry = ParseDump().Structs.Single(s => s.Name == name);
        Type? type = ResolveOurType(name);
        Assert.True(type is not null, $"no binding named {name}");
        Assert.True(type.IsValueType, $"{name} must be a struct");

        Assert.Equal(entry.Size, SizeOf(type));
        Assert.Equal(entry.Size, Marshal.SizeOf(type));

        foreach ((int offset, string field) in entry.Fields)
        {
            FieldInfo? fi = type.GetField(field, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.True(fi is not null, $"{name}.{field} missing");
            Assert.Equal(offset, (int)Marshal.OffsetOf(type, field));
        }
    }

    [Theory]
    [MemberData(nameof(EnumNames))]
    public void Enum_members_match_dump(string name)
    {
        EnumEntry entry = ParseDump().Enums.Single(e => e.Name == name);
        Type? type = ResolveOurType(name);
        Assert.True(type is not null, $"no enum named {name}");
        Assert.True(type.IsEnum);
        Assert.Equal(typeof(int), Enum.GetUnderlyingType(type));
        foreach ((string member, long value) in entry.Members)
        {
            Assert.True(Enum.TryParse(type, member, out object? parsed), $"{name}.{member} missing");
            Assert.Equal(value, Convert.ToInt64(parsed, CultureInfo.InvariantCulture));
        }
    }

    [Fact]
    public void Status_values_match_dump_on_windows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the dump was produced on Windows; posix values are errno based");
        foreach ((string name, long value) in ParseDump().StatusProps)
        {
            if (name == "QUIC_ADDRESS_FAMILY_UNSPEC")
            {
                Assert.Equal(QuicAddressFamily.UNSPEC, value);
                continue;
            }
            FieldInfo? fi = typeof(MsQuicStatus).GetField(name, BindingFlags.Public | BindingFlags.Static);
            Assert.True(fi is not null, $"MsQuicStatus.{name} missing");
            Assert.Equal(value, Convert.ToInt64(fi.GetValue(null), CultureInfo.InvariantCulture));
        }
    }

    [Fact]
    public void Api_table_has_exactly_the_dumped_entries_in_order()
    {
        StructEntry entry = ParseDump().Structs.Single(s => s.Name == "QUIC_API_TABLE");
        FieldInfo[] ours = typeof(QUIC_API_TABLE).GetFields(BindingFlags.Public | BindingFlags.Instance);
        Assert.Equal(entry.Fields.Count, ours.Length);
        for (int i = 0; i < ours.Length; i++)
        {
            Assert.Equal(entry.Fields[i].Field, ours[i].Name);
            Assert.True(ours[i].FieldType.IsFunctionPointer, $"{ours[i].Name} must be a function pointer");
            Assert.True(ours[i].FieldType.IsUnmanagedFunctionPointer, $"{ours[i].Name} must be an unmanaged function pointer");
        }
    }

    /// <summary>QUIC_STATISTICS_V2 is not in the dump (the runtime does not bind it); check it against msquic.h by hand.</summary>
    [Fact]
    public void Statistics_v2_layout_matches_msquic_h()
    {
        Type t = typeof(QUIC_STATISTICS_V2);
        Assert.Equal(208, Marshal.SizeOf<QUIC_STATISTICS_V2>());
        Assert.Equal(0, (int)Marshal.OffsetOf(t, "CorrelationId"));
        Assert.Equal(8, (int)Marshal.OffsetOf(t, "_bitfield"));
        Assert.Equal(12, (int)Marshal.OffsetOf(t, "Rtt"));
        Assert.Equal(24, (int)Marshal.OffsetOf(t, "TimingStart"));
        Assert.Equal(48, (int)Marshal.OffsetOf(t, "HandshakeClientFlight1Bytes"));
        Assert.Equal(60, (int)Marshal.OffsetOf(t, "SendPathMtu"));
        Assert.Equal(64, (int)Marshal.OffsetOf(t, "SendTotalPackets"));
        Assert.Equal(112, (int)Marshal.OffsetOf(t, "SendCongestionCount"));
        Assert.Equal(120, (int)Marshal.OffsetOf(t, "RecvTotalPackets"));
        Assert.Equal(176, (int)Marshal.OffsetOf(t, "RecvValidAckFrames"));
        Assert.Equal(184, (int)Marshal.OffsetOf(t, "KeyUpdateCount"));
        Assert.Equal((int)QUIC_STATISTICS_V2.SIZE_1, (int)Marshal.OffsetOf(t, "KeyUpdateCount") + sizeof(uint));
        Assert.Equal((int)QUIC_STATISTICS_V2.SIZE_2, (int)Marshal.OffsetOf(t, "DestCidUpdateCount") + sizeof(uint));
        Assert.Equal((int)QUIC_STATISTICS_V2.SIZE_3, (int)Marshal.OffsetOf(t, "SendEcnCongestionCount") + sizeof(uint));
        Assert.Equal(200, (int)Marshal.OffsetOf(t, "HandshakeHopLimitTTL"));
        Assert.Equal((int)QUIC_STATISTICS_V2.SIZE_4, (int)Marshal.OffsetOf(t, "RttVariance") + sizeof(uint));

        QUIC_STATISTICS_V2 s = default;
        s._bitfield = 0x7F;
        Assert.True(s.VersionNegotiation && s.StatelessRetry && s.ResumptionAttempted && s.ResumptionSucceeded && s.GreaseBitNegotiated && s.EcnCapable && s.EncryptionOffloaded);
        s._bitfield = 0;
        Assert.False(s.VersionNegotiation || s.StatelessRetry || s.ResumptionAttempted || s.ResumptionSucceeded || s.GreaseBitNegotiated || s.EcnCapable || s.EncryptionOffloaded);
    }

    [Fact]
    public void Extension_tables_and_2_5_handshake_info_have_msquic_h_layout()
    {
        Assert.Equal(248, Marshal.SizeOf<QUIC_API_TABLE>());
        Assert.Equal(8, Marshal.SizeOf<QUIC_API_TABLE_EXT_2_5>());
        Assert.Equal(40, Marshal.SizeOf<QUIC_API_TABLE_PREVIEW_2_5>());
        Assert.Equal(36, Marshal.SizeOf<QUIC_HANDSHAKE_INFO_2_5>());
        Assert.Equal(32, (int)Marshal.OffsetOf<QUIC_HANDSHAKE_INFO_2_5>("TlsGroup"));
        Assert.Equal(28, (int)Marshal.OffsetOf<QUIC_HANDSHAKE_INFO_2_5>("CipherSuite"));
        Assert.Equal(23, (int)QUIC_TLS_GROUP.SECP256R1);
        Assert.Equal(4588, (int)QUIC_TLS_GROUP.X25519MLKEM768);
    }

    [Fact]
    public void Tls_secrets_bitfield_round_trips()
    {
        QUIC_TLS_SECRETS s = default;
        s.IsSet.ClientRandom = true;
        s.IsSet.ClientEarlyTrafficSecret = true;
        s.IsSet.ClientHandshakeTrafficSecret = true;
        s.IsSet.ServerHandshakeTrafficSecret = true;
        s.IsSet.ClientTrafficSecret0 = true;
        s.IsSet.ServerTrafficSecret0 = true;
        Assert.Equal(0x3F, s.IsSet._bitfield);
        Assert.True(s.IsSet.ClientRandom && s.IsSet.ClientEarlyTrafficSecret && s.IsSet.ClientHandshakeTrafficSecret && s.IsSet.ServerHandshakeTrafficSecret && s.IsSet.ClientTrafficSecret0 && s.IsSet.ServerTrafficSecret0);
        s.IsSet.ClientRandom = false;
        s.IsSet.ClientEarlyTrafficSecret = false;
        s.IsSet.ClientHandshakeTrafficSecret = false;
        s.IsSet.ServerHandshakeTrafficSecret = false;
        s.IsSet.ClientTrafficSecret0 = false;
        s.IsSet.ServerTrafficSecret0 = false;
        Assert.Equal(0, s.IsSet._bitfield);
    }

    [Fact]
    public void Event_bitfields_decode_every_bit()
    {
        QUIC_STREAM_EVENT stream = default;
        stream.SHUTDOWN_COMPLETE._bitfield = 0x07;
        Assert.True(stream.SHUTDOWN_COMPLETE.AppCloseInProgress && stream.SHUTDOWN_COMPLETE.ConnectionShutdownByApp && stream.SHUTDOWN_COMPLETE.ConnectionClosedRemotely);
        stream.START_COMPLETE._bitfield = 0x01;
        Assert.True(stream.START_COMPLETE.PeerAccepted);
        QUIC_CONNECTION_EVENT connection = default;
        connection.SHUTDOWN_COMPLETE._bitfield = 0x07;
        Assert.True(connection.SHUTDOWN_COMPLETE.HandshakeCompleted && connection.SHUTDOWN_COMPLETE.PeerAcknowledgedShutdown && connection.SHUTDOWN_COMPLETE.AppCloseInProgress);
        QUIC_LISTENER_EVENT listener = default;
        listener.STOP_COMPLETE._bitfield = 1;
        Assert.True(listener.STOP_COMPLETE.AppCloseInProgress);
        listener.DOS_MODE_CHANGED._bitfield = 1;
        Assert.True(listener.DOS_MODE_CHANGED.DosModeEnabled);
        Assert.True(QuicDatagramSendState.IsFinal(QUIC_DATAGRAM_SEND_STATE.CANCELED));
        Assert.False(QuicDatagramSendState.IsFinal(QUIC_DATAGRAM_SEND_STATE.LOST_SUSPECT));
    }

    [Fact]
    public void Dump_parser_sees_expected_shape()
    {
        Dump dump = ParseDump();
        StructEntry stream = dump.Structs.Single(s => s.Name == "QUIC_STREAM_EVENT+_Anonymous_e__Union+_RECEIVE_e__Struct");
        Assert.Equal(32, stream.Size);
        Assert.Contains((16, "Buffers"), stream.Fields);
        StructEntry hash = dump.Structs.Single(s => s.Name == "QUIC_CERTIFICATE_HASH_STORE");
        Assert.Contains((24, "StoreName"), hash.Fields);
        StructEntry addr = dump.Structs.Single(s => s.Name == "QUIC_ADDR");
        Assert.Equal(3, addr.Fields.Count);
        Assert.Contains(dump.Enums, e => e.Name == "QUIC_SEND_FLAGS" && e.Members.Contains(("FIN", 4L)));
    }
}
