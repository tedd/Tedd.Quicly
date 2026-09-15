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

    /// <summary>Maps a dump type name (nested with '+') onto our type. Our nesting mirrors the dump exactly.</summary>
    private static Type? ResolveOurType(string dumpName) => s_interop.GetType(OurNamespace + dumpName, throwOnError: false);

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
                Assert.Equal(value, QuicAddressFamily.UNSPEC);
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

    [Fact]
    public void Dump_parser_sees_expected_shape()
    {
        Dump dump = ParseDump();
        StructEntry stream = dump.Structs.Single(s => s.Name == "QUIC_STREAM_EVENT+_Anonymous_e__Union+_RECEIVE_e__Struct");
        Assert.Equal(32, stream.Size);
        Assert.Contains((16, "Buffers"), stream.Fields);
        StructEntry hash = dump.Structs.Single(s => s.Name == "QUIC_CERTIFICATE_HASH_STORE");
        Assert.Contains((24, "StoreName"), hash.Fields);
        StructEntry addr = dump.Structs.Single(s => s.Name == "QuicAddr");
        Assert.Equal(3, addr.Fields.Count);
        Assert.Contains(dump.Enums, e => e.Name == "QUIC_SEND_FLAGS" && e.Members.Contains(("FIN", 4L)));
    }
}
