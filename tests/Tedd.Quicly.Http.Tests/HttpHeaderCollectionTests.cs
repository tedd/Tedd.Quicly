using System.Text;
using Tedd.Quicly.Http.Internal;

namespace Tedd.Quicly.Http.Tests;

public class HttpHeaderCollectionTests
{
    [Fact]
    public void Add_get_set_remove()
    {
        var h = new HttpHeaderCollection(2);
        Assert.Equal(0, h.Count);
        h.Add("Host", "a");
        h.Add("X-A", "1");
        h.Add("x-a", "2");
        Assert.Equal(3, h.Count);
        Assert.Equal("1", h["X-A"]);
        Assert.True(h.TryGetValue("X-a", out var v));
        Assert.Equal("1", v);
        Assert.False(h.TryGetValue("nope", out v));
        Assert.Equal(string.Empty, v);
        Assert.Null(h["nope"]);
        Assert.True(h.Contains("host"));
        Assert.False(h.Contains("cookie"));
        Assert.Equal(new HttpHeader("X-A", "1"), h[1]);

        h.Set("x-a", "3");
        Assert.Equal(2, h.Count);
        Assert.Equal("3", h["X-A"]);
        Assert.Equal("Host", h[0].Name);

        h["Cookie"] = "c";
        Assert.Equal("c", h["Cookie"]);
        h["Cookie"] = null;
        Assert.False(h.Contains("Cookie"));

        Assert.True(h.Remove("HOST"));
        Assert.False(h.Remove("HOST"));
        Assert.Equal(1, h.Count);
        h.Clear();
        Assert.Equal(0, h.Count);
    }

    [Fact]
    public void Enumerates()
    {
        var h = new HttpHeaderCollection(0);
        h.Add("A", "1");
        h.Add("B", "2");
        var names = new List<string>();
        foreach (var header in h)
            names.Add(header.Name + "=" + header.Value);
        Assert.Equal(["A=1", "B=2"], names);

        var viaInterface = new List<HttpHeader>();
        foreach (var header in (IEnumerable<HttpHeader>)h)
            viaInterface.Add(header);
        Assert.Equal(2, viaInterface.Count);

        var nonGeneric = ((System.Collections.IEnumerable)h).GetEnumerator();
        Assert.True(nonGeneric.MoveNext());
        Assert.Equal(new HttpHeader("A", "1"), nonGeneric.Current);
        nonGeneric.Reset();
        Assert.True(nonGeneric.MoveNext());
        ((IDisposable)nonGeneric).Dispose();
    }

    [Fact]
    public void Validates_input()
    {
        var h = new HttpHeaderCollection();
        Assert.Throws<ArgumentException>(() => h.Add("", "v"));
        Assert.Throws<ArgumentNullException>(() => h.Add(null!, "v"));
        Assert.Throws<ArgumentNullException>(() => h.Add("A", null!));
        Assert.Throws<ArgumentException>(() => h.Add("A B", "v"));
        Assert.Throws<ArgumentException>(() => h.Add("A:B", "v"));
        Assert.Throws<ArgumentException>(() => h.Add("A", "v\r\nX: y"));
        Assert.Throws<ArgumentException>(() => h.Add("A", "v\0"));
        Assert.Throws<ArgumentNullException>(() => h.TryGetValue(null!, out _));
        Assert.Throws<ArgumentNullException>(() => h.Remove(null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => new HttpHeaderCollection(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => h[0]);
        Assert.Throws<ArgumentOutOfRangeException>(() => h[-1]);
    }

    [Fact]
    public void Grows_beyond_initial_capacity()
    {
        var h = new HttpHeaderCollection(1);
        for (int i = 0; i < 40; i++)
            h.Add("H" + i, i.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(40, h.Count);
        Assert.Equal("39", h["H39"]);
    }

    [Fact]
    public void Steady_state_lookup_does_not_allocate()
    {
        var h = new HttpHeaderCollection(8);
        h.Add("Host", "a");
        h.Add("Connection", "keep-alive");
        h.Add("Accept", "*/*");
        for (int i = 0; i < 1000; i++)
            h.TryGetValue("Accept", out _);
        int found = 0;
        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 10_000; i++)
            {
                if (h.TryGetValue("accept", out _)) found++;
                foreach (var x in h) found += x.Name.Length;
            }
        });
        Assert.True(found > 0);
    }
}

public class ByteBufferWriterTests
{
    [Fact]
    public void Appends_and_grows()
    {
        using var w = new ByteBufferWriter(8);
        w.Append("HTTP/1.1 "u8);
        w.AppendInt64(200);
        w.Append((byte)' ');
        w.AppendLatin1("OK é中");
        w.AppendCrLf();
        w.AppendHex(255);
        w.Append(new byte[100]);
        Assert.True(w.Capacity >= w.Length);
        var text = Encoding.Latin1.GetString(w.WrittenSpan[..w.WrittenSpan.IndexOf((byte)0)]);
        Assert.Equal("HTTP/1.1 200 OK é?\r\nff", text);
        Assert.Equal(w.Length, w.WrittenMemory.Length);
        w.Clear();
        Assert.Equal(0, w.Length);
    }

    [Fact]
    public void Dispose_is_idempotent()
    {
        var w = new ByteBufferWriter(16);
        w.Append((byte)1);
        w.Dispose();
        w.Dispose();
        Assert.Equal(0, w.Length);
    }

    [Fact]
    public void Steady_state_header_serialisation_does_not_allocate()
    {
        // The writer lives in a one-element array: a window delegate cannot capture the `ref` the writes take.
        ByteBufferWriter[] box = [new ByteBufferWriter(1024)];
        try
        {
            for (int i = 0; i < 1000; i++)
                Write(ref box[0]);
            WindowedAllocation.AssertNone(() =>
            {
                for (int i = 0; i < 10_000; i++)
                    Write(ref box[0]);
            });
        }
        finally
        {
            box[0].Dispose();
        }

        static void Write(ref ByteBufferWriter w)
        {
            w.Clear();
            w.Append("HTTP/1.1 "u8);
            w.AppendInt64(200);
            w.Append((byte)' ');
            w.AppendLatin1(HttpReasonPhrases.Get(200));
            w.AppendCrLf();
            w.Append("Content-Length: "u8);
            w.AppendInt64(1234567);
            w.AppendCrLf();
            w.AppendHex(0x1fffe);
            w.AppendCrLf();
        }
    }
}
