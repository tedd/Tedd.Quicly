using System.Globalization;

namespace Tedd.Quicly.Http.Parsing;

/// <summary>RFC 9110 IMF-fixdate formatting with a once-per-second cache for the <c>Date</c> response header.</summary>
internal static class HttpDate
{
    private sealed class Entry(long second, byte[] bytes)
    {
        public readonly long Second = second;
        public readonly byte[] Bytes = bytes;
    }

    private static Entry s_current = new(-1, new byte[29]);

    /// <summary>Returns the current UTC time formatted as an IMF-fixdate (29 ASCII bytes). Allocates at most once per second.</summary>
    public static ReadOnlySpan<byte> GetCurrentBytes()
    {
        var now = DateTime.UtcNow;
        long second = now.Ticks / TimeSpan.TicksPerSecond;
        var entry = Volatile.Read(ref s_current);
        if (entry.Second != second)
        {
            var bytes = new byte[29];
            now.TryFormat(bytes, out _, "r", CultureInfo.InvariantCulture);
            entry = new Entry(second, bytes);
            Volatile.Write(ref s_current, entry);
        }
        return entry.Bytes;
    }

    /// <summary>Formats <paramref name="value"/> as an IMF-fixdate string.</summary>
    public static string Format(DateTime value) => value.ToUniversalTime().ToString("r", CultureInfo.InvariantCulture);

    /// <summary>Parses an IMF-fixdate (the only format modern clients send). Returns UTC.</summary>
    public static bool TryParse(string value, out DateTime result)
    {
        if (DateTime.TryParseExact(value, "r", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out result))
            return true;
        result = default;
        return false;
    }
}
