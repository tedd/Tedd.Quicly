namespace Tedd.Quicly.Archive.Acme;

/// <summary>
/// V0 base64url codec (superseded by <c>Tedd.Quicly.Acme.Base64UrlCodec</c>, see docs/benchmarks/acme.md):
/// standard base64 followed by string surgery. Kept so the benchmark can compare against it.
/// </summary>
public static class Base64UrlV0
{
    public static string Encode(ReadOnlySpan<byte> data)
    {
        return Convert.ToBase64String(data).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    public static byte[] Decode(string text)
    {
        string padded = text.Replace('-', '+').Replace('_', '/');
        switch (padded.Length % 4)
        {
            case 2:
                padded += "==";
                break;
            case 3:
                padded += "=";
                break;
        }

        return Convert.FromBase64String(padded);
    }
}
