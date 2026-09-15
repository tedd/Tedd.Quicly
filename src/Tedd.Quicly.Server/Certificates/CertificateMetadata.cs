using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;

namespace Tedd.Quicly.Server.Certificates;

/// <summary>
/// The small file kept next to the persisted certificate (<c>&lt;CertificatePath&gt;.acme.json</c>) that records which ACME
/// directory issued it, so that a certificate from another directory (staging, before the switch to production) is
/// replaced at start-up instead of being served until it is due. It holds no secrets; the SHA-256 thumbprint ties the
/// record to one certificate, so a replaced PFX is never mistaken for the one recorded.
/// </summary>
internal static class CertificateMetadata
{
    private const string DirectoryProperty = "directoryUrl";
    private const string ThumbprintProperty = "sha256";

    /// <summary>The metadata path belonging to a certificate path.</summary>
    public static string PathFor(string certificatePath) => certificatePath + ".acme.json";

    /// <summary>Records that <paramref name="directoryUrl"/> issued <paramref name="certificate"/>, atomically (temporary file, then rename).</summary>
    /// <exception cref="IOException">The file could not be written.</exception>
    /// <exception cref="UnauthorizedAccessException">The file could not be written.</exception>
    public static void Write(string path, Uri directoryUrl, X509Certificate2 certificate)
    {
        using MemoryStream buffer = new();
        using (Utf8JsonWriter writer = new(buffer, new JsonWriterOptions { Indented = true }))
        {
            writer.WriteStartObject();
            writer.WriteString(DirectoryProperty, directoryUrl.AbsoluteUri);
            writer.WriteString(ThumbprintProperty, Thumbprint(certificate));
            writer.WriteEndObject();
        }

        string target = Path.GetFullPath(path);
        string temp = target + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            File.WriteAllBytes(temp, buffer.ToArray());
            File.Move(temp, target, overwrite: true);
        }
        catch
        {
            File.Delete(temp);
            throw;
        }
    }

    /// <summary>The directory recorded for exactly <paramref name="certificate"/>, or <see langword="null"/> when there is no readable record of it.</summary>
    public static Uri? ReadDirectory(string path, X509Certificate2 certificate)
    {
        try
        {
            using JsonDocument document = JsonDocument.Parse(File.ReadAllBytes(path));
            JsonElement root = document.RootElement;
            return root.ValueKind == JsonValueKind.Object
                && root.TryGetProperty(ThumbprintProperty, out JsonElement thumbprint) && thumbprint.ValueKind == JsonValueKind.String
                && string.Equals(thumbprint.GetString(), Thumbprint(certificate), StringComparison.OrdinalIgnoreCase)
                && root.TryGetProperty(DirectoryProperty, out JsonElement url) && url.ValueKind == JsonValueKind.String
                && Uri.TryCreate(url.GetString(), UriKind.Absolute, out Uri? directory)
                ? directory
                : null;
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException or JsonException)
        {
            return null;
        }
    }

    private static string Thumbprint(X509Certificate2 certificate) => certificate.GetCertHashString(HashAlgorithmName.SHA256);
}
