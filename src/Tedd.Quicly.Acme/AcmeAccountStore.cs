using System.Security.Cryptography;
using System.Text.Json;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme;

/// <summary>How <see cref="AcmeAccountStore"/> protects the file on disk.</summary>
public enum AcmeStoreProtection
{
    /// <summary>Plain JSON; on Unix the file mode is set to <c>0600</c> (best effort). On Windows the parent directory ACL applies.</summary>
    None = 0,

    /// <summary>Windows only: the JSON is encrypted with DPAPI for the current user (<c>CryptProtectData</c>).</summary>
    DpapiCurrentUser = 1,

    /// <summary>Windows only: the JSON is encrypted with DPAPI for the local machine (services running under different accounts).</summary>
    DpapiLocalMachine = 2,
}

/// <summary>
/// Persists the account private key, account URL (<c>kid</c>), directory URL and any in-flight order to a JSON file so
/// the same account is reused across restarts and a crashed order can be resumed. Writes are atomic (temp file +
/// rename); permissions are restricted to the owner on a best-effort basis and the document can optionally be
/// DPAPI-protected on Windows (ADR 0009). Files written with either protection are readable by any store instance.
/// </summary>
public sealed class AcmeAccountStore
{
    /// <summary>Creates a store backed by <paramref name="path"/>.</summary>
    /// <param name="path">JSON file path (created on first <see cref="Save"/>).</param>
    /// <param name="protection">On-disk protection; DPAPI modes require Windows.</param>
    /// <exception cref="PlatformNotSupportedException">A DPAPI mode was requested on a non-Windows platform.</exception>
    public AcmeAccountStore(string path, AcmeStoreProtection protection = AcmeStoreProtection.None)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        if (protection != AcmeStoreProtection.None && !OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException("DPAPI protection is only available on Windows.");
        }

        Path = System.IO.Path.GetFullPath(path);
        Protection = protection;
    }

    /// <summary>Absolute path of the JSON file.</summary>
    public string Path { get; }

    /// <summary>The protection applied when saving.</summary>
    public AcmeStoreProtection Protection { get; }

    /// <summary>True when the file exists.</summary>
    public bool Exists => File.Exists(Path);

    /// <summary>Builds the persisted state from a key and the URLs.</summary>
    public static AcmeAccountState CreateState(AcmeAccountKey key, Uri accountUrl, Uri directoryUrl)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(accountUrl);
        ArgumentNullException.ThrowIfNull(directoryUrl);
        return new AcmeAccountState
        {
            DirectoryUrl = directoryUrl,
            AccountUrl = accountUrl,
            Algorithm = key.Algorithm,
            PrivateKeyPem = key.ExportPem(),
        };
    }

    /// <summary>Loads the stored state, or <see langword="null"/> when the file does not exist.</summary>
    /// <exception cref="InvalidDataException">The file is not a valid account state document (or DPAPI cannot decrypt it).</exception>
    public AcmeAccountState? Load()
    {
        if (!File.Exists(Path))
        {
            return null;
        }

        byte[] bytes = File.ReadAllBytes(Path);
        AcmeAccountState? state;
        try
        {
            AcmeStoreEnvelope? envelope = JsonSerializer.Deserialize(bytes, AcmeJsonContext.Default.AcmeStoreEnvelope);
            if (envelope?.Dpapi is { } protectedBase64)
            {
                bytes = UnprotectDocument(protectedBase64);
            }

            state = JsonSerializer.Deserialize(bytes, AcmeJsonContext.Default.AcmeAccountState);
        }
        catch (JsonException e)
        {
            throw new InvalidDataException("Account store '" + Path + "' is not valid JSON.", e);
        }

        return state ?? throw new InvalidDataException("Account store '" + Path + "' is empty.");
    }

    /// <summary>Loads the stored key, or <see langword="null"/> when the file does not exist.</summary>
    public AcmeAccountKey? LoadKey()
    {
        AcmeAccountState? state = Load();
        return state is null ? null : AcmeAccountKey.Import(state.PrivateKeyPem);
    }

    /// <summary>Writes the state atomically (temp file + rename) and restricts permissions to the owner where supported.</summary>
    public void Save(AcmeAccountState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(Path)!);
        string temp = Path + ".tmp";
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(state, AcmeJsonContext.Default.AcmeAccountState);
        if (Protection != AcmeStoreProtection.None)
        {
            bytes = ProtectDocument(bytes);
        }

        File.WriteAllBytes(temp, bytes);
        RestrictPermissions(temp);
        File.Move(temp, Path, overwrite: true);
    }

    /// <summary>Records (or with <see langword="null"/> clears) the in-flight order on the stored state.</summary>
    /// <exception cref="InvalidOperationException">No account state has been saved yet.</exception>
    public void SavePendingOrder(AcmePendingOrder? pendingOrder)
    {
        AcmeAccountState state = Load() ?? throw new InvalidOperationException("No account state is stored at '" + Path + "'.");
        Save(state with { PendingOrder = pendingOrder });
    }

    /// <summary>Deletes the file if it exists.</summary>
    public void Delete() => File.Delete(Path);

    private byte[] ProtectDocument(byte[] plain)
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException("DPAPI protection is only available on Windows.");
        }

        byte[] blob = Dpapi.Protect(plain, localMachine: Protection == AcmeStoreProtection.DpapiLocalMachine);
        AcmeStoreEnvelope envelope = new() { Dpapi = Convert.ToBase64String(blob) };
        return JsonSerializer.SerializeToUtf8Bytes(envelope, AcmeJsonContext.Default.AcmeStoreEnvelope);
    }

    private byte[] UnprotectDocument(string protectedBase64)
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new InvalidDataException("Account store '" + Path + "' is DPAPI-protected and can only be read on Windows.");
        }

        try
        {
            return Dpapi.Unprotect(Convert.FromBase64String(protectedBase64));
        }
        catch (Exception e) when (e is CryptographicException or FormatException)
        {
            throw new InvalidDataException("Account store '" + Path + "' cannot be decrypted with DPAPI (different user / machine, or corrupt).", e);
        }
    }

    private static void RestrictPermissions(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            return; // NTFS: inherits the parent directory ACL; the user profile is already owner-only by default.
        }

        try
        {
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }
        catch (IOException)
        {
            // Best effort (e.g. FAT / SMB mounts without mode support).
        }
        catch (UnauthorizedAccessException)
        {
            // Best effort.
        }
    }
}
