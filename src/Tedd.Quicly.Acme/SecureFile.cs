namespace Tedd.Quicly.Acme;

/// <summary>
/// Atomic, owner-only file writes for secrets (account key, certificate private key; ADR 0009): the bytes go to a
/// temporary file that is created with mode <c>0600</c> on Unix (so the secret is never readable by others, not even
/// briefly), which is then renamed over the target.
/// </summary>
internal static class SecureFile
{
    /// <summary>Owner read/write, the mode used for every secret file on Unix.</summary>
    internal const UnixFileMode OwnerOnly = UnixFileMode.UserRead | UnixFileMode.UserWrite;

    /// <summary>Writes <paramref name="bytes"/> to <paramref name="path"/> atomically (temp file + rename), creating the directory.</summary>
    public static void WriteAllBytesAtomic(string path, ReadOnlySpan<byte> bytes)
    {
        string fullPath = Path.GetFullPath(path);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        string temp = fullPath + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            using (FileStream stream = new(temp, CreateOptions()))
            {
                stream.Write(bytes);
                stream.Flush(flushToDisk: true);
            }

            RestrictPermissions(temp);
            File.Move(temp, fullPath, overwrite: true);
        }
        catch
        {
            File.Delete(temp);
            throw;
        }
    }

    private static FileStreamOptions CreateOptions()
    {
        FileStreamOptions options = new() { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None };
        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode = OwnerOnly;
        }

        return options;
    }

    /// <summary>Best-effort <c>chmod 0600</c> (covers umask and file systems that ignore the create mode); no-op on Windows.</summary>
    private static void RestrictPermissions(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            return; // NTFS: the file inherits the parent directory ACL (the user profile is owner-only by default).
        }

        try
        {
            File.SetUnixFileMode(path, OwnerOnly);
        }
        catch (IOException)
        {
            // Best effort (FAT / SMB mounts without mode support).
        }
        catch (UnauthorizedAccessException)
        {
            // Best effort.
        }
    }
}
