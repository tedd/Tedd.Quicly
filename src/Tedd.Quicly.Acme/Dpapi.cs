using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.Cryptography;

namespace Tedd.Quicly.Acme;

/// <summary>
/// Windows Data Protection API (<c>crypt32!CryptProtectData</c>) bound directly so no out-of-box package is needed.
/// Protects the account key file for the current user (ADR 0009).
/// </summary>
[SupportedOSPlatform("windows")]
internal static unsafe partial class Dpapi
{
    private const uint CryptProtectUiForbidden = 0x1;
    private const uint CryptProtectLocalMachine = 0x4;

    [StructLayout(LayoutKind.Sequential)]
    private struct DataBlob
    {
        public uint Length;
        public byte* Data;
    }

    [LibraryImport("crypt32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool CryptProtectData(DataBlob* input, char* description, DataBlob* entropy, void* reserved, void* prompt, uint flags, DataBlob* output);

    [LibraryImport("crypt32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool CryptUnprotectData(DataBlob* input, char** description, DataBlob* entropy, void* reserved, void* prompt, uint flags, DataBlob* output);

    [LibraryImport("kernel32.dll")]
    private static partial void* LocalFree(void* memory);

    /// <summary>Encrypts <paramref name="plaintext"/> for the current user (or the machine).</summary>
    /// <exception cref="CryptographicException">DPAPI failed.</exception>
    public static byte[] Protect(ReadOnlySpan<byte> plaintext, bool localMachine)
    {
        uint flags = CryptProtectUiForbidden | (localMachine ? CryptProtectLocalMachine : 0);
        fixed (byte* p = plaintext)
        {
            DataBlob input = new() { Length = (uint)plaintext.Length, Data = p };
            DataBlob output = default;
            if (!CryptProtectData(&input, null, null, null, null, flags, &output))
            {
                throw new CryptographicException(Marshal.GetLastPInvokeError());
            }

            return CopyAndFree(output);
        }
    }

    /// <summary>Decrypts a blob produced by <see cref="Protect"/>.</summary>
    /// <exception cref="CryptographicException">DPAPI failed (wrong user / machine, or corrupt data).</exception>
    public static byte[] Unprotect(ReadOnlySpan<byte> ciphertext)
    {
        fixed (byte* p = ciphertext)
        {
            DataBlob input = new() { Length = (uint)ciphertext.Length, Data = p };
            DataBlob output = default;
            if (!CryptUnprotectData(&input, null, null, null, null, CryptProtectUiForbidden, &output))
            {
                throw new CryptographicException(Marshal.GetLastPInvokeError());
            }

            return CopyAndFree(output);
        }
    }

    private static byte[] CopyAndFree(DataBlob blob)
    {
        try
        {
            return new ReadOnlySpan<byte>(blob.Data, (int)blob.Length).ToArray();
        }
        finally
        {
            LocalFree(blob.Data);
        }
    }
}
