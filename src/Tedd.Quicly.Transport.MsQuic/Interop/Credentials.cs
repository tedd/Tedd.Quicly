using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>SHA-1 thumbprint of a certificate in the current user's MY store (Schannel only).</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_CERTIFICATE_HASH
{
    public fixed byte ShaHash[20];
}

/// <summary>SHA-1 thumbprint plus store name (Schannel only).</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_CERTIFICATE_HASH_STORE
{
    public QUIC_CERTIFICATE_HASH_STORE_FLAGS Flags;
    public fixed byte ShaHash[20];
    /// <summary>Null-terminated store name.</summary>
    public fixed sbyte StoreName[128];
}

/// <summary>PEM files on disk (OpenSSL only).</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_CERTIFICATE_FILE
{
    public sbyte* PrivateKeyFile;
    public sbyte* CertificateFile;
}

/// <summary>PEM files on disk with a password-protected key (OpenSSL only).</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_CERTIFICATE_FILE_PROTECTED
{
    public sbyte* PrivateKeyFile;
    public sbyte* CertificateFile;
    public sbyte* PrivateKeyPassword;
}

/// <summary>In-memory PKCS#12 blob (all TLS providers).</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_CERTIFICATE_PKCS12
{
    public byte* Asn1Blob;
    public uint Asn1BlobLength;
    /// <summary>Optional null-terminated password; null for none.</summary>
    public sbyte* PrivateKeyPassword;
}

/// <summary>Credential configuration passed to <c>ConfigurationLoadCredential</c>.</summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct QUIC_CREDENTIAL_CONFIG
{
    public QUIC_CREDENTIAL_TYPE Type;
    public QUIC_CREDENTIAL_FLAGS Flags;
    public _Anonymous_e__Union Anonymous;
    public sbyte* Principal;
    public void* Reserved;
    /// <summary>Completion callback used with <see cref="QUIC_CREDENTIAL_FLAGS.LOAD_ASYNCHRONOUS"/>.</summary>
    public delegate* unmanaged[Cdecl]<QUIC_HANDLE*, void*, int, void> AsyncHandler;
    public QUIC_ALLOWED_CIPHER_SUITE_FLAGS AllowedCipherSuites;
    public sbyte* CaCertificateFile;

    /// <summary>Union selecting the certificate source according to <see cref="Type"/>.</summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct _Anonymous_e__Union
    {
        [FieldOffset(0)] public QUIC_CERTIFICATE_HASH* CertificateHash;
        [FieldOffset(0)] public QUIC_CERTIFICATE_HASH_STORE* CertificateHashStore;
        /// <summary>Platform certificate context (<c>PCCERT_CONTEXT</c> on Windows).</summary>
        [FieldOffset(0)] public void* CertificateContext;
        [FieldOffset(0)] public QUIC_CERTIFICATE_FILE* CertificateFile;
        [FieldOffset(0)] public QUIC_CERTIFICATE_FILE_PROTECTED* CertificateFileProtected;
        [FieldOffset(0)] public QUIC_CERTIFICATE_PKCS12* CertificatePkcs12;
    }

    [UnscopedRef] public ref void* CertificateContext => ref Anonymous.CertificateContext;
    [UnscopedRef] public ref QUIC_CERTIFICATE_PKCS12* CertificatePkcs12 => ref Anonymous.CertificatePkcs12;
    [UnscopedRef] public ref QUIC_CERTIFICATE_HASH* CertificateHash => ref Anonymous.CertificateHash;
    [UnscopedRef] public ref QUIC_CERTIFICATE_HASH_STORE* CertificateHashStore => ref Anonymous.CertificateHashStore;
    [UnscopedRef] public ref QUIC_CERTIFICATE_FILE* CertificateFile => ref Anonymous.CertificateFile;
    [UnscopedRef] public ref QUIC_CERTIFICATE_FILE_PROTECTED* CertificateFileProtected => ref Anonymous.CertificateFileProtected;
}
