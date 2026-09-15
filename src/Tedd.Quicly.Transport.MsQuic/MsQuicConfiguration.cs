using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>How a client validates the server certificate.</summary>
public enum MsQuicCertificateValidation
{
    /// <summary>Platform validation (system trust store). Self-signed certificates are rejected.</summary>
    System = 0,
    /// <summary>No validation at all (<c>QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION</c>). Development only.</summary>
    InsecureSkipValidation = 1,
    /// <summary>
    /// The certificate is delivered as DER to <see cref="IMsQuicConnectionEvents.PeerCertificateReceived"/> and the
    /// handler decides (<c>INDICATE_CERTIFICATE_RECEIVED | NO_CERTIFICATE_VALIDATION | USE_PORTABLE_CERTIFICATES</c>).
    /// </summary>
    Custom = 2,
}

/// <summary>How a server certificate is handed to MsQuic.</summary>
public enum MsQuicServerCredentialMode
{
    /// <summary><see cref="CertificateContext"/> on Windows, <see cref="Pkcs12"/> elsewhere.</summary>
    Auto = 0,
    /// <summary><c>QUIC_CREDENTIAL_TYPE_CERTIFICATE_CONTEXT</c> (Windows/Schannel; the key is persisted first, see <see cref="MsQuicCertificateHelper"/>).</summary>
    CertificateContext = 1,
    /// <summary><c>QUIC_CREDENTIAL_TYPE_CERTIFICATE_PKCS12</c> (all platforms).</summary>
    Pkcs12 = 2,
}

/// <summary>
/// An MsQuic configuration: ALPN list + <see cref="QUIC_SETTINGS"/> + credential. Shared by any number of
/// connections. Close it after the connections that use it.
/// </summary>
public sealed unsafe class MsQuicConfiguration : IDisposable
{
    private QUIC_HANDLE* _handle;
    private X509Certificate2? _ownedCertificate;

    /// <summary>Owning registration.</summary>
    public MsQuicRegistration Registration { get; }

    /// <summary>The native handle (null after <see cref="Close"/>).</summary>
    public QUIC_HANDLE* Handle => _handle;

    /// <summary>True once <see cref="Close"/> has run.</summary>
    public bool IsClosed => _handle == null;

    /// <summary>The ALPN protocols this configuration was opened with.</summary>
    public string[] Alpns { get; }

    /// <summary>True when a credential has been loaded.</summary>
    public bool HasCredential { get; private set; }

    /// <summary>
    /// True when the loaded credential asks MsQuic to indicate the peer certificate as portable DER bytes
    /// (<c>INDICATE_CERTIFICATE_RECEIVED | USE_PORTABLE_CERTIFICATES</c>). Connections use it to decode
    /// <c>PEER_CERTIFICATE_RECEIVED</c>.
    /// </summary>
    public bool IndicatesPortableCertificate { get; private set; }

    /// <summary>Opens a configuration with the given ALPNs and settings builder (defaults when null).</summary>
    public MsQuicConfiguration(MsQuicRegistration registration, ReadOnlySpan<string> alpns, MsQuicSettings? settings = null)
        : this(registration, alpns, (settings ?? MsQuicSettings.Default).ToNative())
    {
    }

    /// <summary>Opens a configuration with the given ALPNs and native settings.</summary>
    public MsQuicConfiguration(MsQuicRegistration registration, ReadOnlySpan<string> alpns, in QUIC_SETTINGS settings)
    {
        ArgumentNullException.ThrowIfNull(registration);
        ObjectDisposedException.ThrowIf(registration.IsClosed, registration);
        if (alpns.Length == 0) throw new ArgumentException("At least one ALPN is required.", nameof(alpns));
        Registration = registration;
        Alpns = alpns.ToArray();
        AlpnList list = AlpnList.Create(alpns);
        try
        {
            QUIC_HANDLE* handle = null;
            fixed (QUIC_SETTINGS* s = &settings)
            {
                int status = registration.Api.Table->ConfigurationOpen(registration.Handle, list.Buffers, list.Count, s, (uint)sizeof(QUIC_SETTINGS), null, &handle);
                MsQuicException.ThrowIfFailed(status, "ConfigurationOpen");
            }
            _handle = handle;
        }
        finally
        {
            list.Free();
        }
    }

    /// <summary>Creates a server configuration with a certificate loaded.</summary>
    public static MsQuicConfiguration CreateServer(MsQuicRegistration registration, ReadOnlySpan<string> alpns, X509Certificate2 certificate, MsQuicSettings? settings = null, MsQuicServerCredentialMode mode = MsQuicServerCredentialMode.Auto)
    {
        var config = new MsQuicConfiguration(registration, alpns, settings);
        try
        {
            config.LoadServerCredential(certificate, mode);
            return config;
        }
        catch
        {
            config.Close();
            throw;
        }
    }

    /// <summary>Creates a client configuration with the requested validation policy.</summary>
    public static MsQuicConfiguration CreateClient(MsQuicRegistration registration, ReadOnlySpan<string> alpns, MsQuicCertificateValidation validation = MsQuicCertificateValidation.System, MsQuicSettings? settings = null)
    {
        var config = new MsQuicConfiguration(registration, alpns, settings);
        try
        {
            config.LoadClientCredential(validation);
            return config;
        }
        catch
        {
            config.Close();
            throw;
        }
    }

    /// <summary>Raw <c>ConfigurationLoadCredential</c>; returns the status. For credential types the helpers do not cover.</summary>
    public int LoadCredential(QUIC_CREDENTIAL_CONFIG* credential)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        int status = Registration.Api.Table->ConfigurationLoadCredential(_handle, credential);
        if (MsQuicStatus.Succeeded(status))
        {
            HasCredential = true;
            const QUIC_CREDENTIAL_FLAGS portable = QUIC_CREDENTIAL_FLAGS.INDICATE_CERTIFICATE_RECEIVED | QUIC_CREDENTIAL_FLAGS.USE_PORTABLE_CERTIFICATES;
            IndicatesPortableCertificate = (credential->Flags & portable) == portable;
        }
        return status;
    }

    /// <summary>Loads a server certificate (must carry a private key). Throws <see cref="MsQuicException"/> on failure.</summary>
    public void LoadServerCredential(X509Certificate2 certificate, MsQuicServerCredentialMode mode = MsQuicServerCredentialMode.Auto)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        ObjectDisposedException.ThrowIf(_handle == null, this);
        if (!certificate.HasPrivateKey) throw new ArgumentException("The server certificate must have a private key.", nameof(certificate));
        if (mode == MsQuicServerCredentialMode.Auto)
        {
            mode = OperatingSystem.IsWindows() ? MsQuicServerCredentialMode.CertificateContext : MsQuicServerCredentialMode.Pkcs12;
        }

        QUIC_CREDENTIAL_CONFIG cred = default;
        cred.Flags = QUIC_CREDENTIAL_FLAGS.NONE;
        if (mode == MsQuicServerCredentialMode.CertificateContext)
        {
            if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("CERTIFICATE_CONTEXT credentials are Windows only.");
            X509Certificate2 usable = MsQuicCertificateHelper.EnsurePersistedPrivateKey(certificate);
            if (!ReferenceEquals(usable, certificate))
            {
                _ownedCertificate?.Dispose();
                _ownedCertificate = usable;
            }
            cred.Type = QUIC_CREDENTIAL_TYPE.CERTIFICATE_CONTEXT;
            cred.CertificateContext = (void*)usable.Handle;
            int status = LoadCredential(&cred);
            GC.KeepAlive(usable);
            MsQuicException.ThrowIfFailed(status, "ConfigurationLoadCredential(CERTIFICATE_CONTEXT)");
            return;
        }

        byte[] pfx = certificate.Export(X509ContentType.Pkcs12);
        try
        {
            fixed (byte* p = pfx)
            {
                QUIC_CERTIFICATE_PKCS12 pkcs12 = new() { Asn1Blob = p, Asn1BlobLength = (uint)pfx.Length, PrivateKeyPassword = null };
                cred.Type = QUIC_CREDENTIAL_TYPE.CERTIFICATE_PKCS12;
                cred.CertificatePkcs12 = &pkcs12;
                int status = LoadCredential(&cred);
                MsQuicException.ThrowIfFailed(status, "ConfigurationLoadCredential(CERTIFICATE_PKCS12)");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pfx);
        }
    }

    /// <summary>Loads a client credential (no client certificate) with the requested validation policy.</summary>
    public void LoadClientCredential(MsQuicCertificateValidation validation = MsQuicCertificateValidation.System)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        QUIC_CREDENTIAL_CONFIG cred = default;
        cred.Type = QUIC_CREDENTIAL_TYPE.NONE;
        cred.Flags = validation switch
        {
            MsQuicCertificateValidation.InsecureSkipValidation => QUIC_CREDENTIAL_FLAGS.CLIENT | QUIC_CREDENTIAL_FLAGS.NO_CERTIFICATE_VALIDATION,
            MsQuicCertificateValidation.Custom => QUIC_CREDENTIAL_FLAGS.CLIENT | QUIC_CREDENTIAL_FLAGS.INDICATE_CERTIFICATE_RECEIVED | QUIC_CREDENTIAL_FLAGS.NO_CERTIFICATE_VALIDATION | QUIC_CREDENTIAL_FLAGS.USE_PORTABLE_CERTIFICATES,
            MsQuicCertificateValidation.System => QUIC_CREDENTIAL_FLAGS.CLIENT,
            _ => throw new ArgumentOutOfRangeException(nameof(validation)),
        };
        int status = LoadCredential(&cred);
        MsQuicException.ThrowIfFailed(status, "ConfigurationLoadCredential(client)");
    }

    /// <summary>Closes the configuration. Idempotent.</summary>
    public void Close()
    {
        QUIC_HANDLE* handle = _handle;
        if (handle == null) return;
        _handle = null;
        Registration.Api.Table->ConfigurationClose(handle);
        _ownedCertificate?.Dispose();
        _ownedCertificate = null;
    }

    /// <inheritdoc cref="Close"/>
    public void Dispose() => Close();
}

/// <summary>A native array of <see cref="QUIC_BUFFER"/> holding UTF-8 ALPN identifiers (one allocation).</summary>
internal unsafe struct AlpnList
{
    public QUIC_BUFFER* Buffers;
    public uint Count;
    private void* _block;

    public static AlpnList Create(ReadOnlySpan<string> alpns)
    {
        int total = 0;
        for (int i = 0; i < alpns.Length; i++)
        {
            int n = Encoding.UTF8.GetByteCount(alpns[i]);
            if (n is < 1 or > 255) throw new ArgumentException($"ALPN '{alpns[i]}' must be 1..255 bytes.", nameof(alpns));
            total += n;
        }
        nuint headerBytes = (nuint)(sizeof(QUIC_BUFFER) * alpns.Length);
        void* block = NativeMemory.Alloc(headerBytes + (nuint)total);
        var buffers = (QUIC_BUFFER*)block;
        byte* data = (byte*)block + headerBytes;
        for (int i = 0; i < alpns.Length; i++)
        {
            int n = Encoding.UTF8.GetBytes(alpns[i], new Span<byte>(data, total));
            buffers[i] = new QUIC_BUFFER(data, (uint)n);
            data += n;
            total -= n;
        }
        return new AlpnList { Buffers = buffers, Count = (uint)alpns.Length, _block = block };
    }

    public void Free()
    {
        if (_block != null)
        {
            NativeMemory.Free(_block);
            _block = null;
            Buffers = null;
            Count = 0;
        }
    }
}
