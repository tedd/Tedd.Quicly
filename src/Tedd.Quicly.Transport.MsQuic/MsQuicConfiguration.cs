using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>How a client validates the server certificate.</summary>
public enum MsQuicCertificateValidation
{
    /// <summary>Platform validation against the system trust store (default). Self-signed certificates are rejected.</summary>
    SystemRoots = 0,
    /// <summary>No validation at all (<c>QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION</c>). Development only.</summary>
    InsecureSkipValidation = 1,
    /// <summary>
    /// Platform validation runs but its verdict is deferred to the application
    /// (<c>INDICATE_CERTIFICATE_RECEIVED | DEFER_CERTIFICATE_VALIDATION | USE_PORTABLE_CERTIFICATES</c>): the
    /// certificate and chain arrive as DER / PKCS#7 bytes in <see cref="IMsQuicConnectionEvents.PeerCertificateReceived"/>
    /// together with the platform result, and the handler accepts, rejects or defers the decision to a later
    /// <see cref="MsQuicConnection.CompleteCertificateValidation"/> call.
    /// </summary>
    Callback = 2,
}

/// <summary>How a server certificate is handed to MsQuic.</summary>
public enum MsQuicServerCredentialMode
{
    /// <summary><see cref="Pkcs12"/> when the private key is exportable, otherwise <see cref="CertificateContext"/> (Windows).</summary>
    Auto = 0,
    /// <summary><c>QUIC_CREDENTIAL_TYPE_CERTIFICATE_PKCS12</c>: the certificate and key are exported to an in-memory PKCS#12 blob (all TLS providers).</summary>
    Pkcs12 = 1,
    /// <summary><c>QUIC_CREDENTIAL_TYPE_CERTIFICATE_CONTEXT</c> (Windows/Schannel only; the key is persisted first, see <see cref="MsQuicCertificateHelper"/>).</summary>
    CertificateContext = 2,
}

/// <summary>
/// An MsQuic configuration: ALPN list + <see cref="QUIC_SETTINGS"/> + credential. Shared by any number of
/// connections (MsQuic reference-counts it). Close it after the connections that use it. A credential can be
/// loaded exactly once per configuration; certificate renewal means opening a new configuration and letting the
/// listener callback pick it (ADR 0009).
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

    /// <summary>The credential type that was loaded (<see cref="QUIC_CREDENTIAL_TYPE.NONE"/> for clients and before loading).</summary>
    public QUIC_CREDENTIAL_TYPE CredentialType { get; private set; }

    /// <summary>The flags the credential was loaded with.</summary>
    public QUIC_CREDENTIAL_FLAGS CredentialFlags { get; private set; }

    /// <summary>
    /// True when the credential asks MsQuic to indicate the peer certificate as portable DER bytes
    /// (<c>INDICATE_CERTIFICATE_RECEIVED | USE_PORTABLE_CERTIFICATES</c>).
    /// </summary>
    public bool IndicatesPortableCertificate => (CredentialFlags & (QUIC_CREDENTIAL_FLAGS.INDICATE_CERTIFICATE_RECEIVED | QUIC_CREDENTIAL_FLAGS.USE_PORTABLE_CERTIFICATES)) == (QUIC_CREDENTIAL_FLAGS.INDICATE_CERTIFICATE_RECEIVED | QUIC_CREDENTIAL_FLAGS.USE_PORTABLE_CERTIFICATES);

    /// <summary>True when the credential defers certificate validation, so a handler may answer with <see cref="MsQuicCertificateDecision.Defer"/>.</summary>
    public bool DefersCertificateValidation => (CredentialFlags & QUIC_CREDENTIAL_FLAGS.DEFER_CERTIFICATE_VALIDATION) != 0;

    /// <summary>Opens a configuration with the given ALPNs and settings builder (<see cref="MsQuicSettings.Default"/> when null).</summary>
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

    /// <summary>Creates a client configuration with the requested validation policy (<see cref="MsQuicSettings.Client"/> defaults when <paramref name="settings"/> is null).</summary>
    public static MsQuicConfiguration CreateClient(MsQuicRegistration registration, ReadOnlySpan<string> alpns, MsQuicCertificateValidation validation = MsQuicCertificateValidation.SystemRoots, MsQuicSettings? settings = null)
    {
        var config = new MsQuicConfiguration(registration, alpns, settings ?? MsQuicSettings.Client());
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
            CredentialType = credential->Type;
            CredentialFlags = credential->Flags;
        }
        return status;
    }

    /// <summary>
    /// Loads a server certificate (must carry a private key). Throws <see cref="MsQuicException"/> on failure.
    /// </summary>
    /// <remarks>
    /// <see cref="MsQuicServerCredentialMode.Auto"/> prefers PKCS#12: the certificate is exported with its key
    /// to an in-memory blob and MsQuic imports it (no store, no persisted key container). The Schannel build
    /// bundled with .NET accepts this path (covered by the loopback tests). When the key is not exportable the
    /// Windows fallback is a certificate context whose key has been persisted with <c>PersistKeySet | UserKeySet</c>;
    /// on other platforms a non-exportable key is an error. <see cref="CredentialType"/> records which path was used.
    /// </remarks>
    public void LoadServerCredential(X509Certificate2 certificate, MsQuicServerCredentialMode mode = MsQuicServerCredentialMode.Auto)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        ObjectDisposedException.ThrowIf(_handle == null, this);
        if (!certificate.HasPrivateKey) throw new ArgumentException("The server certificate must have a private key.", nameof(certificate));

        byte[]? pfx = null;
        if (mode != MsQuicServerCredentialMode.CertificateContext && MsQuicCertificateHelper.TryExportPkcs12(certificate, out pfx))
        {
            try
            {
                LoadPkcs12Credential(pfx);
                return;
            }
            finally
            {
                CryptographicOperations.ZeroMemory(pfx);
            }
        }
        if (mode == MsQuicServerCredentialMode.Pkcs12)
        {
            throw new ArgumentException("The private key is not exportable, so it cannot be handed to MsQuic as PKCS#12.", nameof(certificate));
        }
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException("CERTIFICATE_CONTEXT credentials are Windows only; provide a certificate with an exportable private key.");
        }
        LoadCertificateContextCredential(certificate);
    }

    private void LoadPkcs12Credential(byte[] pfx)
    {
        fixed (byte* p = pfx)
        {
            QUIC_CERTIFICATE_PKCS12 pkcs12 = new() { Asn1Blob = p, Asn1BlobLength = (uint)pfx.Length, PrivateKeyPassword = null };
            QUIC_CREDENTIAL_CONFIG cred = default;
            cred.Type = QUIC_CREDENTIAL_TYPE.CERTIFICATE_PKCS12;
            cred.Flags = QUIC_CREDENTIAL_FLAGS.NONE;
            cred.CertificatePkcs12 = &pkcs12;
            int status = LoadCredential(&cred);
            MsQuicException.ThrowIfFailed(status, "ConfigurationLoadCredential(CERTIFICATE_PKCS12)");
        }
    }

    private void LoadCertificateContextCredential(X509Certificate2 certificate)
    {
        X509Certificate2 usable = MsQuicCertificateHelper.EnsurePersistedPrivateKey(certificate);
        if (!ReferenceEquals(usable, certificate))
        {
            _ownedCertificate?.Dispose();
            _ownedCertificate = usable;
        }
        QUIC_CREDENTIAL_CONFIG cred = default;
        cred.Type = QUIC_CREDENTIAL_TYPE.CERTIFICATE_CONTEXT;
        cred.Flags = QUIC_CREDENTIAL_FLAGS.NONE;
        cred.CertificateContext = (void*)usable.Handle;
        int status = LoadCredential(&cred);
        GC.KeepAlive(usable);
        MsQuicException.ThrowIfFailed(status, "ConfigurationLoadCredential(CERTIFICATE_CONTEXT)");
    }

    /// <summary>Loads a client credential (no client certificate) with the requested validation policy.</summary>
    public void LoadClientCredential(MsQuicCertificateValidation validation = MsQuicCertificateValidation.SystemRoots)
    {
        ObjectDisposedException.ThrowIf(_handle == null, this);
        QUIC_CREDENTIAL_CONFIG cred = default;
        cred.Type = QUIC_CREDENTIAL_TYPE.NONE;
        cred.Flags = validation switch
        {
            MsQuicCertificateValidation.SystemRoots => QUIC_CREDENTIAL_FLAGS.CLIENT,
            MsQuicCertificateValidation.InsecureSkipValidation => QUIC_CREDENTIAL_FLAGS.CLIENT | QUIC_CREDENTIAL_FLAGS.NO_CERTIFICATE_VALIDATION,
            MsQuicCertificateValidation.Callback => QUIC_CREDENTIAL_FLAGS.CLIENT | QUIC_CREDENTIAL_FLAGS.INDICATE_CERTIFICATE_RECEIVED | QUIC_CREDENTIAL_FLAGS.DEFER_CERTIFICATE_VALIDATION | QUIC_CREDENTIAL_FLAGS.USE_PORTABLE_CERTIFICATES,
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
