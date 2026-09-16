using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Http.Tls;

/// <summary>
/// Loads a PKCS#12 (PFX) file and optionally polls its last-write time to reload it in place. The private key is
/// imported with the default key storage flags: not exportable (ADR 0009) and not ephemeral, because SChannel cannot
/// use an ephemeral key. On Windows that means a per-certificate key container, which is removed when the
/// certificate is disposed or finalized.
/// </summary>
/// <remarks>
/// A reload whose file holds the same certificate keeps the existing instance and disposes the fresh import, so
/// touching the file does not accumulate key containers. A certificate that has been replaced is not disposed: TLS
/// session caches, handshakes in progress and <see cref="Changed"/> subscribers may still hold it, so it is left to
/// finalization.
/// </remarks>
public sealed class FileCertificateSource : ICertificateSource, ICertificateChainSource, IDisposable
{
    private readonly string _path;
    private readonly string? _password;
    private readonly Timer? _timer;
    private readonly Lock _reloadLock = new();
    private X509Certificate2? _current;
    private X509Certificate2Collection? _intermediates;
    private DateTime _lastWriteUtc;
    private bool _disposed;

    /// <summary>
    /// Loads <paramref name="path"/> immediately. When <paramref name="reloadInterval"/> is given the file's modification
    /// time is polled at that interval and the certificate reloaded when it changed.
    /// </summary>
    public FileCertificateSource(string path, string? password = null, TimeSpan? reloadInterval = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        if (reloadInterval is { } interval)
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        _path = Path.GetFullPath(path);
        _password = password;
        _current = Load(out _lastWriteUtc, out _intermediates);
        if (reloadInterval is { } iv)
            _timer = new Timer(static s => ((FileCertificateSource)s!).Poll(), this, iv, iv);
    }

    /// <summary>Full path of the PFX file.</summary>
    public string FilePath => _path;

    /// <inheritdoc/>
    public X509Certificate2? Current => Volatile.Read(ref _current);

    /// <inheritdoc/>
    public X509Certificate2Collection? GetIntermediates(X509Certificate2 certificate)
        => ReferenceEquals(certificate, Volatile.Read(ref _current)) ? Volatile.Read(ref _intermediates) : null;

    /// <inheritdoc/>
    public event Action<X509Certificate2>? Changed;

    /// <summary>Raised when a reload attempt fails (the previous certificate stays in use).</summary>
    public event Action<Exception>? ReloadFailed;

    /// <summary>Re-reads the file now. Returns <see langword="true"/> when a different certificate was loaded.</summary>
    public bool Reload()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_reloadLock)
        {
            var previous = _current!;
            var loaded = Load(out var lastWrite, out var intermediates);
            _lastWriteUtc = lastWrite;
            if (previous.RawDataMemory.Span.SequenceEqual(loaded.RawDataMemory.Span))
            {
                loaded.Dispose(); // same certificate: keep the instance in use, release the duplicate key container
                return false;
            }
            Volatile.Write(ref _intermediates, intermediates);
            Volatile.Write(ref _current, loaded);
            Changed?.Invoke(loaded);
            return true;
        }
    }

    /// <summary>
    /// Loads the whole PKCS#12: the certificate with the private key is served, the others are the intermediates a handshake
    /// has to send (it never downloads them).
    /// </summary>
    private X509Certificate2 Load(out DateTime lastWriteUtc, out X509Certificate2Collection intermediates)
    {
        var info = new FileInfo(_path);
        if (!info.Exists)
            throw new FileNotFoundException("Certificate file not found.", _path);
        lastWriteUtc = info.LastWriteTimeUtc;
        var all = X509CertificateLoader.LoadPkcs12CollectionFromFile(_path, _password, X509KeyStorageFlags.DefaultKeySet);
        X509Certificate2? leaf = null;
        foreach (var certificate in all)
        {
            if (certificate.HasPrivateKey)
            {
                leaf = certificate;
                break;
            }
        }
        if (leaf is null && all.Count > 0)
            leaf = all[0];
        if (leaf is null)
            throw new System.Security.Cryptography.CryptographicException("The certificate file contains no certificate.");
        intermediates = [];
        foreach (var certificate in all)
        {
            if (!ReferenceEquals(certificate, leaf))
                intermediates.Add(certificate);
        }
        return leaf;
    }

    internal void Poll()
    {
        try
        {
            if (_disposed)
                return;
            var lastWrite = File.GetLastWriteTimeUtc(_path);
            if (lastWrite == _lastWriteUtc)
                return;
            Reload();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or System.Security.Cryptography.CryptographicException or ObjectDisposedException)
        {
            ReloadFailed?.Invoke(ex);
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _disposed = true;
        _timer?.Dispose();
    }
}
