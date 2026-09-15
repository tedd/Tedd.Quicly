using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Http.Tls;

/// <summary>
/// Loads a PKCS#12 (PFX) file and optionally polls its last-write time to reload it in place. The private key is
/// imported as exportable into a (non-persisted) key container so that SChannel can use it (ADR 0006).
/// </summary>
public sealed class FileCertificateSource : ICertificateSource, IDisposable
{
    private readonly string _path;
    private readonly string? _password;
    private readonly Timer? _timer;
    private readonly Lock _reloadLock = new();
    private X509Certificate2? _current;
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
        Load();
        if (reloadInterval is { } iv)
            _timer = new Timer(static s => ((FileCertificateSource)s!).Poll(), this, iv, iv);
    }

    /// <summary>Full path of the PFX file.</summary>
    public string FilePath => _path;

    /// <inheritdoc/>
    public X509Certificate2? Current => Volatile.Read(ref _current);

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
            var previous = _current;
            Load();
            bool changed = previous is null || !previous.RawDataMemory.Span.SequenceEqual(_current!.RawDataMemory.Span);
            if (changed)
                Changed?.Invoke(_current!);
            return changed;
        }
    }

    private void Load()
    {
        var info = new FileInfo(_path);
        if (!info.Exists)
            throw new FileNotFoundException("Certificate file not found.", _path);
        var lastWrite = info.LastWriteTimeUtc;
        var cert = X509CertificateLoader.LoadPkcs12FromFile(_path, _password, X509KeyStorageFlags.Exportable);
        Volatile.Write(ref _current, cert);
        _lastWriteUtc = lastWrite;
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
