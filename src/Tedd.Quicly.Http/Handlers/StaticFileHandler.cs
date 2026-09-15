using System.Buffers;
using Tedd.Quicly.Http.Parsing;

namespace Tedd.Quicly.Http.Handlers;

/// <summary>Options for <see cref="StaticFileHandler"/>.</summary>
public sealed class StaticFileOptions
{
    /// <summary>Request path prefix the handler owns (default <c>/</c>).</summary>
    public string RequestPathPrefix { get; set; } = "/";

    /// <summary>File names tried when the path names a directory (default <c>index.html</c>). Directories are never listed.</summary>
    public IList<string> DefaultFileNames { get; } = new List<string> { "index.html" };

    /// <summary>Value for the <c>Cache-Control</c> header, or <see langword="null"/> to omit it.</summary>
    public string? CacheControl { get; set; }

    /// <summary>
    /// Content type for extensions missing from <see cref="ContentTypes"/>. <see langword="null"/> (default) refuses to
    /// serve them, which makes <see cref="ContentTypes"/> an allow-list (ADR 0009).
    /// </summary>
    public string? DefaultContentType { get; set; }

    /// <summary>Largest file served, in bytes; larger files are treated as not found. Default 64 MiB.</summary>
    public long MaxFileSizeBytes { get; set; } = 64L * 1024 * 1024;

    /// <summary>Extension (with leading dot) to content type. Pre-populated with common web types; edit freely.</summary>
    public Dictionary<string, string> ContentTypes { get; } = new(StringComparer.OrdinalIgnoreCase)
    {
        [".html"] = "text/html; charset=utf-8",
        [".htm"] = "text/html; charset=utf-8",
        [".css"] = "text/css; charset=utf-8",
        [".js"] = "text/javascript; charset=utf-8",
        [".mjs"] = "text/javascript; charset=utf-8",
        [".json"] = "application/json; charset=utf-8",
        [".map"] = "application/json; charset=utf-8",
        [".wasm"] = "application/wasm",
        [".txt"] = "text/plain; charset=utf-8",
        [".xml"] = "application/xml; charset=utf-8",
        [".svg"] = "image/svg+xml",
        [".png"] = "image/png",
        [".jpg"] = "image/jpeg",
        [".jpeg"] = "image/jpeg",
        [".gif"] = "image/gif",
        [".webp"] = "image/webp",
        [".ico"] = "image/x-icon",
        [".woff"] = "font/woff",
        [".woff2"] = "font/woff2",
        [".ttf"] = "font/ttf",
        [".otf"] = "font/otf",
        [".pdf"] = "application/pdf",
        [".zip"] = "application/zip",
        [".gz"] = "application/gzip",
        [".mp3"] = "audio/mpeg",
        [".ogg"] = "audio/ogg",
        [".wav"] = "audio/wav",
        [".mp4"] = "video/mp4",
        [".webm"] = "video/webm",
        [".webmanifest"] = "application/manifest+json",
        [".dll"] = "application/octet-stream",
        [".dat"] = "application/octet-stream",
        [".blat"] = "application/octet-stream",
        [".pdb"] = "application/octet-stream",
        [".br"] = "application/octet-stream",
    };
}

/// <summary>
/// Serves files below a root directory for <c>GET</c>/<c>HEAD</c>. Paths are normalised segment by segment
/// (<c>..</c>, backslashes, drive/stream separators and control characters are rejected) and the final
/// full path is verified to lie inside the root; symbolic links and other reparse points anywhere below the
/// root are refused. Only extensions in the content-type allow-list are served, up to
/// <see cref="StaticFileOptions.MaxFileSizeBytes"/>. Supports <c>ETag</c>/<c>Last-Modified</c> with
/// <c>If-None-Match</c>/<c>If-Modified-Since</c> (304) and an optional <c>Cache-Control</c>.
/// Anything not served is left to the next handler (404 by default); directories are never listed.
/// </summary>
public sealed class StaticFileHandler : IHttpHandler
{
    private static readonly SearchValues<char> ForbiddenSegmentChars = SearchValues.Create("\\:\0\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u0009\u000A\u000B\u000C\u000D\u000E\u000F\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001A\u001B\u001C\u001D\u001E\u001F\u007F*?\"<>|");

    private readonly string _root;
    private readonly string _rootWithSeparator;
    private readonly StaticFileOptions _options;

    /// <summary>Creates a handler serving <paramref name="rootDirectory"/>, which must exist.</summary>
    public StaticFileHandler(string rootDirectory, StaticFileOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(rootDirectory);
        _root = Path.GetFullPath(rootDirectory).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        if (!Directory.Exists(_root))
            throw new DirectoryNotFoundException("Static file root not found: " + _root);
        _rootWithSeparator = _root + Path.DirectorySeparatorChar;
        _options = options ?? new StaticFileOptions();
        ArgumentException.ThrowIfNullOrEmpty(_options.RequestPathPrefix);
        if (_options.RequestPathPrefix[0] != '/')
            throw new ArgumentException("RequestPathPrefix must start with '/'.", nameof(options));
        ArgumentOutOfRangeException.ThrowIfNegative(_options.MaxFileSizeBytes);
    }

    /// <summary>Full path of the root directory.</summary>
    public string RootDirectory => _root;

    /// <summary>The options in effect.</summary>
    public StaticFileOptions Options => _options;

    /// <summary>
    /// Resolves a request path (already percent-decoded) to a full file-system path inside the root, or returns
    /// <see langword="false"/> for anything that is malformed or would escape the root.
    /// </summary>
    public bool TryResolve(string requestPath, out string fullPath)
    {
        ArgumentNullException.ThrowIfNull(requestPath);
        fullPath = string.Empty;
        string prefix = _options.RequestPathPrefix;
        ReadOnlySpan<char> rel;
        if (prefix == "/")
        {
            if (requestPath.Length == 0 || requestPath[0] != '/')
                return false;
            rel = requestPath.AsSpan(1);
        }
        else
        {
            if (!requestPath.StartsWith(prefix, StringComparison.Ordinal))
                return false;
            rel = requestPath.AsSpan(prefix.Length);
            if (!prefix.EndsWith('/'))
            {
                if (rel.Length > 0 && rel[0] != '/')
                    return false;
                rel = rel.TrimStart('/');
            }
        }

        var combined = new System.Text.StringBuilder(_rootWithSeparator.Length + rel.Length);
        combined.Append(_root);
        while (!rel.IsEmpty)
        {
            int slash = rel.IndexOf('/');
            var segment = slash < 0 ? rel : rel[..slash];
            rel = slash < 0 ? default : rel[(slash + 1)..];
            if (segment.IsEmpty || segment is ".")
                continue;
            if (segment is ".." || segment.ContainsAny(ForbiddenSegmentChars))
                return false;
            if (segment[^1] == '.' || segment[^1] == ' ')
                return false; // Windows would silently trim these
            combined.Append(Path.DirectorySeparatorChar).Append(segment);
        }

        string candidate = Path.GetFullPath(combined.ToString());
        if (!string.Equals(candidate, _root, StringComparison.Ordinal) && !candidate.StartsWith(_rootWithSeparator, StringComparison.Ordinal))
            return false;
        fullPath = candidate;
        return true;
    }

    /// <summary>Returns the content type for <paramref name="fileName"/>'s extension, or <see langword="null"/> when unknown and no default is configured.</summary>
    public string? GetContentType(string fileName)
    {
        ArgumentNullException.ThrowIfNull(fileName);
        string ext = Path.GetExtension(fileName);
        if (ext.Length > 0 && _options.ContentTypes.TryGetValue(ext, out var ct))
            return ct;
        return _options.DefaultContentType;
    }

    /// <summary>
    /// Whether <paramref name="fullPath"/> (inside the root) or any directory between it and the root is a reparse
    /// point (symbolic link, junction, mount point). The root itself is trusted.
    /// </summary>
    public bool ContainsReparsePoint(string fullPath)
    {
        ArgumentNullException.ThrowIfNull(fullPath);
        FileSystemInfo? info = File.Exists(fullPath) ? new FileInfo(fullPath) : Directory.Exists(fullPath) ? new DirectoryInfo(fullPath) : null;
        while (info is not null && !string.Equals(info.FullName, _root, StringComparison.Ordinal))
        {
            if ((info.Attributes & FileAttributes.ReparsePoint) != 0)
                return true;
            info = info is FileInfo f ? f.Directory : ((DirectoryInfo)info).Parent;
        }
        return false;
    }

    /// <inheritdoc/>
    public async ValueTask<bool> TryHandleAsync(HttpRequestContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (!TryResolve(context.Path, out var fullPath))
            return false;

        FileInfo file;
        try
        {
            file = new FileInfo(fullPath);
            if (!file.Exists)
            {
                if (!Directory.Exists(fullPath))
                    return false;
                file = FindDefaultFile(fullPath)!;
                if (file is null)
                    return false;
            }
            if (ContainsReparsePoint(file.FullName))
                return false;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or System.Security.SecurityException)
        {
            return false;
        }

        var response = context.Response;
        bool isGet = string.Equals(context.Method, "GET", StringComparison.Ordinal);
        if (!isGet && !string.Equals(context.Method, "HEAD", StringComparison.Ordinal))
        {
            response.StatusCode = 405;
            response.Headers.Set("Allow", "GET, HEAD");
            await response.SendTextAsync("405 Method Not Allowed", cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }

        string? contentType = GetContentType(file.Name);
        if (contentType is null)
            return false;
        long length = file.Length;
        if (length > _options.MaxFileSizeBytes)
            return false;

        var lastModified = file.LastWriteTimeUtc;
        lastModified = new DateTime(lastModified.Ticks - (lastModified.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        string etag = "\"" + lastModified.Ticks.ToString("x", System.Globalization.CultureInfo.InvariantCulture) + "-" + length.ToString("x", System.Globalization.CultureInfo.InvariantCulture) + "\"";

        response.Headers.Set("ETag", etag);
        response.Headers.Set("Last-Modified", HttpDate.Format(lastModified));
        if (_options.CacheControl is { } cacheControl)
            response.Headers.Set("Cache-Control", cacheControl);

        if (IsNotModified(context.Headers, etag, lastModified))
        {
            await response.SendStatusAsync(304, cancellationToken).ConfigureAwait(false);
            return true;
        }

        response.ContentType = contentType;
        try
        {
            await response.SendFileAsync(file.FullName, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (!response.HasStarted && ex is IOException or UnauthorizedAccessException)
        {
            // Vanished, locked or unreadable between stat and open: not served (404), never a 500.
            response.Headers.Remove("ETag");
            response.Headers.Remove("Last-Modified");
            response.ContentType = null;
            return false;
        }
        return true;
    }

    private FileInfo? FindDefaultFile(string directory)
    {
        var names = _options.DefaultFileNames;
        for (int i = 0; i < names.Count; i++)
        {
            var candidate = new FileInfo(Path.Combine(directory, names[i]));
            if (candidate.Exists)
                return candidate;
        }
        return null;
    }

    private static bool IsNotModified(HttpHeaderCollection headers, string etag, DateTime lastModified)
    {
        if (headers.TryGetValue("If-None-Match", out var inm))
            return inm.Trim() == "*" || HeaderTokens.ContainsOrdinal(inm, etag);
        if (headers.TryGetValue("If-Modified-Since", out var ims) && HttpDate.TryParse(ims, out var since))
            return lastModified <= since;
        return false;
    }
}
