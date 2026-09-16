using System.Net;
using System.Text;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Http;
using Tedd.Quicly.Server.Certificates;

namespace Tedd.Quicly.Server;

/// <summary>
/// Configuration of a <see cref="QuiclyServer"/> (ARCHITECTURE.md §6.1 and §9, ADR 0009). The server reads it once, in its
/// constructor: later changes to this instance (or to the objects it holds) do not affect a server that already exists.
/// </summary>
public sealed class ServerOptions
{
    /// <summary>Default <see cref="ExpectedPeers"/>.</summary>
    public const int DefaultExpectedPeers = 64;

    /// <summary>Default <see cref="MaxPeers"/>.</summary>
    public const int DefaultMaxPeers = 1024;

    /// <summary>Largest accepted <see cref="MaxPeers"/>, <see cref="ExpectedPeers"/> and unadmitted-connection cap.</summary>
    public const int PeerLimit = 1 << 20;

    /// <summary>
    /// Where the QUIC listener binds (UDP). <see cref="QuiclyServer"/> does not bind it itself: the
    /// <see cref="Core.Transport.ITransportListener"/> it is given does, so a host reads this value when it creates that
    /// listener (for example the MsQuic listener). Its port is also the HTTPS port the HTTP side endpoint redirects to.
    /// Default <c>[::]:443</c>.
    /// </summary>
    public IPEndPoint ListenEndPoint { get; set; } = new(IPAddress.IPv6Any, 443);

    /// <summary>The channel table every client must match (PROTOCOL.md §1). Required.</summary>
    public ChannelTable? Channels { get; set; }

    /// <summary>
    /// How many concurrent peers the server is sized for (ARCHITECTURE.md §9): the shared buffer pool and the per-peer
    /// tables and budgets that still hold their <see cref="Core.Session.PeerOptions"/> defaults scale with it (see
    /// <see cref="ServerSizing"/>). Default 64.
    /// </summary>
    public int ExpectedPeers { get; set; } = DefaultExpectedPeers;

    /// <summary>Most admitted sessions at once; a further Hello is answered <see cref="HelloStatus.ServerFull"/>. Default 1 024.</summary>
    public int MaxPeers { get; set; } = DefaultMaxPeers;

    /// <summary>
    /// Template of every server peer (copied once). The server replaces a few values: <see cref="PeerOptions.Allocator"/>
    /// (a shared pool unless the template names one), <see cref="PeerOptions.SessionGrace"/> (from
    /// <see cref="ServerSessionOptions.Grace"/>), the client-only resume values, and the table and budget sizes still at
    /// their defaults (<see cref="ServerSizing"/>). <see cref="PeerOptions.AutoFlushInterval"/> is implemented by
    /// <see cref="QuiclyServer.PollAll"/>, not by the peers.
    /// </summary>
    public PeerOptions PeerOptions { get; set; } = new();

    /// <summary>Connection and admission limits, the auth-token validator and the auth-failure rate limiter.</summary>
    public ServerAdmissionOptions Admission { get; } = new();

    /// <summary>Session token key and the resume grace period.</summary>
    public ServerSessionOptions Sessions { get; } = new();

    /// <summary>
    /// Server certificate source (static, file or ACME). When set, <see cref="QuiclyServer.StartAsync"/> runs a
    /// <see cref="CertificateProvisioner"/> and binds it to <see cref="CertificateConsumers"/>. <see langword="null"/> (the
    /// default) leaves certificates to whoever created the listener.
    /// </summary>
    public ServerCertificateOptions? Certificate { get; set; }

    /// <summary>
    /// What presents the certificate, typically the QUIC listener's <c>UpdateCertificate</c> method through
    /// <see cref="Certificates.CertificateConsumers.FromDelegate"/>. Each consumer gets the current certificate when the
    /// server starts and again on every renewal.
    /// </summary>
    public IList<ICertificateConsumer> CertificateConsumers { get; } = new List<ICertificateConsumer>();

    /// <summary>Options of the <see cref="CertificateBinder"/> (grace period of superseded certificates); <see langword="null"/> for the defaults.</summary>
    public CertificateBinderOptions? CertificateBinding { get; set; }

    /// <summary>
    /// When a <see cref="Certificate"/> is configured, <see cref="QuiclyServer.StartAsync"/> waits for the first certificate
    /// before it starts the listener (bounded by its cancellation token). Default <see langword="true"/>.
    /// </summary>
    public bool WaitForCertificate { get; set; } = true;

    /// <summary>
    /// The plain-HTTP side endpoint (health and redirect to HTTPS, ADR 0009), or <see langword="null"/> (the default) for
    /// none. When ACME serves <c>http-01</c> it already runs that endpoint; configure health and redirect there instead.
    /// </summary>
    public ServerHttpOptions? Http { get; set; }

    /// <summary>
    /// Code and reason every peer is closed with by <see cref="QuiclyServer.BeginShutdown"/>. Default
    /// <see cref="QuiclyErrorCode.NoError"/>, "server shutting down".
    /// </summary>
    public CloseReason ShutdownReason { get; set; } = new(QuiclyErrorCode.NoError, "server shutting down");

    /// <summary>How long <see cref="QuiclyServer.StopAsync"/> waits for peers to close before it disposes the rest. Default 5 s.</summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Checks every value.</summary>
    /// <exception cref="ArgumentException">A value is missing or out of range (the message says which).</exception>
    internal void Validate()
    {
        if (ListenEndPoint is null)
        {
            throw new ArgumentException("A listen endpoint is required.", nameof(ListenEndPoint));
        }

        if (Channels is null)
        {
            throw new ArgumentException("A channel table is required.", nameof(Channels));
        }

        if (PeerOptions is null)
        {
            throw new ArgumentException("A peer options template is required.", nameof(PeerOptions));
        }

        OptionChecks.Range(ExpectedPeers, 1, PeerLimit, nameof(ExpectedPeers));
        OptionChecks.Range(MaxPeers, 1, PeerLimit, nameof(MaxPeers));
        OptionChecks.NonNegative(ShutdownTimeout, nameof(ShutdownTimeout));
        OptionChecks.CheckCloseReason(ShutdownReason, nameof(ShutdownReason));
        Admission.Validate();
        Sessions.Validate();
        Http?.Validate();
        for (int i = 0; i < CertificateConsumers.Count; i++)
        {
            if (CertificateConsumers[i] is null)
            {
                throw new ArgumentException("CertificateConsumers contains null.", nameof(CertificateConsumers));
            }
        }

        if (Http is { } http && Certificate?.AcmeOptions is { } acme && acme.ChallengeTypes.Contains(AcmeChallengeKind.Http01)
            && OptionChecks.SharePort(http.EndPoint, acme.HttpChallengeEndpoint))
        {
            throw new ArgumentException(
                "The ACME http-01 endpoint (" + acme.HttpChallengeEndpoint + ") already serves plain HTTP on the port of Http.EndPoint ("
                + http.EndPoint + "). Configure health and redirect through AcmeProvisioningOptions (EnableHealthEndpoint, RedirectToHttps), "
                + "or give the side endpoint another port.",
                nameof(Http));
        }
    }
}

/// <summary>
/// Connection limits and admission rules of a <see cref="QuiclyServer"/> (ADR 0009). Enforced by the server and by
/// <see cref="DefaultAdmissionPolicy"/>.
/// </summary>
public sealed class ServerAdmissionOptions
{
    /// <summary>The raw-QUIC ALPN of QUICLY version 1.</summary>
    public const string DefaultAlpn = "quicly/1";

    /// <summary>
    /// Live connections (admitted or not) from one address: IPv4 (and IPv4-mapped IPv6) per address, IPv6 per
    /// <see cref="IPv6PrefixLength"/> prefix. Checked before the handshake and again when a peer's address changes
    /// (migration), which closes the connection with <see cref="QuiclyErrorCode.LimitExceeded"/>. Default 16.
    /// </summary>
    public int MaxConnectionsPerAddress { get; set; } = 16;

    /// <summary>IPv6 addresses sharing this many leading bits count as one address (per-address limit and failure rate limiter). Default 64.</summary>
    public int IPv6PrefixLength { get; set; } = 64;

    /// <summary>
    /// Connections that completed no admission yet, server-wide (ADR 0009); further connections are refused before the
    /// handshake. It also sizes the slot table: <see cref="ServerOptions.MaxPeers"/> plus this value. Default 256.
    /// </summary>
    public int MaxUnadmittedConnections { get; set; } = 256;

    /// <summary>ALPNs accepted before the handshake (ASCII, compared exactly); empty accepts any. Default <c>quicly/1</c>.</summary>
    public IList<string> AllowedAlpns { get; } = new List<string> { DefaultAlpn };

    /// <summary>TLS server names (SNI) accepted before the handshake, compared case-insensitively; empty (the default) accepts any, including none.</summary>
    public IList<string> AllowedServerNames { get; } = new List<string>();

    /// <summary>
    /// Decides the client's auth token on the game thread (inside <see cref="QuiclyServer.PollAll"/>): accept or reject
    /// quickly, or return <see cref="AuthTokenDecision.Pending"/> and call
    /// <see cref="QuiclyServer.CompleteAdmission(QuiclyPeer, bool, string?)"/> later (from any thread, within
    /// <see cref="PeerOptions.AdmissionTimeout"/>). Resumes are validated too (a session token is a locator, not a
    /// credential). <see langword="null"/> accepts every client: development only.
    /// </summary>
    public AuthTokenValidator? AuthTokenValidator { get; set; }

    /// <summary>Failed auth or session tokens one address may present back to back before it is refused (<see cref="AuthFailureRateLimiter"/>). Default 10.</summary>
    public int AuthFailureBurst { get; set; } = AuthFailureRateLimiter.DefaultBurst;

    /// <summary>Time to regain one failure credit. Default 6 s (10 failures a minute sustained).</summary>
    public TimeSpan AuthFailureRefillInterval { get; set; } = TimeSpan.FromMicroseconds(AuthFailureRateLimiter.DefaultRefillIntervalMicros);

    /// <summary>Addresses the failure rate limiter tracks at once. Default 4 096.</summary>
    public int AuthFailureTrackedAddresses { get; set; } = AuthFailureRateLimiter.DefaultCapacity;

    /// <summary>
    /// Sustained shortest time between two admissions of one session (its creation or a resume); <see cref="ResumeBurst"/>
    /// resumes may come faster, back to back. A resume beyond that is answered <see cref="HelloStatus.Rejected"/>
    /// (<see cref="AdmissionFailureReason.ResumeTooSoon"/>), is not charged to the failure rate limiter and leaves the token
    /// usable. The cap keeps one client from filling the token replay cache (see <see cref="SessionTokenAuthority"/>). Zero
    /// disables it; at most one day. Default 1 s.
    /// </summary>
    public TimeSpan MinResumeInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Resumes of one session allowed in quick succession, faster than <see cref="MinResumeInterval"/> (a connection that
    /// drops right after its admission, or flaps): a token bucket that holds this many resumes and regains one every
    /// <see cref="MinResumeInterval"/>. 0 to 1 000; default 3.
    /// </summary>
    public int ResumeBurst { get; set; } = 3;

    internal void Validate()
    {
        OptionChecks.Range(MaxConnectionsPerAddress, 1, int.MaxValue, nameof(MaxConnectionsPerAddress));
        OptionChecks.Range(IPv6PrefixLength, 1, 128, nameof(IPv6PrefixLength));
        OptionChecks.Range(MaxUnadmittedConnections, 1, ServerOptions.PeerLimit, nameof(MaxUnadmittedConnections));
        OptionChecks.Range(AuthFailureBurst, 1, 1_000_000, nameof(AuthFailureBurst));
        OptionChecks.Range(AuthFailureTrackedAddresses, 8, 1 << 22, nameof(AuthFailureTrackedAddresses));
        OptionChecks.Positive(AuthFailureRefillInterval, nameof(AuthFailureRefillInterval));
        if (AuthFailureRefillInterval > TimeSpan.FromDays(1))
        {
            throw new ArgumentException("AuthFailureRefillInterval must be at most one day.", nameof(AuthFailureRefillInterval));
        }

        OptionChecks.NonNegative(MinResumeInterval, nameof(MinResumeInterval));
        if (MinResumeInterval > TimeSpan.FromDays(1))
        {
            throw new ArgumentException("MinResumeInterval must be at most one day.", nameof(MinResumeInterval));
        }

        OptionChecks.Range(ResumeBurst, 0, 1000, nameof(ResumeBurst));
        foreach (string alpn in AllowedAlpns)
        {
            if (string.IsNullOrEmpty(alpn) || alpn.Length > 255 || !Ascii.IsValid(alpn))
            {
                throw new ArgumentException("An ALPN is 1 to 255 ASCII characters.", nameof(AllowedAlpns));
            }
        }

        foreach (string name in AllowedServerNames)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("AllowedServerNames contains an empty name.", nameof(AllowedServerNames));
            }
        }
    }
}

/// <summary>Session tokens and resume (PROTOCOL.md §4.1).</summary>
public sealed class ServerSessionOptions
{
    /// <summary>
    /// The 32-byte HMAC key of session tokens; empty (the default) draws a random key, so tokens do not survive a restart
    /// (neither do sessions). Rotate a running server's key with <see cref="QuiclyServer.RotateSessionKey"/>.
    /// </summary>
    public ReadOnlyMemory<byte> Key { get; set; }

    /// <summary>
    /// How long a session survives the loss of its connection, counted by the server from the moment that connection was lost
    /// (the session registry decides it; a live session stays resumable for its token's <see cref="TokenLifetime"/>). A resume
    /// after it is rejected and the session ends (<see cref="QuiclyServer.SessionEnded"/>). Sent to clients as
    /// <c>HelloAck.graceMicros</c>. Zero ends every session with its connection. Default 30 s.
    /// </summary>
    public TimeSpan Grace { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum age of a session token, counted from the HelloAck that carried it: its <c>expiry</c> field (PROTOCOL.md §4.1).
    /// Tokens are only rotated in HelloAcks, so a connection older than this cannot be resumed even while it is live; how
    /// long a lost session waits for a resume is <see cref="Grace"/>. It does not size
    /// <see cref="ReplayCacheCapacity"/>: a spent token is remembered for one <see cref="Grace"/>, not for its maximum age.
    /// At least <see cref="Grace"/>, at most 30 days. Default 24 hours.
    /// </summary>
    public TimeSpan TokenLifetime { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// Spent session tokens remembered at once, so a token cannot be resumed twice. 0 (the default) sizes the cache from
    /// <see cref="ServerOptions.ExpectedPeers"/>: 32 per expected peer, at least 16 384, at most 4 194 304.
    /// </summary>
    /// <remarks>
    /// <para>The <b>session registry is the authority</b> on single use (PROTOCOL.md §4.1): a resume that commits advances
    /// its session's epoch, so a replayed token no longer matches the session's record and is refused as
    /// <see cref="AdmissionFailureReason.SessionTokenSuperseded"/> whatever the cache holds. The cache is a bounded,
    /// time-expiring second guard, and it <b>never fails closed</b>:</para>
    /// <para>· An entry is kept for one <see cref="Grace"/> — the window in which a replayed token could still match a live
    /// record — not for the token's <see cref="TokenLifetime"/>, which would keep a day of resumes and make the cache, not
    /// the session, the scarce resource.</para>
    /// <para>· When the cache is full its oldest entry is evicted and the new token stored; a resume is never refused because
    /// the cache is full, since that would let one authenticated client (one resume per
    /// <see cref="ServerAdmissionOptions.MinResumeInterval"/>) lock every other client out. The evictions are counted in
    /// <see cref="ServerStatistics.ReplayCacheEvictions"/>, and the entries held in
    /// <see cref="ServerStatistics.ReplayCacheEntries"/>, so a cache under pressure is visible and can be raised.</para>
    /// </remarks>
    public int ReplayCacheCapacity { get; set; }

    internal void Validate()
    {
        if (!Key.IsEmpty && Key.Length != SessionTokenAuthority.KeyLength)
        {
            throw new ArgumentException("A session token key is " + SessionTokenAuthority.KeyLength + " bytes.", nameof(Key));
        }

        OptionChecks.NonNegative(Grace, nameof(Grace));
        if (Grace > TimeSpan.FromDays(1))
        {
            throw new ArgumentException("Grace must be at most one day.", nameof(Grace));
        }

        OptionChecks.Positive(TokenLifetime, nameof(TokenLifetime));
        if (TokenLifetime > TimeSpan.FromDays(30))
        {
            throw new ArgumentException("TokenLifetime must be at most 30 days.", nameof(TokenLifetime));
        }

        if (TokenLifetime < Grace)
        {
            throw new ArgumentException("TokenLifetime must be at least Grace: a lost session could not be resumed for its whole grace period.", nameof(TokenLifetime));
        }

        OptionChecks.Range(ReplayCacheCapacity, 0, 1 << 22, nameof(ReplayCacheCapacity));
    }
}

/// <summary>
/// The optional plain-HTTP side endpoint of a <see cref="QuiclyServer"/>. ADR 0009 defaults: a redirect to HTTPS, health
/// only when asked for, and the <see cref="HttpServer"/>'s own limits (GET/HEAD, bounded headers and connections).
/// </summary>
public sealed class ServerHttpOptions
{
    /// <summary>Default <see cref="HealthPath"/>.</summary>
    public const string DefaultHealthPath = "/healthz";

    /// <summary>
    /// Where the endpoint listens (TCP). Default <c>[::]:80</c>, a dual-mode socket that also accepts IPv4 (falling back to
    /// <c>0.0.0.0</c> on hosts without IPv6). Port 0 binds an ephemeral port (<see cref="QuiclyServer.HttpEndPoint"/>).
    /// </summary>
    public IPEndPoint EndPoint { get; set; } = new(IPAddress.IPv6Any, 80);

    /// <summary>Redirect every request no other handler answered to HTTPS. Default <see langword="true"/>.</summary>
    public bool RedirectToHttps { get; set; } = true;

    /// <summary>The HTTPS port of the redirect; <see langword="null"/> (the default) uses the port of <see cref="ServerOptions.ListenEndPoint"/>.</summary>
    public int? HttpsPort { get; set; }

    /// <summary>Answer <see cref="HealthPath"/> with <c>200 ok</c>. Default <see langword="false"/> (opt-in, ADR 0009).</summary>
    public bool EnableHealthEndpoint { get; set; }

    /// <summary>The health path. Default <c>/healthz</c>.</summary>
    public string HealthPath { get; set; } = DefaultHealthPath;

    /// <summary>Extra handlers, tried in order before health and the redirect.</summary>
    public IList<IHttpHandler> Handlers { get; } = new List<IHttpHandler>();

    /// <summary>Adjusts the <see cref="HttpServerOptions"/> (limits, methods) after the server filled them in, before the endpoint starts.</summary>
    public Action<HttpServerOptions>? Configure { get; set; }

    internal void Validate()
    {
        if (EndPoint is null)
        {
            throw new ArgumentException("An HTTP endpoint is required.", nameof(EndPoint));
        }

        if (HttpsPort is { } port)
        {
            OptionChecks.Range(port, 1, 65535, nameof(HttpsPort));
        }

        if (string.IsNullOrEmpty(HealthPath) || HealthPath[0] != '/')
        {
            throw new ArgumentException("HealthPath must start with a slash.", nameof(HealthPath));
        }

        for (int i = 0; i < Handlers.Count; i++)
        {
            if (Handlers[i] is null)
            {
                throw new ArgumentException("Handlers contains null.", nameof(Handlers));
            }
        }
    }
}

/// <summary>Shared range checks of the option classes.</summary>
internal static class OptionChecks
{
    /// <summary>Longest close reason in UTF-8 bytes (PROTOCOL.md §3.4).</summary>
    public const int MaxReasonBytes = 512;

    public static void Range(int value, int min, int max, string name)
    {
        if (value < min || value > max)
        {
            throw new ArgumentException(name + " must be in [" + min + ", " + max + "] (got " + value + ").", name);
        }
    }

    public static void Positive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentException(name + " must be positive.", name);
        }
    }

    public static void NonNegative(TimeSpan value, string name)
    {
        if (value < TimeSpan.Zero)
        {
            throw new ArgumentException(name + " must not be negative.", name);
        }
    }

    public static void CheckCloseReason(CloseReason reason, string name)
    {
        if ((ulong)reason.Code > uint.MaxValue)
        {
            throw new ArgumentException("Close codes are 32-bit values.", name);
        }

        if (reason.Reason is { } text && Encoding.UTF8.GetByteCount(text) > MaxReasonBytes)
        {
            throw new ArgumentException("A close reason is at most " + MaxReasonBytes + " bytes of UTF-8.", name);
        }
    }

    /// <summary>Whether two listen endpoints would bind the same port (an ephemeral port never conflicts).</summary>
    public static bool SharePort(IPEndPoint a, IPEndPoint b)
    {
        if (a.Port == 0 || b.Port == 0 || a.Port != b.Port)
        {
            return false;
        }

        return a.Address.Equals(b.Address) || IsWildcard(a.Address) || IsWildcard(b.Address);
    }

    private static bool IsWildcard(IPAddress address) => address.Equals(IPAddress.Any) || address.Equals(IPAddress.IPv6Any);
}
