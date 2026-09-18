using System.Net;
using System.Reflection;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Http;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Server.Tests;

public class ServerConfigurationTests
{
    private static ServerOptions Valid()
    {
        ServerOptions options = new() { Channels = ServerTables.Default };
        options.PeerOptions.AllocatorOptions = Pools.Small();
        return options;
    }

    public static TheoryData<string> InvalidOptions() =>
    [
        "channels", "peer-options", "listen", "expected-0", "max-peers", "shutdown-timeout", "shutdown-code", "shutdown-reason",
        "per-address", "prefix", "unadmitted", "burst", "tracked", "refill-zero", "refill-long", "resume-interval",
        "resume-interval-long", "resume-burst", "alpn-empty", "alpn-long", "alpn-unicode", "server-name", "key", "grace-negative",
        "grace-long", "lifetime-zero", "lifetime-long", "lifetime-below-grace", "replay", "http-endpoint", "https-port",
        "health-path", "http-handler", "consumer", "acme-conflict",
    ];

    [Theory]
    [MemberData(nameof(InvalidOptions))]
    public void Invalid_Options_Are_Refused(string which)
    {
        ServerOptions options = Valid();
        switch (which)
        {
            case "channels": options.Channels = null; break;
            case "peer-options": options.PeerOptions = null!; break;
            case "listen": options.ListenEndPoint = null!; break;
            case "expected-0": options.ExpectedPeers = 0; break;
            case "max-peers": options.MaxPeers = ServerOptions.PeerLimit + 1; break;
            case "shutdown-timeout": options.ShutdownTimeout = TimeSpan.FromSeconds(-1); break;
            case "shutdown-code": options.ShutdownReason = new CloseReason((QuiclyErrorCode)0x1_0000_0000UL); break;
            case "shutdown-reason": options.ShutdownReason = new CloseReason(QuiclyErrorCode.NoError, new string('x', 513)); break;
            case "per-address": options.Admission.MaxConnectionsPerAddress = 0; break;
            case "prefix": options.Admission.IPv6PrefixLength = 129; break;
            case "unadmitted": options.Admission.MaxUnadmittedConnections = 0; break;
            case "burst": options.Admission.AuthFailureBurst = 0; break;
            case "tracked": options.Admission.AuthFailureTrackedAddresses = 4; break;
            case "refill-zero": options.Admission.AuthFailureRefillInterval = TimeSpan.Zero; break;
            case "refill-long": options.Admission.AuthFailureRefillInterval = TimeSpan.FromDays(2); break;
            case "resume-interval": options.Admission.MinResumeInterval = TimeSpan.FromSeconds(-1); break;
            case "resume-interval-long": options.Admission.MinResumeInterval = TimeSpan.FromDays(2); break;
            case "resume-burst": options.Admission.ResumeBurst = 1001; break;
            case "alpn-empty": options.Admission.AllowedAlpns.Add(""); break;
            case "alpn-long": options.Admission.AllowedAlpns.Add(new string('a', 256)); break;
            case "alpn-unicode": options.Admission.AllowedAlpns.Add("quïcly"); break;
            case "server-name": options.Admission.AllowedServerNames.Add(" "); break;
            case "key": options.Sessions.Key = new byte[16]; break;
            case "grace-negative": options.Sessions.Grace = TimeSpan.FromSeconds(-1); break;
            case "grace-long": options.Sessions.Grace = TimeSpan.FromDays(2); break;
            case "lifetime-zero": options.Sessions.TokenLifetime = TimeSpan.Zero; break;
            case "lifetime-long": options.Sessions.TokenLifetime = TimeSpan.FromDays(31); break;
            case "lifetime-below-grace": options.Sessions.TokenLifetime = TimeSpan.FromSeconds(10); break;
            case "replay": options.Sessions.ReplayCacheCapacity = -1; break;
            case "http-endpoint": options.Http = new ServerHttpOptions { EndPoint = null! }; break;
            case "https-port": options.Http = new ServerHttpOptions { HttpsPort = 70_000 }; break;
            case "health-path": options.Http = new ServerHttpOptions { HealthPath = "healthz" }; break;
            case "http-handler": options.Http = new ServerHttpOptions(); options.Http.Handlers.Add(null!); break;
            case "consumer": options.CertificateConsumers.Add(null!); break;
            case "acme-conflict":
                options.Http = new ServerHttpOptions();
                options.Certificate = ServerCertificateOptions.Acme(AcmeOptions());
                break;
        }

        using SimulatedNetwork network = new(new VirtualClock(), 1);
        using SimulatedListener listener = new(network);
        Assert.ThrowsAny<ArgumentException>(() => new QuiclyServer(options, listener));
    }

    [Fact]
    public void Acme_On_Another_Port_Or_Without_Http01_Does_Not_Conflict()
    {
        ServerOptions options = Valid();
        options.Http = new ServerHttpOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, 8080) };
        options.Certificate = ServerCertificateOptions.Acme(AcmeOptions());
        options.Validate();

        AcmeProvisioningOptions dnsOnly = AcmeOptions();
        dnsOnly.ChallengeTypes.Clear();
        dnsOnly.ChallengeTypes.Add(AcmeChallengeKind.Dns01);
        dnsOnly.Dns01Provider = new InMemoryDns01Provider();
        options.Http = new ServerHttpOptions();
        options.Certificate = ServerCertificateOptions.Acme(dnsOnly);
        options.Validate();
    }

    [Fact]
    public void Share_Port_Rules()
    {
        IPEndPoint any80 = new(IPAddress.IPv6Any, 80);
        Assert.True(OptionChecks.SharePort(any80, new IPEndPoint(IPAddress.Loopback, 80)));
        Assert.True(OptionChecks.SharePort(new IPEndPoint(IPAddress.Loopback, 80), new IPEndPoint(IPAddress.Any, 80)));
        Assert.True(OptionChecks.SharePort(new IPEndPoint(IPAddress.Loopback, 80), new IPEndPoint(IPAddress.Loopback, 80)));
        Assert.False(OptionChecks.SharePort(new IPEndPoint(IPAddress.Loopback, 80), new IPEndPoint(IPAddress.Parse("10.0.0.1"), 80)));
        Assert.False(OptionChecks.SharePort(any80, new IPEndPoint(IPAddress.IPv6Any, 81)));
        Assert.False(OptionChecks.SharePort(new IPEndPoint(IPAddress.Any, 0), new IPEndPoint(IPAddress.Any, 0)));
    }

    [Fact]
    public async Task A_Positive_Sub_Microsecond_Duration_Never_Becomes_The_Zero_Sentinel()
    {
        // Every caller of QuiclyServer.ToMicros reads 0 as "off" (Grace, AutoFlushInterval, MinResumeInterval) or has had the
        // option validated positive (TokenLifetime, and AuthFailureRefillInterval, whose rate limiter refuses anything below
        // 1 µs), so truncating a positive duration towards zero switched a feature off or broke start-up. Rounding up is the
        // rule PeerOptions.ToMicros already applies.
        Assert.Equal(0, QuiclyServer.ToMicros(TimeSpan.Zero));
        Assert.Equal(1, QuiclyServer.ToMicros(TimeSpan.FromTicks(1)));
        Assert.Equal(1, QuiclyServer.ToMicros(TimeSpan.FromTicks(5)));
        Assert.Equal(1, QuiclyServer.ToMicros(TimeSpan.FromTicks(19)));
        Assert.Equal(2, QuiclyServer.ToMicros(TimeSpan.FromTicks(20)));
        Assert.Equal(1_000, QuiclyServer.ToMicros(TimeSpan.FromMilliseconds(1)));
        Assert.Equal(-1, QuiclyServer.ToMicros(TimeSpan.FromTicks(-10)));

        // End to end: 500 ns values pass validation (they are positive), so the server starts with them — the refill interval
        // used to make the constructor throw — and the features whose 0 means "off" stay on.
        TimeSpan halfMicrosecond = TimeSpan.FromTicks(5);
        await using ServerFixture f = new(o =>
        {
            o.Admission.AuthFailureRefillInterval = halfMicrosecond;
            o.Sessions.Grace = halfMicrosecond;
            o.Sessions.TokenLifetime = halfMicrosecond;
            o.PeerOptions.AutoFlushInterval = halfMicrosecond;
        });
        Assert.Equal(1L, Field(f.Server, "_graceMicros"));
        Assert.Equal(1L, Field(f.Server, "_tokenLifetimeMicros"));
        Assert.Equal(1L, Field(f.Server, "_autoFlushMicros"));

        static long Field(QuiclyServer server, string name) =>
            (long)typeof(QuiclyServer).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(server)!;
    }

    [Fact]
    public void Invalid_Peer_Template_Is_Refused_By_The_Constructor()
    {
        ServerOptions options = Valid();
        options.PeerOptions.PingInterval = TimeSpan.Zero;
        using SimulatedNetwork network = new(new VirtualClock(), 1);
        using SimulatedListener listener = new(network);
        Assert.ThrowsAny<ArgumentException>(() => new QuiclyServer(options, listener));

        options = Valid();
        options.PeerOptions.Clock = null!;
        Assert.ThrowsAny<ArgumentException>(() => new QuiclyServer(options, listener));

        Assert.Throws<ArgumentNullException>(() => new QuiclyServer(null!, listener));
        Assert.Throws<ArgumentNullException>(() => new QuiclyServer(Valid(), null!));
    }

    [Theory]
    [InlineData(10, 1024, 4096, 1024, 256 * 1024)]
    [InlineData(64, 1024, 4096, 1024, 256 * 1024)]
    [InlineData(200, 512, 2048, 512, 256 * 1024)]
    [InlineData(1000, 256, 1024, 256, 128 * 1024)]
    [InlineData(5000, 128, 512, 128, 64 * 1024)]
    public void Sizing_Scales_Defaults_By_Expected_Peers(int expected, int sendTable, int ring, int arena, int budget)
    {
        ServerOptions options = new() { Channels = ServerTables.Default, ExpectedPeers = expected, MaxPeers = 100 };
        ServerSizing sizing = ServerSizing.Compute(options, out SlabAllocatorOptions? pool);
        Assert.Equal(100 + options.Admission.MaxUnadmittedConnections, sizing.SlotCapacity);
        Assert.Equal(sendTable, sizing.SendTableCapacity);
        Assert.Equal(ring, sizing.ReceiveRingCapacity);
        Assert.Equal(arena, sizing.SegmentArenaCapacity);
        Assert.Equal(budget, sizing.SendBudgetBytes);
        Assert.Equal(budget, sizing.ReceiveBudgetBytes);
        Assert.NotNull(pool);
        long expectedPool = Math.Max(16L << 20, Math.Min(1L << 30, (long)Math.Ceiling(expected * 128.0 * 1024 / (16 << 20)) * (16 << 20)));
        Assert.Equal(expectedPool, sizing.SharedPoolBytes);
    }

    [Fact]
    public void Sizing_Keeps_Configured_Values_And_Supplied_Pools()
    {
        ServerOptions options = new() { Channels = ServerTables.Default, ExpectedPeers = 5000 };
        options.PeerOptions.SendTableCapacity = 2048;
        options.PeerOptions.ReceiveRingCapacity = 16;
        options.PeerOptions.SegmentArenaCapacity = 64;
        options.PeerOptions.SendBudgetBytes = 1000;
        options.PeerOptions.ReceiveBudgetBytes = 2000;
        options.PeerOptions.AllocatorOptions = Pools.Small();
        ServerSizing sizing = ServerSizing.Compute(options, out SlabAllocatorOptions? pool);
        Assert.Equal(2048, sizing.SendTableCapacity);
        Assert.Equal(16, sizing.ReceiveRingCapacity);
        Assert.Equal(64, sizing.SegmentArenaCapacity);
        Assert.Equal(1000, sizing.SendBudgetBytes);
        Assert.Equal(2000, sizing.ReceiveBudgetBytes);
        Assert.Same(options.PeerOptions.AllocatorOptions, pool);
        long sum = 0;
        foreach (SizeClassDefinition definition in Pools.Small().SizeClasses)
        {
            sum += definition.SlabBytes;
        }

        Assert.Equal(sum, sizing.SharedPoolBytes);

        using SlabAllocator shared = new(Pools.Small());
        options.PeerOptions.Allocator = shared;
        sizing = ServerSizing.Compute(options, out pool);
        Assert.Null(pool);
        Assert.Equal(0, sizing.SharedPoolBytes);

        options.PeerOptions.Allocator = null;
        options.PeerOptions.AllocatorOptions = new SlabAllocatorOptions { SizeClasses = null! };
        Assert.Equal(0, ServerSizing.Compute(options, out _).SharedPoolBytes);
    }

    [Fact]
    public async Task Server_Uses_A_Supplied_Allocator_And_Derives_Peer_Options()
    {
        using SlabAllocator shared = new(Pools.Small());
        await using ServerFixture f = new(o =>
        {
            o.PeerOptions.Allocator = shared;
            o.Sessions.Grace = TimeSpan.FromSeconds(7);
        });
        Assert.Same(shared, f.Server.Allocator);
        Assert.Same(shared, f.Server.SharedLeases.Allocator);
        Assert.Equal(0, f.Server.Sizing.SharedPoolBytes);
        Assert.Same(ServerTables.Default, f.Server.Channels);
        Assert.Equal(f.SimListener.LocalEndPoint, f.Server.LocalEndPoint);
        Assert.Same(f.SimListener, f.Server.Listener);
        Assert.Equal(64 + 32, f.Server.Capacity);
        Assert.True(f.Server.IsRunning);
        Assert.Null(f.Server.Provisioner);
        Assert.Null(f.Server.Binder);
        Assert.Null(f.Server.HttpEndPoint);
        Assert.Same(f.Server.DefaultAdmissionPolicy, f.Server.AdmissionPolicy);
        QuiclyPeer client = f.ConnectAdmitted();
        f.Run(1_000);
        Assert.Equal(1, f.Server.PeerCount);
        Assert.Equal(1, f.Server.AdmittedCount);
    }

    /// <summary>
    /// The server derives its peers' options from the application's template with <see cref="PeerOptions.Clone"/> and checks
    /// them with <see cref="PeerOptions.Validate"/> in its constructor (instead of copying property by property and building a
    /// throw-away peer to validate them): the template itself is never modified, and the peers run with the server's values.
    /// </summary>
    [Fact]
    public async Task The_Peer_Template_Is_Cloned_And_Validated_Never_Modified()
    {
        PeerOptions? template = null;
        await using ServerFixture f = new(o =>
        {
            template = new PeerOptions
            {
                Clock = o.PeerOptions.Clock,
                AllocatorOptions = Pools.Small(),
                SendTableCapacity = 32,
                ReceiveRingCapacity = 64,
                SegmentArenaCapacity = 64,
                AutoFlushInterval = TimeSpan.FromMilliseconds(10), // implemented by PollAll, not by the peers
                SessionToken = new byte[] { 1, 2, 3 },             // a client-only value the server drops
                LastEpoch = 21,
                RequestChannelTable = true,
                ThreadSafeSend = true,
            };
            o.PeerOptions = template;
            o.Sessions.Grace = TimeSpan.FromSeconds(7);
        });

        Assert.NotNull(template);
        Assert.Equal(TimeSpan.FromMilliseconds(10), template.AutoFlushInterval);
        Assert.Equal(21u, template.LastEpoch);
        Assert.Equal(3, template.SessionToken.Length);
        Assert.True(template.RequestChannelTable);
        Assert.True(template.ThreadSafeSend);
        Assert.Equal(32, template.SendTableCapacity);
        Assert.Null(template.Allocator);       // the server's own pool went to the copy
        Assert.Null(template.WorkSignal);      // so did the work signal
        Assert.NotNull(template.AllocatorOptions);
        Assert.Equal(TimeSpan.FromSeconds(30), template.SessionGrace);

        // The peers themselves: admitted normally, with the server's session grace and no session token of their own.
        QuiclyPeer client = f.ConnectAdmitted();
        QuiclyPeer serverPeer = f.ServerPeerOf(client);
        Assert.Equal(1u, serverPeer.Epoch);
        Assert.NotEqual(0UL, serverPeer.SessionId);
        f.Run(50_000);
        Assert.Equal(PeerState.Connected, client.State);
    }

    private static AcmeProvisioningOptions AcmeOptions()
    {
        AcmeProvisioningOptions options = new()
        {
            DirectoryUrl = new Uri("https://acme.example.test/directory"),
            AgreeToTermsOfService = true,
            AccountStorePath = Path.Combine(Path.GetTempPath(), "quicly-server-config-account.json"),
            CertificatePath = Path.Combine(Path.GetTempPath(), "quicly-server-config.pfx"),
        };
        options.DnsNames.Add("game.example.test");
        options.Contacts.Add("ops@example.test");
        return options;
    }
}
