using System.Net;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Time;

namespace Tedd.Quicly.Benchmarks.Control;

/// <summary>
/// Session-token minting and validation (HMAC-SHA256 over 37 bytes plus the replay-cache step) and the
/// per-address auth-failure limiter used on the admission path.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class SessionTokenBench
{
    private readonly byte[] _scratch = new byte[SessionTokenAuthority.TokenLength];
    private readonly VirtualClock _clock = new(1_000);
    private SessionTokenAuthority _authority = null!;
    private SessionTokenAuthority _rotated = null!;
    private AuthFailureRateLimiter _limiter = null!;
    private byte[] _token = [];
    private byte[] _consumed = [];
    private byte[] _tampered = [];
    private byte[] _underPreviousKey = [];
    private readonly byte[] _address = [203, 0, 113, 7];
    private readonly IPAddress _ipv6 = IPAddress.Parse("2001:db8::7");

    [GlobalSetup]
    public void Setup()
    {
        byte[] key = new byte[32];
        new Random(3).NextBytes(key);
        _authority = new SessionTokenAuthority(key, _clock);
        _token = Mint(_authority, 1);
        _consumed = Mint(_authority, 2);
        _authority.TryValidate(_consumed, out _, out _);
        _tampered = (byte[])_token.Clone();
        _tampered[40] ^= 1;

        byte[] next = (byte[])key.Clone();
        next[0] ^= 0xFF;
        _rotated = new SessionTokenAuthority(key, _clock);
        _underPreviousKey = Mint(_rotated, 3);
        _rotated.RotateKey(next, long.MaxValue);

        _limiter = new AuthFailureRateLimiter(_clock);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _authority.Dispose();
        _rotated.Dispose();
    }

    private byte[] Mint(SessionTokenAuthority authority, ulong sessionId)
    {
        byte[] token = new byte[SessionTokenAuthority.TokenLength];
        authority.Mint(sessionId, 1, long.MaxValue, token);
        return token;
    }

    [Benchmark]
    public int Token_Mint() => _authority.Mint(42, 7, long.MaxValue, _scratch);

    [Benchmark(Baseline = true)]
    public SessionTokenStatus Token_Inspect_Valid() => _authority.TryInspect(_token, out _, out _);

    [Benchmark]
    public SessionTokenStatus Token_Validate_Replayed() => _authority.TryValidate(_consumed, out _, out _);

    [Benchmark]
    public SessionTokenStatus Token_Validate_Tampered() => _authority.TryValidate(_tampered, out _, out _);

    [Benchmark]
    public SessionTokenStatus Token_Inspect_PreviousKey() => _rotated.TryInspect(_underPreviousKey, out _, out _);

    [Benchmark]
    public bool AuthFailures_IPv4_Check_And_Record()
    {
        _limiter.RecordFailure(_address);
        return _limiter.IsAllowed(_address);
    }

    [Benchmark]
    public bool AuthFailures_IPv6_Check_And_Record()
    {
        _limiter.RecordFailure(_ipv6);
        return _limiter.IsAllowed(_ipv6);
    }
}
