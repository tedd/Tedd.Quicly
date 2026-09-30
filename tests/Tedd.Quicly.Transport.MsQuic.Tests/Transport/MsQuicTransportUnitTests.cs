using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>Mapping tables, layout, options and certificate policy: no network.</summary>
public unsafe class MsQuicTransportUnitTests
{
    // ------------------------------------------------------------------ layout

    [Fact]
    public unsafe void TransportSegment_is_bit_identical_to_QUIC_BUFFER()
    {
        Assert.Equal(16, sizeof(TransportSegment));
        Assert.Equal(sizeof(QUIC_BUFFER), sizeof(TransportSegment));
        Assert.Equal(Marshal.OffsetOf<QUIC_BUFFER>(nameof(QUIC_BUFFER.Length)), Marshal.OffsetOf<TransportSegment>(nameof(TransportSegment.Length)));
        Assert.Equal(Marshal.OffsetOf<QUIC_BUFFER>(nameof(QUIC_BUFFER.Buffer)), Marshal.OffsetOf<TransportSegment>(nameof(TransportSegment.Buffer)));

        byte* a = stackalloc byte[8];
        byte* b = stackalloc byte[4];
        TransportSegment* segments = stackalloc TransportSegment[2];
        segments[0] = new TransportSegment(a, 8);
        segments[1] = new TransportSegment(b, 4);
        var buffers = (QUIC_BUFFER*)segments;
        Assert.Equal(8u, buffers[0].Length);
        Assert.True(buffers[0].Buffer == a);
        Assert.Equal(4u, buffers[1].Length);
        Assert.True(buffers[1].Buffer == b);

        QUIC_BUFFER* received = stackalloc QUIC_BUFFER[1];
        received[0] = new QUIC_BUFFER(a, 3);
        var view = new ReadOnlySpan<TransportSegment>(received, 1);
        Assert.Equal(3u, view[0].Length);
        Assert.True(view[0].Buffer == a);
    }

    // ------------------------------------------------------------------ flag and status mapping

    [Theory]
    [InlineData(TransportSendFlags.None, QUIC_SEND_FLAGS.NONE)]
    [InlineData(TransportSendFlags.Priority, QUIC_SEND_FLAGS.DGRAM_PRIORITY)]
    [InlineData(TransportSendFlags.DelaySend, QUIC_SEND_FLAGS.DELAY_SEND)]
    [InlineData(TransportSendFlags.CancelOnBlocked, QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED)]
    [InlineData(TransportSendFlags.Fin | TransportSendFlags.Start | TransportSendFlags.CancelOnLoss, QUIC_SEND_FLAGS.NONE)]
    [InlineData(TransportSendFlags.Priority | TransportSendFlags.DelaySend | TransportSendFlags.CancelOnBlocked, QUIC_SEND_FLAGS.DGRAM_PRIORITY | QUIC_SEND_FLAGS.DELAY_SEND | QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED)]
    public void Datagram_flags_map_to_msquic_flags(TransportSendFlags flags, QUIC_SEND_FLAGS expected) => Assert.Equal(expected, MsQuicTransport.MapDatagramFlags(flags));

    [Theory]
    [InlineData(TransportSendFlags.None, QUIC_SEND_FLAGS.NONE)]
    [InlineData(TransportSendFlags.Fin, QUIC_SEND_FLAGS.FIN)]
    [InlineData(TransportSendFlags.DelaySend, QUIC_SEND_FLAGS.DELAY_SEND)]
    [InlineData(TransportSendFlags.Priority, QUIC_SEND_FLAGS.PRIORITY_WORK)]
    [InlineData(TransportSendFlags.CancelOnLoss, QUIC_SEND_FLAGS.CANCEL_ON_LOSS)]
    [InlineData(TransportSendFlags.Start | TransportSendFlags.CancelOnBlocked, QUIC_SEND_FLAGS.NONE)]
    [InlineData(TransportSendFlags.Fin | TransportSendFlags.DelaySend | TransportSendFlags.Priority | TransportSendFlags.CancelOnLoss, QUIC_SEND_FLAGS.FIN | QUIC_SEND_FLAGS.DELAY_SEND | QUIC_SEND_FLAGS.PRIORITY_WORK | QUIC_SEND_FLAGS.CANCEL_ON_LOSS)]
    public void Stream_flags_map_to_msquic_flags(TransportSendFlags flags, QUIC_SEND_FLAGS expected) => Assert.Equal(expected, MsQuicTransport.MapStreamFlags(flags));

    [Fact]
    public void Flag_tables_cover_every_combination_and_ignore_unknown_bits()
    {
        for (int i = 0; i < 256; i++)
        {
            var flags = (TransportSendFlags)i;
            QUIC_SEND_FLAGS datagram = QUIC_SEND_FLAGS.NONE;
            if ((flags & TransportSendFlags.DelaySend) != 0) datagram |= QUIC_SEND_FLAGS.DELAY_SEND;
            if ((flags & TransportSendFlags.Priority) != 0) datagram |= QUIC_SEND_FLAGS.DGRAM_PRIORITY;
            if ((flags & TransportSendFlags.CancelOnBlocked) != 0) datagram |= QUIC_SEND_FLAGS.CANCEL_ON_BLOCKED;
            QUIC_SEND_FLAGS stream = QUIC_SEND_FLAGS.NONE;
            if ((flags & TransportSendFlags.DelaySend) != 0) stream |= QUIC_SEND_FLAGS.DELAY_SEND;
            if ((flags & TransportSendFlags.Fin) != 0) stream |= QUIC_SEND_FLAGS.FIN;
            if ((flags & TransportSendFlags.Priority) != 0) stream |= QUIC_SEND_FLAGS.PRIORITY_WORK;
            if ((flags & TransportSendFlags.CancelOnLoss) != 0) stream |= QUIC_SEND_FLAGS.CANCEL_ON_LOSS;
            Assert.Equal(datagram, MsQuicTransport.MapDatagramFlags(flags));
            Assert.Equal(stream, MsQuicTransport.MapStreamFlags(flags));
        }
    }

    [Fact]
    public void Status_mapping_table()
    {
        Assert.Equal(TransportStatus.Success, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_SUCCESS, datagramSend: false));
        Assert.Equal(TransportStatus.Success, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_PENDING, datagramSend: true));
        Assert.Equal(TransportStatus.InvalidState, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_INVALID_STATE, datagramSend: true));
        Assert.Equal(TransportStatus.InvalidState, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_INVALID_STATE, datagramSend: false));
        Assert.Equal(TransportStatus.TooLarge, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_INVALID_PARAMETER, datagramSend: true));
        Assert.Equal(TransportStatus.Failed, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_INVALID_PARAMETER, datagramSend: false));
        Assert.Equal(TransportStatus.OutOfMemory, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_OUT_OF_MEMORY, datagramSend: true));
        Assert.Equal(TransportStatus.StreamLimitReached, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_STREAM_LIMIT_REACHED, datagramSend: false));
        Assert.Equal(TransportStatus.NotSupported, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED, datagramSend: true));
        Assert.Equal(TransportStatus.Failed, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_ABORTED, datagramSend: false));
        Assert.Equal(TransportStatus.Failed, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR, datagramSend: true));
        Assert.Equal(TransportStatus.Failed, MsQuicTransport.MapStatus(MsQuicStatus.QUIC_STATUS_CONNECTION_REFUSED, datagramSend: false));
    }

    [Fact]
    public void Datagram_send_states_map_one_to_one()
    {
        Assert.Equal(DatagramSendState.Unknown, MsQuicTransport.MapDatagramSendState(QUIC_DATAGRAM_SEND_STATE.UNKNOWN));
        Assert.Equal(DatagramSendState.Sent, MsQuicTransport.MapDatagramSendState(QUIC_DATAGRAM_SEND_STATE.SENT));
        Assert.Equal(DatagramSendState.LostSuspect, MsQuicTransport.MapDatagramSendState(QUIC_DATAGRAM_SEND_STATE.LOST_SUSPECT));
        Assert.Equal(DatagramSendState.LostDiscarded, MsQuicTransport.MapDatagramSendState(QUIC_DATAGRAM_SEND_STATE.LOST_DISCARDED));
        Assert.Equal(DatagramSendState.Acknowledged, MsQuicTransport.MapDatagramSendState(QUIC_DATAGRAM_SEND_STATE.ACKNOWLEDGED));
        Assert.Equal(DatagramSendState.AcknowledgedSpurious, MsQuicTransport.MapDatagramSendState(QUIC_DATAGRAM_SEND_STATE.ACKNOWLEDGED_SPURIOUS));
        Assert.Equal(DatagramSendState.Canceled, MsQuicTransport.MapDatagramSendState(QUIC_DATAGRAM_SEND_STATE.CANCELED));
        Assert.Equal(DatagramSendState.Unknown, MsQuicTransport.MapDatagramSendState((QUIC_DATAGRAM_SEND_STATE)99));
        foreach (QUIC_DATAGRAM_SEND_STATE state in Enum.GetValues<QUIC_DATAGRAM_SEND_STATE>())
        {
            Assert.Equal(QuicDatagramSendState.IsFinal(state), MsQuicTransport.MapDatagramSendState(state).IsFinal());
        }
    }

    // ------------------------------------------------------------------ options

    [Fact]
    public void Options_defaults_follow_architecture_section_7()
    {
        var options = new MsQuicTransportOptions();
        Assert.Equal(["quicly/1"], options.Alpns);
        Assert.Equal(QUIC_EXECUTION_PROFILE.LOW_LATENCY, options.ExecutionProfile);
        Assert.Equal(QUIC_STREAM_SCHEDULING_SCHEME.ROUND_ROBIN, options.StreamSchedulingScheme);
        Assert.Equal(ServerCertificateValidationMode.SystemRoots, options.ServerCertificateValidation);
        Assert.Equal(MsQuicServerCredentialMode.Auto, options.ServerCredentialMode);
        Assert.Equal(MsQuicKeyStorage.User, options.ServerKeyStorage);
        Assert.Equal(2048, options.MaxStreams);

        // The table has room for everything a default client lets its server open, next to the local streams.
        Assert.True(options.ClientPeerUnidiStreamCount <= MsQuicTransportOptions.PeerStreamRoom(options.MaxStreams));

        MsQuicSettings server = options.CreateServerSettings();
        Assert.Equal((ushort)1, server.PeerBidiStreamCount);
        Assert.Equal((ushort)0, server.PeerUnidiStreamCount);
        Assert.Equal(TimeSpan.Zero, server.KeepAliveInterval);
        Assert.Equal(TimeSpan.FromSeconds(30), server.IdleTimeout);
        Assert.Equal(TimeSpan.FromSeconds(5), server.HandshakeIdleTimeout);
        Assert.Equal(TimeSpan.FromSeconds(6), server.DisconnectTimeout);
        Assert.Equal(QUIC_SERVER_RESUMPTION_LEVEL.NO_RESUME, server.ServerResumptionLevel);

        MsQuicSettings client = options.CreateClientSettings();
        Assert.Equal((ushort)0, client.PeerBidiStreamCount);
        Assert.Equal((ushort)1024, client.PeerUnidiStreamCount);
        Assert.Equal(TimeSpan.FromSeconds(10), client.KeepAliveInterval);
    }

    [Fact]
    public void Settings_hooks_and_timeouts_are_applied()
    {
        var options = new MsQuicTransportOptions
        {
            IdleTimeout = TimeSpan.FromSeconds(7),
            HandshakeIdleTimeout = TimeSpan.FromSeconds(2),
            DisconnectTimeout = TimeSpan.FromSeconds(3),
            ClientKeepAliveInterval = TimeSpan.FromSeconds(4),
            ServerKeepAliveInterval = TimeSpan.FromSeconds(9),
            ServerPeerBidiStreamCount = 3,
            ServerPeerUnidiStreamCount = 5,
            ClientPeerBidiStreamCount = 2,
            ClientPeerUnidiStreamCount = 6,
            ConfigureServerSettings = s => s.MaxAckDelayMs = 11,
            ConfigureClientSettings = s => s.MaxAckDelayMs = 12,
        };
        MsQuicSettings server = options.CreateServerSettings();
        Assert.Equal(TimeSpan.FromSeconds(7), server.IdleTimeout);
        Assert.Equal(TimeSpan.FromSeconds(2), server.HandshakeIdleTimeout);
        Assert.Equal(TimeSpan.FromSeconds(3), server.DisconnectTimeout);
        Assert.Equal(TimeSpan.FromSeconds(9), server.KeepAliveInterval);
        Assert.Equal((ushort)3, server.PeerBidiStreamCount);
        Assert.Equal((ushort)5, server.PeerUnidiStreamCount);
        Assert.Equal(11u, server.MaxAckDelayMs);
        MsQuicSettings client = options.CreateClientSettings();
        Assert.Equal(TimeSpan.FromSeconds(4), client.KeepAliveInterval);
        Assert.Equal((ushort)2, client.PeerBidiStreamCount);
        Assert.Equal((ushort)6, client.PeerUnidiStreamCount);
        Assert.Equal(12u, client.MaxAckDelayMs);
    }

    [Fact]
    public void Clone_is_deep()
    {
        var options = new MsQuicTransportOptions { Alpns = ["a", "b"], PinnedSpkiSha256 = [new byte[32]] };
        MsQuicTransportOptions copy = options.Clone();
        options.Alpns[0] = "changed";
        options.PinnedSpkiSha256[0][0] = 1;
        options.PinnedSpkiSha256.Add(new byte[32]);
        Assert.Equal(["a", "b"], copy.Alpns);
        Assert.Single(copy.PinnedSpkiSha256);
        Assert.Equal(0, copy.PinnedSpkiSha256[0][0]);
    }

    [Fact]
    public void Validate_rejects_bad_options()
    {
        Assert.Throws<ArgumentException>(() => new MsQuicTransportOptions { Alpns = [] }.Validate(client: false));
        Assert.Throws<ArgumentException>(() => new MsQuicTransportOptions { Alpns = [""] }.Validate(client: false));
        Assert.Throws<ArgumentException>(() => new MsQuicTransportOptions { Alpns = [new string('x', 256)] }.Validate(client: true));
        Assert.Throws<ArgumentOutOfRangeException>(() => new MsQuicTransportOptions { MaxStreams = 0 }.Validate(client: false));
        Assert.Throws<ArgumentOutOfRangeException>(() => new MsQuicTransportOptions { MaxStreams = (1 << 20) + 1 }.Validate(client: false));

        // A quarter of the table, at least one slot, stays free for the local streams.
        Assert.Equal(1536, MsQuicTransportOptions.PeerStreamRoom(2048));
        Assert.Equal(768, MsQuicTransportOptions.PeerStreamRoom(1024));
        Assert.Equal(3, MsQuicTransportOptions.PeerStreamRoom(4));
        Assert.Equal(0, MsQuicTransportOptions.PeerStreamRoom(1));
        Assert.Throws<ArgumentException>(() => new MsQuicTransportOptions { ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki }.Validate(client: true));
        Assert.Throws<ArgumentException>(() => new MsQuicTransportOptions { ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki, PinnedSpkiSha256 = [new byte[31]] }.Validate(client: true));
        Assert.Throws<ArgumentException>(() => new MsQuicTransportOptions { ServerCertificateValidation = ServerCertificateValidationMode.Callback }.Validate(client: true));
        Assert.Throws<ArgumentOutOfRangeException>(() => new MsQuicTransportOptions { ServerCertificateValidation = (ServerCertificateValidationMode)42 }.Validate(client: true));
        // Client-only checks are skipped for servers.
        new MsQuicTransportOptions { ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki }.Validate(client: false);
        new MsQuicTransportOptions { ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki, PinnedSpkiSha256 = [new byte[32]] }.Validate(client: true);
        new MsQuicTransportOptions { ServerCertificateValidation = ServerCertificateValidationMode.Callback, ServerCertificateValidator = static (in ServerCertificateContext _) => ServerCertificateDecision.Reject }.Validate(client: true);
    }

    [Fact]
    public void Insecure_guard_needs_debug_or_the_environment_variable()
    {
        Assert.False(MsQuicTransportOptions.IsInsecureAllowed(null, debugBuild: false));
        Assert.False(MsQuicTransportOptions.IsInsecureAllowed("0", debugBuild: false));
        Assert.False(MsQuicTransportOptions.IsInsecureAllowed("true", debugBuild: false));
        Assert.True(MsQuicTransportOptions.IsInsecureAllowed("1", debugBuild: false));
        Assert.True(MsQuicTransportOptions.IsInsecureAllowed(null, debugBuild: true));
        Assert.Equal("QUICLY_ALLOW_INSECURE", MsQuicTransportOptions.AllowInsecureEnvironmentVariable);
    }

    // ------------------------------------------------------------------ certificate policy

    private static MsQuicCertificateDecision Decide(ServerCertificatePolicy policy, byte[] der, int platformStatus, uint errorFlags = 0, string? serverName = "localhost")
        => policy.Decide(new MsQuicPeerCertificateInfo(der, default, null, null, errorFlags, platformStatus, isPortable: true, canDefer: true), serverName);

    [Fact]
    public void Pinned_spki_accepts_only_pinned_keys_whatever_the_platform_says()
    {
        using X509Certificate2 pinned = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        using X509Certificate2 other = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        var messages = new List<string>();
        var policy = new ServerCertificatePolicy(new MsQuicTransportOptions
        {
            ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
            PinnedSpkiSha256 = [SpkiPin.Compute(other), SpkiPin.Compute(pinned)],
            Diagnostic = (_, message, _) => messages.Add(message),
        });
        Assert.Equal(MsQuicCertificateValidation.Callback, policy.WrapperValidation);
        Assert.Equal(MsQuicCertificateDecision.Accept, Decide(policy, pinned.RawData, MsQuicStatus.QUIC_STATUS_CERT_UNTRUSTED_ROOT));
        using X509Certificate2 stranger = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: false, "localhost");
        Assert.Equal(MsQuicCertificateDecision.Reject, Decide(policy, stranger.RawData, MsQuicStatus.QUIC_STATUS_SUCCESS));
        Assert.Equal(MsQuicCertificateDecision.Reject, Decide(policy, [], MsQuicStatus.QUIC_STATUS_SUCCESS));
        Assert.Contains(messages, m => m.Contains("matches no pin", StringComparison.Ordinal));
    }

    [Fact]
    public void Callback_validation_combines_the_verdict_with_the_platform_result()
    {
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        ServerCertificateDecision verdict = ServerCertificateDecision.Accept;
        byte[]? seenLeaf = null;
        string? seenName = null;
        bool seenValid = false;
        var levels = new List<TransportDiagnosticLevel>();
        var policy = new ServerCertificatePolicy(new MsQuicTransportOptions
        {
            ServerCertificateValidation = ServerCertificateValidationMode.Callback,
            ServerCertificateValidator = (in ServerCertificateContext context) =>
            {
                seenLeaf = context.LeafDer.ToArray();
                seenName = context.ServerName;
                seenValid = context.PlatformValid;
                return verdict == (ServerCertificateDecision)99 ? throw new InvalidOperationException("validator failure") : verdict;
            },
            Diagnostic = (level, _, _) => levels.Add(level),
        });
        Assert.Equal(MsQuicCertificateValidation.Callback, policy.WrapperValidation);
        Assert.Equal(MsQuicCertificateDecision.Accept, Decide(policy, certificate.RawData, MsQuicStatus.QUIC_STATUS_SUCCESS));
        Assert.Equal(certificate.RawData, seenLeaf);
        Assert.Equal("localhost", seenName);
        Assert.True(seenValid);
        Assert.Equal(MsQuicCertificateDecision.Reject, Decide(policy, certificate.RawData, MsQuicStatus.QUIC_STATUS_CERT_UNTRUSTED_ROOT));
        Assert.False(seenValid);
        Assert.Equal(MsQuicCertificateDecision.Reject, Decide(policy, certificate.RawData, MsQuicStatus.QUIC_STATUS_SUCCESS, errorFlags: 1));
        verdict = ServerCertificateDecision.AcceptIgnoringPlatformValidation;
        Assert.Equal(MsQuicCertificateDecision.Accept, Decide(policy, certificate.RawData, MsQuicStatus.QUIC_STATUS_CERT_UNTRUSTED_ROOT));
        verdict = ServerCertificateDecision.Reject;
        Assert.Equal(MsQuicCertificateDecision.Reject, Decide(policy, certificate.RawData, MsQuicStatus.QUIC_STATUS_SUCCESS));
        verdict = (ServerCertificateDecision)99;
        Assert.Equal(MsQuicCertificateDecision.Reject, Decide(policy, certificate.RawData, MsQuicStatus.QUIC_STATUS_SUCCESS));
        Assert.Contains(TransportDiagnosticLevel.Error, levels);
        Assert.Contains(TransportDiagnosticLevel.Warning, levels);
    }

    [Fact]
    public void System_roots_and_insecure_modes_follow_the_platform_or_accept_everything()
    {
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        var roots = new ServerCertificatePolicy(new MsQuicTransportOptions());
        Assert.Equal(MsQuicCertificateValidation.SystemRoots, roots.WrapperValidation);
        Assert.Equal(MsQuicCertificateDecision.Accept, Decide(roots, certificate.RawData, MsQuicStatus.QUIC_STATUS_SUCCESS));
        Assert.Equal(MsQuicCertificateDecision.Reject, Decide(roots, certificate.RawData, MsQuicStatus.QUIC_STATUS_CERT_UNTRUSTED_ROOT));
        var insecure = new ServerCertificatePolicy(new MsQuicTransportOptions { ServerCertificateValidation = ServerCertificateValidationMode.DangerousAcceptAnyServerCertificate });
        Assert.Equal(MsQuicCertificateValidation.InsecureSkipValidation, insecure.WrapperValidation);
        Assert.Equal(MsQuicCertificateDecision.Accept, Decide(insecure, [], MsQuicStatus.QUIC_STATUS_CERT_UNTRUSTED_ROOT));
    }

    [Fact]
    public void Spki_pin_is_the_sha256_of_the_subject_public_key_info()
    {
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        byte[] expected = SHA256.HashData(certificate.PublicKey.ExportSubjectPublicKeyInfo());
        Assert.Equal(expected, SpkiPin.Compute(certificate));
        Assert.Equal(expected, SpkiPin.Compute(certificate.RawData));
        Assert.Equal(32, expected.Length);
        Assert.Throws<ArgumentNullException>(() => SpkiPin.Compute((X509Certificate2)null!));
    }

    [Fact]
    public void A_throwing_diagnostic_callback_never_escapes_the_certificate_policy()
    {
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        TransportDiagnosticCallback throwing = static (_, _, _) => throw new InvalidOperationException("diagnostics sink failure");
        var pinned = new ServerCertificatePolicy(new MsQuicTransportOptions
        {
            ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
            PinnedSpkiSha256 = [new byte[32]],
            Diagnostic = throwing,
        });
        Assert.Equal(MsQuicCertificateDecision.Reject, Decide(pinned, certificate.RawData, MsQuicStatus.QUIC_STATUS_SUCCESS));
        var validator = new ServerCertificatePolicy(new MsQuicTransportOptions
        {
            ServerCertificateValidation = ServerCertificateValidationMode.Callback,
            ServerCertificateValidator = static (in ServerCertificateContext _) => throw new InvalidOperationException("validator failure"),
            Diagnostic = throwing,
        });
        Assert.Equal(MsQuicCertificateDecision.Reject, Decide(validator, certificate.RawData, MsQuicStatus.QUIC_STATUS_SUCCESS));
    }

    [Fact]
    public void The_segment_layout_guard_accepts_this_64_bit_process()
    {
        Assert.True(Environment.Is64BitProcess);
        Assert.True(MsQuicTransport.SegmentLayoutMatchesQuicBuffer);
        MsQuicTransport.ThrowIfSegmentLayoutUnsupported();
    }
}
