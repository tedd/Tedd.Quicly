using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

/// <summary>
/// Key-container lifetime on the CERTIFICATE_CONTEXT path (ADR 0009: the container outlives the configuration while
/// connections created with it are alive) and the callback / argument hardening added with it.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class CredentialLifetimeTests
{
    private static X509Certificate2 CreateEphemeral()
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256);
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddHours(1));
    }

    private static (string Name, CngProvider Provider) KeyOf(X509Certificate2 certificate)
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException();
        using ECDsa? key = certificate.GetECDsaPrivateKey();
        CngKey cng = Assert.IsType<ECDsaCng>(key).Key;
        return (cng.KeyName!, cng.Provider!);
    }

    private static bool KeyExists((string Name, CngProvider Provider) key) => OperatingSystem.IsWindows() && CngKey.Exists(key.Name, key.Provider);

    [Fact]
    public async Task Persisted_key_is_deleted_when_the_last_connection_using_a_closed_configuration_is_closed()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the persisted key container exists only on the Windows CERTIFICATE_CONTEXT path");
        using var loopback = new Loopback();
        using X509Certificate2 ephemeral = CreateEphemeral();
        MsQuicConfiguration renewed = MsQuicConfiguration.CreateServer(loopback.Registration, [Loopback.Alpn], ephemeral, Loopback.TestServerSettings());
        try
        {
            MsQuicOwnedCredential credential = Assert.IsType<MsQuicOwnedCredential>(renewed.OwnedCredential);
            var key = KeyOf(credential.Certificate!);
            Assert.Equal(1, credential.ReferenceCount);

            loopback.SelectConfiguration = _ => renewed;
            (MsQuicConnection client, ConnectionRecorder clientEvents, MsQuicConnection server, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();
            Assert.Equal(2, credential.ReferenceCount); // configuration + server connection

            // ADR 0009: swap first (future connections get the old configuration back), then close the renewed one.
            loopback.SelectConfiguration = null;
            renewed.Close();
            Assert.Null(renewed.OwnedCertificate);
            Assert.Equal(1, credential.ReferenceCount);
            Assert.True(KeyExists(key));

            client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
            await clientEvents.ShutdownCompleteTcs.Within();
            await serverEvents.ShutdownCompleteTcs.Within();
            Assert.True(KeyExists(key)); // shutdown alone does not release: only Close does

            server.Close();
            Assert.Equal(0, credential.ReferenceCount);
            Assert.Null(credential.Certificate);
            Assert.False(KeyExists(key));
        }
        finally
        {
            renewed.Close();
        }
    }

    [Fact]
    public void Configurations_hand_out_references_only_while_their_credential_is_alive()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the persisted key container exists only on the Windows CERTIFICATE_CONTEXT path");
        using var registrationScope = new TestRegistration();
        using X509Certificate2 ephemeral = CreateEphemeral();
        MsQuicConfiguration config = MsQuicConfiguration.CreateServer(registrationScope.Registration, ["x"], ephemeral);
        using var connection = new MsQuicConnection(registrationScope.Registration);
        try
        {
            MsQuicOwnedCredential credential = Assert.IsType<MsQuicOwnedCredential>(config.OwnedCredential);
            var key = KeyOf(credential.Certificate!);

            // A client connection cannot take a server configuration: MsQuic refuses and the reference is returned.
            Assert.True(MsQuicStatus.Failed(connection.SetConfiguration(config)));
            Assert.Equal(1, credential.ReferenceCount);

            // Simulate the race where the last reference went away while the configuration was still being handed out.
            credential.Release();
            Assert.False(KeyExists(key));
            Assert.False(credential.TryAddRef());
            Assert.Equal(MsQuicStatus.QUIC_STATUS_INVALID_STATE, connection.SetConfiguration(config));
            Assert.Equal(MsQuicStatus.QUIC_STATUS_INVALID_STATE, connection.Start(config, "localhost", 443));
        }
        finally
        {
            config.Close(); // releases again: the credential is already gone, nothing happens
        }
    }

    [Fact]
    public void Owned_credential_release_tolerates_an_externally_deleted_container()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "key containers are a Windows concept");
        using X509Certificate2 ephemeral = CreateEphemeral();
        X509Certificate2 persisted = MsQuicCertificateHelper.ReimportWithPersistedKey(ephemeral);
        var key = KeyOf(persisted);
        var credential = new MsQuicOwnedCredential(persisted);
        Assert.True(credential.TryAddRef());
        credential.Release();
        Assert.Same(persisted, credential.Certificate);
        if (OperatingSystem.IsWindows())
        {
            using CngKey external = CngKey.Open(key.Name, key.Provider);
            external.Delete();
        }
        credential.Release(); // the container is already gone: no exception, certificate disposed
        Assert.Null(credential.Certificate);
        Assert.False(KeyExists(key));
        credential.Release(); // over-release is harmless
        Assert.False(credential.TryAddRef());
    }

    // ---- callback hardening ---------------------------------------------------------------------------------

    [Fact]
    public unsafe void Callbacks_with_an_invalid_context_answer_internal_error_and_are_counted()
    {
        using var registrationScope = new TestRegistration();
        GCHandle foreign = GCHandle.Alloc("not a wrapper");
        try
        {
            void* wrongType = (void*)GCHandle.ToIntPtr(foreign);
            long before = MsQuicCallbackScope.InvalidContextCount;

            QUIC_CONNECTION_EVENT connectionEvent = default;
            connectionEvent.Type = QUIC_CONNECTION_EVENT_TYPE.CONNECTED;
            Assert.Equal(MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR, MsQuicConnection.NativeCallbackPointer(null, null, &connectionEvent));
            Assert.Equal(MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR, MsQuicConnection.NativeCallbackPointer(null, wrongType, &connectionEvent));

            QUIC_STREAM_EVENT streamEvent = default;
            streamEvent.Type = QUIC_STREAM_EVENT_TYPE.PEER_SEND_SHUTDOWN;
            Assert.Equal(MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR, MsQuicStream.NativeCallbackPointer(null, null, &streamEvent));
            Assert.Equal(MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR, MsQuicStream.NativeCallbackPointer(null, wrongType, &streamEvent));

            QUIC_LISTENER_EVENT listenerEvent = default;
            listenerEvent.Type = QUIC_LISTENER_EVENT_TYPE.STOP_COMPLETE;
            Assert.Equal(MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR, MsQuicListener.NativeCallbackPointer(null, null, &listenerEvent));
            Assert.Equal(MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR, MsQuicListener.NativeCallbackPointer(null, wrongType, &listenerEvent));

            Assert.True(MsQuicCallbackScope.InvalidContextCount - before >= 6);
            Assert.False(MsQuicCallbackScope.IsInsideCallback);
        }
        finally
        {
            foreign.Free();
        }
    }

    [Fact]
    public unsafe void Freed_context_handles_resolve_to_null()
    {
        GCHandle handle = GCHandle.Alloc(new object());
        nint raw = GCHandle.ToIntPtr(handle);
        Assert.NotNull(MsQuicCallbackScope.ResolveContext<object>((void*)raw));
        handle.Free();
        long before = MsQuicCallbackScope.InvalidContextCount;
        // A freed handle either throws InvalidOperationException from FromIntPtr or yields a null target; both are counted.
        Assert.Null(MsQuicCallbackScope.ResolveContext<MsQuicConnection>((void*)raw));
        Assert.True(MsQuicCallbackScope.InvalidContextCount > before);
    }

    private sealed unsafe class OverConsumingStreamEvents : IMsQuicStreamEvents
    {
        public MsQuicReceiveResult Receive(MsQuicStream stream, QUIC_BUFFER* buffers, uint bufferCount, ulong absoluteOffset, ulong totalLength, QUIC_RECEIVE_FLAGS flags)
            => MsQuicReceiveResult.Consumed(totalLength + 5);
    }

    [Fact]
    public unsafe void Receive_handler_claiming_more_than_indicated_is_a_handler_failure()
    {
        using var registrationScope = new TestRegistration();
        using var connection = new MsQuicConnection(registrationScope.Registration);
        TestStatus.AssertAccepted(connection.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, new OverConsumingStreamEvents(), out MsQuicStream? stream));
        Assert.NotNull(stream);
        try
        {
            byte* data = stackalloc byte[4];
            QUIC_BUFFER buffer = new(data, 4);
            QUIC_STREAM_EVENT evt = default;
            evt.Type = QUIC_STREAM_EVENT_TYPE.RECEIVE;
            evt.RECEIVE.Buffers = &buffer;
            evt.RECEIVE.BufferCount = 1;
            evt.RECEIVE.TotalBufferLength = 4;
            Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, MsQuicStream.NativeCallbackPointer(stream.Handle, stream.NativeContext, &evt));
            Assert.Equal(0UL, evt.RECEIVE.TotalBufferLength); // nothing consumed is reported to MsQuic
            Assert.IsType<InvalidOperationException>(stream.LastCallbackException);
            Assert.Same(stream.LastCallbackException, connection.LastCallbackException);
            Assert.True(connection.IsPoisoned);
        }
        finally
        {
            stream.Close();
        }
    }

    [Fact]
    public void Server_names_longer_than_the_sni_limit_are_rejected_before_calling_msquic()
    {
        using var registrationScope = new TestRegistration();
        using MsQuicConfiguration config = MsQuicConfiguration.CreateClient(registrationScope.Registration, ["x"], MsQuicCertificateValidation.InsecureSkipValidation);
        using var connection = new MsQuicConnection(registrationScope.Registration);
        Assert.Equal(255, MsQuicConnection.MaxServerNameLength);
        Assert.Throws<ArgumentException>(() => connection.Start(config, new string('a', 256), 443));
        // Bytes, not characters: 128 two-byte characters are 256 UTF-8 bytes.
        Assert.Throws<ArgumentException>(() => connection.Start(config, new string('é', 128), 443));
        Assert.False(connection.IsClosed);
    }

    /// <summary>
    /// Documents the per-stream cost of the wrapper (one managed object; the GCHandle is not GC heap). Streams that
    /// churn without allocating use the raw table with a caller-owned context instead (see MsQuicStream remarks).
    /// </summary>
    [Fact]
    public void Opening_a_stream_costs_one_small_wrapper_object()
    {
        using var registrationScope = new TestRegistration();
        using var connection = new MsQuicConnection(registrationScope.Registration);
        var events = new OverConsumingStreamEvents();
        TestStatus.AssertAccepted(connection.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, events, out MsQuicStream? warm));
        warm!.Close();

        const int count = 32;
        var streams = new MsQuicStream?[count];
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < count; i++)
        {
            connection.OpenStream(QUIC_STREAM_OPEN_FLAGS.UNIDIRECTIONAL, events, out streams[i]);
        }
        long perStream = (GC.GetAllocatedBytesForCurrentThread() - before) / count;
        for (int i = 0; i < count; i++) streams[i]!.Close();
        Assert.InRange(perStream, 1, 160);
    }
}
