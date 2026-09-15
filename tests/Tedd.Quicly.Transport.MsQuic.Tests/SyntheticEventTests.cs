using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

/// <summary>
/// Drives the wrappers through their real <c>[UnmanagedCallersOnly]</c> entry points with synthetic events that
/// a loopback connection cannot produce on demand (address changes, resumption, DoS mode, cancel-on-loss, unknown
/// event types), exercises the default handler bodies, and covers the failure seams of opening the API table.
/// </summary>
[Collection(MsQuicCollection.Name)]
public unsafe class SyntheticEventTests
{
    private sealed class RecordingConnectionEvents : IMsQuicConnectionEvents
    {
        public IPEndPoint? Local;
        public IPEndPoint? Peer;
        public byte[]? ResumptionState;
        public byte[]? Ticket;
        public bool ThrowOnPeerAddress;

        public void LocalAddressChanged(MsQuicConnection connection, in QUIC_ADDR address) => Local = address.ToIPEndPoint();

        public void PeerAddressChanged(MsQuicConnection connection, in QUIC_ADDR address)
        {
            if (ThrowOnPeerAddress) throw new InvalidOperationException("peer address handler failed");
            Peer = address.ToIPEndPoint();
        }

        public void Resumed(MsQuicConnection connection, ReadOnlySpan<byte> resumptionState) => ResumptionState = resumptionState.ToArray();

        public void ResumptionTicketReceived(MsQuicConnection connection, ReadOnlySpan<byte> ticket) => Ticket = ticket.ToArray();
    }

    private static int Dispatch(MsQuicConnection connection, QUIC_CONNECTION_EVENT evt) => MsQuicConnection.NativeCallbackPointer(connection.Handle, connection.NativeContext, &evt);

    private static int Dispatch(MsQuicStream stream, QUIC_STREAM_EVENT evt) => MsQuicStream.NativeCallbackPointer(stream.Handle, stream.NativeContext, &evt);

    private static int Dispatch(MsQuicListener listener, QUIC_LISTENER_EVENT evt) => MsQuicListener.NativeCallbackPointer(listener.Handle, listener.NativeContext, &evt);

    [Fact]
    public void Connection_events_are_dispatched_through_the_native_callback()
    {
        using var registrationScope = new TestRegistration();
        var events = new RecordingConnectionEvents();
        using var connection = new MsQuicConnection(registrationScope.Registration, events);
        Assert.True(MsQuicStatus.Succeeded(connection.SetParam(MsQuicParam.QUIC_PARAM_CONN_SHARE_UDP_BINDING, (byte)1)));

        QUIC_ADDR local = QUIC_ADDR.FromIPEndPoint(new IPEndPoint(IPAddress.Loopback, 4433));
        QUIC_ADDR peer = QUIC_ADDR.FromIPEndPoint(new IPEndPoint(IPAddress.IPv6Loopback, 5544));
        byte* blob = stackalloc byte[] { 1, 2, 3 };

        QUIC_CONNECTION_EVENT evt = default;
        evt.Type = QUIC_CONNECTION_EVENT_TYPE.LOCAL_ADDRESS_CHANGED;
        evt.LOCAL_ADDRESS_CHANGED.Address = &local;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(connection, evt));
        Assert.Equal(new IPEndPoint(IPAddress.Loopback, 4433), events.Local);

        evt = default;
        evt.Type = QUIC_CONNECTION_EVENT_TYPE.PEER_ADDRESS_CHANGED;
        evt.PEER_ADDRESS_CHANGED.Address = &peer;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(connection, evt));
        Assert.Equal(new IPEndPoint(IPAddress.IPv6Loopback, 5544), events.Peer);

        evt = default;
        evt.Type = QUIC_CONNECTION_EVENT_TYPE.RESUMED;
        evt.RESUMED.ResumptionStateLength = 3;
        evt.RESUMED.ResumptionState = blob;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(connection, evt));
        Assert.Equal([1, 2, 3], events.ResumptionState);

        evt = default;
        evt.Type = QUIC_CONNECTION_EVENT_TYPE.RESUMPTION_TICKET_RECEIVED;
        evt.RESUMPTION_TICKET_RECEIVED.ResumptionTicketLength = 2;
        evt.RESUMPTION_TICKET_RECEIVED.ResumptionTicket = blob + 1;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(connection, evt));
        Assert.Equal([2, 3], events.Ticket);

        // Events the wrapper does not surface are acknowledged.
        evt = default;
        evt.Type = QUIC_CONNECTION_EVENT_TYPE.IDEAL_PROCESSOR_CHANGED;
        evt.IDEAL_PROCESSOR_CHANGED.IdealProcessor = 3;
        evt.IDEAL_PROCESSOR_CHANGED.PartitionIndex = 1;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(connection, evt));
        evt = default;
        evt.Type = QUIC_CONNECTION_EVENT_TYPE.RELIABLE_RESET_NEGOTIATED;
        evt.RELIABLE_RESET_NEGOTIATED.IsNegotiated = 1;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(connection, evt));
        evt = default;
        evt.Type = QUIC_CONNECTION_EVENT_TYPE.ONE_WAY_DELAY_NEGOTIATED;
        evt.ONE_WAY_DELAY_NEGOTIATED.SendNegotiated = 1;
        evt.ONE_WAY_DELAY_NEGOTIATED.ReceiveNegotiated = 1;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(connection, evt));
        evt = default;
        evt.Type = (QUIC_CONNECTION_EVENT_TYPE)99;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(connection, evt));
        Assert.False(MsQuicCallbackScope.IsInsideCallback);

        // A throwing handler is recorded, poisons the connection and answers INTERNAL_ERROR; nothing propagates.
        events.ThrowOnPeerAddress = true;
        evt = default;
        evt.Type = QUIC_CONNECTION_EVENT_TYPE.PEER_ADDRESS_CHANGED;
        evt.PEER_ADDRESS_CHANGED.Address = &peer;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR, Dispatch(connection, evt));
        Assert.True(connection.IsPoisoned);
        Assert.IsType<InvalidOperationException>(connection.LastCallbackException);
        Assert.False(MsQuicCallbackScope.IsInsideCallback);
    }

    [Fact]
    public void Default_connection_handlers_ignore_events_and_reject_what_needs_a_decision()
    {
        using var registrationScope = new TestRegistration();
        using var connection = new MsQuicConnection(registrationScope.Registration);
        IMsQuicConnectionEvents events = connection.Events;
        QUIC_ADDR addr = QUIC_ADDR.FromIPEndPoint(new IPEndPoint(IPAddress.Loopback, 1));
        events.Connected(connection, "x"u8, false);
        events.ShutdownInitiatedByTransport(connection, MsQuicStatus.QUIC_STATUS_CONNECTION_IDLE, 0);
        events.ShutdownInitiatedByPeer(connection, 1);
        events.ShutdownComplete(connection, true, true, false);
        Assert.False(events.PeerStreamStarted(connection, null!, QUIC_STREAM_OPEN_FLAGS.NONE));
        events.StreamsAvailable(connection, 1, 1);
        events.PeerNeedsStreams(connection, true);
        events.DatagramStateChanged(connection, true, 1200);
        events.DatagramReceived(connection, "d"u8, QUIC_RECEIVE_FLAGS.NONE);
        events.DatagramSendStateChanged(connection, null, QUIC_DATAGRAM_SEND_STATE.SENT);
        events.PeerAddressChanged(connection, in addr);
        events.LocalAddressChanged(connection, in addr);
        events.ResumptionTicketReceived(connection, "t"u8);
        events.Resumed(connection, "r"u8);
        Assert.Equal(MsQuicCertificateDecision.Reject, events.PeerCertificateReceived(connection, default));

        // Through the native callback the default certificate decision becomes a bad_certificate status.
        QUIC_CONNECTION_EVENT evt = default;
        evt.Type = QUIC_CONNECTION_EVENT_TYPE.PEER_CERTIFICATE_RECEIVED;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_BAD_CERTIFICATE, Dispatch(connection, evt));
    }

    private sealed class RecordingStreamEvents : IMsQuicStreamEvents
    {
        public bool PeerAcceptedSeen;
        public ulong CancelOnLossCode;
        public ulong IdealBytes;

        public void PeerAccepted(MsQuicStream stream) => PeerAcceptedSeen = true;
        public void CancelOnLoss(MsQuicStream stream, ulong errorCode) => CancelOnLossCode = errorCode;
        public void IdealSendBufferSize(MsQuicStream stream, ulong byteCount) => IdealBytes = byteCount;
    }

    [Fact]
    public void Stream_events_are_dispatched_through_the_native_callback_and_defaults_are_harmless()
    {
        using var registrationScope = new TestRegistration();
        using var connection = new MsQuicConnection(registrationScope.Registration);
        var events = new RecordingStreamEvents();
        TestStatus.AssertAccepted(connection.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, events, out MsQuicStream? stream));
        Assert.NotNull(stream);
        Assert.True(stream.Handle != null);

        QUIC_STREAM_EVENT evt = default;
        evt.Type = QUIC_STREAM_EVENT_TYPE.PEER_ACCEPTED;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(stream, evt));
        Assert.True(events.PeerAcceptedSeen);
        evt = default;
        evt.Type = QUIC_STREAM_EVENT_TYPE.CANCEL_ON_LOSS;
        evt.CANCEL_ON_LOSS.ErrorCode = 0x55;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(stream, evt));
        Assert.Equal(0x55UL, events.CancelOnLossCode);
        evt = default;
        evt.Type = QUIC_STREAM_EVENT_TYPE.IDEAL_SEND_BUFFER_SIZE;
        evt.IDEAL_SEND_BUFFER_SIZE.ByteCount = 65536;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(stream, evt));
        Assert.Equal(65536UL, events.IdealBytes);
        evt = default;
        evt.Type = (QUIC_STREAM_EVENT_TYPE)99;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(stream, evt));

        // Default bodies: no-ops, and Receive consumes everything it is offered.
        stream.Events = null!;
        IMsQuicStreamEvents defaults = stream.Events;
        defaults.StartComplete(stream, MsQuicStatus.QUIC_STATUS_SUCCESS, 0, false);
        Assert.Equal(7UL, defaults.Receive(stream, null, 0, 0, 7, QUIC_RECEIVE_FLAGS.NONE).BytesConsumed);
        defaults.SendComplete(stream, null, false);
        defaults.PeerSendShutdown(stream);
        defaults.PeerSendAborted(stream, 1);
        defaults.PeerReceiveAborted(stream, 1);
        defaults.SendShutdownComplete(stream, true);
        defaults.ShutdownComplete(stream, default);
        defaults.IdealSendBufferSize(stream, 1);
        defaults.PeerAccepted(stream);
        defaults.CancelOnLoss(stream, 1);

        stream.Dispose();
        Assert.True(stream.IsClosed);
        Assert.True(stream.Handle == null);
    }

    private sealed class DosListenerEvents : IMsQuicListenerEvents
    {
        public bool? DosMode;
        public bool Throw;

        public MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, in MsQuicNewConnectionInfo info) => null;

        public void DosModeChanged(MsQuicListener listener, bool enabled)
        {
            if (Throw) throw new InvalidOperationException("dos handler failed");
            DosMode = enabled;
        }
    }

    private sealed class MinimalListenerEvents : IMsQuicListenerEvents
    {
        public MsQuicConfiguration? NewConnection(MsQuicListener listener, MsQuicConnection connection, in MsQuicNewConnectionInfo info) => null;
    }

    [Fact]
    public void Listener_events_are_dispatched_through_the_native_callback()
    {
        using var registrationScope = new TestRegistration();
        var events = new DosListenerEvents();
        using var listener = new MsQuicListener(registrationScope.Registration, events);
        Assert.True(listener.Handle != null);

        QUIC_LISTENER_EVENT evt = default;
        evt.Type = QUIC_LISTENER_EVENT_TYPE.DOS_MODE_CHANGED;
        evt.DOS_MODE_CHANGED._bitfield = 1;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(listener, evt));
        Assert.True(events.DosMode);

        evt = default;
        evt.Type = (QUIC_LISTENER_EVENT_TYPE)99;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, Dispatch(listener, evt));

        events.Throw = true;
        evt = default;
        evt.Type = QUIC_LISTENER_EVENT_TYPE.DOS_MODE_CHANGED;
        Assert.Equal(MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR, Dispatch(listener, evt));
        Assert.IsType<InvalidOperationException>(listener.LastCallbackException);
        Assert.False(MsQuicCallbackScope.IsInsideCallback);

        IMsQuicListenerEvents minimal = new MinimalListenerEvents();
        minimal.StopComplete(listener, false);
        minimal.DosModeChanged(listener, true);
    }

    // ---- MsQuicApi.Open seams -------------------------------------------------------------------------------

    private static QUIC_API_TABLE* s_fakeTable;
    private static uint s_fakeMinor;
    private static bool s_versionQueryFails;
    private static bool s_sizesQueryReachesSecondCall;
    private static int s_closeCalls;

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static int FakeGetParam(QUIC_HANDLE* handle, uint param, uint* length, void* buffer)
    {
        if (param == MsQuicParam.QUIC_PARAM_GLOBAL_LIBRARY_VERSION)
        {
            if (s_versionQueryFails) return MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED;
            uint* v = (uint*)buffer;
            v[0] = 2;
            v[1] = s_fakeMinor;
            v[2] = 7;
            v[3] = 0;
            *length = 16;
            return MsQuicStatus.QUIC_STATUS_SUCCESS;
        }
        if (param == MsQuicParam.QUIC_PARAM_GLOBAL_STATISTICS_V2_SIZES && s_sizesQueryReachesSecondCall)
        {
            if (buffer == null)
            {
                *length = 2 * sizeof(uint);
                return MsQuicStatus.QUIC_STATUS_BUFFER_TOO_SMALL;
            }
            return MsQuicStatus.QUIC_STATUS_INVALID_STATE;
        }
        // TLS provider and a library that predates the statistics-sizes parameter.
        return MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED;
    }

    private static int OpenReturnsFakeTable(uint version, void** table)
    {
        *table = s_fakeTable;
        return MsQuicStatus.QUIC_STATUS_SUCCESS;
    }

    private static int OpenReturnsNullTable(uint version, void** table)
    {
        *table = null;
        return MsQuicStatus.QUIC_STATUS_SUCCESS;
    }

    private static int OpenFails(uint version, void** table) => MsQuicStatus.QUIC_STATUS_INVALID_PARAMETER;

    private static int OpenThrowsDllNotFound(uint version, void** table) => throw new DllNotFoundException("no msquic");

    private static int OpenThrowsEntryPointNotFound(uint version, void** table) => throw new EntryPointNotFoundException("no export");

    private static int OpenThrowsBadImageFormat(uint version, void** table) => throw new BadImageFormatException("wrong bitness");

    private static void FakeClose(void* table) => s_closeCalls++;

    [Fact]
    public void Api_open_reports_missing_broken_and_too_old_libraries()
    {
        s_fakeTable = (QUIC_API_TABLE*)NativeMemory.AllocZeroed((nuint)sizeof(QUIC_API_TABLE));
        bool previousPreview = MsQuicApi.AllowPreviewFeatures;
        try
        {
            s_fakeTable->GetParam = &FakeGetParam;
            s_closeCalls = 0;

            Assert.Equal(MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED, MsQuicApi.Open(&OpenThrowsDllNotFound, &FakeClose, out MsQuicApi? api));
            Assert.Null(api);
            Assert.Equal(MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED, MsQuicApi.Open(&OpenThrowsEntryPointNotFound, &FakeClose, out api));
            Assert.Equal(MsQuicStatus.QUIC_STATUS_NOT_SUPPORTED, MsQuicApi.Open(&OpenThrowsBadImageFormat, &FakeClose, out api));
            Assert.Equal(MsQuicStatus.QUIC_STATUS_INVALID_PARAMETER, MsQuicApi.Open(&OpenFails, &FakeClose, out api));
            Assert.Equal(MsQuicStatus.QUIC_STATUS_INTERNAL_ERROR, MsQuicApi.Open(&OpenReturnsNullTable, &FakeClose, out api));
            Assert.Null(api);
            Assert.Equal(0, s_closeCalls);

            // Too old: the table is closed again and the version error is reported.
            s_fakeMinor = 3;
            Assert.Equal(MsQuicStatus.QUIC_STATUS_VER_NEG_ERROR, MsQuicApi.Open(&OpenReturnsFakeTable, &FakeClose, out api));
            Assert.Null(api);
            Assert.Equal(1, s_closeCalls);

            // Version unknown (the query fails): treated as 0.0, so also refused.
            s_versionQueryFails = true;
            Assert.Equal(MsQuicStatus.QUIC_STATUS_VER_NEG_ERROR, MsQuicApi.Open(&OpenReturnsFakeTable, &FakeClose, out api));
            Assert.Equal(2, s_closeCalls);
            s_versionQueryFails = false;

            // 2.4 with no statistics-sizes parameter and no TLS-provider answer: accepted with conservative defaults.
            s_fakeMinor = 4;
            Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, MsQuicApi.Open(&OpenReturnsFakeTable, &FakeClose, out api));
            Assert.NotNull(api);
            Assert.Equal(new Version(2, 4, 7, 0), api.Version);
            Assert.Empty(api.StatisticsV2Sizes);
            Assert.Equal(QUIC_STATISTICS_V2.SIZE_3, api.StatisticsV2Size);
            Assert.Equal(OperatingSystem.IsWindows() ? QUIC_TLS_PROVIDER.SCHANNEL : QUIC_TLS_PROVIDER.OPENSSL, api.TlsProvider);
            Assert.False(api.TryGetExtension25(out QUIC_API_TABLE_EXT_2_5* extension));
            Assert.True(extension == null);
            MsQuicApi.AllowPreviewFeatures = true;
            Assert.False(api.TryGetPreviewExtension25(out QUIC_API_TABLE_PREVIEW_2_5* preview));
            Assert.True(preview == null);

            // The sizes query answers the size probe but then fails: still no sizes.
            s_sizesQueryReachesSecondCall = true;
            var second = new MsQuicApi(s_fakeTable);
            Assert.Empty(second.StatisticsV2Sizes);
        }
        finally
        {
            s_sizesQueryReachesSecondCall = false;
            MsQuicApi.AllowPreviewFeatures = previousPreview;
            NativeMemory.Free(s_fakeTable);
            s_fakeTable = null;
        }
    }

    [Fact]
    public void Resolver_falls_back_to_default_probing_when_the_runtime_copy_is_missing()
    {
        System.Reflection.Assembly assembly = typeof(MsQuicNative).Assembly;
        string missing = Path.Combine(Path.GetTempPath(), "quicly-no-msquic-" + Guid.NewGuid().ToString("N"));
        // Whether default probing finds a copy depends on the machine; the resolver must answer without throwing.
        _ = MsQuicNative.ResolveLibrary(MsQuicNative.LibraryName, assembly, null, missing);
        _ = MsQuicNative.ResolveLibrary(MsQuicNative.LibraryName, assembly, null, null);
        Assert.NotEqual(IntPtr.Zero, MsQuicNative.ResolveLibrary(MsQuicNative.LibraryName, assembly, null, MsQuicNative.RuntimeDirectory));
    }

    // ---- failure paths of the wrappers that need a live library ---------------------------------------------

    /// <summary>
    /// Recorded: msquic.dll 2.5.10 accepts a second ConfigurationLoadCredential on the same configuration (it swaps
    /// the credential). The helpers refuse instead (ADR 0009), which also keeps a persisted key from being orphaned.
    /// </summary>
    [Fact]
    public void Credentials_are_loaded_once_per_configuration()
    {
        using var registrationScope = new TestRegistration();
        using X509Certificate2 cert = TestCertificates.CreateSelfSigned("CN=twice", TimeSpan.FromHours(1));
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        using X509Certificate2 ephemeral = new CertificateRequest("CN=ephemeral", key, HashAlgorithmName.SHA256).CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddHours(1));
        using MsQuicConfiguration server = MsQuicConfiguration.CreateServer(registrationScope.Registration, ["x"], ephemeral);
        X509Certificate2? owned = server.OwnedCertificate;
        Assert.Throws<InvalidOperationException>(() => server.LoadServerCredential(cert));
        Assert.Throws<InvalidOperationException>(() => server.LoadClientCredential());
        Assert.Same(owned, server.OwnedCertificate);

        using MsQuicConfiguration client = MsQuicConfiguration.CreateClient(registrationScope.Registration, ["x"], MsQuicCertificateValidation.InsecureSkipValidation);
        Assert.Throws<InvalidOperationException>(() => client.LoadClientCredential(MsQuicCertificateValidation.SystemRoots));
        Assert.Equal(QUIC_CREDENTIAL_FLAGS.CLIENT | QUIC_CREDENTIAL_FLAGS.NO_CERTIFICATE_VALIDATION, client.CredentialFlags);

        // The raw call is the documented escape hatch and reaches MsQuic, which accepts the reload.
        QUIC_CREDENTIAL_CONFIG raw = default;
        raw.Type = QUIC_CREDENTIAL_TYPE.NONE;
        raw.Flags = QUIC_CREDENTIAL_FLAGS.CLIENT | QUIC_CREDENTIAL_FLAGS.NO_CERTIFICATE_VALIDATION;
        Assert.True(MsQuicStatus.Succeeded(client.LoadCredential(&raw)));
    }

}

/// <summary>Failure paths of the wrappers that need a live connection (kept out of the unsafe class: they await).</summary>
[Collection(MsQuicCollection.Name)]
public class WrapperFailurePathTests
{
    [Fact]
    public async Task Listener_refuses_the_connection_when_the_chosen_configuration_cannot_be_applied()
    {
        using var loopback = new Loopback();
        // A configuration without a credential cannot be set on a server connection.
        var noCredential = new MsQuicConfiguration(loopback.Registration, [Loopback.Alpn], Loopback.TestServerSettings());
        try
        {
            loopback.SelectConfiguration = _ => noCredential;
            var clientEvents = new ConnectionRecorder();
            loopback.Connect(clientEvents);
            (int status, _) = await clientEvents.TransportShutdownTcs.Within();
            Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
            await clientEvents.ShutdownCompleteTcs.Within();
            Assert.False(clientEvents.ConnectedTcs.Task.IsCompleted);
            MsQuicConnection refused = await loopback.FirstAcceptedTcs.Within();
            // The wrapper handed the handle back to MsQuic and released its context.
            Assert.True(refused.IsClosed);
        }
        finally
        {
            noCredential.Close();
        }
    }

    [Fact]
    public async Task Stream_open_on_a_finished_connection_reports_the_status()
    {
        using var loopback = new Loopback();
        (MsQuicConnection client, ConnectionRecorder clientEvents, _, _) = await loopback.ConnectPairAsync();
        client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
        await clientEvents.ShutdownCompleteTcs.Within();
        int status = client.OpenStream(QUIC_STREAM_OPEN_FLAGS.NONE, null, out MsQuicStream? stream);
        // After SHUTDOWN_COMPLETE MsQuic refuses new streams; the wrapper frees its context and hands back no stream.
        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        Assert.Null(stream);
    }
}
