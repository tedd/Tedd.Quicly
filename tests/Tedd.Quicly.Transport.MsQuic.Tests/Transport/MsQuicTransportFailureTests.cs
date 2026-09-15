using System.Collections.Concurrent;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>Failure containment, table growth, listener refusal paths and synthetic events of the MsQuic transport.</summary>
[Collection(MsQuicCollection.Name)]
public unsafe class MsQuicTransportFailureTests
{
    private static readonly TimeSpan Timeout = TestTimeouts.Default;

    /// <summary>The sink callback that throws in <see cref="A_throwing_sink_is_contained_whatever_the_callback"/>.</summary>
    public enum SinkFault
    {
        Connected,
        CapabilityChanged,
        StreamStarted,
        PeerStreamStarted,
        StreamReceived,
        OverclaimedReceive,
        SendCompleted,
        PeerSendShutdown,
        StreamAborted,
        StreamShutdownComplete,
        DatagramSendState,
        StreamsAvailable,
        Closed,
    }

    public static TheoryData<SinkFault> Faults()
    {
        var data = new TheoryData<SinkFault>();
        foreach (SinkFault fault in Enum.GetValues<SinkFault>()) data.Add(fault);
        return data;
    }

    private sealed class FaultySink(SinkFault fault) : NullTransportSink
    {
        public ITransport? Transport;

        private void Fail(SinkFault at)
        {
            if (at == fault) throw new InvalidOperationException("injected fault in " + at);
        }

        public override void OnConnected(in TransportConnectedInfo info) => Fail(SinkFault.Connected);

        public override void OnDatagramCapabilityChanged(bool enabled, int maxPayload) => Fail(SinkFault.CapabilityChanged);

        public override void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status) => Fail(SinkFault.StreamStarted);

        public override void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) => Fail(SinkFault.PeerStreamStarted);

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            Fail(SinkFault.StreamReceived);
            ReceiveResult all = base.OnStreamReceived(id, segments, absoluteOffset, fin);
            return fault == SinkFault.OverclaimedReceive ? ReceiveResult.Consumed(all.BytesConsumed + 1) : all;
        }

        public override void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) => Fail(SinkFault.SendCompleted);

        public override void OnStreamPeerSendShutdown(TransportStreamId id) => Fail(SinkFault.PeerSendShutdown);

        public override void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => Fail(SinkFault.StreamAborted);

        public override void OnStreamShutdownComplete(TransportStreamId id)
        {
            Transport?.CloseStream(id);
            Fail(SinkFault.StreamShutdownComplete);
        }

        public override void OnDatagramSendStateChanged(ulong context, DatagramSendState state) => Fail(SinkFault.DatagramSendState);

        public override void OnStreamsAvailable(ushort bidirectional, ushort unidirectional) => Fail(SinkFault.StreamsAvailable);

        public override void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus) => Fail(SinkFault.Closed);
    }

    [Theory]
    [MemberData(nameof(Faults))]
    public void A_throwing_sink_is_contained_whatever_the_callback(SinkFault fault)
    {
        // A diagnostics callback that throws must not break the transport either.
        var harness = new MsQuicTransportHarness(o => o.Diagnostic = static (_, _, _) => throw new InvalidOperationException("diagnostics sink failure"));
        byte* payload = (byte*)NativeMemory.AllocZeroed(64);
        var segment = (TransportSegment*)NativeMemory.AllocZeroed((nuint)sizeof(TransportSegment));
        *segment = new TransportSegment(payload, 64);
        bool closed = false;
        try
        {
            var clientInner = new FaultySink(fault);
            var serverInner = new FaultySink(fault);
            var clientSink = new RecordingSink(null, clientInner);
            var serverSink = new RecordingSink(null, serverInner);
            ConformancePair pair = harness.CreatePair(clientSink, serverSink);
            var client = (MsQuicTransport)pair.Client;
            var server = (MsQuicTransport)pair.Server;
            clientInner.Transport = client;
            serverInner.Transport = server;

            // Drive every callback; once an injected fault has shut the connection down the calls fail harmlessly.
            Spin.Until(() => (clientSink.CountOf(RecordedEventKind.Connected) > 0 && client.Capabilities.Datagrams) || clientSink.IsClosed, Timeout);
            client.SendDatagram(segment, 1, 1, TransportSendFlags.None);
            client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId bidi);
            client.SendStream(bidi, segment, 1, 2, TransportSendFlags.Start);
            Spin.Until(() => serverSink.CountOf(RecordedEventKind.StreamReceived) > 0 || serverSink.IsClosed, Timeout);
            server.UpdatePeerStreamLimits(32, 32);
            Spin.Until(() => clientSink.CountOf(RecordedEventKind.StreamsAvailable) > 0 || clientSink.IsClosed, Timeout);
            client.AbortStream(bidi, 5, StreamAbortDirection.Send);
            Spin.Until(() => serverSink.CountOf(RecordedEventKind.StreamAborted) > 0 || serverSink.IsClosed, Timeout);
            client.OpenStream(StreamKind.Unidirectional, 3, 32767, out TransportStreamId uni);
            client.SendStream(uni, segment, 1, 3, TransportSendFlags.Start | TransportSendFlags.Fin);
            Spin.Until(() => serverSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) > 0 || serverSink.IsClosed, Timeout);
            client.OpenStream(StreamKind.Bidirectional, 4, 32767, out _); // never started: reported by the transport at close
            client.Close(9, default);
            closed = Spin.Until(() => clientSink.IsClosed && serverSink.IsClosed, Timeout);
            Assert.True(closed, "both ends must report OnClosed");
            Assert.Equal(1, clientSink.CountOf(RecordedEventKind.Closed));
            Assert.Equal(1, serverSink.CountOf(RecordedEventKind.Closed));
            // The recording sink records a callback before forwarding it to the faulty sink, so the transport may count the
            // exception a moment after the event is visible.
            Assert.True(Spin.Until(() => client.SinkExceptionCount + server.SinkExceptionCount >= 1, Timeout), $"the injected fault in {fault} never fired");
            Exception? recorded = client.LastSinkException ?? server.LastSinkException;
            Assert.IsType<InvalidOperationException>(recorded);
            if (fault == SinkFault.OverclaimedReceive) Assert.Contains("consumed", recorded!.Message, StringComparison.Ordinal);
            if (fault is not (SinkFault.Closed or SinkFault.StreamShutdownComplete))
            {
                // The side whose sink threw first closed the connection with InternalError.
                Assert.True(
                    clientSink.OfKind(RecordedEventKind.Closed)[0].ErrorCode == MsQuicTransport.InternalErrorCode
                    || serverSink.OfKind(RecordedEventKind.Closed)[0].ErrorCode == MsQuicTransport.InternalErrorCode,
                    "no end closed with InternalError");
            }
        }
        finally
        {
            harness.Dispose();
            if (closed)
            {
                NativeMemory.Free(segment);
                NativeMemory.Free(payload);
            }
        }
        Assert.Null(harness.CleanupError);
        Assert.True(harness.SinkExceptionTotal >= 1);
    }

    [Fact]
    public void The_stream_table_grows_on_demand_and_reuses_every_slot()
    {
        var harness = new MsQuicTransportHarness();
        try
        {
            var clientSink = new RecordingSink();
            var serverSink = new RecordingSink();
            ConformancePair pair = harness.CreatePair(clientSink, serverSink);
            var client = (MsQuicTransport)pair.Client;
            var server = (MsQuicTransport)pair.Server;
            Assert.True(clientSink.WaitFor(static e => e.Kind == RecordedEventKind.Connected, Timeout));
            Assert.True(serverSink.WaitFor(static e => e.Kind == RecordedEventKind.Connected, Timeout));
            Assert.Null(server.ConfigurationLease); // released once the handshake completed

            var ids = new List<TransportStreamId>();
            for (int i = 0; i < 40; i++)
            {
                Assert.Equal(TransportStatus.Success, client.OpenStream(i % 2 == 0 ? StreamKind.Bidirectional : StreamKind.Unidirectional, (ulong)i, 32767, out TransportStreamId id));
                ids.Add(id);
            }
            Assert.Equal(40, client.OpenStreamCount);
            var slots = new HashSet<int>();
            foreach (TransportStreamId id in ids) Assert.True(slots.Add(id.Slot));
            foreach (TransportStreamId id in ids) client.CloseStream(id);
            // The native closes run on the cleanup work item: the slots return to the table once it has run.
            Assert.True(Spin.Until(() => client.OpenStreamCount == 0, Timeout), $"open streams {client.OpenStreamCount}");
            for (int i = 0; i < 40; i++)
            {
                Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, (ulong)i, 32767, out TransportStreamId id));
                Assert.Contains(id.Slot, slots);
                Assert.Equal(2u, id.Generation);
            }

            // A throwing handles-closed observer is contained (the connector's own observer still runs first).
            Action<MsQuicTransport>? original = client.HandlesClosedCallback;
            client.HandlesClosedCallback = t =>
            {
                original?.Invoke(t);
                throw new InvalidOperationException("observer failure");
            };
            client.Dispose();
            Assert.True(client.WaitForHandlesClosed(Timeout));
            Assert.Equal(0, client.OpenStreamCount);
        }
        finally
        {
            harness.Dispose();
        }
        Assert.Null(harness.CleanupError);
    }

    [Fact]
    public void Listener_refusal_and_failure_paths_are_contained()
    {
        var harness = new MsQuicTransportHarness();
        X509Certificate2? next = null;
        try
        {
            using (X509Certificate2 publicOnly = X509CertificateLoader.LoadCertificate(harness.Certificate.RawData))
            {
                Assert.Throws<ArgumentException>(() => new MsQuicTransportListener(new IPEndPoint(IPAddress.Loopback, 0), publicOnly, new MsQuicTransportOptions()));
            }

            var errors = new ConcurrentQueue<string>();
            MsQuicTransportOptions serverOptions = harness.ServerOptions();
            serverOptions.Diagnostic = (level, message, _) =>
            {
                errors.Enqueue(message);
                throw new InvalidOperationException("diagnostics sink failure");
            };
            int preHandshakes = 0;
            MsQuicTransportListener listener = harness.StartListener(serverOptions,
                (in NewConnectionInfo _) => Interlocked.Increment(ref preHandshakes) == 1 ? throw new InvalidOperationException("pre-handshake failure") : PreHandshakeDecision.Accept,
                static (ITransport _, in NewConnectionInfo _) => throw new InvalidOperationException("accept failure"));
            listener.CertificateRetired = _ => throw new InvalidOperationException("retired observer failure");
            Assert.Same(harness.Registration, listener.Registration);

            MsQuicTransportOptions clientOptions = harness.ClientOptions();
            clientOptions.HandshakeIdleTimeout = TimeSpan.FromSeconds(2);
            MsQuicTransportConnector connector = harness.CreateConnector(clientOptions);
            Assert.Same(harness.Registration, connector.Registration);
            var first = new RecordingSink();
            harness.Track((MsQuicTransport)((ITransportConnector)connector).Connect(listener.LocalEndPoint, "localhost", first));
            Assert.True(first.WaitFor(static e => e.Kind == RecordedEventKind.Closed, Timeout));
            Assert.Equal(1, listener.PreHandshakeRejectedCount);
            var second = new RecordingSink();
            harness.Track(connector.Connect(listener.LocalEndPoint, "localhost", second));
            Assert.True(second.WaitFor(static e => e.Kind == RecordedEventKind.Closed, Timeout));
            Assert.Equal(1, listener.AcceptRefusedCount);
            Assert.Equal(0, first.CountOf(RecordedEventKind.Connected) + second.CountOf(RecordedEventKind.Connected));
            Assert.Contains(errors, m => m.Contains("pre-handshake callback threw", StringComparison.Ordinal));
            Assert.Contains(errors, m => m.Contains("accept callback threw", StringComparison.Ordinal));

            // Retiring a certificate whose observer throws is contained.
            next = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
            listener.UpdateCertificate(next);
            Assert.True(Spin.Until(() => listener.OpenConfigurationCount == 1, Timeout));
            Assert.Contains(errors, m => m.Contains("CertificateRetired callback threw", StringComparison.Ordinal));

            // A second listener with the same end point and ALPN cannot start, and can be disposed afterwards.
            var clash = new MsQuicTransportListener(listener.LocalEndPoint, harness.Certificate, harness.ServerOptions(), harness.Registration);
            try
            {
                Assert.Throws<MsQuicException>(() => clash.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, static (ITransport _, in NewConnectionInfo _) => null));
            }
            finally
            {
                clash.Dispose();
                clash.Dispose();
            }
            Assert.Equal(0, clash.OpenConfigurationCount);
            listener.Stop();
            listener.Stop();
        }
        finally
        {
            harness.Dispose();
            next?.Dispose();
        }
        Assert.Null(harness.CleanupError);
    }

    [Fact]
    public void A_peer_address_change_is_reported_with_the_new_address()
    {
        var harness = new MsQuicTransportHarness();
        try
        {
            var clientSink = new RecordingSink();
            ConformancePair pair = harness.CreatePair(clientSink, new RecordingSink());
            var client = (MsQuicTransport)pair.Client;
            Assert.True(clientSink.WaitFor(static e => e.Kind == RecordedEventKind.Connected, Timeout));
            var moved = new IPEndPoint(IPAddress.Loopback, 12345);
            QUIC_ADDR address = QUIC_ADDR.FromIPEndPoint(moved);
            QUIC_CONNECTION_EVENT evt = default;
            evt.Type = QUIC_CONNECTION_EVENT_TYPE.PEER_ADDRESS_CHANGED;
            evt.PEER_ADDRESS_CHANGED.Address = &address;
            int status = MsQuicConnection.NativeCallbackPointer(client.Connection.Handle, client.Connection.NativeContext, &evt);
            Assert.Equal(MsQuicStatus.QUIC_STATUS_SUCCESS, status);
            RecordedEvent changed = Assert.Single(clientSink.OfKind(RecordedEventKind.PeerAddressChanged));
            Assert.Equal(moved, changed.RemoteEndPoint);
            Assert.NotNull(changed.LocalEndPoint);
            Assert.Equal(moved, client.RemoteEndPoint);
        }
        finally
        {
            harness.Dispose();
        }
        Assert.Null(harness.CleanupError);
    }
}
