using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Correlated request/response on ReliableOrdered channels (PROTOCOL.md §3.1, §4.3; docs/design/session-layer.md §7.8):
/// the happy path, timeouts processed in Poll and Flush, cancellation that does not cancel the send, many concurrent
/// requests, unmatched responses, and what happens when the session ends.
/// </summary>
public class RequestResponseTests
{
    /// <summary>
    /// 2 movement datagrams · 4 plain ordered · 10 ordered request/response · 11 the same, keyed · 12 the same, LZ4 ·
    /// 13 the same with a 256-byte queue limit.
    /// </summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(4, "chat", ChannelMode.ReliableOrdered)
        .Add(10, "rpc", ChannelMode.ReliableOrdered, o => o.RequestResponse = true)
        .Add(11, "rpc-keyed", ChannelMode.ReliableOrdered, o => { o.RequestResponse = true; o.Keyed = true; })
        .Add(12, "rpc-packed", ChannelMode.ReliableOrdered, o =>
        {
            o.RequestResponse = true;
            o.Compression = ChannelCompression.Lz4;
            o.MinCompressSize = 16;
        })
        .Add(13, "rpc-limited", ChannelMode.ReliableOrdered, o =>
        {
            o.RequestResponse = true;
            o.QueueLimitBytes = 256;
        })
        .Build();

    private static SessionHarness Harness(Action<PeerOptions>? client = null, Action<PeerOptions>? server = null) =>
        new(table: Table,
            client: o =>
            {
                DatagramKit.Quiet(o);
                client?.Invoke(o);
            },
            server: o =>
            {
                DatagramKit.Quiet(o);
                server?.Invoke(o);
            });

    /// <summary>Answers every request of a channel with the request's payload, reversed.</summary>
    private static void Echo(QuiclyPeer peer, ushort channel, List<uint>? ids = null)
    {
        peer.RegisterHandler(channel, (QuiclyPeer self, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            ids?.Add(header.RequestId);
            Span<byte> answer = stackalloc byte[payload.Length];
            for (int i = 0; i < payload.Length; i++)
            {
                answer[i] = payload[payload.Length - 1 - i];
            }

            Assert.Equal(SendStatus.Admitted, self.Respond(in header, answer).Status);
        });
    }

    [Fact]
    public void A_Request_Is_Answered_By_The_Peer()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        List<uint> ids = [];
        Echo(h.Server!, 10, ids);
        List<(ReceiveHeader Header, byte[] Payload)> onClient = [];
        client.RegisterHandler(10, Handlers.Collect(onClient));

        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 1, 2, 3 }, TimeSpan.FromSeconds(1));
        Assert.True(h.RunUntil(() => pending.IsCompleted), "no response arrived");
        ReceiveLease response = pending.Result;

        Assert.Equal(new byte[] { 3, 2, 1 }, response.Payload.ToArray());
        Assert.Equal(2u, response.Header.RequestId);
        Assert.True(response.Header.Flags.HasFlag(ReceiveFlags.IsResponse));
        Assert.Equal(10, response.Header.Channel);
        client.Release(in response);

        Assert.Equal([1u], ids);
        // A response never reaches the channel's handler (PROTOCOL.md §3.1).
        Assert.Empty(onClient);
        PeerStatistics statistics = DatagramKit.Statistics(client);
        Assert.Equal(1, statistics.RequestsSent);
        Assert.Equal(0, statistics.ResponsesUnmatched);
        Assert.Equal(0, statistics.RequestsTimedOut);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);
    }

    [Fact]
    public void A_Request_After_A_Stall_Longer_Than_Its_Timeout_Does_Not_Time_Out_At_Once()
    {
        // The timeout counts from the call. Counted from the peer's last pass it would already be over here, and the Flush
        // that sends the request would time it out first (its timer pass runs before its scheduler pass).
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        Echo(h.Server!, 10);
        h.Pump();
        h.Network.Advance(200_000); // no Poll and no Flush: the last pass is four timeouts old

        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 1, 2, 3 }, TimeSpan.FromMilliseconds(50));
        Assert.Equal(h.Clock.NowMicros + 50_000, OrderedKit.Engine(client).RequestDeadlineMicros);
        client.Flush();
        Assert.False(pending.IsCompleted, "the request timed out in the Flush that sent it");
        Assert.True(h.RunUntil(() => pending.IsCompleted), "no response arrived");
        ReceiveLease response = pending.Result;
        Assert.Equal(new byte[] { 3, 2, 1 }, response.Payload.ToArray());
        client.Release(in response);
        Assert.Equal(0, DatagramKit.Statistics(client).RequestsTimedOut);
    }

    [Fact]
    public void Requests_Of_Every_Shape_Are_Answered()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<uint> keyed = [];
        Echo(server, 11, keyed);
        Echo(server, 12);

        ValueTask<ReceiveLease> withKey = client.SendRequestAsync(new SendHeader(11, 42), new byte[] { 7, 8 }, TimeSpan.FromSeconds(1));
        // A payload LZ4 shrinks, so the response travels compressed and is decoded before the awaiter sees it.
        byte[] compressible = FragmentKit.Compressible(400, 3);
        ValueTask<ReceiveLease> packed = client.SendRequestAsync(new SendHeader(12), compressible, TimeSpan.FromSeconds(1));
        Assert.True(h.RunUntil(() => withKey.IsCompleted && packed.IsCompleted), "not every response arrived");

        ReceiveLease keyedResponse = withKey.Result;
        Assert.Equal(new byte[] { 8, 7 }, keyedResponse.Payload.ToArray());
        Assert.Equal(42UL, keyedResponse.Header.Key);
        client.Release(in keyedResponse);

        ReceiveLease packedResponse = packed.Result;
        Assert.Equal(compressible.Length, packedResponse.Payload.Length);
        Assert.Equal(compressible.Reverse().ToArray(), packedResponse.Payload.ToArray());
        Assert.True(packedResponse.Header.RawLength > 0, "the response arrived compressed");
        client.Release(in packedResponse);
        Assert.Equal([1u], keyed);
    }

    [Fact]
    public void Many_Concurrent_Requests_Are_Matched_By_Their_Ids()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        const int Count = 64;
        List<(ReceiveHeader Header, byte[] Payload)> requests = [];
        server.RegisterHandler(10, Handlers.Collect(requests));

        ValueTask<ReceiveLease>[] pending = new ValueTask<ReceiveLease>[Count];
        for (int i = 0; i < Count; i++)
        {
            pending[i] = client.SendRequestAsync(new SendHeader(10), new byte[] { (byte)i, 0xAA }, TimeSpan.FromSeconds(5));
        }

        Assert.Equal(Count, OrderedKit.Engine(client).OutstandingRequests);
        Assert.True(h.RunUntil(() => requests.Count == Count), $"{requests.Count} of {Count} requests arrived");

        // Answered in reverse order, so nothing but the id can match a response to its request.
        for (int i = Count - 1; i >= 0; i--)
        {
            ReceiveHeader request = requests[i].Header;
            Assert.Equal(SendStatus.Admitted, server.Respond(in request, [requests[i].Payload[0], 0xBB]).Status);
        }

        Assert.True(h.RunUntil(() => pending.All(p => p.IsCompleted)), "not every response arrived");
        for (int i = 0; i < Count; i++)
        {
            ReceiveLease response = pending[i].Result;
            Assert.Equal(new byte[] { (byte)i, 0xBB }, response.Payload.ToArray());
            Assert.Equal((uint)((i * 2) + 2), response.Header.RequestId);
            client.Release(in response);
        }

        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);
        Assert.Equal(Count, DatagramKit.Statistics(client).RequestsSent);
        Assert.Equal(0, DatagramKit.Statistics(client).ResponsesUnmatched);
    }

    [Fact]
    public void A_Request_Times_Out_In_Poll_And_In_Flush()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        // The peer never answers.
        h.Server!.RegisterHandler(10, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });

        ValueTask<ReceiveLease> polled = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.FromMilliseconds(50));
        long deadline = OrderedKit.Engine(client).RequestDeadlineMicros;
        Assert.True(deadline > h.Clock.NowMicros, "the deadline is in the future");
        // The peer publishes deadlines from its timer pass, so the request's shows up at the next Poll or Flush.
        client.Flush();
        Assert.Equal(deadline, client.NextPollDeadlineMicros);
        Assert.True(client.NextPollDeadlineMicros > h.Clock.NowMicros, "a deadline at or before now would make a sleeping host spin");

        h.Network.Advance(60_000);
        client.Poll();
        Assert.True(polled.IsCompleted);
        Assert.Throws<TimeoutException>(() => _ = polled.Result);
        Assert.Equal(1, DatagramKit.Statistics(client).RequestsTimedOut);
        Assert.Equal(long.MaxValue, OrderedKit.Engine(client).RequestDeadlineMicros);

        // A Flush alone serves the deadline too: a host that only flushes still times its requests out.
        ValueTask<ReceiveLease> flushed = client.SendRequestAsync(new SendHeader(10), new byte[] { 2 }, TimeSpan.FromMilliseconds(50));
        h.Network.Advance(60_000);
        client.Flush();
        Assert.True(flushed.IsCompleted);
        Assert.Throws<TimeoutException>(() => _ = flushed.Result);
        Assert.Equal(2, DatagramKit.Statistics(client).RequestsTimedOut);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void A_Response_That_Arrives_After_The_Timeout_Is_Dropped_And_Counted()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> requests = [];
        server.RegisterHandler(10, Handlers.Collect(requests));
        List<(ReceiveHeader Header, byte[] Payload)> onClient = [];
        client.RegisterHandler(10, Handlers.Collect(onClient));

        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.FromMilliseconds(50));
        Assert.True(h.RunUntil(() => requests.Count == 1), "the request did not arrive");
        h.Network.Advance(60_000);
        client.Poll();
        Assert.Throws<TimeoutException>(() => _ = pending.Result);

        ReceiveHeader late = requests[0].Header;
        Assert.Equal(SendStatus.Admitted, server.Respond(in late, [9]).Status);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).ResponsesUnmatched == 1), "the late response was not counted");
        Assert.Empty(onClient);
        Assert.Equal(0, DatagramKit.Statistics(client).ReceiveBytesOutstanding);
    }

    [Fact]
    public void Cancelling_The_Wait_Does_Not_Cancel_The_Request()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> requests = [];
        server.RegisterHandler(10, Handlers.Collect(requests));
        using CancellationTokenSource cts = new();

        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 5 }, TimeSpan.FromSeconds(5), cts.Token);
        cts.Cancel();
        Assert.True(pending.IsCompleted);
        Assert.Throws<OperationCanceledException>(() => _ = pending.Result);

        // ADR 0004: cancelling a wait never cancels the send, so the peer still receives the request.
        Assert.True(h.RunUntil(() => requests.Count == 1), "the canceled request was not sent");
        Assert.Equal(new byte[] { 5 }, requests[0].Payload);
        ReceiveHeader canceled = requests[0].Header;
        Assert.Equal(SendStatus.Admitted, server.Respond(in canceled, [6]).Status);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).ResponsesUnmatched == 1), "its response was not counted as unmatched");
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);

        // The slot is reusable: a second request works.
        ValueTask<ReceiveLease> again = client.SendRequestAsync(new SendHeader(10), new byte[] { 7 }, TimeSpan.FromSeconds(5));
        Assert.True(h.RunUntil(() => requests.Count == 2), "the second request did not arrive");
        ReceiveHeader second = requests[1].Header;
        Assert.Equal(SendStatus.Admitted, server.Respond(in second, [8]).Status);
        Assert.True(h.RunUntil(() => again.IsCompleted), "the second response did not arrive");
        ReceiveLease response = again.Result;
        Assert.Equal(new byte[] { 8 }, response.Payload.ToArray());
        client.Release(in response);
    }

    [Fact]
    public void A_Response_No_Request_Matches_Is_Dropped_And_Counted()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> onClient = [];
        client.RegisterHandler(10, Handlers.Collect(onClient));

        // The peer answers a request this end never made.
        ReceiveHeader invented = new() { Channel = 10, RequestId = 77 };
        Assert.Equal(SendStatus.Admitted, h.Server!.Respond(in invented, [1, 2]).Status);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).ResponsesUnmatched == 1), "the response was not counted");
        Assert.Empty(onClient);
        Assert.Equal(0, DatagramKit.Statistics(client).ReceiveBytesOutstanding);
    }

    [Fact]
    public void Requests_Fail_When_The_Session_Closes()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        h.Server!.RegisterHandler(10, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });

        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        Assert.True(h.RunUntil(() => OrderedKit.Engine(client).OutstandingRequests == 1), "the request was not armed");

        client.Close(CloseReason.Normal);
        Assert.True(h.RunUntilClosed(), "the session did not close");
        Assert.True(pending.IsCompleted);
        InvalidOperationException failure = Assert.Throws<InvalidOperationException>(() => _ = pending.Result);
        Assert.Contains("closed", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);

        // A request on a session that is gone is refused at once.
        ValueTask<ReceiveLease> afterwards = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        Assert.True(afterwards.IsCompleted);
        Assert.Throws<InvalidOperationException>(() => _ = afterwards.Result);
    }

    [Fact]
    public void Requests_Fail_When_The_Connection_Is_Lost_And_The_Session_Resumes()
    {
        using SessionHarness h = new(
            connect: false,
            table: Table,
            client: o => DatagramKit.Quiet(o),
            server: o => DatagramKit.Quiet(o));
        h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
            AdmissionResult.Accept(new byte[] { 9, 8, 7 }, 4242, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);
        Assert.True(h.RunUntilConnected(), "the session did not connect");
        QuiclyPeer client = h.Client;
        QuiclyPeer oldServer = h.Server!;
        oldServer.RegisterHandler(10, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });

        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        Assert.True(h.RunUntil(() => OrderedKit.Engine(client).OutstandingRequests == 1), "the request was not armed");

        // The link dies without a QUICLY Close, as a lost connection does.
        oldServer.Core.Transport!.Close(0, default);
        Assert.True(h.RunUntil(() => client.State == PeerState.Closed), "the client did not see the loss");
        Assert.True(pending.IsCompleted);
        Assert.Throws<InvalidOperationException>(() => _ = pending.Result);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);

        client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.True(h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected), "the session did not resume");
        oldServer.Dispose();
        Assert.Equal(2u, client.Epoch);

        // The resumed session numbers and matches requests again.
        Echo(h.Server!, 10);
        ValueTask<ReceiveLease> resumed = client.SendRequestAsync(new SendHeader(10), new byte[] { 4, 5 }, TimeSpan.FromSeconds(1));
        Assert.True(h.RunUntil(() => resumed.IsCompleted), "the resumed session answered nothing");
        ReceiveLease response = resumed.Result;
        Assert.Equal(new byte[] { 5, 4 }, response.Payload.ToArray());
        client.Release(in response);
        Assert.Equal(2, DatagramKit.Statistics(client).RequestsSent);
    }

    [Fact]
    public void Requests_Fail_When_The_Peer_Is_Disposed()
    {
        // Disposing a peer without closing it first is ordinary teardown. After Dispose no Poll, Flush or Drain runs, so
        // nothing can match a response or serve a timeout any more, and the engine's own Dispose waits for the transport's
        // close callback — which may come much later or never. Dispose itself therefore fails every request, synchronously:
        // nothing runs between the Dispose call and the assertions below, not even one step of the network
        // (docs/design/session-layer.md §7.8).
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        h.Server!.RegisterHandler(10, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        ValueTask<ReceiveLease> patient = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        Task<ReceiveLease> awaited = client.SendRequestAsync(new SendHeader(10), new byte[] { 2 }, TimeSpan.FromSeconds(30)).AsTask();
        Assert.True(h.RunUntil(() => OrderedKit.Engine(client).OutstandingRequests == 2), "the requests were not armed");

        h.DisposeClient();
        Assert.True(patient.IsCompleted, "a request without a timeout outlived the Dispose of its peer");
        Assert.Throws<ObjectDisposedException>(() => _ = patient.Result);
        Assert.True(awaited.IsCompleted, "the continuation of an awaited request did not run when its peer was disposed");
        Assert.IsType<ObjectDisposedException>(awaited.Exception?.InnerException);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);
        Assert.Equal(long.MaxValue, OrderedKit.Engine(client).RequestDeadlineMicros);
        Assert.Throws<ObjectDisposedException>(() => client.SendRequestAsync(new SendHeader(10), new byte[] { 3 }, TimeSpan.Zero));

        // The transport's close callback, when it does come, frees the engine; failing the requests again is a no-op.
        h.Network.RunUntilIdle(1_000_000);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);
    }

    [Fact]
    public void A_Positive_Duration_Never_Becomes_The_Zero_Sentinel()
    {
        // Every caller of PeerOptions.ToMicros reads 0 as "off" or "none" — a request without a timeout, a disabled heartbeat
        // or stream-idle check, no close linger, no group interval, immediate acks — so truncating a positive duration towards
        // zero would silently switch the feature off. Rounding up is done once, for all of them.
        Assert.Equal(0, PeerOptions.ToMicros(TimeSpan.Zero));
        Assert.Equal(1, PeerOptions.ToMicros(TimeSpan.FromTicks(1)));
        Assert.Equal(1, PeerOptions.ToMicros(TimeSpan.FromTicks(5)));
        Assert.Equal(1, PeerOptions.ToMicros(TimeSpan.FromTicks(19)));
        Assert.Equal(2, PeerOptions.ToMicros(TimeSpan.FromTicks(20)));
        Assert.Equal(1_000, PeerOptions.ToMicros(TimeSpan.FromMilliseconds(1)));
        Assert.Equal(-1, PeerOptions.ToMicros(TimeSpan.FromTicks(-10)));

        // End to end for the option whose zero means "disabled": a 500 ns heartbeat timeout is a heartbeat that fires, not one
        // that was switched off.
        using SessionHarness h = new(
            connect: false,
            table: Table,
            client: o =>
            {
                DatagramKit.Quiet(o);
                o.HeartbeatTimeout = TimeSpan.FromTicks(5);
            },
            server: DatagramKit.Quiet);
        Assert.True(
            h.RunUntil(() => h.ClientEvents.Exists(e => e.To == PeerState.Connected) && h.Client.State is PeerState.Closing or PeerState.Closed, 5_000_000),
            $"a 500 ns heartbeat timeout never fired; the client is {h.Client.State}");
    }

    [Fact]
    public void The_Request_Surface_Refuses_What_It_Cannot_Do()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;

        Assert.Throws<ArgumentException>(() => client.SendRequestAsync(new SendHeader(99), default, TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => client.SendRequestAsync(new SendHeader(10), default, TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => client.SendRequestAsync(new SendHeader(11, ulong.MaxValue), default, TimeSpan.FromSeconds(1)));

        // A channel without request ids, and a datagram channel, cannot correlate anything.
        ValueTask<ReceiveLease> plain = client.SendRequestAsync(new SendHeader(4), new byte[] { 1 }, TimeSpan.FromSeconds(1));
        Assert.Throws<NotSupportedException>(() => _ = plain.Result);
        ValueTask<ReceiveLease> datagram = client.SendRequestAsync(new SendHeader(2), new byte[] { 1 }, TimeSpan.FromSeconds(1));
        Assert.Throws<NotSupportedException>(() => _ = datagram.Result);

        using CancellationTokenSource canceled = new();
        canceled.Cancel();
        ValueTask<ReceiveLease> already = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.FromSeconds(1), canceled.Token);
        Assert.Throws<TaskCanceledException>(() => _ = already.Result);

        // Respond needs a request header of a request/response channel.
        ReceiveHeader plainMessage = new() { Channel = 10, RequestId = 0 };
        Assert.Equal(SendStatus.NotSupported, client.Respond(in plainMessage, [1]).Status);
        ReceiveHeader response = new() { Channel = 10, RequestId = 2 };
        Assert.Equal(SendStatus.NotSupported, client.Respond(in response, [1]).Status);
        ReceiveHeader unanswerable = new() { Channel = 10, RequestId = uint.MaxValue };
        Assert.Equal(SendStatus.NotSupported, client.Respond(in unanswerable, [1]).Status);
        ReceiveHeader wrongChannel = new() { Channel = 4, RequestId = 1 };
        Assert.Equal(SendStatus.NotSupported, client.Respond(in wrongChannel, [1]).Status);
        ReceiveHeader unknownChannel = new() { Channel = 99, RequestId = 1 };
        Assert.Equal(SendStatus.InvalidChannel, client.Respond(in unknownChannel, [1]).Status);
    }

    [Fact]
    public void The_Request_Table_Is_Bounded()
    {
        using SessionHarness h = Harness(client: OrderedKit.Roomy, server: OrderedKit.Roomy);
        QuiclyPeer client = h.Client;
        h.Server!.RegisterHandler(10, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        ValueTask<ReceiveLease>[] pending = new ValueTask<ReceiveLease>[ReliableOrderedEngine.MaxOutstandingRequests];
        for (int i = 0; i < pending.Length; i++)
        {
            pending[i] = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
            Assert.False(pending[i].IsCompleted, $"request {i} was refused");
        }

        ValueTask<ReceiveLease> refused = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        Assert.True(refused.IsCompleted);
        InvalidOperationException failure = Assert.Throws<InvalidOperationException>(() => _ = refused.Result);
        Assert.Contains("waiting for a response", failure.Message, StringComparison.Ordinal);
        Assert.Equal(ReliableOrderedEngine.MaxOutstandingRequests, OrderedKit.Engine(client).OutstandingRequests);

        client.Close(CloseReason.Normal);
        Assert.True(h.RunUntilClosed(), "the session did not close");
        foreach (ValueTask<ReceiveLease> request in pending)
        {
            Assert.Throws<InvalidOperationException>(() => _ = request.Result);
        }
    }

    [Fact]
    public void A_Request_The_Channel_Cannot_Take_Fails_Without_Holding_A_Slot()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        // Channel 13 admits 256 bytes of unacknowledged traffic, so the second message does not fit.
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(13), new byte[300]).Status);

        ValueTask<ReceiveLease> refused = client.SendRequestAsync(new SendHeader(13), new byte[300], TimeSpan.FromSeconds(1));
        Assert.True(refused.IsCompleted);
        InvalidOperationException failure = Assert.Throws<InvalidOperationException>(() => _ = refused.Result);
        Assert.Contains("QueueFull", failure.Message, StringComparison.Ordinal);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);
        Assert.Equal(0, DatagramKit.Statistics(client).RequestsSent);

        // The slot it took is back: a request on a channel that can take one still works.
        Echo(h.Server!, 10);
        ValueTask<ReceiveLease> ok = client.SendRequestAsync(new SendHeader(10), new byte[] { 1, 2 }, TimeSpan.FromSeconds(1));
        Assert.True(h.RunUntil(() => ok.IsCompleted), "the later request was not answered");
        ReceiveLease response = ok.Result;
        Assert.Equal(new byte[] { 2, 1 }, response.Payload.ToArray());
        client.Release(in response);
    }

    [Fact]
    public void A_Response_For_Another_Id_Is_Counted_While_A_Request_Waits()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> requests = [];
        server.RegisterHandler(10, Handlers.Collect(requests));

        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        Assert.True(h.RunUntil(() => requests.Count == 1), "the request did not arrive");

        // A response whose id matches no outstanding request is dropped, even while one is waiting.
        ReceiveHeader invented = new() { Channel = 10, RequestId = 4242 * 2 - 1 };
        Assert.Equal(SendStatus.Admitted, server.Respond(in invented, [7]).Status);
        Assert.True(h.RunUntil(() => DatagramKit.Statistics(client).ResponsesUnmatched == 1), "the stray response was not counted");
        Assert.False(pending.IsCompleted, "the waiting request was not disturbed");

        ReceiveHeader mine = requests[0].Header;
        Assert.Equal(SendStatus.Admitted, server.Respond(in mine, [8]).Status);
        Assert.True(h.RunUntil(() => pending.IsCompleted), "the real response did not arrive");
        ReceiveLease response = pending.Result;
        Assert.Equal(new byte[] { 8 }, response.Payload.ToArray());
        client.Release(in response);
    }

    [Fact]
    public void A_Canceled_Slot_Is_Reclaimed_When_Another_Response_Walks_Past_It()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> requests = [];
        server.RegisterHandler(10, Handlers.Collect(requests));
        using CancellationTokenSource cts = new();

        ValueTask<ReceiveLease> canceled = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero, cts.Token);
        ValueTask<ReceiveLease> kept = client.SendRequestAsync(new SendHeader(10), new byte[] { 2 }, TimeSpan.Zero);
        Assert.True(h.RunUntil(() => requests.Count == 2), "the requests did not arrive");
        cts.Cancel();
        Assert.Throws<OperationCanceledException>(() => _ = canceled.Result);

        // Answering the second request walks past the canceled slot, which leaves the live set on the way.
        ReceiveHeader second = requests[1].Header;
        Assert.Equal(SendStatus.Admitted, server.Respond(in second, [9]).Status);
        Assert.True(h.RunUntil(() => kept.IsCompleted), "the second response did not arrive");
        ReceiveLease response = kept.Result;
        Assert.Equal(new byte[] { 9 }, response.Payload.ToArray());
        client.Release(in response);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);
    }

    [Fact]
    public void A_Request_Without_A_Timeout_Outlives_One_That_Has_It()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> requests = [];
        server.RegisterHandler(10, Handlers.Collect(requests));

        ValueTask<ReceiveLease> patient = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        ValueTask<ReceiveLease> impatient = client.SendRequestAsync(new SendHeader(10), new byte[] { 2 }, TimeSpan.FromMilliseconds(50));
        Assert.True(h.RunUntil(() => requests.Count == 2), "the requests did not arrive");

        h.Network.Advance(60_000);
        client.Poll();
        Assert.Throws<TimeoutException>(() => _ = impatient.Result);
        Assert.False(patient.IsCompleted, "a request without a timeout keeps waiting");
        Assert.Equal(1, OrderedKit.Engine(client).OutstandingRequests);
        Assert.Equal(long.MaxValue, OrderedKit.Engine(client).RequestDeadlineMicros);

        ReceiveHeader first = requests[0].Header;
        Assert.Equal(SendStatus.Admitted, server.Respond(in first, [3]).Status);
        Assert.True(h.RunUntil(() => patient.IsCompleted), "the patient request was never answered");
        ReceiveLease response = patient.Result;
        Assert.Equal(new byte[] { 3 }, response.Payload.ToArray());
        client.Release(in response);
    }

    [Fact]
    public void The_Next_Deadline_Moves_To_The_Request_That_Times_Out_Next()
    {
        // A host that only polls sleeps on NextPollDeadlineMicros (ADR 0008 invariant 9), so when one request times out the
        // published deadline must move to the next request's timeout — not stay at the one that passed (a spin) and not
        // vanish (a request that never times out).
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        List<(ReceiveHeader Header, byte[] Payload)> requests = [];
        h.Server!.RegisterHandler(10, Handlers.Collect(requests));

        ValueTask<ReceiveLease> patient = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        ValueTask<ReceiveLease> soon = client.SendRequestAsync(new SendHeader(10), new byte[] { 2 }, TimeSpan.FromMilliseconds(50));
        ValueTask<ReceiveLease> later = client.SendRequestAsync(new SendHeader(10), new byte[] { 3 }, TimeSpan.FromMilliseconds(400));
        long soonDeadline = OrderedKit.Engine(client).RequestDeadlineMicros;
        Assert.True(h.RunUntil(() => requests.Count == 3), "the requests did not arrive");

        h.Network.Advance(60_000);
        client.Poll();
        Assert.Throws<TimeoutException>(() => _ = soon.Result);
        long laterDeadline = OrderedKit.Engine(client).RequestDeadlineMicros;
        Assert.Equal(soonDeadline + 350_000, laterDeadline);
        Assert.True(client.NextPollDeadlineMicros <= laterDeadline, "the poll deadline does not cover the next request's timeout");
        Assert.True(client.NextPollDeadlineMicros > h.Clock.NowMicros, "the published deadline is not in the future");
        Assert.False(later.IsCompleted, "a request timed out before its own timeout");

        h.Network.AdvanceTo(laterDeadline);
        client.Poll();
        Assert.Throws<TimeoutException>(() => _ = later.Result);
        Assert.False(patient.IsCompleted, "the request without a timeout timed out");
        Assert.Equal(long.MaxValue, OrderedKit.Engine(client).RequestDeadlineMicros);
        Assert.Equal(2, DatagramKit.Statistics(client).RequestsTimedOut);
    }

    [Fact]
    public void Cancelling_After_The_Response_Arrived_Does_Not_Undo_It()
    {
        // The response completed the wait first; a cancellation that comes afterwards — before the caller read the result —
        // loses that race and must leave the response in place (the registration is only dropped once the value task is
        // consumed, so the callback does run).
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        Echo(h.Server!, 10);
        using CancellationTokenSource cts = new();

        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 4, 5 }, TimeSpan.Zero, cts.Token);
        Assert.True(h.RunUntil(() => pending.IsCompleted), "no response arrived");
        cts.Cancel();
        ReceiveLease response = pending.Result;
        Assert.Equal(new byte[] { 5, 4 }, response.Payload.ToArray());
        client.Release(in response);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);

        // The slot went back to the pool whole: the next request reuses it and is answered normally.
        ValueTask<ReceiveLease> next = client.SendRequestAsync(new SendHeader(10), new byte[] { 6, 7 }, TimeSpan.Zero);
        Assert.True(h.RunUntil(() => next.IsCompleted), "the next request was not answered");
        ReceiveLease nextResponse = next.Result;
        Assert.Equal(new byte[] { 7, 6 }, nextResponse.Payload.ToArray());
        client.Release(in nextResponse);
    }

    [Fact]
    public void Requests_Are_Answered_On_A_Lossy_Reordering_Link()
    {
        // Requests ride the channel's persistent ordered stream, so the loss of PROTOCOL.md §3.1 is a retransmission delay and
        // never a gap: every response must arrive, and nothing but its id may decide which request it completes. The
        // reordering is applied where a stream channel can have it — the peer answers in reverse order and plain messages of
        // the same channel travel between the requests — while the link delays, jitters and retransmits underneath.
        const int Count = 32;
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 5_000, JitterMicros = 2_000, StreamLossPercent = 5 },
            table: Table,
            client: DatagramKit.Quiet,
            server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> requests = [];
        int plain = 0;
        server.RegisterHandler(10, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            if (header.Flags.HasFlag(ReceiveFlags.IsRequest))
            {
                requests.Add((header, payload.ToArray()));
            }
            else
            {
                plain++;
            }
        });
        List<(ReceiveHeader Header, byte[] Payload)> onClient = [];
        client.RegisterHandler(10, Handlers.Collect(onClient));

        ValueTask<ReceiveLease>[] pending = new ValueTask<ReceiveLease>[Count];
        for (int i = 0; i < Count; i++)
        {
            pending[i] = client.SendRequestAsync(new SendHeader(10), new byte[] { (byte)i, 0xAA }, TimeSpan.Zero);
            Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(10), [(byte)i]).Status);
        }

        Assert.True(
            h.RunUntil(() => requests.Count == Count && plain == Count),
            $"{requests.Count} requests and {plain} plain messages of {Count} each reached the peer");
        for (int i = Count - 1; i >= 0; i--)
        {
            ReceiveHeader request = requests[i].Header;
            Assert.Equal(SendStatus.Admitted, server.Respond(in request, [requests[i].Payload[0], 0xBB]).Status);
        }

        Assert.True(h.RunUntil(() => pending.All(p => p.IsCompleted)), $"{pending.Count(p => p.IsCompleted)} of {Count} responses arrived");
        for (int i = 0; i < Count; i++)
        {
            ReceiveLease response = pending[i].Result;
            Assert.Equal(new byte[] { (byte)i, 0xBB }, response.Payload.ToArray());
            Assert.Equal((uint)((i * 2) + 2), response.Header.RequestId);
            client.Release(in response);
        }

        Assert.Empty(onClient);
        PeerStatistics statistics = DatagramKit.Statistics(client);
        Assert.Equal(Count, statistics.RequestsSent);
        Assert.Equal(0, statistics.RequestsTimedOut);
        Assert.Equal(0, statistics.ResponsesUnmatched);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);
        Assert.Equal(PeerState.Connected, client.State);
    }

    [Fact]
    public void A_Drained_Response_Completes_Its_Request_And_Reclaims_A_Canceled_Slot()
    {
        // Two things only the batch API reaches. Drain offers a response to the engine before it would write it to the
        // caller's span (PROTOCOL.md §3.1), and because Drain runs no timer pass the slot of the canceled wait is still in the
        // live set when the engine walks its table — which is the walk that takes it out.
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> requests = [];
        server.RegisterHandler(10, Handlers.Collect(requests));
        using CancellationTokenSource cts = new();

        ValueTask<ReceiveLease> canceled = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero, cts.Token);
        ValueTask<ReceiveLease> kept = client.SendRequestAsync(new SendHeader(10), new byte[] { 2 }, TimeSpan.Zero);
        Assert.True(h.RunUntil(() => requests.Count == 2), $"{requests.Count} of 2 requests arrived");
        cts.Cancel();
        Assert.Throws<OperationCanceledException>(() => _ = canceled.Result);
        Assert.Equal(2, OrderedKit.Engine(client).OutstandingRequests);

        // No client Poll and no client Flush from here, so nothing sweeps the canceled slot before the walk does.
        ReceiveHeader second = requests[1].Header;
        Assert.Equal(SendStatus.Admitted, server.Respond(in second, [9]).Status);
        server.Flush();
        h.Network.Advance(100_000);

        ReceivedMessage[] buffer = new ReceivedMessage[4];
        Assert.Equal(0, client.Drain(10, buffer));
        Assert.True(kept.IsCompleted, "the drained response did not complete its request");
        ReceiveLease response = kept.Result;
        Assert.Equal(new byte[] { 9 }, response.Payload.ToArray());
        client.Release(in response);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);
        Assert.Equal(0, DatagramKit.Statistics(client).ResponsesUnmatched);
        Assert.Equal(0, DatagramKit.Statistics(client).ReceiveBytesOutstanding);
    }

    [Fact]
    public void Drain_Never_Hands_A_Response_To_The_Batch_API()
    {
        // The batch alternative to a handler is as blind to responses as Poll is: of a response and a plain message of the
        // same channel, only the plain one reaches the caller's span.
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> requests = [];
        server.RegisterHandler(10, Handlers.Collect(requests));

        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        Assert.True(h.RunUntil(() => requests.Count == 1), "the request did not arrive");
        ReceiveHeader request = requests[0].Header;
        Assert.Equal(SendStatus.Admitted, server.Respond(in request, [7]).Status);
        Assert.Equal(SendStatus.Admitted, server.SendCopy(new SendHeader(10), [8]).Status);
        server.Flush();
        h.Network.Advance(100_000);

        ReceivedMessage[] buffer = new ReceivedMessage[4];
        int drained = client.Drain(10, buffer);
        Assert.Equal(1, drained);
        Assert.Equal(new byte[] { 8 }, buffer[0].Payload.ToArray());
        Assert.False(buffer[0].Header.Flags.HasFlag(ReceiveFlags.IsResponse), "a response reached the batch API");
        client.Release(buffer.AsSpan(0, drained));

        Assert.True(pending.IsCompleted, "the response did not complete its request");
        ReceiveLease response = pending.Result;
        Assert.Equal(new byte[] { 7 }, response.Payload.ToArray());
        Assert.True(response.Header.Flags.HasFlag(ReceiveFlags.IsResponse));
        client.Release(in response);
        Assert.Equal(0, DatagramKit.Statistics(client).ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Response_Waiting_Behind_Queued_And_Held_Messages_Is_Taken_By_Drain()
    {
        // The invariant that replaces the per-path checks (docs/design/session-layer.md §7.8, "Where responses are
        // intercepted"): a response is taken where it leaves the receive ring, so the per-channel queues and the held slot —
        // which only ever receive what already left the ring — cannot hold one. Here both are really in use when the response
        // arrives: a Drain-style channel's messages fill the queue pool, the next one is held, and the response waits in the
        // ring behind it. Drain then walks all three paths in order — queue, held slot, ring — and must hand the caller only
        // the plain messages while the response completes its request.
        using ServerHarness h = HeldBehindHarness(out ValueTask<ReceiveLease> pending);
        QuiclyPeer server = h.Server!;

        ReceivedMessage[] drained = new ReceivedMessage[4 * HeldPool];
        int count = server.Drain(2, drained);
        Assert.Equal(HeldPool + 1, count);
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(new[] { (byte)i }, drained[i].Payload.ToArray());
            Assert.False(drained[i].Header.Flags.HasFlag(ReceiveFlags.IsResponse), "a response reached the batch API");
        }

        server.Release(drained.AsSpan(0, count));
        Assert.True(pending.IsCompleted, "the response that waited behind the held message did not complete its request");
        ReceiveLease response = pending.Result;
        Assert.Equal(new byte[] { 0x77 }, response.Payload.ToArray());
        server.Release(in response);
        Assert.Equal(0, h.Statistics().ResponsesUnmatched);
        Assert.Equal(0, OrderedKit.Engine(server).OutstandingRequests);
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Response_Waiting_Behind_Queued_And_Held_Messages_Is_Taken_By_Poll()
    {
        // The Poll side of the same invariant: once the channel gets a handler, Poll's handler-queue loop dispatches the queued
        // messages, then the held one, then reads the ring again — and only the plain messages reach the handler.
        using ServerHarness h = HeldBehindHarness(out ValueTask<ReceiveLease> pending);
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(2, Handlers.Collect(got));

        server.Poll();
        Assert.Equal(HeldPool + 1, got.Count);
        for (int i = 0; i < got.Count; i++)
        {
            Assert.Equal(new[] { (byte)i }, got[i].Payload);
            Assert.False(got[i].Header.Flags.HasFlag(ReceiveFlags.IsResponse), "a response reached a channel handler");
        }

        Assert.True(pending.IsCompleted, "the response that waited behind the held message did not complete its request");
        ReceiveLease response = pending.Result;
        Assert.Equal(new byte[] { 0x77 }, response.Payload.ToArray());
        server.Release(in response);
        Assert.Equal(0, h.Statistics().ResponsesUnmatched);
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }

    /// <summary>Drain-queue node pool of <see cref="HeldBehindHarness"/> (it is the receive ring's capacity).</summary>
    private const int HeldPool = 8;

    /// <summary>
    /// A server peer (the peer under test) with one request outstanding, whose drain queues are full, whose held slot holds a
    /// plain message and whose receive ring holds the request's response behind it — so nothing has matched it yet.
    /// </summary>
    private static ServerHarness HeldBehindHarness(out ValueTask<ReceiveLease> pending)
    {
        ServerHarness h = new(table: Table, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = HeldPool;
        });
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        pending = server.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        Assert.False(pending.IsCompleted, "the request was refused");
        server.Flush();

        // Channel 2 has no handler, so Poll moves its messages to the channel's drain queue: HeldPool of them fill the node
        // pool and the next one is held, which stops Poll from taking anything else out of the ring.
        for (int i = 0; i <= HeldPool; i++)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(DatagramKit.Frame(Table, 2, (uint)i, 0, [(byte)i])));
            h.Network.Advance(1_000);
            server.Poll();
        }

        // The peer's response to request 1 (RequestId 2, PROTOCOL.md §3.1) arrives behind the held message.
        ChannelDefinition rpc = Table[10]!;
        byte[] frames = new byte[32];
        int written = StreamFraming.WritePreamble(frames, rpc.Id);
        StreamMessageHeader header = default;
        header.Length = 1;
        header.RequestId = 2;
        written += StreamFraming.WriteFrameHeader(frames.AsSpan(written), rpc, in header);
        frames[written++] = 0x77;
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(frames.AsSpan(0, written), out TransportStreamId _));
        h.Network.Advance(20_000);
        server.Poll();
        server.Poll();
        Assert.False(pending.IsCompleted, "the response was matched although the held message blocks the ring");
        Assert.Equal(1, OrderedKit.Engine(server).OutstandingRequests);
        return h;
    }

    [Fact]
    public void A_Continuation_Attached_Before_The_Response_Runs_When_It_Arrives()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        Echo(h.Server!, 10);

        // AsTask attaches a continuation to the pooled source before the response exists (IValueTaskSource.OnCompleted).
        Task<ReceiveLease> task = client.SendRequestAsync(new SendHeader(10), new byte[] { 4, 5, 6 }, TimeSpan.FromSeconds(1)).AsTask();
        Assert.True(h.RunUntil(() => task.IsCompleted), "the continuation never ran");
        ReceiveLease response = task.Result;
        Assert.Equal(new byte[] { 6, 5, 4 }, response.Payload.ToArray());
        client.Release(in response);
        Assert.Equal(0, OrderedKit.Engine(client).OutstandingRequests);
    }

    [Fact]
    public void Requests_And_Plain_Messages_Share_The_Channel_In_Order()
    {
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(10, Handlers.Collect(got));

        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(10), [1]).Status);
        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 2 }, TimeSpan.FromSeconds(1));
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(10), [3]).Status);
        Assert.True(h.RunUntil(() => got.Count == 3), $"{got.Count} of 3 messages arrived");

        Assert.Equal([1], got[0].Payload);
        Assert.Equal(0u, got[0].Header.RequestId);
        Assert.Equal([2], got[1].Payload);
        Assert.Equal(1u, got[1].Header.RequestId);
        Assert.True(got[1].Header.Flags.HasFlag(ReceiveFlags.IsRequest));
        Assert.Equal([3], got[2].Payload);
        Assert.Equal(0u, got[2].Header.RequestId);

        ReceiveHeader request = got[1].Header;
        Assert.Equal(SendStatus.Admitted, server.Respond(in request, [4]).Status);
        Assert.True(h.RunUntil(() => pending.IsCompleted), "the response did not arrive");
        ReceiveLease response = pending.Result;
        Assert.Equal([4], response.Payload.ToArray());
        client.Release(in response);
    }
}
