using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Correlated request/response on ReliableOrdered channels (PROTOCOL.md §3.1, §4.3; docs/design/session-layer.md §7.8):
/// the happy path, timeouts processed in Poll and Flush, cancellation that does not cancel the send, many concurrent
/// requests, unmatched responses, and what happens when the session ends.
/// </summary>
public class RequestResponseTests
{
    /// <summary>2 movement datagrams · 4 plain ordered · 10 ordered request/response · 11 the same, keyed · 12 the same, LZ4.</summary>
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
        using SessionHarness h = Harness();
        QuiclyPeer client = h.Client;
        h.Server!.RegisterHandler(10, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.Zero);
        Assert.True(h.RunUntil(() => OrderedKit.Engine(client).OutstandingRequests == 1), "the request was not armed");

        h.DisposeClient();
        h.Network.RunUntilIdle(1_000_000);
        Assert.True(pending.IsCompleted);
        Assert.ThrowsAny<Exception>(() => _ = pending.Result);
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
