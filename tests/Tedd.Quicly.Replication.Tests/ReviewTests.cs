using System.Numerics;

namespace Tedd.Quicly.Replication.Tests;

/// <summary>
/// Review findings (adversarial review of the replication module). Each test states the documented or specified
/// behaviour it checks; they failed against the implementation under review and pass after the follow-up fixes.
/// </summary>
public class ReviewTests
{
    /// <summary>
    /// A server → client delta link following the pairing documented on <see cref="SnapshotHistory"/>: each message
    /// carries its tick and its baseline tick; the client keeps a history of decoded snapshots and echoes the newest
    /// tick it decoded (an application-level acknowledgement); the server calls <c>AckBaseline</c> only with echoes.
    /// The client's channel behaves like <c>UnreliableSequenced</c>: a message older than the newest delivered one is
    /// dropped by the receiver (although the transport acknowledged it).
    /// </summary>
    private sealed class DeltaLink
    {
        public readonly SnapshotHistory Server = new(32, 64 * 1024, 1);
        public readonly SnapshotHistory Client = new(32, 64 * 1024, 0);
        private readonly byte[] _world = new byte[256];
        private readonly byte[] _packet = new byte[DeltaCodec.GetMaxEncodedLength(256)];
        private readonly byte[] _decoded = new byte[256];
        private uint _lastSequencedTick;

        public readonly record struct Message(uint Tick, bool HasBaseline, uint BaselineTick, byte[] Delta);

        /// <summary>The newest tick the client decoded (what it echoes), 0 before the first.</summary>
        public uint NewestDecoded { get; private set; }

        public Message Send(uint tick)
        {
            _world[tick % (uint)_world.Length] ^= (byte)tick;
            Assert.True(Server.Store(tick, _world));
            bool has = Server.TryGetBaseline(0, out uint baselineTick, out ReadOnlySpan<byte> baseline);
            int n = DeltaCodec.Encode(has ? baseline : default, _world, _packet);
            return new Message(tick, has, baselineTick, _packet.AsSpan(0, n).ToArray());
        }

        /// <summary>Receiver side. Returns <see langword="false"/> when the message was dropped or its baseline is missing.</summary>
        public bool Receive(Message message, out bool missingBaseline)
        {
            missingBaseline = false;
            if (message.Tick <= _lastSequencedTick)
            {
                return false;   // sequenced channel: older than the newest delivered message
            }

            _lastSequencedTick = message.Tick;
            ReadOnlySpan<byte> clientBaseline = default;
            if (message.HasBaseline && !Client.TryGet(message.BaselineTick, out clientBaseline))
            {
                missingBaseline = true;
                return false;
            }

            int m = DeltaCodec.Decode(clientBaseline, message.Delta, _decoded);
            Assert.Equal(_world.Length, m);
            Assert.True(Server.TryGet(message.Tick, out ReadOnlySpan<byte> sent));
            Assert.True(_decoded.AsSpan(0, m).SequenceEqual(sent));
            Assert.True(Client.Store(message.Tick, _decoded.AsSpan(0, m)));
            NewestDecoded = message.Tick;
            return true;
        }

        /// <summary>Client lost its history (reconnect); it keeps receiving on the same sequenced channel.</summary>
        public void ClientLosesHistory()
        {
            Client.Clear();
            NewestDecoded = 0;
        }
    }

    /// <summary>
    /// SnapshotHistory's remarks used to recommend <c>AckBaseline</c> on a tracked unreliable send's <c>Delivered</c>,
    /// which PROTOCOL.md §4.3 does not support (a transport ack only proves delivery to the peer's transport): a
    /// sequenced receiver drops tick 2 after tick 3 overtook it, Delivered(2) makes 2 the baseline, and every later
    /// delta is undecodable while every datagram arrives. With the documented application-level acknowledgements
    /// (the client echoes the newest tick it decoded) the dropped datagram never becomes a baseline, and every later
    /// message decodes — also with delayed, reordered and lost echoes.
    /// </summary>
    [Fact]
    public void Review_Documented_Application_Ack_Pairing_Recovers_From_A_Dropped_Stale_Datagram()
    {
        DeltaLink link = new();

        // Tick 1: full snapshot, decoded, echoed.
        Assert.True(link.Receive(link.Send(1), out _));
        Assert.True(link.Server.AckBaseline(0, link.NewestDecoded));

        // Ticks 2 and 3 are sent; 3 overtakes 2 in the network, so the sequenced receiver drops 2. Both would have
        // been transport-acknowledged; the client never echoes 2 because it never decoded it.
        DeltaLink.Message m2 = link.Send(2);
        DeltaLink.Message m3 = link.Send(3);
        Assert.True(link.Receive(m3, out _));
        Assert.False(link.Receive(m2, out bool missing));
        Assert.False(missing);
        uint echoAfter3 = link.NewestDecoded;
        Assert.Equal(3u, echoAfter3);

        // From here on every message is delivered in order; echoes reach the server two ticks late, every fifth one is
        // lost, and the stale echo of tick 3 arrives after newer ones (AckBaseline ignores it as older).
        Queue<(uint Due, uint Echo)> echoes = new();
        echoes.Enqueue((5, echoAfter3));
        int decodedAfter = 0;
        for (uint tick = 4; tick < 100; tick++)
        {
            Assert.True(link.Receive(link.Send(tick), out _), $"tick {tick} did not decode");
            decodedAfter++;
            if (tick % 5 != 0)
            {
                echoes.Enqueue((tick + 2, link.NewestDecoded));
            }

            while (echoes.Count > 0 && echoes.Peek().Due <= tick)
            {
                link.Server.AckBaseline(0, echoes.Dequeue().Echo);
            }

            if (tick == 10)
            {
                link.Server.AckBaseline(0, echoAfter3);
            }
        }

        Assert.Equal(96, decodedAfter);
        Assert.True(link.Server.TryGetBaseline(0, out uint baselineTick, out _));
        Assert.True(baselineTick >= 95);
    }

    /// <summary>
    /// The documented recovery path: a receiver that lost its history (so it misses the baseline of the next delta)
    /// requests a full snapshot; the sender answers with <see cref="SnapshotHistory.ResetPeer"/>. An echo sent before
    /// the loss that arrives after the reset (no cross-channel ordering, PROTOCOL.md §4.7) must not re-establish a
    /// baseline the receiver no longer has.
    /// </summary>
    [Fact]
    public void Review_Full_Snapshot_Request_And_ResetPeer_Recover_A_Receiver_That_Lost_Its_History()
    {
        DeltaLink link = new();
        uint staleEcho = 0;
        for (uint tick = 1; tick <= 10; tick++)
        {
            Assert.True(link.Receive(link.Send(tick), out _));
            if (tick == 9)
            {
                staleEcho = link.NewestDecoded;   // in flight while the client loses its history
            }
            else
            {
                link.Server.AckBaseline(0, link.NewestDecoded);
            }
        }

        link.ClientLosesHistory();
        DeltaLink.Message m11 = link.Send(11);
        Assert.True(m11.HasBaseline);
        Assert.False(link.Receive(m11, out bool missingBaseline));
        Assert.True(missingBaseline);

        // The full-snapshot request arrives: the server resets the peer. The stale echo arrives afterwards.
        link.Server.ResetPeer(0);
        Assert.False(link.Server.AckBaseline(0, staleEcho));
        Assert.False(link.Server.AckBaseline(0, 10));

        // Next message is a full snapshot; its echo becomes the new baseline and deltas resume.
        DeltaLink.Message m12 = link.Send(12);
        Assert.False(m12.HasBaseline);
        Assert.True(link.Receive(m12, out _));
        Assert.True(link.Server.AckBaseline(0, link.NewestDecoded));
        for (uint tick = 13; tick <= 20; tick++)
        {
            DeltaLink.Message message = link.Send(tick);
            Assert.True(message.HasBaseline);
            Assert.True(link.Receive(message, out _), $"tick {tick} did not decode");
            link.Server.AckBaseline(0, link.NewestDecoded);
        }
    }

    /// <summary>
    /// QuantizeUnitVector: "it need not be normalized. Zero or non-finite vectors encode +Z." A finite direction whose
    /// |x| + |y| + |z| overflows float (components around 2e38) is silently encoded as +Z.
    /// </summary>
    [Fact]
    public void Review_UnitVector_Large_Finite_Direction_Is_Not_Replaced_By_PlusZ()
    {
        Vector3 direction = new(2e38f, 2e38f, 0f);
        Vector3 back = Quantization.DequantizeUnitVector(Quantization.QuantizeUnitVector(direction, 12), 12);
        Vector3 expected = Vector3.Normalize(new Vector3(1, 1, 0));
        Assert.True(Vector3.Distance(expected, back) < 0.01f, $"got {back}");
    }

    /// <summary>
    /// QuantizeQuaternion: "it is normalized first. Zero or non-finite quaternions encode identity." A finite,
    /// non-zero quaternion whose squared length underflows (components 1e-25) or overflows (components 1e20) float is
    /// silently encoded as identity instead of the rotation it represents (here 180° about X).
    /// </summary>
    [Theory]
    [InlineData(1e-25f)]
    [InlineData(1e20f)]
    public void Review_Quaternion_Tiny_Or_Huge_Finite_Input_Is_Not_Replaced_By_Identity(float scale)
    {
        Quaternion rotation = new(scale, 0f, 0f, 0f);   // 180° about X
        Quaternion back = Quantization.DequantizeQuaternion(Quantization.QuantizeQuaternion(rotation, 12), 12);
        Assert.True(MathF.Abs(back.X) > 0.999f, $"got {back}");
    }

    private struct RecordingReplay : IReplay<int>
    {
        public List<uint> Replayed;

        public void Replay(uint sequence, ref int state)
        {
            Replayed.Add(sequence);
            state += 1;   // every input adds one
        }
    }

    /// <summary>
    /// Task: "Reconcile(S, ...) drops &lt;= S and returns the corrected state after replaying later inputs". When more
    /// inputs are outstanding than <see cref="PredictionHistory{TState}.Capacity"/>, Record silently drops the oldest
    /// entries and Reconcile then replays only the entries still recorded, skipping inputs S+1.. that were never
    /// acknowledged: the "corrected" state is silently wrong. The replay callback fetches inputs by sequence (from
    /// an InputBuffer), so replaying S+1..newest by sequence — or reporting the truncation — is possible.
    /// </summary>
    [Fact]
    public void Review_Reconcile_After_History_Overflow_Replays_Every_Later_Input()
    {
        PredictionHistory<int> history = new(capacity: 2);
        int predicted = 0;
        for (uint sequence = 1; sequence <= 4; sequence++)
        {
            predicted++;
            history.Record(sequence, predicted);
        }

        RecordingReplay replay = new() { Replayed = [] };

        // The server applied input 0 only; inputs 1..4 are all later and must be replayed.
        int corrected = history.Reconcile(0, 0, ref replay);
        Assert.Equal([1u, 2u, 3u, 4u], replay.Replayed);
        Assert.Equal(4, corrected);
    }

    /// <summary>
    /// Task: "Free bumps generation"; EntityIdAllocator.Free docs: "its generation is bumped (so the old id is no longer
    /// alive)". With generationBits = 1 the skip-zero rule maps generation 1 → 0 → 1, so the generation never changes and
    /// a stale id is alive again as soon as its index is reused — generations give no protection at all.
    /// </summary>
    [Fact]
    public void Review_OneGenerationBit_Stale_Id_Is_Not_Alive_After_Reuse()
    {
        EntityIdAllocator allocator = new(1, generationBits: 1);
        Assert.True(allocator.TryAllocate(0, out EntityId first));
        Assert.True(allocator.Free(first, 0));
        Assert.True(allocator.TryAllocate(0, out EntityId second));
        Assert.Equal(first.Index, second.Index);
        Assert.NotEqual(first.Generation, second.Generation);
        Assert.False(allocator.IsAlive(first));
    }
}
