using System.Numerics;

namespace Tedd.Quicly.Replication.Tests;

/// <summary>
/// Review findings (adversarial review of the replication module). Each test states the documented or specified
/// behaviour it checks; the tests fail against the implementation under review.
/// </summary>
public class ReviewTests
{
    /// <summary>
    /// SnapshotHistory's remarks recommend <c>AckBaseline</c> on a tracked unreliable send's <c>Delivered</c> and state
    /// "any acknowledged baseline is guaranteed to be available there too". PROTOCOL.md §4.3 says a transport ack
    /// only proves delivery to the peer's transport. On an <c>UnreliableSequenced</c> channel the receiver drops a
    /// datagram that arrives after a newer one (it is still transport-acked). If that Delivered completion is
    /// processed before the newer one's, the sender encodes against a snapshot the receiver never decoded; that
    /// undecodable delta is itself transport-acked and becomes the next baseline, and so on: permanent desync with
    /// every later datagram delivered.
    /// </summary>
    [Fact]
    public void Review_Documented_Delivered_Ack_Pairing_Recovers_From_A_Dropped_Stale_Datagram()
    {
        SnapshotHistory server = new(32, 64 * 1024, 1);
        SnapshotHistory client = new(32, 64 * 1024, 0);
        byte[] world = new byte[256];
        byte[] packet = new byte[DeltaCodec.GetMaxEncodedLength(world.Length)];
        byte[] decoded = new byte[world.Length];
        uint lastSequencedTick = 0;

        // Returns true when the client could decode (it drops stale datagrams like UnreliableSequenced, and
        // datagrams whose baseline it does not have).
        bool Deliver(uint tick, bool hasBaseline, uint baselineTick, ReadOnlySpan<byte> delta)
        {
            if (tick <= lastSequencedTick)
            {
                return false;   // sequenced channel: older than the newest delivered message
            }

            lastSequencedTick = tick;
            ReadOnlySpan<byte> clientBaseline = default;
            if (hasBaseline && !client.TryGet(baselineTick, out clientBaseline))
            {
                return false;   // baseline never decoded here
            }

            int m = DeltaCodec.Decode(clientBaseline, delta, decoded);
            Assert.True(m >= 0);
            client.Store(tick, decoded.AsSpan(0, m));
            return true;
        }

        (bool Has, uint Baseline, byte[] Delta) Send(uint tick)
        {
            world[tick % (uint)world.Length] ^= (byte)tick;
            Assert.True(server.Store(tick, world));
            bool has = server.TryGetBaseline(0, out uint baselineTick, out ReadOnlySpan<byte> baseline);
            int n = DeltaCodec.Encode(has ? baseline : default, world, packet);
            return (has, baselineTick, packet.AsSpan(0, n).ToArray());
        }

        // Tick 1: full snapshot, delivered, acked.
        var s1 = Send(1);
        Assert.True(Deliver(1, s1.Has, s1.Baseline, s1.Delta));
        server.AckBaseline(0, 1);

        // Ticks 2 and 3 are sent; 3 overtakes 2 in the network, so the sequenced receiver drops 2.
        var s2 = Send(2);
        var s3 = Send(3);
        Assert.True(Deliver(3, s3.Has, s3.Baseline, s3.Delta));
        Assert.False(Deliver(2, s2.Has, s2.Baseline, s2.Delta));

        // Both were acknowledged by the peer's QUIC stack; the completion for 2 is processed first.
        server.AckBaseline(0, 2);

        // From here on every datagram is delivered in order and acknowledged immediately; the completion for 3
        // arrives late (after 4's), which AckBaseline ignores as older.
        int decodedAfter = 0;
        for (uint tick = 4; tick < 100; tick++)
        {
            var s = Send(tick);
            if (Deliver(tick, s.Has, s.Baseline, s.Delta))
            {
                decodedAfter++;
            }

            server.AckBaseline(0, tick);
            if (tick == 5)
            {
                server.AckBaseline(0, 3);
            }
        }

        // Documented guarantee: an acknowledged baseline is available at the receiver, so the stream recovers.
        Assert.True(decodedAfter > 0, "Receiver never decodes again: every later delta is against a baseline it never had.");
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
