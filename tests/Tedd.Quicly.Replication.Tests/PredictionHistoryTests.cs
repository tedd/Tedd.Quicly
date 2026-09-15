namespace Tedd.Quicly.Replication.Tests;

public record struct PlayerState(int X, int Y);

public record struct Step(int Dx, int Dy);

public class PredictionHistoryTests
{
    private struct Replayer : IReplay<PlayerState>
    {
        public InputBuffer<Step> Inputs;
        public List<uint>? Calls;

        public void Replay(uint sequence, ref PlayerState state)
        {
            Assert.True(Inputs.TryGet(sequence, out Step step));
            state = new PlayerState(state.X + step.Dx, state.Y + step.Dy);
            Calls?.Add(sequence);
        }
    }

    private static (InputBuffer<Step> Inputs, PredictionHistory<PlayerState> History, PlayerState Predicted) Predict(int count, uint first = 0, int capacity = 64)
    {
        InputBuffer<Step> inputs = new(64, 3, first);
        PredictionHistory<PlayerState> history = new(capacity);
        PlayerState state = default;
        for (int i = 0; i < count; i++)
        {
            Step step = new(i + 1, 1);
            uint sequence = inputs.Add(step);
            state = new PlayerState(state.X + step.Dx, state.Y + step.Dy);
            Assert.True(history.Record(sequence, state));
        }

        return (inputs, history, state);
    }

    [Fact]
    public void Constructor_Validates()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new PredictionHistory<PlayerState>(0));
        PredictionHistory<PlayerState> history = new();
        Assert.Equal(PredictionHistory<PlayerState>.DefaultCapacity, history.Capacity);
        Assert.Equal(0, history.Count);
        Assert.False(history.HasReconciled);
    }

    [Fact]
    public void Reconcile_Replays_Unconfirmed_Inputs_On_The_Authoritative_State()
    {
        (InputBuffer<Step> inputs, PredictionHistory<PlayerState> history, _) = Predict(10);
        List<uint> calls = [];
        Replayer replay = new() { Inputs = inputs, Calls = calls };
        PlayerState corrected = history.Reconcile(4, new PlayerState(100, 5), ref replay);
        Assert.Equal(new PlayerState(100 + 6 + 7 + 8 + 9 + 10, 10), corrected);
        Assert.Equal(new uint[] { 5, 6, 7, 8, 9 }, calls);
        Assert.Equal(5, history.Count);
        Assert.True(history.HasReconciled);
        Assert.Equal(4u, history.LastReconciledSequence);
        Assert.False(history.TryGetPredicted(4, out PlayerState dropped));
        Assert.Equal(default, dropped);
        Assert.True(history.TryGetPredicted(9, out PlayerState newest));
        Assert.Equal(corrected, newest);
        Assert.True(history.TryGetPredicted(5, out PlayerState five));
        Assert.Equal(new PlayerState(106, 6), five);

        // Stale or repeated authoritative states change nothing.
        calls.Clear();
        Assert.Equal(corrected, history.Reconcile(3, new PlayerState(-1, -1), ref replay));
        Assert.Equal(corrected, history.Reconcile(4, new PlayerState(-1, -1), ref replay));
        Assert.Empty(calls);
        Assert.Equal(5, history.Count);
    }

    [Fact]
    public void Correct_Predictions_Survive_Reconciliation_Unchanged()
    {
        (InputBuffer<Step> inputs, PredictionHistory<PlayerState> history, PlayerState predicted) = Predict(20);
        Replayer replay = new() { Inputs = inputs };
        PlayerState server = default;
        for (uint s = 0; s < 15; s++)
        {
            Assert.True(inputs.TryGet(s, out Step step));
            server = new PlayerState(server.X + step.Dx, server.Y + step.Dy);
            Assert.Equal(predicted, history.Reconcile(s, server, ref replay));
        }
    }

    [Fact]
    public void Reconcile_Beyond_Everything_Recorded_Returns_The_Authoritative_State()
    {
        (InputBuffer<Step> inputs, PredictionHistory<PlayerState> history, _) = Predict(5);
        List<uint> calls = [];
        Replayer replay = new() { Inputs = inputs, Calls = calls };
        PlayerState authoritative = new(7, 7);
        Assert.Equal(authoritative, history.Reconcile(20, authoritative, ref replay));
        Assert.Equal(0, history.Count);
        Assert.Empty(calls);
        Assert.False(history.Record(15, default));
        Assert.False(history.Record(20, default));
        Assert.True(history.Record(21, new PlayerState(1, 1)));

        // Stale with nothing newer recorded returns the newest recorded state; with nothing recorded, the argument.
        Assert.Equal(new PlayerState(1, 1), history.Reconcile(19, authoritative, ref replay));
        history.Clear();
        Assert.False(history.HasReconciled);
        Assert.Equal(authoritative, history.Reconcile(0, authoritative, ref replay));
        Assert.Equal(new PlayerState(3, 3), history.Reconcile(0, new PlayerState(3, 3), ref replay));
    }

    [Fact]
    public void Record_Requires_Increasing_Sequences_And_Drops_The_Oldest_When_Full()
    {
        PredictionHistory<PlayerState> history = new(3);
        Assert.True(history.Record(5, new PlayerState(5, 0)));
        Assert.False(history.Record(5, default));
        Assert.False(history.Record(4, default));
        Assert.True(history.Record(6, new PlayerState(6, 0)));
        Assert.True(history.Record(7, new PlayerState(7, 0)));
        Assert.True(history.Record(8, new PlayerState(8, 0)));
        Assert.Equal(3, history.Count);
        Assert.Equal(1, history.OverflowCount);
        Assert.False(history.TryGetPredicted(5, out _));
        Assert.True(history.TryGetPredicted(6, out PlayerState six));
        Assert.Equal(new PlayerState(6, 0), six);
    }

    [Fact]
    public void Overflow_Still_Replays_Every_Unacknowledged_Input_By_Sequence()
    {
        // Inputs 0..9 (sequence s moves X by s + 1 and Y by 1); only 6..9 still have a recorded state.
        (InputBuffer<Step> inputs, PredictionHistory<PlayerState> history, _) = Predict(10, capacity: 4);
        Assert.Equal(6, history.OverflowCount);
        List<uint> calls = [];
        Replayer replay = new() { Inputs = inputs, Calls = calls };

        PlayerState corrected = history.Reconcile(2, new PlayerState(1000, 0), ref replay);
        Assert.Equal(new uint[] { 3, 4, 5, 6, 7, 8, 9 }, calls);
        Assert.Equal(new PlayerState(1000 + 4 + 5 + 6 + 7 + 8 + 9 + 10, 7), corrected);
        Assert.True(history.TryGetPredicted(9, out PlayerState newest));
        Assert.Equal(corrected, newest);

        // Input 5 was dropped and is still unacknowledged after 4.
        calls.Clear();
        corrected = history.Reconcile(4, new PlayerState(0, 0), ref replay);
        Assert.Equal(new uint[] { 5, 6, 7, 8, 9 }, calls);
        Assert.Equal(new PlayerState(6 + 7 + 8 + 9 + 10, 5), corrected);

        // Past every dropped input only recorded entries are replayed again.
        calls.Clear();
        corrected = history.Reconcile(7, new PlayerState(0, 0), ref replay);
        Assert.Equal(new uint[] { 8, 9 }, calls);
        Assert.Equal(new PlayerState(9 + 10, 2), corrected);
        calls.Clear();
        history.Reconcile(8, new PlayerState(0, 0), ref replay);
        Assert.Equal(new uint[] { 9 }, calls);

        history.Clear();
        Assert.Equal(0, history.OverflowCount);
    }

    [Fact]
    public void Overflow_Replay_Crosses_The_Sequence_Wrap()
    {
        (InputBuffer<Step> inputs, PredictionHistory<PlayerState> history, _) = Predict(6, uint.MaxValue - 3, capacity: 2);
        List<uint> calls = [];
        Replayer replay = new() { Inputs = inputs, Calls = calls };
        history.Reconcile(uint.MaxValue - 3, default, ref replay);
        Assert.Equal(new uint[] { uint.MaxValue - 2, uint.MaxValue - 1, uint.MaxValue, 0, 1 }, calls);
    }

    [Fact]
    public void Sequences_Wrap_Around()
    {
        (InputBuffer<Step> inputs, PredictionHistory<PlayerState> history, _) = Predict(4, uint.MaxValue - 1);
        List<uint> calls = [];
        Replayer replay = new() { Inputs = inputs, Calls = calls };
        history.Reconcile(uint.MaxValue, new PlayerState(0, 0), ref replay);
        Assert.Equal(new uint[] { 0, 1 }, calls);
        Assert.Equal(2, history.Count);
    }
}
